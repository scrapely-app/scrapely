/**
 * Redirects every browser launch (Crawlee browser pool, raw playwright /
 * puppeteer) to the Scrapely remote browser over CDP. The remote browser
 * never takes a proxy: `proxy_url` is stripped from the CDP URL, custom
 * (BYOP) proxies are warned about and ignored, and location comes from
 * `browserConfiguration`, a platform ProxyConfiguration, or the run default
 * (see applyProxyInfoToCdpUrl).
 */
import { isBrowserConfiguration } from './browser-configuration.js';
import { subdivisionToStateName } from './browser-locations.js';
import { wrapConnectWithRetry } from './cdp-retry.js';
import { handlePackageManagerProcess } from './node-wrapper.js';

let _cdpUrl: string | undefined;

/**
 * Lazily get CDP URL from environment or cached value.
 * This allows non-browser actors to work without CDP being set.
 */
function getCdpUrl(): string | undefined {
    return _cdpUrl || process.env.CDP_URL;
}

/**
 * Set CDP URL explicitly (for testing or advanced use cases).
 * Pass `undefined` to clear the override and fall back to `CDP_URL` env.
 * Normally the CDP URL is read from CDP_URL env var lazily.
 */
export function setCdpUrl(url: string | undefined) {
    _cdpUrl = url;
}

/** @internal Exported for tests. */
export function getCdpUrlForTests(): string | undefined {
    return getCdpUrl();
}

/**
 * Whether the process runs on the Scrapely platform (worker-injected env).
 */
function isAtHome(): boolean {
    return !!(process.env.SCRAPELY_IS_AT_HOME || process.env.APIFY_IS_AT_HOME);
}

/**
 * Unpatched builds (browser patch off at build time) keep their browser
 * binaries; the worker sets SCRAPELY_BROWSER_PATCH=0 so every launch goes to
 * the original local launcher instead of the remote CDP browser. Read lazily
 * at launch time.
 */
function browserPatchDisabled(): boolean {
    return process.env.SCRAPELY_BROWSER_PATCH === '0';
}

/** Platform sandbox: no crash reporter; read-only rootfs + tmpfs /tmp loses crash dirs. */
const SANDBOX_CHROME_ARGS = [
    '--disable-breakpad',
    '--disable-crash-reporter',
    '--disable-dev-shm-usage',
    '--crash-dumps-dir=/tmp',
];

/** Add sandbox-safe Chrome args to local launch options when running on the platform. */
function withSandboxArgs<T extends { args?: string[] }>(options: T | undefined): T {
    const opts = { ...(options ?? {}) } as T;
    if (!isAtHome()) return opts;
    const args = [...(opts.args ?? [])];
    for (const arg of SANDBOX_CHROME_ARGS) {
        if (!args.includes(arg)) args.push(arg);
    }
    opts.args = args;
    return opts;
}

/**
 * Require CDP_URL for platform browser runs. Local launch is disabled on the
 * platform unless the build is unpatched (SCRAPELY_BROWSER_PATCH=0) — otherwise
 * a worker container without CDP_URL is a misconfiguration and must fail
 * loudly instead of launching Chromium inside the platform container.
 */
function requireCdpUrl(): string {
    const cdpUrl = getCdpUrl();
    if (!cdpUrl) {
        throw new Error(
            'cdpUrl is required. Local browser launch is disabled — all runs must use the remote browser.',
        );
    }
    return cdpUrl;
}

/** @internal Shape of the ProxyInfo fields we honor on the CDP URL. */
interface ProxyInfoLike {
    /** Full proxy URL (credentials and gateway grammar username included) */
    url?: string;
    /** Platform proxy country (our enriched ProxyInfo). */
    countryCode?: string;
    /** Platform proxy ISO 3166-2 subdivision (our enriched ProxyInfo). */
    subdivisionCode?: string;
    groups?: string[];
    sessionId?: string | number;
}

interface ProxyLocation {
    country?: string;
    subdivision?: string;
}

const BYOP_IGNORED_WARNING =
    '[scrapely] Custom proxies are ignored when using the Scrapely remote browser — it routes traffic through its own ' +
    'integrated proxy. Set the location with browserConfiguration (Actor.createBrowserConfiguration({ countryCode, state, city })) ' +
    'or ProxyConfiguration countryCode.';
const GROUPS_SESSION_INFO =
    '[scrapely] Proxy groups and session IDs do not apply to the Scrapely remote browser; routing is location-based ' +
    '(proxy_country/proxy_state). Each browser connection is a fresh session.';

let byopWarned = false;
let groupsInfoLogged = false;
let unknownSubdivisionWarned = false;

/** @internal Reset the once-per-process browser proxy messages (tests only). */
export function resetBrowserProxyWarningsForTests(): void {
    byopWarned = false;
    groupsInfoLogged = false;
    unknownSubdivisionWarned = false;
}

/** Parse a proxy URL, accepting puppeteer-style scheme-less `host:port`. */
function parseProxyUrl(proxyUrl: string): URL | undefined {
    for (const candidate of [proxyUrl, `http://${proxyUrl}`]) {
        try {
            const parsed = new URL(candidate);
            if (parsed.hostname) return parsed;
        } catch {
            // try the next form
        }
    }
    return undefined;
}

/**
 * Platform gateway hostname injected by the worker (same vars as
 * getPlatformProxyEnv). Read directly: this module also loads in SDK-less
 * processes (base image register hook) and must not pull in @crawlee/core.
 */
function platformProxyHostname(): string | undefined {
    const hostname = (process.env.SCRAPELY_PROXY_HOSTNAME || process.env.APIFY_PROXY_HOSTNAME || '').trim().toLowerCase();
    return hostname || undefined;
}

function isPlatformProxyUrl(proxyUrl: string): boolean {
    const platformHost = platformProxyHostname();
    if (!platformHost) return false;
    return parseProxyUrl(proxyUrl)?.hostname.toLowerCase() === platformHost;
}

/** `scheme://***@host:port` — never log proxy credentials. */
function redactProxyUrl(proxyUrl: string): string {
    const parsed = parseProxyUrl(proxyUrl);
    if (!parsed) return '<unparseable proxy URL>';
    const userinfo = parsed.username || parsed.password ? '***@' : '';
    return `${parsed.protocol}//${userinfo}${parsed.host}`;
}

/**
 * Location from the gateway username grammar (`sub,groups-X,session-Y,country-US_CA`).
 */
function parseProxyUrlLocation(proxyUrl: string | null | undefined): ProxyLocation {
    if (!proxyUrl) return {};
    const parsed = parseProxyUrl(proxyUrl);
    if (!parsed?.username) return {};
    let username: string;
    try {
        username = decodeURIComponent(parsed.username);
    } catch {
        username = parsed.username;
    }
    for (const fragment of username.split(',')) {
        const match = /^country-([A-Za-z]{2})(?:_([A-Za-z0-9]{1,3}))?$/.exec(fragment.trim());
        if (match) return { country: match[1].toUpperCase(), subdivision: match[2]?.toUpperCase() };
    }
    return {};
}

/** Location requested by a platform-gateway proxyInfo; `{}` for none or BYOP. */
function resolveLaunchLocation(proxyInfo: ProxyInfoLike | null | undefined): ProxyLocation {
    const proxyUrl = proxyInfo?.url;
    if (typeof proxyUrl !== 'string' || !proxyUrl) return {};

    if (!isPlatformProxyUrl(proxyUrl)) {
        if (!byopWarned) {
            byopWarned = true;
            console.warn(`${BYOP_IGNORED_WARNING} (ignored: ${redactProxyUrl(proxyUrl)})`);
        }
        return {};
    }

    if ((proxyInfo?.groups?.length || proxyInfo?.sessionId != null) && !groupsInfoLogged) {
        groupsInfoLogged = true;
        console.info(GROUPS_SESSION_INFO);
    }
    const grammar = parseProxyUrlLocation(proxyUrl);
    return {
        country: proxyInfo?.countryCode ?? grammar.country,
        subdivision: proxyInfo?.subdivisionCode ?? grammar.subdivision,
    };
}

/**
 * Build the remote browser CDP URL for one browser connection.
 *
 * The Scrapely remote browser never takes a proxy: it routes through its own
 * integrated proxy, selected by location params. Semantics:
 * - The worker-injected `proxy_country` is the run default; `proxy_url` is
 *   never sent and is removed if present.
 * - A platform-gateway `proxyInfo` (crawler ProxyConfiguration) overrides
 *   `proxy_country` / `proxy_state` (ISO subdivision codes mapped to region
 *   names). A custom (BYOP) proxy is warned about once and ignored.
 * - `browserConfiguration` params always win.
 * - When nothing changes, the CDP URL is returned byte-identical.
 *
 * @internal Exported for tests.
 */
export function applyProxyInfoToCdpUrl(
    cdpUrl: string,
    proxyInfo?: ProxyInfoLike | null,
    browserConfiguration?: unknown,
): string {
    if (browserConfiguration != null && !isBrowserConfiguration(browserConfiguration)) {
        throw new Error(
            'browserConfiguration must be created with Actor.createBrowserConfiguration() or new BrowserConfiguration().',
        );
    }

    const url = new URL(cdpUrl);
    const params = url.searchParams;
    let changed = false;

    const setParam = (key: string, value: string) => {
        if (params.get(key) === value) return;
        params.set(key, value);
        changed = true;
    };

    if (params.has('proxy_url')) {
        params.delete('proxy_url');
        changed = true;
    }

    const location = resolveLaunchLocation(proxyInfo);
    const country = location.country?.toUpperCase();
    let state: string | undefined;
    if (location.subdivision) {
        state = subdivisionToStateName(country, location.subdivision);
        if (!state && !unknownSubdivisionWarned) {
            unknownSubdivisionWarned = true;
            console.warn(
                `[scrapely] Subdivision "${country ?? '?'}-${location.subdivision}" has no Scrapely browser mapping; ` +
                    "routing by country only. Use browserConfiguration { state: '<name>' } to target a region.",
            );
        }
    }
    if (country) setParam('proxy_country', country);
    if (state) setParam('proxy_state', state);

    if (browserConfiguration != null) {
        for (const [key, value] of Object.entries(browserConfiguration.toCdpParams())) {
            setParam(key, value);
        }
    }

    return changed ? url.toString() : cdpUrl;
}

/**
 * @internal Normalize a browser-pool LaunchContext's proxy data into ProxyInfoLike.
 *
 * Crawlee's browser crawler sets both `launchContext.proxyInfo` (the ProxyInfo
 * object, `.url` included) and `launchContext.proxyUrl` (the full proxy URL)
 * per browser launch. The URL fallback covers callers that only provide the URL.
 */
function proxyInfoFromLaunchContext(launchContext: any): ProxyInfoLike | undefined {
    const info = launchContext?.proxyInfo;
    if (typeof info?.url === 'string' && info.url) {
        return {
            url: info.url,
            countryCode: info.countryCode,
            subdivisionCode: info.subdivisionCode,
            groups: info.groups,
            sessionId: info.sessionId,
        };
    }

    const proxyUrl = launchContext?.proxyUrl;
    if (typeof proxyUrl === 'string' && proxyUrl) return { url: proxyUrl };

    return undefined;
}

/**
 * Resolve Playwright's chromium BrowserType for CDP connect.
 *
 * Crawlee's PlaywrightPlugin.library is a BrowserType (chromium|firefox|webkit).
 * Remote browser is always Chromium CDP, so firefox/webkit/camoufox launchers
 * are redirected to chromium.connectOverCDP — never denied, never local launch.
 */
function getPlaywrightChromium(library: any): { connectOverCDP: (url: string) => Promise<any> } {
    // Full playwright module passed as library (has .chromium)
    if (library?.chromium && typeof library.chromium.connectOverCDP === 'function') {
        return library.chromium;
    }

    // BrowserType that is already chromium
    if (
        typeof library?.connectOverCDP === 'function' &&
        typeof library?.name === 'function' &&
        library.name() === 'chromium'
    ) {
        return library;
    }

    // firefox / webkit / unknown BrowserType → force chromium from playwright
    // Prefer playwright, fall back to playwright-core (images may ship either).
    try {
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        const pw = require('playwright');
        if (pw?.chromium?.connectOverCDP) {
            return pw.chromium;
        }
    } catch {
        // ignore
    }
    try {
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        const pwCore = require('playwright-core');
        if (pwCore?.chromium?.connectOverCDP) {
            return pwCore.chromium;
        }
    } catch {
        // ignore
    }

    // Last resort: if library has connectOverCDP, use it (may be chromium-shaped)
    if (typeof library?.connectOverCDP === 'function') {
        return library;
    }

    throw new Error(
        'Unable to resolve Playwright chromium for remote CDP connect. Install playwright or playwright-core.',
    );
}

/**
 * Redirect a PlaywrightPlugin class's `_launch` to remote Chromium CDP
 * (chromium / firefox / webkit / camoufox) on the platform; a normal local
 * launch when running locally without CDP_URL. Idempotent per class.
 */
function patchPlaywrightPluginClass(PlaywrightPlugin: any): void {
    const proto = PlaywrightPlugin?.prototype;
    if (!proto || typeof proto._launch !== 'function' || proto._scrapelyCdpPatched) return;
    const origPlaywrightLaunch = proto._launch;
    proto._launch = async function (launchContext: any) {
        if (browserPatchDisabled() || (!getCdpUrl() && !isAtHome())) {
            return origPlaywrightLaunch.call(this, launchContext);
        }
        const cdpUrl = applyProxyInfoToCdpUrl(
            requireCdpUrl(),
            proxyInfoFromLaunchContext(launchContext),
            launchContext?.browserConfiguration,
        );
        const chromium = getPlaywrightChromium(this.library);
        wrapConnectWithRetry(chromium, 'connectOverCDP', getCdpUrl);
        return chromium.connectOverCDP(cdpUrl);
    };
    proto._scrapelyCdpPatched = true;
}

/**
 * Redirect a PuppeteerPlugin class's `_launch` to remote Chromium CDP via
 * browserWSEndpoint on the platform; a normal local launch when running
 * locally without CDP_URL. Idempotent per class.
 */
function patchPuppeteerPluginClass(PuppeteerPlugin: any): void {
    const proto = PuppeteerPlugin?.prototype;
    if (!proto || typeof proto._launch !== 'function' || proto._scrapelyCdpPatched) return;
    const origPuppeteerLaunch = proto._launch;
    proto._launch = async function (launchContext: any) {
        if (browserPatchDisabled() || (!getCdpUrl() && !isAtHome())) {
            return origPuppeteerLaunch.call(this, launchContext);
        }
        const cdpUrl = applyProxyInfoToCdpUrl(
            requireCdpUrl(),
            proxyInfoFromLaunchContext(launchContext),
            launchContext?.browserConfiguration,
        );
        const launcher = this.library;
        if (!launcher || typeof launcher.connect !== 'function') {
            throw new Error(
                'Puppeteer library is missing connect(). Install puppeteer or puppeteer-core.',
            );
        }
        wrapConnectWithRetry(launcher, 'connect', getCdpUrl);
        return launcher.connect({ browserWSEndpoint: cdpUrl });
    };
    proto._scrapelyCdpPatched = true;
}

/**
 * Patch the browser plugins exported by one loaded copy of
 * `@crawlee/browser-pool` (or one of its plugin files). Used for the SDK's own
 * copy (eager pass below) and for every actor-local copy (Module._load hook),
 * since an actor that installs its own Crawlee resolves a different instance.
 *
 * @internal Exported for tests.
 */
export function patchBrowserPoolModule(mod: any): void {
    if (!mod || (typeof mod !== 'object' && typeof mod !== 'function')) return;
    patchPlaywrightPluginClass(mod.PlaywrightPlugin);
    patchPuppeteerPluginClass(mod.PuppeteerPlugin);
    patchControllerProxyOptions(mod.PlaywrightController);
    patchControllerProxyOptions(mod.PuppeteerController);
}

/**
 * With incognito pages Crawlee turns the launch proxy into per-context proxy
 * options. On the remote browser that would route pages through a custom
 * proxy (or double-proxy the gateway), so no per-page proxy is set while
 * connected over CDP.
 */
function patchControllerProxyOptions(Controller: any): void {
    const proto = Controller?.prototype;
    if (!proto || typeof proto.normalizeProxyOptions !== 'function' || proto._scrapelyProxyOptionsPatched) return;
    const origNormalize = proto.normalizeProxyOptions;
    proto.normalizeProxyOptions = function (this: any, ...args: unknown[]) {
        if (!browserPatchDisabled() && getCdpUrl()) return {};
        return origNormalize.apply(this, args);
    };
    proto._scrapelyProxyOptionsPatched = true;
}

// ─── Crawler `browserConfiguration` option ──────────────────────────────────
//
// Crawlee validates crawler options with ow.exactShape, and every crawler
// class copies its parent's optionsShape at module load, so the option is
// added to each class's own shape: BasicCrawler (browser crawlers forward
// unknown keys to it) and every browser crawler. HTTP crawlers never accept
// it — the key is removed from their (possibly copied) shapes so passing it
// there fails with Crawlee's normal unknown-option error.
//
// The predicate reuses the class's own `sessionPoolOptions` predicate
// (ow.optional.object): an ow predicate from a different ow copy is not
// recognized as a predicate by exactShape.
//
// Acceptance is never gated on CDP_URL / SCRAPELY_BROWSER_PATCH — only the
// `_launch` redirect is — so the same actor code runs locally unchanged.

function isCrawlerClass(value: any): boolean {
    return typeof value === 'function' && !!value.optionsShape && typeof value.optionsShape === 'object';
}

function patchCrawlerClass(CrawlerClass: any): void {
    const shape = CrawlerClass.optionsShape;
    if (!Object.prototype.hasOwnProperty.call(CrawlerClass, 'optionsShape')) return;
    const isBrowserCrawler = 'launchContext' in shape;
    const isBasicCrawler = CrawlerClass.name === 'BasicCrawler';
    if (isBrowserCrawler || isBasicCrawler) {
        if (!('browserConfiguration' in shape) && shape.sessionPoolOptions) {
            shape.browserConfiguration = shape.sessionPoolOptions;
        }
    } else if ('browserConfiguration' in shape) {
        delete shape.browserConfiguration;
    }

    const proto = CrawlerClass.prototype;
    if (
        proto &&
        Object.prototype.hasOwnProperty.call(proto, '_extendLaunchContext') &&
        typeof proto._extendLaunchContext === 'function' &&
        !proto._scrapelyBrowserConfigPatched
    ) {
        const origExtend = proto._extendLaunchContext;
        proto._extendLaunchContext = async function (this: any, pageId: unknown, launchContext: any) {
            await origExtend.call(this, pageId, launchContext);
            const browserConfiguration = this.options?.browserConfiguration;
            if (browserConfiguration != null && launchContext && launchContext.browserConfiguration === undefined) {
                if (typeof launchContext.extend === 'function') {
                    launchContext.extend({ browserConfiguration });
                } else {
                    launchContext.browserConfiguration = browserConfiguration;
                }
            }
        };
        proto._scrapelyBrowserConfigPatched = true;
    }
}

/**
 * Add the `browserConfiguration` option to the crawler classes exported by
 * one loaded Crawlee module (package index or crawler file). Idempotent.
 *
 * @internal Exported for tests.
 */
export function patchCrawleeCrawlerModule(mod: any): void {
    if (!mod || (typeof mod !== 'object' && typeof mod !== 'function')) return;
    const candidates = isCrawlerClass(mod) ? [mod] : [];
    for (const key of Object.keys(mod)) {
        let value: unknown;
        try {
            value = mod[key];
        } catch {
            continue;
        }
        if (isCrawlerClass(value)) candidates.push(value);
    }
    for (const CrawlerClass of candidates) patchCrawlerClass(CrawlerClass);
}

const CRAWLER_PACKAGES = 'basic|browser|playwright|puppeteer|http|cheerio|jsdom|linkedom';

/** Requests that load (a part of) some copy of a Crawlee crawler package. */
const CRAWLER_REQUEST_RE = new RegExp(
    `^(@crawlee/(${CRAWLER_PACKAGES})|crawlee)$` +
        `|[/\\\\]@crawlee[/\\\\](${CRAWLER_PACKAGES})[/\\\\]index\\.js$` +
        `|[/\\\\]node_modules[/\\\\]crawlee[/\\\\]index\\.js$` +
        '|(^|[/\\\\])internals[/\\\\][a-z-]+-crawler(\\.js)?$',
);

/** Cached module filenames of Crawlee crawler packages (any copy). */
const CRAWLER_FILENAME_RE = new RegExp(
    `[/\\\\](@crawlee[/\\\\](${CRAWLER_PACKAGES})|crawlee)[/\\\\](internals[/\\\\][a-z-]+-crawler\\.js|index\\.js)$`,
);

/**
 * Patch Crawlee copies that loaded before this module (e.g. an actor that
 * imports `crawlee` before `scrapely` in a local run). Copies loaded later are
 * patched by the Module._load hook.
 *
 * @internal Exported for tests.
 */
export function patchLoadedCrawleeCopies(): void {
    const cache: Record<string, { exports?: unknown } | undefined> = require.cache ?? {};
    for (const [filename, cached] of Object.entries(cache)) {
        if (cached?.exports && CRAWLER_FILENAME_RE.test(filename)) {
            patchCrawleeCrawlerModule(cached.exports);
        }
    }
}

// ─── Crawlee launchers: `cdpUrl` option + sandbox-safe local launch args ─────
//
// Applied when a launcher module loads (Module._load hook below), never by
// importing Crawlee up front: loading @crawlee/browser-pool and playwright
// eagerly cost ~2.4 s per Node process on the platform.

/** Requests that load a Crawlee launcher file (`require('./internals/browser-launcher')`). */
const LAUNCHER_REQUEST_RE = /(^|[/\\])internals[/\\](browser|playwright|puppeteer)-launcher(\.js)?$/;

/** Cached module filenames of Crawlee launcher files (any copy). */
const LAUNCHER_FILENAME_RE =
    /[/\\]@crawlee[/\\](browser|playwright|puppeteer)[/\\]internals[/\\](browser|playwright|puppeteer)-launcher\.js$/;

/**
 * An `ow.optional.string` predicate from the same ow copy as the launcher's
 * own shape (a predicate from another ow copy is not recognized by
 * exactShape). Reuses an existing optional-string entry, else loads ow next
 * to the module that required the launcher.
 */
function optionalStringPredicate(shape: Record<string, unknown>, parentFilename: string | undefined): unknown {
    if (shape.userAgent) return shape.userAgent;
    if (shape.userDataDir) return shape.userDataDir;
    if (!parentFilename) return undefined;
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { createRequire } = require('module');
    const owModule = createRequire(parentFilename)('ow');
    return (owModule.default ?? owModule).optional.string;
}

function addCdpUrlOption(Launcher: any, parentFilename: string | undefined): void {
    const shape = Launcher?.optionsShape;
    if (!shape || typeof shape !== 'object' || 'cdpUrl' in shape) return;
    const predicate = optionalStringPredicate(shape, parentFilename);
    if (predicate) shape.cdpUrl = predicate;
}

/**
 * Patch the launcher classes exported by one loaded Crawlee launcher file (or
 * package index). Idempotent.
 *
 * - `cdpUrl` becomes a valid launcher option (PlaywrightLauncher and
 *   PuppeteerLauncher copy BrowserLauncher's shape when their file loads, so
 *   each class gets it; browser-launcher.js loads first, so the copies inherit it).
 * - Local launches get sandbox-safe Chrome args on the platform. Playwright/Puppeteer
 *   pool launches use CDP and skip this.
 *
 * @internal Exported for tests.
 */
export function patchBrowserLauncherModule(mod: any, parentFilename?: string): void {
    if (!mod || (typeof mod !== 'object' && typeof mod !== 'function')) return;
    for (const key of ['BrowserLauncher', 'PlaywrightLauncher', 'PuppeteerLauncher']) {
        let Launcher: any;
        try {
            Launcher = mod[key];
        } catch {
            continue;
        }
        if (typeof Launcher !== 'function') continue;
        addCdpUrlOption(Launcher, parentFilename);
    }

    const BrowserLauncher = mod.BrowserLauncher;
    const proto = BrowserLauncher?.prototype;
    if (!proto || typeof proto.createLaunchOptions !== 'function' || proto._scrapelySandboxArgsPatched) return;
    const origCreateLaunchOptions = proto.createLaunchOptions;
    proto.createLaunchOptions = function (this: unknown, ...args: unknown[]) {
        const launchOptions = origCreateLaunchOptions.apply(this, args);

        // The sandbox has no Chrome crashpad_handler support. Also needed
        // with ReadonlyRootfs + tmpfs /tmp where /tmp/.X11-unix and the crashpad
        // database dir don't persist from the base image.
        if (isAtHome() && launchOptions) {
            launchOptions.args = launchOptions.args || [];
            for (const arg of SANDBOX_CHROME_ARGS) {
                if (!launchOptions.args.includes(arg)) {
                    launchOptions.args.push(arg);
                }
            }
        }

        return launchOptions;
    };
    proto._scrapelySandboxArgsPatched = true;
}

// ─── Raw Playwright/Puppeteer API → remote CDP ──────────────────────────────
//
// SDK-less Node actors call playwright (or puppeteer) directly instead of
// going through Crawlee's browser pool, so the pool patches above never see
// those launches and a local browser would silently bypass the platform's
// remote browser. Patch the BrowserType instances / puppeteer launcher
// themselves. Semantics mirror the Python SDK's raw patch:
// - CDP URL configured → connectOverCDP (playwright) / connect({browserWSEndpoint})
//   (puppeteer); local-only launch options are dropped with a warning. The
//   remote browser takes no proxy: a launch `proxy` option only contributes its
//   location when it is the platform gateway, otherwise it is warned about and
//   ignored. A `browserConfiguration` launch option sets the browser params.
// - `browserConfiguration` is always removed before any original launch, so
//   local runs accept and ignore it.
// - No CDP URL, not on the platform → original local launch (local development).
// - No CDP URL, on the platform → throw: local launch is disabled.
// - SCRAPELY_BROWSER_PATCH=0 (unpatched build) → original local launch, with
//   sandbox-safe Chrome args on the platform, even when a CDP URL is configured.
//
// Delivery: the scrapely/actor-node* base images set
// NODE_OPTIONS=--import /opt/scrapely/register.mjs, which imports this module
// in every node process — reaching actors that never import apify/scrapely.
//
// Nothing is imported up front. Every patch applies when the target module
// loads, whichever copy (SDK tree or actor workdir) the request resolved to:
// - CommonJS loads, including CJS packages imported from ESM (Node passes the
//   resolved filename): a Module._load hook.
// - Pure-ESM puppeteer builds (`import puppeteer from 'puppeteer'`): a load
//   hook patches the PuppeteerNode class as its module evaluates, before the
//   default instance and its bound `launch`/`connect` exports exist.
// - Modules that loaded before this one (e.g. a local run that imports
//   playwright before scrapely): patched from require.cache.

/** @internal Launch options shape shared by raw playwright + puppeteer launches. */
interface RawLaunchOptionsLike {
    headless?: boolean;
    args?: string[];
    executablePath?: string;
    channel?: string;
    env?: unknown;
    proxy?: { server?: string; username?: string; password?: string };
    timeout?: number;
    slowMo?: number;
    userDataDir?: string;
    browserConfiguration?: unknown;
    [key: string]: unknown;
}

/**
 * Split our `browserConfiguration` option off raw launch options. The options
 * object is returned as-is when the key is absent.
 */
function takeBrowserConfiguration(options: RawLaunchOptionsLike | undefined): {
    options: RawLaunchOptionsLike;
    browserConfiguration: unknown;
} {
    if (!options || typeof options !== 'object' || !('browserConfiguration' in options)) {
        return { options: options ?? {}, browserConfiguration: undefined };
    }
    const { browserConfiguration, ...rest } = options;
    return { options: rest, browserConfiguration };
}

/** Options that only make sense for a local launch (no remote-CDP equivalent). */
const RAW_LAUNCH_ONLY_KEYS = new Set([
    'headless',
    'args',
    'ignoreDefaultArgs',
    'ignoreArgs',
    'executablePath',
    'channel',
    'env',
    'devtools',
    'chromiumSandbox',
    'firefoxUserPrefs',
    'proxy',
    'tracesDir',
    'waitForExtensions',
    'userDataDir',
    'user_data_dir',
    'downloadsPath',
]);

function composeRawProxyUrl(proxy?: { server?: string; username?: string; password?: string }): string | undefined {
    if (!proxy?.server) return undefined;
    if (proxy.username) {
        try {
            const url = new URL(proxy.server);
            url.username = proxy.username;
            url.password = proxy.password ?? '';
            return url.toString();
        } catch {
            return proxy.server;
        }
    }
    return proxy.server;
}

function rawLaunchCdpUrl(options: RawLaunchOptionsLike | undefined, browserConfiguration: unknown): string | undefined {
    const cdpUrl = getCdpUrl();
    if (!cdpUrl) return undefined;
    const proxyUrl = composeRawProxyUrl(options?.proxy);
    return applyProxyInfoToCdpUrl(cdpUrl, proxyUrl ? { url: proxyUrl } : undefined, browserConfiguration);
}

function rawConnectOptions(options: RawLaunchOptionsLike): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    if (options.timeout !== undefined) out.timeout = options.timeout;
    if (options.slowMo !== undefined) out.slowMo = options.slowMo;
    return out;
}

function warnDroppedRawOptions(kind: string, name: string, options: RawLaunchOptionsLike): void {
    const dropped = Object.keys(options).filter((k) => k !== 'timeout' && k !== 'slowMo');
    if (dropped.length > 0) {
        console.warn(
            `[scrapely] playwright ${name}.${kind} redirected to the remote CDP browser; ` +
                `local launch options ignored: ${dropped.join(', ')} ` +
                '(the remote browser manages headless mode, binary and flags).',
        );
    }
}
/** @internal Exported for tests. */
export function patchRawBrowserType(chromium: { connectOverCDP: (url: string, opts?: unknown) => Promise<any> }, browserType: any, name: string): void {
    if (!browserType || typeof browserType.launch !== 'function') return;
    if (browserType._scrapelyRawCdpPatched) return;

    const origLaunch = browserType.launch;
    const origLpc = browserType.launchPersistentContext;

    browserType.launch = async function rawLaunchPatched(this: any, launchOptions: RawLaunchOptionsLike = {}) {
        const { options, browserConfiguration } = takeBrowserConfiguration(launchOptions);
        if (browserPatchDisabled()) return origLaunch.call(this, withSandboxArgs(options));
        const cdpUrl = rawLaunchCdpUrl(options, browserConfiguration);
        if (!cdpUrl) {
            if (!isAtHome()) return origLaunch.call(this, options);
            throw new Error('cdpUrl is required. Local browser launch is disabled — all runs must use the remote browser.');
        }
        warnDroppedRawOptions('launch', name, options);
        return chromium.connectOverCDP(cdpUrl, rawConnectOptions(options));
    };

    if (typeof origLpc === 'function') {
        browserType.launchPersistentContext = async function rawLpcPatched(this: any, ...lpcArgs: any[]) {
            // Playwright signature: launchPersistentContext(userDataDir, options);
            // an options-only call is accepted too.
            const optionsIndex = typeof lpcArgs[0] === 'string' ? 1 : 0;
            const userDataDir = optionsIndex === 1 ? lpcArgs[0] : undefined;
            const { options, browserConfiguration } = takeBrowserConfiguration(lpcArgs[optionsIndex]);
            const withOptions = (opts: RawLaunchOptionsLike | undefined) =>
                optionsIndex === 1 ? [userDataDir, opts] : [opts];

            if (browserPatchDisabled()) return origLpc.call(this, ...withOptions(withSandboxArgs(options)));
            const cdpUrl = rawLaunchCdpUrl(options, browserConfiguration);
            if (!cdpUrl) {
                if (!isAtHome()) {
                    return origLpc.call(this, ...withOptions(lpcArgs[optionsIndex] === undefined ? undefined : options));
                }
                throw new Error('cdpUrl is required. Local browser launch is disabled — all runs must use the remote browser.');
            }
            const profileDir = userDataDir ?? options.userDataDir;
            if (profileDir) {
                console.warn(
                    `[scrapely] playwright ${name}.launchPersistentContext(userDataDir=${profileDir}) redirected to the ` +
                        'remote CDP browser — persistent profiles have no remote equivalent; a fresh context is returned.',
                );
            }
            warnDroppedRawOptions('launchPersistentContext', name, options);
            const browser = await chromium.connectOverCDP(cdpUrl, rawConnectOptions(options));
            const contextOptions: Record<string, unknown> = {};
            for (const [k, v] of Object.entries(options)) {
                if (!RAW_LAUNCH_ONLY_KEYS.has(k) && k !== 'timeout' && k !== 'slowMo') contextOptions[k] = v;
            }
            return browser.newContext(contextOptions);
        };
    }

    browserType._scrapelyRawCdpPatched = true;
}

/** Patch the chromium/firefox/webkit BrowserTypes of a playwright module. */
function patchRawPlaywrightModule(pw: any): void {
    if (!pw?.chromium || typeof pw.chromium.connectOverCDP !== 'function') return;
    // Retry transient connect failures (429/5xx/network) for the platform CDP
    // host: covers direct chromium.connectOverCDP(CDP_URL) calls in actor code
    // and the redirected launches, which call this same property.
    wrapConnectWithRetry(pw.chromium, 'connectOverCDP', getCdpUrl);
    for (const name of ['chromium', 'firefox', 'webkit']) {
        patchRawBrowserType(pw.chromium, pw[name], name);
    }
}

/**
 * The patched puppeteer `launch`: remote CDP connect on the platform, the
 * original local launch otherwise. `connect` is the launcher's (retry-wrapped)
 * connect; `origLaunch` the unpatched launch, both called with `self`.
 */
async function puppeteerLaunchPatched(
    self: any,
    origLaunch: (this: any, options: RawLaunchOptionsLike) => Promise<any>,
    moduleName: string,
    launchOptions: RawLaunchOptionsLike = {},
): Promise<any> {
    const { options, browserConfiguration } = takeBrowserConfiguration(launchOptions);
    if (browserPatchDisabled()) return origLaunch.call(self, withSandboxArgs(options));
    const cdpUrl = rawLaunchCdpUrl(options, browserConfiguration);
    if (!cdpUrl) {
        if (!isAtHome()) return origLaunch.call(self, options);
        throw new Error('cdpUrl is required. Local browser launch is disabled — all runs must use the remote browser.');
    }
    const dropped = Object.keys(options).filter((k) => k !== 'timeout' && k !== 'slowMo' && k !== 'protocol');
    if (dropped.length > 0) {
        console.warn(
            `[scrapely] ${moduleName}.launch redirected to the remote CDP browser; ` +
                `local launch options ignored: ${dropped.join(', ')}.`,
        );
    }
    return self.connect({ browserWSEndpoint: cdpUrl, ...rawConnectOptions(options) });
}

/**
 * Patch puppeteer's PuppeteerNode class (puppeteer-core `node/PuppeteerNode.js`,
 * shared by puppeteer and puppeteer-core, CJS and ESM builds). The package
 * entry creates its default instance right after this class loads, and the
 * constructor binds `launch`/`connect` from the prototype, so patching the
 * prototype first also covers the named exports (`import { launch } from
 * 'puppeteer'`), which are those bound methods. Idempotent per class.
 *
 * @internal Exported for tests.
 */
export function patchPuppeteerNodeClass(PuppeteerNode: any): void {
    const proto = PuppeteerNode?.prototype;
    if (!proto || typeof proto.launch !== 'function') return;
    if (Object.prototype.hasOwnProperty.call(proto, '_scrapelyRawCdpPatched')) return;
    wrapConnectWithRetry(proto, 'connect', getCdpUrl);
    const origLaunch = proto.launch;
    proto.launch = function rawPuppeteerLaunchPatched(this: any, launchOptions?: RawLaunchOptionsLike) {
        const moduleName = this?._isPuppeteerCore === false ? 'puppeteer' : 'puppeteer-core';
        return puppeteerLaunchPatched(this, origLaunch, moduleName, launchOptions);
    };
    proto._scrapelyRawCdpPatched = true;
}

/**
 * Patch a raw puppeteer launcher instance: the default export (also exposed as
 * module props). Covers puppeteer copies whose instance already exists (loaded
 * before this module) or whose class could not be patched first. Skipped when
 * the instance's class is already patched.
 */
function patchRawPuppeteerModule(mod: any, moduleName: string): void {
    const launcher = mod?.default ?? mod;
    if (typeof launcher?.launch !== 'function' || launcher._scrapelyRawCdpPatched) {
        return;
    }
    const origBoundLaunch = launcher.launch;
    const origBoundConnect = launcher.connect;
    wrapConnectWithRetry(launcher, 'connect', getCdpUrl);
    const origLaunch = launcher.launch.bind(launcher);

    launcher.launch = async function rawPuppeteerLaunchPatched(launchOptions: RawLaunchOptionsLike = {}) {
        return puppeteerLaunchPatched(launcher, origLaunch, moduleName, launchOptions);
    };
    launcher._scrapelyRawCdpPatched = true;

    // Named exports (`exports.launch = puppeteer.launch`) hold the original
    // bound methods; point them at the patched ones.
    if (mod && mod !== launcher) {
        for (const [key, original] of [
            ['launch', origBoundLaunch],
            ['connect', origBoundConnect],
        ] as const) {
            try {
                if (mod[key] === original && mod[key] !== launcher[key]) mod[key] = launcher[key];
            } catch {
                // read-only export binding — the default export is still patched
            }
        }
        // ESM-only puppeteer (v25+) loaded through require(): the named
        // `launch` is a read-only binding to the original method, which hands
        // off to a BrowserLauncher. Patch that class so it redirects too.
        if (mod.launch === origBoundLaunch) patchPuppeteerBrowserLauncherClass(mod.BrowserLauncher, moduleName);
    }
}

/**
 * Patch puppeteer's BrowserLauncher class (base of ChromeLauncher and
 * FirefoxLauncher), which every PuppeteerNode launch goes through. Calls that
 * already went through a patched launch decide the same way again (no CDP URL
 * → local launch). Idempotent per class.
 */
function patchPuppeteerBrowserLauncherClass(BrowserLauncher: any, moduleName: string): void {
    const proto = BrowserLauncher?.prototype;
    if (!proto || typeof proto.launch !== 'function') return;
    if (Object.prototype.hasOwnProperty.call(proto, '_scrapelyRawCdpPatched')) return;
    const origLaunch = proto.launch;
    proto.launch = function browserLauncherLaunchPatched(this: any, launchOptions?: RawLaunchOptionsLike) {
        return puppeteerLaunchPatched(
            this.puppeteer,
            (options) => origLaunch.call(this, options),
            moduleName,
            launchOptions,
        );
    };
    proto._scrapelyRawCdpPatched = true;
}

/**
 * Requests that load (a part of) some copy of @crawlee/browser-pool:
 * - the bare package name (require from @crawlee/playwright etc.),
 * - an absolute path to its index.js (ESM → CJS interop loads by filename),
 * - the relative plugin files the package's own index.js requires.
 */
const BROWSER_POOL_REQUEST_RE =
    /^@crawlee\/browser-pool$|[/\\]@crawlee[/\\]browser-pool[/\\]index\.js$|(^|[/\\])(playwright|puppeteer)-plugin(\.js)?$/;

/** Cached module filenames of @crawlee/browser-pool (index or plugin files, any copy). */
const BROWSER_POOL_FILENAME_RE =
    /[/\\]@crawlee[/\\]browser-pool[/\\](index|playwright[/\\]playwright-plugin|puppeteer[/\\]puppeteer-plugin)\.js$/;

/** playwright / playwright-core package entry, by filename (ESM imports and require.cache). */
const PLAYWRIGHT_ENTRY_FILENAME_RE = /[/\\]node_modules[/\\](playwright|playwright-core)[/\\]index\.js$/;

/**
 * puppeteer / puppeteer-core package entry (default instance), by filename:
 * `lib/cjs/puppeteer/…` up to v24, `lib/puppeteer/…` in the ESM-only v25+.
 */
const PUPPETEER_ENTRY_FILENAME_RE =
    /[/\\]node_modules[/\\](puppeteer|puppeteer-core)[/\\]lib[/\\](?:cjs[/\\])?puppeteer[/\\](puppeteer|puppeteer-core)\.js$/;

/** puppeteer-core's PuppeteerNode class file (CJS), as requested by node/node.js or cached. */
const PUPPETEER_NODE_REQUEST_RE = /(^|[/\\])PuppeteerNode(\.js)?$/;

/**
 * Apply the patches to a module coming back from the hooked loader.
 * `request` is the specifier as written by the caller (an absolute filename
 * when ESM imports a CJS package), `loaded` is whatever instance it actually
 * resolved to, and `loadRoot` resolves a package root through the original
 * loader (no hook recursion) for subpath requests.
 */
function patchLoadedRawModule(
    request: unknown,
    loaded: any,
    loadRoot: (name: string) => any,
    parentFilename: string | undefined,
): void {
    if (typeof request !== 'string') return;

    // Crawlee browser pool — the actor's own copy (e.g. /usr/src/app/node_modules)
    // is a different instance from the SDK's, so its plugins need the CDP patch too.
    if (BROWSER_POOL_REQUEST_RE.test(request)) {
        patchBrowserPoolModule(loaded);
        return;
    }

    if (LAUNCHER_REQUEST_RE.test(request)) {
        patchBrowserLauncherModule(loaded, parentFilename);
        return;
    }

    if (CRAWLER_REQUEST_RE.test(request)) {
        patchCrawleeCrawlerModule(loaded);
        return;
    }

    if (request === 'playwright' || request === 'playwright-core' || PLAYWRIGHT_ENTRY_FILENAME_RE.test(request)) {
        patchRawPlaywrightModule(loaded);
        return;
    }

    if (PUPPETEER_NODE_REQUEST_RE.test(request)) {
        patchPuppeteerNodeClass(loaded?.PuppeteerNode);
        return;
    }
    if (request === 'puppeteer' || request === 'puppeteer-core') {
        patchRawPuppeteerModule(loaded, request);
        return;
    }
    const puppeteerEntry = PUPPETEER_ENTRY_FILENAME_RE.exec(request);
    if (puppeteerEntry) {
        patchRawPuppeteerModule(loaded, puppeteerEntry[1]);
        return;
    }

    // BrowserType subpath loads, e.g. require('playwright/chromium') — the
    // export is the BrowserType itself, not the package root. Resolve the root
    // for its chromium (the connectOverCDP anchor), patch the root, then the
    // subpath export. The launch() shape check ignores deep imports into the
    // package that are not BrowserTypes.
    if (!request.startsWith('playwright/') && !request.startsWith('playwright-core/')) return;
    const subpath = /^(playwright|playwright-core)\/(.+)$/.exec(request);
    if (subpath && typeof loaded?.launch === 'function') {
        try {
            const root = loadRoot(subpath[1]);
            patchRawPlaywrightModule(root);
            if (root?.chromium) patchRawBrowserType(root.chromium, loaded, subpath[2]);
        } catch {
            // package root unresolvable — no chromium anchor to connect through
        }
    }
}

/**
 * `require('puppeteer')` of an ESM-only puppeteer (v25+) on Node without
 * module.registerHooks (Node 20, 22.0-22.14): require(esm) bypasses both
 * Module._load for its inner modules and the off-thread ESM hook, and the
 * returned namespace is frozen, so the named `launch` export would keep the
 * original. Load and patch the PuppeteerNode class first (same ESM module
 * instance the entry imports), so the entry's default instance binds the
 * patched methods. Older CJS builds are covered by the regular hook anyway.
 */
const puppeteerEntriesPreloaded = new Set<string>();

function preloadPuppeteerNodeClass(request: string, parent: any, origLoad: (...args: any[]) => any): void {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const Module: any = require('module');
    if (typeof Module.registerHooks === 'function') return;
    const parentDir = typeof parent?.path === 'string' ? parent.path : '';
    const key = `${parentDir}\0${request}`;
    if (puppeteerEntriesPreloaded.has(key)) return;
    puppeteerEntriesPreloaded.add(key);
    try {
        // puppeteer's own puppeteer-core (may be nested under puppeteer).
        const entry = Module._resolveFilename(request, parent, false);
        const coreRequest = 'puppeteer-core/internal/node/PuppeteerNode.js';
        const corePath = Module.createRequire(entry).resolve(coreRequest);
        if (!/[/\\]lib[/\\]puppeteer[/\\]node[/\\]PuppeteerNode\.js$/.test(corePath)) return;
        patchPuppeteerNodeClass(origLoad(corePath, parent, false)?.PuppeteerNode);
    } catch {
        // not installed / different layout: the regular patches still apply
    }
}

/**
 * Hook the CommonJS loader so the patches reach every copy of the browser
 * libraries and Crawlee, whenever they load.
 *
 * Module._load is the chokepoint every require() passes through AFTER
 * resolution — including CJS packages imported from ESM, which Node funnels
 * through the CJS loader with the resolved filename — so patching here covers
 * whichever copy (SDK tree or actor workdir) the request actually resolved to.
 * Chained onto the current _load (composes with tsx's hook in dev) and
 * idempotent.
 */
function installRawModuleLoadHook(): void {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const Module: any = require('module');
    const origLoad: any = Module._load;
    if (typeof origLoad !== 'function' || origLoad._scrapelyRawCdpHook) return;

    const hookedLoad = function (this: unknown, request: unknown, parent: any, isMain: boolean) {
        if (request === 'puppeteer' || request === 'puppeteer-core') {
            preloadPuppeteerNodeClass(request, parent, origLoad);
        }
        const loaded = origLoad.apply(this, [request, parent, isMain]);
        try {
            patchLoadedRawModule(
                request,
                loaded,
                (name: string) => origLoad(name, parent, isMain),
                typeof parent?.filename === 'string' ? parent.filename : undefined,
            );
        } catch (err) {
            // Never break the actor's module loading — warn and hand back the
            // unpatched module rather than crashing the process.
            console.warn(`[scrapely] CDP patch failed for '${String(request)}':`, (err as Error)?.message);
        }
        return loaded;
    };
    hookedLoad._scrapelyRawCdpHook = true;
    Module._load = hookedLoad;
}

/**
 * Patch copies that loaded before this module (e.g. a local run that imports
 * crawlee or playwright before scrapely). Copies loaded later are patched by
 * the loader hooks.
 *
 * @internal Exported for tests.
 */
export function patchLoadedModules(): void {
    const cache: Record<string, { exports?: unknown; loaded?: boolean } | undefined> = require.cache ?? {};
    for (const [filename, cached] of Object.entries(cache)) {
        const mod: any = cached?.exports;
        if (!mod || !cached?.loaded) continue;
        try {
            if (CRAWLER_FILENAME_RE.test(filename)) patchCrawleeCrawlerModule(mod);
            if (BROWSER_POOL_FILENAME_RE.test(filename)) patchBrowserPoolModule(mod);
            if (LAUNCHER_FILENAME_RE.test(filename)) patchBrowserLauncherModule(mod, filename);
            if (PLAYWRIGHT_ENTRY_FILENAME_RE.test(filename)) patchRawPlaywrightModule(mod);
            const puppeteerEntry = PUPPETEER_ENTRY_FILENAME_RE.exec(filename);
            // Existing instance first: once its class is patched, the instance
            // reads as patched but still holds the original bound methods.
            if (puppeteerEntry) patchRawPuppeteerModule(mod, puppeteerEntry[1]);
        } catch (err) {
            console.warn(`[scrapely] CDP patch failed for '${filename}':`, (err as Error)?.message);
        }
    }
    for (const [filename, cached] of Object.entries(cache)) {
        if (cached?.loaded && /[/\\]lib[/\\]cjs[/\\]puppeteer[/\\]node[/\\]PuppeteerNode\.js$/.test(filename)) {
            patchPuppeteerNodeClass((cached.exports as any)?.PuppeteerNode);
        }
    }
}

// ─── Pure-ESM puppeteer ─────────────────────────────────────────────────────
//
// `import puppeteer from 'puppeteer'` loads puppeteer's ESM build, which never
// passes through Module._load. A module load hook appends one call to
// PuppeteerNode.js that hands the class to patchPuppeteerNodeClass (through a
// global, so it also works from Node 20's off-thread loader). The hook is only
// registered when puppeteer is installed where ESM code can import it (ESM
// resolution ignores NODE_PATH), so other actors pay nothing.

/** @internal Global the ESM load hook's appended code calls. */
export const PUPPETEER_NODE_PATCH_GLOBAL = 'scrapely.patchPuppeteerNodeClass';

/** puppeteer-core's ESM PuppeteerNode.js (URL): lib/esm/puppeteer/... up to v24, lib/puppeteer/... (ESM only) from v25. */
export const PUPPETEER_NODE_ESM_URL_RE = /\/puppeteer-core\/lib\/(esm\/)?puppeteer\/node\/PuppeteerNode\.js$/;

/** Appended to the ESM PuppeteerNode.js source; runs right after the class is defined. */
export const PUPPETEER_NODE_ESM_SUFFIX =
    `\n;globalThis[Symbol.for(${JSON.stringify(PUPPETEER_NODE_PATCH_GLOBAL)})]?.(PuppeteerNode);\n`;

/** Directories ESM code in this process resolves bare imports from: cwd and the main script's ancestors. */
function esmLookupDirs(): string[] {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const path = require('path');
    const starts = [process.cwd()];
    if (process.argv[1]) starts.push(path.dirname(path.resolve(process.argv[1])));
    const dirs = new Set<string>();
    for (const start of starts) {
        let dir = start;
        for (;;) {
            dirs.add(dir);
            const parent = path.dirname(dir);
            if (parent === dir) break;
            dir = parent;
        }
    }
    return [...dirs];
}

/** @internal Whether an ESM import of puppeteer / puppeteer-core could succeed in this process. */
export function esmPuppeteerInstalled(): boolean {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const fs = require('fs');
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const path = require('path');
    for (const dir of esmLookupDirs()) {
        for (const name of ['puppeteer-core', 'puppeteer']) {
            if (fs.existsSync(path.join(dir, 'node_modules', name, 'package.json'))) return true;
        }
    }
    return false;
}

function installEsmPuppeteerHook(): void {
    (globalThis as any)[Symbol.for(PUPPETEER_NODE_PATCH_GLOBAL)] = patchPuppeteerNodeClass;
    if (process.env.SCRAPELY_ESM_PUPPETEER_HOOK === '0' || !esmPuppeteerInstalled()) return;
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const Module: any = require('module');
    if (Module._scrapelyEsmPuppeteerHook) return;
    Module._scrapelyEsmPuppeteerHook = true;
    try {
        // Node 22's synchronous hooks break require(esm) of CommonJS graphs that
        // ESM imports (crawlee -> linkedom: "request for 'boolbase' is not in cache");
        // Node 22 uses the off-thread loader instead.
        const nodeMajor = Number(process.versions.node.split('.')[0]);
        if (typeof Module.registerHooks === 'function' && nodeMajor >= 24) {
            // Node 24+: synchronous, in-thread.
            Module.registerHooks({
                load(url: string, context: unknown, nextLoad: (url: string, context: unknown) => any) {
                    const result = nextLoad(url, context);
                    if (result?.format === 'module' && result.source != null && PUPPETEER_NODE_ESM_URL_RE.test(url)) {
                        const source =
                            typeof result.source === 'string' ? result.source : new TextDecoder().decode(result.source);
                        return { ...result, source: source + PUPPETEER_NODE_ESM_SUFFIX };
                    }
                    return result;
                },
            });
        } else if (typeof Module.register === 'function') {
            // Node 20.6+ / 22: off-thread loader (esm-loader-hooks.mjs).
            // eslint-disable-next-line @typescript-eslint/no-var-requires
            const { pathToFileURL } = require('url');
            Module.register('./esm-loader-hooks.mjs', pathToFileURL(__filename));
        }
    } catch (err) {
        console.warn('[scrapely] ESM puppeteer hook unavailable:', (err as Error)?.message);
    }
}

function applyRawBrowserPatches(): void {
    installRawModuleLoadHook();
    installEsmPuppeteerHook();
    patchLoadedModules();
}

// Package-manager processes (npm/npx/yarn/pnpm) only spawn the actor; they
// never run actor code, so they get no hooks. `npm start` whose script is a
// plain `node <file>` runs that node process directly (see node-wrapper.ts).
if (!handlePackageManagerProcess()) {
    applyRawBrowserPatches();
}
