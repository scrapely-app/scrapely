/**
 * Options of the Scrapely remote browser (Evomi websocket parameters).
 * Passed to browser crawlers as `browserConfiguration`, next to
 * `proxyConfiguration`, or to a raw `chromium.launch({ browserConfiguration })`.
 */
export interface BrowserConfigurationOptions {
    /** Exit country, ISO 3166-1 alpha-2 (e.g. `US`). Sent as `proxy_country`. */
    countryCode?: string;
    /** Region name, not an ISO code (e.g. `California`). Sent as `proxy_state`. */
    state?: string;
    /** City name (e.g. `San Jose`). Sent as `proxy_city`. */
    city?: string;
    /** Fingerprint operating system. Sent as `os`. */
    os?: 'windows' | 'macos' | 'android';
    /** IANA timezone (e.g. `Asia/Tokyo`). Defaults to the exit country's primary zone. */
    timezone?: string;
    /** Comma-separated language tags without q-values (e.g. `de-CH,de`). */
    language?: string;
    /** Built-in ad/tracker blocking. Browser default: off. */
    adblock?: boolean;
    /** Automatic Cloudflare Turnstile solving. Browser default: on. */
    solveCloudflare?: boolean;
    /** Actor-input alias for `countryCode`. */
    browserCountry?: string;
    /** Actor-input alias for `state`. */
    browserState?: string;
    /** Actor-input alias for `city`. */
    browserCity?: string;
}

const BROWSER_OS_VALUES = ['windows', 'macos', 'android'] as const;
const MAX_TIMEZONE_LENGTH = 64;
const MAX_LANGUAGE_LENGTH = 128;

type NormalizedOptions = Omit<BrowserConfigurationOptions, 'browserCountry' | 'browserState' | 'browserCity'>;

function fail(field: string, message: string): never {
    throw new Error(`BrowserConfiguration: "${field}" ${message}`);
}

function requireString(field: string, value: unknown): string {
    if (typeof value !== 'string') fail(field, `must be a string (got ${typeof value}).`);
    const trimmed = value.trim();
    if (!trimmed) fail(field, 'must not be empty.');
    return trimmed;
}

/** Evomi location format: lowercase, dots for spaces (`New York` → `new.york`). */
function normalizeLocationName(field: string, value: unknown): string {
    return requireString(field, value).toLowerCase().replace(/\s+/g, '.');
}

function requireBoolean(field: string, value: unknown): boolean {
    if (typeof value !== 'boolean') fail(field, `must be a boolean (got ${typeof value}).`);
    return value;
}

function normalizeOptions(input: BrowserConfigurationOptions): NormalizedOptions {
    const out: NormalizedOptions = {};

    const countryCode = input.countryCode ?? input.browserCountry;
    if (countryCode != null) {
        const code = requireString('countryCode', countryCode).toUpperCase();
        if (!/^[A-Z]{2}$/.test(code)) {
            fail('countryCode', `must be a two-letter ISO 3166-1 country code (got "${countryCode}").`);
        }
        out.countryCode = code;
    }

    const state = input.state ?? input.browserState;
    if (state != null) out.state = normalizeLocationName('state', state);

    const city = input.city ?? input.browserCity;
    if (city != null) out.city = normalizeLocationName('city', city);

    if (input.os != null) {
        const os = requireString('os', input.os).toLowerCase();
        if (!(BROWSER_OS_VALUES as readonly string[]).includes(os)) {
            fail('os', `must be one of ${BROWSER_OS_VALUES.join(', ')} (got "${input.os}").`);
        }
        out.os = os as NormalizedOptions['os'];
    }

    if (input.timezone != null) {
        const timezone = requireString('timezone', input.timezone);
        if (timezone.length > MAX_TIMEZONE_LENGTH) {
            fail('timezone', `must be at most ${MAX_TIMEZONE_LENGTH} characters.`);
        }
        out.timezone = timezone;
    }

    if (input.language != null) {
        const language = requireString('language', input.language);
        if (language.length > MAX_LANGUAGE_LENGTH) {
            fail('language', `must be at most ${MAX_LANGUAGE_LENGTH} characters.`);
        }
        if (/;\s*q=/i.test(language)) {
            fail('language', 'must be a plain tag list without q-values (e.g. "de-CH,de").');
        }
        out.language = language;
    }

    if (input.adblock != null) out.adblock = requireBoolean('adblock', input.adblock);
    if (input.solveCloudflare != null) out.solveCloudflare = requireBoolean('solveCloudflare', input.solveCloudflare);

    return out;
}

/**
 * Scrapely remote browser configuration: location and fingerprint options
 * applied to the browser connection (the remote browser brings its own
 * proxy — custom proxies are not supported for browsers).
 *
 * ```javascript
 * const browserConfiguration = await Actor.createBrowserConfiguration({ countryCode: 'US', state: 'California' });
 * const crawler = new PlaywrightCrawler({ browserConfiguration, requestHandler });
 * ```
 *
 * Outside the Scrapely remote browser (local runs, unpatched builds) the
 * option is accepted and ignored, so the same code runs everywhere.
 */
export class BrowserConfiguration {
    readonly options: Readonly<NormalizedOptions>;

    constructor(options: BrowserConfigurationOptions = {}) {
        this.options = Object.freeze(normalizeOptions(options ?? {}));
    }

    /** Remote browser websocket parameters set by this configuration. */
    toCdpParams(): Record<string, string> {
        const { countryCode, state, city, os, timezone, language, adblock, solveCloudflare } = this.options;
        const params: Record<string, string> = {};
        if (countryCode) params.proxy_country = countryCode;
        if (state) params.proxy_state = state;
        if (city) params.proxy_city = city;
        if (os) params.os = os;
        if (timezone) params.timezone = timezone;
        if (language) params.language = language;
        if (adblock !== undefined) params.adblock = String(adblock);
        if (solveCloudflare !== undefined) params.solve_cloudflare = String(solveCloudflare);
        return params;
    }
}

/**
 * Create a {@link BrowserConfiguration} (async for parity with
 * `createProxyConfiguration`). Actor input fields `browserCountry` /
 * `browserState` / `browserCity` can be spread straight in.
 */
export async function createBrowserConfiguration(options: BrowserConfigurationOptions = {}): Promise<BrowserConfiguration> {
    return new BrowserConfiguration(options);
}

/** @internal Whether a value can be applied as a browser configuration. */
export function isBrowserConfiguration(value: unknown): value is Pick<BrowserConfiguration, 'toCdpParams'> {
    return !!value && typeof (value as any).toCdpParams === 'function';
}
