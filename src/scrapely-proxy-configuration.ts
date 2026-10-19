// SPDX-License-Identifier: Apache-2.0
// Derived from apify-sdk-js (src/proxy_configuration.ts),
// Copyright Apify Technologies s.r.o., licensed under the Apache License 2.0.
// Modified by TeraShift GmbH. See LICENSE-APACHE and NOTICE.
import http from 'node:http';
import tls from 'node:tls';

import {
    ProxyConfiguration as CrawleeProxyConfiguration,
    type ProxyConfigurationOptions as CoreProxyConfigurationOptions,
} from '@crawlee/core';
import { Configuration } from './configuration.js';
import {
    getPlatformProxyEnv,
    type PlatformProxyEnv,
    type ProxyConfigurationOptions,
    type ProxyInfo,
} from './proxy-configuration.js';

type UrlList = (string | null)[];

/** Options accepted by newUrl()/newProxyInfo() (second parameter). */
type TieredProxyOptions = Parameters<CrawleeProxyConfiguration['newUrl']>[1];

/** Max length of a proxy session ID. */
const MAX_SESSION_ID_LENGTH = 50;
/** Allowed session ID characters: 0-9 a-z A-Z . _ ~ */
const SESSION_ID_REGEX = /^[a-zA-Z0-9._~]+$/;

/** Two-letter ISO country code. */
const COUNTRY_CODE_REGEX = /^[A-Z]{2}$/;
/**
 * ISO 3166-2 subdivision codes are 1-3 uppercase alphanumeric characters,
 * e.g. 'CA' (California), 'NSW' (New South Wales), '9' (Wien, AT-9).
 */
const SUBDIVISION_CODE_REGEX = /^[A-Z0-9]{1,3}$/;

/** Status-check timeout / attempts. */
const CHECK_ACCESS_REQUEST_TIMEOUT_MILLIS = 4_000;
const CHECK_ACCESS_MAX_ATTEMPTS = 2;

/**
 * Sentinel values that reserve a slot for the platform proxy inside
 * `proxyUrls` / `tieredProxyUrls` / `tieredProxyConfig[].proxyUrls`,
 * allowing BYOP and platform proxies to be mixed in one configuration
 * (both `'auto'` and `'http://auto'` are accepted).
 */
const PLATFORM_PROXY_SENTINELS = new Set(['auto', 'http://auto']);

/**
 * Internal placeholder that occupies a rotation slot on the Crawlee side and is
 * translated to a real platform proxy URL lazily in `newUrl()`. It must be a
 * valid URL so Crawlee's validation accepts it, and it must never collide with
 * a real upstream (the `.invalid` TLD is reserved by RFC 2606).
 */
const PLATFORM_PROXY_PLACEHOLDER_HOST = 'scrapely-platform.invalid';
const PLATFORM_PROXY_PLACEHOLDER = `http://${PLATFORM_PROXY_PLACEHOLDER_HOST}`;

/**
 * Per-tier placeholder variant so a sentinel inside `tieredProxyConfig` /
 * `tieredProxyUrls` translates with that tier's own groups/countryCode.
 */
function platformPlaceholderForTier(tierIndex: number): string {
    return `http://t${tierIndex}.${PLATFORM_PROXY_PLACEHOLDER_HOST}`;
}

/** True when a resolved URL is an internal platform-proxy placeholder. */
function isPlatformPlaceholder(url: string | null | undefined): boolean {
    return (
        url === PLATFORM_PROXY_PLACEHOLDER || (url != null && url.endsWith(`.${PLATFORM_PROXY_PLACEHOLDER_HOST}`))
    );
}

/** Returns true for platform-proxy sentinel values ('auto', 'http://auto'). */
export function isPlatformProxySentinel(url: string | null | undefined): boolean {
    return url != null && PLATFORM_PROXY_SENTINELS.has(url);
}

/**
 * Validates a proxy session ID: at most 50
 * characters, and only `0-9`, `a-z`, `A-Z`, `.`, `_` and `~`. Throws otherwise.
 */
export function validateProxySessionId(sessionId: string): string {
    if (sessionId.length > MAX_SESSION_ID_LENGTH) {
        throw new Error(`Session ID must not be longer than ${MAX_SESSION_ID_LENGTH} characters.`);
    }
    if (!SESSION_ID_REGEX.test(sessionId)) {
        throw new Error(
            `Session ID "${sessionId}" contains unsupported characters. ` +
                'Only "a-z", "A-Z", "0-9", ".", "_" and "~" are allowed.',
        );
    }
    return sessionId;
}

/** `groups`, else the input-schema alias `scrapelyProxyGroups`, else the legacy `apifyProxyGroups`. */
function platformGroups(options: ProxyConfigurationOptions): string[] | undefined {
    if (options.groups?.length) return options.groups;
    return options.scrapelyProxyGroups ?? options.apifyProxyGroups;
}

/**
 * Resolves and validates the country/subdivision options.
 * Country must be a two-letter ISO code; subdivision an ISO 3166-2 code
 * (1-3 uppercase alphanumeric characters) and requires a country. Returns
 * the normalized values used by the username grammar.
 */
export function resolveCountryOptions(options: ProxyConfigurationOptions): {
    countryCode?: string;
    subdivisionCode?: string;
} {
    const countryCode = options.countryCode ?? options.scrapelyProxyCountry ?? options.apifyProxyCountry;
    const subdivisionCode = options.subdivisionCode ?? options.scrapelyProxySubdivision ?? options.apifyProxySubdivision;

    if (countryCode != null && !COUNTRY_CODE_REGEX.test(countryCode)) {
        throw new Error(
            `ProxyConfiguration: "countryCode" must be a two-letter ISO country code (got "${countryCode}").`,
        );
    }
    if (subdivisionCode != null) {
        if (!SUBDIVISION_CODE_REGEX.test(subdivisionCode)) {
            throw new Error(
                `ProxyConfiguration: "subdivisionCode" must be a 1-3 character ISO 3166-2 subdivision code ` +
                    `(got "${subdivisionCode}").`,
            );
        }
        if (!countryCode) {
            throw new Error('ProxyConfiguration: "subdivisionCode" requires "countryCode" to be set.');
        }
    }
    return { countryCode, subdivisionCode };
}

/**
 * Strips URL-breaking characters from a username fragment. The gateway grammar
 * is comma-based, so fragments themselves must never contain `,@:/?#` or
 * whitespace (the username is NOT percent-encoded).
 */
function sanitizeUsernameFragment(value: string): string {
    return value.replace(/[:@/?#\s]/g, '');
}

/**
 * Canonical percent-encoding for proxy URL userinfo (username/password).
 * Matches Python's urllib.parse.quote(safe='') exactly: escapes everything
 * except A-Z a-z 0-9 - _ . ~ — including !'()* which encodeURIComponent
 * leaves unencoded. Both encodings are RFC 3986-valid, but SDKs must emit
 * byte-identical URLs.
 */
function encodeUserInfoPart(value: string): string {
    return encodeURIComponent(value).replace(
        /[!'()*]/g,
        (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`,
    );
}

/**
 * Gateway upstream error codes (590-599, proxy-chain semantics). The gateway
 * uses these instead of a bare 502 to describe upstream failures. See docs/evomi-integration.md for the contract.
 */
const PROXY_STATUS_CODE_DESCRIPTIONS: Record<number, string> = {
    590: 'Non Successful: upstream responded with a non-200 status code',
    591: 'reserved for further use',
    592: 'Status Code Out Of Range: upstream responded with status code different than 100-999',
    593: 'Not Found: DNS lookup failed (EAI_NODATA or EAI_NONAME)',
    594: 'Connection Refused: upstream refused connection',
    595: 'Connection Reset: connection reset due to loss of connection or timeout',
    596: 'Broken Pipe: trying to write on a closed socket',
    597: 'Auth Failed: incorrect upstream credentials',
    598: 'reserved for further use',
    599: 'Upstream Error: generic upstream error',
};

/** Status payload returned by the gateway status endpoint. */
export interface ProxyStatus {
    connected: boolean;
    connectionError: string;
    isManInTheMiddle: boolean;
}

/** `fromPayload` is false when the status was derived from an HTTP error code. */
type FetchedProxyStatus = ProxyStatus & { fromPayload: boolean };

/**
 * Performs a GET through an HTTP forward proxy (the platform gateway).
 * - http targets: absolute-form request line + `Proxy-Authorization` header
 * - https targets: CONNECT tunnel + TLS + a raw HTTP/1.1 request
 * Network errors reject; HTTP status codes always resolve.
 */
async function requestThroughProxy(
    targetUrl: string,
    proxyUrl: string,
    timeoutMs: number,
): Promise<{ statusCode: number; body: string }> {
    const target = new URL(targetUrl);
    const proxy = new URL(proxyUrl);
    const credentials = `${decodeURIComponent(proxy.username)}:${decodeURIComponent(proxy.password)}`;
    const proxyAuthorization = `Basic ${Buffer.from(credentials).toString('base64')}`;

    if (target.protocol === 'http:') {
        return await new Promise((resolve, reject) => {
            const req = http.request(
                {
                    host: proxy.hostname,
                    port: Number(proxy.port) || 80,
                    method: 'GET',
                    path: targetUrl,
                    headers: {
                        Host: target.host,
                        'Proxy-Authorization': proxyAuthorization,
                        Accept: 'application/json',
                    },
                    setHost: false,
                },
                (res) => {
                    let body = '';
                    res.setEncoding('utf8');
                    res.on('data', (chunk: string) => {
                        body += chunk;
                    });
                    res.on('end', () => resolve({ statusCode: res.statusCode ?? 0, body }));
                },
            );
            req.setTimeout(timeoutMs, () => req.destroy(new Error(`request timed out after ${timeoutMs}ms`)));
            req.on('error', reject);
            req.end();
        });
    }

    // HTTPS target: CONNECT tunnel through the gateway, then TLS + raw HTTP/1.1.
    return await new Promise((resolve, reject) => {
        const connectReq = http.request({
            host: proxy.hostname,
            port: Number(proxy.port) || 80,
            method: 'CONNECT',
            path: `${target.hostname}:${target.port || 443}`,
            headers: {
                Host: `${target.hostname}:${target.port || 443}`,
                'Proxy-Authorization': proxyAuthorization,
            },
            setHost: false,
        });
        connectReq.setTimeout(timeoutMs, () =>
            connectReq.destroy(new Error(`CONNECT timed out after ${timeoutMs}ms`)),
        );
        connectReq.on('error', reject);
        connectReq.on('connect', (res, socket) => {
            if (res.statusCode !== 200) {
                socket.destroy();
                resolve({ statusCode: res.statusCode ?? 0, body: '' });
                return;
            }
            const tlsSocket = tls.connect({ socket, servername: target.hostname }, () => {
                tlsSocket.write(
                    `GET ${target.pathname}${target.search} HTTP/1.1\r\n` +
                        `Host: ${target.host}\r\n` +
                        'Accept: application/json\r\n' +
                        'Connection: close\r\n\r\n',
                );
            });
            let raw = '';
            tlsSocket.setEncoding('utf8');
            tlsSocket.on('data', (chunk: string) => {
                raw += chunk;
            });
            tlsSocket.on('end', () => {
                const separatorIndex = raw.indexOf('\r\n\r\n');
                const head = separatorIndex >= 0 ? raw.slice(0, separatorIndex) : raw;
                const responseBody = separatorIndex >= 0 ? raw.slice(separatorIndex + 4) : '';
                const statusCode = Number.parseInt(head.split(' ')[1] ?? '', 10);
                resolve({ statusCode: Number.isFinite(statusCode) ? statusCode : 0, body: responseBody });
            });
            tlsSocket.on('error', reject);
        });
        connectReq.end();
    });
}

/**
 * Build a platform proxy URL using the gateway username grammar:
 *   http://GROUPS-G1+G2,country-US,session-a1b2c3:PASSWORD@HOST:PORT
 *
 * `groups` (joined with `+`), `country` and `session` fragments are
 * comma-joined. When the server injected a subaccount username
 * (SCRAPELY_PROXY_USERNAME / APIFY_PROXY_USERNAME) it is prepended so the
 * gateway can attribute traffic to the right Scrapely tenant; without it the
 * username falls back to `auto` when no fragments apply. The password
 * and gateway host/port come from the environment credentials injected by the
 * server.
 */
export function buildPlatformProxyUrl(
    options: ProxyConfigurationOptions,
    env: PlatformProxyEnv,
    sessionId?: string,
): string {
    const groups = platformGroups(options);
    const { countryCode, subdivisionCode } = resolveCountryOptions(options);
    const password = options.password ?? env.password;

    // Username grammar: groups, then session, then country — with the
    // subdivision merged into the country fragment (ISO 3166-2: `country-US_CA`).
    const usernameParts: string[] = [];
    if (groups?.length) {
        usernameParts.push(`groups-${groups.map(sanitizeUsernameFragment).join('+')}`);
    }
    if (sessionId) {
        usernameParts.push(`session-${sanitizeUsernameFragment(validateProxySessionId(sessionId))}`);
    }
    if (subdivisionCode) {
        usernameParts.push(`country-${sanitizeUsernameFragment(countryCode!)}_${sanitizeUsernameFragment(subdivisionCode)}`);
    } else if (countryCode) {
        usernameParts.push(`country-${sanitizeUsernameFragment(countryCode)}`);
    }
    const fragments = usernameParts.join(',');

    const username = env.username
        ? sanitizeUsernameFragment(env.username) + (fragments ? `,${fragments}` : '')
        : fragments || 'auto';

    return `http://${username}:${encodeUserInfoPart(password)}@${env.hostname}:${env.port}`;
}

function urlListHasSentinel(urls: UrlList | undefined): boolean {
    return !!urls?.some((url) => isPlatformProxySentinel(url));
}

function replaceSentinelUrls(urls: UrlList, placeholder: string = PLATFORM_PROXY_PLACEHOLDER): UrlList {
    return urls.map((url) => (isPlatformProxySentinel(url) ? placeholder : url));
}

/** True when any proxy URL list reserves a platform slot via the 'auto' sentinel. */
export function hasPlatformSentinel(options: ProxyConfigurationOptions): boolean {
    return (
        urlListHasSentinel(options.proxyUrls) ||
        urlListHasSentinel(options.tieredProxyUrls?.flat()) ||
        !!options.tieredProxyConfig?.some((tier) => urlListHasSentinel(tier.proxyUrls))
    );
}

function urlListHasRealUrl(urls: UrlList | undefined): boolean {
    return !!urls?.some((url) => url != null && !isPlatformProxySentinel(url));
}

/**
 * True when the configuration contains real (non-sentinel) custom proxies:
 * a BYOP entry in any list or a custom newUrlFunction. Sentinel-only lists do
 * not count — they are platform slots and may legally combine with
 * groups/countryCode.
 */
function hasRealCustomProxyOptions(options: ProxyConfigurationOptions): boolean {
    return (
        urlListHasRealUrl(options.proxyUrls) ||
        typeof options.newUrlFunction === 'function' ||
        !!options.tieredProxyUrls?.some((tier) => urlListHasRealUrl(tier)) ||
        !!options.tieredProxyConfig?.some((tier) => urlListHasRealUrl(tier.proxyUrls))
    );
}

/**
 * Maps declarative tiers to URL lists handed to Crawlee. Tiers with explicit
 * `proxyUrls` pass through (sentinels become per-tier placeholders); tiers
 * without any URLs become per-tier placeholders too, so their platform
 * options (groups/countryCode/password) and the session ID are applied
 * lazily in `newUrl()` while preserving session stickiness.
 */
function mapTieredProxyConfig(
    tieredProxyConfig: Omit<ProxyConfigurationOptions, 'tieredProxyConfig' | 'tieredProxyUrls'>[],
    globalOptions: ProxyConfigurationOptions,
    env: PlatformProxyEnv | null,
    placeholderOptions: Map<string, ProxyConfigurationOptions>,
): UrlList[] {
    return tieredProxyConfig.map((tier, index) => {
        if (tier.newUrlFunction) {
            throw new Error(
                '[Scrapely] tieredProxyConfig tiers with newUrlFunction are not supported. Use tieredProxyUrls instead.',
            );
        }
        if (tier.proxyUrls) {
            if (tier.proxyUrls.some((url) => isPlatformProxySentinel(url))) {
                // Register the tier's own platform options so the placeholder
                // translates with this tier's groups/countryCode, not globals.
                placeholderOptions.set(platformPlaceholderForTier(index), { ...globalOptions, ...tier });
            }
            return replaceSentinelUrls(tier.proxyUrls, platformPlaceholderForTier(index));
        }
        if (!env) {
            // Defensive backstop: the constructor already rejects this case with
            // a more detailed message; keep it in case of direct invocation.
            throw new Error(
                '[Scrapely] tieredProxyConfig tiers without proxyUrls require platform proxy credentials ' +
                    '(SCRAPELY_PROXY_* or APIFY_PROXY_* env), which were not injected into this environment.',
            );
        }
        // URL-less tiers also become per-tier placeholders so newUrl(sessionId)
        // resolves them lazily with the tier's groups/countryCode AND the
        // session ID — building a concrete URL here would lose session
        // stickiness (the gateway would rotate IPs for repeated sessions).
        placeholderOptions.set(platformPlaceholderForTier(index), { ...globalOptions, ...tier });
        return [platformPlaceholderForTier(index)];
    });
}

function toCrawleeOptions(
    options: ProxyConfigurationOptions,
    env: PlatformProxyEnv | null,
    placeholderOptions: Map<string, ProxyConfigurationOptions>,
): CoreProxyConfigurationOptions {
    const crawleeOptions: CoreProxyConfigurationOptions = {};

    if (options.proxyUrls) {
        crawleeOptions.proxyUrls = replaceSentinelUrls(options.proxyUrls);
    }
    if (options.newUrlFunction) {
        crawleeOptions.newUrlFunction = options.newUrlFunction;
    }
    if (options.tieredProxyUrls) {
        crawleeOptions.tieredProxyUrls = options.tieredProxyUrls.map((tier, index) => {
            const hasSentinel = tier.some((url) => isPlatformProxySentinel(url));
            if (hasSentinel) {
                placeholderOptions.set(platformPlaceholderForTier(index), options);
            }
            return replaceSentinelUrls(tier, platformPlaceholderForTier(index));
        });
    } else if (options.tieredProxyConfig) {
        crawleeOptions.tieredProxyUrls = mapTieredProxyConfig(options.tieredProxyConfig, options, env, placeholderOptions);
    }

    return crawleeOptions;
}

function hasApifyOnlyOptions(options: ProxyConfigurationOptions): boolean {
    const groups = platformGroups(options);
    const { countryCode, subdivisionCode } = resolveCountryOptions(options);
    return !!(groups?.length || countryCode || subdivisionCode || options.password);
}

function hasCustomProxyOptions(options: ProxyConfigurationOptions): boolean {
    return !!(
        (options.proxyUrls && options.proxyUrls.length > 0) ||
        typeof options.newUrlFunction === 'function' ||
        (options.tieredProxyUrls && options.tieredProxyUrls.length > 0) ||
        (options.tieredProxyConfig && options.tieredProxyConfig.length > 0)
    );
}

/**
 * True when platform URLs will be generated either way (pure platform mode,
 * a sentinel slot, or a declarative tier that expands to platform URLs).
 */
function needsPlatformProxyUrls(options: ProxyConfigurationOptions): boolean {
    return (
        !hasCustomProxyOptions(options) ||
        hasPlatformSentinel(options) ||
        !!options.tieredProxyConfig?.some((tier) => !tier.proxyUrls)
    );
}

/**
 * Scrapely ProxyConfiguration.
 *
 * Modes:
 *  - Platform proxy: when the server injected APIFY_PROXY_* or SCRAPELY_PROXY_*
 *    credentials (Evomi-backed) and no custom proxies were requested, the
 *    configuration routes through the platform gateway using the gateway
 *    username grammar (groups/country/session fragments).
 *  - BYOP: custom proxyUrls / newUrlFunction / tieredProxyUrls are passed
 *    through to Crawlee unchanged.
 *  - Mixed: the 'auto' (or 'http://auto') sentinel inside proxyUrls /
 *    tieredProxyUrls / tieredProxyConfig[].proxyUrls reserves a slot for the
 *    platform proxy alongside BYOP entries. The sentinel is translated to a
 *    concrete platform URL lazily on every newUrl() call, so Crawlee's
 *    rotation and session-stickiness semantics apply to the mix unchanged.
 */
export class ProxyConfiguration extends CrawleeProxyConfiguration {
    readonly config: Configuration;

    private readonly platformEnv: PlatformProxyEnv | null;

    private readonly platformOptions: ProxyConfigurationOptions;

    /** Platform options to use when translating each per-tier placeholder. */
    private readonly tierPlaceholderOptions: Map<string, ProxyConfigurationOptions>;

    constructor(
        options: ProxyConfigurationOptions = {},
        config = Configuration.getGlobalConfig(),
    ) {
        const { proxyUrls, newUrlFunction } = options;
        const platformEnv = getPlatformProxyEnv();

        // Validate country/subdivision options eagerly (bad
        // values throw at construction, not on the first newUrl() call).
        resolveCountryOptions(options);

        if (hasRealCustomProxyOptions(options) && hasApifyOnlyOptions(options)) {
            throw new Error(
                'Cannot combine custom proxies with Scrapely Proxy! ' +
                    'It is not allowed to set "options.proxyUrls" or "options.newUrlFunction" combined with ' +
                    '"options.groups" or "options.scrapelyProxyGroups" and "options.countryCode" or ' +
                    '"options.scrapelyProxyCountry", "options.subdivisionCode" or "options.scrapelyProxySubdivision".',
            );
        }

        if (needsPlatformProxyUrls(options) && !platformEnv) {
            throw new Error(
                '[Scrapely] The platform proxy was requested (via the "auto" sentinel, tieredProxyConfig tiers without ' +
                    'proxyUrls, or by relying on the default platform configuration), but no platform proxy credentials ' +
                    '(SCRAPELY_PROXY_* or APIFY_PROXY_*) were injected into this environment. ' +
                    'The platform proxy is unavailable in local runs — remove the platform proxy entries to run locally, ' +
                    'or run the actor on Scrapely with proxy access enabled.',
            );
        }

        // Pure platform proxy mode activates only when credentials were
        // injected AND the user did not bring any proxy list/function at all.
        const purePlatformMode = !!platformEnv && !hasCustomProxyOptions(options);
        const tierPlaceholderOptions = new Map<string, ProxyConfigurationOptions>();

        super({
            ...toCrawleeOptions(options, platformEnv, tierPlaceholderOptions),
            ...(purePlatformMode && platformEnv
                ? {
                      newUrlFunction: (sessionId?: string | number) =>
                          buildPlatformProxyUrl(
                              options,
                              platformEnv,
                              sessionId != null ? validateProxySessionId(String(sessionId)) : undefined,
                          ),
                  }
                : {}),
            ['validateRequired' as string]: false,
        });

        this.config = config;
        this.platformEnv = needsPlatformProxyUrls(options) ? platformEnv : null;
        this.platformOptions = options;
        this.tierPlaceholderOptions = tierPlaceholderOptions;

        if (proxyUrls && newUrlFunction) {
            this._throwCannotCombineCustomMethods();
        }
    }

    /**
     * Returns a new proxy URL. Translates the internal platform-proxy
     * placeholder to a concrete platform URL (applying groups/countryCode and
     * session stickiness) whenever Crawlee's rotation lands on it; BYOP
     * entries pass through unchanged. Session IDs are validated (max 50 chars,
     * only `0-9 a-z A-Z . _ ~`).
     */
    override async newUrl(sessionId?: string | number, options?: TieredProxyOptions): Promise<string | undefined> {
        const sid = sessionId != null ? validateProxySessionId(String(sessionId)) : undefined;
        const url = await super.newUrl(sid, options);
        const platformEnv = this.platformEnv;
        if (url != null && isPlatformPlaceholder(url) && platformEnv) {
            // Per-tier placeholders translate with the tier's own options;
            // the generic one falls back to the top-level configuration.
            const platformOptions = url === PLATFORM_PROXY_PLACEHOLDER
                ? this.platformOptions
                : this.tierPlaceholderOptions.get(url) ?? this.platformOptions;
            return buildPlatformProxyUrl(platformOptions, platformEnv, sid);
        }
        return url;
    }

    /**
     * Returns a new ProxyInfo. When the resolved URL points at the platform
     * gateway, `groups`, `countryCode` and `subdivisionCode` are attached.
     */
    override async newProxyInfo(sessionId?: string | number, options?: TieredProxyOptions): Promise<ProxyInfo | undefined> {
        const sid = sessionId != null ? validateProxySessionId(String(sessionId)) : undefined;
        const info = await super.newProxyInfo(sid, options);
        if (info && this.platformEnv && info.hostname === this.platformEnv.hostname) {
            const groups = platformGroups(this.platformOptions);
            const { countryCode, subdivisionCode } = resolveCountryOptions(this.platformOptions);
            return {
                ...info,
                ...(groups?.length ? { groups } : {}),
                ...(countryCode ? { countryCode } : {}),
                ...(subdivisionCode ? { subdivisionCode } : {}),
            };
        }
        return info;
    }

    /**
     * Fetches the gateway status payload by sending
     * `GET {statusUrl}/?format=json` *through the platform gateway*, so the
     * proxy handshake itself authenticates the run's credentials. Retries
     * connection errors (4s timeout, 2 attempts); returns
     * undefined when the endpoint stays unreachable.
     */
    private async fetchProxyStatus(): Promise<FetchedProxyStatus | undefined> {
        const env = this.platformEnv;
        if (!env?.statusUrl) return undefined;

        const statusUrl = env.statusUrl;
        const url = new URL(statusUrl);
        url.searchParams.set('format', 'json');
        const targetUrl = url.toString();
        const proxyUrl = buildPlatformProxyUrl(this.platformOptions, env);

        for (let attempt = 1; attempt <= CHECK_ACCESS_MAX_ATTEMPTS; attempt++) {
            try {
                const { statusCode, body } = await requestThroughProxy(
                    targetUrl,
                    proxyUrl,
                    CHECK_ACCESS_REQUEST_TIMEOUT_MILLIS,
                );

                if (statusCode === 407) {
                    return {
                        connected: false,
                        connectionError:
                            'Platform proxy authentication failed (HTTP 407). Check the injected ' +
                            'SCRAPELY_PROXY_*/APIFY_PROXY_* credentials.',
                        isManInTheMiddle: false,
                        fromPayload: false,
                    };
                }
                if (statusCode >= 590 && statusCode <= 599) {
                    const description = PROXY_STATUS_CODE_DESCRIPTIONS[statusCode] ?? 'upstream error';
                    return {
                        connected: false,
                        connectionError: `Platform proxy upstream error (HTTP ${statusCode}: ${description}).`,
                        isManInTheMiddle: false,
                        fromPayload: false,
                    };
                }
                if (statusCode >= 200 && statusCode < 300) {
                    try {
                        const parsed = JSON.parse(body) as Partial<ProxyStatus>;
                        return {
                            connected: parsed.connected === true,
                            connectionError: parsed.connectionError ?? '',
                            isManInTheMiddle: parsed.isManInTheMiddle === true,
                            fromPayload: true,
                        };
                    } catch {
                        // Malformed JSON — fall through and retry.
                    }
                }
            } catch {
                // Retry connection errors.
            }
        }
        return undefined;
    }

    /**
     * Verifies the proxy configuration. For platform proxy mode with an
     * injected status URL, performs the access check through the
     * gateway (`GET {statusUrl}/?format=json`). When the gateway answers
     * `connected: false` (e.g. an unknown proxy group), it throws with the
     * gateway's `connectionError` on the platform and warns locally;
     * timeouts and HTTP errors only warn.
     * BYOP mode only verifies that a test URL can be generated.
     */
    async initialize(options: { checkAccess?: boolean } = {}): Promise<boolean> {
        const { checkAccess = true } = options;

        if (!checkAccess) {
            return true;
        }

        // Access check: GET {statusUrl}/?format=json *through the
        // platform gateway* so the proxy handshake itself authenticates the
        // run's credentials.
        if (checkAccess && this.platformEnv?.statusUrl) {
            const status = await this.fetchProxyStatus();
            if (!status) {
                console.warn(
                    '[Scrapely] Platform proxy access check timed out. Watch out for errors with status code 407. ' +
                        "If you see some, it most likely means you don't have access to either all or some of the proxies you're trying to use.",
                );
            } else {
                this.isManInTheMiddle = status.isManInTheMiddle;
                if (!status.connected && status.fromPayload && this.config.get('isAtHome')) {
                    throw new Error(status.connectionError);
                }
                if (!status.connected) {
                    console.warn(`[Scrapely] Platform proxy access check failed: ${status.connectionError}`);
                } else {
                    console.log('[Scrapely] Platform proxy access check succeeded.');
                }
            }
        }

        try {
            const testUrl = await this.newUrl('test.session');
            if (testUrl) {
                console.log('[Scrapely] Proxy configuration initialized. Test proxy URL available.');
            }
            return true;
        } catch (err) {
            console.warn(`[Scrapely] Proxy configuration verification failed: ${(err as Error).message}`);
            return true;
        }
    }
}
