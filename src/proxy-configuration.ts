import type {
    ProxyConfigurationFunction,
    ProxyConfigurationOptions as CoreProxyConfigurationOptions,
    ProxyInfo as CoreProxyInfo,
} from '@crawlee/core';
import { ProxyConfiguration } from './scrapely-proxy-configuration.js';

/**
 * Proxy info enriched with the platform-proxy username fields
 * (extends Crawlee's `ProxyInfo` type).
 */
export interface ProxyInfo extends CoreProxyInfo {
    /** Proxy groups used by the platform proxy (e.g. `['RESIDENTIAL']`). */
    groups?: string[];
    /** Two-letter ISO country code requested from the platform proxy. */
    countryCode?: string;
    /** ISO 3166-2 subdivision code requested from the platform proxy. */
    subdivisionCode?: string;
}

/**
 * Options for creating a proxy configuration.
 *
 * Modes supported:
 *  - Platform proxy (default on Scrapely runs): when the server injects
 *    APIFY_PROXY_* or SCRAPELY_PROXY_* credentials, groups/countryCode/password
 *    build gateway usernames automatically.
 *  - Bring-your-own-proxy (BYOP): custom proxyUrls / newUrlFunction /
 *    tieredProxyUrls always win over the platform proxy.
 *  - Mixed: the 'auto' (or 'http://auto') sentinel inside proxyUrls /
 *    tieredProxyUrls / tieredProxyConfig[].proxyUrls reserves a slot for the
 *    platform proxy alongside BYOP entries.
 */
export interface ProxyConfigurationOptions extends CoreProxyConfigurationOptions {
    /**
     * Multiple different ProxyConfigurationOptions stratified into tiers.
     * Tiers with `proxyUrls` map directly to tieredProxyUrls; tiers without
     * URLs expand their platform options (groups/countryCode/password) to a
     * concrete platform proxy URL.
     */
    tieredProxyConfig?: Omit<ProxyConfigurationOptions, 'tieredProxyConfig' | 'tieredProxyUrls'>[];

    /**
     * Platform proxy groups. Effective in platform proxy
     * mode; ignored for BYOP configurations.
     */
    groups?: string[];

    /**
     * Platform proxy country code. Effective in platform
     * proxy mode; ignored for BYOP configurations.
     */
    countryCode?: string;

    /**
     * Platform proxy subdivision code, e.g. US state.
     * Requires `countryCode`. Effective in platform proxy mode; ignored for
     * BYOP configurations. Emitted as a merged `country-XX_YY` username
     * fragment (ISO 3166-2).
     */
    subdivisionCode?: string;

    /**
     * Input-schema alias for groups. Normalized to groups.
     */
    scrapelyProxyGroups?: string[];

    /**
     * Input-schema alias for subdivisionCode. Normalized to subdivisionCode.
     */
    scrapelyProxySubdivision?: string;

    /**
     * Input-schema alias for countryCode. Normalized to countryCode.
     */
    scrapelyProxyCountry?: string;

    // Legacy input-schema aliases, kept for existing inputs and ported actors;
    // used only when the matching scrapelyProxy* option is not set.
    apifyProxyGroups?: string[];
    apifyProxySubdivision?: string;
    apifyProxyCountry?: string;

    /**
     * Platform proxy password. Defaults to the password injected by the
     * server (APIFY_PROXY_PASSWORD / SCRAPELY_PROXY_PASSWORD).
     */
    password?: string;
}

/**
 * Options for Actor.createProxyConfiguration().
 */
export interface CreateProxyConfigurationOptions extends ProxyConfigurationOptions {
    /**
     * The proxy input field's switch (input schema `editor: "proxy"`). When
     * `false` and no custom proxies are given, this method returns `undefined`.
     */
    useScrapelyProxy?: boolean;

    // Legacy key of the same switch, kept for existing inputs and ported actors;
    // used only when useScrapelyProxy is not a boolean.
    useApifyProxy?: boolean;

    /**
     * Whether to verify the proxy configuration by testing a connection.
     * @default true
     */
    checkAccess?: boolean;
}

/**
 * Platform proxy connection details injected into run containers by the
 * Scrapely server (backed by Evomi). SCRAPELY_* vars take precedence over the
 * APIFY_* aliases so Scrapely-first deployments stay unambiguous.
 */
export interface PlatformProxyEnv {
    hostname: string;
    port: number;
    password: string;
    /**
     * Subaccount username injected by the server. When present it is prefixed
     * to every generated gateway username (groups/country/session fragments
     * are appended after it) so the gateway attributes traffic to the right
     * Scrapely tenant.
     */
    username?: string;
    statusUrl?: string;
}

/**
 * Read platform proxy credentials from the environment.
 * Returns null when the run has no platform proxy (BYOP / no-proxy mode).
 */
export function getPlatformProxyEnv(): PlatformProxyEnv | null {
    const hostname = process.env.SCRAPELY_PROXY_HOSTNAME || process.env.APIFY_PROXY_HOSTNAME;
    const portStr = process.env.SCRAPELY_PROXY_PORT || process.env.APIFY_PROXY_PORT;
    const password = process.env.SCRAPELY_PROXY_PASSWORD || process.env.APIFY_PROXY_PASSWORD;
    if (!hostname || !password) return null;

    const parsedPort = parseInt(portStr || '', 10);
    const username = process.env.SCRAPELY_PROXY_USERNAME || process.env.APIFY_PROXY_USERNAME;
    return {
        hostname,
        // Same bounds as the Python SDK (_platform_env): invalid/out-of-range
        // ports fall back to the default gateway port instead of producing a
        // broken URL.
        port: Number.isFinite(parsedPort) && parsedPort > 0 && parsedPort < 65536 ? parsedPort : 8000,
        password,
        username: username || undefined,
        statusUrl: process.env.SCRAPELY_PROXY_STATUS_URL || process.env.APIFY_PROXY_STATUS_URL || undefined,
    };
}

const BYOP_WARNING =
    '[Scrapely] No proxy configured. Scrapely injects platform proxy credentials (APIFY_PROXY_*/SCRAPELY_PROXY_*) into runs when your account has proxy access. ' +
    'Locally, either add proxyUrls to your actor input, or copy your proxy credentials from the Scrapely dashboard (Proxy page) into the ' +
    'SCRAPELY_PROXY_HOSTNAME, SCRAPELY_PROXY_PORT, SCRAPELY_PROXY_USERNAME and SCRAPELY_PROXY_PASSWORD environment variables. ' +
    'Using the platform proxy from outside Scrapely requires an active plan or a prepaid (PAYG) balance of at least $1; ' +
    'free accounts get run-scoped credentials injected into their on-platform runs only.';

function hasCustomProxyOptions(options: ProxyConfigurationOptions): boolean {
    return !!(
        (options.proxyUrls && options.proxyUrls.length > 0) ||
        typeof options.newUrlFunction === 'function' ||
        (options.tieredProxyUrls && options.tieredProxyUrls.length > 0) ||
        (options.tieredProxyConfig && options.tieredProxyConfig.length > 0)
    );
}

function normalizeProxyInput(options: CreateProxyConfigurationOptions): CreateProxyConfigurationOptions {
    const normalized = { ...options };

    const groups = normalized.scrapelyProxyGroups ?? normalized.apifyProxyGroups;
    if (groups && !normalized.groups) {
        normalized.groups = groups;
    }
    const countryCode = normalized.scrapelyProxyCountry ?? normalized.apifyProxyCountry;
    if (countryCode && !normalized.countryCode) {
        normalized.countryCode = countryCode;
    }
    const subdivisionCode = normalized.scrapelyProxySubdivision ?? normalized.apifyProxySubdivision;
    if (subdivisionCode && !normalized.subdivisionCode) {
        normalized.subdivisionCode = subdivisionCode;
    }

    return normalized;
}

/**
 * Creates a proxy configuration for use with crawlers.
 *
 * The platform proxy (Evomi-backed) is injected into runs via the SCRAPELY_PROXY
 * and APIFY_PROXY environment variables. Platform proxy options (groups, countryCode, ...)
 * build gateway usernames in platform mode; proxyUrls, newUrlFunction and
 * tieredProxyUrls configure bring-your-own proxies instead.
 */
export async function createProxyConfiguration(
    options: CreateProxyConfigurationOptions = {},
): Promise<ProxyConfiguration | undefined> {
    const normalized = normalizeProxyInput(options);
    const { useScrapelyProxy, useApifyProxy, checkAccess = true, ...proxyOptions } = normalized;

    // `useScrapelyProxy: false` (or the legacy `useApifyProxy: false`; a boolean
    // useScrapelyProxy wins) explicitly disables the PLATFORM proxy
    // only — custom proxies from the same input must still be used. The standard
    // "Custom proxy URLs" input shape is `{ useScrapelyProxy: false, proxyUrls: [...] }`;
    // returning undefined for it would silently drop the user's own proxies and
    // crawl on their real IPs. We only bail when NO custom proxy mode is given —
    // proxyUrls, tieredProxyUrls, tieredProxyConfig and newUrlFunction all count
    // (Python SDK Actor.create_proxy_configuration parity).
    const usePlatformProxy = typeof useScrapelyProxy === 'boolean' ? useScrapelyProxy : useApifyProxy;
    const dontUsePlatformProxy = usePlatformProxy === false;
    const dontUseCustomProxies = !hasCustomProxyOptions(proxyOptions);
    if (dontUsePlatformProxy && dontUseCustomProxies) {
        return undefined;
    }

    if (!hasCustomProxyOptions(proxyOptions)) {
        // Platform proxy mode: credentials were injected into the environment
        // by the Scrapely server (Evomi-backed). groups/countryCode become
        // meaningful gateway username fragments instead of being ignored.
        if (!getPlatformProxyEnv()) {
            console.warn(BYOP_WARNING);
            return undefined;
        }
    }

    const proxyConfiguration = new ProxyConfiguration(proxyOptions);
    await proxyConfiguration.initialize({ checkAccess });

    return proxyConfiguration;
}

export type { ProxyConfigurationFunction } from '@crawlee/core';
