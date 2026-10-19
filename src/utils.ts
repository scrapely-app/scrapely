/**
 * v2-style utility functions for scrapely.
 * These adapt the v2-style call signatures to Crawlee v3 internally.
 */
import type { CheerioAPI } from 'cheerio';
import type { RequestQueue } from 'crawlee';

/**
 * Options for enqueueLinks (v2-style API).
 */
export interface EnqueueLinksOptions {
    /** Cheerio instance ($) - REQUIRED */
    $: CheerioAPI;
    /** RequestQueue instance - REQUIRED */
    requestQueue: RequestQueue;
    /** CSS selector to find links. Default: 'a[href]' */
    selector?: string;
    /** Base URL for resolving relative URLs */
    baseUrl?: string;
    /** Optional userData to merge into the queued requests */
    userData?: Record<string, any>;
    /** Optional transformation function for each URL */
    transformRequestFunction?: (request: { url: string; userData?: Record<string, any> }) => { url: string; userData?: Record<string, any> } | null;
}

/**
 * Enqueues links found on the page using a CSS selector.
 * 
 * This v2-style implementation adapts to Crawlee v3 internally.
 * It extracts URLs from the page using cheerio and adds them to the request queue.
 * 
 * @example
 * ```typescript
 * // Inside a CheerioCrawler requestHandler:
 * await Actor.utils.enqueueLinks({
 *     $,
 *     requestQueue,
 *     selector: 'a[href*="/app/"]',
 *     baseUrl: new URL(request.url).origin,
 * });
 * ```
 */
export async function enqueueLinks(options: EnqueueLinksOptions): Promise<void> {
    const { 
        $, 
        requestQueue, 
        selector = 'a[href]', 
        baseUrl,
        userData = {},
        transformRequestFunction 
    } = options;

    // Collect all matching URLs
    const urls: string[] = [];
    
    $(selector).each((_, element) => {
        const href = $(element).attr('href');
        if (href) {
            // Resolve relative URLs if baseUrl is provided
            let fullUrl: string;
            try {
                fullUrl = baseUrl ? new URL(href, baseUrl).href : href;
                urls.push(fullUrl);
            } catch {
                // Skip invalid URLs
            }
        }
    });

    // Deduplicate URLs
    const uniqueUrls = [...new Set(urls)];

    // Add each URL to the request queue
    for (const url of uniqueUrls) {
        const request: { url: string; userData: Record<string, any> } = {
            url,
            userData: { ...userData },
        };

        // Apply transformation if provided
        const transformed = transformRequestFunction 
            ? transformRequestFunction(request)
            : request;

        if (transformed) {
            await requestQueue.addRequest(transformed);
        }
    }
}

/** Options of the v2-style `utils.requestAsBrowser`; other got options pass through. */
export interface RequestAsBrowserOptions extends Record<string, any> {
    url: string;
    method?: string;
    headers?: Record<string, string>;
    payload?: string | Buffer;
    proxyUrl?: string;
    languageCode?: string;
    countryCode?: string;
    isMobile?: boolean;
    json?: boolean;
    ignoreSslErrors?: boolean;
    useInsecureHttpParser?: boolean;
    abortFunction?: unknown;
}

/**
 * v2-style `utils.requestAsBrowser`: an HTTP request with browser-like headers,
 * here through got-scraping (what v2 itself switched to). Resolves to the got
 * response (`body`, `statusCode`, `headers`, `url`).
 */
export async function requestAsBrowser(options: RequestAsBrowserOptions): Promise<any> {
    const {
        payload, languageCode, countryCode, isMobile, json, ignoreSslErrors,
        abortFunction: _abortFunction, ...gotOptions
    } = options;
    const { gotScraping } = await import('@crawlee/utils');
    const locale = languageCode ? `${languageCode}${countryCode ? `-${countryCode}` : ''}` : undefined;
    return gotScraping({
        ...(payload !== undefined ? { body: payload } : {}),
        ...(json ? { responseType: 'json' } : {}),
        ...(ignoreSslErrors ? { https: { rejectUnauthorized: false } } : {}),
        headerGeneratorOptions: {
            ...(locale ? { locales: [locale] } : {}),
            ...(isMobile ? { devices: ['mobile'] } : {}),
        },
        ...gotOptions,
    } as any);
}

let userAgents: { getHeaders(options?: object): Record<string, string> } | undefined;

/** v2-style `utils.getRandomUserAgent`: a realistic desktop browser User-Agent. */
export function getRandomUserAgent(): string {
    if (!userAgents) {
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        const { HeaderGenerator } = require('header-generator');
        userAgents = new HeaderGenerator({ devices: ['desktop'] });
    }
    const headers = userAgents!.getHeaders();
    return headers['user-agent'] ?? headers['User-Agent'];
}

// Loaded on first use: the SDK entry must not load Crawlee (see lazy-exports.ts).
// eslint-disable-next-line @typescript-eslint/no-var-requires
const crawleeUtils = () => require('@crawlee/utils');

/**
 * Utility namespace providing v2-style helper functions.
 * These adapt to Crawlee v3 APIs internally.
 */
export const utils = {
    enqueueLinks,
    requestAsBrowser,
    getRandomUserAgent,
    sleep: (millis?: number) => crawleeUtils().sleep(millis),
    downloadListOfUrls: (options: any) => crawleeUtils().downloadListOfUrls(options),
    extractUrls: (options: any) => crawleeUtils().extractUrls(options),
    htmlToText: (html: any) => crawleeUtils().htmlToText(html),
    isDocker: (forceReset?: boolean) => crawleeUtils().isDocker(forceReset),
    get log() { return require('@apify/log').default; },
    get puppeteer() { return require('@crawlee/puppeteer').puppeteerUtils; },
    get playwright() { return require('@crawlee/playwright').playwrightUtils; },
};