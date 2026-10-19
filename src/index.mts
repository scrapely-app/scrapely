/**
 * ESM entry (`import ... from 'scrapely'`, and the `apify` shim's
 * `export * from 'scrapely'`).
 *
 * Re-exports the CommonJS entry, plus ESM stand-ins for the names that load
 * heavy packages (Crawlee crawlers, scrapely-client): Node reads every
 * detected export of a CommonJS module when ESM imports it, so index.js hides
 * those names from cjs-module-lexer and they are exported here instead. They
 * load on first use (see lazy-exports.ts).
 */
import { createRequire } from 'node:module';

import type * as Sdk from './index.js';

const require = createRequire(import.meta.url);
const sdk = require('./index.js') as typeof Sdk;
const { lazyExport } = require('./lazy-exports.js') as typeof import('./lazy-exports.js');
const { onScrapelyClientLoaded } = require('./client-loader.js') as typeof import('./client-loader.js');

export * from './index.js';

/** `import scrapely from 'scrapely'`: the CommonJS exports object, as before this entry existed. */
export default sdk;

export const PlaywrightCrawler = lazyExport<typeof Sdk.PlaywrightCrawler>('PlaywrightCrawler', 'class');
export const PuppeteerCrawler = lazyExport<typeof Sdk.PuppeteerCrawler>('PuppeteerCrawler', 'class');
export const CheerioCrawler = lazyExport<typeof Sdk.CheerioCrawler>('CheerioCrawler', 'class');
export const HttpCrawler = lazyExport<typeof Sdk.HttpCrawler>('HttpCrawler', 'class');
export const BasicCrawler = lazyExport<typeof Sdk.BasicCrawler>('BasicCrawler', 'class');
export const BrowserCrawler = lazyExport<typeof Sdk.BrowserCrawler>('BrowserCrawler', 'class');
export const JSDOMCrawler = lazyExport<typeof Sdk.JSDOMCrawler>('JSDOMCrawler', 'class');
export const LinkeDOMCrawler = lazyExport<typeof Sdk.LinkeDOMCrawler>('LinkeDOMCrawler', 'class');
export const AdaptivePlaywrightCrawler = lazyExport<typeof Sdk.AdaptivePlaywrightCrawler>('AdaptivePlaywrightCrawler', 'class');
export const RenderingTypePredictor = lazyExport<typeof Sdk.RenderingTypePredictor>('RenderingTypePredictor', 'class');
export const SessionPool = lazyExport<typeof Sdk.SessionPool>('SessionPool', 'class');
export const Session = lazyExport<typeof Sdk.Session>('Session', 'class');
export const AutoscaledPool = lazyExport<typeof Sdk.AutoscaledPool>('AutoscaledPool', 'class');
export const createBasicRouter = lazyExport<typeof Sdk.createBasicRouter>('createBasicRouter', 'class');
export const createHttpRouter = lazyExport<typeof Sdk.createHttpRouter>('createHttpRouter', 'class');
export const createCheerioRouter = lazyExport<typeof Sdk.createCheerioRouter>('createCheerioRouter', 'class');
export const createJSDOMRouter = lazyExport<typeof Sdk.createJSDOMRouter>('createJSDOMRouter', 'class');
export const createLinkeDOMRouter = lazyExport<typeof Sdk.createLinkeDOMRouter>('createLinkeDOMRouter', 'class');
export const createPlaywrightRouter = lazyExport<typeof Sdk.createPlaywrightRouter>('createPlaywrightRouter', 'class');
export const createAdaptivePlaywrightRouter = lazyExport<typeof Sdk.createAdaptivePlaywrightRouter>('createAdaptivePlaywrightRouter', 'class');
export const createPuppeteerRouter = lazyExport<typeof Sdk.createPuppeteerRouter>('createPuppeteerRouter', 'class');
export const ApifyClient = lazyExport<typeof Sdk.ApifyClient>('ApifyClient', 'class');
export const playwrightUtils: typeof Sdk.playwrightUtils = lazyExport('playwrightUtils', 'object');
export const puppeteerUtils: typeof Sdk.puppeteerUtils = lazyExport('puppeteerUtils', 'object');

// The client's AsyncLocalStorage: methods (run, getStore, ...) act on the real instance.
export const pushDataChargingContext: typeof Sdk.pushDataChargingContext = lazyExport('pushDataChargingContext', 'object');

// A symbol cannot be stood in for: a live binding, set as soon as the SDK
// loads scrapely-client (an Actor creating its API client, charging,
// ApifyClient above, ...). Only clients from that module carry the marker,
// so it is always set before any client could be checked for it.
export let USES_PUSH_DATA_INTERCEPTION: typeof Sdk.USES_PUSH_DATA_INTERCEPTION | undefined;
onScrapelyClientLoaded((client) => {
    USES_PUSH_DATA_INTERCEPTION = client.USES_PUSH_DATA_INTERCEPTION;
});
