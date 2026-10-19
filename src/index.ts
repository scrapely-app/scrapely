import './log-env.js';
import type * as CrawleeBasic from '@crawlee/basic';
import type * as CrawleeBrowser from '@crawlee/browser';
import type * as CrawleeCore from '@crawlee/core';
import type * as CrawleeCheerio from '@crawlee/cheerio';
import type * as CrawleeHttp from '@crawlee/http';
import type * as CrawleeJsdom from '@crawlee/jsdom';
import type * as CrawleeLinkedom from '@crawlee/linkedom';
import type * as CrawleePlaywright from '@crawlee/playwright';
import type * as CrawleePuppeteer from '@crawlee/puppeteer';
import type * as ScrapelyClient from 'scrapely-client';
import { defineLazyExports } from './lazy-exports.js';

export { setCdpUrl } from './patch-browser-pool.js';
export {
    Actor,
    ACTOR_PERMISSION_LEVEL,
    type ActorInput,
    type InitOptions,
    type MainOptions,
    type ExitOptions,
    type RebootOptions,
    type AbortOptions,
    type PushDataChargeOptions,
    type MetamorphOptions,
    type RecordOptions,
    type OpenStorageOptions,
    type CallOptions,
    type CallTaskOptions,
    type StartOptions,
    type SetStatusMessageOptions,
    type WebhookOptions,
    type ApifyEnv,
    type UserFunc,
    type Token,
    type Timeout,
    EXIT_CODES,
} from './actor';
export type {
    StorageAlias,
    StorageId,
    StorageName,
    StorageIdentifier,
    StorageIdentifierWithoutAlias,
} from './storage-identifiers.js';
// Client exports (scrapely-client provides ApifyClient as an alias of ScrapelyClient).
// ApifyClient loads scrapely-client on first access (see the lazy exports at the end).
export type { ApifyClientOptions, ActorRun } from 'scrapely-client';
export declare const ApifyClient: typeof ScrapelyClient.ApifyClient;
export type ApifyClient = ScrapelyClient.ApifyClient;
export { Configuration, type ConfigurationOptions } from './configuration.js';
export {
    DEGRADED_STATUS_MESSAGE,
    IpBanned,
    SolveAttemptError,
    SolverAuthError,
    SolverRateLimited,
    VendorUnavailable,
    degradedStatusMessage,
    detectVendor,
    isBlocked,
    reportSolveOutcome,
    solve,
    solverConfigured,
    solverStatus,
    stepAllowed,
    type Clearance,
    type SolveCookie,
    type SolveOptions,
    type SolverStepRequest,
    type SolverVendorStatus,
} from './solver.js';
export { ScrapelyEventManager as PlatformEventManager } from './events/scrapely-event-manager.js';
export {
    ChargingManager,
    ChargeUncertainError,
    ChargeRejectedError,
    DEFAULT_DATASET_ITEM_EVENT,
    APIFY_DEFAULT_DATASET_ITEM_EVENT,
    SCRAPELY_DEFAULT_DATASET_ITEM_EVENT,
    hasDefaultDatasetItemEventInPricing,
    isDefaultDatasetItemEvent,
    resolveDefaultDatasetItemEventName,
    mergeChargeResults,
    pushDataAndCharge,
    pushDataWithCharges,
    validateRowCharges,
    type ChargeOptions,
    type ChargeResult,
    type ActorPricingInfo,
} from './charging.js';

export {
    createChargingClient,
    type PpeAwarePushDataContext,
} from './charging_client.js';
export declare const pushDataChargingContext: typeof ScrapelyClient.pushDataChargingContext;
export declare const USES_PUSH_DATA_INTERCEPTION: typeof ScrapelyClient.USES_PUSH_DATA_INTERCEPTION;

export { BillingTracker, type BillingClient } from './storage/billing-tracker.js';

// Storage exports - API-based storage 
export { ApiStorageClientFactory, type ApiStorageConfig } from './storage/api-storage-client.js';

// Proxy configuration exports
export { ProxyConfiguration, isPlatformProxySentinel, validateProxySessionId } from './scrapely-proxy-configuration.js';
export {
    createProxyConfiguration,
    getPlatformProxyEnv,
    type ProxyConfigurationOptions,
    type CreateProxyConfigurationOptions,
    type PlatformProxyEnv,
    type ProxyInfo,
    type ProxyConfigurationFunction,
} from './proxy-configuration.js';

// Remote browser configuration exports
export {
    BrowserConfiguration,
    createBrowserConfiguration,
    type BrowserConfigurationOptions,
} from './browser-configuration.js';

// Re-export commonly used crawlee types from specific packages (not the umbrella 'crawlee' package)
// This ensures compatibility when actors only have @crawlee/puppeteer or @crawlee/playwright installed
export {
    Dataset,
    KeyValueStore,
    RequestQueue,
    log,
    Log,
    LogLevel,
    Logger,
    LoggerJson,
    LoggerText,
} from '@crawlee/core';
// Cheap core classes the reference SDK re-exports from crawlee. @crawlee/core is
// already loaded by the exports above, so these are plain re-exports.
export {
    Request,
    RequestList,
    Router,
    RequestState,
    EventType,
    Statistics,
    Snapshotter,
    SystemStatus,
    NonRetryableError,
    RetryRequestError,
    MissingRouteError,
    CriticalError,
    useState,
    mergeCookies,
} from '@crawlee/core';
export type {
    LoggerOptions,
    KeyValueStoreIteratorOptions,
    KeyValueStoreOptions,
    DatasetConsumer,
    DatasetContent,
    DatasetDataOptions,
    DatasetIteratorOptions,
    DatasetMapper,
    DatasetOptions,
    DatasetReducer,
    QueueOperationInfo,
    RequestQueueOperationOptions,
    RequestQueueOptions,
} from '@crawlee/core';
export type { KeyConsumer } from './storage/key-value-store-client.js';

// v2-style utils namespace
export { utils, enqueueLinks, type EnqueueLinksOptions } from './utils.js';

// Request list (requestListSources input format; parity with the Python SDK's ScrapelyRequestList)
export { ScrapelyRequestList, type RequestListSource } from './request-list.js';

// Crawler exports, loaded on first access: importing
// the SDK must not load Crawlee's crawlers, jsdom, linkedom or playwright.
// CommonJS gets self-replacing getters (defineLazyExports below), ESM gets the
// stand-ins from index.mts.
export declare const PlaywrightCrawler: typeof CrawleePlaywright.PlaywrightCrawler;
export type PlaywrightCrawler = CrawleePlaywright.PlaywrightCrawler;
export declare const PuppeteerCrawler: typeof CrawleePuppeteer.PuppeteerCrawler;
export type PuppeteerCrawler = CrawleePuppeteer.PuppeteerCrawler;
export declare const CheerioCrawler: typeof CrawleeCheerio.CheerioCrawler;
export type CheerioCrawler = CrawleeCheerio.CheerioCrawler;
export declare const HttpCrawler: typeof CrawleeHttp.HttpCrawler;
export type HttpCrawler<
    Context extends CrawleeHttp.InternalHttpCrawlingContext<any, any, CrawleeHttp.HttpCrawler<Context>>,
> = CrawleeHttp.HttpCrawler<Context>;
export declare const BasicCrawler: typeof CrawleeBasic.BasicCrawler;
export type BasicCrawler<Context extends CrawleeBasic.CrawlingContext = CrawleeBasic.BasicCrawlingContext> =
    CrawleeBasic.BasicCrawler<Context>;
export declare const BrowserCrawler: typeof CrawleeBrowser.BrowserCrawler;
export type BrowserCrawler<
    InternalBrowserPoolOptions extends import('@crawlee/browser-pool').BrowserPoolOptions = import('@crawlee/browser-pool').BrowserPoolOptions,
    LaunchOptions extends Record<string, any> | undefined = Record<string, any>,
    Context extends CrawleeBrowser.BrowserCrawlingContext = CrawleeBrowser.BrowserCrawlingContext,
    GoToOptions extends Record<string, any> = Record<string, any>,
> = CrawleeBrowser.BrowserCrawler<InternalBrowserPoolOptions, LaunchOptions, Context, GoToOptions>;
export declare const JSDOMCrawler: typeof CrawleeJsdom.JSDOMCrawler;
export type JSDOMCrawler = CrawleeJsdom.JSDOMCrawler;
export declare const LinkeDOMCrawler: typeof CrawleeLinkedom.LinkeDOMCrawler;
export type LinkeDOMCrawler = CrawleeLinkedom.LinkeDOMCrawler;

// Pools, sessions, adaptive crawler and router factories, also loaded on first access.
export declare const SessionPool: typeof CrawleeCore.SessionPool;
export type SessionPool = CrawleeCore.SessionPool;
export declare const Session: typeof CrawleeCore.Session;
export type Session = CrawleeCore.Session;
export declare const AutoscaledPool: typeof CrawleeCore.AutoscaledPool;
export type AutoscaledPool = CrawleeCore.AutoscaledPool;
export declare const AdaptivePlaywrightCrawler: typeof CrawleePlaywright.AdaptivePlaywrightCrawler;
export type AdaptivePlaywrightCrawler = CrawleePlaywright.AdaptivePlaywrightCrawler;
export declare const RenderingTypePredictor: typeof CrawleePlaywright.RenderingTypePredictor;
export type RenderingTypePredictor = CrawleePlaywright.RenderingTypePredictor;
export declare const createBasicRouter: typeof CrawleeBasic.createBasicRouter;
export declare const createHttpRouter: typeof CrawleeHttp.createHttpRouter;
export declare const createCheerioRouter: typeof CrawleeCheerio.createCheerioRouter;
export declare const createJSDOMRouter: typeof CrawleeJsdom.createJSDOMRouter;
export declare const createLinkeDOMRouter: typeof CrawleeLinkedom.createLinkeDOMRouter;
export declare const createPlaywrightRouter: typeof CrawleePlaywright.createPlaywrightRouter;
export declare const createAdaptivePlaywrightRouter: typeof CrawleePlaywright.createAdaptivePlaywrightRouter;
export declare const createPuppeteerRouter: typeof CrawleePuppeteer.createPuppeteerRouter;

// Utility exports for browser automation (also loaded on first access)
export declare const playwrightUtils: typeof CrawleePlaywright.playwrightUtils;
export declare const puppeteerUtils: typeof CrawleePuppeteer.puppeteerUtils;

// Note: crawlee types are available through the real crawlee package which is installed
// as a dependency. The build system creates a symlink from node_modules/crawlee to the
// real crawlee package inside scrapely/node_modules/crawlee.

// Must stay a plain call (not `export ... from`): cjs-module-lexer must not see
// these names, or every ESM import of the SDK would load them (see lazy-exports.ts).
// module.exports, not `exports`: tsx/esbuild replace module.exports in dev and tests.
defineLazyExports(module.exports);
