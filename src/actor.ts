// SPDX-License-Identifier: Apache-2.0
// Derived from apify-sdk-js (src/actor.ts),
// Copyright Apify Technologies s.r.o., licensed under the Apache License 2.0.
// Modified by TeraShift GmbH. See LICENSE-APACHE and NOTICE.
import './log-env.js';
import { EventEmitter } from 'node:events';

/** Actor permission level. */
export enum ACTOR_PERMISSION_LEVEL {
    /** User has no special permissions */
    NONE = 'NONE',
    /** User can run the actor */
    RUN = 'RUN',
    /** User can modify the actor */
    WRITE = 'WRITE',
    /** User has full administrative access */
    ADMIN = 'ADMIN',
}
import type { ApifyClient } from 'scrapely-client';
import type { ActorRun, Webhook } from 'scrapely-client';
import type { ActorCallOptions, ActorStartOptions, RunAbortOptions, TaskCallOptions } from 'scrapely-client';
import { Configuration as CoreConfiguration, type EventTypeName } from '@crawlee/core';
import { Configuration, type ConfigurationOptions } from './configuration.js';
import { withProxyKeyAliases } from './proxy-input-aliases.js';
import type { StorageClient } from '@crawlee/types';
import { setApiStorage, disconnectStorage, getApiStorageFactory } from './storage/patch-storage.js';
import { ApiStorageClientFactory, fetchWithRetries, type ApiStorageConfig } from './storage/api-storage-client.js';
import type { StorageManagerOptions } from '@crawlee/core';
import { connectEvents, disconnectEvents } from './events/events-client.js';
import { ScrapelyEventManager } from './events/scrapely-event-manager.js';
import { EventType } from '@crawlee/core';
import { decryptSecretInputs } from './crypto/secrets.js';
import {
    ChargingManager,
    pushDataAndCharge,
    setActiveChargingManager,
    type ChargeOptions as ChargingChargeOptions,
    type ChargeResult as ChargingChargeResult,
} from './charging.js';
import { pushDataWithCharges } from './charging.js';

/** Options for a multi-event charged push (`Actor.pushData(row, { charges })`). */
export interface PushDataChargeOptions {
    /** Events per row, e.g. `{ result: 1, page: 3 }` (per batch with `chargePer: 'batch'`). */
    charges?: Record<string, number>;
    /** One event per row (same as `pushData(item, eventName)`); not together with `charges`. */
    eventName?: string;
    /**
     * Push to this named dataset (name, or a Dataset from `Actor.openDataset`) instead of the
     * default one, with the same charge-first semantics, e.g.
     * `pushData(rows, { eventName: 'comment', dataset: \`comments-${runId}\` })`. Rows of a named
     * dataset carry no implicit default-dataset-item event and are not gated by the default
     * dataset's spending-limit safety net; charged pushes store only the rows that were charged.
     */
    dataset?: string | Dataset;
    /**
     * 'row' (default): `charges` for every row. 'batch': once for the whole push, e.g.
     * `pushData(pageRows, { charges: { document: 1 }, chargePer: 'batch' })`: all rows are
     * stored if the charge is accepted, none if it is refused.
     */
    chargePer?: 'row' | 'batch';
}
import { createChargingClient, type PpeAwarePushDataContext } from './charging_client.js';
import { loadedScrapelyClient } from './client-loader.js';
import {
    createProxyConfiguration,
    type CreateProxyConfigurationOptions,
    type ProxyConfigurationOptions,
    type ProxyInfo,
    type ProxyConfigurationFunction,
} from './proxy-configuration.js';
import {
    createBrowserConfiguration,
    type BrowserConfiguration,
    type BrowserConfigurationOptions,
} from './browser-configuration.js';
import type { UserPlan } from './storage/ttl-manager.js';
import { utils as utilsNamespace } from './utils.js';
import { degradedStatusMessage, solve as solveChallenge, solverStatus as fetchSolverStatus, type Clearance, type SolveOptions, type SolverVendorStatus } from './solver.js';

// Lazy imports for crawlee to prevent global fetch patching
import type { Dataset, KeyValueStore, RequestQueue } from 'crawlee';

/** "429 Run API rate limit exceeded ..." for an API error, the message otherwise. */
function describeRunApiError(err: unknown): string {
    const status = (err as { statusCode?: unknown }).statusCode;
    const message = (err as Error)?.message ?? String(err);
    return typeof status === 'number' ? `${status} ${message}` : message;
}

/** User function passed to {@link Actor.main}. */
export type UserFunc<T = unknown> = () => T | Promise<T>;
type Dictionary = Record<string, any>;

/** Value types returned by {@link Actor.getInput}. */
export type ActorInput = Dictionary | string | Buffer;

/** Pre-init fallback; after init, {@link Actor.events} returns the EventManager emitter. */
const fallbackEvents = new EventEmitter();
let apifyClientInstance: ApifyClient | undefined;
let currentUserPlan: UserPlan = 'free';
let currentRunId: string | undefined;
let currentUserId: string | undefined;
let cachedInput: Record<string, any> | null | undefined = undefined;

const gracefulShutdownHandlers: {
    aborting?: () => void;
    migrating?: () => void;
} = {};

/**
 * Registered SIGINT/SIGTERM flush handlers. Tracked (module-level, like
 * {@link gracefulShutdownHandlers}) so `_shutdownActor` and the failed-init
 * rollback can remove them — otherwise an init retry would stack duplicates.
 */
const signalExitHandlers: {
    sigint?: () => void;
    sigterm?: () => void;
} = {};

export interface Token {
    token?: string;
}

/** Run timeout in seconds, or 'inherit' (the time remaining in this run). */
export interface Timeout {
    timeout?: TimeoutOption;
}

/**
 * Exit codes for the Actor process.
 * Must be in the range 1-128 to avoid collision with signal exits.
 */
export const EXIT_CODES = {
    SUCCESS: 0,
    ERROR_USER_FUNCTION_THREW: 91,
    ERROR_UNKNOWN: 92,
};

export interface CallOptions extends Omit<ActorCallOptions, 'timeout'>, Token {
    timeout?: TimeoutOption;
}

export interface StartOptions extends Omit<ActorStartOptions, 'waitForFinish' | 'timeout'>, Token {
    timeout?: TimeoutOption;
}

export interface CallTaskOptions extends Omit<TaskCallOptions, 'timeout'>, Token {
    timeout?: TimeoutOption;
}

export interface AbortOptions extends RunAbortOptions, Token {
    statusMessage?: string;
}

function resolveCallTimeout(timeout?: TimeoutOption): number | undefined {
    if (timeout === 'inherit') {
        const remainingMs = Actor.getRemainingTime();
        if (remainingMs == null) {
            console.warn(
                '[Scrapely] Using `inherit` for timeout is only possible when running on the platform and when ' +
                    'the timeout for the Actor run is set (ACTOR_TIMEOUT_AT).',
            );
            return undefined;
        }
        return Math.max(0, Math.ceil(remainingMs / 1000));
    }
    return timeout;
}

/** Billing context for storage operations */
export interface BillingContext {
    runId: string;
    userId: string;
}

export type { ConfigurationOptions } from './configuration.js';

function isApiStorageConfig(storage: unknown): storage is ApiStorageConfig {
    return (
        typeof storage === 'object' &&
        storage !== null &&
        'apiUrl' in storage &&
        'token' in storage &&
        'runId' in storage
    );
}

function isStorageClient(storage: unknown): storage is StorageClient {
    return typeof storage === 'object' && storage !== null && 'datasets' in storage;
}

function applyConfigurationOptions(configuration: Configuration, options?: ConfigurationOptions): void {
    if (!options) return;
    for (const key of Object.keys(options) as (keyof ConfigurationOptions)[]) {
        const value = options[key];
        if (value !== undefined) {
            configuration.set(key, value);
        }
    }
}

/**
 * Options for Actor.init()
 */
export interface InitOptions {
    /** Crawlee configuration overrides (env vars still take precedence). */
    configuration?: ConfigurationOptions;
    /** API storage config or custom {@link StorageClient} for local/testing. */
    storage?: ApiStorageConfig | StorageClient;
    /**
     * Whether to automatically handle platform shutdown signals.
     * When enabled, `Actor.exit()` is called on `aborting` events and `Actor.reboot()` on `migrating` events.
     * @default true
     */
    gracefulShutdown?: boolean;
    /**
     * Delay in milliseconds before calling `Actor.exit()`/`Actor.reboot()` on `aborting`/`migrating` events.
     * @default 0
     */
    gracefulShutdownDelayMillis?: number;
}

/**
 * Options for Actor.exit()
 */
export interface ExitOptions {
    /** Exit with given status message */
    statusMessage?: string;
    /**
     * Amount of time, in seconds, to wait for all event handlers to finish before exiting the process.
     * @default 30
     */
    timeoutSecs?: number;
    /** Exit code, defaults to 0 */
    exitCode?: number;
    /** Call `process.exit()`? Defaults to true */
    exit?: boolean;
}

/**
 * Options for Actor.setValue() / KeyValueStore.setValue()
 */
export interface RecordOptions {
    /** Content type of the value. If specified, value must be a String or Buffer. */
    contentType?: string;
    /** If set to true, the set-record API call will not be retried if it times out. */
    doNotRetryTimeouts?: boolean;
    /** Specifies a custom timeout for the set-record API call, in seconds. */
    timeoutSecs?: number;
}

/**
 * Options for Actor.setStatusMessage()
 */
export interface SetStatusMessageOptions {
    /** If true, the status message will be marked as terminal (final status of the run) */
    isStatusMessageTerminal?: boolean;
    /** Log level for the status message: 'DEBUG', 'INFO', 'WARNING', 'ERROR' */
    level?: 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR';
}

/**
 * Options for Actor.addWebhook()
 */
export interface WebhookOptions {
    /** Array of event types to trigger the webhook */
    eventTypes: string[];
    /** URL to receive the webhook POST request */
    requestUrl: string;
    /** Payload template for the webhook body */
    payloadTemplate?: string;
    /** Idempotency key to prevent duplicate webhooks on actor restart */
    idempotencyKey?: string;
    /** Headers template for the webhook request */
    headersTemplate?: string;
    /** Description of the webhook */
    description?: string;
    /** If true, ignore SSL errors when sending the webhook */
    ignoreSslErrors?: boolean;
    /** If true, the webhook will not retry on failure */
    doNotRetry?: boolean;
    /** If true, the webhook will interpolate strings in the payloadTemplate and headersTemplate */
    shouldInterpolateStrings?: boolean;
    /** If true, indicates the webhook is a platform integration */
    isApifyIntegration?: boolean;
}

/**
 * Options for timeout in call/start methods.
 * Can be a number in seconds, or 'inherit' to use the remaining time of the current run.
 */
export type TimeoutOption = number | 'inherit';

/**
 * Options for Actor.main()
 */
export interface MainOptions extends ExitOptions, InitOptions {}

/**
 * Options for Actor.useState()
 */
export interface UseStateOptions {
    /** Name of the key-value store to use for state persistence */
    keyValueStoreName?: string;
    /** Configuration object (for advanced use) */
    config?: any;
}

/**
 * Options for Actor.openDataset(), openKeyValueStore(), and openRequestQueue().
 */
export interface OpenStorageOptions {
    /**
     * If set to `true`, cloud storage is used even when `CRAWLEE_STORAGE_DIR` / `APIFY_LOCAL_STORAGE_DIR` is set.
     */
    forceCloud?: boolean;
    /** SDK configuration instance (advanced use). */
    config?: Configuration;
}

type StorageIdentifierArg =
    | string
    | null
    | { id: string }
    | { name: string }
    | { alias: string };

/**
 * Options for Actor.metamorph()
 */
export interface MetamorphOptions {
    /** Content type for the input. If not specified, input is expected to be JSON */
    contentType?: string;
    /** Tag or number of the target Actor build to metamorph into */
    build?: string;
}

/**
 * Options for Actor.reboot()
 */
export interface RebootOptions {
    /** @internal */
    customAfterSleepMillis?: number;
}

/** Result of a charge operation. */
export type ChargeResult = ChargingChargeResult;

/**
 * Parsed representation of the environment variables.
 * This object is returned by the {@apilink Actor.getEnv} function.
 */
export interface ApifyEnv {
    /** ID of the Actor (ACTOR_ID) */
    actorId: string | null;
    /** ID of the Actor build used in the run (ACTOR_BUILD_ID) */
    actorBuildId: string | null;
    /** Build number of the Actor build (ACTOR_BUILD_NUMBER) */
    actorBuildNumber: string | null;
    /** Comma-separated list of build tags (ACTOR_BUILD_TAGS) */
    actorBuildTags: string | null;
    /** WebSocket URL for Actor events (ACTOR_EVENTS_WS_URL) */
    actorEventsWsUrl: string | null;
    /** Full name of the actor (owner/actor-name) (ACTOR_FULL_NAME) */
    actorFullName: string | null;
    /** Maximum paid dataset items (ACTOR_MAX_PAID_DATASET_ITEMS) */
    actorMaxPaidDatasetItems: number | null;
    /** Permission level the Actor is run under (ACTOR_PERMISSION_LEVEL) */
    actorPermissionLevel: ACTOR_PERMISSION_LEVEL | null;
    /** ID of the Actor run (ACTOR_RUN_ID) */
    actorRunId: string | null;
    /** ID of the Actor task (ACTOR_TASK_ID) */
    actorTaskId: string | null;
    /** Base URL for the API (APIFY_API_BASE_URL) */
    apiBaseUrl: string | null;
    /** Public base URL of the API (APIFY_API_PUBLIC_BASE_URL) */
    apiPublicBaseUrl: string | null;
    /** Chrome executable path (APIFY_CHROME_EXECUTABLE_PATH) */
    chromeExecutablePath: string | null;
    /** Container port (ACTOR_WEB_SERVER_PORT) */
    containerPort: number | null;
    /** Container URL (ACTOR_WEB_SERVER_URL) */
    containerUrl: string | null;
    /** Dedicated CPUs (ACTOR_DEDICATED_CPUS) */
    dedicatedCpus: string | null;
    /** ID of the default dataset (ACTOR_DEFAULT_DATASET_ID) */
    defaultDatasetId: string | null;
    /** ID of the default key-value store (ACTOR_DEFAULT_KEY_VALUE_STORE_ID) */
    defaultKeyValueStoreId: string | null;
    /** ID of the default request queue (ACTOR_DEFAULT_REQUEST_QUEUE_ID) */
    defaultRequestQueueId: string | null;
    /** Disable outdated warning (APIFY_DISABLE_OUTDATED_WARNING) */
    disableOutdatedWarning: 1 | null;
    /** Fact (APIFY_FACT) */
    fact: string | null;
    /** If "1", run in headless mode (APIFY_HEADLESS) */
    headless: string | null;
    /** Input key (ACTOR_INPUT_KEY) */
    inputKey: string | null;
    /** Input secrets private key file (APIFY_INPUT_SECRETS_PRIVATE_KEY_FILE) */
    inputSecretsPrivateKeyFile: string | null;
    /** Input secrets private key passphrase (APIFY_INPUT_SECRETS_PRIVATE_KEY_PASSPHRASE) */
    inputSecretsPrivateKeyPassphrase: string | null;
    /** If set to "1", running on the platform (APIFY_IS_AT_HOME) */
    isAtHome: string | null;
    /** Local storage directory (CRAWLEE_STORAGE_DIR) */
    localStorageDir: string | null;
    /** Log format (APIFY_LOG_FORMAT) */
    logFormat: string | null;
    /** Log level - DEBUG, INFO, WARNING, ERROR (APIFY_LOG_LEVEL) */
    logLevel: string | null;
    /** Amount of memory allocated for the Actor, in megabytes (ACTOR_MEMORY_MBYTES) */
    memoryMbytes: number | null;
    /** Origin for the Actor run - how it was started (APIFY_META_ORIGIN) */
    metaOrigin: string | null;
    /** Proxy hostname (APIFY_PROXY_HOSTNAME) */
    proxyHostname: string | null;
    /** Proxy password (APIFY_PROXY_PASSWORD) */
    proxyPassword: string | null;
    /** Proxy port (APIFY_PROXY_PORT) */
    proxyPort: string | null;
    /** Proxy status URL (APIFY_PROXY_STATUS_URL) */
    proxyStatusUrl: string | null;
    /** SDK latest version */
    sdkLatestVersion: string | null;
    /** Date when the Actor was started (ACTOR_STARTED_AT) */
    startedAt: Date | null;
    /** System info interval in millis (APIFY_SYSTEM_INFO_INTERVAL_MILLIS) */
    systemInfoIntervalMillis: string | null;
    /** Date when the Actor will time out (ACTOR_TIMEOUT_AT) */
    timeoutAt: Date | null;
    /** Authentication token (APIFY_TOKEN or SCRAPELY_TOKEN) */
    token: string | null;
    /** ID of the user who started the Actor (APIFY_USER_ID) */
    userId: string | null;
    /** If "1", the user is a paying user (APIFY_USER_IS_PAYING) */
    userIsPaying: string | null;
    /** Workflow key (ACTOR_WORKFLOW_KEY) */
    workflowKey: string | null;
    /** Maximum total charge in USD (ACTOR_MAX_TOTAL_CHARGE_USD) - Scrapely extension */
    maxTotalChargeUsd: number | null;
    /** User's plan (SCRAPELY_USER_PLAN) - Scrapely extension */
    userPlan: string | null;
    /** Restart on error flag (ACTOR_RESTART_ON_ERROR) - Scrapely extension */
    restartOnError: string | null;
}

export class Actor {
    /** Configuration for this Actor instance (pass to crawlers as second argument). */
    readonly config: Configuration;

    /** Whether this Actor instance was initialized. Set by {@link Actor.init}. */
    initialized = false;

    /**
     * Tracks that init() has started installing process-global lifecycle state
     * (storage factory with run credentials, events WebSocket, event manager,
     * graceful-shutdown/signal handlers, charging). Deliberately separate from
     * the public {@link initialized} flag: `_shutdownActor` must tear down
     * partial state from a FAILED init (so it stays retryable), while
     * `initialized` — used by `_ensureActorInit` — flips only after init fully
     * succeeds.
     */
    private _lifecycleStarted = false;

    /**
     * Set by {@link Actor.fail} for the rest of the run: a later {@link Actor.exit}
     * keeps the failure's exit code and status message, so a failed run never
     * ends as SUCCEEDED (e.g. `fail({ exit: false })` followed by `exit()`).
     */
    private static _failure: { exitCode: number; statusMessage?: string } | undefined;

    /** Created on first use: scrapely-client is only loaded when something needs the API client. */
    private _apifyClient: ApifyClient | undefined;
    private readonly chargingManager: ChargingManager;

    /**
     * Creates an Actor instance (alternative to the static helpers).
     */
    constructor(options: ConfigurationOptions = {}) {
        this.config =
            Object.keys(options).length === 0 ? Configuration.getGlobalConfig() : new Configuration(options);
        this.chargingManager = new ChargingManager(this.config, () => this.apifyClient);
        Actor._instance ??= this;
    }

    /** Default ApifyClient instance for this Actor (instance accessor). */
    get apifyClient(): ApifyClient {
        this._apifyClient ??= this.newClient();
        return this._apifyClient;
    }

    private _ensureInit(methodName: string): void {
        if (!this.initialized) {
            throw new Error(
                `Actor.${methodName}() requires Actor.init() to be called first. ` +
                    `Hint: Use Actor.main() which calls init() for you.`,
            );
        }
    }

    private static _actorConfig(): Configuration {
        return Actor.getDefaultInstance().config;
    }

    /**
     * Event emitter for Actor events (same instance as the platform EventManager).
     * Supports events: 'persistState', 'migrating', 'aborting', 'cpuInfo'.
     *
     * Usage:
     * ```typescript
     * Actor.events.on('persistState', () => {
     *     // Save state before migration
     * });
     * ```
     */
    static get events(): EventEmitter {
        const actor = Actor._instance;
        if (actor?.initialized) {
            return (actor.config.getEventManager() as unknown as { events: EventEmitter }).events;
        }
        return fallbackEvents;
    }

    /**
     * Throws an error if the Actor was not initialized.
     * Used internally to ensure methods are called after init().
     */
    private static _ensureActorInit(methodName: string): void {
        if (!Actor.getDefaultInstance().initialized) {
            throw new Error(
                `Actor.${methodName}() requires Actor.init() to be called first. ` +
                `Hint: Use Actor.main() which calls init() for you.`
            );
        }
    }

    /**
     * Returns the remaining time in milliseconds until the Actor run times out.
     * Uses the ACTOR_TIMEOUT_AT environment variable set by the platform.
     * 
     * @returns Milliseconds remaining, or undefined if no timeout is set (not running on platform)
     */
    static getRemainingTime(): number | undefined {
        const timeoutAt = process.env.ACTOR_TIMEOUT_AT;
        if (!timeoutAt) return undefined;
        
        const timeoutDate = new Date(timeoutAt);
        const remaining = timeoutDate.getTime() - Date.now();
        return Math.max(0, remaining);
    }

    /**
     * Initializes this Actor instance.
     *
     * @param options - Initialization options
     */
    async init(options: InitOptions = {}): Promise<void> {
        if (this.initialized) return;
        cachedInput = undefined; // a fresh init reads this run's input again
        Actor._failure = undefined; // a new lifecycle starts unfailed
        // NOTE: `initialized` is set at the END of init — a failed platform init
        // must stay retryable instead of leaving a half-initialized Actor.
        Actor.installFatalChargeHandlers();

        const config = this.config;
        (CoreConfiguration as { globalConfig?: Configuration }).globalConfig = Configuration.getGlobalConfig();
        applyConfigurationOptions(config, options.configuration);

        if (config.get('isAtHome')) {
            config.set('purgeOnStart', false);
        }

        // NOTE: CDP URL is set lazily in patch-browser-pool.ts (remote browser via CDP_URL).

        // From here on, init() mutates process-global lifecycle state (storage
        // factory with run credentials, events WebSocket, event manager,
        // graceful-shutdown handlers, charging). `_lifecycleStarted` marks that
        // this state exists even before init finishes, so `_shutdownActor` can
        // tear it down if init fails — the public `initialized` flag must NOT
        // be used for that (it stays false until the whole init succeeds).
        this._lifecycleStarted = true;
        let eventManager: ScrapelyEventManager | undefined;
        const prevApifyClientInstance = apifyClientInstance;
        let apifyClientAssigned = false;

        try {
            if (options.storage) {
                if (isApiStorageConfig(options.storage)) {
                    console.log(`[Scrapely] Initializing API storage for run ${options.storage.runId}`);
                    setApiStorage(options.storage, config);
                } else if (isStorageClient(options.storage)) {
                    config.useStorageClient(options.storage);
                } else {
                    throw new Error('Actor.init() storage option must be ApiStorageConfig or StorageClient');
                }
            } else {
                const apiUrl = config.get('apiBaseUrl');
                const token = config.get('token');
                const runId = config.get('actorRunId');

                if (!apiUrl || !token || !runId) {
                    if (config.get('isAtHome')) {
                        throw new Error(
                            'Missing required configuration for secure storage. ' +
                            'Required: apiBaseUrl (SCRAPELY_API_URL), token (SCRAPELY_TOKEN/APIFY_TOKEN), actorRunId (ACTOR_RUN_ID). ' +
                            'Containers must be started by the scrapely-server, or pass storage in Actor.init().',
                        );
                    }

                    // Local development without platform credentials: keep Crawlee's
                    // default storage client (memory, or `./storage` filesystem), the
                    // same fallback the Python SDK uses (`Smart → FS`). Storage, input
                    // (KeyValueStore INPUT key), charging, and platform events all
                    // degrade to local behavior — no remote run exists to talk to.
                    console.log('[Scrapely] Local run detected — using Crawlee local storage');
                } else {
                    console.log(`[Scrapely] Initializing API storage for run ${runId}`);
                    setApiStorage({ apiUrl, token, runId }, config);
                }
            }

            const runToken =
                (options.storage && isApiStorageConfig(options.storage)
                    ? options.storage.token
                    : undefined) ?? config.get('token');
            const runId =
                (options.storage && isApiStorageConfig(options.storage)
                    ? options.storage.runId
                    : undefined) ?? config.get('actorRunId');

            const eventsWsUrl = config.get('actorEventsWsUrl');
            if (eventsWsUrl && runToken && runId) {
                connectEvents(eventsWsUrl, runToken, runId);
            }

            Configuration.storage.enterWith(config);

            eventManager = new ScrapelyEventManager(config);
            config.useEventManager(eventManager);
            await eventManager.init();

            // Register handlers for aborting and migrating events for automatic graceful shutdown.
            // - aborting: calls Actor.exit() to terminate the run gracefully
            // - migrating: calls Actor.reboot() to speed up migration (the run continues on a new worker)
            if (options.gracefulShutdown !== false) {
                const delay = options.gracefulShutdownDelayMillis ?? 0;

                gracefulShutdownHandlers.aborting = () => {
                    setTimeout(() => {
                        Actor.exit().catch((err) => {
                            console.error(`[Scrapely] Failed to exit gracefully: ${(err as Error).message}`);
                        });
                    }, delay);
                };
                this.on('aborting', gracefulShutdownHandlers.aborting);

                gracefulShutdownHandlers.migrating = () => {
                    setTimeout(() => {
                        Actor.reboot().catch((err) => {
                            console.error(`[Scrapely] Failed to reboot on migration: ${(err as Error).message}`);
                        });
                    }, delay);
                };
                this.on('migrating', gracefulShutdownHandlers.migrating);
            }

            // Same as before the client became lazy: this Actor's client (once
            // created) is the process default returned by newClient().
            if (this._apifyClient) {
                apifyClientInstance = this._apifyClient;
                apifyClientAssigned = true;
            }
            await this.chargingManager.init();
            // Storage clients refuse default-dataset writes past this run's limit.
            setActiveChargingManager(this.chargingManager);

            // Flush pending PPE intents before hard exit so uncertain charges can still settle.
            const gracefulSignalExit = (signal: string) => {
                const doExit = () => {
                    process.exit(0);
                };
                // Bound flush so a hung network cannot block container stop forever.
                const flushTimeout = setTimeout(doExit, 5_000);
                Actor._shutdownActor(5)
                    .catch((err) => {
                        console.warn(
                            `[Scrapely] Shutdown flush on ${signal} failed: ${(err as Error).message}`,
                        );
                    })
                    .finally(() => {
                        clearTimeout(flushTimeout);
                        doExit();
                    });
            };
            signalExitHandlers.sigint = () => gracefulSignalExit('SIGINT');
            signalExitHandlers.sigterm = () => gracefulSignalExit('SIGTERM');
            process.on('SIGINT', signalExitHandlers.sigint);
            process.on('SIGTERM', signalExitHandlers.sigterm);

            this.initialized = true;
        } catch (err) {
            // Rollback of partial init. Without this, a failed init would leave
            // the events WebSocket open (auto-reconnecting with the failed run's
            // token), the API storage factory holding stale run credentials and
            // caches, an open event manager, and registered handlers — so an
            // init retry would overlay another run's credentials and handlers
            // on this same process. NOTE: charging state is intentionally NOT
            // closed here — `charge()` requires a completed init, so a partial
            // init cannot have admitted charges, and closing the manager would
            // also reject charges made after a successful retry.
            try {
                if (signalExitHandlers.sigint) {
                    process.removeListener('SIGINT', signalExitHandlers.sigint);
                    signalExitHandlers.sigint = undefined;
                }
                if (signalExitHandlers.sigterm) {
                    process.removeListener('SIGTERM', signalExitHandlers.sigterm);
                    signalExitHandlers.sigterm = undefined;
                }

                if (gracefulShutdownHandlers.aborting) {
                    this.off('aborting', gracefulShutdownHandlers.aborting);
                    gracefulShutdownHandlers.aborting = undefined;
                }
                if (gracefulShutdownHandlers.migrating) {
                    this.off('migrating', gracefulShutdownHandlers.migrating);
                    gracefulShutdownHandlers.migrating = undefined;
                }

                if (eventManager) {
                    await eventManager.close().catch(() => undefined);
                }
                disconnectEvents();
                disconnectStorage();
                // Also when the client was only created during this init (lazily,
                // e.g. by the pricing lookup): never leave a failed init's client
                // as the process default.
                if (apifyClientAssigned || apifyClientInstance !== prevApifyClientInstance) {
                    apifyClientInstance = prevApifyClientInstance;
                }
            } finally {
                this._lifecycleStarted = false;
            }
            throw err;
        }
    }

    static async init(options: InitOptions = {}): Promise<void> {
        return Actor.getDefaultInstance().init(options);
    }

    /**
     * Runs the main user function that performs the job of the Actor
     * and terminates the process when the user function finishes.
     * 
     * @param userFunc - User function to be executed
     * @param options - Main options (exit, init, gracefulShutdown, etc.)
     */
    static async main<T>(userFunc: UserFunc<T>, options?: MainOptions): Promise<T> {
        if (!userFunc || typeof userFunc !== 'function') {
            throw new Error(
                `First parameter for Actor.main() must be a function (was '${userFunc === null ? 'null' : typeof userFunc}').`,
            );
        }

        await Actor.init(options);
        try {
            const result = await userFunc();
            await Actor._shutdownActor(options?.timeoutSecs ?? 30);
            await new Promise<void>((resolve) => process.stdout.write('', () => resolve()));
            // A run that called fail({ exit: false }) stays failed.
            process.exit(Actor._failure?.exitCode ?? options?.exitCode ?? 0);
            return result;
        } catch (err) {
            const cause = (err as any).cause;
            const causeStr = cause ? ` (cause: ${cause.message ?? String(cause)})` : '';
            console.error(`[Scrapely] Actor failed: ${(err as Error).message}${causeStr}`);
            // Still flush pending charges on failure paths before hard exit.
            try {
                await Actor._shutdownActor(options?.timeoutSecs ?? 30);
            } catch (shutdownErr) {
                console.warn(
                    `[Scrapely] Shutdown after failure failed: ${(shutdownErr as Error).message}`,
                );
            }
            await new Promise<void>((resolve) => process.stderr.write('', () => resolve()));
            process.exit(options?.exitCode ?? 1);
            throw err;
        }
    }

    /**
     * Gracefully exits the Actor run.
     * 
     * @param options - Exit options
     */
    static async exit(messageOrOptions?: string | ExitOptions, options: ExitOptions = {}): Promise<void> {
        const opts: ExitOptions =
            typeof messageOrOptions === 'string'
                ? { ...options, statusMessage: messageOrOptions }
                : { ...messageOrOptions, ...options };

        Actor._ensureActorInit('exit');

        const failure = Actor._failure;
        if (failure) {
            // Actor.fail() already ran: the run stays failed. Keep its exit code and
            // status message; this exit() only finishes the shutdown.
            if (opts.statusMessage) console.log(`[Scrapely] ${opts.statusMessage}`);
            console.log(`[Scrapely] Actor.exit() after Actor.fail(): the run stays failed (exit code ${failure.exitCode}).`);
            return Actor._exitWith({ ...opts, exitCode: failure.exitCode, statusMessage: undefined });
        }
        return Actor._exitWith(opts);
    }

    private static async _exitWith(opts: ExitOptions): Promise<void> {
        const code = opts.exitCode ?? 0;
        const timeoutSecs = opts.timeoutSecs ?? 30;
        const shouldExit = opts.exit ?? true;

        // The exit/fail message becomes the run's terminal status
        // message. Stored before teardown, while the run and process are alive.
        if (opts.statusMessage) await Actor._persistTerminalStatusMessage(opts.statusMessage, code);

        await Actor._shutdownActor(timeoutSecs);

        if (shouldExit) {
            await new Promise<void>((resolve) => process.stdout.write('', () => resolve()));
            process.exit(code);
        }
    }

    static async fail(messageOrOptions?: string | ExitOptions, options: ExitOptions = {}): Promise<void> {
        const opts =
            typeof messageOrOptions === 'string'
                ? { exitCode: 1, statusMessage: messageOrOptions, ...options }
                : { exitCode: 1, ...messageOrOptions, ...options };
        console.error(`[Scrapely] Actor failed: ${opts.statusMessage ?? 'Unknown error'}`);
        await new Promise<void>((resolve) => process.stderr.write('', () => resolve()));
        Actor._ensureActorInit('fail');
        if (Actor._failure) {
            // Failed already: the first failure's code and message stay.
            return Actor._exitWith({ ...opts, exitCode: Actor._failure.exitCode, statusMessage: undefined });
        }
        Actor._failure = { exitCode: opts.exitCode ?? 1, statusMessage: opts.statusMessage };
        await Actor._exitWith({ ...opts, exitCode: opts.exitCode ?? 1 });
    }

    static async getInput<T = ActorInput>(): Promise<T | null> {
        Actor._ensureActorInit('getInput');

        // Check for metamorph input first
        // When an actor is metamorphed, the input is stored under INPUT-METAMORPH-{n}
        const metamorphCount = process.env.ACTOR_METAMORPH_COUNT;
        if (metamorphCount) {
            const count = parseInt(metamorphCount, 10);
            if (count > 0) {
                const { KeyValueStore } = await import('@crawlee/core');
                const store = await KeyValueStore.open();
                const metamorphKey = `INPUT-METAMORPH-${count}`;
                const metamorphInput = await store.getValue<T>(metamorphKey);
                if (metamorphInput !== null && metamorphInput !== undefined) {
                    // Same treatment as the regular input: decrypt secret fields
                    // and let the synchronous Actor.input getter return it.
                    if (typeof metamorphInput === 'object' && !Array.isArray(metamorphInput)) {
                        const decrypted = withProxyKeyAliases(decryptSecretInputs(metamorphInput as Record<string, any>));
                        cachedInput = decrypted;
                        return decrypted as T;
                    }
                    return metamorphInput;
                }
                // Fall through to regular INPUT if metamorph key not found
            }
        }

        let input: T | null = null;

        const raw = process.env.ACTOR_INPUT;
        if (raw) {
            // Try base64 decode first (server passes input as base64)
            try {
                const decoded = Buffer.from(raw, 'base64').toString('utf-8');
                input = JSON.parse(decoded) as T;
            } catch {
                // If base64 decode fails, try direct JSON parse
                input = JSON.parse(raw) as T;
            }
        } else {
            const { KeyValueStore } = await import('@crawlee/core');
            const store = await KeyValueStore.open();
            const inputKey = Actor._actorConfig().get('inputKey', 'INPUT');
            input = await store.getValue<T>(inputKey);
        }

        // Decrypt secret inputs after loading (must happen here, not in init(),
        // because the input needs to be available first)
        if (input && typeof input === 'object' && !Array.isArray(input)) {
            // Proxy objects carry both namings (scrapelyProxy* and apifyProxy*).
            const decrypted = withProxyKeyAliases(decryptSecretInputs(input as Record<string, any>));
            // Large inputs are only in the INPUT record (no ACTOR_INPUT env):
            // let the synchronous Actor.input getter return what was loaded here.
            cachedInput = decrypted;
            return decrypted as T;
        }

        return input;
    }

    static async getInputOrThrow<T = ActorInput>(): Promise<T> {
        const input = await Actor.getInput<T>();
        if (input == null) {
            throw new Error('Input does not exist');
        }
        return input;
    }

    /**
     * Synchronously get the actor input (cached from first access).
     * The platform sets ACTOR_INPUT only for small inputs; for large ones this
     * returns the input once `await Actor.getInput()` has loaded it from the
     * default key-value store.
     */
    static get input(): Record<string, any> | undefined {
        if (cachedInput !== undefined) return cachedInput ?? undefined;
        const raw = process.env.ACTOR_INPUT;
        if (!raw) {
            // Not cached: a later Actor.getInput() fills it from the INPUT record.
            return undefined;
        }
        try {
            // Try base64 decode first (server passes input as base64)
            const decoded = Buffer.from(raw, 'base64').toString('utf-8');
            cachedInput = JSON.parse(decoded);
        } catch {
            try {
                // Fallback to direct JSON parse
                cachedInput = JSON.parse(raw);
            } catch {
                cachedInput = null;
                return undefined;
            }
        }

        // Decrypt secret inputs after loading
        if (cachedInput && typeof cachedInput === 'object' && !Array.isArray(cachedInput)) {
            cachedInput = withProxyKeyAliases(decryptSecretInputs(cachedInput as Record<string, any>));
        }

        return cachedInput ?? undefined;
    }

    /**
     * Stores an object or an array of objects to the default {@apilink Dataset} of the current Actor run.
     * 
     * This is just a convenient shortcut for {@apilink Dataset.pushData}.
     * 
     * **IMPORTANT**: Make sure to use the `await` keyword when calling `pushData()`,
     * otherwise the Actor process might finish before the data are stored!
     * 
     * @param item - Object or array of objects containing data to be stored in the default dataset.
     * The objects must be serializable to JSON and the JSON representation of each object must be smaller than 9MB.
     * @returns Promise that resolves when the data has been stored
     */
    static pushData<Data extends Record<string, any>>(item: Data | Data[]): Promise<void>;
    /**
     * Stores an object or an array of objects to the default {@apilink Dataset} of the current Actor run.
     * 
     * This is just a convenient shortcut for {@apilink Dataset.pushData}.
     * 
     * **IMPORTANT**: Make sure to use the `await` keyword when calling `pushData()`,
     * otherwise the Actor process might finish before the data are stored!
     * 
     * @param item - Object or array of objects containing data to be stored in the default dataset.
     * The objects must be serializable to JSON and the JSON representation of each object must be smaller than 9MB.
     * @param eventName - Event name for pay-per-event pricing. If provided, the method will
     * attempt to charge for each pushed item.
     * @returns ChargeResult indicating if the user's spending limit was reached
     */
    static pushData<Data extends Record<string, any>>(item: Data | Data[], eventName: string): Promise<ChargeResult>;
    /**
     * Stores rows that each cost several pay-per-event events, charged atomically:
     * `pushData(row, { charges: { result: 1, page: 3 } })` charges 1 `result` and 3 `page`
     * events per row. A row is stored only when all of its events were charged; rows the
     * spending limit cannot pay for in full are not stored.
     */
    static pushData<Data extends Record<string, any>>(item: Data | Data[], options: PushDataChargeOptions): Promise<ChargeResult>;
    static async pushData<Data extends Record<string, any>>(item: Data | Data[], eventNameOrOptions?: string | PushDataChargeOptions): Promise<void | ChargeResult> {
        return Actor.getDefaultInstance().pushData(item, eventNameOrOptions as string);
    }

    async pushData<Data extends Record<string, any>>(item: Data | Data[], eventNameOrOptions?: string | PushDataChargeOptions): Promise<void | ChargeResult> {
        Actor._ensureActorInit('pushData');

        if (eventNameOrOptions !== undefined && typeof eventNameOrOptions === 'object') {
            const opts = eventNameOrOptions;
            const { Dataset } = await import('@crawlee/core');
            const dataset = opts.dataset === undefined
                ? await Dataset.open()
                : typeof opts.dataset === 'string' ? await Dataset.open(opts.dataset) : opts.dataset;
            const isDefaultDataset = dataset.id === this.config.get('defaultDatasetId');
            if (opts.charges !== undefined) {
                if (opts.eventName !== undefined) throw new Error('Pass either eventName or charges, not both');
                return pushDataWithCharges({
                    chargingManager: this.chargingManager,
                    items: item,
                    charges: opts.charges,
                    chargePer: opts.chargePer ?? 'row',
                    isDefaultDataset,
                    pushFn: async (rows) => { await dataset.pushData(rows); },
                    validateItems: await crawleeItemValidator(),
                });
            }
            if (opts.chargePer !== undefined && opts.chargePer !== 'row') throw new Error("chargePer needs charges: {...}");
            if (isDefaultDataset) return this.pushData(item, opts.eventName as string);
            if (opts.eventName === 'apify-default-dataset-item' || opts.eventName === 'scrapely-default-dataset-item') {
                throw new Error(`Cannot charge for synthetic event '${opts.eventName}' manually`);
            }
            if (opts.eventName === undefined) {
                // A plain push to a named dataset: not gated by the default dataset's safety net.
                await dataset.pushData(item);
                return;
            }
            return pushDataAndCharge({
                chargingManager: this.chargingManager,
                items: item,
                eventName: opts.eventName,
                isDefaultDataset: false,
                pushFn: async (rows) => { await dataset.pushData(rows); },
                validateItems: await crawleeItemValidator(),
            });
        }
        const eventName = eventNameOrOptions;

        // Exact synthetic names only — prefix would block real priced events.
        if (
            eventName === 'apify-default-dataset-item' ||
            eventName === 'scrapely-default-dataset-item'
        ) {
            throw new Error(`Cannot charge for synthetic event '${eventName}' manually`);
        }

        const { Dataset } = await import('@crawlee/core');
        const dataset = await Dataset.open();

        if (eventName === undefined && !this.usesPushDataInterception(dataset)) {
            // Charge-limit safety net (also for local storage): this push is not
            // charged, so past the run's spending limit it is not stored.
            if (
                this.initialized &&
                dataset.id === this.config.get('defaultDatasetId') &&
                this.chargingManager.isChargeLimitReached()
            ) {
                this.chargingManager._noteRefusedDefaultDatasetItems(Array.isArray(item) ? item.length : 1);
                return;
            }
            await dataset.pushData(item);
            return;
        }

        if (this.usesPushDataInterception(dataset)) {
            return this.pushDataViaInterceptedClient(dataset, item, eventName);
        }

        return this.pushDataWithExplicitCharging(dataset, item, eventName);
    }

    private usesPushDataInterception(dataset: Dataset): boolean {
        // Only a scrapely-client dataset client carries the marker, so when the
        // SDK's scrapely-client was never loaded no client can be intercepting.
        const client = loadedScrapelyClient();
        if (!client) return false;
        return Boolean((dataset.client as unknown as Record<symbol, boolean>)[client.USES_PUSH_DATA_INTERCEPTION]);
    }

    private async pushDataViaInterceptedClient<Data extends Record<string, any>>(
        dataset: Dataset,
        item: Data | Data[],
        eventName: string | undefined,
    ): Promise<ChargeResult> {
        const context: PpeAwarePushDataContext = { eventName };

        await loadedScrapelyClient()!.pushDataChargingContext.run(context, async () => {
            await dataset.pushData(item);
        });

        return (
            context.chargeResult ?? {
                eventChargeLimitReached: false,
                chargedCount: 0,
                chargeableWithinLimit: {},
            }
        );
    }

    private async pushDataWithExplicitCharging<Data extends Record<string, any>>(
        dataset: Dataset,
        items: Data | Data[],
        explicitEventName: string | undefined,
    ): Promise<void | ChargeResult> {
        if (!this.initialized && explicitEventName === undefined) {
            await dataset.pushData(items);
            return;
        }

        const isDefaultDataset = dataset.id === this.config.get('defaultDatasetId');

        return pushDataAndCharge({
            chargingManager: this.chargingManager,
            items,
            eventName: explicitEventName,
            isDefaultDataset,
            pushFn: async (limitedItems) => dataset.pushData(limitedItems),
            validateItems: await crawleeItemValidator(),
        });
    }

    static async getValue<T>(key: string): Promise<T | null> {
        Actor._ensureActorInit('getValue');
        const { KeyValueStore } = await import('@crawlee/core');
        const store = await KeyValueStore.open();
        return store.getValue<T>(key);
    }

    /**
     * Stores or deletes a value in the default KeyValueStore associated with the current Actor run.
     * 
     * This is just a convenient shortcut for KeyValueStore.setValue.
     * 
     * **Example:**
     * ```typescript
     * // Store an object (serialized to JSON)
     * await Actor.setValue('OUTPUT', { foo: 'bar' });
     * 
     * // Delete a record
     * await Actor.setValue('my-key', null);
     * ```
     * 
     * @param key - Unique record key
     * @param value - Record data. If null, the record is deleted. If no contentType is specified,
     * value can be any JSON-serializable object. If contentType is set, value must be String or Buffer.
     * @param options - Record options
     */
    static async setValue<T>(key: string, value: T | null, options?: RecordOptions): Promise<void> {
        Actor._ensureActorInit('setValue');
        const { KeyValueStore } = await import('@crawlee/core');
        const store = await KeyValueStore.open();
        await store.setValue(key, value, options);
    }

    private static _debugCaptureCount = 0;
    /**
     * A payload up to DEBUG_CAPTURE_MAX_BYTES characters is stored as it is; a larger one gzip-compressed
     * (base64, at most DEBUG_CAPTURE_MAX_BYTES characters again), keeping up to DEBUG_CAPTURE_MAX_CHARS
     * characters / DEBUG_CAPTURE_MAX_ENCODED bytes of it. A plain cut at 256 KiB lost the part a check
     * needed when a large API document starts with site navigation.
     */
    static readonly DEBUG_CAPTURE_MAX_BYTES = 256 * 1024;
    static readonly DEBUG_CAPTURE_MAX_CHARS = 8 * 1024 * 1024;
    static readonly DEBUG_CAPTURE_MAX_ENCODED = 8 * 1024 * 1024;
    static readonly DEBUG_CAPTURE_MAX_RECORDS = 500;

    /** The key-value store record name for a capture key (item URL or id). */
    static async debugCaptureRecordKey(key: string): Promise<string> {
        const { createHash } = await import('node:crypto');
        return `DEBUG_CAPTURE_${createHash('sha1').update(String(key).trim(), 'utf8').digest('hex').slice(0, 24)}`;
    }

    /**
     * Save the raw source payload (API JSON or HTML) an item was extracted from, keyed by the
     * item's URL or id, for platform diagnostics. Does nothing unless the platform enabled
     * capture for this run (SCRAPELY_DEBUG_CAPTURE=1 on its own diagnostic runs).
     */
    /** debugCapture for synchronous code: fire and forget. Does nothing unless capture is enabled for the run. */
    static debugCaptureSync(key: string, payload: unknown): void {
        if (process.env.SCRAPELY_DEBUG_CAPTURE !== '1' || !key) return;
        void Actor.debugCapture(key, payload);
    }

    static async debugCapture(key: string, payload: unknown): Promise<void> {
        if (process.env.SCRAPELY_DEBUG_CAPTURE !== '1' || !key) return;
        if (Actor._debugCaptureCount >= Actor.DEBUG_CAPTURE_MAX_RECORDS) return;
        try {
            const text = typeof payload === 'string' ? payload
                : payload instanceof Uint8Array ? Buffer.from(payload.buffer, payload.byteOffset, payload.byteLength).toString('utf8')
                    : JSON.stringify(payload) ?? '';
            const record = await Actor.debugCaptureRecord(String(key), text);
            Actor._debugCaptureCount++;
            await Actor.setValue(await Actor.debugCaptureRecordKey(key), record);
        } catch {
            // never break the run
        }
    }

    /** The record for one capture: plain when small, else gzip-base64 (plain head as the last resort). */
    static async debugCaptureRecord(key: string, text: string): Promise<Record<string, unknown>> {
        const limit = Actor.DEBUG_CAPTURE_MAX_BYTES;
        if (text.length <= limit) return { key, payload: text };
        const { gzipSync } = await import('node:zlib');
        const raw = Buffer.from(text.slice(0, Actor.DEBUG_CAPTURE_MAX_CHARS), 'utf8');
        const kept = raw.subarray(0, Actor.DEBUG_CAPTURE_MAX_ENCODED);
        const packed = gzipSync(kept, { level: 6 }).toString('base64');
        if (packed.length <= limit) {
            const truncated = text.length > Actor.DEBUG_CAPTURE_MAX_CHARS || kept.length < raw.length;
            return { key, payload: packed, encoding: 'gzip-base64', length: text.length, truncated };
        }
        return { key, payload: text.slice(0, limit), length: text.length, truncated: true };
    }

    /**
     * Helper to resolve storage identifier to a string ID or name.
     * Supports: string, { id: '...' }, { name: '...' }, { alias: '...' }
     */
    private static _resolveStorageIdentifier(
        identifier: string | null | undefined | StorageIdentifierArg,
        storageType: 'datasets' | 'keyValueStores' | 'requestQueues',
        actorConfig: Configuration = Actor._actorConfig(),
    ): string | null | undefined {
        if (!identifier) return identifier;
        
        if (typeof identifier === 'string') {
            return identifier;
        }
        
        if ('id' in identifier) {
            return identifier.id;
        }
        
        if ('name' in identifier) {
            return identifier.name;
        }
        
        if ('alias' in identifier) {
            // Try to resolve alias from ACTOR_STORAGES_JSON
            const raw = actorConfig.get('actorStoragesJson');
            if (raw) {
                try {
                    let parsed: any;
                    try {
                        parsed = JSON.parse(raw);
                    } catch {
                        const decoded = Buffer.from(raw, 'base64').toString('utf-8');
                        parsed = JSON.parse(decoded);
                    }
                    const id = parsed?.[storageType]?.[identifier.alias];
                    if (typeof id === 'string' && id) return id;
                } catch {
                    // Fall through to return alias as name
                }
            }
            // Running outside platform: fall back to alias behaving like a name
            return identifier.alias;
        }
        
        return null;
    }

    private static _buildStorageOpenOptions(
        actorConfig: Configuration,
        options?: OpenStorageOptions,
    ): StorageManagerOptions | undefined {
        if (!options) return undefined;

        const config = options.config ?? actorConfig;
        const storageOptions: StorageManagerOptions = { config };

        if (options.forceCloud) {
            const factory = getApiStorageFactory();
            if (factory) {
                storageOptions.storageClient = factory;
            } else {
                const apiUrl = config.get('apiBaseUrl');
                const token = config.get('token');
                const runId = config.get('actorRunId');
                if (apiUrl && token && runId) {
                    storageOptions.storageClient = new ApiStorageClientFactory({ apiUrl, token, runId });
                } else {
                    console.warn(
                        '[Scrapely] forceCloud was requested but no API storage credentials are '
                        + 'available (apiBaseUrl/token/actorRunId) and no storage factory is '
                        + 'initialized — falling back to the default (local) storage client.',
                    );
                }
            }
        }

        return storageOptions;
    }

    private static async _openDataset<T extends Record<string, any>>(
        actorConfig: Configuration,
        nameOrIdOrOptions?: string | null | StorageIdentifierArg,
        openOptions?: OpenStorageOptions,
    ): Promise<Dataset<T>> {
        const { Dataset } = await import('@crawlee/core');
        const resolved = Actor._resolveStorageIdentifier(nameOrIdOrOptions, 'datasets', actorConfig);
        const storageOpts = Actor._buildStorageOpenOptions(actorConfig, openOptions);
        return Dataset.open<T>(resolved as string | undefined, storageOpts);
    }

    private static async _openKeyValueStore(
        actorConfig: Configuration,
        nameOrIdOrOptions?: string | null | StorageIdentifierArg,
        openOptions?: OpenStorageOptions,
    ): Promise<KeyValueStore> {
        const { KeyValueStore } = await import('@crawlee/core');
        const resolved = Actor._resolveStorageIdentifier(nameOrIdOrOptions, 'keyValueStores', actorConfig);
        const storageOpts = Actor._buildStorageOpenOptions(actorConfig, openOptions);
        return KeyValueStore.open(resolved as string | undefined, storageOpts);
    }

    private static async _openRequestQueue(
        actorConfig: Configuration,
        nameOrIdOrOptions?: string | null | StorageIdentifierArg,
        openOptions?: OpenStorageOptions,
    ): Promise<RequestQueue> {
        const { RequestQueue } = await import('@crawlee/core');
        const resolved = Actor._resolveStorageIdentifier(nameOrIdOrOptions, 'requestQueues', actorConfig);
        const storageOpts = Actor._buildStorageOpenOptions(actorConfig, openOptions);
        return RequestQueue.open(resolved as any, storageOpts);
    }

    static async openDataset<T extends Record<string, any>>(
        nameOrIdOrOptions?: string | null | StorageIdentifierArg,
        openOptions?: OpenStorageOptions,
    ): Promise<Dataset<T>> {
        Actor._ensureActorInit('openDataset');
        return Actor._openDataset<T>(Actor._actorConfig(), nameOrIdOrOptions, openOptions);
    }

    static async openKeyValueStore(
        nameOrIdOrOptions?: string | null | StorageIdentifierArg,
        openOptions?: OpenStorageOptions,
    ): Promise<KeyValueStore> {
        Actor._ensureActorInit('openKeyValueStore');
        return Actor._openKeyValueStore(Actor._actorConfig(), nameOrIdOrOptions, openOptions);
    }

    static async openRequestQueue(
        nameOrIdOrOptions?: string | null | StorageIdentifierArg,
        openOptions?: OpenStorageOptions,
    ): Promise<RequestQueue> {
        Actor._ensureActorInit('openRequestQueue');
        return Actor._openRequestQueue(Actor._actorConfig(), nameOrIdOrOptions, openOptions);
    }

    static isAtHome(): boolean {
        return !!Actor._actorConfig().get('isAtHome');
    }

    /**
     * Returns true when the Actor run was started in standby mode
     * (Python SDK `Actor.is_standby` parity; detected via `metaOrigin === 'STANDBY'`).
     */
    static isStandby(): boolean {
        return Actor._actorConfig().get('metaOrigin') === 'STANDBY';
    }

    /**
     * Returns a new {@apilink ApifyEnv} object which contains information parsed from all the environment variables.
     * 
     * For the list of the environment variables, see the Actor documentation.
     * If some variables are not defined or are invalid, the corresponding value in the resulting object will be null.
     * 
     * @returns Parsed environment variables with proper types
     */
    static getEnv(): ApifyEnv {
        const env = process.env;
        
        // Helper to parse dates
        const parseDate = (value: string | undefined): Date | null => {
            if (!value) return null;
            const unix = Date.parse(value);
            return unix > 0 ? new Date(unix) : null;
        };
        
        // Helper to parse integers
        const parseIntOrNull = (value: string | undefined): number | null => {
            if (!value) return null;
            const parsed = parseInt(value, 10);
            return isNaN(parsed) ? null : parsed;
        };
        
        // Helper to parse floats
        const parseFloatOrNull = (value: string | undefined): number | null => {
            if (!value) return null;
            const parsed = parseFloat(value);
            return isNaN(parsed) ? null : parsed;
        };
        
        // Helper for disableOutdatedWarning (1 or null)
        const parseDisableWarning = (value: string | undefined): 1 | null => {
            return value === '1' ? 1 : null;
        };

        return {
            // Fields in alphabetical order
            actorId: env.ACTOR_ID || null,
            actorBuildId: env.ACTOR_BUILD_ID || null,
            actorBuildNumber: env.ACTOR_BUILD_NUMBER || null,
            actorBuildTags: env.ACTOR_BUILD_TAGS || null,
            actorEventsWsUrl: env.ACTOR_EVENTS_WS_URL || env.ACTOR_EVENTS_WEBSOCKET_URL || null,
            actorFullName: env.ACTOR_FULL_NAME || null,
            actorMaxPaidDatasetItems: parseIntOrNull(env.ACTOR_MAX_PAID_DATASET_ITEMS),
            actorPermissionLevel: (env.ACTOR_PERMISSION_LEVEL as ACTOR_PERMISSION_LEVEL) || null,
            actorRunId: env.ACTOR_RUN_ID || null,
            actorTaskId: env.ACTOR_TASK_ID || null,
            apiBaseUrl: env.APIFY_API_BASE_URL || env.SCRAPELY_API_URL || null,
            apiPublicBaseUrl: env.APIFY_API_PUBLIC_BASE_URL || null,
            chromeExecutablePath: env.APIFY_CHROME_EXECUTABLE_PATH || null,
            containerPort: parseIntOrNull(env.ACTOR_WEB_SERVER_PORT),
            containerUrl: env.ACTOR_WEB_SERVER_URL || null,
            dedicatedCpus: env.ACTOR_DEDICATED_CPUS || null,
            defaultDatasetId: env.ACTOR_DEFAULT_DATASET_ID || null,
            defaultKeyValueStoreId: env.ACTOR_DEFAULT_KEY_VALUE_STORE_ID || null,
            defaultRequestQueueId: env.ACTOR_DEFAULT_REQUEST_QUEUE_ID || null,
            disableOutdatedWarning: parseDisableWarning(env.APIFY_DISABLE_OUTDATED_WARNING),
            fact: env.APIFY_FACT || null,
            headless: env.APIFY_HEADLESS || null,
            inputKey: env.ACTOR_INPUT_KEY || null,
            inputSecretsPrivateKeyFile: env.APIFY_INPUT_SECRETS_PRIVATE_KEY_FILE || null,
            inputSecretsPrivateKeyPassphrase: env.APIFY_INPUT_SECRETS_PRIVATE_KEY_PASSPHRASE || null,
            isAtHome: env.APIFY_IS_AT_HOME || env.SCRAPELY_IS_AT_HOME || null,
            localStorageDir: env.CRAWLEE_STORAGE_DIR || env.APIFY_LOCAL_STORAGE_DIR || null,
            logFormat: env.APIFY_LOG_FORMAT || null,
            logLevel: env.APIFY_LOG_LEVEL || null,
            memoryMbytes: parseIntOrNull(env.ACTOR_MEMORY_MBYTES),
            metaOrigin: env.APIFY_META_ORIGIN || env.ACTOR_META_ORIGIN || null,
            proxyHostname: env.APIFY_PROXY_HOSTNAME || null,
            proxyPassword: env.APIFY_PROXY_PASSWORD || null,
            proxyPort: env.APIFY_PROXY_PORT || null,
            proxyStatusUrl: env.APIFY_PROXY_STATUS_URL || null,
            sdkLatestVersion: null, // Not available from env
            startedAt: parseDate(env.ACTOR_STARTED_AT),
            systemInfoIntervalMillis: env.APIFY_SYSTEM_INFO_INTERVAL_MILLIS || null,
            timeoutAt: parseDate(env.ACTOR_TIMEOUT_AT),
            token: env.APIFY_TOKEN || env.SCRAPELY_TOKEN || null,
            userId: env.APIFY_USER_ID || null,
            userIsPaying: env.APIFY_USER_IS_PAYING || null,
            workflowKey: env.ACTOR_WORKFLOW_KEY || null,
            // Scrapely-specific extensions
            maxTotalChargeUsd: parseFloatOrNull(env.ACTOR_MAX_TOTAL_CHARGE_USD),
            userPlan: env.SCRAPELY_USER_PLAN || null,
            restartOnError: env.ACTOR_RESTART_ON_ERROR || null,
        };
    }

    /**
     * Full technical name of the actor (owner-username/actor-name).
     */
    static get fullName(): string | undefined {
        return process.env.ACTOR_FULL_NAME || undefined;
    }

    /**
     * ID of the Actor task, or undefined if running outside a task.
     */
    static get taskId(): string | undefined {
        return Actor._actorConfig().get('actorTaskId') || undefined;
    }

    /**
     * ID of the Actor build used in the run.
     */
    static get buildId(): string | undefined {
        return process.env.ACTOR_BUILD_ID || undefined;
    }

    /**
     * Build number of the Actor build used in the run.
     */
    static get buildNumber(): string | undefined {
        return process.env.ACTOR_BUILD_NUMBER || undefined;
    }

    /**
     * Comma-separated list of tags of the Actor build used in the run.
     */
    static get buildTags(): string[] {
        return (process.env.ACTOR_BUILD_TAGS || '')
            .split(',')
            .map(t => t.trim())
            .filter(t => t.length > 0);
    }

    /**
     * Date when the Actor was started (ISO 8601 UTC).
     */
    static get startedAt(): string | undefined {
        return process.env.ACTOR_STARTED_AT || undefined;
    }

    /**
     * Date when the Actor will time out (ISO 8601 UTC).
     */
    static get timeoutAt(): string | undefined {
        return process.env.ACTOR_TIMEOUT_AT || undefined;
    }

    /**
     * API token of the user who started the Actor.
     */
    static get token(): string | undefined {
        return Actor._actorConfig().get('token') || undefined;
    }

    /**
     * ID of the user who started the Actor.
     */
    static get userId(): string | undefined {
        return Actor._actorConfig().get('userId') || undefined;
    }

    /**
     * Whether the user who started the Actor is a paying user.
     */
    static get isPaying(): boolean {
        return Actor._actorConfig().get('userIsPaying') === true;
    }

    /**
     * Public URL of the platform API. May be used to interact with the platform programmatically.
     */
    static get apiPublicBaseUrl(): string | undefined {
        return Actor._actorConfig().get('apiPublicBaseUrl') || undefined;
    }

    /**
     * Get a single environment variable value by name.
     * Alias for process.env[key].
     */
    static getenv(key: string): string | undefined {
        return process.env[key];
    }

  
  
   

   
  

    on(event: EventTypeName | 'cpuInfo', listener: (...args: any[]) => void): void {
        this.config.getEventManager().on(event as EventTypeName, listener);
    }

    off(event: EventTypeName | 'cpuInfo', listener?: (...args: any[]) => void): void {
        this.config.getEventManager().off(event as EventTypeName, listener);
    }

    static on(event: EventTypeName | 'cpuInfo', listener: (...args: any[]) => void): void {
        Actor.getDefaultInstance().on(event, listener);
    }

    static off(event: EventTypeName | 'cpuInfo', listener?: (...args: any[]) => void): void {
        Actor.getDefaultInstance().off(event, listener);
    }

    /**
     * Closes the event manager, disconnects platform events, and flushes storage.
     * Called automatically at the end of {@link Actor.main} and {@link Actor.exit}.
     */

    private static _fatalShutdownStarted = false;

    /** One-shot bounded reconciliation on fatal process errors. */
    private static installFatalChargeHandlers(): void {
        if ((Actor as any)._fatalHandlersInstalled) return;
        (Actor as any)._fatalHandlersInstalled = true;
        const runOnce = (reason: string, err?: unknown) => {
            if (Actor._fatalShutdownStarted) return;
            Actor._fatalShutdownStarted = true;
            if (err) console.error(`[Scrapely] Fatal ${reason}:`, err);
            else console.warn(`[Scrapely] Fatal ${reason} — attempting bounded charge flush`);
            // Independent hard-exit timer: a hung flush must not leave a
            // corrupted process alive. unref so the timer alone keeps nothing running.
            const hardExit = setTimeout(() => process.exit(1), 10_000);
            hardExit.unref?.();
            void Actor._shutdownActor(5)
                .catch(() => undefined)
                .finally(() => {
                    // Registering an uncaughtException listener SUPPRESSES Node's
                    // default termination — a fatal error MUST terminate the process
                    // so the platform can retry the run from a clean state.
                    process.exit(1);
                });
        };
        process.once('uncaughtException', (err) => runOnce('uncaughtException', err));
        process.once('unhandledRejection', (err) => runOnce('unhandledRejection', err));
    }

    private static async _shutdownActor(timeoutSecs = 30): Promise<void> {
        const actor = Actor.getDefaultInstance();
        // Tear down whenever init() started installing lifecycle state — including
        // a partially initialized Actor whose init() failed. The public
        // `initialized` flag stays false in that case, but the events WebSocket,
        // storage factory, event manager, and handlers exist and must not leak
        // into a retry, so that flag must NOT be used to skip this cleanup.
        if (!actor._lifecycleStarted) {
            return;
        }

        if (gracefulShutdownHandlers.aborting) {
            Actor.off('aborting', gracefulShutdownHandlers.aborting);
        }
        if (gracefulShutdownHandlers.migrating) {
            Actor.off('migrating', gracefulShutdownHandlers.migrating);
        }
        if (signalExitHandlers.sigint) {
            process.removeListener('SIGINT', signalExitHandlers.sigint);
        }
        if (signalExitHandlers.sigterm) {
            process.removeListener('SIGTERM', signalExitHandlers.sigterm);
        }

        // Best-effort: drain unresolved PPE charge intents under a hard deadline.
        // timeoutSecs bounds the flush so hung network cannot block exit forever.
        const flushDeadlineMs = Math.min(Math.max(1, timeoutSecs) * 1000, 30_000);
        // Only a fully initialized run can have admitted charges; a partial init
        // has none, and closing the manager here would also reject charges made
        // after a successful init retry on the same instance.
        if (actor.initialized) {
            try {
                const cm = actor.chargingManager;
                // F6: reject NEW charges before the final flush so nothing can be
                // admitted after it while teardown is closing storages/credentials.
                if (cm && typeof (cm as { close?: () => void }).close === 'function') {
                    (cm as { close: () => void }).close();
                }
                if (cm && typeof (cm as { flushPending?: (max?: number, deadlineMs?: number) => Promise<number> }).flushPending === 'function') {
                    await Promise.race([
                        // The flush's own deadline ends its retries a little before
                        // the race gives up on it, so none is cut off mid-request.
                        (cm as { flushPending: (max?: number, deadlineMs?: number) => Promise<number> }).flushPending(
                            64,
                            Math.max(500, flushDeadlineMs - 1000),
                        ),
                        new Promise<void>((resolve) => setTimeout(resolve, flushDeadlineMs)),
                    ]);
                }
            } catch (err) {
                console.warn(
                    `[Scrapely] ChargingManager.flushPending failed during shutdown: ${(err as Error).message}`,
                );
            }
        }

        const eventManager = Actor._actorConfig().getEventManager();
        await eventManager.close();

        disconnectEvents();
        disconnectStorage();
        setActiveChargingManager(undefined);

        // Let buffered stdout/stderr reach the log pipe before the process exits
        // (pipe writes are async in Node). Storage was flushed above, so this
        // replaces the old fixed 1 s sleep: it resolves as soon as both streams
        // drain, usually within milliseconds, and is still bounded.
        await waitForStdioDrain(Math.min(1000, flushDeadlineMs));
    }

    /**
     * Store the exit/fail status message on the run as terminal. Best effort and
     * bounded: a failing or slow API is logged and never blocks the exit.
     */
    private static async _persistTerminalStatusMessage(message: string, exitCode: number, timeoutMs = 10_000): Promise<void> {
        let timer: NodeJS.Timeout | undefined;
        try {
            await Promise.race([
                Actor.setStatusMessage(message, {
                    isStatusMessageTerminal: true,
                    level: exitCode > 0 ? 'ERROR' : 'INFO',
                    // Stop retrying when the exit stops waiting for it.
                    retryWindowMs: timeoutMs,
                } as SetStatusMessageOptions),
                new Promise<never>((_, reject) => {
                    timer = setTimeout(
                        () => reject(new Error(`timed out after ${timeoutMs}ms`)),
                        timeoutMs,
                    );
                }),
            ]);
        } catch (err) {
            console.warn(`[Scrapely] Failed to store the final status message on the run: ${(err as Error).message}`);
        } finally {
            if (timer) clearTimeout(timer);
        }
    }

    /**
     * Sets the status message for the current Actor run.
     * 
     * @param message - The status message to set
     * @param options - Optional settings for the status message
     * @returns The updated ActorRun object, or an empty object if running locally
     */
    static async setStatusMessage(message: string, options?: SetStatusMessageOptions): Promise<ActorRun> {
        const { isStatusMessageTerminal, level } = options || {};
        Actor._ensureActorInit('setStatusMessage');

        const loggedStatusMessage = `[Status message]: ${message}`;
        switch (level) {
            case 'DEBUG':
                console.debug(loggedStatusMessage);
                break;
            case 'WARNING':
                console.warn(loggedStatusMessage);
                break;
            case 'ERROR':
                console.error(loggedStatusMessage);
                break;
            default:
                console.log(loggedStatusMessage);
                break;
        }

        const config = Actor._actorConfig();
        const client = config.getStorageClient();
        if (typeof client.setStatusMessage === 'function') {
            await Promise.race([
                client.setStatusMessage(message, { isStatusMessageTerminal, level }),
                new Promise<void>((_, reject) =>
                    setTimeout(() => reject(new Error('Setting status message timed out after 1s')), 1000),
                ),
            ]).catch((err) => {
                console.warn(`[Scrapely] ${(err as Error).message}`);
            });
        }

        const apiUrl = config.get('apiBaseUrl');
        const token = config.get('token');
        const runId = config.get('actorRunId');

        if (!apiUrl || !token || !runId) {
            return {} as ActorRun;
        }

        // Setting the message is idempotent: a 429 (run API rate limit), 5xx or
        // dropped connection is retried with backoff instead of failing the run.
        const body = await fetchWithRetries(
            `${apiUrl}/v2/actor-runs/${runId}`,
            {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${token}`,
                },
                body: JSON.stringify({
                    statusMessage: message,
                    isStatusMessageTerminal,
                }),
            },
            {
                idempotent: true,
                timeoutMs: 10_000,
                label: 'PUT status message',
                retryWindowMs: (options as { retryWindowMs?: number } | undefined)?.retryWindowMs,
            },
            async (response) => (await response.json().catch(() => ({}))) as { data?: ActorRun },
        ).catch((err) => {
            const error = new Error(`Failed to update status message: ${describeRunApiError(err)}`);
            // A progress update is best effort: if the API still fails after the
            // retry window (e.g. a long 503), log it and carry on instead of
            // crashing the run. Terminal updates keep throwing.
            if (isStatusMessageTerminal) throw error;
            console.warn(`[Scrapely] ${error.message} (continuing)`);
            return undefined;
        });
        return (body?.data ?? {}) as ActorRun;
    }

    /**
     * Anti-bot solver status for a vendor (free, cached for 15 s, never throws).
     * `usable: false` means the vendor cannot be solved right now: take your
     * fallback (for example the hosted browser), or fail the items that need a
     * solve at once and call `setSolverDegradedStatus()`.
     */
    static async solverStatus(vendor: string): Promise<SolverVendorStatus> {
        return fetchSolverStatus(vendor);
    }

    /**
     * Solves one anti-bot challenge on the Actor's own session through the
     * platform solver. See `solve` in `scrapely` (solver.ts) for the options.
     */
    static async solve(options: SolveOptions): Promise<Clearance> {
        return solveChallenge(options);
    }

    /**
     * Sets the standard status message for a run whose site protection cannot
     * be solved right now ("Site protection temporarily unavailable, results
     * may be incomplete", plus "(N of M items skipped)" when both counts are
     * given). Never throws: the run goes on.
     */
    static async setSolverDegradedStatus(skipped?: number, total?: number): Promise<void> {
        try {
            await Actor.setStatusMessage(degradedStatusMessage(skipped, total));
        } catch (err) {
            console.warn(`Could not set the solver status message: ${(err as Error).message}`);
        }
    }

    /**
     * Creates a proxy configuration and returns a promise resolving to an instance
     * of the ProxyConfiguration class that is already initialized.
     * 
     * Configures connection to a proxy server with the provided options. Proxy servers
     * are used to prevent target websites from blocking your crawlers based on IP address
     * rate limits or blacklists. Setting proxy configuration in your crawlers automatically
     * configures them to use the selected proxies for all connections.
     * 
     * **Example usage:**
     * ```javascript
     * const proxyConfiguration = await Actor.createProxyConfiguration({
     *     proxyUrls: ['http://user:pass@proxy1.example.com:8080'],
     * });
     * 
     * const crawler = new CheerioCrawler({
     *     proxyConfiguration,
     *     requestHandler({ proxyInfo }) {
     *         const usedProxyUrl = proxyInfo.url; // Getting the proxy URL
     *     },
     * });
     * ```
     * 
     * Pass the proxy input field (`editor: "proxy"`) as is: this function returns
     * `undefined` when `{ useScrapelyProxy: false }` is passed without custom proxies.
     * 
     * @param options - Proxy configuration options
     * @returns Promise resolving to ProxyConfiguration instance, or undefined
     */
    static async createProxyConfiguration(
        options: CreateProxyConfigurationOptions = {}
    ): Promise<ReturnType<typeof createProxyConfiguration>> {
        return createProxyConfiguration(options);
    }

    /**
     * Creates a Scrapely remote browser configuration (location and
     * fingerprint options). Pass it to browser crawlers next to
     * `proxyConfiguration`:
     *
     * ```javascript
     * const browserConfiguration = await Actor.createBrowserConfiguration({ countryCode: 'US', os: 'windows' });
     * const crawler = new PlaywrightCrawler({ browserConfiguration, requestHandler });
     * ```
     *
     * Ignored outside the Scrapely remote browser (local runs), so the same
     * code runs locally and on the platform.
     */
    static async createBrowserConfiguration(
        options: BrowserConfigurationOptions = {},
    ): Promise<BrowserConfiguration> {
        return createBrowserConfiguration(options);
    }

    /** @internal */
    private static _instance: Actor;

    /**
     * Get the default Actor instance (singleton).
     * @returns The default Actor instance
     */
    static getDefaultInstance(): Actor {
        Actor._instance ??= new Actor();
        return Actor._instance;
    }

    /**
     * Default Configuration instance (static accessor).
     * Used after Actor.init() without new Actor(), e.g. Actor.config.get('standbyPort').
     */
    static get config(): Configuration {
        return Actor.getDefaultInstance().config;
    }

    /**
     * Default API client instance for the Scrapely platform.
     */
    static get apifyClient(): ApifyClient {
        return Actor.getDefaultInstance().apifyClient;
    }

    /**
     * Returns a new API client (ApifyClient) pointed at the Scrapely platform.
     */
    newClient(options?: { token?: string; baseUrl?: string }): ApifyClient {
        const config = this.config;
        const clientOptions = {
            token: options?.token ?? config.get('token') ?? '',
            // Fall back to the client's own default when the configuration has no value,
            // instead of forcing an empty string (which would produce a relative `/v2` base URL).
            baseUrl: options?.baseUrl ?? config.get('apiBaseUrl'),
            // Public URLs (getRecordPublicUrl, createItemsPublicUrl) must not be minted
            // against a different host than the API the client talks to. Follow an explicit
            // per-call base URL override, then the configured public base URL.
            publicBaseUrl: options?.baseUrl ?? config.get('apiPublicBaseUrl'),
        };

        if (options?.token || options?.baseUrl) {
            return createChargingClient(clientOptions, this);
        }

        if (apifyClientInstance) {
            return apifyClientInstance;
        }

        apifyClientInstance = createChargingClient(clientOptions, this);
        return apifyClientInstance;
    }

    static newClient(options?: { token?: string; baseUrl?: string }): ApifyClient {
        return Actor.getDefaultInstance().newClient(options);
    }

    /** Instance API — delegates to {@link Actor.charge}. */
    async charge(
        options: ChargingChargeOptions | string,
        count?: number,
    ): Promise<ChargeResult> {
        return Actor.charge(options, count);
    }

    /**
     * Charge for a custom pricing event (pay-per-event Actors).
     */
    static async charge(
        options: ChargingChargeOptions | string,
        count?: number,
    ): Promise<ChargeResult> {
        Actor._ensureActorInit('charge');

        const chargeOptions: ChargingChargeOptions =
            typeof options === 'string' ? { eventName: options, count: count ?? 1 } : options;

        return Actor.getDefaultInstance().chargingManager.charge(chargeOptions);
    }

    static getChargingManager(): ChargingManager {
        return Actor.getDefaultInstance().chargingManager;
    }

    getChargingManager(): ChargingManager {
        return this.chargingManager;
    }

    /**
     * Metamorph the current actor run into a run of another actor.
     * Metamorph behavior:
     * 1. Stores new input under INPUT-METAMORPH-{n} key in the default KVS
     * 2. Signals the server to swap the container to the target actor's image
     * 3. Exits the current process (the container will be replaced)
     * 
     * @param targetActorId - The ID or username/actorName of the target actor
     * @param input - Optional input for the target actor (uses current input if not provided)
     * @param options - Optional build tag and content type for the target actor
     */
    static async metamorph(
        targetActorId: string,
        input?: Record<string, any> | string | Buffer,
        options?: MetamorphOptions
    ): Promise<void> {
        const config = Actor._actorConfig();
        const apiUrl = config.get('apiBaseUrl');
        const token = config.get('token');
        const runId = config.get('actorRunId');

        if (!apiUrl || !token || !runId) {
            throw new Error(
                'Metamorph requires apiBaseUrl, token, and actorRunId in configuration',
            );
        }

        // Wire format (server contract, Python SDK parity): the new input (if
        // any) goes in the request body's `input` field; the server stores it as
        // INPUT-METAMORPH-<n> (INPUT keeps the original) and tracks the metamorph
        // count in run stats. `inputKey` is NOT part of the contract.
        const body: Record<string, any> = { targetActorId };
        if (options?.build) {
            body.build = options.build;
        }
        if (input !== undefined) {
            body.input = input;
        }

        console.log(`[Scrapely] Metamorphosing to actor ${targetActorId}`);

        // Signal the server to perform the metamorph
        try {
            // Retried only while certainly not applied (429, limiter/HAProxy 503,
            // refused connection): a second metamorph must never be started.
            await fetchWithRetries(
                `${apiUrl}/v2/actor-runs/${runId}/metamorph`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${token}`,
                    },
                    body: JSON.stringify(body),
                },
                { idempotent: false, timeoutMs: 30_000, label: 'POST metamorph' },
                async () => undefined,
            ).catch((err) => {
                throw new Error(`Metamorph request failed: ${describeRunApiError(err)}`);
            });

            console.log('[Scrapely] Metamorph request accepted, exiting...');
        } catch (err) {
            console.error(`[Scrapely] Metamorph failed: ${(err as Error).message}`);
            throw err;
        }

        // Flush pending PPE intents before hard exit (same class of loss as SIGTERM).
        try {
            await Actor._shutdownActor(5);
        } catch (err) {
            console.warn(
                `[Scrapely] flushPending during metamorph failed: ${(err as Error).message}`,
            );
        }

        // Exit so the worker can restart the container with the target actor.
        process.exit(0);
    }

    /**
     * Persist runtime state across server migrations.
     *
     * Long-running Actor jobs may need to migrate between servers.
     * Without state persistence, your job's progress is lost during
     * migration, causing it to restart from the beginning on the new server.
     *
     * This method automatically saves your state to the key-value store
     * whenever a `persistState` event occurs (every 60 seconds and on exit),
     * and restores it when the Actor restarts after a migration.
     *
     * The state must be JSON-serializable (values must survive JSON.stringify/parse).
     * For short-running Actors (less than a few minutes), state persistence is
     * typically unnecessary.
     *
     * @param nameOrInitialState - Either the state key name (string) or default state object
     * @param defaultValue - Default state used when no previously persisted state exists (when first param is string)
     * @param options - Options for state persistence
     * @returns The current state object (mutate it directly; it saves on persistState events)
     */
    static async useState<State extends Dictionary = Dictionary>(
        nameOrInitialState?: string | State,
        defaultValue: State = {} as State,
        options?: UseStateOptions,
    ): Promise<State> {
        Actor._ensureActorInit('useState');

        let stateKey: string;
        let initialState: State;
        if (typeof nameOrInitialState === 'string') {
            stateKey = nameOrInitialState;
            initialState = defaultValue;
        } else if (nameOrInitialState !== undefined) {
            // Legacy: useState(initialState) stores under STATE
            stateKey = 'STATE';
            initialState = nameOrInitialState;
        } else {
            stateKey = 'APIFY_GLOBAL_STATE';
            initialState = defaultValue;
        }

        const { KeyValueStore } = await import('@crawlee/core');
        const kvStore = await KeyValueStore.open(options?.keyValueStoreName, {
            config: options?.config ?? Actor._actorConfig(),
        });
        return kvStore.getAutoSavedValue(stateKey, initialState);
    }

    /**
     * Reboot the Actor run.
     *
     * This method is designed to be called in the `migrating` event handler
     * to speed up the migration process and ensure state consistency.
     *
     * After calling this, the Actor container will be stopped and restarted
     * on a new server. Any state persisted via Actor.useState() will be
     * automatically restored.
     *
     * @param options - Reboot options
     */
    static async reboot(options?: RebootOptions): Promise<void> {
        console.log('[Scrapely] Rebooting...');
        Actor._ensureActorInit('reboot');
        // Emit persistState to ensure state is saved before reboot
        Actor._actorConfig().getEventManager().emit('persistState' as never, { isMigrating: true });
        // Wait a moment for state to be persisted
        await new Promise((resolve) => setTimeout(resolve, 1000));
        // Call the reboot endpoint to signal the worker
        const config = Actor._actorConfig();
        const apiUrl = config.get('apiBaseUrl') || 'http://localhost:3000';
        const token = config.get('token') || '';
        const runId = config.get('actorRunId') || '';
        try {
            // Retried only while certainly not applied (429, limiter/HAProxy 503,
            // refused connection).
            const status = await fetchWithRetries(
                `${apiUrl}/v2/actor-runs/${runId}/reboot`,
                {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${token}`,
                        'Content-Type': 'application/json',
                    },
                },
                { idempotent: false, timeoutMs: 30_000, label: 'POST reboot' },
                async (response) => response.status,
            ).catch((err) => {
                throw new Error(`Reboot request failed: ${describeRunApiError(err)}`);
            });
            console.log(`[Scrapely] Reboot request accepted (${status})`);
            // Wait briefly for the reboot signal to be processed, then exit
            const sleepMillis = options?.customAfterSleepMillis ?? Actor._actorConfig().get('metamorphAfterSleepMillis', 1000);
            await new Promise((resolve) => setTimeout(resolve, sleepMillis));
        } catch (err) {
            console.error(`[Scrapely] Failed to send reboot request: ${(err as Error).message}`);
            throw err;
        }
        // Flush pending PPE intents before hard exit (same class of loss as SIGTERM/metamorph).
        try {
            await Actor._shutdownActor(5);
        } catch (err) {
            console.warn(
                `[Scrapely] flushPending during reboot failed: ${(err as Error).message}`,
            );
        }
        // Exit so the worker can restart the container.
        process.exit(0);
    }

    /**
     * Abort an Actor run on the Scrapely platform.
     * 
     * @param runId - The ID of the run to abort. Defaults to the current run.
     * @param options - Abort options
     * @returns The ActorRun object with details about the aborted run
     */
    static async abort(runId?: string, options: AbortOptions = {}): Promise<ActorRun> {
        const targetRunId = runId ?? Actor._actorConfig().get('actorRunId');
        if (!targetRunId) {
            throw new Error('Actor.abort() requires a run ID');
        }

        const { token, statusMessage, ...rest } = options;
        const client = token ? Actor.newClient({ token }) : Actor.apifyClient;

        if (statusMessage) {
            await client.run(targetRunId).update({
                statusMessage,
                isStatusMessageTerminal: true,
            });
        }

        return client.run(targetRunId).abort(rest);
    }

    static async addWebhook(options: WebhookOptions): Promise<Webhook | undefined> {
        if (!Actor.isAtHome()) {
            console.warn('[Scrapely] Actor.addWebhook() is only supported on the platform. Ignoring.');
            return undefined;
        }

        const runId = Actor._actorConfig().get('actorRunId');
        if (!runId) {
            throw new Error('Environment variable ACTOR_RUN_ID is not set!');
        }

        return Actor.apifyClient.webhooks().create({
            ...options,
            eventTypes: options.eventTypes as Webhook['eventTypes'],
            isAdHoc: true,
            condition: {
                actorRunId: runId,
            },
        });
    }

    /**
     * Runs an Actor on the platform and waits for it to finish (instance API).
     */
    async call(actorId: string, input?: unknown, options: CallOptions = {}): Promise<ActorRun> {
        return Actor.call(actorId, input, options);
    }

    static async call(actorId: string, input?: unknown, options: CallOptions = {}): Promise<ActorRun> {
        const timeout = resolveCallTimeout(options.timeout);
        const { token, ...rest } = options;
        const client = token ? Actor.newClient({ token }) : Actor.apifyClient;
        return client.actor(actorId).call(input, { ...rest, timeout });
    }

    /**
     * Runs an Actor task on the platform and waits for it to finish (instance API).
     */
    async callTask(taskId: string, input?: Dictionary, options: CallTaskOptions = {}): Promise<ActorRun> {
        return Actor.callTask(taskId, input, options);
    }

    static async callTask(taskId: string, input?: Dictionary, options: CallTaskOptions = {}): Promise<ActorRun> {
        const timeout = resolveCallTimeout(options.timeout);
        const { token, ...rest } = options;
        const client = token ? Actor.newClient({ token }) : Actor.apifyClient;
        return client.task(taskId).call(input, { ...rest, timeout });
    }

    static async start(actorId: string, input?: Dictionary, options: StartOptions = {}): Promise<ActorRun> {
        const timeout = resolveCallTimeout(options.timeout);
        const { token, ...rest } = options;
        const client = token ? Actor.newClient({ token }) : Actor.apifyClient;
        return client.actor(actorId).start(input, { ...rest, timeout });
    }

    // -------------------------------------------------------------------------
    // Instance API (mirrors static methods on Actor class)
    // -------------------------------------------------------------------------

    async main<T>(userFunc: UserFunc<T>, options?: MainOptions): Promise<T> {
        return Actor.main(userFunc, options);
    }

    async exit(messageOrOptions?: string | ExitOptions, options: ExitOptions = {}): Promise<void> {
        return Actor.exit(messageOrOptions, options);
    }

    async fail(messageOrOptions?: string | ExitOptions, options: ExitOptions = {}): Promise<void> {
        return Actor.fail(messageOrOptions, options);
    }

    async getInput<T = ActorInput>(): Promise<T | null> {
        this._ensureInit('getInput');
        return Actor.getInput<T>();
    }

    async getInputOrThrow<T = ActorInput>(): Promise<T> {
        this._ensureInit('getInputOrThrow');
        return Actor.getInputOrThrow<T>();
    }

    async getValue<T>(key: string): Promise<T | null> {
        this._ensureInit('getValue');
        return Actor.getValue<T>(key);
    }

    async setValue<T>(key: string, value: T | null, options?: RecordOptions): Promise<void> {
        this._ensureInit('setValue');
        return Actor.setValue(key, value, options);
    }

    async openDataset<T extends Record<string, any>>(
        nameOrIdOrOptions?: string | null | StorageIdentifierArg,
        openOptions?: OpenStorageOptions,
    ): Promise<Dataset<T>> {
        this._ensureInit('openDataset');
        return Actor._openDataset<T>(this.config, nameOrIdOrOptions, {
            ...openOptions,
            config: openOptions?.config ?? this.config,
        });
    }

    async openKeyValueStore(
        nameOrIdOrOptions?: string | null | StorageIdentifierArg,
        openOptions?: OpenStorageOptions,
    ): Promise<KeyValueStore> {
        this._ensureInit('openKeyValueStore');
        return Actor._openKeyValueStore(this.config, nameOrIdOrOptions, {
            ...openOptions,
            config: openOptions?.config ?? this.config,
        });
    }

    async openRequestQueue(
        nameOrIdOrOptions?: string | null | StorageIdentifierArg,
        openOptions?: OpenStorageOptions,
    ): Promise<RequestQueue> {
        this._ensureInit('openRequestQueue');
        return Actor._openRequestQueue(this.config, nameOrIdOrOptions, {
            ...openOptions,
            config: openOptions?.config ?? this.config,
        });
    }

    isAtHome(): boolean {
        return !!this.config.get('isAtHome');
    }

    /** Instance mirror of {@link Actor.isStandby} (Python SDK `Actor.is_standby` parity). */
    isStandby(): boolean {
        return this.config.get('metaOrigin') === 'STANDBY';
    }

    getEnv(): ApifyEnv {
        return Actor.getEnv();
    }

    async setStatusMessage(message: string, options?: SetStatusMessageOptions): Promise<ActorRun> {
        this._ensureInit('setStatusMessage');
        return Actor.setStatusMessage(message, options);
    }

    async createProxyConfiguration(
        options: CreateProxyConfigurationOptions = {},
    ): Promise<ReturnType<typeof createProxyConfiguration>> {
        return Actor.createProxyConfiguration(options);
    }

    async createBrowserConfiguration(
        options: BrowserConfigurationOptions = {},
    ): Promise<BrowserConfiguration> {
        return Actor.createBrowserConfiguration(options);
    }

    async metamorph(
        targetActorId: string,
        input?: Record<string, any> | string | Buffer,
        options?: MetamorphOptions,
    ): Promise<void> {
        return Actor.metamorph(targetActorId, input, options);
    }

    async reboot(options?: RebootOptions): Promise<void> {
        return Actor.reboot(options);
    }

    async abort(runId?: string, options: AbortOptions = {}): Promise<ActorRun> {
        return Actor.abort(runId, options);
    }

    async addWebhook(options: WebhookOptions): Promise<Webhook | undefined> {
        return Actor.addWebhook(options);
    }

    async start(actorId: string, input?: Dictionary, options: StartOptions = {}): Promise<ActorRun> {
        return Actor.start(actorId, input, options);
    }

    async useState<State extends Dictionary = Dictionary>(
        nameOrInitialState?: string | State,
        defaultValue: State = {} as State,
        options?: UseStateOptions,
    ): Promise<State> {
        this._ensureInit('useState');
        return Actor.useState(nameOrInitialState, defaultValue, {
            ...options,
            config: options?.config ?? this.config,
        });
    }

    /**
     * Utility namespace.
     * Provides helper functions like enqueueLinks that accept the v2-style call signature.
     */
    static readonly utils = utilsNamespace;

}

/** Resolves once stdout and stderr have flushed their buffers, or after `maxMs`. */
export async function waitForStdioDrain(maxMs: number, streams: NodeJS.WritableStream[] = [process.stdout, process.stderr]): Promise<void> {
    const drained = Promise.all(streams.map((stream) => new Promise<void>((resolve) => {
        const s = stream as NodeJS.WritableStream & {
            writableLength?: number;
            writableEnded?: boolean;
            writableFinished?: boolean;
        };
        if (!s.writableLength || s.writableFinished) return resolve();
        // An ended stream accepts no more writes; wait for it to finish flushing.
        if (s.writableEnded) {
            s.once('finish', () => resolve());
            return;
        }
        // A zero-length write's callback runs after everything queued before it.
        s.write('', () => resolve());
    })));
    let timer: NodeJS.Timeout | undefined;
    await Promise.race([drained, new Promise<void>((resolve) => { timer = setTimeout(resolve, maxMs); })]);
    if (timer) clearTimeout(timer);
}

/**
 * Crawlee's own item rules (an object, serializable, at most ~9 MB), checked
 * before a charge: Dataset.pushData would refuse such an item only after it
 * was charged. Undefined when this crawlee version does not export the check.
 */
async function crawleeItemValidator(): Promise<((items: unknown[]) => void) | undefined> {
    const core = (await import('@crawlee/core')) as { checkAndSerialize?: (item: unknown, limitBytes: number, index?: number) => string };
    const check = core.checkAndSerialize;
    if (typeof check !== 'function') return undefined;
    // Same limit as Dataset.pushData (MAX_PAYLOAD_SIZE_BYTES minus its 0.01% safety buffer).
    const max = 9 * 1024 * 1024;
    const limit = max - Math.ceil(max * 0.0001);
    return (items) => items.forEach((item, index) => check(item, limit, index));
}
