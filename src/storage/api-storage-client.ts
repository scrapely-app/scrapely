import type {
    DatasetClient, DatasetInfo, DatasetClientListOptions, DatasetClientUpdateOptions,
    KeyValueStoreClient, KeyValueStoreInfo, KeyValueStoreRecord,
    KeyValueStoreClientGetRecordOptions, KeyValueStoreClientListOptions,
    KeyValueStoreClientListData, KeyValueStoreRecordOptions,
    KeyValueStoreClientUpdateOptions,
    RequestQueueClient, RequestQueueInfo, QueueHead, QueueOperationInfo,
    RequestSchema, UpdateRequestSchema, RequestOptions, BatchAddRequestsResult,
    ListOptions, ListAndLockOptions, ListAndLockHeadResult, RequestQueueHeadItem,
    ProlongRequestLockOptions, ProlongRequestLockResult, DeleteRequestLockOptions,
    PaginatedList, Dictionary, StorageClient, RequestQueueOptions,
} from '@crawlee/types';
import { randomUUID } from 'node:crypto';
import { Readable } from 'node:stream';

import { defaultDatasetIdFromEnv, refuseDefaultDatasetPush } from '../charging.js';
import type { ListRequestsOptions, ListRequestsResult } from './request-queue-client.js';
import {
    forEachKeyPaginated,
    makePaginatingEntriesIterable,
    makePaginatingKeysIterable,
    makePaginatingValuesIterable,
} from './kvs-iteration.js';
import { pathSegment } from './utils.js';

/** Query params shared by request queue API calls. */
interface RqQueryOptions {
    forefront?: boolean;
    clientKey?: string;
    lockSecs?: number;
    limit?: number;
    runId?: string;
}

function buildRqQueryParams(baseClientKey: string, options?: RqQueryOptions): string {
    const params = new URLSearchParams();
    params.set('clientKey', options?.clientKey ?? baseClientKey);
    if (options?.forefront) params.set('forefront', 'true');
    if (options?.lockSecs !== undefined) params.set('lockSecs', String(options.lockSecs));
    if (options?.limit !== undefined) params.set('limit', String(options.limit));
    if (options?.runId) params.set('runId', options.runId);
    const query = params.toString();
    return query ? `?${query}` : '';
}

function mapRequestQueueInfo(id: string, result: Record<string, unknown>): RequestQueueInfo {
    return {
        id: (result.id as string) ?? id,
        name: (result.name as string | null) ?? undefined,
        userId: (result.userId as string | undefined) ?? undefined,
        createdAt: parseApiDate(result.createdAt),
        modifiedAt: parseApiDate(result.modifiedAt),
        accessedAt: parseApiDate(result.accessedAt),
        expireAt: result.expireAt as string | undefined,
        totalRequestCount: result.totalRequestCount as number,
        handledRequestCount: result.handledRequestCount as number,
        pendingRequestCount: result.pendingRequestCount as number,
        hadMultipleClients: result.hadMultipleClients as boolean | undefined,
        actId: (result.actId as string | undefined) ?? (result.actorId as string | undefined),
        actRunId: result.actRunId as string | undefined,
        stats: result.stats as RequestQueueInfo['stats'],
    };
}

/**
 * Configuration for API-based storage client
 */
export interface ApiStorageConfig {
    apiUrl: string;      // SCRAPELY_API_URL - internal API endpoint
    token: string;       // SCRAPELY_TOKEN - user's API key
    runId: string;       // Current run ID
}

/**
 * Adaptive rate limiter that tracks server-provided rate limit headers.
 * Coordinates across all runs for a user by reading X-RateLimit-* headers.
 */
class AdaptiveRateLimiter {
    private limit: number = 1000;
    private remaining: number = 1000;
    private resetAt: number = 0;
    private lowWaterMark: number = 50;  // Start throttling when remaining < this
    private minDelay: number = 50;       // Minimum delay between requests (ms)
    private maxDelay: number = 1000;     // Maximum delay when rate limited (ms)
    private lastRequestTime: number = 0;

    /**
     * Update internal state from response headers.
     * Called after each successful API response.
     */
    updateFromHeaders(headers: Headers): void {
        const limitHeader = headers.get('X-RateLimit-Limit');
        const remainingHeader = headers.get('X-RateLimit-Remaining');
        const resetHeader = headers.get('X-RateLimit-Reset');

        if (limitHeader) this.limit = parseInt(limitHeader, 10) || 600;
        if (remainingHeader) this.remaining = parseInt(remainingHeader, 10) || 0;
        if (resetHeader) this.resetAt = parseInt(resetHeader, 10) * 1000; // Convert to ms
    }

    /**
     * Wait before making the next request if needed.
     * Uses adaptive throttling based on remaining capacity.
     */
    async waitForSlot(): Promise<void> {
        const now = Date.now();

        // If we're past the reset time, reset our tracking
        if (now >= this.resetAt) {
            this.remaining = this.limit;
        }

        // Calculate adaptive delay based on remaining capacity
        let delay = this.minDelay;

        if (this.remaining < this.lowWaterMark) {
            // We're running low - slow down proportionally
            const ratio = this.remaining / this.lowWaterMark;
            delay = Math.min(this.maxDelay, this.minDelay + (1 - ratio) * 500);
        } else if (this.remaining < this.limit * 0.2) {
            // Under 20% capacity - moderate slowdown
            delay = 100;
        }

        // Ensure minimum time between requests
        const timeSinceLastRequest = now - this.lastRequestTime;
        if (timeSinceLastRequest < delay) {
            await new Promise(resolve => setTimeout(resolve, delay - timeSinceLastRequest));
        }

        this.lastRequestTime = Date.now();
    }
}

/**
 * Unwrap v2 response envelopes ({ data: ... }).
 */
function unwrapData<T>(value: unknown): T {
    if (Array.isArray(value)) return value as T;
    if (value && typeof value === 'object' && 'data' in value) {
        return (value as { data: T }).data;
    }
    return value as T;
}

function parseApiDate(value: unknown): Date {
    if (value instanceof Date) return value;
    if (typeof value === 'string' && value) {
        const parsed = new Date(value);
        if (!isNaN(parsed.getTime())) return parsed;
    }
    return new Date();
}

const STORAGE_UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** Crawlee opens by name or id; only UUIDs are valid API resource ids. */
function isStorageUuid(id: string): boolean {
    return STORAGE_UUID_REGEX.test(id);
}

export class ApiStorageClientError extends Error {
    readonly statusCode: number;

    constructor(message: string, statusCode: number) {
        super(message);
        this.name = 'ApiStorageClientError';
        this.statusCode = statusCode;
    }
}

function isApiStorageClientError(err: unknown): err is ApiStorageClientError {
    return err instanceof ApiStorageClientError;
}

// PUT is left out: a KV record PUT is counted (and billed) as a write each
// time it is applied, so a lost response must not be re-sent blindly.
const IDEMPOTENT_METHODS = new Set(['GET', 'HEAD', 'DELETE', 'OPTIONS']);
// HAProxy's own 503 page: no backend was available, so nothing was forwarded.
const NO_BACKEND_MARKER = 'No server is available to handle this request';
const CONNECT_FAILURE_CODES = new Set(['ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH', 'UND_ERR_CONNECT_TIMEOUT']);

/**
 * Whether a non-2xx answer is retried. 429 always: the run API limiter and
 * dataset backpressure refuse it before anything is written. Idempotent
 * methods: also 500/502/503/504. Others (dataset push, KV PUT) only on a 503
 * that certainly was not processed: the rate limiter refused it
 * ("rate-limit-unavailable", before the handler), or HAProxy had no backend
 * (its own page, nothing forwarded). Any other 503 may mean "outcome unknown"
 * (e.g. a direct dataset write whose commit was not confirmed), and re-sending
 * could store the items twice.
 */
export function isRetryableResponse(status: number, idempotent: boolean, errorType?: string, bodyText?: string): boolean {
    if (status === 429) return true;
    if (idempotent) return status === 500 || status === 502 || status === 503 || status === 504;
    if (status !== 503) return false;
    return errorType === 'rate-limit-unavailable' || (bodyText ?? '').includes(NO_BACKEND_MARKER);
}

/** Default time budget for retrying one API call (429, 503, network errors). */
export const DEFAULT_API_RETRY_WINDOW_MS = 300_000;

/**
 * Response header the API sets on every dataset push that sent an
 * Idempotency-Key, once it de-duplicates such pushes. An older API ignores the
 * key, and re-sending a push there could store the rows twice, so ambiguous
 * failures of keyed writes are retried only while the latest keyed answer from
 * that API origin carried this header. A 2xx without it (an older instance,
 * e.g. mid-deploy or after a rollback) switches the retries off again.
 */
const IDEMPOTENCY_SUPPORT_HEADER = 'x-scrapely-idempotency';
const keyedWritesDeduped = new Map<string, boolean>();
/** Test override for every origin (undefined: learn from answers). */
let keyedWritesDedupedOverride: boolean | undefined;

function originOf(url: string): string {
    try { return new URL(url).origin; } catch { return url; }
}

/** Whether the API at `url` (any URL of it; default: the last one seen) de-duplicates keyed writes. */
export function apiDedupesKeyedWrites(url?: string): boolean {
    if (keyedWritesDedupedOverride !== undefined) return keyedWritesDedupedOverride;
    if (url !== undefined) return keyedWritesDeduped.get(originOf(url)) === true;
    return [...keyedWritesDeduped.values()].pop() === true;
}

/** Test hook: forget what was learned about the API, or force an answer for every origin. */
export function _resetKeyedWriteSupport(value?: boolean): void {
    keyedWritesDeduped.clear();
    keyedWritesDedupedOverride = value === true ? true : undefined;
}

function noteKeyedAnswer(url: string, response: Response): void {
    const origin = originOf(url);
    if (response.headers.has(IDEMPOTENCY_SUPPORT_HEADER)) {
        keyedWritesDeduped.delete(origin);
        keyedWritesDeduped.set(origin, true);
    } else if (response.ok) {
        // Stored by a handler that ignored the key: this API (instance) does not de-duplicate.
        keyedWritesDeduped.delete(origin);
        keyedWritesDeduped.set(origin, false);
        if (keyedWritesDedupedOverride === true) keyedWritesDedupedOverride = undefined;
    }
}

/** API error types of a keyed write: still being stored (retry), or stored-or-not unknown (final). */
const KEY_IN_PROGRESS = 'idempotency-key-in-progress';
const OUTCOME_UNKNOWN = 'push-outcome-unknown';

/**
 * Marks an error whose request may have been applied by an earlier attempt
 * (timeout, dropped connection or 5xx before the final answer), so callers
 * don't treat it as "nothing was stored".
 */
function markOutcomeUnknown<E>(err: E): E {
    if (err && typeof err === 'object') (err as { outcomeUnknown?: boolean }).outcomeUnknown = true;
    return err;
}
const MAX_BACKOFF_MS = 30_000;

/**
 * How long one API call keeps retrying before it gives up:
 * SCRAPELY_API_RETRY_WINDOW_SECS when set, else {@link BaseApiClient.retryWindowMs}
 * (5 minutes by default).
 */
export function apiRetryWindowMs(): number {
    const env = Number(process.env.SCRAPELY_API_RETRY_WINDOW_SECS);
    if (process.env.SCRAPELY_API_RETRY_WINDOW_SECS && Number.isFinite(env) && env >= 0) return env * 1000;
    return BaseApiClient.retryWindowMs;
}

function parseRetryAfterMs(retryAfter: string | null | undefined, now: number): number | undefined {
    if (retryAfter === null || retryAfter === undefined || retryAfter.trim() === '') return undefined;
    let secs = Number(retryAfter);
    if (!Number.isFinite(secs)) {
        const at = Date.parse(retryAfter);
        secs = Number.isFinite(at) ? Math.max(0, (at - now) / 1000) : NaN;
    }
    // Not capped: a Retry-After past the retry window ends the retries instead.
    return Number.isFinite(secs) && secs >= 0 ? secs * 1000 : undefined;
}

/**
 * Delay before retry number `attempt + 1`: exponential backoff (0.5 s doubling,
 * capped at 30 s) with equal jitter, and never earlier than the server's
 * Retry-After (seconds or HTTP date) plus a little jitter so
 * throttled clients don't all come back in the same instant.
 */
export function serverRetryDelayMs(attempt: number, retryAfter: string | null, random: () => number = Math.random, now: number = Date.now()): number {
    const exp = Math.min(MAX_BACKOFF_MS, 500 * 2 ** Math.min(attempt, 16));
    const backoff = exp / 2 + random() * (exp / 2);
    const ra = parseRetryAfterMs(retryAfter, now);
    if (ra === undefined) return backoff;
    return Math.max(ra + random() * Math.min(1000, ra * 0.2 + 100), backoff);
}

function fetchErrorCode(err: unknown): string | undefined {
    const e = err as { code?: string; cause?: { code?: string; cause?: { code?: string } } };
    return e?.cause?.code ?? e?.cause?.cause?.code ?? e?.code;
}

/** fetch() failed before the request reached the server. */
export function isConnectFailure(err: unknown): boolean {
    const code = fetchErrorCode(err);
    if (code !== undefined && CONNECT_FAILURE_CODES.has(code)) return true;
    // A TCP connect that timed out (`connect ETIMEDOUT`, or Node's happy-eyeballs
    // AggregateError of connect attempts): nothing was sent. An ETIMEDOUT on an
    // established socket (syscall "read") is not, so the syscall decides.
    for (let e: any = err, depth = 0; e && depth < 4; e = e.cause, depth++) {
        if (e.code === 'ETIMEDOUT' || e.code === 'ECONNREFUSED' || e.code === 'EHOSTUNREACH' || e.code === 'ENETUNREACH') {
            if (e.syscall === 'connect') return true;
            const attempts = Array.isArray(e.errors) ? e.errors : [];
            if (attempts.length > 0 && attempts.every((a: any) => a?.syscall === 'connect')) return true;
        }
    }
    return false;
}

function describeFetchError(err: unknown): string {
    return fetchErrorCode(err) ?? (err as Error)?.message ?? String(err);
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

// Shared by every API call of this process (storage clients, status message,
// metamorph, reboot), so throttling seen on one slows the others too.
const sharedRateLimiter = new AdaptiveRateLimiter();

export interface RetryingFetchOptions {
    /** Safe to re-send when the outcome is unknown (5xx, timeout, reset). */
    idempotent: boolean;
    /** Per-attempt timeout. */
    timeoutMs: number;
    /** Retry timeouts (idempotent calls only; a timed-out write may have been applied). */
    retryTimeouts?: boolean;
    /** Total time budget for retries; defaults to {@link apiRetryWindowMs}. */
    retryWindowMs?: number;
    /** Short label for log lines, e.g. "POST /datasets/x/items". */
    label: string;
    /**
     * Idempotency-Key for a write (one per logical write, the same on every
     * attempt). Once the API is known to de-duplicate keyed writes, the write
     * is retried like an idempotent call: a re-send of an applied write returns
     * the original answer instead of applying it again.
     */
    idempotencyKey?: string;
}

export interface RetryingFetchInit {
    method: string;
    headers: Record<string, string>;
    body?: BodyInit;
}

/**
 * fetch() with the API retry policy. Retries, with exponential backoff and
 * jitter that honours Retry-After, until the retry window (5 minutes by
 * default) runs out:
 * - 429 (run API rate limit, dataset backpressure): always; nothing was applied.
 * - 503 from the limiter or HAProxy: always; nothing was forwarded.
 * - 500/502/503/504, timeouts and dropped connections: idempotent calls, and
 *   writes with an Idempotency-Key once the API is known to de-duplicate them.
 * - Refused/unresolvable connections: always; the request never left.
 * Any other write (KV PUT, metamorph, a keyed push to an API that did not say
 * it de-duplicates) whose request may have reached the API is never re-sent:
 * a re-send could apply it twice. Such failures surface to the caller instead,
 * marked `outcomeUnknown` when an attempt may have been applied.
 * Non-2xx answers that are not retried throw ApiStorageClientError.
 */
export async function fetchWithRetries<R>(
    url: string,
    init: RetryingFetchInit,
    options: RetryingFetchOptions,
    decode: (response: Response) => Promise<R>,
): Promise<R> {
    const { timeoutMs, label, idempotencyKey } = options;
    const headers = idempotencyKey ? { ...init.headers, 'Idempotency-Key': idempotencyKey } : init.headers;
    // Re-sending is safe for idempotent calls, and for keyed writes once the
    // API showed it de-duplicates them (learned from any keyed answer).
    const safeToResend = () => options.idempotent || (idempotencyKey !== undefined && apiDedupesKeyedWrites(url));
    const retryTimeouts = () => options.retryTimeouts !== false && safeToResend();
    const deadline = Date.now() + Math.max(0, options.retryWindowMs ?? apiRetryWindowMs());
    let attempt = 0;
    // An attempt of a non-idempotent call may have been applied (timeout,
    // dropped connection, 5xx): the final error must not claim "nothing applied".
    let maybeApplied = false;
    const fail = <E>(err: E): E => (maybeApplied && !options.idempotent ? markOutcomeUnknown(err) : err);

    /** Sleeps before the next attempt; false once the retry window is used up. */
    const backoff = async (why: string, retryAfter: string | null): Promise<boolean> => {
        // Retrying sooner than asked would only be refused again: give up when
        // the next try would land past the window.
        const waitMs = serverRetryDelayMs(attempt, retryAfter);
        if (Date.now() + waitMs > deadline) return false;
        attempt++;
        console.warn(`[Scrapely] ${label} ${why}; retry ${attempt} in ${Math.round(waitMs)}ms`);
        await sleep(waitMs);
        return true;
    };

    for (;;) {
        await sharedRateLimiter.waitForSlot();
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
        try {
            const response = await fetch(url, { method: init.method, headers, body: init.body, signal: controller.signal });
            sharedRateLimiter.updateFromHeaders(response.headers);
            if (idempotencyKey) noteKeyedAnswer(url, response);

            if (response.ok) {
                try {
                    // Decoded inside the retry loop: a body cut off after
                    // the headers is a transport failure like any other.
                    return await decode(response);
                } catch (bodyErr) {
                    // The request was applied (2xx); a write whose answer
                    // timed out while being read must not be sent again.
                    maybeApplied = true;
                    if ((bodyErr as Error).name === 'AbortError') {
                        if (!retryTimeouts()) {
                            const timeoutError = new Error(`Request timed out after ${timeoutMs}ms (response body${options.idempotent ? '' : '; the request was applied'})`);
                            (timeoutError as any).isTimeout = true;
                            throw fail(timeoutError);
                        }
                        if (await backoff(`response body timed out after ${timeoutMs}ms`, null)) continue;
                        throw fail(bodyErr);
                    }
                    if (safeToResend() && await backoff(`response body failed (${describeFetchError(bodyErr)})`, null)) continue;
                    throw fail(bodyErr);
                }
            }

            const text = await response.text().catch(() => '');
            let errorBody: { error?: { message?: string; type?: string } } | undefined;
            try { errorBody = JSON.parse(text); } catch { errorBody = undefined; }
            const errorType = errorBody?.error?.type;
            const apiError = new ApiStorageClientError(
                errorBody?.error?.message || response.statusText || `API request failed: ${response.status}`,
                response.status,
            );
            if (idempotencyKey && errorType === OUTCOME_UNKNOWN) {
                // Stored or not, nobody knows: the API refuses repeats of this key.
                maybeApplied = true;
                throw fail(apiError);
            }
            // A 5xx other than the limiter's / HAProxy's 503 may come after the write was applied.
            if (!options.idempotent && response.status >= 500 && !isRetryableResponse(response.status, false, errorType, text)) {
                maybeApplied = true;
            }
            const resend = safeToResend();
            // 409 for a keyed write: an earlier attempt is still being stored.
            const inProgress = idempotencyKey !== undefined && response.status === 409 && errorType === KEY_IN_PROGRESS;
            if ((inProgress || isRetryableResponse(response.status, resend, errorType, text))
                && await backoff(`answered ${response.status}`, response.headers.get('retry-after'))) {
                continue;
            }
            if (inProgress) maybeApplied = true;
            throw fail(apiError);
        } catch (err) {
            // HTTP errors (already decided above) are final.
            if (isApiStorageClientError(err)) throw err;
            if ((err as any)?.isTimeout) throw err;
            if ((err as any)?.outcomeUnknown) throw err;

            if ((err as Error).name === 'AbortError') {
                maybeApplied = true;
                const timeoutError = new Error(`Request timed out after ${timeoutMs}ms`);
                (timeoutError as any).isTimeout = true;
                if (retryTimeouts() && await backoff(`timed out after ${timeoutMs}ms`, null)) continue;
                throw fail(timeoutError);
            }

            // fetch() failed without a response. A refused/unresolvable
            // connection never reached the API, so it is safe for every
            // method; anything else (reset mid-request) only when re-sending is safe.
            const connectFailure = isConnectFailure(err);
            if (!connectFailure) maybeApplied = true;
            if ((safeToResend() || connectFailure) && await backoff(`failed (${describeFetchError(err)})`, null)) continue;
            throw fail(err);
        } finally {
            clearTimeout(timeoutId);
        }
    }
}

/** Map GET storage errors: rethrow 403, return undefined for 404 (triggers getOrCreate for names only). */
function handleStorageGetError(err: unknown): undefined {
    if (isApiStorageClientError(err)) {
        if (err.statusCode === 403) throw err;
        if (err.statusCode === 404) return undefined;
    }
    throw err;
}

function storageCollectionHeaders(config: ApiStorageConfig): Record<string, string> {
    return {
        Authorization: `Bearer ${config.token}`,
        'X-Scrapely-Run-Id': config.runId,
        'Content-Type': 'application/json',
    };
}

/** POST /v2/{datasets|key-value-stores|request-queues} to create or get-by-name. */
async function postStorageCollection(
    config: ApiStorageConfig,
    path: string,
    name?: string,
): Promise<Record<string, unknown>> {
    const query = name ? `?name=${encodeURIComponent(name)}` : '';
    // Not marked repeatable even by name: a 500 after the create (e.g. scoped
    // token registration failed) must surface, since a re-send would take the
    // existing-name path and hide it. POST rules apply (limiter/HAProxy 503,
    // refused connection).
    return new CollectionApiClient(config).call<Record<string, unknown>>('POST', `${path}${query}`);
}

/**
 * Helper to create AsyncIterable + Promise result (matches Crawlee pattern)
 */
function makeAsyncIterableResult<T>(promiseFn: () => Promise<PaginatedList<T>>): AsyncIterable<T> & Promise<PaginatedList<T>> {
    const promise = promiseFn();
    const iterable = {
        [Symbol.asyncIterator]: () => {
            let items: T[] | null = null;
            let index = 0;
            return {
                async next() {
                    if (!items) items = (await promise).items;
                    if (index >= items.length) return { done: true as const, value: undefined };
                    return { done: false as const, value: items[index++] };
                },
            };
        },
        then: promise.then.bind(promise),
        catch: promise.catch.bind(promise),
        finally: promise.finally.bind(promise),
        [Symbol.toStringTag]: 'AsyncIterablePromise',
    } as any;
    return iterable;
}

/**
 * Options for API requests (passed through from RecordOptions)
 */
interface ApiRequestOptions {
    /** Timeout in seconds for the request */
    timeoutSecs?: number;
    /** If true, don't retry on timeout errors */
    doNotRetryTimeouts?: boolean;
    /** Override the method-based idempotency (e.g. get-or-create by name is safe to repeat). */
    idempotent?: boolean;
    /** Idempotency-Key for a write; see {@link RetryingFetchOptions.idempotencyKey}. */
    idempotencyKey?: string;
}

/**
 * Base class for API-based storage clients.
 * Communicates with internal storage API instead of direct DB access.
 * No sensitive credentials are stored - only the user's API key.
 */
export abstract class BaseApiClient {
    protected headers: Record<string, string>;
    
    // How long one call keeps retrying 429/503/network errors before it gives
    // up (SCRAPELY_API_RETRY_WINDOW_SECS overrides it). Long enough that a
    // throttled run slows down instead of failing, and covers an API redeploy.
    static retryWindowMs = DEFAULT_API_RETRY_WINDOW_MS;

    // Default timeout for requests (30 seconds)
    private static defaultTimeoutMs = 30000;

    constructor(protected config: ApiStorageConfig) {
        this.headers = {
            'Authorization': `Bearer ${config.token}`,
            'X-Scrapely-Run-Id': config.runId,
            'Content-Type': 'application/json',
        };
    }

    /**
     * Sends the request and returns the successful (2xx) response, with the
     * retry policy of {@link fetchWithRetries}.
     */
    private async send<R>(
        method: string,
        path: string,
        body: any,
        contentType: string | undefined,
        options: ApiRequestOptions | undefined,
        decode: (response: Response) => Promise<R>,
    ): Promise<R> {
        const headers = { ...this.headers };
        if (contentType) {
            headers['Content-Type'] = contentType;
        }
        const timeoutMs = options?.timeoutSecs
            ? options.timeoutSecs * 1000
            : BaseApiClient.defaultTimeoutMs;
        return fetchWithRetries(
            `${this.config.apiUrl}/v2${path}`,
            {
                method,
                headers,
                // Binary bodies (KV records) go out as the raw bytes, never JSON/base64.
                body: body === undefined ? undefined
                    : typeof body === 'string' || body instanceof Uint8Array ? (body as BodyInit)
                    : JSON.stringify(body),
            },
            {
                idempotent: options?.idempotent ?? IDEMPOTENT_METHODS.has(method.toUpperCase()),
                timeoutMs,
                retryTimeouts: options?.doNotRetryTimeouts !== true,
                label: `${method} ${path}`,
                idempotencyKey: options?.idempotencyKey,
            },
            decode,
        );
    }

    protected async request<T>(
        method: string,
        path: string,
        body?: any,
        contentType?: string,
        options?: ApiRequestOptions
    ): Promise<T> {
        return this.send(method, path, body, contentType, options, async (response): Promise<T> => {
            // Handle empty responses (204 No Content)
            if (response.status === 204) {
                return undefined as T;
            }

            // Handle text/plain responses
            const responseContentType = response.headers.get('content-type') || '';
            if (responseContentType.includes('text/plain')) {
                return (await response.text()) as T;
            }

            // Handle binary responses
            if (responseContentType.includes('application/octet-stream')) {
                return Buffer.from(await response.arrayBuffer()) as T;
            }

            return unwrapData<T>(await response.json());
        });
    }

    /** HEAD request: true on 2xx, false on 404; other errors throw. */
    protected async exists(path: string): Promise<boolean> {
        try {
            return await this.send('HEAD', path, undefined, undefined, undefined, async () => true);
        } catch (err) {
            if (isApiStorageClientError(err) && err.statusCode === 404) return false;
            throw err;
        }
    }

    protected async requestWithHeaders<T>(
        method: string,
        path: string,
        body?: any,
        contentType?: string,
    ): Promise<{ data: T; contentType: string }> {
        return this.send(method, path, body, contentType, undefined, async (response): Promise<{ data: T; contentType: string }> => {
            if (response.status === 204) {
                return { data: undefined as T, contentType: '' };
            }

            const responseContentType = response.headers.get('content-type') || 'application/json';

            const mime = responseContentType.split(';')[0].trim().toLowerCase() || 'application/json';

            if (mime === 'application/json' || mime.endsWith('+json')) {
                return { data: await response.json() as T, contentType: mime };
            }

            if (mime.startsWith('text/')) {
                return { data: await response.text() as T, contentType: mime };
            }

            // Any other content type (video/mp4, image/png, application/zip, ...) is binary.
            return { data: Buffer.from(await response.arrayBuffer()) as T, contentType: mime };
        });
    }
}

/** Collection-level calls (create/get by name, list) with the same retry policy. */
class CollectionApiClient extends BaseApiClient {
    call<T>(method: string, path: string, options?: ApiRequestOptions): Promise<T> {
        return this.request<T>(method, path, undefined, undefined, options);
    }
}

/**
 * Extended options for listing dataset items with filtering support.
 * Matches Crawlee's DatasetDataOptions interface.
 */
interface ExtendedDatasetListOptions extends DatasetClientListOptions {
    /** An array of field names that will be included in the result. */
    fields?: string[];
    /** If true then the function returns only non-empty items and skips hidden fields. */
    clean?: boolean;
    /** If true then the function doesn't return hidden fields (fields starting with "#"). */
    skipHidden?: boolean;
    /** If true then the function doesn't return empty items. */
    skipEmpty?: boolean;
}

/**
 * Dataset client that communicates via internal API
 */
export class ApiDatasetClient<Data extends Dictionary = Dictionary> extends BaseApiClient implements DatasetClient<Data> {
    private id: string;

    constructor(config: ApiStorageConfig, datasetId: string) {
        super(config);
        this.id = datasetId;
    }

    async get(): Promise<DatasetInfo | undefined> {
        if (!isStorageUuid(this.id)) return undefined;
        try {
            const data = await this.request<Record<string, unknown>>('GET', `/datasets/${this.id}`);
            return {
                id: String(data.id ?? this.id),
                name: (data.name as string | null) ?? undefined,
                createdAt: parseApiDate(data.createdAt),
                modifiedAt: parseApiDate(data.modifiedAt),
                accessedAt: parseApiDate(data.accessedAt),
                itemCount: typeof data.itemCount === 'number'
                    ? data.itemCount
                    : parseInt(String(data.itemCount ?? '0'), 10),
            };
        } catch (err) {
            return handleStorageGetError(err);
        }
    }

    async update(newFields: DatasetClientUpdateOptions): Promise<Partial<DatasetInfo>> {
        throw new Error('Dataset rename not supported from container');
    }

    async delete(): Promise<void> {
        await this.request('DELETE', `/datasets/${this.id}`);
    }

    async downloadItems(): Promise<Buffer> {
        const result = await this._listItems();
        return Buffer.from(JSON.stringify(result.items));
    }

    listItems(options?: ExtendedDatasetListOptions): AsyncIterable<Data> & Promise<PaginatedList<Data>> {
        return makeAsyncIterableResult(() => this._listItems(options));
    }

    /**
     * Iterates over dataset entries (index-value pairs) using an async generator.
     * Required by Crawlee's Dataset.entries() method.
     */
    listEntries(options?: ExtendedDatasetListOptions): AsyncIterable<[number, Data]> & Promise<PaginatedList<[number, Data]>> {
        return makeAsyncIterableResult(async () => {
            const result = await this._listItems(options);
            const entries: [number, Data][] = result.items.map((item, idx) => [
                (result.offset ?? 0) + idx,
                item,
            ]);
            return {
                total: result.total,
                count: entries.length,
                offset: result.offset,
                limit: result.limit,
                items: entries,
            };
        });
    }

    private async _listItems(options?: ExtendedDatasetListOptions): Promise<PaginatedList<Data>> {
        const params = new URLSearchParams();
        const offset = options?.offset ?? 0;
        const limit = options?.limit;
        if (options?.offset) params.set('offset', String(options.offset));
        if (options?.limit) params.set('limit', String(options.limit));
        if (options?.desc) params.set('desc', 'true');
        if (options?.fields) params.set('fields', options.fields.join(','));
        if (options?.clean) params.set('clean', 'true');
        if (options?.skipHidden) params.set('skipHidden', 'true');
        if (options?.skipEmpty) params.set('skipEmpty', 'true');

        const queryString = params.toString();
        const path = `/datasets/${this.id}/items${queryString ? `?${queryString}` : ''}`;

        const result = await this.request<Data[] | PaginatedList<Data>>('GET', path);

        // v2 returns a bare JSON array; internal API returned { total, count, offset, limit, items }
        if (Array.isArray(result)) {
            return {
                total: result.length,
                count: result.length,
                offset,
                limit: limit ?? result.length,
                items: result,
            };
        }

        return result;
    }

    async pushItems(items: Data | Data[] | string | string[]): Promise<void> {
        let parsed: Data[];
        if (typeof items === 'string') {
            parsed = JSON.parse(items);
            if (!Array.isArray(parsed)) parsed = [parsed];
        } else if (Array.isArray(items)) {
            parsed = items.map(i => (typeof i === 'string' ? JSON.parse(i) : i));
        } else {
            parsed = [items];
        }

        // Charge-limit safety net: a pay-per-event run past its spending limit
        // must not store unbilled rows in its default dataset.
        if (this.id === defaultDatasetIdFromEnv() && refuseDefaultDatasetPush(parsed.length)) return;

        // One key per push, the same on every attempt: the API stores a
        // re-sent push once, so a push whose connection dropped can be retried.
        await this.request('POST', `/datasets/${this.id}/items`, parsed, undefined, { idempotencyKey: randomUUID() });
    }
}

/**
 * Key-Value Store client that communicates via internal API
 */
export class ApiKeyValueStoreClient extends BaseApiClient implements KeyValueStoreClient {
    private id: string;

    constructor(config: ApiStorageConfig, storeId: string) {
        super(config);
        this.id = storeId;
    }

    async get(): Promise<KeyValueStoreInfo | undefined> {
        if (!isStorageUuid(this.id)) return undefined;
        try {
            const data = await this.request<Record<string, unknown>>('GET', `/key-value-stores/${this.id}`);
            return {
                id: String(data.id ?? this.id),
                name: (data.name as string | null) ?? undefined,
                createdAt: parseApiDate(data.createdAt),
                modifiedAt: parseApiDate(data.modifiedAt),
                accessedAt: parseApiDate(data.accessedAt),
            };
        } catch (err) {
            return handleStorageGetError(err);
        }
    }

    async update(newFields: KeyValueStoreClientUpdateOptions): Promise<Partial<KeyValueStoreInfo>> {
        throw new Error('KVS rename not supported from container');
    }

    async delete(): Promise<void> {
        if (!isStorageUuid(this.id)) return;
        await this.request('DELETE', `/key-value-stores/${this.id}`);
    }

    async recordExists(key: string): Promise<boolean> {
        return this.exists(`/key-value-stores/${this.id}/records/${pathSegment(key)}`);
    }

    async getRecord(key: string, options?: KeyValueStoreClientGetRecordOptions): Promise<KeyValueStoreRecord | undefined> {
        try {
            const { data, contentType } = await this.requestWithHeaders<unknown>(
                'GET',
                `/key-value-stores/${this.id}/records/${pathSegment(key)}`,
            );

            if (data === undefined || data === null) return undefined;

            let value = data;

            if (options?.buffer && !Buffer.isBuffer(value)) {
                value = Buffer.from(typeof value === 'string' ? value : JSON.stringify(value));
            }

            return { key, value, contentType: contentType || 'application/json' };
        } catch (err) {
            // Missing record only; anything else (API unavailable after the
            // retries) must not look like "no record" (lost state, restarts).
            if (isApiStorageClientError(err) && err.statusCode === 404) return undefined;
            throw err;
        }
    }

    async setRecord(record: KeyValueStoreRecord, options?: KeyValueStoreRecordOptions): Promise<void> {
        const { key, value, contentType } = record;
        
        let body: any;
        let ct: string;

        // Handle Readable streams (e.g., fs.createReadStream)
        // Streams are objects with a pipe method. We need to buffer them.
        if (value && typeof value === 'object' && typeof (value as any).pipe === 'function') {
            const chunks: Buffer[] = [];
            for await (const chunk of value) {
                chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
            }
            body = Buffer.concat(chunks);
            ct = 'application/octet-stream';
        } else if (value instanceof Uint8Array) {
            // Raw bytes, stored as-is; the API returns them unchanged on GET.
            body = value;
            ct = 'application/octet-stream';
        } else if (value instanceof ArrayBuffer) {
            body = Buffer.from(value);
            ct = 'application/octet-stream';
        } else if (typeof value === 'string') {
            body = value;
            ct = 'text/plain';
        } else {
            body = value;
            ct = 'application/json';
        }

        // Pass timeout/retry options from KeyValueStoreRecordOptions to the API request
        const requestOptions: ApiRequestOptions | undefined = options ? {
            timeoutSecs: (options as any).timeoutSecs,
            doNotRetryTimeouts: (options as any).doNotRetryTimeouts,
        } : undefined;

        await this.request('PUT', `/key-value-stores/${this.id}/records/${pathSegment(key)}`, body, contentType || ct, requestOptions);
    }

    async deleteRecord(key: string): Promise<void> {
        await this.request('DELETE', `/key-value-stores/${this.id}/records/${pathSegment(key)}`);
    }

    async listKeys(options?: KeyValueStoreClientListOptions): Promise<KeyValueStoreClientListData> {
        const params = new URLSearchParams();
        if (options?.prefix) params.set('prefix', options.prefix);
        if (options?.limit) params.set('limit', String(options.limit));
        if (options?.exclusiveStartKey) params.set('exclusiveStartKey', options.exclusiveStartKey);
        if (options?.collection) params.set('collection', options.collection);

        const queryString = params.toString();
        const path = `/key-value-stores/${this.id}/keys${queryString ? `?${queryString}` : ''}`;
        
        return this.request<KeyValueStoreClientListData>('GET', path);
    }

    keys(options?: KeyValueStoreClientListOptions): AsyncIterable<string> & Promise<KeyValueStoreClientListData> {
        return makePaginatingKeysIterable((opts) => this.listKeys(opts), options);
    }

    values(options?: KeyValueStoreClientListOptions): AsyncIterable<unknown> & Promise<unknown[]> {
        return makePaginatingValuesIterable(
            (opts) => this.listKeys(opts),
            (key) => this.getRecord(key),
            options,
        );
    }

    entries(options?: KeyValueStoreClientListOptions): AsyncIterable<[string, unknown]> & Promise<[string, unknown][]> {
        return makePaginatingEntriesIterable(
            (opts) => this.listKeys(opts),
            (key) => this.getRecord(key),
            options,
        );
    }

    /**
     * Iterates over all keys in the key-value store, calling the iteratee function for each key.
     */
    async forEachKey(iteratee: (key: string, index: number, info: { size: number }) => Promise<void> | void, options?: KeyValueStoreClientListOptions): Promise<void> {
        return forEachKeyPaginated((opts) => this.listKeys(opts), iteratee, options);
    }

    /**
     * Gets a value from the key-value store that is automatically saved when modified.
     * If the key doesn't exist, returns the defaultValue and stores it for future use.
     * 
     * @param key - The key of the record to get
     * @param defaultValue - The default value to return and store if the key doesn't exist
     * @returns The stored value or the defaultValue
     */
    async getAutoSavedValue<T>(key: string, defaultValue: T): Promise<T> {
        const record = await this.getRecord(key);
        if (record === undefined) {
            // Store the default value and return it
            await this.setRecord({ key, value: defaultValue as any });
            return defaultValue;
        }
        return record.value as T;
    }

    /**
     * Returns a URL for the given key that may be used to publicly access the value
     * in the remote key-value store.
     * 
     * @param key - The key of the record to get the public URL for
     * @returns A public URL for accessing the record
     */
    getPublicUrl(key: string): string {
        // apiUrl is the container-internal API address; links handed to users
        // must use the public base URL the platform injects.
        const publicBase = (process.env.APIFY_API_PUBLIC_BASE_URL || process.env.ACTOR_API_PUBLIC_BASE_URL || this.config.apiUrl).replace(/\/+$/, '');
        return `${publicBase}/v2/key-value-stores/${this.id}/records/${pathSegment(key)}`;
    }
}

/**
 * Request Queue client that communicates via internal API
 */
export class ApiRequestQueueClient extends BaseApiClient implements RequestQueueClient {
    private id: string;
    private readonly clientKey: string;

    constructor(config: ApiStorageConfig, queueId: string, options?: RequestQueueOptions) {
        super(config);
        this.id = queueId;
        this.clientKey = options?.clientKey || randomUUID().replace(/-/g, '').slice(0, 16);
    }

    async get(): Promise<RequestQueueInfo | undefined> {
        if (!isStorageUuid(this.id)) return undefined;
        try {
            const result = await this.request<Record<string, unknown>>('GET', `/request-queues/${this.id}`);
            return mapRequestQueueInfo(this.id, result);
        } catch (err) {
            return handleStorageGetError(err);
        }
    }

    async update(newFields: { name?: string }): Promise<Partial<RequestQueueInfo> | undefined> {
        if (!isStorageUuid(this.id)) return undefined;
        const result = await this.request<RequestQueueInfo>('PUT', `/request-queues/${this.id}`, newFields);
        return { name: result.name ?? newFields.name };
    }

    async delete(): Promise<void> {
        if (!isStorageUuid(this.id)) return;
        await this.request('DELETE', `/request-queues/${this.id}`);
    }

    async addRequest(request: RequestSchema, options?: RequestOptions): Promise<QueueOperationInfo> {
        const query = buildRqQueryParams(this.clientKey, { forefront: options?.forefront });
        return this.request<QueueOperationInfo>('POST', `/request-queues/${this.id}/requests${query}`, request);
    }

    async batchAddRequests(requests: RequestSchema[], options?: RequestOptions): Promise<BatchAddRequestsResult> {
        const processedRequests: BatchAddRequestsResult['processedRequests'] = [];
        const unprocessedRequests: BatchAddRequestsResult['unprocessedRequests'] = [];
        const BATCH_SIZE = 25;

        for (let i = 0; i < requests.length; i += BATCH_SIZE) {
            const chunk = requests.slice(i, i + BATCH_SIZE);
            try {
                const query = buildRqQueryParams(this.clientKey, { forefront: options?.forefront });
                const result = await this.request<BatchAddRequestsResult>(
                    'POST',
                    `/request-queues/${this.id}/requests/batch${query}`,
                    chunk,
                );
                processedRequests.push(...result.processedRequests);
                unprocessedRequests.push(...result.unprocessedRequests);
            } catch {
                for (const request of chunk) {
                    unprocessedRequests.push({
                        uniqueKey: request.uniqueKey,
                        url: request.url,
                        method: request.method,
                    });
                }
            }
        }

        return { processedRequests, unprocessedRequests };
    }

    async getRequest(id: string): Promise<RequestOptions | undefined> {
        try {
            return await this.request<RequestOptions>('GET', `/request-queues/${this.id}/requests/${pathSegment(id)}`);
        } catch {
            return undefined;
        }
    }

    async updateRequest(request: UpdateRequestSchema, options?: RequestOptions): Promise<QueueOperationInfo> {
        const query = buildRqQueryParams(this.clientKey, { forefront: options?.forefront });
        return this.request<QueueOperationInfo>(
            'PUT',
            `/request-queues/${this.id}/requests/${pathSegment(request.id)}${query}`,
            request,
        );
    }

    async deleteRequest(id: string): Promise<void> {
        await this.request('DELETE', `/request-queues/${this.id}/requests/${pathSegment(id)}`);
    }

    async listHead(options?: ListOptions): Promise<QueueHead> {
        const query = buildRqQueryParams(this.clientKey, { limit: options?.limit });
        const result = await this.request<{
            items: RequestQueueHeadItem[];
            limit: number;
            hadMultipleClients: boolean;
            queueModifiedAt?: string;
        }>('GET', `/request-queues/${this.id}/head${query}`);

        return {
            limit: result.limit,
            queueModifiedAt: result.queueModifiedAt ? new Date(result.queueModifiedAt) : new Date(),
            hadMultipleClients: result.hadMultipleClients,
            items: result.items,
        };
    }

    async listAndLockHead(options: ListAndLockOptions): Promise<ListAndLockHeadResult> {
        const query = buildRqQueryParams(this.clientKey, {
            limit: options.limit || 25,
            lockSecs: options.lockSecs,
        });
        const result = await this.request<ListAndLockHeadResult & { queueModifiedAt?: string }>(
            'POST',
            `/request-queues/${this.id}/head/lock${query}`,
        );

        return {
            ...result,
            queueModifiedAt: result.queueModifiedAt ? new Date(result.queueModifiedAt) : new Date(),
        };
    }

    async prolongRequestLock(id: string, options: ProlongRequestLockOptions): Promise<ProlongRequestLockResult> {
        const query = buildRqQueryParams(this.clientKey, {
            lockSecs: options.lockSecs,
            forefront: options.forefront,
        });
        const result = await this.request<{ lockExpiresAt: string }>(
            'PUT',
            `/request-queues/${this.id}/requests/${pathSegment(id)}/lock${query}`,
        );
        return { lockExpiresAt: new Date(result.lockExpiresAt) };
    }

    async deleteRequestLock(id: string, options?: DeleteRequestLockOptions): Promise<void> {
        const query = buildRqQueryParams(this.clientKey, { forefront: options?.forefront });
        await this.request('DELETE', `/request-queues/${this.id}/requests/${pathSegment(id)}/lock${query}`);
    }

    /**
     * List requests in the queue with optional filtering and pagination.
     */
    async listRequests(options?: ListRequestsOptions): Promise<ListRequestsResult> {
        const params = new URLSearchParams();
        params.set('clientKey', options?.clientKey ?? this.clientKey);
        if (options?.limit !== undefined) params.set('limit', String(options.limit));
        if (options?.cursor) params.set('cursor', options.cursor);
        if (options?.exclusiveStartId) params.set('exclusiveStartId', options.exclusiveStartId);
        if (options?.filter?.length) {
            for (const f of options.filter) params.append('filter', f);
        }
        const query = params.toString();
        return this.request<ListRequestsResult>(
            'GET',
            `/request-queues/${this.id}/requests${query ? `?${query}` : ''}`,
        );
    }

    /**
     * Unlock all requests locked by this clientKey.
     */
    async unlockRequests(options: {
        clientKey?: string;
        forefront?: boolean;
        runId?: string;
    }): Promise<{ unlockedCount: number }> {
        const query = buildRqQueryParams(options.clientKey ?? this.clientKey, {
            forefront: options.forefront,
            runId: options.runId,
        });
        return this.request<{ unlockedCount: number }>(
            'POST',
            `/request-queues/${this.id}/requests/unlock${query}`,
        );
    }
}

/**
 * Dataset collection client (API-based)
 */
class ApiDatasetCollectionClient {
    constructor(private config: ApiStorageConfig) {}

    async getOrCreate(name?: string): Promise<DatasetInfo> {
        const data = await postStorageCollection(this.config, '/datasets', name);
        return {
            id: String(data.id),
            name: (data.name as string | null) ?? name,
            createdAt: parseApiDate(data.createdAt),
            modifiedAt: parseApiDate(data.modifiedAt),
            accessedAt: parseApiDate(data.accessedAt),
            itemCount: typeof data.itemCount === 'number'
                ? data.itemCount
                : parseInt(String(data.itemCount ?? '0'), 10),
        };
    }

    async list(): Promise<PaginatedList<DatasetInfo>> {
        // Containers don't list all datasets - return empty
        return { total: 0, count: 0, offset: 0, limit: 0, items: [] };
    }
}

/**
 * Key-value store collection client (API-based)
 */
class ApiKeyValueStoreCollectionClient {
    constructor(private config: ApiStorageConfig) {}

    async getOrCreate(name?: string): Promise<KeyValueStoreInfo> {
        const data = await postStorageCollection(this.config, '/key-value-stores', name);
        return {
            id: String(data.id),
            name: (data.name as string | null) ?? name,
            createdAt: parseApiDate(data.createdAt),
            modifiedAt: parseApiDate(data.modifiedAt),
            accessedAt: parseApiDate(data.accessedAt),
        };
    }

    async list(): Promise<PaginatedList<KeyValueStoreInfo>> {
        return { total: 0, count: 0, offset: 0, limit: 0, items: [] };
    }
}

/**
 * Request queue collection client (API-based)
 */
class ApiRequestQueueCollectionClient {
    constructor(private config: ApiStorageConfig) {}

    async getOrCreate(name?: string): Promise<RequestQueueInfo> {
        const data = await postStorageCollection(this.config, '/request-queues', name);
        return {
            id: String(data.id),
            name: (data.name as string | null) ?? name,
            createdAt: parseApiDate(data.createdAt),
            modifiedAt: parseApiDate(data.modifiedAt),
            accessedAt: parseApiDate(data.accessedAt),
            totalRequestCount: typeof data.totalRequestCount === 'number'
                ? data.totalRequestCount
                : parseInt(String(data.totalRequestCount ?? '0'), 10),
            handledRequestCount: typeof data.handledRequestCount === 'number'
                ? data.handledRequestCount
                : parseInt(String(data.handledRequestCount ?? '0'), 10),
            pendingRequestCount: typeof data.pendingRequestCount === 'number'
                ? data.pendingRequestCount
                : parseInt(String(data.pendingRequestCount ?? '0'), 10),
            hadMultipleClients: data.hadMultipleClients === true || data.hadMultipleClients === '1',
        };
    }

    async list(): Promise<PaginatedList<RequestQueueInfo>> {
        const data = await new CollectionApiClient(this.config).call<PaginatedList<Record<string, unknown>>>('GET', '/request-queues');
        return {
            total: data.total,
            count: data.count,
            offset: data.offset,
            limit: data.limit,
            items: data.items.map((item) => mapRequestQueueInfo(String(item.id), item)),
        };
    }
}

/**
 * Factory for creating API-based storage clients.
 * Implements StorageClient interface for Crawlee compatibility.
 */
export class ApiStorageClientFactory implements StorageClient {
    constructor(private config: ApiStorageConfig) {}

    datasets(): ApiDatasetCollectionClient {
        return new ApiDatasetCollectionClient(this.config);
    }

    dataset<Data extends Dictionary = Dictionary>(id: string): DatasetClient<Data> {
        return new ApiDatasetClient<Data>(this.config, id);
    }

    keyValueStores(): ApiKeyValueStoreCollectionClient {
        return new ApiKeyValueStoreCollectionClient(this.config);
    }

    keyValueStore(id: string): KeyValueStoreClient {
        return new ApiKeyValueStoreClient(this.config, id);
    }

    requestQueues(): ApiRequestQueueCollectionClient {
        return new ApiRequestQueueCollectionClient(this.config);
    }

    requestQueue(id: string, options?: RequestQueueOptions): RequestQueueClient {
        return new ApiRequestQueueClient(this.config, id, options);
    }

    async purge(): Promise<void> {
        // Containers don't purge storage
    }

    async teardown(): Promise<void> {
        // no-op: cleanup handled by server
    }

    async setStatusMessage(message: string): Promise<void> {
        console.log(`[Scrapely] Status: ${message}`);
    }
}
