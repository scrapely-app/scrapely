import type Redis from 'ioredis';

/** Full request object as returned by listRequests endpoint */
export interface RequestQueueRequest {
    id: string;
    retryCount: number;
    uniqueKey: string;
    url: string;
    method: string;
    loadedUrl?: string | null;
    payload?: string | null;
    noRetry?: boolean;
    errorMessages?: string[] | null;
    headers?: Record<string, string> | null;
    userData?: Record<string, unknown> | null;
    handledAt?: string | null;
}

/** Per-queue Redis keys read by listRequestsPage(). */
export interface RqListRequestsKeys {
    requestsKey: string;
    pendingKey: string;
    lockedKey: string;
}

export interface RqListRequestsParams {
    now: number;
    limit: number;
    /** Cursor / deprecated exclusiveStartId: all IDs <= this one are skipped. */
    startId?: string;
    filter?: readonly ('locked' | 'pending')[];
}

export interface RqListRequestsPage {
    items: RequestQueueRequest[];
    /** True when IDs remain past this page (drives nextCursor). */
    hasMore: boolean;
}

/**
 * Index of the first element strictly greater than `target` in a
 * lexicographically sorted array (binary search).
 *
 * Unlike indexOf(), it returns the correct position even when `target` itself
 * is absent, so a cursor item deleted between pages advances pagination past
 * its old position instead of restarting from page 1 (which used to loop
 * pagination clients back to the first page forever). This also matches the
 * documented exclusiveStartId semantics: "all requests up to this one
 * (including) are skipped from the result".
 */
export function upperBoundSorted(sortedIds: readonly string[], target: string): number {
    let lo = 0;
    let hi = sortedIds.length;
    while (lo < hi) {
        const mid = (lo + hi) >>> 1;
        if (sortedIds[mid]! <= target) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

/** Map a stored request JSON blob to the API response shape. */
function parseStoredRequest(raw: string): RequestQueueRequest {
    const req = JSON.parse(raw);
    return {
        id: req.id,
        retryCount: req.retryCount ?? 0,
        uniqueKey: req.uniqueKey,
        url: req.url,
        method: req.method ?? 'GET',
        loadedUrl: req.loadedUrl ?? null,
        payload: req.payload ?? null,
        noRetry: req.noRetry ?? false,
        errorMessages: req.errorMessages ?? null,
        headers: req.headers ?? null,
        userData: req.userData ?? null,
        handledAt: req.handledAt ?? null,
    };
}

/**
 * One page of request-queue listing: filtering + cursor pagination in two
 * round-trips, shared by the storage client and the API server route.
 *
 * Round-trip 1 — a single pipeline:
 *   - HKEYS: all request IDs (field names only, not the bodies);
 *   - ZRANGEBYSCORE locked (now +inf — only when filtering by 'locked'. The
 *     locked ZSET is scored by lockUntil ms, so one exclusive-bound range read
 *     exactly replaces the old per-id ZSCORE loop (score > now);
 *   - ZRANGE pending 0 -1 — only when filtering by 'pending'.
 * Round-trip 2 — one HMGET for the page's request bodies.
 */
export async function listRequestsPage(
    redis: Redis,
    keys: RqListRequestsKeys,
    params: RqListRequestsParams,
): Promise<RqListRequestsPage> {
    const hasLockedFilter = params.filter?.includes('locked') ?? false;
    const hasPendingFilter = params.filter?.includes('pending') ?? false;

    const readPipeline = redis.pipeline();
    readPipeline.hkeys(keys.requestsKey);
    if (hasLockedFilter) readPipeline.zrangebyscore(keys.lockedKey, `(${params.now}`, '+inf');
    // ioredis 6 types: stop must be string | Buffer (not number)
    if (hasPendingFilter) readPipeline.zrange(keys.pendingKey, 0, '-1');
    const results = await readPipeline.exec();

    const at = (i: number): string[] => {
        const value = results?.[i]?.[1];
        return Array.isArray(value) ? (value as string[]) : [];
    };

    const allRequestIds = at(0);
    // Sort IDs for consistent lexicographic ordering (API contract)
    allRequestIds.sort();

    const activeLockedIds = hasLockedFilter ? new Set(at(1)) : new Set<string>();
    const pendingIds = hasPendingFilter
        ? new Set(at(hasLockedFilter ? 2 : 1))
        : new Set<string>();

    // Filter requests based on filter parameter
    let filteredIds = allRequestIds;
    if (hasLockedFilter && hasPendingFilter) {
        // Both filters: return locked OR pending (not handled-only requests)
        filteredIds = allRequestIds.filter(id => activeLockedIds.has(id) || pendingIds.has(id));
    } else if (hasLockedFilter) {
        // Only locked: requests with non-expired locks
        filteredIds = allRequestIds.filter(id => activeLockedIds.has(id));
    } else if (hasPendingFilter) {
        // Only pending: requests in pending set
        filteredIds = allRequestIds.filter(id => pendingIds.has(id));
    }
    // If no filter, return all requests

    // Advance past the cursor (first ID strictly greater than startId —
    // see upperBoundSorted for why this must not be indexOf()).
    const startIndex = params.startId ? upperBoundSorted(filteredIds, params.startId) : 0;

    // Apply pagination
    const paginatedIds = filteredIds.slice(startIndex, startIndex + params.limit);
    const hasMore = startIndex + params.limit < filteredIds.length;

    // Fetch full request data for the page in one round-trip
    const items: RequestQueueRequest[] = [];
    if (paginatedIds.length > 0) {
        const raws = await redis.hmget(keys.requestsKey, ...paginatedIds);
        for (const raw of raws) {
            if (!raw) continue;
            items.push(parseStoredRequest(raw));
        }
    }

    return { items, hasMore };
}
