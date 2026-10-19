import type {
    RequestQueueClient, RequestQueueInfo, QueueHead, QueueOperationInfo,
    RequestSchema, UpdateRequestSchema, RequestOptions, BatchAddRequestsResult,
    ListOptions, ListAndLockOptions, ListAndLockHeadResult, RequestQueueHeadItem,
    ProlongRequestLockOptions, ProlongRequestLockResult, DeleteRequestLockOptions,
} from '@crawlee/types';

// RequestQueueRequest now lives in rq-list-requests.ts, next to the shared
// listing/pagination logic. Re-exported here to preserve the import surface.
export type { RequestQueueRequest } from './rq-list-requests.js';

/** Options for listRequests endpoint */
export interface ListRequestsOptions {
    limit?: number;
    cursor?: string;
    exclusiveStartId?: string;
    filter?: ('locked' | 'pending')[];
    clientKey?: string;
}

/** Result from listRequests endpoint */
export interface ListRequestsResult {
    items: RequestQueueRequest[];
    count: number;
    limit: number;
    exclusiveStartId?: string;
    cursor?: string;
    nextCursor?: string;
}
import type { DragonflyClient } from './dragonfly-client.js';
import type { RequestQueueOptions } from '@crawlee/types';
import { uniqueKeyToRequestId, nowISO } from './utils.js';
import { BillingTracker } from './billing-tracker.js';
import { listAndLockHeadAtomic, matchesLockOwner, unlockRequestsAtomic } from './rq-lock-script.js';
import { listRequestsPage, type RequestQueueRequest } from './rq-list-requests.js';
import { randomUUID } from 'node:crypto';

export class DragonflyRequestQueueClient implements RequestQueueClient {
    private infoKey: string;
    private requestsKey: string;
    private pendingKey: string;
    private lockedKey: string;
    private lockOwnersKey: string;
    private readonly clientKey: string;

    constructor(private client: DragonflyClient, private id: string, options?: RequestQueueOptions) {
        this.clientKey = options?.clientKey || randomUUID().replace(/-/g, '').slice(0, 16);
        this.infoKey = client.key('rq', id, 'info');
        this.requestsKey = client.key('rq', id, 'requests');
        this.pendingKey = client.key('rq', id, 'pending');
        this.lockedKey = client.key('rq', id, 'locked');
        this.lockOwnersKey = client.key('rq', id, 'lockOwners');
    }

    private estimateStoredRequestBytes(raw: string | null): number {
        return raw ? Buffer.byteLength(raw, 'utf8') : 0;
    }

    /**
     * Track a write operation for billing.
     */
    private async trackWrite(count: number = 1): Promise<void> {
        if (this.client.billingContext) {
            const billingTracker = new BillingTracker(this.client);
            await billingTracker.trackWrite(this.client.billingContext.userId, count);
            await this.client.redis.hincrby(this.infoKey, 'writeCount', count);
            // Register this queue as accessed for per-run aggregation
            await this.client.redis.sadd(
                this.client.key('run', this.client.billingContext.runId, 'rqs_accessed'),
                this.id
            );
        }
    }

    /**
     * Track a read operation for billing.
     */
    private async trackRead(count: number = 1, bytesTransferred: number = 0): Promise<void> {
        if (this.client.billingContext) {
            const billingTracker = new BillingTracker(this.client);
            await billingTracker.trackRead(this.client.billingContext.userId, count);
            await this.client.redis.hincrby(this.infoKey, 'readCount', count);
            
            // Track internal data transfer (bytes returned to actor)
            if (bytesTransferred > 0) {
                await billingTracker.trackInternalTransfer(this.client.billingContext.runId, this.client.billingContext.userId, bytesTransferred);
            }
            
            // Register this queue as accessed for per-run aggregation
            await this.client.redis.sadd(
                this.client.key('run', this.client.billingContext.runId, 'rqs_accessed'),
                this.id
            );
        }
    }

    /**
     * Track storage bytes for billing.
     */
    private async trackStorageBytes(bytes: number): Promise<void> {
        if (this.client.billingContext) {
            const billingTracker = new BillingTracker(this.client);
            await billingTracker.trackStorageBytes(
                this.client.billingContext.runId,
                this.client.billingContext.userId,
                bytes
            );
        }
    }

    /**
     * Track clientKey access for hadMultipleClients.
     * Returns the current hadMultipleClients flag after update.
     */
    private async trackClientKey(clientKey?: string): Promise<boolean> {
        if (clientKey) {
            const existingClientKey = await this.client.redis.hget(this.infoKey, 'clientKey');
            if (existingClientKey && existingClientKey !== clientKey) {
                await this.client.redis.hset(this.infoKey, 'hadMultipleClients', '1');
            } else if (!existingClientKey) {
                await this.client.redis.hset(this.infoKey, 'clientKey', clientKey);
            }
        } else {
            const existingClientKey = await this.client.redis.hget(this.infoKey, 'clientKey');
            if (existingClientKey) {
                await this.client.redis.hset(this.infoKey, 'hadMultipleClients', '1');
            }
        }
        const flag = await this.client.redis.hget(this.infoKey, 'hadMultipleClients');
        return flag === '1';
    }

    async get(): Promise<RequestQueueInfo | undefined> {
        const info = await this.client.redis.hgetall(this.infoKey);
        if (!info || !info.id) return undefined;
        return {
            id: info.id,
            name: info.name || undefined,
            userId: info.userId,
            createdAt: new Date(info.createdAt),
            modifiedAt: new Date(info.modifiedAt),
            accessedAt: new Date(info.accessedAt),
            totalRequestCount: parseInt(info.totalRequestCount || '0', 10),
            handledRequestCount: parseInt(info.handledRequestCount || '0', 10),
            pendingRequestCount: parseInt(info.pendingRequestCount || '0', 10),
            hadMultipleClients: info.hadMultipleClients === '1',
            stats: {
                readCount: parseInt(info.readCount || '0', 10),
                writeCount: parseInt(info.writeCount || '0', 10),
                deleteCount: parseInt(info.deleteCount || '0', 10),
                headItemReadCount: parseInt(info.headItemReadCount || '0', 10),
                storageBytes: parseInt(info.storageBytes || '0', 10),
            },
        };
    }

    async update(newFields: { name?: string }): Promise<Partial<RequestQueueInfo> | undefined> {
        if (newFields.name !== undefined) {
            await this.client.redis.hset(this.infoKey, 'name', newFields.name);
            const indexKey = this.client.key('rq:index');
            const raw = await this.client.redis.hget(indexKey, `id:${this.id}`);
            if (raw) {
                const data = JSON.parse(raw);
                if (data.name) await this.client.redis.hdel(indexKey, `name:${data.name}`);
                data.name = newFields.name;
                await this.client.redis.hset(indexKey, `id:${this.id}`, JSON.stringify(data));
                await this.client.redis.hset(indexKey, `name:${newFields.name}`, JSON.stringify(data));
            }
        }
        await this.client.redis.hset(this.infoKey, 'modifiedAt', nowISO().toISOString());
        return { name: newFields.name };
    }

    async delete(): Promise<void> {
        const info = await this.client.redis.hgetall(this.infoKey);
        await this.client.redis.del(this.infoKey, this.requestsKey, this.pendingKey, this.lockedKey, this.lockOwnersKey);
        const indexKey = this.client.key('rq:index');
        await this.client.redis.hdel(indexKey, `id:${this.id}`);
        if (info?.name) await this.client.redis.hdel(indexKey, `name:${info.name}`);
    }

    async addRequest(request: RequestSchema, options?: RequestOptions): Promise<QueueOperationInfo> {
        const requestId = request.id || uniqueKeyToRequestId(request.uniqueKey);
        const existing = await this.client.redis.hget(this.requestsKey, requestId);

        if (existing) {
            const parsed = JSON.parse(existing);
            return {
                requestId,
                wasAlreadyPresent: true,
                wasAlreadyHandled: !!parsed.handledAt,
            };
        }

        const stored = { ...request, id: requestId };
        const score = options?.forefront ? 0 : Date.now();

        const pipeline = this.client.redis.pipeline();
        pipeline.hset(this.requestsKey, requestId, JSON.stringify(stored));
        pipeline.zadd(this.pendingKey, String(score), requestId);
        pipeline.hincrby(this.infoKey, 'totalRequestCount', 1);
        pipeline.hincrby(this.infoKey, 'pendingRequestCount', 1);
        pipeline.hset(this.infoKey, 'modifiedAt', nowISO().toISOString());
        await pipeline.exec();

        // Track write and storage bytes for billing
        const bytes = BillingTracker.estimateRequestBytes(stored);
        await Promise.all([
            this.trackWrite(1),
            this.trackStorageBytes(bytes),
        ]);
        
        // Track internal data transfer (bytes written to storage)
        if (this.client.billingContext) {
            const billingTracker = new BillingTracker(this.client);
            await billingTracker.trackInternalTransfer(this.client.billingContext.runId, this.client.billingContext.userId, bytes);
        }

        return { requestId, wasAlreadyPresent: false, wasAlreadyHandled: false };
    }

    async batchAddRequests(requests: RequestSchema[], options?: RequestOptions): Promise<BatchAddRequestsResult> {
        const processedRequests = [];
        const unprocessedRequests = [];

        for (const request of requests) {
            try {
                const result = await this.addRequest(request, options);
                processedRequests.push({
                    uniqueKey: request.uniqueKey,
                    requestId: result.requestId,
                    wasAlreadyPresent: result.wasAlreadyPresent,
                    wasAlreadyHandled: result.wasAlreadyHandled,
                });
            } catch {
                unprocessedRequests.push({
                    uniqueKey: request.uniqueKey,
                    url: request.url,
                    method: request.method,
                });
            }
        }

        return { processedRequests, unprocessedRequests };
    }

    async getRequest(id: string): Promise<RequestOptions | undefined> {
        const raw = await this.client.redis.hget(this.requestsKey, id);
        if (!raw) return undefined;
        
        // Track read for billing with bytes transferred
        const bytesTransferred = Buffer.byteLength(raw, 'utf8');
        await this.trackRead(1, bytesTransferred);
        
        return JSON.parse(raw);
    }

    async updateRequest(request: UpdateRequestSchema, options?: RequestOptions): Promise<QueueOperationInfo> {
        const existing = await this.client.redis.hget(this.requestsKey, request.id);

        if (!existing) {
            return this.addRequest(request, options);
        }

        const old = JSON.parse(existing);
        const wasHandled = !!old.handledAt;
        const isNowHandled = !!request.handledAt;
        const updatedRequest = { ...old, ...request };
        const oldBytes = this.estimateStoredRequestBytes(existing);
        const newBytes = BillingTracker.estimateRequestBytes(updatedRequest);
        const deltaBytes = newBytes - oldBytes;

        await this.client.redis.hset(this.requestsKey, request.id, JSON.stringify(updatedRequest));

        if (!wasHandled && isNowHandled) {
            const pipeline = this.client.redis.pipeline();
            pipeline.zrem(this.pendingKey, request.id);
            pipeline.zrem(this.lockedKey, request.id);
            pipeline.hdel(this.lockOwnersKey, request.id);
            pipeline.hincrby(this.infoKey, 'handledRequestCount', 1);
            pipeline.hincrby(this.infoKey, 'pendingRequestCount', -1);
            pipeline.hset(this.infoKey, 'modifiedAt', nowISO().toISOString());
            await pipeline.exec();
        } else if (wasHandled && !isNowHandled) {
            const score = options?.forefront ? 0 : Date.now();
            const pipeline = this.client.redis.pipeline();
            pipeline.zadd(this.pendingKey, String(score), request.id);
            pipeline.hincrby(this.infoKey, 'handledRequestCount', -1);
            pipeline.hincrby(this.infoKey, 'pendingRequestCount', 1);
            pipeline.hset(this.infoKey, 'modifiedAt', nowISO().toISOString());
            await pipeline.exec();
        }

        if (deltaBytes !== 0) {
            await this.trackStorageBytes(deltaBytes);
        }

        return {
            requestId: request.id,
            wasAlreadyPresent: true,
            wasAlreadyHandled: wasHandled,
        };
    }

    async deleteRequest(id: string): Promise<void> {
        const raw = await this.client.redis.hget(this.requestsKey, id);
        if (!raw) return;

        const request = JSON.parse(raw);
        const releasedBytes = this.estimateStoredRequestBytes(raw);
        const pipeline = this.client.redis.pipeline();
        pipeline.hdel(this.requestsKey, id);
        pipeline.zrem(this.pendingKey, id);
        pipeline.zrem(this.lockedKey, id);
        pipeline.hdel(this.lockOwnersKey, id);
        pipeline.hincrby(this.infoKey, 'totalRequestCount', -1);

        if (request.handledAt) {
            pipeline.hincrby(this.infoKey, 'handledRequestCount', -1);
        } else {
            pipeline.hincrby(this.infoKey, 'pendingRequestCount', -1);
        }

        pipeline.hset(this.infoKey, 'modifiedAt', nowISO().toISOString());
        await pipeline.exec();

        if (releasedBytes > 0) {
            await this.trackStorageBytes(-releasedBytes);
        }
    }

    async listHead(options?: ListOptions): Promise<QueueHead> {
        const limit = options?.limit ?? 100;
        const now = Date.now();

        // Two round-trips total (same pattern as listRequestsPage): one
        // pipeline for the pending head + all actively locked IDs (replaces
        // the per-id ZSCORE loop), then a single HMGET for the request bodies
        // (replaces the per-id HGET loop).
        const pipeline = this.client.redis.pipeline();
        // ioredis 6 types: stop must be string | Buffer (not number)
        pipeline.zrange(this.pendingKey, 0, String(limit - 1));
        // The locked ZSET is scored by lockUntil ms, so one exclusive-bound
        // range read equals the old per-id ZSCORE > now check.
        pipeline.zrangebyscore(this.lockedKey, `(${now}`, '+inf');
        const results = await pipeline.exec();

        const at = (i: number): string[] => {
            const value = results?.[i]?.[1];
            return Array.isArray(value) ? (value as string[]) : [];
        };
        const ids = at(0);
        const activeLocks = new Set(at(1));

        // Filter out locked items
        const items: RequestQueueHeadItem[] = [];
        const candidates = ids.filter((id) => !activeLocks.has(id)).slice(0, limit);
        if (candidates.length > 0) {
            const raws = await this.client.redis.hmget(this.requestsKey, ...candidates);
            for (const raw of raws) {
                if (!raw) continue;
                const req = JSON.parse(raw);
                items.push({
                    id: req.id,
                    retryCount: req.retryCount ?? 0,
                    uniqueKey: req.uniqueKey,
                    url: req.url,
                    method: req.method ?? 'GET',
                });
            }
        }

        // Track reads for billing with bytes transferred
        if (items.length > 0) {
            const totalBytes = items.reduce((sum, item) => sum + JSON.stringify(item).length, 0);
            await this.trackRead(items.length, totalBytes);
        }

        const modifiedAt = await this.client.redis.hget(this.infoKey, 'modifiedAt');
        const hadMultipleClients = await this.trackClientKey(this.clientKey);

        return {
            limit,
            queueModifiedAt: modifiedAt ? new Date(modifiedAt) : nowISO(),
            hadMultipleClients,
            items,
        };
    }

    /**
     * List all requests in the queue with optional filtering and pagination.
     * Supports cursor-based pagination (preferred) and deprecated exclusiveStartId.
     * @param options.limit - Max number of requests to return (default 100, max 10000)
     * @param options.cursor - Base64-encoded cursor from previous response.
     *        If the cursor's request was deleted, listing resumes just past its
     *        sorted position (it never restarts from the first page).
     * @param options.exclusiveStartId - DEPRECATED: Skip requests up to this ID
     * @param options.filter - Filter by state: 'locked' or 'pending'
     * @param options.clientKey - Client identifier for hadMultipleClients tracking
     */
    async listRequests(options?: ListRequestsOptions): Promise<ListRequestsResult> {
        const limit = Math.min(options?.limit ?? 100, 10000);
        const now = Date.now();

        // Validate that filter is not used with deprecated exclusiveStartId
        if (options?.filter && options.filter.length > 0 && options?.exclusiveStartId) {
            throw new Error('filter parameter is not compatible with deprecated exclusiveStartId parameter');
        }

        // Track clientKey for hadMultipleClients
        if (options?.clientKey) {
            const existingClientKey = await this.client.redis.hget(this.infoKey, 'clientKey');
            if (existingClientKey && existingClientKey !== options.clientKey) {
                await this.client.redis.hset(this.infoKey, 'hadMultipleClients', '1');
            } else if (!existingClientKey) {
                await this.client.redis.hset(this.infoKey, 'clientKey', options.clientKey);
            }
        } else {
            // No clientKey provided - treat as new client
            const existingClientKey = await this.client.redis.hget(this.infoKey, 'clientKey');
            if (existingClientKey) {
                await this.client.redis.hset(this.infoKey, 'hadMultipleClients', '1');
            }
        }

        // Decode cursor to get starting position
        let startId: string | undefined;
        if (options?.cursor) {
            try {
                const decoded = JSON.parse(Buffer.from(options.cursor, 'base64').toString('utf8'));
                startId = decoded.requestId;
            } catch {
                // Invalid cursor, start from beginning
            }
        } else if (options?.exclusiveStartId) {
            // Deprecated but still supported
            startId = options.exclusiveStartId;
        }

        // Fetch, filter and paginate via the shared helper. Two round-trips
        // total: one pipeline (HKEYS + only the ZSET reads the filter needs)
        // and one HMGET of the page's request bodies.
        const { items, hasMore } = await listRequestsPage(this.client.redis, {
            requestsKey: this.requestsKey,
            pendingKey: this.pendingKey,
            lockedKey: this.lockedKey,
        }, {
            now,
            limit,
            startId,
            filter: options?.filter,
        });

        // Track reads for billing
        if (items.length > 0) {
            const totalBytes = items.reduce((sum, item) => sum + JSON.stringify(item).length, 0);
            await this.trackRead(items.length, totalBytes);
        }

        // Build response
        const result: ListRequestsResult = {
            items,
            count: items.length,
            limit,
        };

        // Set exclusiveStartId for backward compatibility
        if (startId) {
            result.exclusiveStartId = startId;
        }

        // Set cursor in response if one was provided
        if (options?.cursor) {
            result.cursor = options.cursor;
        }

        // Set nextCursor if there are more results
        if (hasMore && items.length > 0) {
            const lastItem = items[items.length - 1];
            result.nextCursor = Buffer.from(JSON.stringify({ requestId: lastItem.id })).toString('base64');
        }

        return result;
    }

    async listAndLockHead(options: ListAndLockOptions & { clientKey?: string }): Promise<ListAndLockHeadResult> {
        const limit = Math.min(options.limit ?? 25, 25);
        const now = Date.now();
        const lockUntil = now + options.lockSecs * 1000;
        const clientKey = options.clientKey || this.clientKey;

        const runId = this.client.billingContext?.runId || null;
        const ownerValue = runId ? `${clientKey}:${runId}` : clientKey;

        const { lockedIds, activeLockCount } = await listAndLockHeadAtomic(this.client.redis, {
            pendingKey: this.pendingKey,
            lockedKey: this.lockedKey,
            lockOwnersKey: this.lockOwnersKey,
            requestsKey: this.requestsKey,
        }, {
            now,
            lockUntil,
            limit,
            ownerValue,
        });

        const items: RequestQueueHeadItem[] = [];
        if (lockedIds.length > 0) {
            // Single HMGET instead of one HGET round-trip per locked ID
            const raws = await this.client.redis.hmget(this.requestsKey, ...lockedIds);
            for (const raw of raws) {
                if (!raw) continue;
                const req = JSON.parse(raw);
                items.push({
                    id: req.id,
                    retryCount: req.retryCount ?? 0,
                    uniqueKey: req.uniqueKey,
                    url: req.url,
                    method: req.method ?? 'GET',
                });
            }
        }

        if (items.length > 0) {
            const totalBytes = items.reduce((sum, item) => sum + JSON.stringify(item).length, 0);
            await this.trackRead(items.length, totalBytes);
        }

        const modifiedAt = await this.client.redis.hget(this.infoKey, 'modifiedAt');
        const hadMultipleClients = await this.trackClientKey(clientKey);

        return {
            limit,
            queueModifiedAt: modifiedAt ? new Date(modifiedAt) : nowISO(),
            hadMultipleClients,
            lockSecs: options.lockSecs,
            items,
            // From the atomic lock script: active locks held by ANY client
            // (including this call's own).
            queueHasLockedRequests: activeLockCount > 0,
        };
    }

    async prolongRequestLock(id: string, options: ProlongRequestLockOptions): Promise<ProlongRequestLockResult> {
        // One pipeline for the three reads instead of sequential round-trips
        const pipeline = this.client.redis.pipeline();
        pipeline.hget(this.requestsKey, id);
        pipeline.zscore(this.lockedKey, id);
        pipeline.hget(this.lockOwnersKey, id);
        const results = await pipeline.exec();

        const raw = (results?.[0]?.[1] as string | null) ?? null;
        const lockScore = (results?.[1]?.[1] as string | null) ?? null;
        const owner = (results?.[2]?.[1] as string | null) ?? null;

        if (!raw) {
            throw new Error(`Request with ID ${id} not found in queue ${this.id}`);
        }
        const request = JSON.parse(raw);
        if (request.handledAt) {
            throw new Error(`Request with ID ${id} has already been handled in queue ${this.id}`);
        }

        const now = Date.now();
        if (!lockScore || parseInt(lockScore, 10) <= now) {
            throw new Error(`Request with ID ${id} is not locked in queue ${this.id}`);
        }

        const runId = this.client.billingContext?.runId || null;
        if (owner && !matchesLockOwner(owner, this.clientKey, runId)) {
            throw new Error(`Request with ID ${id} is not locked by this client in queue ${this.id}`);
        }

        const lockUntil = now + options.lockSecs * 1000;
        await this.client.redis.zadd(this.lockedKey, String(lockUntil), id);
        return { lockExpiresAt: new Date(lockUntil) };
    }

    async deleteRequestLock(id: string, options?: DeleteRequestLockOptions): Promise<void> {
        await this.client.redis.zrem(this.lockedKey, id);
        await this.client.redis.hdel(this.lockOwnersKey, id);
        if (options?.forefront) {
            await this.client.redis.zadd(this.pendingKey, '0', id);
        }
    }

    /**
     * Unlock all requests locked by a specific clientKey.
     * This is useful for batch unlocking when a client is done processing.
     * @param options.clientKey - The client key to unlock requests for
     * @param options.forefront - If true, move unlocked requests to the front of the queue
     * @param options.runId - Optional run ID to only unlock requests for a specific run
     */
    async unlockRequests(options: { clientKey: string; forefront?: boolean; runId?: string }): Promise<{ unlockedCount: number }> {
        // Single atomic round-trip (UNLOCK_REQUESTS_LUA): owner matching, lock
        // removal and optional forefront re-queueing all happen server-side,
        // replacing the previous HGETALL + 2-3 round-trips per locked request.
        const unlockedCount = await unlockRequestsAtomic(this.client.redis, {
            lockedKey: this.lockedKey,
            lockOwnersKey: this.lockOwnersKey,
            pendingKey: this.pendingKey,
        }, {
            clientKey: options.clientKey,
            forefront: options.forefront,
            runId: options.runId,
        });
        return { unlockedCount };
    }
}
