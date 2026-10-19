import Redis from 'ioredis';
import type { StorageClient, SetStatusMessageOptions, RequestQueueOptions } from '@crawlee/types';
import { DragonflyDatasetCollectionClient } from './dataset-collection-client.js';
import { DragonflyDatasetClient } from './dataset-client.js';
import { DragonflyKeyValueStoreCollectionClient } from './kv-store-collection-client.js';
import { DragonflyKeyValueStoreClient } from './key-value-store-client.js';
import { DragonflyRequestQueueCollectionClient } from './rq-collection-client.js';
import { DragonflyRequestQueueClient } from './request-queue-client.js';
import { TtlManager, TtlConfig, UserPlan } from './ttl-manager.js';

/** The platform-wide storage namespace shared by all users and runs. */
const PLATFORM_KEY_PREFIX = 'crawlee:';
/** SCAN COUNT hint for deleteByPattern(): keys examined per scan step. */
const DEFAULT_SCAN_COUNT = 500;
/** Max keys per UNLINK call in deleteByPattern(): keeps command buffers small. */
const DEFAULT_UNLINK_BATCH = 1000;

export interface BillingContext {
    runId: string;
    userId: string;
}

export interface DragonflyClientOptions {
    host: string;
    port?: number;
    password?: string;
    keyPrefix?: string;
    /**
     * Single-tenant escape hatch: allow purge() to delete the entire default
     * `crawlee:` key prefix even when no billingContext.runId is set. The
     * `crawlee:` namespace is shared by every user/run on the platform, so
     * purging it wholesale would delete other tenants' data — hence opt-in.
     */
    allowUnscopedPurge?: boolean;
    /** User plan for TTL determination */
    userPlan?: UserPlan;
    /** Custom TTL configuration */
    ttlConfig?: Partial<TtlConfig>;
    /** Billing context for tracking usage */
    billingContext?: BillingContext;
}

export class DragonflyClient implements StorageClient {
    readonly redis: Redis;
    readonly keyPrefix: string;
    readonly ttlManager: TtlManager;
    readonly userPlan: UserPlan;
    readonly billingContext?: BillingContext;
    readonly allowUnscopedPurge: boolean;
    stats = { rateLimitErrors: [] as number[] };

    constructor(options: DragonflyClientOptions) {
        const redisOpts: any = {
            host: options.host,
            port: options.port ?? 6379,
            lazyConnect: false,
            retryStrategy: (times: number) => Math.min(times * 100, 3000),
            keepAlive: 10000,
        };
        if (options.password) redisOpts.password = options.password;
        this.redis = new Redis(redisOpts);
        this.redis.on('error', () => {});
        this.keyPrefix = options.keyPrefix ?? PLATFORM_KEY_PREFIX;
        this.userPlan = options.userPlan ?? 'free';
        this.ttlManager = new TtlManager(options.ttlConfig);
        this.billingContext = options.billingContext;
        this.allowUnscopedPurge = options.allowUnscopedPurge ?? false;
    }

    key(...parts: string[]): string {
        return this.keyPrefix + parts.join(':');
    }

    datasets(): DragonflyDatasetCollectionClient {
        return new DragonflyDatasetCollectionClient(this);
    }

    dataset(id: string): DragonflyDatasetClient {
        return new DragonflyDatasetClient(this, id);
    }

    keyValueStores(): DragonflyKeyValueStoreCollectionClient {
        return new DragonflyKeyValueStoreCollectionClient(this);
    }

    keyValueStore(id: string): DragonflyKeyValueStoreClient {
        return new DragonflyKeyValueStoreClient(this, id);
    }

    requestQueues(): DragonflyRequestQueueCollectionClient {
        return new DragonflyRequestQueueCollectionClient(this);
    }

    requestQueue(id: string, options?: RequestQueueOptions): DragonflyRequestQueueClient {
        return new DragonflyRequestQueueClient(this, id, options);
    }

    /**
     * Purges storage, mirroring Crawlee's StorageClient.purge() contract: clear
     * only the current run's default storages (dataset, KV store, request queue).
     *
     * Scoping rules — the default `crawlee:` prefix is shared by every user/run
     * on the platform (datasets, request queues, billing, auth tokens all live
     * under it), so purging the whole prefix would delete other tenants' data:
     *
     *  1. With `billingContext.runId`: resolve the run's storageIds from
     *     `{prefix}run:{runId}` and delete only those three storage namespaces.
     *  2. Without a run ID, but with a custom (scoped) `keyPrefix`: delete the
     *     whole prefix — it is assumed dedicated to this client.
     *  3. Without a run ID and with the shared default prefix: refuse, unless
     *     `allowUnscopedPurge` was set explicitly (single-tenant opt-in).
     *
     * Deletion uses SCAN + batched UNLINK (see deleteByPattern) instead of a
     * blocking KEYS walk followed by a single huge DEL.
     */
    async purge(): Promise<void> {
        const runId = this.billingContext?.runId;
        if (runId && (await this.purgeRunDefaultStorages(runId)) !== null) return;

        if (this.keyPrefix === PLATFORM_KEY_PREFIX && !this.allowUnscopedPurge) {
            throw new Error(
                'DragonflyClient.purge() refused: keyPrefix "crawlee:" is shared by all users/runs ' +
                "of the platform, so purging it would delete other tenants' data. Provide a " +
                "billingContext.runId (purges only that run's default storages), pass a scoped " +
                'keyPrefix, or set allowUnscopedPurge: true for single-tenant deployments.'
            );
        }

        await this.deleteByPattern(`${this.keyPrefix}*`);
    }

    /**
     * Delete the default storages (dataset / KV store / request queue) of a run,
     * resolved from the run's metadata hash (`{prefix}run:{runId}` → storageIds).
     *
     * @returns the number of keys deleted, or null when run metadata is missing
     *          (expired or never created) and nothing could be scoped.
     */
    private async purgeRunDefaultStorages(runId: string): Promise<number | null> {
        const data = await this.redis.hgetall(this.key('run', runId));
        if (!data || !data.storageIds) return null;

        let storageIds: { dataset?: string; kvStore?: string; requestQueue?: string };
        try {
            storageIds = JSON.parse(data.storageIds);
        } catch {
            return null; // corrupt metadata — do not guess, fall back to prefix rules
        }

        let deleted = 0;
        if (storageIds.dataset) deleted += await this.deleteByPattern(this.key('dataset', storageIds.dataset, '*'));
        if (storageIds.kvStore) deleted += await this.deleteByPattern(this.key('kvs', storageIds.kvStore, '*'));
        if (storageIds.requestQueue) deleted += await this.deleteByPattern(this.key('rq', storageIds.requestQueue, '*'));
        return deleted;
    }

    /**
     * Incrementally delete every key matching `pattern` using SCAN + batched
     * UNLINK. Unlike KEYS + DEL, this never walks the keyspace in one blocking
     * pass, never materializes the full key list in a single reply, and never
     * sends one gigantic delete command; UNLINK also reclaims memory
     * asynchronously. Keys that vanish mid-scan are simply no-ops.
     *
     * @returns number of keys deleted (best effort).
     */
    async deleteByPattern(pattern: string, options?: { scanCount?: number; unlinkBatch?: number }): Promise<number> {
        const scanCount = Math.max(1, options?.scanCount ?? DEFAULT_SCAN_COUNT);
        const unlinkBatch = Math.max(1, options?.unlinkBatch ?? DEFAULT_UNLINK_BATCH);

        let deleted = 0;
        let cursor = '0';
        do {
            const [nextCursor, keys] = await this.redis.scan(cursor, 'MATCH', pattern, 'COUNT', scanCount);
            cursor = nextCursor;
            // A single SCAN reply can exceed COUNT, so chunk the UNLINKs too.
            for (let i = 0; i < keys.length; i += unlinkBatch) {
                const chunk = keys.slice(i, i + unlinkBatch);
                await this.redis.unlink(...chunk);
                deleted += chunk.length;
            }
        } while (cursor !== '0');
        return deleted;
    }

    async teardown(): Promise<void> {
        // no-op: crawlee calls this during crawler.run() shutdown
        // we manage the Redis connection lifecycle ourselves via disconnectDragonfly()
    }

    async disconnect(): Promise<void> {
        await this.redis.quit();
    }

    async setStatusMessage(message: string, options?: SetStatusMessageOptions): Promise<void> {
        const statusKey = this.key('status');
        await this.redis.hset(statusKey, {
            message,
            level: options?.level ?? 'INFO',
            isTerminal: options?.isStatusMessageTerminal ? '1' : '0',
        });
    }
}
