import type Redis from 'ioredis';

/**
 * TTL configuration in seconds
 */
export interface TtlConfig {
    /** TTL for unnamed datasets (free users) - default 48 hours (Free plan) */
    datasetFree: number;
    /** TTL for unnamed datasets (paid users) - default 31 days (Starter, the shortest paid plan) */
    datasetPaid: number;
    /** TTL for unnamed KV stores (free users) - default 48 hours (Free plan) */
    kvFree: number;
    /** TTL for unnamed KV stores (paid users) - default 31 days (Starter, the shortest paid plan) */
    kvPaid: number;
    /** Fallback TTL for request queues (safety net for orphaned RQs) - default 24 hours */
    requestQueueFallback: number;
    /** TTL for logs in Redis (free users) - default 12 hours */
    logsFree: number;
    /** TTL for logs in Redis (paid users) - default 7 days */
    logsPaid: number;
    /** Maximum log size in bytes - default 5MB */
    logMaxSize: number;
}

/**
 * Default TTL configuration
 */
export const DEFAULT_TTL_CONFIG: TtlConfig = {
    datasetFree: 48 * 60 * 60,          // 48 hours
    datasetPaid: 31 * 24 * 60 * 60,     // 31 days
    kvFree: 48 * 60 * 60,               // 48 hours
    kvPaid: 31 * 24 * 60 * 60,          // 31 days
    requestQueueFallback: 24 * 60 * 60, // 24 hours
    logsFree: 12 * 60 * 60,             // 12 hours
    logsPaid: 7 * 24 * 60 * 60,         // 7 days
    logMaxSize: 5 * 1024 * 1024,        // 5MB
};

export type StorageType = 'dataset' | 'kv' | 'rq' | 'logs';
export type UserPlan = 'free' | 'paid';

/**
 * Manages TTL (Time-To-Live) for Dragonfly storage keys.
 * 
 * Retention policy:
 * - Named storage: Indefinite (billed), no TTL set
 * - Unnamed datasets/KV stores: the plan's retention (Free 48 h, Starter 31 d,
 *   Scale 60 d, Business 90 d); the defaults here are Free and Starter
 * - Request queues: Explicitly deleted after run, 24h fallback TTL for orphaned RQs
 * - Logs: Short TTL in Redis, persisted to S3
 */
export class TtlManager {
    private config: TtlConfig;

    constructor(config?: Partial<TtlConfig>) {
        this.config = { ...DEFAULT_TTL_CONFIG, ...config };
    }

    /**
     * Get the TTL for a storage type based on user plan and whether it's named.
     * Returns null for named storage (no TTL = indefinite).
     */
    getTtl(storageType: 'dataset' | 'kv', plan: UserPlan, isNamed: boolean): number | null;
    getTtl(storageType: 'rq', plan: UserPlan): number;
    getTtl(storageType: 'logs', plan: UserPlan): number;
    getTtl(storageType: StorageType, plan: UserPlan, isNamed?: boolean): number | null {
        // Named storage gets no TTL (indefinite retention, billed)
        if ((storageType === 'dataset' || storageType === 'kv') && isNamed) {
            return null;
        }

        switch (storageType) {
            case 'dataset':
                return plan === 'paid' ? this.config.datasetPaid : this.config.datasetFree;
            case 'kv':
                return plan === 'paid' ? this.config.kvPaid : this.config.kvFree;
            case 'rq':
                return this.config.requestQueueFallback;
            case 'logs':
                return plan === 'paid' ? this.config.logsPaid : this.config.logsFree;
            default:
                return null;
        }
    }

    /**
     * Set expiry on multiple keys atomically using a pipeline.
     * Uses 'NX' flag to only set TTL if it doesn't already exist.
     */
    async setExpiry(redis: Redis, keys: string[], ttl: number): Promise<void> {
        if (keys.length === 0 || ttl <= 0) return;

        const pipeline = redis.pipeline();
        for (const key of keys) {
            // 'NX' = only set expiry if key has no expiry
            // This prevents accidentally shortening TTL on access
            pipeline.expire(key, ttl, 'NX');
        }
        await pipeline.exec();
    }

    /**
     * Set expiry on multiple keys, overwriting any existing TTL.
     * Use this when you want to guarantee the TTL is set to a specific value.
     */
    async setExpiryForce(redis: Redis, keys: string[], ttl: number): Promise<void> {
        if (keys.length === 0 || ttl <= 0) return;

        const pipeline = redis.pipeline();
        for (const key of keys) {
            pipeline.expire(key, ttl);
        }
        await pipeline.exec();
    }

    /**
     * Get the maximum log size in bytes
     */
    getLogMaxSize(): number {
        return this.config.logMaxSize;
    }

    /**
     * Get the full TTL config
     */
    getConfig(): Readonly<TtlConfig> {
        return { ...this.config };
    }

    /**
     * Create TtlConfig from environment variables
     */
    static fromEnv(): TtlConfig {
        const parseEnv = (key: string, defaultValue: number): number => {
            const val = process.env[key];
            if (val) {
                const parsed = parseInt(val, 10);
                if (!isNaN(parsed) && parsed > 0) return parsed;
            }
            return defaultValue;
        };

        return {
            datasetFree:          parseEnv('SCRAPELY_TTL_DATASET_FREE', DEFAULT_TTL_CONFIG.datasetFree),
            datasetPaid:          parseEnv('SCRAPELY_TTL_DATASET_PAID', DEFAULT_TTL_CONFIG.datasetPaid),
            // Bug fix: these two env vars were silently ignored before
            kvFree:               parseEnv('SCRAPELY_TTL_KV_FREE',      DEFAULT_TTL_CONFIG.kvFree),
            kvPaid:               parseEnv('SCRAPELY_TTL_KV_PAID',      DEFAULT_TTL_CONFIG.kvPaid),
            requestQueueFallback: parseEnv('SCRAPELY_TTL_RQ_FALLBACK',  DEFAULT_TTL_CONFIG.requestQueueFallback),
            logsFree:             parseEnv('SCRAPELY_TTL_LOGS_FREE',    DEFAULT_TTL_CONFIG.logsFree),
            logsPaid:             parseEnv('SCRAPELY_TTL_LOGS_PAID',    DEFAULT_TTL_CONFIG.logsPaid),
            logMaxSize:           parseEnv('SCRAPELY_LOG_MAX_SIZE',     DEFAULT_TTL_CONFIG.logMaxSize),
        };
    }
}