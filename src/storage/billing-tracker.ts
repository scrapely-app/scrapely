/**
 * Interface for billing client - can be implemented by DragonflyClient or server's DragonflyConnection
 * Uses 'any' for redis to avoid type conflicts between different ioredis versions in different packages
 */
export interface BillingClient {
    redis: any; // ioredis Redis client - using 'any' to avoid cross-package type conflicts
    key(...parts: string[]): string;
}

/**
 * Tracks storage usage for billing purposes.
 *
 * Storage is tracked in bytes for:
 * - Datasets (items stored)
 * - KV stores (records stored)
 * - Request queues (requests stored)
 * - Logs (log size)
 *
 * This data is used to calculate storage-hour charges.
 */
export class BillingTracker {
    constructor(private client: BillingClient) {}

    /**
     * Get the Redis key for user's billing data.
     * Uses the client's key prefix for proper environment isolation.
     */
    private getBillingKey(userId: string): string {
        return this.client.key('billing', userId);
    }

    /**
     * Get the Redis key for run's storage bytes
     */
    private getRunStorageKey(runId: string): string {
        return this.client.key('billing', 'run', runId);
    }

    /**
     * Track storage bytes for a run.
     * Called when data is written to storage.
     *
     * @param runId      - The run producing the storage.
     * @param userId     - Owner of the run (used for the user-level billing key).
     * @param bytes      - Number of bytes to add.
     * @param ttlSeconds - Optional TTL to set on the per-run billing key (NX — only
     *   applied when the key is first created).  Pass the same TTL as the run's data
     *   so the billing key expires together with the run and never accumulates forever.
     *
     * Fix: all Redis writes are batched into a single pipeline to reduce round-trips
     * from 5 down to 1 and to make the operation safer under concurrent writers.
     */
    async trackStorageBytes(runId: string, userId: string, bytes: number, ttlSeconds?: number): Promise<void> {
        const runKey = this.getRunStorageKey(runId);
        const billingKey = this.getBillingKey(userId);
        const now = String(Date.now());

        const pipeline = this.client.redis.pipeline();
        pipeline.hincrby(runKey, 'storageBytes', bytes);
        pipeline.hincrby(billingKey, 'totalStorageBytes', bytes);
        pipeline.hset(runKey, 'updatedAt', now);
        pipeline.hset(billingKey, 'updatedAt', now);

        // Attach a TTL to the per-run billing key so it expires alongside run data.
        // NX ensures we only set it on the first write; subsequent writes don't reset it.
        if (ttlSeconds && ttlSeconds > 0) {
            pipeline.expire(runKey, ttlSeconds, 'NX');
        }

        await pipeline.exec();
    }

    /**
     * Reduce the user's running total when a run's storage is released.
     * Should be called from the run cleanup path (e.g. when delete() is invoked on
     * RunMetadataManager or when a run's TTL fires and its data is gone).
     * Clamps the total at 0 to guard against double-release.
     *
     * Note: the read-decrement-clamp sequence is not fully atomic.  At MVP scale
     * this is acceptable; a Lua script can be used later if needed.
     */
    async releaseStorageBytes(runId: string, userId: string): Promise<void> {
        const runKey = this.getRunStorageKey(runId);
        const billingKey = this.getBillingKey(userId);

        // Read the bytes that were charged to this run
        const stored = await this.client.redis.hget(runKey, 'storageBytes');
        const bytes = parseInt(stored || '0', 10);

        if (bytes > 0) {
            // Decrement user total, but never go below 0
            await this.client.redis.hincrby(billingKey, 'totalStorageBytes', -bytes);

            // Clamp to 0
            const current = parseInt((await this.client.redis.hget(billingKey, 'totalStorageBytes')) || '0', 10);
            if (current < 0) {
                await this.client.redis.hset(billingKey, 'totalStorageBytes', '0');
            }
        }

        // Remove the per-run billing key entirely
        await this.client.redis.del(runKey);
    }

    /**
     * Track read operations for billing.
     */
    async trackRead(userId: string, count: number = 1): Promise<void> {
        const billingKey = this.getBillingKey(userId);
        await this.client.redis.hincrby(billingKey, 'readCount', count);
    }

    /**
     * Track write operations for billing.
     */
    async trackWrite(userId: string, count: number = 1): Promise<void> {
        const billingKey = this.getBillingKey(userId);
        await this.client.redis.hincrby(billingKey, 'writeCount', count);
    }

    /**
     * Track list operations for billing (KV store listKeys).
     */
    async trackList(userId: string, count: number = 1): Promise<void> {
        const billingKey = this.getBillingKey(userId);
        await this.client.redis.hincrby(billingKey, 'listCount', count);
    }

    /**
     * Get billing summary for a user.
     * Now includes `storageHourBytes` and `internalTransferBytes` so callers can display accumulated
     * storage-hour usage and internal transfer without having to call separate methods.
     */
    async getBillingSummary(userId: string): Promise<BillingSummary> {
        const billingKey = this.getBillingKey(userId);
        const data = await this.client.redis.hgetall(billingKey);

        return {
            userId,
            totalStorageBytes:  parseInt(data.totalStorageBytes  || '0', 10),
            readCount:          parseInt(data.readCount          || '0', 10),
            writeCount:         parseInt(data.writeCount         || '0', 10),
            listCount:          parseInt(data.listCount          || '0', 10),
            storageHourBytes:   parseInt(data.storageHourBytes   || '0', 10),
            storageHourCount:   parseInt(data.storageHourCount   || '0', 10),
            internalTransferBytes: parseInt(data.internalTransferBytes || '0', 10),
            externalTransferBytes: parseInt(data.externalTransferBytes || '0', 10),
            updatedAt:          data.updatedAt ? parseInt(data.updatedAt, 10) : null,
        };
    }

    /**
     * Get storage bytes for a specific run.
     */
    async getRunStorageBytes(runId: string): Promise<number> {
        const runKey = this.getRunStorageKey(runId);
        const data = await this.client.redis.hget(runKey, 'storageBytes');
        return parseInt(data || '0', 10);
    }

    /**
     * Calculate storage hours for billing.
     * Storage hours = bytes * hours stored.
     * This is called periodically (e.g., hourly) to record usage.
     */
    async recordStorageHour(userId: string, bytes: number): Promise<void> {
        const billingKey = this.getBillingKey(userId);

        const pipeline = this.client.redis.pipeline();
        // This represents: bytes stored for 1 hour
        pipeline.hincrby(billingKey, 'storageHourBytes', bytes);
        pipeline.hincrby(billingKey, 'storageHourCount', 1);
        await pipeline.exec();
    }

    /**
     * Get and reset storage hours (for periodic billing).
     * Returns the total storage-hour-bytes and resets the counters.
     *
     * Note: the read-reset sequence is not fully atomic.  Two concurrent billing
     * cron jobs could both read the same non-zero value and both reset it, causing
     * one batch to be silently discarded.  At MVP scale a single billing worker is
     * assumed; use a Lua script (HGET+HSET in one round-trip) if concurrency is needed.
     */
    async getAndResetStorageHours(userId: string): Promise<{
        storageHourBytes: number;
        storageHourCount: number;
    }> {
        const billingKey = this.getBillingKey(userId);
        const data = await this.client.redis.hgetall(billingKey);

        const result = {
            storageHourBytes: parseInt(data.storageHourBytes || '0', 10),
            storageHourCount: parseInt(data.storageHourCount || '0', 10),
        };

        // Reset the counters
        const pipeline = this.client.redis.pipeline();
        pipeline.hset(billingKey, 'storageHourBytes', '0');
        pipeline.hset(billingKey, 'storageHourCount', '0');
        await pipeline.exec();

        return result;
    }

    /**
     * Estimate bytes for a dataset item.
     * Rough estimate based on JSON string length.
     */
    static estimateItemBytes(item: any): number {
        return Buffer.byteLength(JSON.stringify(item), 'utf-8');
    }

    /**
     * Estimate bytes for a KV store record.
     */
    static estimateRecordBytes(key: string, value: Buffer | string | object): number {
        const keyBytes = Buffer.byteLength(key, 'utf-8');
        let valueBytes: number;

        if (Buffer.isBuffer(value)) {
            valueBytes = value.length;
        } else if (typeof value === 'string') {
            valueBytes = Buffer.byteLength(value, 'utf-8');
        } else {
            valueBytes = Buffer.byteLength(JSON.stringify(value), 'utf-8');
        }

        return keyBytes + valueBytes;
    }

    /**
     * Estimate bytes for a request queue item.
     */
    static estimateRequestBytes(request: any): number {
        return Buffer.byteLength(JSON.stringify(request), 'utf-8');
    }

    /**
     * Get the Redis key for run's transfer bytes.
     * Uses the client's key prefix for proper environment isolation.
     */
    private getRunTransferKey(runId: string): string {
        return this.client.key('billing', 'run', runId, 'transfer');
    }

    /**
     * Track internal data transfer for billing.
     * Called when data is read from storage (datasets, KV stores, request queues).
     * Internal transfer = data transferred between actor and Scrapely storage.
     *
     * @param runId - The run that caused the transfer (for per-run attribution)
     * @param userId - The user whose storage was accessed
     * @param bytes - Number of bytes transferred
     */
    async trackInternalTransfer(runId: string, userId: string, bytes: number): Promise<void> {
        const runTransferKey = this.getRunTransferKey(runId);

        // Track per-run only (run-scoped billing)
        await this.client.redis.hincrby(runTransferKey, 'internalTransferBytes', bytes);
    }

    /**
     * Track external data transfer for billing (egress).
     * Called when data leaves the platform to users via API (e.g., downloading dataset items, fetching KV records).
     * External egress = data going out of our system to the internet.
     *
     * @param runId - The run that caused the transfer (for per-run attribution)
     * @param userId - The user who downloaded the data
     * @param bytes - Number of bytes transferred
     */
    async trackExternalTransfer(runId: string, userId: string, bytes: number): Promise<void> {
        const runTransferKey = this.getRunTransferKey(runId);

        // Track per-run only (run-scoped billing)
        await this.client.redis.hincrby(runTransferKey, 'externalTransferBytes', bytes);
    }

    /**
     * Get transfer bytes for a specific run.
     */
    async getRunTransferBytes(runId: string): Promise<{ internalTransferBytes: number; externalTransferBytes: number }> {
        const runTransferKey = this.getRunTransferKey(runId);
        const data = await this.client.redis.hgetall(runTransferKey);

        return {
            internalTransferBytes: parseInt(data.internalTransferBytes || '0', 10),
            externalTransferBytes: parseInt(data.externalTransferBytes || '0', 10),
        };
    }

    /**
     * Clear transfer bytes for a specific run (cleanup after billing).
     */
    async clearRunTransferBytes(runId: string): Promise<void> {
        const runTransferKey = this.getRunTransferKey(runId);
        await this.client.redis.del(runTransferKey);
    }

}

export interface BillingSummary {
    userId: string;
    totalStorageBytes: number;
    readCount: number;
    writeCount: number;
    listCount: number;
    /** Accumulated storage-hour bytes since last reset (for periodic billing). */
    storageHourBytes: number;
    /** Number of hourly snapshots recorded since last reset. */
    storageHourCount: number;
    /** Accumulated internal transfer bytes (actor ↔ storage) since last reset. */
    internalTransferBytes: number;
    /** Accumulated external egress bytes (platform → user via API) since last reset. */
    externalTransferBytes: number;
    updatedAt: number | null;
}
