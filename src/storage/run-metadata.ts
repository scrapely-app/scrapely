import type { DragonflyClient } from './dragonfly-client.js';
import type { UserPlan } from './ttl-manager.js';
import { BillingTracker } from './billing-tracker.js';

/**
 * Metadata for a single actor run
 */
export interface RunMetadata {
    /** Unique run ID */
    runId: string;
    /** User ID who initiated the run */
    userId: string;
    /** User's plan (affects TTL) */
    plan: UserPlan;
    /** When the run was created */
    createdAt: Date;
    /** When the run finished (null if still running) */
    finishedAt: Date | null;
    /** Storage IDs created for this run */
    storageIds: {
        dataset: string;
        kvStore: string;
        requestQueue: string;
    };
    /** Current run status */
    status: RunStatus;
    /** Total storage bytes used (for billing) */
    storageBytes: number;
    /** Container ID if running in Docker */
    containerId?: string;
    /** Actor name/version */
    actorName?: string;
    actorVersion?: string;
}

export type RunStatus = 'running' | 'success' | 'failed' | 'timed_out' | 'killed';

/**
 * Number of most-recent runs per user that are retained indefinitely (no TTL).
 * The 10 latest runs are always accessible regardless of plan.
 * Older runs beyond this window fall back to the standard plan-based TTL.
 */
const KEEP_N_RUNS = 10;

/**
 * Manages run metadata in Dragonfly.
 * Keys are stored with TTL matching the storage TTL so metadata is cleaned up
 * when the associated storage expires.
 */
export class RunMetadataManager {
    constructor(private client: DragonflyClient) {}

    /**
     * Get the Redis sorted-set key that indexes all run IDs for a user.
     * Sorted by createdAt timestamp (score) so ZREVRANGE gives most-recent-first.
     */
    private getUserRunsKey(userId: string): string {
        return this.client.key('users', userId, 'runs');
    }

    /**
     * Create a new run metadata entry
     */
    async create(data: Omit<RunMetadata, 'finishedAt' | 'storageBytes'> & { storageBytes?: number }): Promise<void> {
        const key = this.getRunKey(data.runId);

        const metadata: RunMetadata = {
            ...data,
            finishedAt: null,
            storageBytes: data.storageBytes ?? 0,
        };

        await this.client.redis.hset(key, {
            runId: metadata.runId,
            userId: metadata.userId,
            plan: metadata.plan,
            createdAt: metadata.createdAt.toISOString(),
            finishedAt: '',
            storageIds: JSON.stringify(metadata.storageIds),
            status: metadata.status,
            storageBytes: String(metadata.storageBytes),
            containerId: metadata.containerId ?? '',
            actorName: metadata.actorName ?? '',
            actorVersion: metadata.actorVersion ?? '',
        });

        // Register in the per-user sorted set (score = ms timestamp → most-recent-first via ZREVRANGE)
        const userRunsKey = this.getUserRunsKey(metadata.userId);
        await this.client.redis.zadd(userRunsKey, metadata.createdAt.getTime(), metadata.runId);

        // Set TTL on run metadata - use dataset TTL as reference (longest retention)
        const ttl = this.client.ttlManager.getTtl('dataset', metadata.plan, false);
        if (ttl) {
            await this.client.redis.expire(key, ttl);
        }

        // Enforce top-N indefinite retention — will PERSIST this run if it's in the
        // most-recent KEEP_N_RUNS and may re-apply TTL to any run that was just demoted.
        await this.refreshTopNProtection(metadata.userId);
    }

    /**
     * Get run metadata by ID
     */
    async get(runId: string): Promise<RunMetadata | null> {
        const key = this.getRunKey(runId);
        const data = await this.client.redis.hgetall(key);

        if (!data || !data.runId) return null;

        return {
            runId: data.runId,
            userId: data.userId,
            plan: data.plan as UserPlan,
            createdAt: new Date(data.createdAt),
            finishedAt: data.finishedAt ? new Date(data.finishedAt) : null,
            storageIds: JSON.parse(data.storageIds),
            status: data.status as RunStatus,
            storageBytes: parseInt(data.storageBytes || '0', 10),
            containerId: data.containerId || undefined,
            actorName: data.actorName || undefined,
            actorVersion: data.actorVersion || undefined,
        };
    }

    /**
     * Update run metadata.
     * Silently no-ops if the run does not exist (prevents ghost hash creation).
     */
    async update(runId: string, updates: Partial<Pick<RunMetadata, 'status' | 'storageBytes' | 'finishedAt' | 'containerId'>>): Promise<void> {
        const key = this.getRunKey(runId);

        // Guard: only update if the hash already exists
        const exists = await this.client.redis.exists(key);
        if (!exists) return;

        const updateData: Record<string, string> = {};

        if (updates.status !== undefined) {
            updateData.status = updates.status;
        }
        if (updates.storageBytes !== undefined) {
            updateData.storageBytes = String(updates.storageBytes);
        }
        if (updates.finishedAt !== undefined && updates.finishedAt !== null) {
            updateData.finishedAt = updates.finishedAt.toISOString();
        }
        if (updates.containerId !== undefined) {
            updateData.containerId = updates.containerId;
        }

        if (Object.keys(updateData).length > 0) {
            await this.client.redis.hset(key, updateData);
        }
    }

    /**
     * Mark a run as finished with a specific status
     */
    async finish(runId: string, status: RunStatus, storageBytes?: number): Promise<void> {
        await this.update(runId, {
            status,
            finishedAt: new Date(),
            ...(storageBytes !== undefined && { storageBytes }),
        });
    }

    /**
     * Delete run metadata and remove from the user's sorted-set index.
     */
    async delete(runId: string): Promise<void> {
        // Fetch metadata first so we know the userId for the sorted-set cleanup
        const metadata = await this.get(runId);
        const key = this.getRunKey(runId);
        await this.client.redis.del(key);

        if (metadata) {
            await new BillingTracker(this.client).releaseStorageBytes(runId, metadata.userId);
            const userRunsKey = this.getUserRunsKey(metadata.userId);
            await this.client.redis.zrem(userRunsKey, runId);
            // Re-evaluate top-N so the run that was sitting at position N+1 (if any)
            // gets promoted to PERSIST now that there's a slot open.
            await this.refreshTopNProtection(metadata.userId);
        }
    }

    /**
     * List runs by user ID, most-recent first.
     *
     * Uses a per-user sorted set (ZADD on create, ZREM on delete) instead of a
     * full KEYS scan so lookups are O(log N + M) rather than O(N-total-runs).
     * Stale entries left behind by TTL expiry are detected and pruned lazily.
     */
    async listByUser(userId: string): Promise<RunMetadata[]> {
        const userRunsKey = this.getUserRunsKey(userId);

        // ZREVRANGE returns members sorted by score descending (most-recent first)
        const runIds = await this.client.redis.zrevrange(userRunsKey, 0, -1);
        const runs: RunMetadata[] = [];
        const staleIds: string[] = [];

        for (const runId of runIds) {
            const metadata = await this.get(runId);
            if (!metadata) {
                // Run hash expired via TTL or was deleted without going through delete()
                staleIds.push(runId);
                continue;
            }
            runs.push(metadata);
        }

        // Lazily prune stale entries from the sorted set
        if (staleIds.length > 0) {
            await this.client.redis.zrem(userRunsKey, ...staleIds);
        }

        return runs; // Already in descending createdAt order from ZREVRANGE
    }

    /**
     * Enforce the top-N indefinite retention policy for a user.
     *
     * - The KEEP_N_RUNS most-recent runs get PERSIST (no TTL → kept forever).
     * - Runs beyond that window that currently have no TTL (were previously in the
     *   top N) get the plan-based TTL re-applied so they will eventually expire.
     * - The per-user sorted set is PERSISTed while protected runs exist, and gets
     *   a plan-based TTL when no protected runs remain (prevents a memory leak for
     *   inactive users whose sorted set would otherwise never expire).
     *
     * Called after every create() and delete() so the window stays accurate.
     *
     * Performance: previously used N sequential awaits per phase; now each phase is
     * a single pipelined round-trip regardless of the number of runs.
     */
    private async refreshTopNProtection(userId: string): Promise<void> {
        const userRunsKey = this.getUserRunsKey(userId);
        const allRunIds = await this.client.redis.zrevrange(userRunsKey, 0, -1);

        // All runs have been deleted — clean up the now-empty sorted set immediately
        // rather than leaving it to drift in memory indefinitely.
        if (allRunIds.length === 0) {
            await this.client.redis.del(userRunsKey);
            return;
        }

        const topIds = allRunIds.slice(0, KEEP_N_RUNS);
        const olderIds = allRunIds.slice(KEEP_N_RUNS);

        // ── Phase 1: PERSIST all top-N run keys and the sorted set itself ─────────
        // The sorted set must outlive the runs it indexes, so it gets PERSIST too.
        if (topIds.length > 0) {
            const persistPipeline = this.client.redis.pipeline();
            for (const runId of topIds) {
                persistPipeline.persist(this.getRunKey(runId));
            }
            persistPipeline.persist(userRunsKey);
            await persistPipeline.exec();
        }

        // ── Phase 2: Batch-read TTLs for all older-than-top-N run keys ───────────
        if (olderIds.length === 0) return;

        const ttlPipeline = this.client.redis.pipeline();
        for (const runId of olderIds) {
            ttlPipeline.ttl(this.getRunKey(runId));
        }
        const ttlResults = await ttlPipeline.exec();

        // Collect only the runs that were demoted (exist but have no expiry, TTL = -1)
        const demotedIds = olderIds.filter(
            (_, i) => (ttlResults?.[i]?.[1] as number) === -1,
        );

        if (demotedIds.length === 0) {
            // No demoted runs, but if top-N is also empty we still need to set the
            // sorted-set TTL so inactive users' index keys don't live forever.
            if (topIds.length === 0) {
                const sortedSetTtl = this.client.ttlManager.getTtl('dataset', 'free', false);
                if (sortedSetTtl) await this.client.redis.expire(userRunsKey, sortedSetTtl, 'NX');
            }
            return;
        }

        // ── Phase 3: Batch-read the plan field for each demoted run ──────────────
        const planPipeline = this.client.redis.pipeline();
        for (const runId of demotedIds) {
            planPipeline.hget(this.getRunKey(runId), 'plan');
        }
        const planResults = await planPipeline.exec();

        // ── Phase 4: Apply plan-based TTLs in a single pipeline ──────────────────
        const expirePipeline = this.client.redis.pipeline();
        for (let i = 0; i < demotedIds.length; i++) {
            const runId = demotedIds[i]!;
            const plan = ((planResults?.[i]?.[1] as string | null) ?? 'free') as UserPlan;
            const expireSecs = this.client.ttlManager.getTtl('dataset', plan, false);
            if (expireSecs) {
                expirePipeline.expire(this.getRunKey(runId), expireSecs);
            }
        }

        // If there are no top-N protected runs, also set the sorted-set TTL so it
        // doesn't persist forever once all remaining runs age out of protection.
        // NX ensures we only apply this once; subsequent calls won't shorten it.
        if (topIds.length === 0) {
            const sortedSetTtl = this.client.ttlManager.getTtl('dataset', 'free', false);
            if (sortedSetTtl) expirePipeline.expire(userRunsKey, sortedSetTtl, 'NX');
        }

        await expirePipeline.exec();
    }

    /**
     * Get the Redis key for a run
     */
    private getRunKey(runId: string): string {
        return this.client.key('run', runId);
    }
}
