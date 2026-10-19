import type { DragonflyClient } from './dragonfly-client.js';
import type { UserPlan } from './ttl-manager.js';
import { RunMetadataManager, type RunStatus } from './run-metadata.js';

/**
 * Log configuration
 */
export interface LogConfig {
    /** Maximum log size in bytes before truncation (default: 5MB) */
    maxLogSize: number;
    /** S3 bucket for long-term log storage */
    s3Bucket?: string;
    /** S3 region */
    s3Region?: string;
    /** S3 endpoint (for non-AWS S3-compatible storage) */
    s3Endpoint?: string;
}

const DEFAULT_LOG_CONFIG: LogConfig = {
    maxLogSize: 5 * 1024 * 1024, // 5MB
};

/**
 * Manages log storage in Dragonfly with optional S3 archival.
 *
 * Log lifecycle:
 * 1. Logs are written to Dragonfly during execution (fast, real-time tailing)
 * 2. Log size is capped at maxLogSize (trailing portion kept, truncated flag set)
 * 3. After run completes, logs are uploaded to S3 for long-term storage
 * 4. TTL in Redis is short (12h free, 7d paid) since S3 is the source of truth
 */
export class LogManager {
    private config: LogConfig;

    constructor(private client: DragonflyClient, config?: Partial<LogConfig>) {
        this.config = { ...DEFAULT_LOG_CONFIG, ...config };
    }

    /**
     * Get the Redis key for a run's logs
     */
    private getLogKey(runId: string): string {
        return this.client.key('logs', runId);
    }

    /**
     * Get the Redis key for the truncation flag on a run's logs.
     * Set to '1' the first time the log buffer overflows maxLogSize.
     */
    private getTruncatedKey(runId: string): string {
        return this.client.key('logs', runId, 'truncated');
    }

    /**
     * Advance a byte offset forward until it points to the start of a valid
     * UTF-8 code-point boundary in `buf`.  Continuation bytes (10xxxxxx) are
     * skipped so we never cut a multibyte sequence in half.
     *
     * Returns the adjusted offset (always ≤ buf.length).
     */
    private static alignUtf8Boundary(buf: Buffer, offset: number): number {
        while (offset < buf.length && (buf[offset]! & 0xc0) === 0x80) {
            offset++;
        }
        return offset;
    }

    /**
     * Append log data to a run's log buffer.
     *
     * Implements trailing-only storage — if max size is exceeded, older data is
     * truncated from the front.  The cut point is aligned to a UTF-8 code-point
     * boundary so the stored string is always valid UTF-8.
     *
     * A separate 'truncated' flag key is set the first time an overflow happens
     * so callers can surface a "log was truncated" notice to the user.
     *
     * Fix (race condition): APPEND + STRLEN are batched in a single pipeline to
     * reduce round-trips.  The subsequent GETRANGE/SET truncation is unavoidably
     * a second step, but at MVP scale log writes per run are effectively serial.
     */
    async appendLog(runId: string, data: string): Promise<void> {
        if (data === '') {
            // Redis APPEND of an empty string still creates the key — preserve
            // that behaviour but skip the unnecessary pipeline call.
            const key = this.getLogKey(runId);
            await this.client.redis.append(key, '');
            return;
        }

        const key = this.getLogKey(runId);
        const buffer = Buffer.from(data, 'utf-8');

        // Batch APPEND + STRLEN into one pipeline to reduce round-trips
        const pipeline = this.client.redis.pipeline();
        pipeline.append(key, buffer);
        pipeline.strlen(key);
        const results = await pipeline.exec();

        // results[1][1] is the STRLEN response
        const currentLength = results?.[1]?.[1] as number ?? 0;

        if (currentLength > this.config.maxLogSize) {
            const rawExcess = currentLength - this.config.maxLogSize;

            // Use getrangeBuffer to get the raw UTF-8 bytes without any ioredis
            // string decoding.  If we used getrange(), ioredis would UTF-8-decode
            // the bytes into a JS string, and re-encoding with Buffer.from(s,'binary')
            // would corrupt multi-byte sequences (latin-1 ≠ utf-8 for high bytes).
            const trailingBuf: Buffer = await (this.client.redis as any).getrangeBuffer(
                key, rawExcess, -1,
            ) as Buffer;

            // Advance past any UTF-8 continuation bytes (10xxxxxx) so we start at
            // a valid code-point boundary and never store a broken sequence.
            const alignedOffset = LogManager.alignUtf8Boundary(trailingBuf, 0);
            const safeTrailing = trailingBuf.subarray(alignedOffset);

            // Overwrite the key with only the safe trailing portion
            await this.client.redis.set(key, safeTrailing);

            // Mark log as truncated (NX — only set once; ignore if already set)
            await this.client.redis.set(this.getTruncatedKey(runId), '1', 'NX');
        }
    }

    /**
     * Get logs for a run
     */
    async getLogs(runId: string): Promise<string | null> {
        const key = this.getLogKey(runId);
        return await this.client.redis.get(key);
    }

    /**
     * Get log size in bytes
     */
    async getLogSize(runId: string): Promise<number> {
        const key = this.getLogKey(runId);
        return await this.client.redis.strlen(key);
    }

    /**
     * Set TTL on logs based on user plan.
     * Should be called when run starts.
     * Uses NX so a retry never resets an already-running countdown.
     */
    async setLogTtl(runId: string, plan: UserPlan): Promise<void> {
        const key = this.getLogKey(runId);
        const ttl = this.client.ttlManager.getTtl('logs', plan);
        if (ttl !== null && ttl > 0) {
            await this.client.ttlManager.setExpiry(this.client.redis, [key], ttl);
        }
    }

    /**
     * Upload logs to S3 for long-term storage.
     * Called after run completes.
     */
    async uploadToS3(runId: string, userId: string): Promise<string | null> {
        if (!this.config.s3Bucket) {
            console.log(`[LogManager] S3 not configured, skipping upload for run ${runId}`);
            return null;
        }

        const logs = await this.getLogs(runId);
        if (!logs) {
            console.log(`[LogManager] No logs to upload for run ${runId}`);
            return null;
        }

        const s3Key = `logs/${userId}/${runId}.log`;

        // Note: NOT DONE — wire up AWS SDK upload here

        return s3Key;
    }

    /**
     * Delete logs from Redis (after S3 upload or when no longer needed)
     */
    async deleteLogs(runId: string): Promise<void> {
        const pipeline = this.client.redis.pipeline();
        pipeline.del(this.getLogKey(runId));
        pipeline.del(this.getTruncatedKey(runId));
        await pipeline.exec();
    }

    /**
     * Stream logs for real-time tailing.
     * Returns an async generator that yields log chunks as they're written.
     * Automatically stops when the run finishes (success, failed, timed_out, killed).
     *
     * Fix (offset invalidation): if a truncation has occurred since the last
     * poll (i.e. currentLength < lastOffset), the offset is reset to 0 so we
     * re-stream from the start of the new (shorter) buffer instead of silently
     * dropping bytes.
     */
    async *streamLogs(runId: string, options?: { pollInterval?: number }): AsyncGenerator<string, void, unknown> {
        const pollInterval = options?.pollInterval ?? 100;
        const key = this.getLogKey(runId);
        const metadataManager = new RunMetadataManager(this.client);
        let lastOffset = 0;

        const finishedStatuses: RunStatus[] = ['success', 'failed', 'timed_out', 'killed'];

        while (true) {
            // Check if run is still active
            const metadata = await metadataManager.get(runId);

            // If metadata is null the run doesn't exist (never created or already deleted).
            // Treat this as a terminal condition — yield any buffered bytes and stop.
            if (!metadata) {
                const currentLength = await this.client.redis.strlen(key);
                // Reset offset if the buffer was truncated underneath us
                if (currentLength < lastOffset) lastOffset = 0;
                if (currentLength > lastOffset) {
                    const remaining = await this.client.redis.getrange(key, lastOffset, -1);
                    if (remaining) yield remaining;
                }
                return;
            }

            if (finishedStatuses.includes(metadata.status)) {
                // Run finished — yield any remaining logs and stop
                const currentLength = await this.client.redis.strlen(key);
                // Reset offset if the buffer was truncated underneath us
                if (currentLength < lastOffset) lastOffset = 0;
                if (currentLength > lastOffset) {
                    const remaining = await this.client.redis.getrange(key, lastOffset, -1);
                    if (remaining) yield remaining;
                }
                return;
            }

            // Yield any new logs
            const currentLength = await this.client.redis.strlen(key);
            if (currentLength < lastOffset) {
                // Buffer was truncated while we were streaming — reset and re-read
                lastOffset = 0;
            }
            if (currentLength > lastOffset) {
                const newData = await this.client.redis.getrange(key, lastOffset, -1);
                if (newData) {
                    yield newData;
                }
                lastOffset = currentLength;
            }

            await new Promise(resolve => setTimeout(resolve, pollInterval));
        }
    }

    /**
     * Get log metadata (for API responses).
     *
     * `wasTruncated` replaces the previous `hasMore: false` which was always
     * false and conveyed no useful information.  When `wasTruncated` is true
     * the caller should surface a notice like "Log was truncated — only the
     * trailing portion is shown."
     */
    async getLogMetadata(runId: string): Promise<{
        size: number;
        maxSize: number;
        wasTruncated: boolean;
    }> {
        const [size, truncatedFlag] = await Promise.all([
            this.getLogSize(runId),
            this.client.redis.get(this.getTruncatedKey(runId)),
        ]);
        return {
            size,
            maxSize: this.config.maxLogSize,
            wasTruncated: truncatedFlag === '1',
        };
    }

    /**
     * Create LogConfig from environment variables
     */
    static fromEnv(): LogConfig {
        return {
            maxLogSize: parseInt(process.env.SCRAPELY_LOG_MAX_SIZE ?? '', 10) || DEFAULT_LOG_CONFIG.maxLogSize,
            s3Bucket: process.env.SCRAPELY_S3_LOG_BUCKET,
            s3Region: process.env.SCRAPELY_S3_REGION || 'us-east-1',
            s3Endpoint: process.env.SCRAPELY_S3_ENDPOINT,
        };
    }
}
