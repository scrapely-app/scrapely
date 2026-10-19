import { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand, ListObjectsV2Command, HeadObjectCommand } from '@aws-sdk/client-s3';
import type { UserPlan } from './ttl-manager.js';

/**
 * S3 storage configuration for Hetzner Object Storage
 */
export interface S3Config {
    endpoint: string;
    accessKey: string;
    secretKey: string;
    bucket: string;
    region?: string;
}

/**
 * Metadata stored with each S3 log object
 */
export interface LogMetadata {
    userId: string;
    runId: string;
    createdAt: string;
    expiresAt: string;
    userPlan: UserPlan;
    contentType: string;
    size?: number;
}

/**
 * TTL for logs based on user plan (in seconds)
 */
const LOG_TTL: Record<UserPlan, number> = {
    free: 7 * 24 * 60 * 60,    // 7 days
    paid: 31 * 24 * 60 * 60,  // 31 days
};

/**
 * S3 client for log storage with Hetzner Object Storage support.
 * 
 * Storage model:
 * - Path: logs/{userId}/{runId}.log
 * - Metadata includes TTL info for cleanup job
 * - Supports streaming for large logs
 */
export class S3LogClient {
    private client: S3Client;
    private bucket: string;

    constructor(config: S3Config) {
        this.bucket = config.bucket;
        this.client = new S3Client({
            endpoint: config.endpoint,
            region: config.region || 'us-east-1',
            credentials: {
                accessKeyId: config.accessKey,
                secretAccessKey: config.secretKey,
            },
            forcePathStyle: true, // Required for Hetzner S3-compatible storage
        });
    }

    /**
     * Get the S3 key for a run's log
     */
    private getLogKey(userId: string, runId: string): string {
        return `logs/${userId}/${runId}.log`;
    }

    /**
     * Calculate expiration timestamp based on user plan
     */
    private calculateExpiresAt(userPlan: UserPlan): string {
        const ttlSeconds = LOG_TTL[userPlan];
        const expiresAt = new Date(Date.now() + ttlSeconds * 1000);
        return expiresAt.toISOString();
    }

    /**
     * Upload log content to S3 with metadata for TTL tracking.
     * Called when a run completes.
     */
    async uploadLog(
        userId: string,
        runId: string,
        content: string | Buffer,
        userPlan: UserPlan
    ): Promise<{ key: string; metadata: LogMetadata }> {
        const now = new Date();
        const key = this.getLogKey(userId, runId);
        const expiresAt = this.calculateExpiresAt(userPlan);
        const buffer = typeof content === 'string' ? Buffer.from(content, 'utf-8') : content;

        const metadata: LogMetadata = {
            userId,
            runId,
            createdAt: now.toISOString(),
            expiresAt,
            userPlan,
            contentType: 'text/plain; charset=utf-8',
            size: buffer.length,
        };

        await this.client.send(new PutObjectCommand({
            Bucket: this.bucket,
            Key: key,
            Body: buffer,
            ContentType: 'text/plain; charset=utf-8',
            Metadata: {
                'user-id': userId,
                'run-id': runId,
                'created-at': metadata.createdAt,
                'expires-at': expiresAt,
                'user-plan': userPlan,
            },
        }));

        return { key, metadata };
    }

    /**
     * Get log content from S3.
     * Returns null if not found.
     */
    async getLog(userId: string, runId: string): Promise<string | null> {
        const key = this.getLogKey(userId, runId);

        try {
            const response = await this.client.send(new GetObjectCommand({
                Bucket: this.bucket,
                Key: key,
            }));

            if (!response.Body) return null;

            const content = await response.Body.transformToString('utf-8');
            return content;
        } catch (error: any) {
            if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
                return null;
            }
            throw error;
        }
    }

    /**
     * Get log as a stream for large logs.
     */
    async getLogStream(userId: string, runId: string): Promise<NodeJS.ReadableStream | null> {
        const key = this.getLogKey(userId, runId);

        try {
            const response = await this.client.send(new GetObjectCommand({
                Bucket: this.bucket,
                Key: key,
            }));

            if (!response.Body) return null;

            // Convert WebStream to NodeJS ReadableStream if needed
            return response.Body as unknown as NodeJS.ReadableStream;
        } catch (error: any) {
            if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
                return null;
            }
            throw error;
        }
    }

    /**
     * Get metadata for a log object without fetching content.
     */
    async getLogMetadata(userId: string, runId: string): Promise<LogMetadata | null> {
        const key = this.getLogKey(userId, runId);

        try {
            const response = await this.client.send(new HeadObjectCommand({
                Bucket: this.bucket,
                Key: key,
            }));

            const metadata = response.Metadata || {};
            return {
                userId: metadata['user-id'] || userId,
                runId: metadata['run-id'] || runId,
                createdAt: metadata['created-at'] || new Date().toISOString(),
                expiresAt: metadata['expires-at'] || '',
                userPlan: (metadata['user-plan'] as UserPlan) || 'free',
                contentType: 'text/plain; charset=utf-8',
                size: response.ContentLength,
            };
        } catch (error: any) {
            if (error.$metadata?.httpStatusCode === 404) {
                return null;
            }
            throw error;
        }
    }

    /**
     * Delete a log from S3.
     */
    async deleteLog(userId: string, runId: string): Promise<boolean> {
        const key = this.getLogKey(userId, runId);

        try {
            await this.client.send(new DeleteObjectCommand({
                Bucket: this.bucket,
                Key: key,
            }));
            return true;
        } catch (error: any) {
            if (error.$metadata?.httpStatusCode === 404) {
                return false;
            }
            throw error;
        }
    }

    /**
     * List logs that have expired (for cleanup job).
     * Returns up to 1000 expired logs at a time.
     */
    async listExpiredLogs(): Promise<Array<{ key: string; userId: string; runId: string; expiresAt: string }>> {
        const now = new Date();
        const expiredLogs: Array<{ key: string; userId: string; runId: string; expiresAt: string }> = [];

        let continuationToken: string | undefined;

        do {
            const response = await this.client.send(new ListObjectsV2Command({
                Bucket: this.bucket,
                Prefix: 'logs/',
                ContinuationToken: continuationToken,
            }));

            if (!response.Contents) break;

            for (const object of response.Contents) {
                if (!object.Key) continue;

                // Parse metadata to check expiration
                try {
                    const headResponse = await this.client.send(new HeadObjectCommand({
                        Bucket: this.bucket,
                        Key: object.Key,
                    }));

                    const expiresAtStr = headResponse.Metadata?.['expires-at'];
                    if (!expiresAtStr) continue;

                    const expiresAt = new Date(expiresAtStr);
                    if (expiresAt <= now) {
                        // Parse userId and runId from key: logs/{userId}/{runId}.log
                        const parts = object.Key.split('/');
                        if (parts.length >= 3) {
                            const userId = parts[1];
                            const runId = parts[2].replace('.log', '');
                            expiredLogs.push({
                                key: object.Key,
                                userId,
                                runId,
                                expiresAt: expiresAtStr,
                            });
                        }
                    }
                } catch {
                    // Skip objects we can't read metadata for
                    continue;
                }
            }

            continuationToken = response.NextContinuationToken;
        } while (continuationToken);

        return expiredLogs;
    }

    /**
     * Delete multiple expired logs in batch.
     * Returns count of deleted objects.
     */
    async deleteExpiredLogs(): Promise<{ deleted: number; errors: number }> {
        const expiredLogs = await this.listExpiredLogs();
        let deleted = 0;
        let errors = 0;

        for (const log of expiredLogs) {
            try {
                await this.deleteLog(log.userId, log.runId);
                deleted++;
            } catch {
                errors++;
            }
        }

        return { deleted, errors };
    }

    /**
     * Check if S3 is properly configured and accessible.
     */
    async healthCheck(): Promise<boolean> {
        try {
            await this.client.send(new HeadObjectCommand({
                Bucket: this.bucket,
                Key: '__health_check__',
            }));
            return true; // Object exists or bucket is accessible
        } catch (error: any) {
            // NoSuchKey means bucket exists but object doesn't - still healthy
            if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
                return true;
            }
            return false;
        }
    }

    /**
     * Create S3Config from environment variables.
     */
    static fromEnv(): S3Config {
        return {
            endpoint: process.env.S3_ENDPOINT || 'https://nbg1.your-objectstorage.com',
            accessKey: process.env.S3_ACCESS_KEY || '',
            secretKey: process.env.S3_SECRET_KEY || '',
            bucket: process.env.S3_BUCKET || 'scrapely-logs',
            region: process.env.S3_REGION || 'us-east-1',
        };
    }
}

// Singleton instance
let s3LogClient: S3LogClient | null = null;

/**
 * Get or create the S3 log client singleton.
 */
export function getS3LogClient(): S3LogClient | null {
    const config = S3LogClient.fromEnv();
    if (!config.accessKey || !config.secretKey) {
        return null;
    }
    if (!s3LogClient) {
        s3LogClient = new S3LogClient(config);
    }
    return s3LogClient;
}

/**
 * Initialize S3 client with explicit config.
 */
export function initS3LogClient(config: S3Config): S3LogClient {
    s3LogClient = new S3LogClient(config);
    return s3LogClient;
}