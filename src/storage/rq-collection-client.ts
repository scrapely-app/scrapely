import type { RequestQueueCollectionClient, RequestQueueInfo, PaginatedList } from '@crawlee/types';
import type { DragonflyClient } from './dragonfly-client.js';
import { generateId, nowISO } from './utils.js';

export class DragonflyRequestQueueCollectionClient implements RequestQueueCollectionClient {
    constructor(private client: DragonflyClient) {}

    async getOrCreate(name?: string): Promise<RequestQueueInfo> {
        const indexKey = this.client.key('rq:index');

        if (name) {
            const existing = await this.client.redis.hget(indexKey, `name:${name}`);
            if (existing) return JSON.parse(existing);
        }

        const id = generateId();
        const now = nowISO();
        // Real userId when billing context is active; legacy placeholder otherwise.
        // Matches the KVS collection client convention — the server's storage usage
        // and TTL jobs match on this field to attribute storages to users, so an
        // empty value would orphan the queue from per-user accounting.
        const userId = this.client.billingContext?.userId ?? '1';
        const data: RequestQueueInfo = {
            id,
            name,
            userId,
            createdAt: now,
            modifiedAt: now,
            accessedAt: now,
            totalRequestCount: 0,
            handledRequestCount: 0,
            pendingRequestCount: 0,
            hadMultipleClients: false,
            stats: { readCount: 0, writeCount: 0, deleteCount: 0, headItemReadCount: 0, storageBytes: 0 },
        };

        await this.client.redis.hset(indexKey, `id:${id}`, JSON.stringify(data));
        if (name) await this.client.redis.hset(indexKey, `name:${name}`, JSON.stringify(data));

        const infoKey = this.client.key('rq', id, 'info');
        const requestsKey = this.client.key('rq', id, 'requests');
        const pendingKey = this.client.key('rq', id, 'pending');
        const lockedKey = this.client.key('rq', id, 'locked');
        const lockOwnersKey = this.client.key('rq', id, 'lockOwners');

        await this.client.redis.hset(infoKey, {
            id,
            name: name ?? '',
            userId,
            totalRequestCount: '0',
            handledRequestCount: '0',
            pendingRequestCount: '0',
            hadMultipleClients: '0',
            createdAt: now.toISOString(),
            modifiedAt: now.toISOString(),
            accessedAt: now.toISOString(),
        });

        // Set 24h fallback TTL on all RQ keys (safety net for orphaned RQs from crashed containers)
        // RQs should be explicitly deleted after run completes, this TTL is just a fallback
        const ttl = this.client.ttlManager.getTtl('rq', this.client.userPlan);
        await this.client.ttlManager.setExpiry(this.client.redis, [infoKey, requestsKey, pendingKey, lockedKey, lockOwnersKey], ttl);

        return data;
    }

    async list(): Promise<PaginatedList<RequestQueueInfo>> {
        const indexKey = this.client.key('rq:index');
        const all = await this.client.redis.hgetall(indexKey);
        const items: RequestQueueInfo[] = [];
        const seen = new Set<string>();

        for (const [key, val] of Object.entries(all)) {
            if (!key.startsWith('id:')) continue;
            const parsed = JSON.parse(val);
            if (seen.has(parsed.id)) continue;
            seen.add(parsed.id);

            const infoKey = this.client.key('rq', parsed.id, 'info');

            // Skip and clean up stale index entries whose keys have expired via TTL
            const exists = await this.client.redis.exists(infoKey);
            if (!exists) {
                await this.client.redis.hdel(indexKey, `id:${parsed.id}`);
                if (parsed.name) await this.client.redis.hdel(indexKey, `name:${parsed.name}`);
                continue;
            }

            items.push(parsed);
        }

        return { total: items.length, count: items.length, offset: 0, limit: items.length, items };
    }
}
