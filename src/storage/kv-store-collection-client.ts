import type { KeyValueStoreCollectionClient, KeyValueStoreInfo, PaginatedList } from '@crawlee/types';
import type { DragonflyClient } from './dragonfly-client.js';
import { generateId, nowISO } from './utils.js';

export class DragonflyKeyValueStoreCollectionClient implements KeyValueStoreCollectionClient {
    constructor(private client: DragonflyClient) {}

    async getOrCreate(name?: string): Promise<KeyValueStoreInfo> {
        const indexKey = this.client.key('kvs:index');

        if (name) {
            const existing = await this.client.redis.hget(indexKey, `name:${name}`);
            if (existing) return JSON.parse(existing);
        }

        const id = generateId();
        const now = nowISO();
        // Derive ownership from the active billing context (as the request-queue
        // client does); the server authorizes against this persisted field.
        // The legacy '1' placeholder applies only when no context is present.
        const userId = this.client.billingContext?.userId ?? '1';
        const data: KeyValueStoreInfo = {
            id,
            name,
            userId,
            createdAt: now,
            modifiedAt: now,
            accessedAt: now,
        };

        await this.client.redis.hset(indexKey, `id:${id}`, JSON.stringify(data));
        if (name) await this.client.redis.hset(indexKey, `name:${name}`, JSON.stringify(data));

        const infoKey = this.client.key('kvs', id, 'info');
        const recordsKey = this.client.key('kvs', id, 'records');
        const metaKey = this.client.key('kvs', id, 'meta');

        await this.client.redis.hset(infoKey, {
            id,
            name: name ?? '',
            userId,
            createdAt: now.toISOString(),
            modifiedAt: now.toISOString(),
            accessedAt: now.toISOString(),
        });

        // Set TTL for unnamed KV stores (named stores have indefinite retention)
        if (!name) {
            const ttl = this.client.ttlManager.getTtl('kv', this.client.userPlan, false);
            if (ttl) {
                await this.client.ttlManager.setExpiry(this.client.redis, [infoKey, recordsKey, metaKey], ttl);
            }
        }

        return data;
    }

    async list(): Promise<PaginatedList<KeyValueStoreInfo>> {
        const indexKey = this.client.key('kvs:index');
        const all = await this.client.redis.hgetall(indexKey);
        const items: KeyValueStoreInfo[] = [];
        const seen = new Set<string>();

        for (const [key, val] of Object.entries(all)) {
            if (!key.startsWith('id:')) continue;
            const parsed = JSON.parse(val);
            if (seen.has(parsed.id)) continue;
            seen.add(parsed.id);

            const infoKey = this.client.key('kvs', parsed.id, 'info');

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
