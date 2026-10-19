import type { DatasetCollectionClient, DatasetCollectionData, PaginatedList, Dataset } from '@crawlee/types';
import type { DragonflyClient } from './dragonfly-client.js';
import { generateId, nowISO } from './utils.js';

export class DragonflyDatasetCollectionClient implements DatasetCollectionClient {
    constructor(private client: DragonflyClient) {}

    async getOrCreate(name?: string): Promise<DatasetCollectionData> {
        const indexKey = this.client.key('datasets:index');

        if (name) {
            const existing = await this.client.redis.hget(indexKey, `name:${name}`);
            if (existing) return JSON.parse(existing);
        }

        const id = generateId();
        const now = nowISO();
        const data: DatasetCollectionData = {
            id,
            name,
            createdAt: now,
            modifiedAt: now,
            accessedAt: now,
        };

        await this.client.redis.hset(indexKey, `id:${id}`, JSON.stringify(data));
        if (name) await this.client.redis.hset(indexKey, `name:${name}`, JSON.stringify(data));

        const infoKey = this.client.key('dataset', id, 'info');
        const itemsKey = this.client.key('dataset', id, 'items');

        await this.client.redis.hset(infoKey, {
            id,
            name: name ?? '',
            itemCount: '0',
            createdAt: now.toISOString(),
            modifiedAt: now.toISOString(),
            accessedAt: now.toISOString(),
        });

        // Set TTL for unnamed datasets (named datasets have indefinite retention)
        if (!name) {
            const ttl = this.client.ttlManager.getTtl('dataset', this.client.userPlan, false);
            if (ttl) {
                await this.client.ttlManager.setExpiry(this.client.redis, [infoKey, itemsKey], ttl);
            }
        }

        return data;
    }

    async list(): Promise<PaginatedList<Dataset>> {
        const indexKey = this.client.key('datasets:index');
        const all = await this.client.redis.hgetall(indexKey);
        const items: Dataset[] = [];
        const seen = new Set<string>();

        for (const [key, val] of Object.entries(all)) {
            if (!key.startsWith('id:')) continue;
            const parsed = JSON.parse(val);
            if (seen.has(parsed.id)) continue;
            seen.add(parsed.id);

            const infoKey = this.client.key('dataset', parsed.id, 'info');

            // Skip and clean up stale index entries whose keys have expired via TTL
            const exists = await this.client.redis.exists(infoKey);
            if (!exists) {
                await this.client.redis.hdel(indexKey, `id:${parsed.id}`);
                if (parsed.name) await this.client.redis.hdel(indexKey, `name:${parsed.name}`);
                continue;
            }

            const itemCount = parseInt(await this.client.redis.hget(infoKey, 'itemCount') || '0', 10);
            items.push({ ...parsed, itemCount });
        }

        return { total: items.length, count: items.length, offset: 0, limit: items.length, items };
    }
}
