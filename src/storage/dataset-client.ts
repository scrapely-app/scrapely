import type { DatasetClient, DatasetClientListOptions, DatasetClientUpdateOptions, DatasetInfo, PaginatedList, Dictionary } from '@crawlee/types';

/**
 * Extended options for listing dataset items with filtering support.
 * Matches Crawlee's DatasetDataOptions interface.
 */
interface ExtendedDatasetListOptions extends DatasetClientListOptions {
    /** An array of field names that will be included in the result. */
    fields?: string[];
    /** If true then the function returns only non-empty items and skips hidden fields. */
    clean?: boolean;
    /** If true then the function doesn't return hidden fields (fields starting with "#"). */
    skipHidden?: boolean;
    /** If true then the function doesn't return empty items. */
    skipEmpty?: boolean;
}
import type { DragonflyClient } from './dragonfly-client.js';
import { nowISO } from './utils.js';
import { BillingTracker } from './billing-tracker.js';
import { pushItemsAtomic } from './dataset-push-script.js';

function makeAsyncIterableResult<T>(promiseFn: () => Promise<PaginatedList<T>>): AsyncIterable<T> & Promise<PaginatedList<T>> {
    const promise = promiseFn();
    const iterable = {
        [Symbol.asyncIterator]: () => {
            let items: T[] | null = null;
            let index = 0;
            return {
                async next() {
                    if (!items) items = (await promise).items;
                    if (index >= items.length) return { done: true as const, value: undefined };
                    return { done: false as const, value: items[index++] };
                },
            };
        },
        then: promise.then.bind(promise),
        catch: promise.catch.bind(promise),
        finally: promise.finally.bind(promise),
        [Symbol.toStringTag]: 'AsyncIterablePromise',
    } as any;
    return iterable;
}

export class DragonflyDatasetClient<Data extends Dictionary = Dictionary> implements DatasetClient<Data> {
    private infoKey: string;
    private itemsKey: string;

    constructor(private client: DragonflyClient, private id: string) {
        this.infoKey = client.key('dataset', id, 'info');
        this.itemsKey = client.key('dataset', id, 'items');
    }

    async get(): Promise<DatasetInfo | undefined> {
        const info = await this.client.redis.hgetall(this.infoKey);
        if (!info || !info.id) return undefined;
        return {
            id: info.id,
            name: info.name || undefined,
            createdAt: new Date(info.createdAt),
            modifiedAt: new Date(info.modifiedAt),
            accessedAt: new Date(info.accessedAt),
            itemCount: parseInt(info.itemCount || '0', 10),
            storageBytes: parseInt(info.storageBytes || '0', 10),
        } as DatasetInfo & { storageBytes: number };
    }

    async update(newFields: DatasetClientUpdateOptions): Promise<Partial<DatasetInfo>> {
        const now = nowISO();
        if (newFields.name !== undefined) {
            await this.client.redis.hset(this.infoKey, 'name', newFields.name);

            const indexKey = this.client.key('datasets:index');
            const raw = await this.client.redis.hget(indexKey, `id:${this.id}`);
            if (raw) {
                const data = JSON.parse(raw);
                if (data.name) await this.client.redis.hdel(indexKey, `name:${data.name}`);
                data.name = newFields.name;
                await this.client.redis.hset(indexKey, `id:${this.id}`, JSON.stringify(data));
                await this.client.redis.hset(indexKey, `name:${newFields.name}`, JSON.stringify(data));
            }
        }
        await this.client.redis.hset(this.infoKey, 'modifiedAt', now.toISOString());
        return { name: newFields.name };
    }

    async delete(): Promise<void> {
        const info = await this.client.redis.hgetall(this.infoKey);
        await this.client.redis.del(this.infoKey, this.itemsKey);

        const indexKey = this.client.key('datasets:index');
        await this.client.redis.hdel(indexKey, `id:${this.id}`);
        if (info?.name) await this.client.redis.hdel(indexKey, `name:${info.name}`);
    }

    async downloadItems(): Promise<Buffer> {
        const result = await this._listItems();
        return Buffer.from(JSON.stringify(result.items));
    }

    listItems(options?: ExtendedDatasetListOptions): AsyncIterable<Data> & Promise<PaginatedList<Data>> {
        return makeAsyncIterableResult(() => this._listItems(options));
    }

    /**
     * Iterates over dataset entries (index-value pairs) using an async generator.
     * Required by Crawlee's Dataset.entries() method.
     */
    listEntries(options?: ExtendedDatasetListOptions): AsyncIterable<[number, Data]> & Promise<PaginatedList<[number, Data]>> {
        return makeAsyncIterableResult(async () => {
            const result = await this._listItems(options);
            const entries: [number, Data][] = result.items.map((item, idx) => [
                (result.offset ?? 0) + idx,
                item,
            ]);
            return {
                total: result.total,
                count: entries.length,
                offset: result.offset,
                limit: result.limit,
                items: entries,
            };
        });
    }

    private async _listItems(options?: ExtendedDatasetListOptions): Promise<PaginatedList<Data>> {
        const exists = await this.client.redis.exists(this.infoKey);
        if (!exists) return { total: 0, count: 0, offset: 0, limit: 0, items: [] };

        // Item indices are dense (0..N-1: pushes append sequentially and there is
        // no per-item delete), so pages can be addressed by index directly instead
        // of pulling the whole hash on every request (matches the server route).
        const totalCount = await this.client.redis.hlen(this.itemsKey);

        const desc = options?.desc ?? false;
        const offset = options?.offset ?? 0;
        const limit = options?.limit ?? totalCount;

        const pageIndices: number[] = [];
        for (let n = 0; n < limit && offset + n < totalCount; n++) {
            pageIndices.push(desc ? totalCount - 1 - offset - n : offset + n);
        }

        let items: Data[] = pageIndices.length
            ? (await this.client.redis.hmget(this.itemsKey, ...pageIndices.map(String)))
                .filter((raw): raw is string => raw !== null)
                .map(raw => JSON.parse(raw) as Data)
            : [];

        // Apply filtering options
        items = this._applyFilters(items, options);

        // Track reads for billing - increment both user-level and info hash counters
        if (items.length > 0 && this.client.billingContext) {
            const billingTracker = new BillingTracker(this.client);
            await billingTracker.trackRead(this.client.billingContext.userId, items.length);
            await this.client.redis.hincrby(this.infoKey, 'readCount', items.length);
            
            // Track internal data transfer (bytes returned to actor)
            const bytesTransferred = items.reduce((sum, item) => sum + BillingTracker.estimateItemBytes(item), 0);
            await billingTracker.trackInternalTransfer(this.client.billingContext.runId, this.client.billingContext.userId, bytesTransferred);
            
            // Register this dataset as accessed for per-run aggregation
            await this.client.redis.sadd(
                this.client.key('run', this.client.billingContext.runId, 'datasets_accessed'),
                this.id
            );
        }

        return { total: totalCount, count: items.length, offset, limit, desc: options?.desc, items };
    }

    /**
     * Apply filtering options to dataset items.
     */
    private _applyFilters(items: Data[], options?: ExtendedDatasetListOptions): Data[] {
        if (!options) return items;

        return items.map(item => {
            let filteredItem = { ...item };

            // Skip hidden fields (fields starting with "#")
            if (options.skipHidden || options.clean) {
                for (const key of Object.keys(filteredItem)) {
                    if (key.startsWith('#')) {
                        delete filteredItem[key as keyof Data];
                    }
                }
            }

            // Filter to only specified fields
            if (options.fields && options.fields.length > 0) {
                const filtered: Partial<Data> = {};
                for (const field of options.fields) {
                    if (field in filteredItem) {
                        filtered[field as keyof Data] = filteredItem[field as keyof Data];
                    }
                }
                filteredItem = filtered as Data;
            }

            return filteredItem;
        }).filter(item => {
            // Skip empty items
            if (options.skipEmpty || options.clean) {
                return Object.keys(item).length > 0;
            }
            return true;
        }) as Data[];
    }

    async pushItems(items: Data | Data[] | string | string[], billingContext?: { runId: string; userId: string }): Promise<void> {
        const exists = await this.client.redis.exists(this.infoKey);
        if (!exists) throw new Error(`Dataset with id: ${this.id} does not exist.`);

        let parsed: Data[];
        if (typeof items === 'string') {
            try { parsed = JSON.parse(items); } catch { throw new Error('Data items must be a JSON object, array, or JSON string.'); }
            if (!Array.isArray(parsed)) parsed = [parsed];
        } else if (Array.isArray(items)) {
            parsed = items.map(i => (typeof i === 'string' ? JSON.parse(i) : i));
        } else if (typeof items === 'object' && items !== null) {
            parsed = [items];
        } else {
            throw new Error('Data items must be a JSON object, array, or JSON string.');
        }

        for (const item of parsed) {
            if (typeof item !== 'object' || item === null || Array.isArray(item)) {
                throw new Error('Each item must be a JSON object.');
            }
        }

        // Calculate total bytes for billing
        let totalBytes = 0;
        const serializedItems: string[] = [];

        for (let i = 0; i < parsed.length; i++) {
            const serialized = JSON.stringify(parsed[i]);
            serializedItems.push(serialized);
            totalBytes += BillingTracker.estimateItemBytes(parsed[i]);
        }

        // Atomic slot allocation: the previous read-then-write of itemCount in
        // separate round trips allowed concurrent pushes to allocate the same
        // slot indices, silently overwriting items.
        await pushItemsAtomic(this.client.redis, this.infoKey, this.itemsKey, serializedItems, totalBytes);

        // Track billing using client's billing context
        if (this.client.billingContext) {
            const billingTracker = new BillingTracker(this.client);
            await billingTracker.trackStorageBytes(this.client.billingContext.runId, this.client.billingContext.userId, totalBytes);
            await billingTracker.trackWrite(this.client.billingContext.userId, parsed.length);
            await this.client.redis.hincrby(this.infoKey, 'writeCount', parsed.length);
            
            // Track internal data transfer (bytes written to storage)
            await billingTracker.trackInternalTransfer(this.client.billingContext.runId, this.client.billingContext.userId, totalBytes);
            
            // Register this dataset as accessed for per-run aggregation
            await this.client.redis.sadd(
                this.client.key('run', this.client.billingContext.runId, 'datasets_accessed'),
                this.id
            );
        }
    }
}
