import type {
    KeyValueStoreClient, KeyValueStoreInfo, KeyValueStoreRecord,
    KeyValueStoreClientGetRecordOptions, KeyValueStoreClientListOptions,
    KeyValueStoreClientUpdateOptions, KeyValueStoreClientListData, KeyValueStoreItemData,
    KeyValueStoreRecordOptions,
} from '@crawlee/types';
import { Readable } from 'node:stream';
import type { DragonflyClient } from './dragonfly-client.js';
import { nowISO } from './utils.js';
import { BillingTracker } from './billing-tracker.js';
import { Configuration } from '../configuration.js';
import {
    filterKeysByCollection,
    forEachKeyPaginated,
    makePaginatingEntriesIterable,
    makePaginatingKeysIterable,
    makePaginatingValuesIterable,
} from './kvs-iteration.js';

/**
 * Type for the iteratee function used in forEachKey
 */
export type KeyConsumer = (key: string, index: number, info: { size: number }) => Promise<void> | void;

/**
 * Options for forEachKey iteration
 */
export interface ForEachKeyOptions extends KeyValueStoreClientListOptions {}

function makeAsyncIterableListKeys(
    promiseFn: () => Promise<KeyValueStoreClientListData>,
): Partial<AsyncIterable<KeyValueStoreItemData>> & Promise<KeyValueStoreClientListData> {
    const promise = promiseFn();
    const iterable = {
        [Symbol.asyncIterator]: () => {
            let items: KeyValueStoreItemData[] | null = null;
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

export class DragonflyKeyValueStoreClient implements KeyValueStoreClient {
    private infoKey: string;
    private recordsKey: string;
    private metaKey: string;

    constructor(private client: DragonflyClient, private id: string) {
        this.infoKey = client.key('kvs', id, 'info');
        this.recordsKey = client.key('kvs', id, 'records');
        this.metaKey = client.key('kvs', id, 'meta');
    }

    private estimateStoredRecordBytes(key: string, stored: string | null, contentType?: string): number {
        if (stored === null) return 0;
        if (contentType === 'application/octet-stream') {
            return BillingTracker.estimateRecordBytes(key, Buffer.from(stored, 'base64'));
        }
        return BillingTracker.estimateRecordBytes(key, stored);
    }

    async get(): Promise<KeyValueStoreInfo | undefined> {
        const info = await this.client.redis.hgetall(this.infoKey);
        if (!info || !info.id) return undefined;
        return {
            id: info.id,
            name: info.name || undefined,
            userId: info.userId,
            createdAt: new Date(info.createdAt),
            modifiedAt: new Date(info.modifiedAt),
            accessedAt: new Date(info.accessedAt),
        };
    }

    async update(newFields: KeyValueStoreClientUpdateOptions): Promise<Partial<KeyValueStoreInfo>> {
        if (newFields.name !== undefined) {
            await this.client.redis.hset(this.infoKey, 'name', newFields.name);
            const indexKey = this.client.key('kvs:index');
            const raw = await this.client.redis.hget(indexKey, `id:${this.id}`);
            if (raw) {
                const data = JSON.parse(raw);
                if (data.name) await this.client.redis.hdel(indexKey, `name:${data.name}`);
                data.name = newFields.name;
                await this.client.redis.hset(indexKey, `id:${this.id}`, JSON.stringify(data));
                await this.client.redis.hset(indexKey, `name:${newFields.name}`, JSON.stringify(data));
            }
        }
        await this.client.redis.hset(this.infoKey, 'modifiedAt', nowISO().toISOString());
        return { name: newFields.name };
    }

    async delete(): Promise<void> {
        const info = await this.client.redis.hgetall(this.infoKey);
        await this.client.redis.del(this.infoKey, this.recordsKey, this.metaKey);
        const indexKey = this.client.key('kvs:index');
        await this.client.redis.hdel(indexKey, `id:${this.id}`);
        if (info?.name) await this.client.redis.hdel(indexKey, `name:${info.name}`);
    }

    async recordExists(key: string): Promise<boolean> {
        return (await this.client.redis.hexists(this.recordsKey, key)) === 1;
    }

    async getRecord(key: string, options?: KeyValueStoreClientGetRecordOptions): Promise<KeyValueStoreRecord | undefined> {
        const raw = await this.client.redis.hget(this.recordsKey, key);
        if (raw === null) return undefined;

        // Track read for billing - increment both user-level and info hash counters
        if (this.client.billingContext) {
            const billingTracker = new BillingTracker(this.client);
            await billingTracker.trackRead(this.client.billingContext.userId, 1);
            await this.client.redis.hincrby(this.infoKey, 'readCount', 1);
            
            // Track internal data transfer (bytes returned to actor)
            const bytesTransferred = Buffer.byteLength(raw, 'utf8');
            await billingTracker.trackInternalTransfer(this.client.billingContext.runId, this.client.billingContext.userId, bytesTransferred);
            
            // Register this store as accessed for per-run aggregation
            await this.client.redis.sadd(
                this.client.key('run', this.client.billingContext.runId, 'kvs_accessed'),
                this.id
            );
        }

        const meta = await this.client.redis.hget(this.metaKey, key);
        const contentType = meta ? JSON.parse(meta).contentType : undefined;
        const isBinary = contentType === 'application/octet-stream';

        let value: any;
        if (isBinary) {
            value = Buffer.from(raw, 'base64');
        } else if (options?.buffer) {
            value = Buffer.from(raw);
        } else {
            try { value = JSON.parse(raw); } catch { value = raw; }
        }

        if (options?.stream) {
            const buf = Buffer.isBuffer(value) ? value : Buffer.from(typeof value === 'string' ? value : JSON.stringify(value));
            value = Readable.from(buf);
        }

        return { key, value, contentType };
    }

    async setRecord(record: KeyValueStoreRecord, _options?: KeyValueStoreRecordOptions, billingContext?: { runId: string; userId: string }): Promise<void> {
        const { key, value, contentType } = record;
        let stored: string;
        let billingValue: any = value;  // Use for billing estimation (may differ from original if stream)

        // Handle Readable streams (e.g., fs.createReadStream)
        // Streams are objects with a pipe method. We need to buffer them.
        if (value && typeof value === 'object' && typeof (value as any).pipe === 'function') {
            const chunks: Buffer[] = [];
            for await (const chunk of value) {
                chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
            }
            const buffered = Buffer.concat(chunks);
            stored = buffered.toString('base64');
            billingValue = buffered;  // Use buffered value for billing estimation
        } else if (Buffer.isBuffer(value)) {
            stored = value.toString('base64');
        } else if (typeof value === 'string') {
            stored = value;
        } else {
            stored = JSON.stringify(value);
        }

        const [existingRaw, existingMetaRaw] = await Promise.all([
            this.client.redis.hget(this.recordsKey, key),
            this.client.redis.hget(this.metaKey, key),
        ]);
        const existingContentType = existingMetaRaw ? JSON.parse(existingMetaRaw).contentType as string | undefined : undefined;
        const previousBytes = this.estimateStoredRecordBytes(key, existingRaw, existingContentType);
        const nextBytes = BillingTracker.estimateRecordBytes(key, billingValue);
        const deltaBytes = nextBytes - previousBytes;

        const pipeline = this.client.redis.pipeline();
        pipeline.hset(this.recordsKey, key, stored);

        if (contentType) {
            pipeline.hset(this.metaKey, key, JSON.stringify({ contentType }));
        }

        pipeline.hset(this.infoKey, 'modifiedAt', nowISO().toISOString());
        await pipeline.exec();

        // Track billing using client's billing context
        if (this.client.billingContext) {
            const billingTracker = new BillingTracker(this.client);
            if (deltaBytes !== 0) {
                await billingTracker.trackStorageBytes(this.client.billingContext.runId, this.client.billingContext.userId, deltaBytes);
            }
            await billingTracker.trackWrite(this.client.billingContext.userId);
            await this.client.redis.hincrby(this.infoKey, 'writeCount', 1);
            
            // Track internal data transfer (bytes written to storage)
            await billingTracker.trackInternalTransfer(this.client.billingContext.runId, this.client.billingContext.userId, nextBytes);
            
            // Register this store as accessed for per-run aggregation
            await this.client.redis.sadd(
                this.client.key('run', this.client.billingContext.runId, 'kvs_accessed'),
                this.id
            );
        }
    }

    async deleteRecord(key: string): Promise<void> {
        const [existingRaw, existingMetaRaw] = await Promise.all([
            this.client.redis.hget(this.recordsKey, key),
            this.client.redis.hget(this.metaKey, key),
        ]);
        const pipeline = this.client.redis.pipeline();
        pipeline.hdel(this.recordsKey, key);
        pipeline.hdel(this.metaKey, key);
        pipeline.hset(this.infoKey, 'modifiedAt', nowISO().toISOString());
        await pipeline.exec();

        if (this.client.billingContext && existingRaw !== null) {
            const billingTracker = new BillingTracker(this.client);
            const existingContentType = existingMetaRaw ? JSON.parse(existingMetaRaw).contentType as string | undefined : undefined;
            const releasedBytes = this.estimateStoredRecordBytes(key, existingRaw, existingContentType);
            if (releasedBytes > 0) {
                await billingTracker.trackStorageBytes(
                    this.client.billingContext.runId,
                    this.client.billingContext.userId,
                    -releasedBytes,
                );
            }
        }
    }

    listKeys(options?: KeyValueStoreClientListOptions): Partial<AsyncIterable<KeyValueStoreItemData>> & Promise<KeyValueStoreClientListData> {
        return makeAsyncIterableListKeys(() => this._listKeys(options));
    }

    private async _listKeys(options?: KeyValueStoreClientListOptions): Promise<KeyValueStoreClientListData> {
        // HKEYS returns only field names; HGETALL would transfer every record's
        // value (including base64-encoded blobs) just to discard them here.
        let allKeys = await this.client.redis.hkeys(this.recordsKey);

        if (options?.prefix) {
            allKeys = allKeys.filter(k => k.startsWith(options.prefix!));
        }

        if (options?.collection) {
            const info = await this.client.redis.hgetall(this.infoKey);
            allKeys = filterKeysByCollection(allKeys, info.schema, options.collection);
        }

        allKeys.sort();

        if (options?.exclusiveStartKey) {
            // Keys are sorted, so keep everything strictly after the start key.
            // Comparing (instead of indexOf) also handles the start key having been
            // deleted between pages, which previously restarted the page from the
            // beginning and could make paginating iterators loop forever.
            allKeys = allKeys.filter((k) => k > options.exclusiveStartKey!);
        }

        const limit = options?.limit ?? allKeys.length;
        const isTruncated = allKeys.length > limit;
        const sliced = allKeys.slice(0, limit);

        // Fetch value lengths only (HSTRLEN) in one pipelined round trip;
        // this avoids transferring each record's full value just to compute its size.
        const pipeline = this.client.redis.pipeline();
        for (const key of sliced) pipeline.hstrlen(this.recordsKey, key);
        const results = await pipeline.exec();
        const items: KeyValueStoreItemData[] = sliced.map((key, i) => ({
            key,
            size: (results?.[i]?.[1] as number) ?? 0,
        }));

        // Track list operation for billing
        if (this.client.billingContext) {
            const billingTracker = new BillingTracker(this.client);
            await billingTracker.trackList(this.client.billingContext.userId, 1);
            // Increment the per-store listCount so the worker can tally KVS list operations
            await this.client.redis.hincrby(this.infoKey, 'listCount', 1);
            // Register this store as accessed for per-run aggregation (always, even if empty)
            await this.client.redis.sadd(
                this.client.key('run', this.client.billingContext.runId, 'kvs_accessed'),
                this.id
            );
        }

        return {
            count: items.length,
            limit,
            exclusiveStartKey: options?.exclusiveStartKey,
            isTruncated,
            nextExclusiveStartKey: isTruncated ? sliced[sliced.length - 1] : undefined,
            items,
        };
    }

    keys(options?: KeyValueStoreClientListOptions): AsyncIterable<string> & Promise<KeyValueStoreClientListData> {
        return makePaginatingKeysIterable((opts) => this._listKeys(opts), options);
    }

    values(options?: KeyValueStoreClientListOptions): AsyncIterable<unknown> & Promise<unknown[]> {
        return makePaginatingValuesIterable(
            (opts) => this._listKeys(opts),
            (key) => this.getRecord(key),
            options,
        );
    }

    entries(options?: KeyValueStoreClientListOptions): AsyncIterable<[string, unknown]> & Promise<[string, unknown][]> {
        return makePaginatingEntriesIterable(
            (opts) => this._listKeys(opts),
            (key) => this.getRecord(key),
            options,
        );
    }

    /**
     * Iterates over all keys in the key-value store, calling the iteratee function for each key.
     */
    async forEachKey(iteratee: KeyConsumer, options?: ForEachKeyOptions): Promise<void> {
        return forEachKeyPaginated((opts) => this._listKeys(opts), iteratee, options);
    }

    /**
     * Gets a value from the key-value store that is automatically saved when modified.
     * If the key doesn't exist, returns the defaultValue and stores it for future use.
     * 
     * @param key - The key of the record to get
     * @param defaultValue - The default value to return and store if the key doesn't exist
     * @returns The stored value or the defaultValue
     */
    async getAutoSavedValue<T>(key: string, defaultValue: T): Promise<T> {
        const record = await this.getRecord(key);
        if (record === undefined) {
            // Store the default value and return it
            await this.setRecord({ key, value: defaultValue as any });
            return defaultValue;
        }
        return record.value as T;
    }

    /**
     * Returns a URL for the given key that may be used to publicly access the value
     * in the remote key-value store.
     * 
     * @param key - The key of the record to get the public URL for
     * @returns A public URL for accessing the record
     */
    getPublicUrl(key: string): string {
        const config = Configuration.getGlobalConfig();
        const baseUrl = config.get('apiPublicBaseUrl') || config.get('apiBaseUrl') || 'https://api.scrape.ly';
        return `${baseUrl}/v2/key-value-stores/${this.id}/records/${key}`;
    }
}
