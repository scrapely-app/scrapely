import type {
    KeyValueStoreClientListData,
    KeyValueStoreClientListOptions,
    KeyValueStoreItemData,
} from '@crawlee/types';

export type ListKeysFn = (options?: KeyValueStoreClientListOptions) => Promise<KeyValueStoreClientListData>;

/**
 * Filter keys by collection config from a KVS schema.
 */
export function filterKeysByCollection(
    allKeys: string[],
    schemaJson: string | undefined,
    collection: string,
): string[] {
    if (!schemaJson) {
        throw new Error('Key-value store schema is not configured for collection filtering.');
    }

    let schema: { collections?: Record<string, { keyPrefix?: string; key?: string; keys?: string[] }> };
    try {
        schema = JSON.parse(schemaJson);
    } catch {
        throw new Error('Key-value store schema is invalid JSON.');
    }

    const cfg = schema?.collections?.[collection];
    if (!cfg) {
        throw new Error(`Collection '${collection}' is not defined in the key-value store schema.`);
    }

    if (cfg.keyPrefix && typeof cfg.keyPrefix === 'string') {
        return allKeys.filter((k) => k.startsWith(cfg.keyPrefix!));
    }
    if (cfg.key && typeof cfg.key === 'string') {
        return allKeys.filter((k) => k === cfg.key);
    }
    if (Array.isArray(cfg.keys)) {
        const set = new Set(cfg.keys.map(String));
        return allKeys.filter((k) => set.has(k));
    }

    return allKeys;
}

export async function* paginateKeyItems(
    listKeys: ListKeysFn,
    options: KeyValueStoreClientListOptions = {},
): AsyncGenerator<KeyValueStoreItemData> {
    let exclusiveStartKey = options.exclusiveStartKey;
    const { prefix, collection, limit } = options;

    for (;;) {
        const response = await listKeys({ prefix, collection, limit, exclusiveStartKey });
        for (const item of response.items) {
            yield item;
        }
        if (!response.isTruncated || !response.nextExclusiveStartKey) {
            break;
        }
        exclusiveStartKey = response.nextExclusiveStartKey;
    }
}

export async function forEachKeyPaginated(
    listKeys: ListKeysFn,
    iteratee: (key: string, index: number, info: { size: number }) => Promise<void> | void,
    options: KeyValueStoreClientListOptions = {},
    startIndex = 0,
): Promise<void> {
    let index = startIndex;
    let exclusiveStartKey = options.exclusiveStartKey;
    const { prefix, collection, limit } = options;

    for (;;) {
        const response = await listKeys({ prefix, collection, limit, exclusiveStartKey });
        for (const item of response.items) {
            await iteratee(item.key, index++, { size: item.size });
        }
        if (!response.isTruncated || !response.nextExclusiveStartKey) {
            return;
        }
        exclusiveStartKey = response.nextExclusiveStartKey;
    }
}

function makeHybridIterable<T, P>(
    iterate: () => AsyncGenerator<T>,
    promise: Promise<P>,
): AsyncIterable<T> & Promise<P> {
    // A native Promise has no own enumerable properties (then/catch/finally live on
    // Promise.prototype), so Object.assign(iterable, promise) copies nothing and the
    // result is not thenable — awaiting it resolved to the iterable object itself.
    // Explicitly bind the promise methods instead (same pattern as
    // makeAsyncIterableListKeys in key-value-store-client.ts).
    const iterable = {
        [Symbol.asyncIterator]: iterate,
        then: promise.then.bind(promise),
        catch: promise.catch.bind(promise),
        finally: promise.finally.bind(promise),
        [Symbol.toStringTag]: 'AsyncIterablePromise',
    } as any;
    return iterable;
}

export function makePaginatingKeysIterable(
    listKeys: ListKeysFn,
    options?: KeyValueStoreClientListOptions,
): AsyncIterable<string> & Promise<KeyValueStoreClientListData> {
    const opts = options ?? {};
    return makeHybridIterable(
        () => (async function* () {
            for await (const item of paginateKeyItems(listKeys, opts)) {
                yield item.key;
            }
        })(),
        listKeys(opts),
    );
}

export function makePaginatingValuesIterable(
    listKeys: ListKeysFn,
    getRecord: (key: string) => Promise<{ value: unknown } | undefined>,
    options?: KeyValueStoreClientListOptions,
): AsyncIterable<unknown> & Promise<unknown[]> {
    const opts = options ?? {};
    return makeHybridIterable(
        () => (async function* () {
            for await (const item of paginateKeyItems(listKeys, opts)) {
                const record = await getRecord(item.key);
                if (record) yield record.value;
            }
        })(),
        (async () => {
            const values: unknown[] = [];
            for await (const item of paginateKeyItems(listKeys, opts)) {
                const record = await getRecord(item.key);
                if (record) values.push(record.value);
            }
            return values;
        })(),
    );
}

export function makePaginatingEntriesIterable(
    listKeys: ListKeysFn,
    getRecord: (key: string) => Promise<{ value: unknown } | undefined>,
    options?: KeyValueStoreClientListOptions,
): AsyncIterable<[string, unknown]> & Promise<[string, unknown][]> {
    const opts = options ?? {};
    return makeHybridIterable(
        () => (async function* () {
            for await (const item of paginateKeyItems(listKeys, opts)) {
                const record = await getRecord(item.key);
                if (record) yield [item.key, record.value] as [string, unknown];
            }
        })(),
        (async () => {
            const entries: [string, unknown][] = [];
            for await (const item of paginateKeyItems(listKeys, opts)) {
                const record = await getRecord(item.key);
                if (record) entries.push([item.key, record.value]);
            }
            return entries;
        })(),
    );
}
