import { createHash, randomUUID } from 'node:crypto';

export function uniqueKeyToRequestId(uniqueKey: string): string {
    return createHash('sha256')
        .update(uniqueKey)
        .digest('base64')
        .replace(/(\+|\/|=)/g, '')
        .slice(0, 15);
}

export function generateId(): string {
    return randomUUID();
}

export function nowISO(): Date {
    return new Date();
}

/**
 * Percent-encode a value so it stays one URL path segment. Request ids come
 * from queue contents another client may have written: unencoded, an id such
 * as `..` or `../../key-value-stores/x` would send the call to another
 * resource. `''`, `.` and `..` are rejected because a URL parser resolves dot
 * segments even when they are percent-encoded.
 */
export function pathSegment(value: string): string {
    if (typeof value !== 'string') {
        throw new TypeError(`Expected a string for a URL path segment, got ${typeof value}.`);
    }
    if (value === '' || value === '.' || value === '..') {
        throw new Error(`"${value}" cannot be used as a URL path segment.`);
    }
    return encodeURIComponent(value);
}

/**
 * Field of a named storage in the global `{prefix}datasets:index` /
 * `kvs:index` / `rq:index` hash: scoped by owner (`name:{userId}:{name}`, the
 * same field the server writes), so two accounts using one name never share
 * or overwrite each other's pointer.
 */
export function ownerNameField(userId: string, name: string): string {
    return `name:${userId}:${name}`;
}

/** The owner a new storage is created for: the active billing context's user. */
export function requireStorageOwner(billingContext: { userId?: string } | undefined, kind: string): string {
    const userId = billingContext?.userId;
    if (!userId) throw new Error(`Cannot create a ${kind} without a billing context (no owner user id).`);
    return userId;
}

/**
 * Remove an owner's name pointer, but only while it still points at `storageId`
 * (a pointer the owner has since moved to another storage is left alone).
 */
export async function releaseOwnerName(
    redis: { hget(key: string, field: string): Promise<string | null>; hdel(key: string, ...fields: string[]): Promise<number> },
    indexKey: string,
    userId: string | null | undefined,
    name: string | null | undefined,
    storageId: string,
): Promise<void> {
    if (!userId || !name) return;
    const field = ownerNameField(userId, name);
    const raw = await redis.hget(indexKey, field);
    if (!raw) return;
    try {
        if ((JSON.parse(raw) as { id?: unknown })?.id !== storageId) return;
    } catch {
        return;
    }
    await redis.hdel(indexKey, field);
}
