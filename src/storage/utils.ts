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
