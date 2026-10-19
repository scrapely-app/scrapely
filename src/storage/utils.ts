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
