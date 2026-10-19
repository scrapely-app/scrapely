/**
 * Storage identifier types, kept under these names for backward compatibility.
 */

/** Identifies a storage by its alias from the Actor's schema storages. */
export interface StorageAlias {
    alias: string;
}

/** Identifies a storage by its platform ID. */
export interface StorageId {
    id: string;
}

/** Identifies a storage by its name. */
export interface StorageName {
    name: string;
}

/** A plain string (ID or name), `{ alias }`, `{ id }` or `{ name }`. */
export type StorageIdentifier = string | StorageAlias | StorageId | StorageName;

/** A plain string (ID or name), `{ id }` or `{ name }` (key-value stores and request queues). */
export type StorageIdentifierWithoutAlias = string | StorageId | StorageName;
