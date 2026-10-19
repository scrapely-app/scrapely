import { KeyValueStore } from '@crawlee/core';
import type { KeyValueStoreClient } from '@crawlee/types';
import type { Configuration } from '../configuration.js';
import { Configuration as ScrapelyConfiguration } from '../configuration.js';
import { ApiStorageClientFactory, type ApiStorageConfig } from './api-storage-client.js';

/**
 * Extended KeyValueStoreClient interface with additional methods
 */
interface ExtendedKeyValueStoreClient extends KeyValueStoreClient {
    getAutoSavedValue?<T>(key: string, defaultValue: T): Promise<T>;
    getPublicUrl?(key: string): string;
}

let _apiFactory: ApiStorageClientFactory | null = null;

/**
 * Cache for auto-saved values. Map structure: storeId -> key -> value
 * This allows getAutoSavedValue to return the same object reference,
 * and persistState listeners can iterate and save all cached values.
 */
const autoSaveCache = new Map<string, Map<string, any>>();

/**
 * Track which stores have registered persistState listeners
 */
const persistStateRegistered = new Set<string>();

/**
 * Register a persistState listener for a store if not already registered.
 * This listener saves all cached values for that store when persistState is emitted.
 */
function ensurePersistStateListener(storeId: string, config: Configuration): void {
    if (persistStateRegistered.has(storeId)) {
        return;
    }

    const eventManager = config.getEventManager();
    const persistStateIntervalMillis = config.get('persistStateIntervalMillis') ?? 60000;
    // Use half the interval for timeout (matching Crawlee's approach)
    const timeoutSecs = persistStateIntervalMillis / 2000;

    eventManager.on('persistState', async () => {
        const storeCache = autoSaveCache.get(storeId);
        if (!storeCache) return;

        const factory = _apiFactory;
        if (!factory) return;

        const kvClient = factory.keyValueStore(storeId) as ExtendedKeyValueStoreClient;

        const promises: Promise<void>[] = [];
        for (const [key, value] of storeCache) {
            promises.push(
                kvClient.setRecord!({ key, value }, { timeoutSecs, doNotRetryTimeouts: true } as any).catch((error) => {
                    console.warn(`[Scrapely] Failed to persist state value to ${key}:`, (error as Error).message);
                })
            );
        }
        await Promise.all(promises);
    });

    persistStateRegistered.add(storeId);
}

/**
 * Configure API-based storage (for secure container execution).
 * No DB credentials are exposed - only the user's API key.
 * This is the ONLY way containers should access storage.
 */
export function setApiStorage(
    apiConfig: ApiStorageConfig,
    configuration?: Configuration,
): ApiStorageClientFactory {
    _apiFactory = new ApiStorageClientFactory(apiConfig);
    const cfg = configuration ?? ScrapelyConfiguration.getGlobalConfig();
    cfg.useStorageClient(_apiFactory);
    
    // Patch KeyValueStore to expose listKeys method
    patchKeyValueStore();
    
    return _apiFactory;
}

/**
 * Patch KeyValueStore to expose listKeys, getAutoSavedValue, and getPublicUrl methods
 * that delegate to the underlying client. forEachKey is left to Crawlee's native
 * paginating implementation (via client.listKeys).
 */
function patchKeyValueStore(): void {
    const origOpen = KeyValueStore.open.bind(KeyValueStore);
    
    // Override the open method to return a patched store
    (KeyValueStore as any).open = async function(id?: string) {
        const store = await origOpen(id);
        
        // Add listKeys if not already present
        if (!(store as any).listKeys) {
            const storeId = (store as any).id || (store as any).name || 'default';
            (store as any).listKeys = async function(options?: any) {
                const factory = _apiFactory;
                if (!factory) {
                    throw new Error('API storage client not initialized');
                }
                const kvClient = factory.keyValueStore(storeId);
                return kvClient.listKeys(options);
            };
        }

        // Always override keys() — Crawlee's built-in keys() is an async generator
        // that only runs when iterated with `for await`. Actors that call `await kvs.keys()`
        // without iterating never trigger billing. Replace it with a Promise-based version
        // that calls _listKeys() immediately (and thus increments listCount for billing).
        {
            const storeId = (store as any).id || (store as any).name || 'default';
            (store as any).keys = async function(options?: any) {
                const factory = _apiFactory;
                if (!factory) {
                    throw new Error('API storage client not initialized');
                }
                const kvClient = factory.keyValueStore(storeId);
                return kvClient.keys!(options);
            };
        }

        // Add getAutoSavedValue method - returns value with auto-save behavior
        // This implementation matches Crawlee's approach:
        // 1. Check cache first (return same object reference for subsequent calls)
        // 2. Fetch value from storage if not cached
        // 3. Cache the value (so mutations are tracked)
        // 4. Register persistState listener to save cached values
        {
            const storeId = (store as any).id || (store as any).name || 'default';
            const config = ScrapelyConfiguration.getGlobalConfig();
            
            (store as any).getAutoSavedValue = async function<T>(key: string, defaultValue: T): Promise<T> {
                // Check if we have a cached value for this store/key
                let storeCache = autoSaveCache.get(storeId);
                if (storeCache?.has(key)) {
                    return storeCache.get(key) as T;
                }

                // Fetch value from storage
                const factory = _apiFactory;
                if (!factory) {
                    throw new Error('API storage client not initialized');
                }
                const kvClient = factory.keyValueStore(storeId) as ExtendedKeyValueStoreClient;
                
                // Get the value (this stores defaultValue if key doesn't exist)
                const value = await kvClient.getAutoSavedValue!(key, defaultValue);

                // Initialize store cache if needed
                if (!storeCache) {
                    storeCache = new Map<string, any>();
                    autoSaveCache.set(storeId, storeCache);
                }

                // Check cache again (race condition: another call might have cached it)
                if (storeCache.has(key)) {
                    return storeCache.get(key) as T;
                }

                // Cache the value - mutations to this object will be persisted
                storeCache.set(key, value);

                // Register persistState listener for this store
                ensurePersistStateListener(storeId, config);

                return value as T;
            };
        }

        // Add getPublicUrl method - returns public URL for a record
        {
            const storeId = (store as any).id || (store as any).name || 'default';
            (store as any).getPublicUrl = function(key: string): string {
                const factory = _apiFactory;
                if (!factory) {
                    throw new Error('API storage client not initialized');
                }
                const kvClient = factory.keyValueStore(storeId) as ExtendedKeyValueStoreClient;
                return kvClient.getPublicUrl!(key);
            };
        }
        
        return store;
    };

    // Add static getAutoSavedValue method - operates on default store
    (KeyValueStore as any).getAutoSavedValue = async function<T>(key: string, defaultValue: T): Promise<T> {
        const store = await KeyValueStore.open();
        return (store as any).getAutoSavedValue(key, defaultValue);
    };

    // Add static recordExists method - operates on default store
    (KeyValueStore as any).recordExists = async function(key: string): Promise<boolean> {
        const store = await KeyValueStore.open();
        return store.recordExists(key);
    };
}

export function getApiStorageFactory(): ApiStorageClientFactory | null {
    return _apiFactory;
}

export function isApiStorageConfigured(): boolean {
    return _apiFactory !== null;
}

export function disconnectStorage(): void {
    // API storage doesn't need explicit disconnect
    _apiFactory = null;
    // Clear caches and listener registrations
    autoSaveCache.clear();
    persistStateRegistered.clear();
}
