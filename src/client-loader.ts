/**
 * scrapely-client, loaded on first use (axios, proxy-agent and friends cost
 * ~0.7 s on the platform, and most runs never need the API client).
 *
 * `USES_PUSH_DATA_INTERCEPTION` and `pushDataChargingContext` come from this
 * module (not declared in the SDK) so the SDK and the client share a single
 * symbol and a single async-local storage instance. Identity matters:
 * `Actor.pushData()` checks dataset clients for this symbol, and the ambient
 * charging context read inside the client's intercepted `pushItems()` must be
 * the one `Actor.pushData()` entered.
 *
 * Loaded by the SDK entry: keep this file free of heavy imports.
 */
export type ScrapelyClientModule = typeof import('scrapely-client');

let scrapelyClientModule: ScrapelyClientModule | undefined;
const loadListeners: Array<(mod: ScrapelyClientModule) => void> = [];

function loaded(mod: ScrapelyClientModule): ScrapelyClientModule {
    if (scrapelyClientModule) return scrapelyClientModule;
    scrapelyClientModule = mod;
    for (const listener of loadListeners.splice(0)) listener(mod);
    return mod;
}

/** The SDK's scrapely-client (loads it on first call). */
export function scrapelyClient(): ScrapelyClientModule {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    return scrapelyClientModule ?? loaded(require('scrapely-client') as ScrapelyClientModule);
}

/** The SDK's scrapely-client if it is already loaded, without loading it. */
export function loadedScrapelyClient(): ScrapelyClientModule | undefined {
    if (scrapelyClientModule) return scrapelyClientModule;
    try {
        const cached = require.cache[require.resolve('scrapely-client')];
        if (cached?.loaded) return loaded(cached.exports as ScrapelyClientModule);
    } catch {
        // not resolvable — then nothing can be using it
    }
    return undefined;
}

/** Run `listener` once the SDK's scrapely-client is loaded (now, if it already is). */
export function onScrapelyClientLoaded(listener: (mod: ScrapelyClientModule) => void): void {
    const mod = loadedScrapelyClient();
    if (mod) listener(mod);
    else loadListeners.push(listener);
}
