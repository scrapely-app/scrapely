import type { ApifyClient, ApifyClientOptions, ChargingHooks, PpeAwarePushDataContext } from 'scrapely-client';

import {
    type ChargingManager,
    hasDefaultDatasetItemEventInPricing,
    mergeChargeResults,
    pushDataAndCharge,
} from './charging.js';
import { scrapelyClient } from './client-loader.js';
import type { Configuration } from './configuration.js';

export type { PpeAwarePushDataContext };

/**
 * Adapts the SDK's {@link ChargingManager} to `scrapely-client`'s
 * {@link ChargingHooks} protocol. The client only routes `pushItems()`
 * through these hooks for the run's default dataset; all pricing/budget/ledger
 * decisions stay in the SDK.
 */
function createChargingHooks(
    actor: { config: Configuration; getChargingManager: () => ChargingManager },
): ChargingHooks {
    return {
        // Called by the client at `dataset()` time, so pricing is consulted
        // lazily — same timing as the former subclass override.
        shouldChargeDatasetPushes() {
            const chargingManager = actor.getChargingManager();
            // Past the spending limit, route default-dataset pushes through
            // pushDataAndCharge too, which refuses rows nobody pays for.
            return (
                hasDefaultDatasetItemEventInPricing(chargingManager.getPricingInfo().perEventPrices) ||
                (chargingManager.isChargeLimitReached?.() ?? false)
            );
        },
        async pushAndCharge({ items, eventName, pushFn }) {
            return pushDataAndCharge({
                chargingManager: actor.getChargingManager(),
                items,
                eventName,
                // The client only intercepts the default dataset.
                isDefaultDataset: true,
                pushFn: async (limitedItems) => pushFn(limitedItems as typeof items),
            });
        },
        mergeChargeResults,
    };
}

/**
 * Returns a `scrapely-client` `ApifyClient` configured with the SDK's charging
 * hooks, so pushes to the run's default dataset are intercepted and charged
 * pay-per-event by the client itself (no subclassing).
 *
 * `defaultDatasetId` is captured from the configuration at construction time;
 * the pricing check happens lazily on every `dataset()` call.
 */
export function createChargingClient(
    options: ApifyClientOptions,
    actor: { config: Configuration; getChargingManager: () => ChargingManager },
): ApifyClient {
    const { ApifyClient } = scrapelyClient();
    return new ApifyClient({
        ...options,
        defaultDatasetId: actor.config.get('defaultDatasetId'),
        charging: createChargingHooks(actor),
    });
}
