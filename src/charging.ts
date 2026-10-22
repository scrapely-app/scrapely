// SPDX-License-Identifier: Apache-2.0
// Derived from apify-sdk-js (src/charging.ts),
// Copyright Apify Technologies s.r.o., licensed under the Apache License 2.0.
// Modified by TeraShift GmbH. See LICENSE-APACHE and NOTICE.
import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';

import type { ApifyClient } from 'scrapely-client';
import type { ActorRunPricingInfo } from 'scrapely-client';

import type { Configuration } from './configuration.js';
import { apiRetryWindowMs, isConnectFailure, serverRetryDelayMs } from './storage/api-storage-client.js';

/** Client safe-replay window; server TTL is longer (35d). */
export const CHARGE_IDEMPOTENCY_SAFE_AGE_MS = 30 * 24 * 60 * 60 * 1000;

const PENDING_MAX_ENTRIES = 1024;
// Same-key attempts per charge / void. Each attempt is a single request
// (scrapely-client's own retries are switched off for it, see runClientOnce),
// so the retries here can honour deadlines: 429, 408, 5xx, timeouts and
// network errors are re-sent with the same idempotency key, with exponential
// backoff and jitter, never sooner than the server's Retry-After, within the API
// retry window (5 minutes, SCRAPELY_API_RETRY_WINDOW_SECS) or the caller's
// deadline. Then the charge stays pending / the void is re-queued and retried
// before the next push and at exit, the same as the Python SDK.
const POST_ATTEMPTS = 64;

/** 408/429 and 5xx: the platform could not take the request now; try again later. */
function isTransientStatus(status: number | undefined): boolean {
    return status === 408 || status === 429 || (status !== undefined && status >= 500);
}

export class ChargeUncertainError extends Error {
    readonly operationId: string;
    readonly eventName: string;
    readonly count: number;

    constructor(
        message: string,
        opts: { operationId: string; eventName: string; count: number },
    ) {
        super(message);
        this.name = 'ChargeUncertainError';
        this.operationId = opts.operationId;
        this.eventName = opts.eventName;
        this.count = opts.count;
    }
}

interface PendingChargeIntent {
    idempotencyKey: string;
    eventName: string;
    count: number;
    createdAt: number;
}


/** Synthetic event for default-dataset items in pay-per-event actors. */

/** Definitive platform rejection (402/400/409). Do not retry with the same key. */
export class ChargeRejectedError extends Error {
    readonly operationId: string;
    readonly eventName: string;
    readonly count: number;
    readonly statusCode?: number;
    readonly reason: string;

    constructor(
        message: string,
        opts: {
            operationId: string;
            eventName: string;
            count: number;
            statusCode?: number;
            reason: string;
        },
    ) {
        super(message);
        this.name = 'ChargeRejectedError';
        this.operationId = opts.operationId;
        this.eventName = opts.eventName;
        this.count = opts.count;
        this.statusCode = opts.statusCode;
        this.reason = opts.reason;
    }
}

/** True when a ChargeRejectedError means the run's spending limit is used up. */
function isChargeLimitRejection(err: ChargeRejectedError): boolean {
    return (
        err.statusCode === 402 ||
        err.reason === 'charge-limit-exceeded' ||
        /charge-limit-exceeded/i.test(err.message)
    );
}

/*
 * Charge-limit safety net for the default dataset.
 *
 * Once a pay-per-event run has used up its spending limit (maxTotalChargeUsd),
 * rows written to the run's default dataset would go out unbilled. The SDK then
 * refuses such writes: nothing is stored, one warning is logged and
 * `ChargingManager.isChargeLimitReached()` turns true. Pushes made by
 * `pushDataAndCharge` are decided there and run inside `chargeGatedPush`, so
 * the storage client does not check them twice.
 */
const chargeGatedPush = new AsyncLocalStorage<boolean>();

/** The charging manager of the running Actor; storage clients consult it. */
let activeChargingManager: ChargingManager | undefined;

/** @internal Register the manager whose limit guards default-dataset writes. */
export function setActiveChargingManager(manager: ChargingManager | undefined): void {
    activeChargingManager = manager;
}

/** @internal The run's default dataset ID, as the platform passes it. */
export function defaultDatasetIdFromEnv(): string | undefined {
    return process.env.ACTOR_DEFAULT_DATASET_ID || process.env.APIFY_DEFAULT_DATASET_ID || undefined;
}

/**
 * @internal True when a write of `itemCount` rows to the default dataset must
 * be dropped (pay-per-event run past its spending limit). Called by storage
 * clients for writes that did not go through `pushDataAndCharge`
 * (`Actor.openDataset().pushData()`, crawler `pushData`, ...).
 */
export function refuseDefaultDatasetPush(itemCount: number): boolean {
    if (chargeGatedPush.getStore()) return false;
    return activeChargingManager?._refuseDefaultDatasetPush(itemCount) ?? false;
}

export const APIFY_DEFAULT_DATASET_ITEM_EVENT = 'apify-default-dataset-item';

/** Scrapely-native synthetic event for default-dataset items. */
export const SCRAPELY_DEFAULT_DATASET_ITEM_EVENT = 'scrapely-default-dataset-item';

/** Alias — imported actors expect this constant name. */
export const DEFAULT_DATASET_ITEM_EVENT = APIFY_DEFAULT_DATASET_ITEM_EVENT;

export function isDefaultDatasetItemEvent(eventName: string): boolean {
    return eventName === APIFY_DEFAULT_DATASET_ITEM_EVENT || eventName === SCRAPELY_DEFAULT_DATASET_ITEM_EVENT;
}

export function resolveDefaultDatasetItemEventName(
    pricingInfo: Record<string, unknown>,
): string {
    if (APIFY_DEFAULT_DATASET_ITEM_EVENT in pricingInfo) {
        return APIFY_DEFAULT_DATASET_ITEM_EVENT;
    }
    if (SCRAPELY_DEFAULT_DATASET_ITEM_EVENT in pricingInfo) {
        return SCRAPELY_DEFAULT_DATASET_ITEM_EVENT;
    }
    return APIFY_DEFAULT_DATASET_ITEM_EVENT;
}

export function hasDefaultDatasetItemEventInPricing(perEventPrices: Record<string, number>): boolean {
    return (
        APIFY_DEFAULT_DATASET_ITEM_EVENT in perEventPrices ||
        SCRAPELY_DEFAULT_DATASET_ITEM_EVENT in perEventPrices
    );
}

/**
 * Events that must never be POSTed to the charge API (server derives them at settle).
 * Explicit names only — a prefix heuristic would drop real priced events.
 */
function isSyntheticEvent(eventName: string): boolean {
    return isDefaultDatasetItemEvent(eventName);
}


export interface ChargeOptions {
    eventName: string;
    count?: number;
}

export interface ChargeResult {
    eventChargeLimitReached: boolean;
    chargedCount: number;
    chargeableWithinLimit: Record<string, number>;
}

export interface ActorPricingInfo {
    pricingModel?: ActorRunPricingInfo['pricingModel'];
    maxTotalChargeUsd: number;
    isPayPerEvent: boolean;
    perEventPrices: Record<string, number>;
}

interface ChargingStateItem {
    chargeCount: number;
    totalChargedAmount: number;
}

export function mergeChargeResults(a: ChargeResult, b: ChargeResult): ChargeResult {
    return {
        eventChargeLimitReached: a.eventChargeLimitReached || b.eventChargeLimitReached,
        chargedCount: a.chargedCount + b.chargedCount,
        chargeableWithinLimit: Object.fromEntries(
            Object.entries(a.chargeableWithinLimit).map(([key, oldValue]) => [
                key,
                Math.min(oldValue, b.chargeableWithinLimit[key] ?? oldValue),
            ]),
        ),
    };
}

/**
 * Pay-per-event charging for platform runs.
 * Reports charges via POST /v2/actor-runs/:runId/charge (Redis → worker billing).
 */
export class ChargingManager {
    /** Set during actor shutdown; rejects new charge operations after the final flush. */
    private _closed = false;

    /** Sticky: the platform refused or capped a charge (HTTP 402 / fewer accepted) or the local gate trimmed one. */
    private chargeLimitHit = false;
    /** Default-dataset rows refused after the limit (one warning, then a count). */
    private refusedDefaultDatasetItems = 0;
    /**
     * Prepaid-row balance: priced events charged through charge() minus rows
     * pushed to the default dataset without a per-row charge. An Actor that
     * charges first and then pushes plainly (charge('product'); pushData(row))
     * has a positive balance after the charge, so the row it already paid for is
     * still saved when that charge used up the spending limit. Push-then-charge
     * Actors never go positive, so they get nothing extra for free.
     */
    private prepaidRowBalance = 0;
    /**
     * Run spend the platform counts against maxTotalChargeUsd that this SDK does
     * not charge itself (compute/proxy/browser usage, server-billed events).
     * Learnt from the charge endpoint's remainingBudgetNd; only ever grows.
     */
    private platformOtherUsd = 0;
    private lastRemainingBudgetNd: number | null = null;
    /** True once the platform reported a remaining budget (its usage is then in the local budget). */
    private platformBudgetSeen = false;

    /** maxTotalChargeUsd minus this SDK's charges and the platform's other run spend. */
    private remainingBudgetUsd(): number {
        return this.maxTotalChargeUsd - this.calculateTotalChargedAmount() - this.platformOtherUsd;
    }

    /** Fold the last charge response's remainingBudgetNd into the local budget (monotonic). */
    private syncPlatformBudget(): void {
        const nd = this.lastRemainingBudgetNd;
        this.lastRemainingBudgetNd = null;
        if (nd === null || nd < 0 || !Number.isFinite(this.maxTotalChargeUsd)) return;
        this.platformBudgetSeen = true;
        const other = Number((this.maxTotalChargeUsd - nd / 1e9 - this.calculateTotalChargedAmount()).toFixed(9));
        if (other > this.platformOtherUsd) this.platformOtherUsd = other;
    }

    /**
     * @internal Whole rows a multi-event retry can pay for after a short answer:
     * what the voided attempt was accepted (budget the void gives back) at the
     * full per-row cost, or the local remaining budget once the platform has
     * reported it (then it includes the leftover after the short answer).
     */
    _rowsAfterShort(attempt: Array<{ name: string; charged: number }>, perRow: Record<string, number>): number {
        const rowCost = Object.entries(perRow).reduce((a, [n, c]) => a + (this.calculateEventPrice(n) ?? 0) * c, 0);
        if (rowCost <= 0) return 0;
        const spent = attempt.reduce((a, op) => a + (this.calculateEventPrice(op.name) ?? 0) * op.charged, 0);
        let fit = Math.floor(Number((spent / rowCost).toFixed(4)));
        if (this.platformBudgetSeen && Number.isFinite(this.maxTotalChargeUsd)) {
            fit = Math.max(fit, Math.floor(Number((this.remainingBudgetUsd() / rowCost).toFixed(4))));
        }
        return Math.max(0, fit);
    }

    /** @internal Platform spend outside this SDK's charges (tests). */
    _platformOtherUsd(): number {
        return this.platformOtherUsd;
    }

    /** @internal Called by Actor shutdown before the final flush. */
    close(): void {
        this._closed = true;
        if (this.refusedDefaultDatasetItems > 0) {
            console.warn(
                `[Scrapely] ${this.refusedDefaultDatasetItems} result(s) pushed to the default dataset after the spending limit was reached were not saved`,
            );
        }
    }

    /**
     * Whether this pay-per-event run has used up its spending limit.
     *
     * True once a charge was refused or capped by the limit (the platform's
     * HTTP 402 or a partial acceptance, or the local budget gate), or once the
     * remaining budget cannot pay for one event of the cheapest priced type.
     * From then on, pushes to the default dataset that are not paid for are
     * refused. Always false for actors without pay-per-event pricing, without
     * a limit, or with only free events.
     */
    isChargeLimitReached(): boolean {
        if (this.chargingState === undefined || !this.isPayPerEvent) return false;
        if (this.chargeLimitHit) return true;
        if (!Number.isFinite(this.maxTotalChargeUsd)) return false;
        const prices = Object.values(this.pricingInfo)
            .map(({ price }) => price)
            .filter((price) => price > 0);
        if (prices.length === 0) return false;
        // Same rounding as the budget gate, so 0.03 - 0.02 still pays a $0.01 event.
        return this.calculateMaxChargesByPrice(Math.min(...prices)) <= 0;
    }

    private markChargeLimitHit(): void {
        if (this.isPayPerEvent) this.chargeLimitHit = true;
    }

    /** @internal Count rows dropped because of the limit; warn on the first one only. */
    _noteRefusedDefaultDatasetItems(count: number): void {
        if (count <= 0) return;
        if (this.refusedDefaultDatasetItems === 0) {
            const limit = Number.isFinite(this.maxTotalChargeUsd) ? `$${this.maxTotalChargeUsd}` : 'the limit';
            console.warn(
                `[Scrapely] The spending limit of this run (maxTotalChargeUsd = ${limit}) has been reached: ` +
                    'results pushed to the default dataset are no longer saved. The Actor should stop now ' +
                    '(check ChargeResult.eventChargeLimitReached or Actor.getChargingManager().isChargeLimitReached()).',
            );
        }
        this.refusedDefaultDatasetItems += count;
    }

    /** @internal */
    _markChargeLimitHit(): void {
        this.markChargeLimitHit();
    }

    /** @internal Whether every row of a push with these events is charged a positive price. */
    _isPaidPerItem(eventName: string | undefined, isDefaultDataset: boolean): boolean {
        const names: string[] = [];
        if (eventName !== undefined) names.push(eventName);
        if (isDefaultDataset) names.push(resolveDefaultDatasetItemEventName(this.pricingInfo));
        return names.some((name) => (this.calculateEventPrice(name) ?? 0) > 0);
    }

    /** @internal */
    _notePrepaidCharge(eventName: string, chargedCount: number): void {
        if (chargedCount > 0 && !isSyntheticEvent(eventName) && (this.pricingInfo[eventName]?.price ?? 0) > 0) {
            this.prepaidRowBalance += chargedCount;
        }
    }

    /** @internal */
    _isPayPerEvent(): boolean {
        return this.chargingState !== undefined && this.isPayPerEvent;
    }

    /**
     * @internal The priced part of a multi-event row (`charges`), plus the
     * default-dataset-item event on the default dataset when it is priced, and
     * how many rows the remaining budget pays for in full.
     */
    _rowCharges(charges: Record<string, number>, isDefaultDataset: boolean, rowCount: number, batch = false): { perRow: Record<string, number>; rows: number } {
        const perRow: Record<string, number> = {};
        for (const [name, n] of Object.entries(charges)) {
            if ((this.calculateEventPrice(name) ?? 0) > 0) perRow[name] = n;
        }
        if (isDefaultDataset) {
            const def = resolveDefaultDatasetItemEventName(this.pricingInfo);
            // Per stored row, also for a batch.
            if (!(def in perRow) && (this.calculateEventPrice(def) ?? 0) > 0) perRow[def] = batch ? rowCount : 1;
        }
        const rowCost = Object.entries(perRow).reduce((a, [name, n]) => a + (this.calculateEventPrice(name) ?? 0) * n, 0);
        // Units charged for: rows, or the one batch (rowCost is then the batch's cost).
        let rows = batch ? 1 : rowCount;
        if (rowCost > 0 && Number.isFinite(this.maxTotalChargeUsd)) {
            const fit = Math.floor(Number((this.remainingBudgetUsd() / rowCost).toFixed(4)));
            rows = Math.min(rows, Math.max(0, fit));
        }
        return { perRow, rows };
    }

    /** @internal Current prepaid-row balance (tests). */
    _prepaidRowBalance(): number {
        return this.prepaidRowBalance;
    }

    /**
     * @internal Account for rows pushed without a per-row charge. Returns how
     * many may be stored: all while the limit is not reached, afterwards only
     * the rows earlier charges already paid for (the positive balance).
     */
    _takeUnpaidRows(itemCount: number): number {
        if (itemCount <= 0) return 0;
        if (!this.isChargeLimitReached()) {
            this.prepaidRowBalance -= itemCount;
            return itemCount;
        }
        const allowed = Math.max(0, Math.min(itemCount, this.prepaidRowBalance));
        this.prepaidRowBalance -= allowed;
        return allowed;
    }

    /** @internal */
    _refuseDefaultDatasetPush(itemCount: number): boolean {
        if (itemCount <= 0 || !this.isPayPerEvent) return false;
        if (!this.isChargeLimitReached()) {
            this._takeUnpaidRows(itemCount);
            return false;
        }
        // All-or-nothing here (storage clients cannot store a slice): store the
        // batch only when earlier charges paid for every row of it.
        if (this.prepaidRowBalance >= itemCount) {
            this.prepaidRowBalance -= itemCount;
            return false;
        }
        this._noteRefusedDefaultDatasetItems(itemCount);
        return true;
    }

    /** @internal Whether shutdown has closed this manager to new charges. */
    isClosed(): boolean {
        return this._closed;
    }

    private readonly LOCAL_CHARGING_LOG_DATASET_NAME = 'charging_log';
    private readonly PLATFORM_CHARGING_LOG_DATASET_ID_KEY = 'CHARGING_LOG_DATASET_ID';

    private maxTotalChargeUsd: number;
    private readonly isAtHome: boolean;
    private readonly actorRunId: string;
    private pricingModel?: ActorRunPricingInfo['pricingModel'];
    private pricingInfo: Record<string, { price: number; title: string }> = {};
    private chargingState?: Record<string, ChargingStateItem>;
    private chargingLogDataset?: { pushData: (data: Record<string, unknown>) => Promise<void> };
    private notPpeWarningPrinted = false;
    /** Idempotency key of the last charge the platform accepted (pushDataAndCharge void-on-failure). */
    private lastAcceptedOperationId: string | undefined;
    /** Voids not yet confirmed by the platform; retried before the next push and at exit. */
    private pendingVoids: Array<{ eventName: string; operationId: string; reservedCount: number; notFoundMeansVoided?: boolean }> = [];
    /**
     * @internal Platform charges for results that were not stored and that the
     * platform definitively refused to void (event -> count). They stay billed,
     * so the next push of that event uses them instead of charging again.
     */
    _voidRefusedCredit: Record<string, number> = {};

    /** Unresolved platform POSTs: key = idempotency UUID (never event name). */
    private readonly pendingCharges = new Map<string, PendingChargeIntent>();
    /**
     * Record in the run's default key-value store that mirrors pendingCharges,
     * so a container crash does not lose them: settlement re-applies them under
     * their own idempotency keys. One record per container, so a restarted
     * container never overwrites its predecessor's.
     */
    private readonly pendingRecordKey = `SCRAPELY_PENDING_CHARGES-${randomUUID().replace(/-/g, '').slice(0, 12)}`;
    /** Intent keys last written to that record ('' = nothing written / empty). */
    private persistedPendingSig = '';
    private flushInFlight: Promise<number> | null = null;
    private operationTail: Promise<void> = Promise.resolve();
    private readonly postAttempts = POST_ATTEMPTS;
    private readonly pendingMaxEntries = PENDING_MAX_ENTRIES;
    private readonly idempotencySafeAgeMs = CHARGE_IDEMPOTENCY_SAFE_AGE_MS;

    private readonly apifyClientSource: ApifyClient | (() => ApifyClient);

    /**
     * @param apifyClient - the API client, or a function returning it (called
     *   on first use, so runs that never need the API never load the client).
     */
    constructor(
        private readonly configuration: Configuration,
        apifyClient: ApifyClient | (() => ApifyClient),
    ) {
        this.apifyClientSource = apifyClient;
        this.maxTotalChargeUsd = configuration.get('maxTotalChargeUsd') || Infinity;
        this.isAtHome = !!configuration.get('isAtHome');
        this.actorRunId = configuration.get('actorRunId') || '';
    }

    private get apifyClient(): ApifyClient {
        const source = this.apifyClientSource;
        return typeof source === 'function' ? source() : source;
    }

    /** @internal Serialize allowance calculation, side effects, and local reservation. */
    async _withOperationLock<T>(operation: () => Promise<T>): Promise<T> {
        const previous = this.operationTail;
        let release!: () => void;
        this.operationTail = new Promise<void>((resolve) => {
            release = resolve;
        });
        await previous;
        try {
            return await operation();
        } finally {
            release();
        }
    }


    private get isPayPerEvent(): boolean {
        return this.pricingModel === 'PAY_PER_EVENT';
    }

    private ensureInitialized(): void {
        if (this.chargingState === undefined) {
            throw new Error('ChargingManager is not initialized');
        }
    }

    private async fetchPricingInfo(): Promise<{
        pricingInfo?: ActorRunPricingInfo;
        chargedEventCounts?: Record<string, number>;
        maxTotalChargeUsd: number;
    }> {
        const actorPricingInfo = this.configuration.get('actorPricingInfo');
        const chargedEventCounts = this.configuration.get('chargedEventCounts');
        // The platform worker always injects the charged event counts, and the
        // pricing info only when the actor has pricing (both from the same
        // actor.pricingInfos[0] the run object reports). Counts without pricing
        // therefore mean "no pricing": no API round trip at init.
        if (chargedEventCounts) {
            return {
                pricingInfo: actorPricingInfo ? (JSON.parse(actorPricingInfo) as ActorRunPricingInfo) : undefined,
                chargedEventCounts: JSON.parse(chargedEventCounts) as Record<string, number>,
                maxTotalChargeUsd: this.configuration.get('maxTotalChargeUsd') || Infinity,
            };
        }

        if (this.isAtHome) {
            if (!this.actorRunId) {
                throw new Error('Actor run ID not found even though the Actor is running on the platform');
            }

            const run = await this.apifyClient.run(this.actorRunId).get();
            if (!run) {
                throw new Error('Actor run not found');
            }

            return {
                pricingInfo: run.pricingInfo,
                chargedEventCounts: run.chargedEventCounts,
                maxTotalChargeUsd: run.options?.maxTotalChargeUsd || Infinity,
            };
        }

        return {
            pricingInfo: undefined,
            chargedEventCounts: {},
            maxTotalChargeUsd: this.configuration.get('maxTotalChargeUsd') || Infinity,
        };
    }

    /**
     * Initialize the ChargingManager by loading pricing information and charging state via platform API.
     */
    async init(): Promise<void> {
        const useChargingLogDataset = this.configuration.get('useChargingLogDataset');
        if (useChargingLogDataset && this.isAtHome) {
            throw new Error(
                'Using the ACTOR_USE_CHARGING_LOG_DATASET environment variable is only supported in a local development environment',
            );
        }

        if (this.configuration.get('testPayPerEvent') && this.isAtHome) {
            throw new Error(
                'Using the ACTOR_TEST_PAY_PER_EVENT environment variable is only supported in a local development environment',
            );
        }

        const { pricingInfo, chargedEventCounts, maxTotalChargeUsd } = await this.fetchPricingInfo();

        if (this.configuration.get('testPayPerEvent')) {
            this.pricingModel = 'PAY_PER_EVENT';
        } else {
            this.pricingModel ??= pricingInfo?.pricingModel;
        }

        if (pricingInfo?.pricingModel === 'PAY_PER_EVENT') {
            const events =
                (pricingInfo as {
                    pricingPerEvent?: {
                        actorChargeEvents?: Record<string, { eventPriceUsd: number; eventTitle: string }>;
                    };
                }).pricingPerEvent?.actorChargeEvents ?? {};
            for (const [eventName, eventPricing] of Object.entries(events)) {
                this.pricingInfo[eventName] = {
                    price: eventPricing.eventPriceUsd,
                    title: eventPricing.eventTitle,
                };
            }

            this.maxTotalChargeUsd = maxTotalChargeUsd;
        }

        this.chargingState = {};

        for (const [eventName, chargeCount] of Object.entries(chargedEventCounts ?? {})) {
            this.chargingState[eventName] = {
                chargeCount,
                totalChargedAmount: chargeCount * (this.pricingInfo[eventName]?.price ?? 0),
            };
        }

        if (!this.isPayPerEvent || !useChargingLogDataset) {
            return;
        }

        const { Dataset, KeyValueStore } = await import('@crawlee/core');

        if (this.isAtHome) {
            const defaultStore = await KeyValueStore.open();
            const storedDatasetId = await defaultStore.getValue<string>(this.PLATFORM_CHARGING_LOG_DATASET_ID_KEY);
            const datasetId =
                storedDatasetId ??
                (await (async () => {
                    const dataset = await this.apifyClient.datasets().getOrCreate();
                    await defaultStore.setValue(this.PLATFORM_CHARGING_LOG_DATASET_ID_KEY, dataset.id);
                    return dataset.id;
                })());
            this.chargingLogDataset = await Dataset.open(datasetId);
        } else {
            if (this.configuration.get('purgeOnStart')) {
                const dataset = await Dataset.open(this.LOCAL_CHARGING_LOG_DATASET_NAME);
                await dataset.drop();
            }
            this.chargingLogDataset = await Dataset.open(this.LOCAL_CHARGING_LOG_DATASET_NAME);
        }
    }

    getPricingInfo(): ActorPricingInfo {
        if (this.chargingState === undefined) {
            throw new Error('ChargingManager is not initialized');
        }

        return {
            pricingModel: this.pricingModel,
            maxTotalChargeUsd: Number.isFinite(this.maxTotalChargeUsd) ? this.maxTotalChargeUsd : Infinity,
            isPayPerEvent: this.isPayPerEvent,
            perEventPrices: Object.fromEntries(
                Object.entries(this.pricingInfo).map(([name, { price }]) => [name, price]),
            ),
        };
    }

    getChargedEventCount(eventName: string): number {
        this.ensureInitialized();

        if (isDefaultDatasetItemEvent(eventName)) {
            return (
                (this.chargingState![APIFY_DEFAULT_DATASET_ITEM_EVENT]?.chargeCount ?? 0) +
                (this.chargingState![SCRAPELY_DEFAULT_DATASET_ITEM_EVENT]?.chargeCount ?? 0)
            );
        }

        return this.chargingState![eventName]?.chargeCount ?? 0;
    }

    getMaxTotalChargeUsd(): number {
        this.ensureInitialized();
        return this.maxTotalChargeUsd;
    }

    /**
     * Helper to calculate how many items can be pushed within charging limits.
     * Returns the limited items and count to charge.
     */
    calculatePushDataLimits<T>(params: {
        eventName: undefined | string;
        isDefaultDataset: boolean;
        items: T | T[];
    }): { eventsToCharge: Record<string, number>; limitedItems: T[] } {
        this.ensureInitialized();

        const { eventName, isDefaultDataset, items } = params;
        const itemsArray = Array.isArray(items) ? items : [items];

        if (!this.isPayPerEvent) {
            return {
                eventsToCharge: {},
                limitedItems: itemsArray,
            };
        }

        const defaultDatasetItemEvent = resolveDefaultDatasetItemEventName(this.pricingInfo);

        const itemPrice =
            ((eventName !== undefined ? this.calculateEventPrice(eventName) : undefined) ?? 0) +
            ((isDefaultDataset ? this.calculateEventPrice(defaultDatasetItemEvent) : undefined) ?? 0);

        const maxChargedCount =
            itemPrice > 0 ? this.calculateMaxChargesByPrice(itemPrice) : Infinity;

        // Dataset writes must never exceed the reservable budget. Direct charge()
        // may preserve its legacy one-event overdraw behavior, but persisting an
        // item first requires a strict allowance.
        const itemsToKeep = Math.min(itemsArray.length, maxChargedCount);

        const eventsToCharge: Record<string, number> = {};
        if (eventName !== undefined && itemsToKeep > 0) {
            eventsToCharge[eventName] = itemsToKeep;
        }
        if (isDefaultDataset && itemsToKeep > 0) {
            eventsToCharge[defaultDatasetItemEvent] = itemsToKeep;
        }

        return {
            limitedItems:
                itemsToKeep >= itemsArray.length ? itemsArray : itemsArray.slice(0, itemsToKeep),
            eventsToCharge,
        };
    }

    private calculateTotalChargedAmount(): number {
        this.ensureInitialized();
        const result = Object.values(this.chargingState!).reduce(
            (sum, item) => sum + item.totalChargedAmount,
            0,
        );
        return Number(result.toFixed(6));
    }

    /**
     * How many events of a given type can still be charged for before reaching the limit;
     * If the event is not registered, returns Infinity (free of charge)
     */
    calculateMaxEventChargeCountWithinLimit(eventName: string): number {
        this.ensureInitialized();

        const price = this.calculateEventPrice(eventName);
        if (!price) {
            return Infinity;
        }

        return this.calculateMaxChargesByPrice(price);
    }

    private calculateEventPrice(eventName: string): number | undefined {
        if (!this.isAtHome) {
            return 1;
        }
        if (isDefaultDatasetItemEvent(eventName)) {
            return (
                this.pricingInfo[APIFY_DEFAULT_DATASET_ITEM_EVENT]?.price ??
                this.pricingInfo[SCRAPELY_DEFAULT_DATASET_ITEM_EVENT]?.price
            );
        }
        return this.pricingInfo[eventName]?.price;
    }

    private getEventPricing(eventName: string): { price: number; title: string } {
        const canonicalEventName = isDefaultDatasetItemEvent(eventName)
            ? resolveDefaultDatasetItemEventName(this.pricingInfo)
            : eventName;

        if (this.pricingInfo[canonicalEventName]) {
            return this.pricingInfo[canonicalEventName];
        }

        return {
            price: this.isAtHome ? 0 : 1,
            title: `Unknown event '${eventName}'`,
        };
    }

    private normalizeChargingEventName(eventName: string): string {
        if (isDefaultDatasetItemEvent(eventName)) {
            return resolveDefaultDatasetItemEventName(this.pricingInfo);
        }
        return eventName;
    }

    private calculateMaxChargesByPrice(price: number): number {
        const unroundedResult = this.remainingBudgetUsd() / price;

        const roundedResult = Math.floor(Number(unroundedResult.toFixed(4)));

        return Math.max(0, roundedResult);
    }

    /** @internal Remaining allowance per event (pushDataAndCharge's uncertain path). */
    _chargeableWithinLimit(): Record<string, number> {
        return this.chargingState === undefined ? {} : this.getChargeableWithinLimit();
    }

    private getChargeableWithinLimit(): Record<string, number> {
        if (Object.keys(this.pricingInfo).length === 0) {
            return {};
        }
        return Object.fromEntries(
            Object.keys(this.pricingInfo).map((name) => [
                name,
                this.calculateMaxEventChargeCountWithinLimit(name),
            ]),
        );
    }

    private quarantineStalePending(): void {
        const now = Date.now();
        for (const [key, pending] of this.pendingCharges) {
            if (now - pending.createdAt > this.idempotencySafeAgeMs) {
                // Unwind local reservation — dropping without reverse permanently
                // consumes budget the same way a rejected pending used to.
                const eventPrice = this.getEventPricing(pending.eventName).price;
                this.applyAcceptedCount(pending.eventName, pending.count, 0, eventPrice);
                this.pendingCharges.delete(key);
                console.error(
                    `[Scrapely] Quarantined stale charge intent past safe idempotency age ` +
                        `(key=${key} event=${pending.eventName} count=${pending.count}); ` +
                        `unwound local reservation; will not replay`,
                );
            }
        }
    }

    /**
     * The run client with scrapely-client's internal retries off (one request
     * per call) and its timeout capped at `timeoutMs`, so the retry loops here
     * decide when to retry and never outlive their deadline. `meta.retryAfter`
     * receives the Retry-After of the last answer (ApifyApiError drops the
     * headers). A client without that shape (tests, custom clients) is used as
     * it is.
     */
    private runClientOnce(timeoutMs?: number) {
        const meta: { retryAfter: string | null } = { retryAfter: null };
        const run = this.apifyClient.run(this.actorRunId) as unknown as ReturnType<ApifyClient['run']> & {
            httpClient?: { maxRetries?: number; timeoutMillis?: number; axios?: { request?: (cfg: unknown) => Promise<unknown> } };
        };
        const http = run.httpClient;
        if (http && typeof http === 'object' && typeof http.maxRetries === 'number') {
            const once = Object.create(http) as { maxRetries: number; timeoutMillis?: number; axios?: unknown };
            once.maxRetries = 0;
            if (timeoutMs !== undefined && typeof http.timeoutMillis === 'number') {
                once.timeoutMillis = Math.max(1000, Math.min(http.timeoutMillis, timeoutMs));
            }
            const axios = http.axios;
            if (axios && typeof axios.request === 'function') {
                const request = axios.request.bind(axios);
                once.axios = Object.assign(Object.create(axios), {
                    request: async (cfg: unknown) => {
                        const response = (await request(cfg)) as { headers?: { get?: (k: string) => unknown; [k: string]: unknown } };
                        const h = response?.headers;
                        const value = typeof h?.get === 'function' ? h.get('retry-after') : h?.['retry-after'];
                        meta.retryAfter = value === undefined || value === null ? null : String(value);
                        return response;
                    },
                });
            }
            (run as { httpClient: unknown }).httpClient = once;
        }
        return { run, meta };
    }

    /**
     * Sleep before the next same-key attempt; false when none is left (attempts,
     * the API retry window from `started`, or `deadline` would be passed).
     * Never sooner than the server's Retry-After, and at least 1 s after any
     * 429 (the server's minimum, also when the header is missing or malformed).
     */
    private async backoffBeforeRetry(
        attempt: number,
        started: number,
        err: unknown,
        deadline?: number,
        retryAfter: string | null = null,
    ): Promise<boolean> {
        if (attempt + 1 >= this.postAttempts) return false;
        let delay = serverRetryDelayMs(attempt, retryAfter);
        if (httpStatusOf(err) === 429) delay = Math.max(delay, 1000 + Math.random() * 1000);
        const end = Math.min(started + apiRetryWindowMs(), deadline ?? Number.POSITIVE_INFINITY);
        if (Date.now() + delay >= end) return false;
        await new Promise((r) => setTimeout(r, delay));
        return true;
    }

    /** Time left for one attempt before `deadline`, if any. */
    private static attemptTimeout(deadline?: number): number | undefined {
        return deadline === undefined ? undefined : Math.max(0, deadline - Date.now());
    }

    /**
     * POST a charge intent. Returns acceptedCount on success.
     * Throws ChargeRejectedError for definitive platform rejections (no retry).
     * Returns null when the outcome is uncertain after retries (the intent stays
     * pending); `deadline` (epoch ms) ends the retries earlier, e.g. at exit.
     */
    private async postChargeWithRetries(intent: PendingChargeIntent, deadline?: number): Promise<number | null> {
        let lastError: unknown;
        const started = Date.now();
        for (let attempt = 0; attempt < this.postAttempts; attempt++) {
            const { run, meta } = this.runClientOnce(ChargingManager.attemptTimeout(deadline));
            try {
                const response = await run.charge({
                    eventName: intent.eventName,
                    count: intent.count,
                    idempotencyKey: intent.idempotencyKey,
                });
                const body = extractChargeResponseBody(response);
                const accepted = readAcceptedCount(body, intent.count);
                const remaining = body.remainingBudgetNd;
                this.lastRemainingBudgetNd = typeof remaining === 'number' && Number.isFinite(remaining) && remaining >= 0 ? remaining : null;
                return accepted;
            } catch (err) {
                lastError = err;
                const rejected = classifyChargeHttpError(err, intent);
                if (rejected) {
                    throw rejected;
                }
                if (!(await this.backoffBeforeRetry(attempt, started, err, deadline, meta.retryAfter))) break;
            }
        }
        console.warn(
            `[Scrapely] Failed to record charge for event ${intent.eventName} ` +
                `(count=${intent.count}, key=${intent.idempotencyKey}): ` +
                `${(lastError as Error)?.message ?? lastError}`,
        );
        return null;
    }

    /** @internal Idempotency key of the charge accepted by the last _chargeExclusive call, if any. */
    _takeLastAcceptedOperationId(): string | undefined {
        const id = this.lastAcceptedOperationId;
        this.lastAcceptedOperationId = undefined;
        return id;
    }

    /**
     * @internal Mirror the unconfirmed charges to the run's default key-value
     * store (best effort, only when the set changed since the last write).
     */
    async _persistPendingCharges(): Promise<void> {
        if (!this.isAtHome || !this.actorRunId) return;
        const intents = [...this.pendingCharges.values()].map(({ idempotencyKey, eventName, count, createdAt }) => ({ idempotencyKey, eventName, count, createdAt }));
        const sig = intents.map((i) => i.idempotencyKey).sort().join(',');
        if (sig === this.persistedPendingSig) return;
        const storeId = process.env.ACTOR_DEFAULT_KEY_VALUE_STORE_ID
            || process.env.APIFY_DEFAULT_KEY_VALUE_STORE_ID
            || (this.configuration.get('defaultKeyValueStoreId') as string | undefined);
        if (!storeId || storeId === 'default') return;
        let timer: ReturnType<typeof setTimeout> | undefined;
        try {
            const store = (this.apifyClient as unknown as {
                keyValueStore(id: string): { setRecord(r: { key: string; value: unknown }): Promise<unknown> };
            }).keyValueStore(storeId);
            await Promise.race([
                store.setRecord({ key: this.pendingRecordKey, value: { v: 1, intents } }),
                new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('timed out')), 15_000); }),
            ]);
            this.persistedPendingSig = sig;
        } catch (err) {
            console.warn(`[Scrapely] Could not persist ${intents.length} unconfirmed charge(s): ${(err as Error).message}`);
        } finally {
            if (timer) clearTimeout(timer);
        }
    }

    /**
     * @internal Reverse an accepted charge whose items were never stored
     * (POST /v2/actor-runs/:runId/charge/void), then unwind the local
     * reservation. Returns true when the platform voided it.
     */
    /**
     * @internal Cancel a charge whose outcome was never confirmed, for items that
     * were then not stored: drop the pending intent (so the exit flush never
     * posts it) and void it in case the original request did apply. A 404 here
     * means it never applied.
     */
    async _cancelUncertainCharge(eventName: string, operationId: string, reservedCount: number): Promise<boolean> {
        this.pendingCharges.delete(operationId);
        const voided = await this._voidCharge(eventName, operationId, reservedCount, true);
        // A persisted copy is harmless either way (the void leaves a tombstone),
        // but keep the record small.
        await this._persistPendingCharges();
        return voided;
    }

    /**
     * @internal Void (or cancel, when unconfirmed) a whole charge and say what
     * happened: 'voided' (confirmed, local reservation released), 'stands' (a
     * definitive answer that it cannot be voided) or 'unknown' (queued for retry,
     * reservation kept).
     */
    async _voidOutcome(eventName: string, operationId: string, reservedCount: number, uncertain: boolean): Promise<'voided' | 'stands' | 'unknown'> {
        const queued = this.pendingVoids.length;
        const ok = uncertain
            ? await this._cancelUncertainCharge(eventName, operationId, reservedCount)
            : await this._voidCharge(eventName, operationId, reservedCount);
        if (ok) return 'voided';
        return this.pendingVoids.length > queued ? 'unknown' : 'stands';
    }

    /** @internal A charge for `count` unstored results stands (void refused): keep it as credit. */
    _noteUnvoidable(eventName: string, count: number): void {
        if (count <= 0) return;
        this._voidRefusedCredit[eventName] = (this._voidRefusedCredit[eventName] ?? 0) + count;
        console.warn(
            `[Scrapely] Charge for ${count} unstored '${eventName}' result(s) could not be voided; ` +
                `the next push of '${eventName}' uses it instead of charging again`,
        );
    }

    /** @internal Use credit for a whole charge of `count` events (all or nothing); returns the count used. */
    _takeVoidCredit(eventName: string, count: number): number {
        const have = this._voidRefusedCredit[eventName] ?? 0;
        if (count <= 0 || have < count) return 0;
        this._voidRefusedCredit[eventName] = have - count;
        return count;
    }

    /** @internal */
    _returnVoidCredit(eventName: string, count: number): void {
        if (count > 0) this._voidRefusedCredit[eventName] = (this._voidRefusedCredit[eventName] ?? 0) + count;
    }

    /** @internal Void (or cancel) a charge for results that were not stored; a refused void becomes credit. */
    async _voidOrCredit(eventName: string, operationId: string, reservedCount: number, uncertain: boolean): Promise<'voided' | 'stands' | 'unknown'> {
        const outcome = await this._voidOutcome(eventName, operationId, reservedCount, uncertain);
        if (outcome === 'stands') this._noteUnvoidable(eventName, reservedCount);
        return outcome;
    }

    async _voidCharge(
        eventName: string,
        operationId: string,
        reservedCount: number,
        notFoundMeansVoided = false,
        deadline?: number,
    ): Promise<boolean> {
        if (!this.isAtHome || !this.actorRunId) return false;
        const started = Date.now();
        for (let attempt = 0; attempt < this.postAttempts; attempt++) {
            const { run, meta } = this.runClientOnce(ChargingManager.attemptTimeout(deadline));
            const runClient = run as unknown as {
                httpClient: { call(req: Record<string, unknown>): Promise<unknown> };
                _url(path?: string): string;
            };
            try {
                await runClient.httpClient.call({
                    url: runClient._url('charge/void'),
                    method: 'POST',
                    headers: { 'idempotency-key': operationId },
                });
                // Confirmed (voided or already voided): only now give the budget back.
                this.applyAcceptedCount(eventName, reservedCount, 0);
                return true;
            } catch (err) {
                const status = httpStatusOf(err);
                // Only the void route's own answer proves it never applied; a server
                // without the route (older deployment) also answers 404.
                if (status === 404 && notFoundMeansVoided && apiErrorTypeOf(err) === 'charge-not-found') {
                    // The unconfirmed charge never reached the ledger: nothing billed.
                    this.applyAcceptedCount(eventName, reservedCount, 0);
                    return true;
                }
                // 404: nothing to void; 409: already settled; 4xx auth: permanent.
                // The charge stands; keep the local reservation that matches it.
                // 408/429 (rate limited) and 5xx are not answers: re-queue below.
                if (status !== undefined && status >= 400 && status < 500 && !isTransientStatus(status)) return false;
                if (!(await this.backoffBeforeRetry(attempt, started, err, deadline, meta.retryAfter))) break;
            }
        }
        // Outcome unknown: keep the reservation and retry before the next push / at exit.
        this.pendingVoids.push({ eventName, operationId, reservedCount, notFoundMeansVoided });
        console.error(`[Scrapely] Void of charge ${operationId} ('${eventName}') not confirmed yet; will retry`);
        return false;
    }

    /**
     * @internal Retry unconfirmed voids (caller holds the operation lock).
     * Returns false while any earlier void is still unconfirmed.
     */
    async _retryPendingVoids(deadline?: number): Promise<boolean> {
        for (let round = 0; ; round++) {
            const pending = this.pendingVoids;
            this.pendingVoids = [];
            for (const v of pending) {
                await this._voidCharge(v.eventName, v.operationId, v.reservedCount, v.notFoundMeansVoided, deadline);
            }
            if (this.pendingVoids.length === 0) return true;
            // With a deadline (exit flush): keep retrying with backoff while time is left.
            const delay = serverRetryDelayMs(round, null);
            if (deadline === undefined || Date.now() + delay >= deadline) return false;
            await new Promise((r) => setTimeout(r, delay));
        }
    }

    /** Clamp local reservation down when the server accepted fewer events. */
    /** @internal Used by pushDataAndCharge dual-event reconciliation. */
    applyAcceptedCount(
        eventName: string,
        reservedCount: number,
        acceptedCount: number,
        eventPrice?: number,
    ): number {
        if (acceptedCount >= reservedCount) {
            return reservedCount;
        }
        const price = eventPrice ?? this.getEventPricing(eventName).price;
        const delta = reservedCount - acceptedCount;
        const state = this.chargingState?.[eventName];
        if (state) {
            state.chargeCount = Math.max(0, state.chargeCount - delta);
            state.totalChargedAmount = Math.max(0, state.totalChargedAmount - delta * price);
        }
        return Math.max(0, acceptedCount);
    }

    /**
     * Best-effort flush of open charge intents (same keys, no local counter bump).
     * @returns number of intents confirmed
     */
    /**
     * Best-effort flush of open charge intents under the operation lock.
     * Drains in batches until empty or deadlineMs elapses.
     */
    async flushPending(maxIntents = 32, deadlineMs?: number): Promise<number> {
        if (!this.actorRunId) return 0;

        // Serialize with charge/push so concurrent flush cannot double-unwind.
        return this._withOperationLock(() => this._flushPendingExclusive(maxIntents, deadlineMs));
    }

    /** @internal Caller must hold the operation lock. */
    async _flushPendingExclusive(maxIntents = 32, deadlineMs?: number): Promise<number> {
        if (!this.actorRunId) return 0;
        const deadline = deadlineMs !== undefined ? Date.now() + Math.max(0, deadlineMs) : undefined;
        // Voids first: an unconfirmed one means the user may be billed for items
        // that were never stored. At exit (with a deadline) keep retrying them.
        await this._retryPendingVoids(deadline);

        if (this.flushInFlight) {
            return this.flushInFlight;
        }

        this.flushInFlight = (async () => {
            let confirmedTotal = 0;
            // Drain loop: keep flushing batches until empty or deadline.
            for (;;) {
                if (deadline !== undefined && Date.now() >= deadline) break;
                this.quarantineStalePending();
                if (this.pendingCharges.size === 0) break;
                const snapshot = [...this.pendingCharges.values()].slice(0, Math.max(0, maxIntents));
                if (snapshot.length === 0) break;
                let confirmed = 0;
                for (const intent of snapshot) {
                    if (deadline !== undefined && Date.now() >= deadline) break;
                    try {
                        const accepted = await this.postChargeWithRetries(intent, deadline);
                        if (accepted !== null) {
                            if (accepted < intent.count) this.markChargeLimitHit();
                            const eventPrice = this.getEventPricing(intent.eventName).price;
                            this.applyAcceptedCount(
                                intent.eventName,
                                intent.count,
                                accepted,
                                eventPrice,
                            );
                            this.pendingCharges.delete(intent.idempotencyKey);
                            confirmed += 1;
                        }
                    } catch (err) {
                        if (err instanceof ChargeRejectedError) {
                            if (isChargeLimitRejection(err)) this.markChargeLimitHit();
                            const eventPrice = this.getEventPricing(intent.eventName).price;
                            this.applyAcceptedCount(intent.eventName, intent.count, 0, eventPrice);
                            this.pendingCharges.delete(intent.idempotencyKey);
                            console.warn(
                                `[Scrapely] Dropping rejected pending charge ` +
                                    `(event=${intent.eventName} key=${intent.idempotencyKey}): ${err.message}`,
                            );
                        } else {
                            throw err;
                        }
                    }
                }
                confirmedTotal += confirmed;
                // No progress and still pending → stop to avoid tight loop on uncertain.
                if (confirmed === 0) break;
            }
            await this._persistPendingCharges();
            return confirmedTotal;
        })();

        try {
            return await this.flushInFlight;
        } finally {
            this.flushInFlight = null;
        }
    }

    async charge({ eventName, count = 1 }: ChargeOptions): Promise<ChargeResult> {
        return this._withOperationLock(async () => {
            const result = await this._chargeExclusive({ eventName, count });
            this._notePrepaidCharge(eventName, result.chargedCount);
            return result;
        });
    }

    /** @internal Caller must hold the operation lock. */
    async _chargeExclusive({ eventName, count = 1 }: ChargeOptions): Promise<ChargeResult> {
        if (this._closed) {
            throw new Error('ChargingManager is closed (actor is shutting down); charge rejected');
        }
        const calculateChargeableWithinLimit = () => this.getChargeableWithinLimit();
        const normalizedEventName =
            this.chargingState === undefined ? eventName : this.normalizeChargingEventName(eventName);

        if (!this.isPayPerEvent) {
            if (!this.notPpeWarningPrinted) {
                console.warn(
                    '[Scrapely] Ignored attempt to charge for an event - the Actor does not use the pay-per-event pricing',
                );
                this.notPpeWarningPrinted = true;
            }

            return {
                eventChargeLimitReached: false,
                chargedCount: 0,
                chargeableWithinLimit: this.chargingState === undefined ? {} : calculateChargeableWithinLimit(),
            };
        }

        this.ensureInitialized();

        if (!Number.isInteger(count) || count < 1) {
            throw new Error('count must be a positive integer');
        }

        try {
            // Already under operation lock — call exclusive path (no re-enter lock).
            await this._flushPendingExclusive();
        } catch (err) {
            console.warn(`[Scrapely] flushPending before charge failed: ${(err as Error).message}`);
        }

        const maxEventChargeCount = this.calculateMaxEventChargeCountWithinLimit(normalizedEventName);

        let chargedCount = (() => {
            if (count <= maxEventChargeCount) {
                return count;
            }

            if (this.calculateTotalChargedAmount() <= this.maxTotalChargeUsd) {
                return maxEventChargeCount + 1;
            }

            return 0;
        })();

        if (chargedCount < count) this.markChargeLimitHit();

        if (chargedCount === 0) {
            return {
                eventChargeLimitReached: count > 0,
                chargedCount: 0,
                chargeableWithinLimit: calculateChargeableWithinLimit(),
            };
        }

        const eventPricing = this.getEventPricing(normalizedEventName);
        const canPost =
            this.isAtHome &&
            !!this.actorRunId &&
            !isSyntheticEvent(normalizedEventName) &&
            this.pricingInfo[normalizedEventName] !== undefined;

        let intent: PendingChargeIntent | undefined;

        if (canPost) {
            if (this.pendingCharges.size >= this.pendingMaxEntries) {
                console.warn(
                    `[Scrapely] Too many unresolved charge intents (${this.pendingCharges.size}); ` +
                        `refusing new billable charge for event ${normalizedEventName}`,
                );
                return {
                    eventChargeLimitReached: false,
                    chargedCount: 0,
                    chargeableWithinLimit: calculateChargeableWithinLimit(),
                };
            }
            intent = {
                idempotencyKey: randomUUID(),
                eventName: normalizedEventName,
                count: chargedCount,
                createdAt: Date.now(),
            };
            this.pendingCharges.set(intent.idempotencyKey, intent);
        }

        // Reserve local budget once per logical operation (no rollback on POST failure).
        this.chargingState![normalizedEventName] ??= { chargeCount: 0, totalChargedAmount: 0 };
        this.chargingState![normalizedEventName].chargeCount += chargedCount;
        this.chargingState![normalizedEventName].totalChargedAmount += chargedCount * eventPricing.price;

        if (canPost && intent) {
            let accepted: number | null;
            try {
                accepted = await this.postChargeWithRetries(intent);
            } catch (err) {
                if (err instanceof ChargeRejectedError) {
                    if (isChargeLimitRejection(err)) this.markChargeLimitHit();
                    // Server definitively rejected: unwind local reservation.
                    this.pendingCharges.delete(intent.idempotencyKey);
                    chargedCount = this.applyAcceptedCount(
                        normalizedEventName,
                        chargedCount,
                        0,
                        eventPricing.price,
                    );
                    return {
                        eventChargeLimitReached: true,
                        chargedCount: 0,
                        chargeableWithinLimit: calculateChargeableWithinLimit(),
                    };
                }
                throw err;
            }
            if (accepted !== null) {
                this.pendingCharges.delete(intent.idempotencyKey);
                if (accepted > 0) this.lastAcceptedOperationId = intent.idempotencyKey;
                // The platform capped the charge at the run's limit.
                if (accepted < chargedCount) this.markChargeLimitHit();
                chargedCount = this.applyAcceptedCount(
                    normalizedEventName,
                    chargedCount,
                    accepted,
                    eventPricing.price,
                );
                this.syncPlatformBudget();
            } else {
                // Not confirmed: keep a durable copy so a crash before the next
                // flush cannot lose it (settlement re-applies it).
                await this._persistPendingCharges();
                throw new ChargeUncertainError(
                    `Charge for event '${normalizedEventName}' (count=${chargedCount}) was not confirmed by the platform; operationId=${intent.idempotencyKey}`,
                    {
                        operationId: intent.idempotencyKey,
                        eventName: normalizedEventName,
                        count: chargedCount,
                    },
                );
            }
        } else if (
            this.isAtHome &&
            !isSyntheticEvent(normalizedEventName) &&
            this.pricingInfo[normalizedEventName] === undefined
        ) {
            console.warn(`[Scrapely] Attempting to charge for an unknown event '${normalizedEventName}'`);
        }

        if (this.chargingLogDataset !== undefined) {
            await this.chargingLogDataset.pushData({
                eventName: normalizedEventName,
                eventTitle: eventPricing.title,
                eventPriceUsd: eventPricing.price,
                chargedCount,
                timestamp: new Date().toISOString(),
            });
        }

        if (chargedCount < count) {
            const subject = count === 1 ? 'instance' : 'instances';
            console.log(
                `[Scrapely] Charging ${count} ${subject} of '${normalizedEventName}' event would exceed maxTotalChargeUsd - only ${chargedCount} events were charged`,
            );
        }

        return {
            eventChargeLimitReached: this.calculateMaxEventChargeCountWithinLimit(normalizedEventName) <= 0,
            chargedCount,
            chargeableWithinLimit: calculateChargeableWithinLimit(),
        };
    }

}

/**
 * Helper for PPE-aware pushing of data to the dataset.
 *
 * 1. Calculate limits based on budget
 * 2. Push limited items via the provided callback
 * 3. Charge for the events
 */

function extractChargeResponseBody(response: unknown): Record<string, unknown> {
    if (!response || typeof response !== 'object') return {};
    const r = response as Record<string, unknown>;
    // The API client's httpClient.call returns { data, statusCode, ... }
    if (r.data && typeof r.data === 'object' && !Array.isArray(r.data)) {
        return r.data as Record<string, unknown>;
    }
    return r;
}

function readAcceptedCount(body: Record<string, unknown>, fallback: number): number {
    const raw = body.acceptedCount;
    if (typeof raw === 'number' && Number.isFinite(raw) && raw >= 0) {
        return Math.floor(raw);
    }
    return fallback;
}

function classifyChargeHttpError(
    err: unknown,
    intent: PendingChargeIntent,
): ChargeRejectedError | null {
    const status =
        (err as { statusCode?: number; status?: number })?.statusCode ??
        (err as { statusCode?: number; status?: number })?.status ??
        (err as { response?: { statusCode?: number; status?: number } })?.response?.statusCode ??
        (err as { response?: { statusCode?: number; status?: number } })?.response?.status;

    // Definitive client/billing/auth outcomes — do not retry.
    // 401/403: ownership or scoped-run authorization failures are permanent for this key.
    if (
        status === 400 ||
        status === 401 ||
        status === 402 ||
        status === 403 ||
        status === 404 ||
        status === 405 ||
        status === 409
    ) {
        return new ChargeRejectedError(
            `Charge rejected by platform (HTTP ${status}) for event '${intent.eventName}'`,
            {
                operationId: intent.idempotencyKey,
                eventName: intent.eventName,
                count: intent.count,
                statusCode: status,
                reason: status === 402 ? 'charge-limit-exceeded' : `http-${status}`,
            },
        );
    }

    // A 5xx / 408 / 429 answer is never a definitive rejection, whatever its text
    // says: the charge may have applied, and only a retry with the same
    // Idempotency-Key can tell (classifying it as rejected unwound the local
    // count and the caller charged again under a new key).
    if (status !== undefined && isTransientStatus(status)) return null;
    const message = String((err as Error)?.message ?? err ?? '');
    if (
        /charge-limit-exceeded|unknown-event|ledger-sealed|invalid-input|conflict/i.test(message)
    ) {
        return new ChargeRejectedError(message, {
            operationId: intent.idempotencyKey,
            eventName: intent.eventName,
            count: intent.count,
            statusCode: status,
            reason: 'message-match',
        });
    }
    return null;
}

/**
 * @internal Fail before anything is charged when an item cannot be serialized
 * for the dataset (circular reference, BigInt, a bare function or undefined):
 * the push would otherwise fail after the charge, with nothing stored.
 */
export function assertItemsSerializable(items: unknown[]): void {
    items.forEach((item, index) => {
        let json: string | undefined;
        try {
            json = JSON.stringify(item);
        } catch (err) {
            throw new Error(`Dataset item at index ${index} is not serializable to JSON: ${(err as Error).message}`);
        }
        if (json === undefined) throw new Error(`Dataset item at index ${index} is not serializable to JSON`);
    });
}

// Credit / void helpers that also work with partial managers (tests, older shims).
const takeVoidCredit = (m: ChargingManager, name: string, count: number): number =>
    isSyntheticEvent(name) ? 0 : (m._takeVoidCredit?.(name, count) ?? 0);
const returnVoidCredit = (m: ChargingManager, name: string, count: number): void => m._returnVoidCredit?.(name, count);
async function voidOrCredit(
    m: ChargingManager,
    name: string,
    operationId: string,
    reserved: number,
    uncertain: boolean,
    creditIfStands = true,
): Promise<'voided' | 'stands' | 'unknown'> {
    if (creditIfStands && m._voidOrCredit) return m._voidOrCredit(name, operationId, reserved, uncertain);
    if (m._voidOutcome) return m._voidOutcome(name, operationId, reserved, uncertain);
    if (uncertain) await m._cancelUncertainCharge?.(name, operationId, reserved);
    else await m._voidCharge(name, operationId, reserved);
    return 'voided';
}

export async function pushDataAndCharge<T>({
    chargingManager,
    items,
    eventName,
    isDefaultDataset,
    pushFn,
    validateItems,
}: {
    chargingManager: ChargingManager;
    items: T | T[];
    eventName: string | undefined;
    isDefaultDataset: boolean;
    pushFn: (limitedItems: T | T[]) => Promise<void>;
    /** Extra checks the storage would make after the charge (e.g. crawlee's object / size rules). */
    validateItems?: (items: T[]) => void;
}): Promise<ChargeResult> {
    return chargingManager._withOperationLock(async () => {
        if (chargingManager.isClosed()) {
            throw new Error('ChargingManager is closed (actor is shutting down); charge rejected');
        }
        // A void that did not go through for an earlier failed push must land
        // before this push charges again, or one unsaved result is billed twice.
        const voidsConfirmed = (await chargingManager._retryPendingVoids?.()) !== false;
        const requested = Array.isArray(items) ? items.length : 1;
        // Charge-limit safety net: once the run's spending limit is used up, a
        // default-dataset push that does not pay per row (no event, or only free
        // events) stores nothing. Paid pushes go on below, where the budget cap
        // stores only the rows that are actually charged.
        // Rows the Actor already paid for with an earlier charge() (charge first,
        // then a plain push) are still stored: see ChargingManager._takeUnpaidRows.
        // The result still reports the limit, so the Actor stops after them.
        let unpaidPastLimit = false;
        if (
            isDefaultDataset &&
            requested > 0 &&
            // Optional calls: partial managers (tests, older shims) skip the net, as before.
            chargingManager._isPayPerEvent?.() === true &&
            !chargingManager._isPaidPerItem?.(eventName, isDefaultDataset)
        ) {
            unpaidPastLimit = chargingManager.isChargeLimitReached?.() ?? false;
            const allowed = chargingManager._takeUnpaidRows(requested);
            if (allowed < requested) {
                chargingManager._noteRefusedDefaultDatasetItems(requested - allowed);
                if (allowed === 0) {
                    return {
                        eventChargeLimitReached: true,
                        chargedCount: 0,
                        chargeableWithinLimit: chargingManager._chargeableWithinLimit?.() ?? {},
                    };
                }
                items = (Array.isArray(items) ? items.slice(0, allowed) : items) as T | T[];
            }
        }
        const { limitedItems, eventsToCharge } = chargingManager.calculatePushDataLimits({
            items,
            eventName,
            isDefaultDataset,
        });
        // Only events sent to the platform count: the synthetic default-dataset
        // event is local bookkeeping and never charged through the API.
        if (!voidsConfirmed && Object.keys(eventsToCharge).some((name) => !isSyntheticEvent(name))) {
            // The charge for an earlier, unsaved push may still stand: charging
            // this push now could bill one result twice. Let the caller retry.
            // Pushes that charge nothing go through.
            throw new Error(
                '[Scrapely] An earlier charge for a failed dataset push could not be voided yet; retry the push',
            );
        }

        const itemsArray = Array.isArray(items) ? items : [items];
        const allItemsTrimmed = itemsArray.length > 0 && limitedItems.length === 0;
        if (isDefaultDataset && limitedItems.length < itemsArray.length) {
            // Rows beyond what the budget can pay for are dropped.
            chargingManager._markChargeLimitHit?.();
            chargingManager._noteRefusedDefaultDatasetItems?.(itemsArray.length - limitedItems.length);
        }

        if (limitedItems.length === 0) {
            return {
                eventChargeLimitReached: allItemsTrimmed,
                chargedCount: 0,
                chargeableWithinLimit: {},
            };
        }

        // Charge BEFORE persisting so a server cap/reject cannot leave unbilled
        // items in the dataset. Operation lock is held across both steps.
        let chargeResult: ChargeResult | undefined;
        if (Object.keys(eventsToCharge).length > 0) {
            // An item the storage would refuse fails here, before it is charged.
            assertItemsSerializable(limitedItems);
            validateItems?.(limitedItems as T[]);
            const results: ChargeResult[] = [];
            // Synthetic/default-dataset events first (local-only); then explicit.
            const ordered = Object.entries(eventsToCharge).sort(([a], [b]) => {
                const aSyn = isSyntheticEvent(a) ? 0 : 1;
                const bSyn = isSyntheticEvent(b) ? 0 : 1;
                return aSyn - bSyn;
            });
            // credit: events of this push paid from credit (an earlier charge that could not be voided).
            const reservedInOp: Array<{ name: string; reserved: number; operationId?: string; uncertain?: boolean; credit?: number }> = [];
            // Undo charge i of this operation: void a platform charge, unwind a local
            // one, give credit back. A refused void becomes credit (its results were
            // not stored) unless the caller handles that itself (creditIfStands=false).
            const undoOp = async (i: number, creditIfStands = true): Promise<'voided' | 'stands' | 'unknown'> => {
                const entry = reservedInOp[i]!;
                const { name, reserved, operationId, uncertain } = entry;
                if (reserved <= 0) return 'voided';
                if (entry.credit) {
                    returnVoidCredit(chargingManager, name, entry.credit);
                    entry.credit = 0;
                    return 'voided';
                }
                if (operationId) return voidOrCredit(chargingManager, name, operationId, reserved, !!uncertain, creditIfStands);
                chargingManager.applyAcceptedCount(name, reserved, 0);
                return 'voided';
            };
            try {
                for (const [name, chargeCount] of ordered) {
                    chargingManager._takeLastAcceptedOperationId?.();
                    const credit = takeVoidCredit(chargingManager, name, chargeCount);
                    if (credit > 0) {
                        results.push({ eventChargeLimitReached: false, chargedCount: credit, chargeableWithinLimit: chargingManager._chargeableWithinLimit?.() ?? {} });
                        reservedInOp.push({ name, reserved: credit, credit });
                        continue;
                    }
                    let r: ChargeResult;
                    try {
                        r = await chargingManager._chargeExclusive({
                            eventName: name,
                            count: chargeCount,
                        });
                    } catch (chargeErr) {
                        if (!(chargeErr instanceof ChargeUncertainError)) throw chargeErr;
                        // The platform did not confirm the charge (5xx / timeouts after
                        // retries). Never drop the items for that: the intent stays
                        // pending with its idempotency key and is re-posted by the exit
                        // flush, so the items are stored now and billed once.
                        console.warn(
                            `[Scrapely] Charge for '${name}' not confirmed yet (${chargeErr.operationId}); storing the items and retrying the charge later`,
                        );
                        r = {
                            eventChargeLimitReached: false,
                            chargedCount: chargeErr.count,
                            chargeableWithinLimit: chargingManager._chargeableWithinLimit?.() ?? {},
                        };
                        results.push(r);
                        reservedInOp.push({ name, reserved: chargeErr.count, operationId: chargeErr.operationId, uncertain: true });
                        continue;
                    }
                    results.push(r);
                    reservedInOp.push({ name, reserved: r.chargedCount, operationId: chargingManager._takeLastAcceptedOperationId?.() });
                }
            } catch (err) {
                // Failure after a partial multi-event reserve: undo every prior
                // charge of this operation, voiding the ones the platform accepted
                // (not only unwinding them locally).
                for (let i = 0; i < reservedInOp.length; i++) await undoOp(i);
                throw err;
            }
            // One platform event accepted more than another: only the smaller
            // count of items is stored, so the larger platform charge is voided
            // and charged again for exactly that count. A charge that cannot be
            // voided (definitive answer) stays as before; one whose void is not
            // confirmed yet fails the push so the caller retries it.
            // (Today a push posts at most one platform event; this keeps it
            // correct if that changes.)
            const stood = new Set<number>();
            for (let round = 0; round < 3; round++) {
                const acceptedNow = Math.min(limitedItems.length, ...results.map((r) => r.chargedCount));
                const excess = results
                    .map((r, i) => i)
                    .filter((i) => reservedInOp[i]?.operationId && !stood.has(i) && results[i]!.chargedCount > acceptedNow);
                if (acceptedNow <= 0 || excess.length === 0) break;
                for (const i of excess) {
                    const name = reservedInOp[i]!.name;
                    // A refused void here keeps its stored share; the rest becomes credit below.
                    const outcome = await undoOp(i, false);
                    if (outcome === 'stands') {
                        stood.add(i);
                        continue;
                    }
                    if (outcome === 'unknown') {
                        for (let j = 0; j < reservedInOp.length; j++) if (j !== i) await undoOp(j);
                        throw new Error('[Scrapely] An earlier charge for a failed dataset push could not be voided yet; retry the push');
                    }
                    chargingManager._takeLastAcceptedOperationId?.();
                    let r: ChargeResult;
                    let operationId: string | undefined;
                    let uncertain = false;
                    try {
                        r = await chargingManager._chargeExclusive({ eventName: name, count: acceptedNow });
                        operationId = chargingManager._takeLastAcceptedOperationId?.();
                    } catch (chargeErr) {
                        if (!(chargeErr instanceof ChargeUncertainError)) throw chargeErr;
                        r = {
                            eventChargeLimitReached: false,
                            chargedCount: chargeErr.count,
                            chargeableWithinLimit: chargingManager._chargeableWithinLimit?.() ?? {},
                        };
                        operationId = chargeErr.operationId;
                        uncertain = true;
                    }
                    results[i] = r;
                    reservedInOp[i] = { name, reserved: r.chargedCount, operationId, uncertain };
                }
            }
            // Persist only what every charged event accepted.
            // When multiple events are charged for the same items, use the
            // minimum accepted count so storage never exceeds any billable event.
            const acceptedForItems = Math.min(
                limitedItems.length,
                ...results.map((r) => r.chargedCount),
            );

            // Unwind any event that reserved more than we will persist.
            // Critical for synthetic default-dataset (local-only): if the
            // explicit server charge caps below the synthetic reservation,
            // leaving the synthetic count high permanently overstates local spend.
            for (let i = 0; i < ordered.length; i++) {
                const [name] = ordered[i]!;
                const reserved = results[i]!.chargedCount;
                if (reserved > acceptedForItems) {
                    // Nothing stored and a platform charge was made: leave its local
                    // count to the void below, which releases it only once the
                    // platform confirms (never on a server without the void route).
                    const platformOp = acceptedForItems <= 0 && reservedInOp[i]?.operationId;
                    if (!platformOp) {
                        const entry = reservedInOp[i]!;
                        let over = reserved - acceptedForItems;
                        const back = Math.min(over, entry.credit ?? 0);
                        if (back > 0) {
                            returnVoidCredit(chargingManager, name, back);
                            entry.credit = (entry.credit ?? 0) - back;
                            entry.reserved -= back;
                            over -= back;
                        }
                        // The platform refused to void it: the unstored share stays billed.
                        if (over > 0 && stood.has(i)) chargingManager._noteUnvoidable?.(name, over);
                        else if (over > 0) chargingManager.applyAcceptedCount(name, acceptedForItems + over, acceptedForItems);
                    }
                    results[i] = {
                        ...results[i]!,
                        chargedCount: acceptedForItems,
                        eventChargeLimitReached: true,
                    };
                }
            }
            // Report rows charged for the pushed event: summing the
            // explicit and default-dataset events would count every row twice.
            chargeResult = { ...results.reduce(mergeChargeResults), chargedCount: acceptedForItems };
            if (isDefaultDataset && acceptedForItems < limitedItems.length) {
                chargingManager._noteRefusedDefaultDatasetItems?.(limitedItems.length - acceptedForItems);
            }

            if (acceptedForItems <= 0) {
                // Nothing will be stored (another event of this push was capped to
                // zero): undo every platform charge this operation made, so no
                // result is billed for items that are never saved.
                for (let i = 0; i < reservedInOp.length; i++) {
                    const entry = reservedInOp[i]!;
                    const { name, operationId, uncertain } = entry;
                    let { reserved } = entry;
                    if (entry.credit) {
                        returnVoidCredit(chargingManager, name, entry.credit);
                        reserved -= entry.credit;
                        entry.credit = 0;
                    }
                    if (reserved <= 0 || !operationId) continue;
                    // Original count: the local reservation is released only when
                    // the platform confirms the void (or that it never applied).
                    await voidOrCredit(chargingManager, name, operationId, reserved, !!uncertain);
                }
                return {
                    eventChargeLimitReached: true,
                    chargedCount: chargeResult.chargedCount,
                    chargeableWithinLimit: chargeResult.chargeableWithinLimit,
                };
            }
            const toPersist = limitedItems.slice(0, acceptedForItems);
            try {
                await chargeGatedPush.run(true, () =>
                    pushFn(Array.isArray(items) ? toPersist : (toPersist[0] as T | T[])),
                );
            } catch (pushErr) {
                // Storage failed after the charge was accepted: a result is billed
                // only if it was saved, so void this operation's platform charges
                // and unwind the local (synthetic) reservations, then surface the
                // error so the caller can retry the push (which charges again).
                console.error(
                    `[Scrapely] Dataset push failed after charge accepted ` +
                        `(items=${acceptedForItems}): ${(pushErr as Error).message}; voiding the charge`,
                );
                // Only when the push certainly stored nothing: the storage API
                // answered with an error, or the connection was never made. A
                // timeout or reset after sending may have stored the items, so
                // the charge stands for those (they may be in the dataset).
                if (!pushCertainlyFailed(pushErr)) throw pushErr;
                for (let i = 0; i < reservedInOp.length; i++) {
                    const entry = reservedInOp[i]!;
                    const { name, operationId, uncertain } = entry;
                    let reserved = results[i]!.chargedCount;
                    if (entry.credit) {
                        const back = Math.min(reserved, entry.credit);
                        returnVoidCredit(chargingManager, name, back);
                        entry.credit -= back;
                        reserved -= back;
                    }
                    if (reserved <= 0) continue;
                    if (operationId) {
                        // An unconfirmed charge is cancelled (never re-posted for items
                        // that were not stored); a refused void becomes credit.
                        await voidOrCredit(chargingManager, name, operationId, reserved, !!uncertain);
                    } else if (isSyntheticEvent(name)) {
                        chargingManager.applyAcceptedCount(name, reserved, 0);
                    }
                }
                throw pushErr;
            }
            return unpaidPastLimit ? { ...chargeResult, eventChargeLimitReached: true } : chargeResult;
        }

        // No billable events — still persist the budget-limited items.
        await chargeGatedPush.run(true, () =>
            pushFn(Array.isArray(items) ? limitedItems : (limitedItems[0] as T | T[])),
        );
        return {
            eventChargeLimitReached: unpaidPastLimit,
            chargedCount: 0,
            chargeableWithinLimit: {},
        };
    });
}

/** Validate `{ eventName: eventsPerRow }` for a multi-event push. */
export function validateRowCharges(charges: unknown): Record<string, number> {
    if (!charges || typeof charges !== 'object' || Array.isArray(charges) || Object.keys(charges).length === 0) {
        throw new Error('charges must be a non-empty object of { eventName: countPerRow }');
    }
    const out: Record<string, number> = {};
    for (const [name, count] of Object.entries(charges as Record<string, unknown>)) {
        if (!name) throw new Error('charges keys must be event names');
        if (isSyntheticEvent(name)) throw new Error(`Cannot charge for synthetic event '${name}' manually`);
        if (typeof count !== 'number' || !Number.isInteger(count) || count < 1) {
            throw new Error(`charges['${name}'] must be a positive integer (events per row)`);
        }
        out[name] = count;
    }
    return out;
}

/**
 * Push rows that each cost several events, atomically per row
 * (`charges = { result: 1, page: 3 }`). A row is stored only when every one of
 * its events was charged: rows the budget cannot pay for in full are not
 * stored, and an operation that came back short is voided and re-charged for
 * the rows that are kept, so no row is stored part-paid and no event is billed
 * for a row that was not stored. Unpriced events are ignored.
 */
export async function pushDataWithCharges<T>({
    chargingManager,
    items,
    charges,
    isDefaultDataset,
    pushFn,
    chargePer = 'row',
    validateItems,
}: {
    chargingManager: ChargingManager;
    items: T | T[];
    charges: Record<string, number>;
    isDefaultDataset: boolean;
    pushFn: (rows: T | T[]) => Promise<void>;
    /** 'batch': charge `charges` once for the whole push; all rows stored if accepted, none if refused. */
    chargePer?: 'row' | 'batch';
    /** Extra checks the storage would make after the charge (e.g. crawlee's object / size rules). */
    validateItems?: (items: T[]) => void;
}): Promise<ChargeResult> {
    const perRowIn = validateRowCharges(charges);
    if (chargePer !== 'row' && chargePer !== 'batch') throw new Error("chargePer must be 'row' or 'batch'");
    const batch = chargePer === 'batch';
    const rowsIn = Array.isArray(items) ? items : [items];
    const single = !Array.isArray(items);
    const store = (rows: T[]) => chargeGatedPush.run(true, () => pushFn(single ? (rows[0] as T) : rows));
    if (rowsIn.length === 0) return { eventChargeLimitReached: false, chargedCount: 0, chargeableWithinLimit: {} };

    return chargingManager._withOperationLock(async () => {
        if (chargingManager.isClosed()) {
            throw new Error('ChargingManager is closed (actor is shutting down); charge rejected');
        }
        const voidsConfirmed = (await chargingManager._retryPendingVoids?.()) !== false;
        if (!chargingManager._isPayPerEvent()) {
            await store(rowsIn);
            return { eventChargeLimitReached: false, chargedCount: 0, chargeableWithinLimit: {} };
        }
        let { perRow, rows } = chargingManager._rowCharges(perRowIn, isDefaultDataset, rowsIn.length, batch);
        if (Object.keys(perRow).length === 0) {
            // Only free events: an unpaid push (prepaid-row rules apply).
            const pastLimit = chargingManager.isChargeLimitReached();
            const allowed = isDefaultDataset ? chargingManager._takeUnpaidRows(rowsIn.length) : rowsIn.length;
            if (allowed < rowsIn.length) chargingManager._noteRefusedDefaultDatasetItems(rowsIn.length - allowed);
            if (allowed > 0) await store(rowsIn.slice(0, allowed));
            return { eventChargeLimitReached: pastLimit, chargedCount: 0, chargeableWithinLimit: chargingManager._chargeableWithinLimit() };
        }
        if (!voidsConfirmed && Object.keys(perRow).some((n) => !isSyntheticEvent(n))) {
            throw new Error('[Scrapely] An earlier charge for a failed dataset push could not be voided yet; retry the push');
        }

        // An item the storage would refuse fails here, before it is charged.
        assertItemsSerializable(rowsIn);
        validateItems?.(rowsIn);

        // credit: paid by an earlier charge whose results were not stored (void refused).
        type Op = { name: string; charged: number; operationId?: string; uncertain: boolean; credit?: boolean };
        const undo = async (ops: Op[]) => {
            for (const op of ops) {
                if (op.charged <= 0) continue;
                if (op.credit) returnVoidCredit(chargingManager, op.name, op.charged);
                else if (!op.operationId) chargingManager.applyAcceptedCount(op.name, op.charged, 0);
                // Cancel / void; a refused void stays billed and becomes credit.
                else await voidOrCredit(chargingManager, op.name, op.operationId, op.charged, op.uncertain);
            }
        };
        const order = Object.entries(perRow).sort(([a], [b]) => Number(isSyntheticEvent(a)) - Number(isSyntheticEvent(b)) || a.localeCompare(b));
        let ops: Op[] = [];
        for (let attempt = 0; attempt < 6 && rows > 0; attempt++) {
            ops = [];
            let shortTo: number | null = null;
            for (const [name, n] of order) {
                const want = n * rows;
                chargingManager._takeLastAcceptedOperationId?.();
                let got: number;
                if (takeVoidCredit(chargingManager, name, want) > 0) {
                    got = want;
                    ops.push({ name, charged: want, uncertain: false, credit: true });
                } else {
                    try {
                        const r = await chargingManager._chargeExclusive({ eventName: name, count: want });
                        got = r.chargedCount;
                        ops.push({ name, charged: got, operationId: chargingManager._takeLastAcceptedOperationId?.(), uncertain: false });
                    } catch (err) {
                        if (!(err instanceof ChargeUncertainError)) {
                            await undo(ops);
                            throw err;
                        }
                        // Not confirmed: stays pending and is re-posted at exit, so it counts as accepted.
                        got = err.count;
                        ops.push({ name, charged: got, operationId: err.operationId, uncertain: true });
                    }
                }
                if (got < want) {
                    shortTo = Math.floor(got / n);
                    break;
                }
            }
            if (shortTo === null) break;
            // An event came back short: void this attempt and retry with the whole
            // rows the budget covers at the FULL per-row cost (sizing from the short
            // event alone came back short again: appstore 64df0a3608d74af88, 0 saved).
            // At least one single-row try: the last affordable row may still fit.
            const attemptOps = ops.map((op) => ({ name: op.name, charged: op.charged }));
            await undo(ops);
            ops = [];
            rows = Math.min(rows - 1, Math.max(chargingManager._rowsAfterShort(attemptOps, perRow), 1));
        }

        if (rows <= 0 || ops.length === 0) {
            await undo(ops);
            chargingManager._markChargeLimitHit();
            if (isDefaultDataset) chargingManager._noteRefusedDefaultDatasetItems(rowsIn.length);
            return { eventChargeLimitReached: true, chargedCount: 0, chargeableWithinLimit: chargingManager._chargeableWithinLimit() };
        }
        const kept = batch ? rowsIn : rowsIn.slice(0, rows);
        if (kept.length < rowsIn.length) {
            chargingManager._markChargeLimitHit();
            if (isDefaultDataset) chargingManager._noteRefusedDefaultDatasetItems(rowsIn.length - kept.length);
        }
        try {
            await store(kept);
        } catch (pushErr) {
            console.error(`[Scrapely] Dataset push failed after charges accepted (rows=${kept.length}): ${(pushErr as Error).message}; voiding the charges`);
            if (pushCertainlyFailed(pushErr)) await undo(ops);
            throw pushErr;
        }
        return {
            eventChargeLimitReached: kept.length < rowsIn.length || chargingManager.isChargeLimitReached(),
            chargedCount: kept.length,
            chargeableWithinLimit: chargingManager._chargeableWithinLimit(),
        };
    });
}

/** HTTP status of an API/client error, if the server answered. */
function httpStatusOf(err: unknown): number | undefined {
    const e = err as { statusCode?: unknown; status?: unknown; response?: { status?: unknown; statusCode?: unknown } } | undefined;
    const raw = e?.statusCode ?? e?.status ?? e?.response?.status ?? e?.response?.statusCode;
    return typeof raw === 'number' && Number.isFinite(raw) ? raw : undefined;
}

const NOT_SENT_CODES = new Set(['ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH']);

/**
 * @internal True when a dataset push certainly stored nothing: the storage API
 * answered with an error status, or the request never reached it. Timeouts and
 * resets after the request was sent are not proof (the write may have landed),
 * and neither is any error of a push one of whose earlier attempts may have
 * been applied (`outcomeUnknown`, set by the storage client's retries).
 */
export function pushCertainlyFailed(err: unknown): boolean {
    for (let e: unknown = err, depth = 0; e && depth < 4; e = (e as { cause?: unknown }).cause, depth++) {
        if ((e as { outcomeUnknown?: unknown }).outcomeUnknown === true) return false;
    }
    if (isConnectFailure(err)) return true;
    for (let e: unknown = err, depth = 0; e && depth < 4; e = (e as { cause?: unknown }).cause, depth++) {
        const status = httpStatusOf(e);
        if (status !== undefined) return status >= 400;
        const code = (e as { code?: unknown }).code;
        if (typeof code === 'string' && NOT_SENT_CODES.has(code)) return true;
    }
    return false;
}

/** API error type (e.g. 'charge-not-found') of a client error, if the server sent one. */
function apiErrorTypeOf(err: unknown): string | undefined {
    const e = err as { type?: unknown; data?: { error?: { type?: unknown } }; response?: { data?: { error?: { type?: unknown } } } } | undefined;
    const t = e?.type ?? e?.data?.error?.type ?? e?.response?.data?.error?.type;
    return typeof t === 'string' ? t : undefined;
}
