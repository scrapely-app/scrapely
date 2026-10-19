import { betterClearInterval } from '@apify/utilities';
import { EventManager, EventType, type EventTypeName } from '@crawlee/core';

import type { Configuration } from '../configuration.js';
import { events as platformEvents } from './events-client.js';

/**
 * Crawlee EventManager wired to Scrapely platform WebSocket events and persistState interval.
 */
export class ScrapelyEventManager extends EventManager {
    private readonly wsUnsubs: Array<() => void> = [];

    constructor(config: Configuration) {
        super(config);
    }

    override async init(): Promise<void> {
        if (this.initialized) {
            return;
        }

        await super.init();
        this.bindPlatformEvents();
    }

    override async close(): Promise<void> {
        this.unbindPlatformEvents();
        await super.close();
    }

    private bindPlatformEvents(): void {
        const forward = (name: EventTypeName | 'cpuInfo', handler: (...args: any[]) => void) => {
            platformEvents.on(name, handler);
            this.wsUnsubs.push(() => platformEvents.off(name, handler));
        };

        forward(EventType.PERSIST_STATE, (data) => {
            this.emit(EventType.PERSIST_STATE, data);
        });

        forward(EventType.MIGRATING, () => {
            if (this.intervals.persistState) {
                betterClearInterval(this.intervals.persistState);
                this.intervals.persistState = undefined;
            }
            this.emit(EventType.PERSIST_STATE, { isMigrating: true });
            this.emit(EventType.MIGRATING);
        });

        forward(EventType.ABORTING, () => {
            this.emit(EventType.ABORTING);
        });

        // The base EventManager never measures the container, so these are the only
        // readings Crawlee's Snapshotter (and therefore autoscaling) gets on the platform.
        forward(EventType.SYSTEM_INFO, (data) => {
            this.emit(EventType.SYSTEM_INFO, data);
        });

        forward('cpuInfo', (data) => {
            this.emit('cpuInfo' as EventTypeName, data);
        });
    }

    private unbindPlatformEvents(): void {
        for (const unsub of this.wsUnsubs) {
            unsub();
        }
        this.wsUnsubs.length = 0;
    }
}
