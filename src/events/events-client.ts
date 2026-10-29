import { EventEmitter } from 'node:events';
import WebSocket from 'ws';

/**
 * System event received from the Scrapely platform via WebSocket.
 * Matches the format documented for ACTOR_EVENTS_WEBSOCKET_URL.
 */
export interface SystemEvent {
    name: 'systemInfo' | 'cpuInfo' | 'aborting' | 'persistState' | 'migrating';
    createdAt: string;
    data: Record<string, unknown>;
}

export interface SystemInfoData {
    createdAt: string;
    isCpuOverloaded: boolean;
    cpuCurrentUsage: number;
    cpuAvgUsage: number;
    cpuMaxUsage: number;
    memCurrentBytes: number;
    memAvgBytes: number;
    memMaxBytes: number;
}

export interface CpuInfoData {
    isCpuOverloaded: boolean;
}

export interface PersistStateData {
    isMigrating: boolean;
}

let ws: WebSocket | null = null;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
const events = new EventEmitter();
let connected = false;
let shouldReconnect = true;

/**
 * Connect to the Scrapely events WebSocket server.
 * Called during Actor.init().
 * 
 * @param eventsWsUrl - The ACTOR_EVENTS_WEBSOCKET_URL env var value
 * @param token - The SCRAPELY_TOKEN for authentication
 * @param runId - The ACTOR_RUN_ID
 */
export function connectEvents(eventsWsUrl: string, token: string, runId: string): void {
    if (ws && ws.readyState === WebSocket.OPEN) return;

    shouldReconnect = true;

    try {
        // The token goes in a header, not the URL: URLs end up in access logs.
        const url = `${eventsWsUrl}?runId=${encodeURIComponent(runId)}`;
        ws = new WebSocket(url, { headers: { Authorization: `Bearer ${token}` } });

        ws.onopen = () => {
            connected = true;
            console.log(`[Scrapely Events] Connected to ${eventsWsUrl}`);
        };

        ws.onmessage = (event) => {
            try {
                const systemEvent: SystemEvent = JSON.parse(event.data as string);
                events.emit('systemEvent', systemEvent);
                events.emit(systemEvent.name, systemEvent.data);
            } catch (err) {
                console.error(`[Scrapely Events] Failed to parse event: ${(err as Error).message}`);
            }
        };

        ws.onclose = () => {
            connected = false;
            console.log('[Scrapely Events] Disconnected');
            
            // Reconnect after 5 seconds if still running
            if (shouldReconnect) {
                reconnectTimer = setTimeout(() => {
                    console.log('[Scrapely Events] Reconnecting...');
                    connectEvents(eventsWsUrl, token, runId);
                }, 5000);
            }
        };

        ws.onerror = (err) => {
            console.error(`[Scrapely Events] WebSocket error: ${(err as any)?.message || 'unknown'}`);
        };
    } catch (err) {
        console.error(`[Scrapely Events] Failed to connect: ${(err as Error).message}`);
        // Retry after 5 seconds
        if (shouldReconnect) {
            reconnectTimer = setTimeout(() => {
                connectEvents(eventsWsUrl, token, runId);
            }, 5000);
        }
    }
}

/**
 * Disconnect from the events WebSocket server.
 * Called during Actor.exit().
 */
export function disconnectEvents(): void {
    shouldReconnect = false;

    if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
    }

    if (ws) {
        try {
            ws.close(1000, 'Actor exiting');
        } catch {
            // Ignore close errors
        }
        ws = null;
    }
    connected = false;
}

/**
 * Check if the events WebSocket is connected.
 */
export function isEventsConnected(): boolean {
    return connected;
}

/**
 * Subscribe to system events.
 * Use in conjunction with Actor.on() to receive typed events.
 */
export function onSystemEvent(listener: (event: SystemEvent) => void): void {
    events.on('systemEvent', listener);
}

/**
 * Subscribe to specific system event types.
 */
export function onEvent(name: SystemEvent['name'], listener: (data: Record<string, unknown>) => void): void {
    events.on(name, listener);
}

/**
 * Remove a specific event listener.
 */
export function offEvent(name: SystemEvent['name'], listener: (...args: any[]) => void): void {
    events.off(name, listener);
}

export { events };