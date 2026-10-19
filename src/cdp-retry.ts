/**
 * Retry remote-browser (CDP) connects that fail for transient reasons.
 *
 * The remote browser answers 429 when it is at capacity and 503 when it cannot
 * start a browser or validate the session right now, both with a Retry-After
 * delay (repeated in the response body, because Playwright only puts the
 * status line and body into its error message). Connection errors and
 * timeouts are transient too. Those are retried with exponential backoff,
 * waiting at least the Retry-After delay when the error carries one. Any other
 * status (400/401/402/403/404) fails at once.
 *
 * Tuning (environment):
 * - SCRAPELY_CDP_CONNECT_ATTEMPTS: total attempts, default 5 (1 disables retries).
 * - SCRAPELY_CDP_RETRY_BASE_SECS: first backoff delay, default 1; doubles per attempt.
 * - SCRAPELY_CDP_RETRY_MAX_SECS: cap for one backoff delay, default 30.
 * - SCRAPELY_CDP_RETRY_AFTER_MAX_SECS: longest Retry-After the SDK waits out,
 *   default 120. A longer Retry-After ends the retries (the SDK never retries
 *   earlier than the server asked).
 */

const RETRY_WRAPPED = Symbol.for('scrapely.cdpRetryWrapped');

const REASONS =
    'Too Many Requests|Internal Server Error|Bad Gateway|Service Unavailable|Gateway Timeout|' +
    'Request Timeout|Bad Request|Unauthorized|Payment Required|Forbidden|Not Found';
const STATUS_RE = new RegExp(`\\b([1-5]\\d\\d) (?:${REASONS})\\b`);
const WS_STATUS_RE = /Unexpected server response:?\s*(\d{3})/i;
const RETRY_AFTER_RE = /Retry-After:?\s*(\d+(?:\.\d+)?)/i;
const TRANSIENT_MARKERS = [
    'econnrefused',
    'econnreset',
    'etimedout',
    'epipe',
    'eai_again',
    'ehostunreach',
    'enetunreach',
    'socket hang up',
    'timeout',
    'timed out',
    'connection closed',
    'connection refused',
    'connection reset',
    'websocket error',
    'net::err_',
    'target closed',
];

function envNumber(name: string, fallback: number): number {
    const value = Number(process.env[name]);
    return Number.isFinite(value) && value > 0 ? value : fallback;
}

function maxAttempts(): number {
    return Math.max(1, Math.floor(envNumber('SCRAPELY_CDP_CONNECT_ATTEMPTS', 5)));
}

function errorText(err: unknown): string {
    if (err instanceof Error) {
        const code = (err as { code?: string }).code;
        return `${err.name}: ${err.message}${code ? ` ${code}` : ''}`;
    }
    return String(err);
}

/** @internal Exported for tests. */
export function classifyConnectError(err: unknown): { retryable: boolean; status?: number; retryAfterSecs?: number } {
    const text = errorText(err);
    const statusMatch = WS_STATUS_RE.exec(text) ?? STATUS_RE.exec(text);
    const retryAfterMatch = RETRY_AFTER_RE.exec(text);
    const retryAfterSecs = retryAfterMatch ? Number(retryAfterMatch[1]) : undefined;
    if (statusMatch) {
        const status = Number(statusMatch[1]);
        return { retryable: status === 408 || status === 429 || status >= 500, status, retryAfterSecs };
    }
    const lowered = text.toLowerCase();
    return { retryable: TRANSIENT_MARKERS.some((m) => lowered.includes(m)), retryAfterSecs };
}

/** @internal Delay in ms before retry number `attempt` (1-based), honoring Retry-After. */
export function backoffDelayMs(attempt: number, retryAfterSecs?: number): number {
    const base = envNumber('SCRAPELY_CDP_RETRY_BASE_SECS', 1);
    const cap = envNumber('SCRAPELY_CDP_RETRY_MAX_SECS', 30);
    let delay = Math.min(cap, base * 2 ** (attempt - 1)) * (0.75 + Math.random() * 0.5);
    if (retryAfterSecs !== undefined) delay = Math.max(delay, retryAfterSecs);
    return Math.round(delay * 1000);
}

function describe(err: unknown, status?: number): string {
    if (status !== undefined) return `HTTP ${status}`;
    // First line only, URL masked: connect errors can carry the session URL with ids.
    const first = (err instanceof Error ? err.message : String(err)).trim().split('\n')[0] || 'error';
    return first.replace(/(wss?|https?):\/\/\S+/g, '<cdp-url>').slice(0, 200);
}

/** @internal Overridable for tests. */
export const retryTiming = {
    sleep: (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)),
};

/** Run a CDP connect, retrying transient failures with backoff. */
export async function connectWithRetry<T>(connect: () => Promise<T>): Promise<T> {
    const attempts = maxAttempts();
    for (let attempt = 1; ; attempt++) {
        try {
            return await connect();
        } catch (err) {
            const { retryable, status, retryAfterSecs } = classifyConnectError(err);
            if (!retryable || attempt >= attempts) throw err;
            if (retryAfterSecs !== undefined && retryAfterSecs > envNumber('SCRAPELY_CDP_RETRY_AFTER_MAX_SECS', 120)) {
                // Never retry earlier than the server asked; give up instead of waiting that long.
                throw err;
            }
            const delayMs = backoffDelayMs(attempt, retryAfterSecs);
            console.warn(
                `[scrapely] Remote browser connect failed (${describe(err, status)}); ` +
                    `retrying in ${(delayMs / 1000).toFixed(1)}s (attempt ${attempt + 1}/${attempts})`,
            );
            await retryTiming.sleep(delayMs);
        }
    }
}

/** True when `url` points at the same host as the platform's CDP_URL. */
export function isPlatformCdpUrl(url: unknown, platformCdpUrl: string | undefined): boolean {
    if (typeof url !== 'string' || !platformCdpUrl) return false;
    try {
        const target = new URL(url);
        const platform = new URL(platformCdpUrl);
        return !!target.host && target.host.toLowerCase() === platform.host.toLowerCase();
    } catch {
        return false;
    }
}

function connectTarget(arg: unknown): unknown {
    if (typeof arg === 'string') return arg;
    if (arg && typeof arg === 'object') {
        const o = arg as Record<string, unknown>;
        return o.endpointURL ?? o.wsEndpoint ?? o.browserWSEndpoint ?? o.browserURL;
    }
    return undefined;
}

export function isRetryWrapped(fn: unknown): boolean {
    return typeof fn === 'function' && (fn as any)[RETRY_WRAPPED] === true;
}

/**
 * Make `target[method]` (Playwright `connectOverCDP` or Puppeteer `connect`)
 * retry transient failures when it connects to the platform CDP host. Covers
 * actor code that connects to CDP_URL directly as well as the SDK's own
 * redirected launches. Other hosts are left alone. Idempotent.
 */
export function wrapConnectWithRetry(
    target: any,
    method: 'connectOverCDP' | 'connect',
    getPlatformCdpUrl: () => string | undefined,
): void {
    const original = target?.[method];
    if (typeof original !== 'function' || isRetryWrapped(original)) return;
    const wrapped = function (this: unknown, ...args: unknown[]) {
        const self = this ?? target;
        if (!isPlatformCdpUrl(connectTarget(args[0]), getPlatformCdpUrl())) {
            return original.apply(self, args);
        }
        return connectWithRetry(() => original.apply(self, args));
    };
    (wrapped as any)[RETRY_WRAPPED] = true;
    try {
        target[method] = wrapped;
    } catch {
        // frozen object: leave it unwrapped
    }
}
