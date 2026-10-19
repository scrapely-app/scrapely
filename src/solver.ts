/**
 * Anti-bot solver helpers (JS). Python parity: scrapely-python `scrapely.solver`.
 *
 * The platform runs the challenge solver behind /v2/solver; Actors never
 * hold a solver key. Runs of Actors that may use it get SCRAPELY_SOLVER_URL
 * and SCRAPELY_SOLVER_API_KEY (the run's own token).
 *
 *   const st = await Actor.solverStatus('datadome');      // free, cached 15 s
 *   if (!st.usable) { ...fallback or fail the item fast... }
 *
 *   const clearance = await Actor.solve({
 *       url, response: { status, body },                  // the blocked response
 *       fetch: (req) => mySession(req),                   // send step requests on YOUR session
 *       sessionId: 's-42', identity: 'chrome-146-macos',
 *   });
 *   // clearance.cookies -> put them in your cookie jar (fetch already sent the rest)
 *
 * Errors: SolveAttemptError (retry on a fresh exit), IpBanned, VendorUnavailable
 * (fail fast, `code`), SolverRateLimited, SolverAuthError.
 */

export const DEGRADED_STATUS_MESSAGE = 'Site protection temporarily unavailable, results may be incomplete';

const COMPAT_SUFFIXES = ['/compat/v1', '/compat/xhrdev'];
const STATUS_TTL_MS = 15_000;
const MAX_BODY = 4 * 1024 * 1024;
const UNAVAILABLE_CODES = new Set(['vendor_degraded', 'vendor_down', 'provider_unavailable', 'vendor_unsupported', 'solver_not_enabled']);

export class SolveAttemptError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'SolveAttemptError';
    }
}

export class IpBanned extends SolveAttemptError {
    constructor(message: string) {
        super(message);
        this.name = 'IpBanned';
    }
}

export class SolverRateLimited extends SolveAttemptError {
    constructor(readonly retryAfterSecs: number | null = null) {
        super(`solver rate limit hit (HTTP 429${retryAfterSecs !== null ? `, retry after ${retryAfterSecs}s` : ''})`);
        this.name = 'SolverRateLimited';
    }
}

export class SolverAuthError extends SolveAttemptError {
    constructor(message: string) {
        super(message);
        this.name = 'SolverAuthError';
    }
}

/** The platform says this vendor cannot be solved right now. Fail fast; do not retry. */
export class VendorUnavailable extends Error {
    constructor(readonly code: string, readonly vendor: string | null = null, readonly retryAfterSecs: number | null = null, message?: string) {
        super(message ?? `anti-bot solver unavailable (${code}${vendor ? `, ${vendor}` : ''})`);
        this.name = 'VendorUnavailable';
    }
}

export interface SolverVendorStatus {
    status: 'healthy' | 'degraded' | 'down' | 'unsupported' | 'unknown';
    usable: boolean;
    code: string | null;
    successRate30m: number | null;
    since: string | null;
    recheckAfterSecs: number;
}

export interface SolverStepRequest {
    id: string;
    method: 'GET' | 'POST';
    url: string;
    headers: Record<string, string>;
    body?: string;
}

export interface SolveCookie {
    name: string;
    value: string;
    domain: string;
    path: string;
}

export interface SolveOptions {
    /** The page that was blocked. */
    url: string;
    /** The blocked response. */
    response: { status: number; body: string };
    /** Sends one step request on the Actor's own session (same exit IP); returns its status and body. */
    fetch: (request: SolverStepRequest) => Promise<{ status: number; body: string }>;
    /** 'datadome', 'akamai-sbsd' or 'auto' (default). */
    vendor?: string;
    /** Proxy session id (enables the platform's clearance cache for that exit). */
    sessionId?: string;
    /** Browser profile the TLS impersonation claims (default chrome-146-macos), or an object with locale. */
    identity?: string | { profile?: string; acceptLanguage?: string; timezone?: string; languages?: string };
    /** Cookie header the session holds for the site (SBSD needs it). */
    cookieHeader?: () => string | null | Promise<string | null>;
    /** Verifies the clearance (cookies already applied by you or by `fetch`); reported to the platform. */
    verify?: (cookies: SolveCookie[]) => Promise<boolean> | boolean;
    timeoutMs?: number;
}

export interface Clearance {
    solveId: string | null;
    cookies: SolveCookie[];
    ttlHintSecs: number;
    cached: boolean;
    verified: boolean | null;
}

function solverEnv(): { url: string; key: string } | null {
    const url = (process.env.SCRAPELY_SOLVER_URL ?? '').trim().replace(/\/+$/, '');
    const key = (process.env.SCRAPELY_SOLVER_API_KEY ?? '').trim();
    return url && key ? { url, key } : null;
}

function gateway(): { base: string; key: string } | null {
    const env = solverEnv();
    if (!env) return null;
    const suffix = COMPAT_SUFFIXES.find((s) => env.url.endsWith(s));
    if (!suffix) return null;
    return { base: env.url.slice(0, -suffix.length), key: env.key };
}

/** True when the platform solver API is available to this run. */
export function solverConfigured(): boolean {
    return gateway() !== null;
}

/**
 * The protection behind a *challenge* response. Ordinary pages that merely load a
 * vendor script are not challenges: DataDome, PerimeterX and Kasada need a 4xx
 * status; an SBSD bundle counts only on a short interstitial page.
 */
export function detectVendor(status: number, body: string): string | null {
    const all = body || '';
    const sample = all.slice(0, 20000);
    const head = sample.slice(0, 4000).toLowerCase();
    const challenged = status >= 400;
    if (challenged && (head.includes('captcha-delivery.com') || /var\s+dd\s*=\s*\{/.test(sample) || head.includes('datadome'))) return 'datadome';
    if (all.length <= 20000 && /<script[^>]+src="([^"]+\?v=[0-9a-f-]{36}[^"]*)"/i.test(sample)) return 'akamai-sbsd';
    if (challenged && /_pxappid|px-captcha|perimeterx|\/_px\d?\//i.test(sample)) return 'perimeterx';
    if (challenged && /kpsdk/i.test(sample)) return 'kasada';
    return null;
}

/** A step may only go to the target page's own host or to captcha-delivery.com, over https. */
export function stepAllowed(stepUrl: string, targetUrl: string): boolean {
    try {
        const step = new URL(stepUrl);
        const target = new URL(targetUrl);
        if (step.protocol !== 'https:') return false;
        const host = step.hostname.toLowerCase();
        return host === target.hostname.toLowerCase() || host === 'captcha-delivery.com' || host.endsWith('.captcha-delivery.com');
    } catch {
        return false;
    }
}

export function isBlocked(response: { status: number; body: string }, vendor?: string): boolean {
    const found = detectVendor(response.status, response.body);
    return !vendor || vendor === 'auto' ? found !== null : found === vendor;
}

export function degradedStatusMessage(skipped?: number, total?: number): string {
    return skipped !== undefined && total !== undefined ? `${DEGRADED_STATUS_MESSAGE} (${skipped} of ${total} items skipped)` : DEGRADED_STATUS_MESSAGE;
}

const statusCache = new Map<string, { at: number; value: SolverVendorStatus }>();

function unknownStatus(code: string | null = null): SolverVendorStatus {
    return { status: 'unknown', usable: code === null, code, successRate30m: null, since: null, recheckAfterSecs: 300 };
}

/** Vendor status from the platform (free, cached for 15 s). Never throws. */
export async function solverStatus(vendor: string): Promise<SolverVendorStatus> {
    const name = (vendor || '').trim().toLowerCase();
    const hit = statusCache.get(name);
    if (hit && Date.now() - hit.at < STATUS_TTL_MS) return hit.value;
    const gw = gateway();
    if (!gw) return unknownStatus(solverEnv() ? null : 'solver_not_enabled');
    try {
        const res = await fetch(`${gw.base}/status?vendors=${encodeURIComponent(name)}`, {
            headers: { authorization: `Bearer ${gw.key}` },
            signal: AbortSignal.timeout(10_000),
        });
        if (!res.ok) return unknownStatus();
        const data = (await res.json()) as { vendors?: Record<string, SolverVendorStatus> };
        const value = data.vendors?.[name] ?? unknownStatus();
        statusCache.set(name, { at: Date.now(), value });
        return value;
    } catch {
        return unknownStatus();
    }
}

async function api(path: string, body: unknown, key: string, base: string, timeoutMs: number): Promise<{ status: number; data: any; retryAfter: string | null }> {
    let res: Response;
    try {
        res = await fetch(`${base}${path}`, {
            method: 'POST',
            headers: { authorization: `Bearer ${key}`, 'content-type': 'application/json' },
            body: JSON.stringify(body),
            signal: AbortSignal.timeout(timeoutMs),
        });
    } catch (err) {
        // Gateway unreachable or timed out: one failed try, retried on a fresh session.
        throw new SolveAttemptError(`solver gateway unreachable: ${(err as Error).name}`);
    }
    let data: any = {};
    try { data = await res.json(); } catch { data = {}; }
    return { status: res.status, data, retryAfter: res.headers.get('retry-after') };
}

function handle(r: { status: number; data: any; retryAfter: string | null }, vendor: string | null): any {
    const { status, data } = r;
    if (status === 503 && UNAVAILABLE_CODES.has(String(data.code ?? ''))) throw new VendorUnavailable(String(data.code), data.vendor ?? vendor, data.retryAfterSecs ?? null);
    if (status === 429) throw new SolverRateLimited(r.retryAfter ? Number(r.retryAfter) : (data.retryAfterSecs ?? null));
    if (status === 401 || status === 403) {
        if (data.code === 'solver_not_enabled') throw new VendorUnavailable('solver_not_enabled', vendor, null, 'the anti-bot solver is not enabled for this Actor');
        throw new SolverAuthError(`solver refused the run token (HTTP ${status})`);
    }
    if (status >= 400) throw new SolveAttemptError(`solver HTTP ${status}: ${data.message ?? ''}`.trim());
    return data;
}

/** Solves one challenge on the Actor's session through the platform. */
export async function solve(options: SolveOptions): Promise<Clearance> {
    const gw = gateway();
    const vendor = options.vendor ?? 'auto';
    if (!gw) throw new VendorUnavailable('solver_not_enabled', vendor, null, 'the platform solver is not available to this run');
    const timeoutMs = (options.timeoutMs ?? 60_000) + 30_000;
    const identity = typeof options.identity === 'string' ? { profile: options.identity } : (options.identity ?? { profile: 'chrome-146-macos' });
    const cookieHeader = async () => (options.cookieHeader ? await options.cookieHeader() : null);
    let data = handle(await api('/solve', {
        vendor,
        url: options.url,
        blocked: { status: options.response.status, body: (options.response.body ?? '').slice(0, MAX_BODY) },
        identity,
        timeoutMs: options.timeoutMs ?? 60_000,
        session: { id: options.sessionId, cookieHeader: await cookieHeader() },
    }, gw.key, gw.base, timeoutMs), vendor);
    for (let step = 0; step < 8; step++) {
        if (data.action === 'done') {
            const clearance: Clearance = {
                solveId: data.solveId ?? null,
                cookies: data.cookies ?? [],
                ttlHintSecs: Number(data.ttlHintSecs) || 0,
                cached: !!data.cached,
                verified: null,
            };
            if (options.verify) {
                const ok = !!(await options.verify(clearance.cookies));
                clearance.verified = ok;
                if (clearance.solveId) {
                    await api(`/solves/${clearance.solveId}/outcome`, { verified: ok }, gw.key, gw.base, 15_000).catch(() => undefined);
                }
                if (!ok) throw new SolveAttemptError('still blocked after the clearance was applied');
            }
            return clearance;
        }
        if (data.action === 'failed') {
            const message = String(data.message ?? data.reason ?? 'solve failed');
            if (data.reason === 'ip_banned') throw new IpBanned(message);
            throw new SolveAttemptError(message);
        }
        if (data.action !== 'fetch' && data.action !== 'submit') throw new SolveAttemptError(`unexpected solver answer: ${data.action}`);
        const results = [];
        for (const req of (data.requests ?? []) as SolverStepRequest[]) {
            if (!req || !stepAllowed(String(req.url ?? ''), options.url) || (req.method !== 'GET' && req.method !== 'POST')) {
                throw new SolveAttemptError('the solver asked for a request outside the target site; refused');
            }
            let r: { status: number; body: string };
            try {
                r = await options.fetch(req);
            } catch (err) {
                throw new SolveAttemptError(`challenge request failed: ${(err as Error).name}`);
            }
            results.push({ id: req.id, status: r.status, body: (r.body ?? '').slice(0, MAX_BODY) });
        }
        data = handle(await api('/solve', { solveId: data.solveId, results, session: { id: options.sessionId, cookieHeader: await cookieHeader() } }, gw.key, gw.base, timeoutMs), vendor);
    }
    throw new SolveAttemptError('the solve did not finish');
}

/** Reports your own verification of a clearance (when `verify` was not passed to solve). */
export async function reportSolveOutcome(clearance: Clearance, verified: boolean): Promise<void> {
    const gw = gateway();
    if (!gw || !clearance.solveId) return;
    clearance.verified = verified;
    await api(`/solves/${clearance.solveId}/outcome`, { verified }, gw.key, gw.base, 15_000).catch(() => undefined);
}

/** Test hook. */
export function clearSolverStatusCache(): void {
    statusCache.clear();
}
