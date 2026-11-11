/**
 * Results a resumed run already saved are not saved or charged again.
 *
 * A resurrected or rebooted run keeps its default dataset, but the Actor's code
 * starts again from the beginning. An Actor that keeps no state of its own (no
 * request queue) then collects the results it saved before once more, and each
 * charged push would bill them a second time.
 *
 * On the first charged push to the default dataset of such a run, the SDK reads
 * the result key (`id` unless the Actor sets another with `Actor.setResultKey`)
 * of the rows already saved. A later charged row whose key is one of them is
 * neither stored nor charged: the dataset already holds it and it was paid for
 * when it was first saved. It still counts as accepted, in the push result and
 * in `getChargedEventCount`, so Actors that compare what they pushed with what
 * was charged do not read it as a refused charge.
 *
 * The key must identify a row. When the saved rows repeat a key, or this run
 * pushes one key twice, it does not, and the run goes on as before: every row
 * is stored and charged. Nothing is skipped in a first run, in a run that saved
 * nothing yet, after a metamorph, or for pushes that are not charged per row.
 *
 * A resumed generation also starts the Actor's own result count from zero. An
 * Actor that declares its run-level result limit (`Actor.setResultLimit(n)`,
 * e.g. its `maxItems` input) gets it applied across generations: once the rows
 * saved before plus the new rows of this generation reach `n`, further charged
 * rows are refused like at the spending limit (not stored, not charged, and the
 * push reports the limit), so the run ends with `n` results, not more.
 */
export const MAX_SAVED_ROWS = 200_000;
const PAGE = 5_000;
const OFF_VALUES = new Set(['0', 'off', 'false', 'no']);

type Key = string;

/** What the dedupe needs from the run's API client (scrapely-client / apify-client shape). */
export interface ResumeDedupeClient {
    dataset(id: string): {
        get(): Promise<{ itemCount?: number } | undefined>;
        listItems(opts: { fields?: string[]; offset?: number; limit?: number }): Promise<{ items: unknown[] }>;
    };
    run(id: string): { get(): Promise<{ stats?: Record<string, unknown> } | undefined> };
}

function scalar(value: unknown): string | undefined {
    if (typeof value === 'number' && Number.isInteger(value)) return String(value);
    if (typeof value === 'string' && value.trim()) return value;
    return undefined;
}

/** The row's key, or undefined when the row lacks a usable value for a key field. */
export function rowKey(row: unknown, fields: string[]): Key | undefined {
    if (!row || typeof row !== 'object' || Array.isArray(row)) return undefined;
    const parts: string[] = [];
    for (const name of fields) {
        const part = scalar((row as Record<string, unknown>)[name]);
        if (part === undefined) return undefined;
        parts.push(part);
    }
    return JSON.stringify(parts);
}

function normalizeKey(key: string | string[] | null | undefined): string[] | null {
    if (key === null || key === undefined) return null;
    const names = typeof key === 'string' ? [key] : [...key];
    if (names.length === 0 || !names.every((n) => typeof n === 'string' && n.length > 0)) {
        throw new Error('result key must be a field name or a list of field names');
    }
    return names;
}

export class ResumeDedupe {
    private fields: string[] | null = ['id'];
    private state: 'unknown' | 'on' | 'off' = 'unknown';
    private saved = new Set<Key>();
    private seen = new Set<Key>();
    private skippedTotal = 0;
    private announced = false;
    private loading?: Promise<void>;
    /** Run-level result limit the Actor declared (undefined: not declared). */
    private limit?: number;
    /** Rows the default dataset held when this resumed generation started (undefined: not a resumed run). */
    private resumedRows?: number;
    /** New rows this generation stored through charged pushes. */
    private newRows = 0;
    private limitAnnounced = false;

    setResultLimit(limit: number | null | undefined): void {
        if (limit === null || limit === undefined) {
            this.limit = undefined;
            return;
        }
        if (typeof limit !== 'number' || !Number.isInteger(limit) || limit < 1) {
            throw new Error('result limit must be a positive integer or null');
        }
        this.limit = limit;
    }

    /** New rows this resumed generation may still store under the declared limit (undefined: no cap). */
    remaining(): number | undefined {
        if (this.limit === undefined || this.resumedRows === undefined) return undefined;
        return Math.max(0, this.limit - this.resumedRows - this.newRows);
    }

    noteNewRows(count: number): void {
        if (count > 0) this.newRows += count;
    }

    noteCapped(): void {
        if (this.limitAnnounced) return;
        this.limitAnnounced = true;
        console.log(`[Scrapely] This run was resumed with ${this.resumedRows} results saved before; it stops at its limit of ${this.limit} results`);
    }

    setResultKey(key: string | string[] | null | undefined): void {
        const fields = normalizeKey(key);
        if (JSON.stringify(fields) === JSON.stringify(this.fields)) return;
        this.fields = fields;
        if (this.state === 'on') {
            this.state = 'unknown';
            this.saved = new Set();
            this.seen = new Set();
        }
    }

    get skipped(): number {
        return this.skippedTotal;
    }

    private off(reason: string): void {
        if (this.state === 'on') {
            console.log(`[Scrapely] Results this run saved before it was resumed are charged again from here on: ${reason}`);
        }
        this.state = 'off';
        this.saved = new Set();
        this.seen = new Set();
    }

    private async load(opts: { client?: ResumeDedupeClient; runId?: string; datasetId?: string; isAtHome: boolean }): Promise<void> {
        this.state = 'off';
        const fields = this.fields;
        if (OFF_VALUES.has((process.env.SCRAPELY_RESUME_DEDUPE ?? '').trim().toLowerCase())) return;
        if (!fields && this.limit === undefined) return;
        const { client, runId, datasetId, isAtHome } = opts;
        if (!isAtHome || !client || !runId || !datasetId) return;
        const saved = new Set<Key>();
        try {
            const info = await client.dataset(datasetId).get();
            const itemCount = Number(info?.itemCount ?? 0) || 0;
            if (itemCount <= 0) return; // first run, or nothing saved yet
            const run = await client.run(runId).get();
            const stats = run?.stats ?? {};
            const resumed = (Number(stats.resurrectCount) || 0) + (Number(stats.rebootCount) || 0);
            if (resumed <= 0 || (Number(stats.metamorph) || 0) > 0) return;
            this.resumedRows = itemCount;
            if (!fields) return; // no result key: only the declared limit applies
            if (itemCount > MAX_SAVED_ROWS) {
                console.log(
                    `[Scrapely] This run was resumed with ${itemCount} saved results, more than the ${MAX_SAVED_ROWS} the SDK reads `
                    + 'to skip results saved before; every result is saved and charged as it comes',
                );
                return;
            }
            let keyed = 0;
            let offset = 0;
            while (offset < itemCount) {
                const page = await client.dataset(datasetId).listItems({ fields, offset, limit: PAGE });
                const rows = page?.items ?? [];
                if (rows.length === 0) break;
                for (const row of rows) {
                    const key = rowKey(row, fields);
                    if (key === undefined) continue;
                    keyed++;
                    // Saved rows repeat their result key: it does not identify a row.
                    if (saved.has(key)) return;
                    saved.add(key);
                }
                offset += rows.length;
            }
            if (keyed < 2) return;
        } catch {
            // Could not read the saved rows: every row is saved and charged as before.
            return;
        }
        this.saved = saved;
        this.seen = new Set();
        this.state = 'on';
    }

    /** Decide once per process whether this run is a resumed one with saved rows. */
    async ensure(opts: { client?: ResumeDedupeClient; runId?: string; datasetId?: string; isAtHome: boolean }): Promise<boolean> {
        if (this.state === 'unknown') {
            this.loading ??= this.load(opts).finally(() => { this.loading = undefined; });
            await this.loading;
        }
        return this.state === 'on' || this.remaining() !== undefined;
    }

    /** Split rows into rows to push, keys of skipped rows, and the key of each row to push. Nothing is recorded yet. */
    plan<T>(rows: T[]): { kept: T[]; skipped: Key[]; pushed: Array<Key | undefined> } {
        const fields = this.fields;
        if (this.state !== 'on' || !fields) return { kept: rows, skipped: [], pushed: [] };
        const kept: T[] = [];
        const skipped: Key[] = [];
        const pushed: Array<Key | undefined> = [];
        const batch = new Set<Key>();
        for (const row of rows) {
            const key = rowKey(row, fields);
            if (key === undefined) {
                kept.push(row);
                pushed.push(undefined);
                continue;
            }
            if (this.seen.has(key) || batch.has(key)) {
                // This run pushes one key twice: the key does not identify a row,
                // so nothing of this push is skipped either.
                this.off('the result key repeats in this run');
                return { kept: rows, skipped: [], pushed: [] };
            }
            batch.add(key);
            if (this.saved.has(key)) {
                skipped.push(key);
            } else {
                kept.push(row);
                pushed.push(key);
            }
        }
        return { kept, skipped, pushed };
    }

    /** Record a push that went through: `pushed` = keys of the rows it stored. */
    commit(skipped: Key[], pushed: Array<Key | undefined>): number {
        for (const key of skipped) {
            this.saved.delete(key);
            this.seen.add(key);
        }
        for (const key of pushed) if (key !== undefined) this.seen.add(key);
        const n = skipped.length;
        if (n > 0) {
            this.skippedTotal += n;
            if (!this.announced) {
                this.announced = true;
                console.log(
                    `[Scrapely] This run was resumed: results it saved before (same ${(this.fields ?? []).map((f) => `'${f}'`).join(' + ')}) `
                    + 'are not saved or charged again',
                );
            }
        }
        return n;
    }
}

let instance: ResumeDedupe | undefined;

export function getResumeDedupe(): ResumeDedupe {
    instance ??= new ResumeDedupe();
    return instance;
}

/** Tests: forget the per-process state. */
export function resetResumeDedupe(): void {
    instance = undefined;
}
