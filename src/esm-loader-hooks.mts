/**
 * Off-thread ESM load hook (module.register) for Node versions without
 * module.registerHooks (Node 20, 22.0-22.14). Registered by
 * patch-browser-pool.ts only when puppeteer is installed for ESM imports.
 *
 * Appends one call to puppeteer-core's ESM PuppeteerNode.js that hands the
 * class to patchPuppeteerNodeClass (published on a global by the preload in
 * the main thread), before the package entry creates its default instance.
 * Keep the pattern and suffix in sync with PUPPETEER_NODE_ESM_URL_RE /
 * PUPPETEER_NODE_ESM_SUFFIX in patch-browser-pool.ts (a test checks this).
 */
export const PUPPETEER_NODE_ESM_URL_RE = /\/puppeteer-core\/lib\/(esm\/)?puppeteer\/node\/PuppeteerNode\.js$/;
export const PUPPETEER_NODE_ESM_SUFFIX =
    '\n;globalThis[Symbol.for("scrapely.patchPuppeteerNodeClass")]?.(PuppeteerNode);\n';

interface LoadResult {
    format?: string | null;
    source?: string | ArrayBuffer | ArrayBufferView | null;
    shortCircuit?: boolean;
}

export async function load(
    url: string,
    context: unknown,
    nextLoad: (url: string, context: unknown) => Promise<LoadResult>,
): Promise<LoadResult> {
    const result = await nextLoad(url, context);
    if (result?.format === 'module' && result.source != null && PUPPETEER_NODE_ESM_URL_RE.test(url)) {
        const source =
            typeof result.source === 'string' ? result.source : new TextDecoder().decode(result.source as ArrayBuffer);
        return { ...result, source: source + PUPPETEER_NODE_ESM_SUFFIX };
    }
    return result;
}
