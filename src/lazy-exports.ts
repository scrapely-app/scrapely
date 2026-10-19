/**
 * Public SDK names that load heavy packages: Crawlee's crawlers (jsdom,
 * linkedom, cheerio, the browser pool, playwright) and scrapely-client.
 * Importing the SDK used to load all of them, ~4 s at 1 core on the platform;
 * they now load on first access.
 *
 * - CommonJS (`require('scrapely').PlaywrightCrawler`): a getter on the
 *   package exports that requires the package once, then becomes a plain value.
 * - ESM (`import { PlaywrightCrawler } from 'scrapely'` / 'apify'): index.mts
 *   exports a lazy stand-in per name (see lazyExport). Node reads every
 *   detected export of a CommonJS module when ESM imports it, so these getters
 *   are deliberately defined in a form cjs-module-lexer does not detect.
 *
 * Loaded by the SDK entry: keep this file free of heavy imports.
 */
import { scrapelyClient } from './client-loader.js';

/** name → [package, export]. */
export const LAZY_EXPORTS: Readonly<Record<string, readonly [string, string]>> = {
    PlaywrightCrawler: ['@crawlee/playwright', 'PlaywrightCrawler'],
    PuppeteerCrawler: ['@crawlee/puppeteer', 'PuppeteerCrawler'],
    CheerioCrawler: ['@crawlee/cheerio', 'CheerioCrawler'],
    HttpCrawler: ['@crawlee/http', 'HttpCrawler'],
    BasicCrawler: ['@crawlee/basic', 'BasicCrawler'],
    BrowserCrawler: ['@crawlee/browser', 'BrowserCrawler'],
    JSDOMCrawler: ['@crawlee/jsdom', 'JSDOMCrawler'],
    LinkeDOMCrawler: ['@crawlee/linkedom', 'LinkeDOMCrawler'],
    AdaptivePlaywrightCrawler: ['@crawlee/playwright', 'AdaptivePlaywrightCrawler'],
    RenderingTypePredictor: ['@crawlee/playwright', 'RenderingTypePredictor'],
    SessionPool: ['@crawlee/core', 'SessionPool'],
    Session: ['@crawlee/core', 'Session'],
    AutoscaledPool: ['@crawlee/core', 'AutoscaledPool'],
    createBasicRouter: ['@crawlee/basic', 'createBasicRouter'],
    createHttpRouter: ['@crawlee/http', 'createHttpRouter'],
    createCheerioRouter: ['@crawlee/cheerio', 'createCheerioRouter'],
    createJSDOMRouter: ['@crawlee/jsdom', 'createJSDOMRouter'],
    createLinkeDOMRouter: ['@crawlee/linkedom', 'createLinkeDOMRouter'],
    createPlaywrightRouter: ['@crawlee/playwright', 'createPlaywrightRouter'],
    createAdaptivePlaywrightRouter: ['@crawlee/playwright', 'createAdaptivePlaywrightRouter'],
    createPuppeteerRouter: ['@crawlee/puppeteer', 'createPuppeteerRouter'],
    playwrightUtils: ['@crawlee/playwright', 'playwrightUtils'],
    puppeteerUtils: ['@crawlee/puppeteer', 'puppeteerUtils'],
    ApifyClient: ['scrapely-client', 'ApifyClient'],
    USES_PUSH_DATA_INTERCEPTION: ['scrapely-client', 'USES_PUSH_DATA_INTERCEPTION'],
    pushDataChargingContext: ['scrapely-client', 'pushDataChargingContext'],
};

/** Resolve one lazy export (loads its package on first call). */
export function loadLazyExport(name: string): unknown {
    const entry = LAZY_EXPORTS[name];
    if (!entry) throw new Error(`Unknown lazy export: ${name}`);
    if (entry[0] === 'scrapely-client') return (scrapelyClient() as Record<string, unknown>)[entry[1]];
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    return require(entry[0])[entry[1]];
}

/** Define every lazy export as a self-replacing getter on a CommonJS exports object. */
export function defineLazyExports(target: Record<string, unknown>): void {
    for (const name of Object.keys(LAZY_EXPORTS)) {
        Object.defineProperty(target, name, {
            enumerable: true,
            configurable: true,
            get() {
                const value = loadLazyExport(name);
                Object.defineProperty(target, name, { value, enumerable: true, configurable: true, writable: false });
                return value;
            },
        });
    }
}

/**
 * ESM stand-in for a lazy export: a Proxy that loads the real value on first
 * use and forwards everything to it. For classes it is constructible and
 * extendable (`new X()`, `class Y extends X`, `instanceof X`, statics); for
 * objects (playwrightUtils, ...) property access works as usual. The one
 * visible difference: the stand-in is not `===` the package's own export.
 */
export function lazyExport<T>(name: string, kind: 'class' | 'object'): T {
    let loaded = false;
    let real: any;
    const load = (): any => {
        if (!loaded) {
            real = loadLazyExport(name);
            loaded = true;
        }
        return real;
    };
    const boundMethods = new Map<Function, Function>();
    const bound = (fn: Function, self: unknown): Function => {
        let b = boundMethods.get(fn);
        if (!b) {
            b = fn.bind(self) as Function;
            boundMethods.set(fn, b);
        }
        return b;
    };
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    const target: any = kind === 'class' ? function LazyExport() {} : {};
    const proxy: any = new Proxy(target, {
        get(_t, prop, receiver) {
            const value = load();
            const result = Reflect.get(value, prop, receiver === proxy ? value : receiver);
            // Object stand-ins: methods run on the real object (private fields,
            // internal slots), e.g. pushDataChargingContext.run().
            if (kind === 'object' && typeof result === 'function') return bound(result, value);
            return result;
        },
        set(_t, prop, newValue, receiver) {
            const value = load();
            return Reflect.set(value, prop, newValue, receiver === proxy ? value : receiver);
        },
        has: (_t, prop) => Reflect.has(load(), prop),
        ownKeys: () => {
            const keys = Reflect.ownKeys(load());
            if (kind === 'class' && !keys.includes('prototype')) keys.push('prototype');
            return keys;
        },
        getOwnPropertyDescriptor(_t, prop) {
            const desc = Reflect.getOwnPropertyDescriptor(load(), prop);
            if (!desc) return undefined;
            // Stay compatible with the stand-in target (Proxy invariants): only
            // the target function's own `prototype` may be non-configurable.
            if (kind === 'class' && prop === 'prototype') return { ...desc, writable: true, configurable: false };
            return { ...desc, configurable: true };
        },
        defineProperty: (_t, prop, desc) => Reflect.defineProperty(load(), prop, desc),
        deleteProperty: (_t, prop) => Reflect.deleteProperty(load(), prop),
        getPrototypeOf: () => Reflect.getPrototypeOf(load()),
        construct(_t, args, newTarget) {
            const value = load();
            return Reflect.construct(value, args, newTarget === proxy ? value : newTarget);
        },
        apply: (_t, thisArg, args) => Reflect.apply(load(), thisArg, args),
    });
    return proxy as T;
}
