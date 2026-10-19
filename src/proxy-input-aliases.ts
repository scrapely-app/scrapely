/**
 * Proxy input objects with both namings. An input-schema proxy field may be
 * written with the Scrapely names (`useScrapelyProxy`, `scrapelyProxyGroups`,
 * `scrapelyProxyCountry`, `scrapelyProxySubdivision`) or the legacy names
 * (`useApifyProxy`, `apifyProxy*`). Actor.getInput() and Actor.input return
 * every set option under both names, so actor code that reads either naming
 * sees the user's choice.
 *
 * Precedence matches createProxyConfiguration(): a boolean `useScrapelyProxy`
 * wins over `useApifyProxy`, and a set `scrapelyProxy*` value wins over its
 * `apifyProxy*` counterpart. Nothing is removed: keys with unset or
 * non-boolean values stay as sent.
 */

const OPTION_NAMES = ['Groups', 'Country', 'Subdivision'] as const;
const SWITCH_KEYS = ['useScrapelyProxy', 'useApifyProxy'] as const;

function isPlainObject(value: unknown): value is Record<string, unknown> {
    return !!value && typeof value === 'object' && !Array.isArray(value);
}

function isSet(value: unknown): boolean {
    return value !== undefined && value !== null;
}

function isProxyObject(value: Record<string, unknown>): boolean {
    return SWITCH_KEYS.some((k) => Object.hasOwn(value, k))
        || OPTION_NAMES.some((n) => Object.hasOwn(value, `scrapelyProxy${n}`) || Object.hasOwn(value, `apifyProxy${n}`));
}

function aliasProxyObject(value: Record<string, unknown>): Record<string, unknown> {
    const result: Record<string, unknown> = { ...value };
    const use = SWITCH_KEYS.map((k) => value[k]).find((v): v is boolean => typeof v === 'boolean');
    if (use !== undefined) {
        for (const k of SWITCH_KEYS) result[k] = use;
    }
    for (const name of OPTION_NAMES) {
        const scrapely = `scrapelyProxy${name}`;
        const apify = `apifyProxy${name}`;
        const option = isSet(value[scrapely]) ? value[scrapely] : value[apify];
        if (isSet(option)) {
            result[scrapely] = option;
            result[apify] = option;
        }
    }
    return result;
}

function sameObject(a: Record<string, unknown>, b: Record<string, unknown>): boolean {
    const keys = Object.keys(a);
    return keys.length === Object.keys(b).length && keys.every((k) => Object.hasOwn(b, k) && a[k] === b[k]);
}

/** `input` with every top-level proxy object under both namings; `input` itself when nothing changes. Never mutates. */
export function withProxyKeyAliases<T>(input: T): T {
    if (!isPlainObject(input)) return input;
    let result: Record<string, unknown> | undefined;
    for (const [key, value] of Object.entries(input)) {
        if (!isPlainObject(value) || !isProxyObject(value)) continue;
        const aliased = aliasProxyObject(value);
        if (sameObject(aliased, value)) continue;
        result ??= { ...input };
        result[key] = aliased;
    }
    return (result ?? input) as T;
}
