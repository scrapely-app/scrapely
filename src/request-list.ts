/**
 * ScrapelyRequestList - parses the standard Scrapely `requestListSources` input format.
 *
 * Parity port of the Python SDK's `scrapely.request_loaders.ScrapelyRequestList`.
 * It supports two kinds of entries:
 *
 * - **Direct URLs** - entries with a `url` key are converted to requests directly.
 * - **Remote URL lists** - entries with a `requestsFromUrl` key point to a remote resource
 *   (e.g. a plain-text file). The resource is fetched and all URLs found in the response
 *   body are extracted and converted to requests.
 *
 * Both kinds of entries can optionally specify `method`, `payload`, `headers`, and
 * `userData` fields that will be applied to every request created from that entry.
 *
 * Usage:
 *   ```javascript
 *   import { Actor, ScrapelyRequestList } from 'scrapely';
 *
 *   const input = await Actor.getInput() ?? {};
 *   const requestList = await ScrapelyRequestList.open(input.requestListSources ?? []);
 *   // requestList.requests -> crawlee Request[]
 *   ```
 */

import { Request } from '@crawlee/core';

/** Regex for extracting URLs from remote text files (matches URLs without commas). */
const URL_NO_COMMAS_REGEX =
    /https?:\/\/(www\.)?([^\W_]|[^\W_][-\w0-9@:%._+~#=]{0,254}[^\W_])\.[a-z]{2,63}(:\d{1,5})?(\/[-\w@:%+.~#?&/=()]*)?/g;

/**
 * A single entry of the `requestListSources` input format.
 * Accepts both camelCase (Scrapely UI) and snake_case keys.
 */
export interface RequestListSource {
    /** Direct URL to crawl (mutually exclusive with `requestsFromUrl`). */
    url?: string;
    /** Remote resource whose body is scanned for URLs to crawl. */
    requestsFromUrl?: string;
    requests_from_url?: string;
    /** HTTP method applied to all requests from this entry (default: GET). */
    method?: string;
    /** HTTP payload applied to all requests from this entry. */
    payload?: string;
    /** HTTP headers applied to all requests from this entry. */
    headers?: Record<string, string>;
    /** Custom user data applied to all requests from this entry. */
    userData?: Record<string, any>;
    user_data?: Record<string, any>;
}

function createRequest(source: RequestListSource, url: string): Request {
    return new Request({
        url,
        method: source.method as any,
        payload: source.payload,
        headers: source.headers,
        // Deep-copy so Request's constructor (which writes __crawlee into the
        // dict) cannot corrupt the shared input, and nested values are not
        // aliased across requests (Python SDK parity: deepcopy).
        userData: structuredClone(source.userData ?? source.user_data ?? {}),
    });
}

async function fetchRequestsFromUrl(source: RequestListSource): Promise<Request[]> {
    const remoteUrl = source.requestsFromUrl ?? source.requests_from_url;
    if (!remoteUrl) return [];

    const response = await fetch(remoteUrl);
    if (!response.ok) {
        throw new Error(`Failed to fetch requestsFromUrl "${remoteUrl}": ${response.status} ${response.statusText}`);
    }
    const body = await response.text();

    const requests: Request[] = [];
    for (const match of body.matchAll(URL_NO_COMMAS_REGEX)) {
        requests.push(createRequest(source, match[0]));
    }
    return requests;
}

export class ScrapelyRequestList {
    /** Resolved requests, ready to be added to a RequestQueue or crawler. */
    readonly requests: Request[];

    private constructor(requests: Request[]) {
        this.requests = requests;
    }

    /**
     * Create a new `ScrapelyRequestList` from the standard Scrapely request list sources input.
     *
     * @param requestListSourcesInput List of entries with either a `url` key (direct URL) or a
     * `requestsFromUrl` key (remote resource scanned for URLs). When not provided or empty,
     * an empty request list is returned.
     */
    static async open(requestListSourcesInput?: RequestListSource[]): Promise<ScrapelyRequestList> {
        const sources = requestListSourcesInput ?? [];

        const simpleUrlSources = sources.filter((source) => source.url);
        const remoteUrlSources = sources.filter((source) => !source.url && (source.requestsFromUrl ?? source.requests_from_url));

        const simpleUrlRequests = simpleUrlSources.map((source) => createRequest(source, source.url!));
        const remoteUrlRequests = (await Promise.all(remoteUrlSources.map(fetchRequestsFromUrl))).flat();

        return new ScrapelyRequestList([...simpleUrlRequests, ...remoteUrlRequests]);
    }
}
