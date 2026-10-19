# Scrapely Node.js SDK

Build JavaScript and TypeScript Actors for the [Scrapely](https://scrape.ly) platform. The SDK gives you the Actor lifecycle, input and output, datasets, key-value stores and request queues, Crawlee crawlers, remote browsers over CDP, platform events, run control, webhooks and pay-per-event charging.

## Install

```bash
npm install scrapely
```

## Quick start

```ts
import { Actor } from 'scrapely';

await Actor.main(async () => {
    const input = await Actor.getInputOrThrow<{ url: string }>();
    await Actor.pushData({ url: input.url, status: 'processed' });
});
```

`Actor.main()` initializes the Actor, awaits your function, closes events and storage, and exits the process. Always `await` writes so they reach the platform before the process ends.

## Crawling

```ts
import { Actor, PlaywrightCrawler } from 'scrapely';

await Actor.main(async () => {
    const { startUrls } = await Actor.getInputOrThrow<{ startUrls: string[] }>();

    const crawler = new PlaywrightCrawler({
        maxConcurrency: 5,
        async requestHandler({ request, page }) {
            await Actor.pushData({ url: request.url, title: await page.title() });
        },
    });

    await crawler.run(startUrls);
});
```

On the platform, browser crawlers connect to a remote Chromium over CDP instead of launching a local browser. Locally, they launch a normal browser unless you set `CDP_URL` or call `setCdpUrl()`.

## What's included

- **Actor lifecycle:** `Actor.main()`, `init()`, `exit()`, `fail()`, status messages, reboot, metamorph and abort
- **Input and output:** `getInput()`, `getInputOrThrow()`, `pushData()`, `getValue()`, `setValue()`
- **Storage:** `Dataset`, `KeyValueStore` and `RequestQueue`, backed by the Scrapely API on the platform and by local files during development
- **Crawlers:** `PlaywrightCrawler`, `PuppeteerCrawler`, `CheerioCrawler`, `HttpCrawler`, `BasicCrawler`, `BrowserCrawler`, `JSDOMCrawler` and `LinkeDOMCrawler`
- **Remote browsers:** automatic CDP wiring through `CDP_URL`, or `setCdpUrl()`
- **Events:** state persistence, migration and graceful shutdown
- **Run control:** `Actor.call()`, `Actor.callTask()`, `Actor.start()` and ad-hoc webhooks
- **Charging:** `Actor.charge()`, `ChargingManager` and `pushDataAndCharge()` for pay-per-event Actors
- **Logging and utilities:** `log`, `utils` and `enqueueLinks`

## Related packages

- [`scrapely-client`](https://www.npmjs.com/package/scrapely-client): JavaScript API client
- [`scrapely-cli`](https://www.npmjs.com/package/scrapely-cli): create, run and push Actors from your terminal
- [`scrapely-python`](https://pypi.org/project/scrapely-python/): the Python SDK

## License

ISC (see [LICENSE](./LICENSE)), except for a few files that stay under the Apache License 2.0
(see [LICENSE-APACHE](./LICENSE-APACHE) and [NOTICE](./NOTICE)).
