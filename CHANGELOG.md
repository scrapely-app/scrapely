# Changelog

All notable changes to this project will be documented in this file.

## [1.2.40] - 2026-10-30

- Actors on puppeteer 25 (ESM-only) are sent to the remote browser when puppeteer is loaded with `require()` before the SDK on a run without the preload. The SDK only recognised puppeteer's old `lib/cjs` build, so such a `puppeteer.launch()` tried to start a local browser; the named `launch` export (`const { launch } = require('puppeteer')`) is covered too. puppeteer 24 and older behave as before.

## [1.2.39] - 2026-10-29

- A refused charge names the limit that refused it. When a charge is refused or cut short, the platform now says why (HTTP 402 `error.data.reason`, and `limitedBy` on a partial acceptance): the run's item limit (`maxItems`), its maximum total charge (`maxTotalChargeUsd`) or the account balance. The SDK used to call all of them the spending limit. Now its log lines name the real limit; `ChargingManager.getChargeLimitReason()` returns `'max-items'`, `'max-charge'` or `'insufficient-funds'` and `getChargeLimitMessage()` the stop message ("Stopped: reached the item limit you set (800 items)", "Stopped: reached the maximum charge you set ($3.00)", "Stopped: your account has no credit left for this run"); and once a limit was reached, a status message (`Actor.setStatusMessage`, `Actor.exit`, `Actor.fail`) whose clause blames the maximum charge gets that clause replaced by the real stop message. Platforms that do not send the reason yet: inferred from the 402 message and `ACTOR_MAX_PAID_DATASET_ITEMS`. What is charged and saved does not change.

## [1.2.38] - 2026-10-27

- A key-value store record write (`Actor.setValue`, `KeyValueStore.setValue`) is no longer lost to one failed answer. Each write now sends an `Idempotency-Key`, and when its answer is lost (a `502`/`504` page from the edge or load balancer, a dropped connection, a timeout) it is sent again with the same key; the API applies and counts it once. Before, the write was never re-sent, so one such page on the final `OUTPUT` write failed the whole run. An API that answers a keyed write without `X-Scrapely-Idempotency` (an older server) gets no re-sends, as before.

## [1.2.37] - 2026-10-26

- Opening a named dataset, key-value store or request queue (`Actor.openDataset('name')` and the like) retries when the API answers `503 storage-name-busy`. The API sends it when another request kept creating or renaming a storage of that name for over 15 s; nothing was changed, so the open is simply repeated. It used to fail the open.
- Once `Actor.fail()` has run, the run stays failed: a later `Actor.exit()`, or the end of `Actor.main()`, keeps the failure's exit code and status message. `fail()` normally ends the process at once, so this only changed runs that called `fail({ exit: false })` and then `exit()` or returned from `Actor.main()`; they used to exit with 0. A second `fail()` keeps the first failure. `fail({ exitCode: undefined })` exits with 1.

## [1.2.36] - 2026-10-22

- Debug captures (`Actor.debugCapture`, `Actor.debugCaptureSync`) keep payloads over 256 KiB whole. They are stored gzip-compressed and base64-encoded as `{ key, payload, encoding: 'gzip-base64', length, truncated }` instead of being cut at 262,144 characters; up to 8 Mi characters are kept. A payload that does not compress enough keeps its first 256 KiB, now marked `truncated: true` with its full `length`. `Uint8Array` / `Buffer` payloads are stored as UTF-8 text. Captures are only active on the platform's own diagnostic runs (`SCRAPELY_DEBUG_CAPTURE=1`).

## [1.2.35] - 2026-10-20

- The storage client percent-encodes request ids and key-value store record keys as one URL path segment and refuses `''`, `.` and `..`. A request id such as `..` read from a queue used to send the call to another resource.

## [1.2.34] - 2026-10-19

- Initial public release.
