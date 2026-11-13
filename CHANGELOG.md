# Changelog

All notable changes to this project will be documented in this file.

## [1.2.44] - 2026-11-13

- Maintenance release.

## [1.2.43] - 2026-11-10

- **A resurrected or rebooted run stops at the Actor's own result limit.** A resumed run starts the Actor's code again, and with it the Actor's own result count, so an Actor whose source returns results in a different order each run (search pages, feeds) could end with more results than its limit: with `maxResults` 60, 24 rows saved before the abort plus the resumed run's own 60 (some of them saved before and skipped) gave 67. `Actor.setResultLimit(n)` declares the Actor's run-level result limit, e.g. its `maxItems` input. In a resumed run the rows saved before count towards it: once they and the new rows reach `n`, further charged rows are refused like at the spending limit (not saved, not charged, the push reports the limit), so the run ends with `n` results. Declare only a limit that counts all results of the run, not one per search. Nothing changes in a first run or for Actors that do not declare a limit.

## [1.2.42] - 2026-11-10

- **A resurrected or rebooted run no longer charges again for results it had already saved.** Such a run keeps its default dataset, but the Actor's code starts from the beginning, and an Actor without its own progress state collects the results it saved before once more. On the first charged push to the default dataset of such a run, the SDK reads the result key of the rows already saved (one read of the dataset's `id` values; `id` by default). A charged row whose key is one of them is not saved and not charged again: the dataset already holds it and it was paid for when it was first saved. It counts as accepted in the push result (`chargedCount`) and in `getChargedEventCount()`, so Actors that compare pushed and charged counts do not stop. The run's spending limit is not used by these rows.
  - `Actor.setResultKey('id' | ['id', 'checkIn'] | null)` names the field(s) that identify one result row; `null` turns this off. `SCRAPELY_RESUME_DEDUPE=off` turns it off for a run.
  - Nothing changes when the key does not identify a row: if the saved rows repeat a key, or the resumed run pushes one key twice (for example the same result from two different searches), every row is saved and charged as before. Nothing changes in a first run, a run that saved nothing yet, after a metamorph, for rows without the key, for named datasets or for pushes that are not charged per row. A first run reads only the dataset's item count once.

## [1.2.41] - 2026-11-08

- An atomic multi-event push (`Actor.pushData(rows, { charges })`) whose charge the limit covers only in part no longer logs "Charging 5 instances of 'audio-minute' event would exceed the end of the account balance - only 3 events were charged". A row is saved only when all of its events are charged, so the SDK gives those 3 back and nothing is charged: the line was untrue. It now says what happened: "Saving this item needs 5 'audio-minute' event(s), but the account balance covers only 3: it was not saved and nothing was charged for it." When some rows still fit, it names how many were saved and charged in full, and when giving a charge back is not confirmed it says so instead of "nothing was charged". A plain `Actor.charge()` that comes back short keeps its line: those events do stay charged.
- `ChargeResult.coveredCounts` (from `pushData(rows, { charges })`): for each event whose charge came back short, how many events the limit covered (given back, not charged), e.g. `{ 'audio-minute': 3 }`. Actors can cut a row to what the limit covers without reading the log. What is charged and saved does not change.
- A spending limit of $0 (`maxTotalChargeUsd: 0`) is a real limit, the same as on the platform. It used to be read as "no limit". A missing or non-numeric value still means no limit.
- A plain `Actor.pushData(items)` to the default dataset of a pay-per-event Actor whose dataset item event is priced goes through the charged push: rows past the spending limit are not saved, and it returns a `ChargeResult`. Before, all rows were saved and only the ones within the limit were paid.
- A dataset push that fails with a `503 push-outcome-unknown` (the server could not tell whether it was stored) keeps its charge instead of voiding it.
- `utils.enqueueLinks({ limit })`: enqueue at most `limit` links per page; links are added to the queue in batches of 100.
- Dragonfly storage clients: named datasets, key-value stores and request queues are looked up per owner (`name:{userId}:{name}`, as the server stores them), a storage is created only with a known owner, and keys created later (dataset items, records, the log truncation marker) take their storage's expiry. Actor runs on the platform use the API storage client and are not affected.

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
