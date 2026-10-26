# Changelog

All notable changes to this project will be documented in this file.

## [1.2.37] - 2026-10-26

- Opening a named dataset, key-value store or request queue (`Actor.openDataset('name')` and the like) retries when the API answers `503 storage-name-busy`. The API sends it when another request kept creating or renaming a storage of that name for over 15 s; nothing was changed, so the open is simply repeated. It used to fail the open.
- Once `Actor.fail()` has run, the run stays failed: a later `Actor.exit()`, or the end of `Actor.main()`, keeps the failure's exit code and status message. `fail()` normally ends the process at once, so this only changed runs that called `fail({ exit: false })` and then `exit()` or returned from `Actor.main()`; they used to exit with 0. A second `fail()` keeps the first failure. `fail({ exitCode: undefined })` exits with 1.

## [1.2.36] - 2026-10-22

- Debug captures (`Actor.debugCapture`, `Actor.debugCaptureSync`) keep payloads over 256 KiB whole. They are stored gzip-compressed and base64-encoded as `{ key, payload, encoding: 'gzip-base64', length, truncated }` instead of being cut at 262,144 characters; up to 8 Mi characters are kept. A payload that does not compress enough keeps its first 256 KiB, now marked `truncated: true` with its full `length`. `Uint8Array` / `Buffer` payloads are stored as UTF-8 text. Captures are only active on the platform's own diagnostic runs (`SCRAPELY_DEBUG_CAPTURE=1`).

## [1.2.35] - 2026-10-20

- The storage client percent-encodes request ids and key-value store record keys as one URL path segment and refuses `''`, `.` and `..`. A request id such as `..` read from a queue used to send the call to another resource.

## [1.2.34] - 2026-10-19

- Initial public release.
