# Changelog

All notable changes to `ankusa` (the npm package) are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `decodeMessage(data: string | Uint8Array)`, the queue-message decoder: parses
  the v1 envelope, checks the body forms and integrity (`size_mismatch`,
  `integrity`, `tenant_mismatch`), and ignores unknown keys. Every failure is
  an `InvalidMessageError` with `retryable: false`, a `code`, and the
  offending `field`. The decoded `Message` also carries the inline bytes as a
  non-enumerable `body`.
- `idempotencyKey(messageOrHeaders, { includeReplay? })`: `source_id:dedupe_key`
  when a non-empty `dedupe_key` is set, else `id`, plus `#replay:<replay_id>`
  when `includeReplay` is set and the delivery is a replay. Accepts a decoded
  `Message` or the `HookHeaders` of an HTTP delivery.
- `HookHeaders` gains `dedupeKey` (from `x-ankusa-dedupe-key`) and `replayId`
  (from `x-ankusa-replay-id`); each is `null` when the header is absent or
  empty.
- Admin replay jobs: `createReplay(spec)`, `getReplay(id)`, `listReplays()`,
  and `updateReplay(id, patch)`, with the `Replay`, `ReplayList`, `ReplaySpec`,
  and `ReplayPatch` types. A `404` is `AdminRejectedError` with code
  `replay_not_found`; a `409 replay_finished` is `AdminRejectedError` too.

### Removed

- `replayDeadLetters()`, `ReplayFilter`, and `Replayed`: the server's
  `POST /v1/dlq/replay` is gone in favour of replay jobs.

### Changed

- The generated admin types (`src/admin/admin-schema.d.ts`) are regenerated
  from the server's current OpenAPI document: the replay routes replace
  `/v1/dlq/replay`, and `GET /v1/wal`, `GET /v1/rate-limits` and
  `/v1/tenants/{tenant}/rate-limit` are now included. `/v1/wal` describes the
  node's store (`next_seq`, `hooks`, `deliveries`, `disk_bytes`), and the
  mutated routes type a `503` (`NodeStoreUnavailable`,
  `{"error": "store_unavailable"}`).

## [0.3.0] - 2026-10-01

### Added

- `InvalidRouteIdError` (a `RoutesError`): `getRoute`, `replaceRoute`,
  `updateRoute`, and `deleteRoute` reject an id that is not a string, is
  empty, or is exactly `.` or `..` before making any request. A URL parser
  normalizes those away, so they used to address the collection endpoint and
  return the route list as if it were one route.

### Removed

- `HookHeaders.seq` and the `x-ankusa-seq` header. It was this node's WAL
  position, not a per-source sequence: in a fleet every node's log starts at
  1, so two nodes emit `1, 2, 3` for different hooks and any consumer ordering
  or deduping on it was wrong. Dedupe on `x-ankusa-id`.

### Fixed

- A `3xx` from the routes or admin listener is now
  `RoutesUnavailableError`/`AdminUnavailableError` (retryable), like a `5xx`,
  instead of `RoutesRejectedError`/`AdminRejectedError`. Redirects are not
  followed, so an unfollowed one means the caller never reached the listener;
  this matches the Python SDK.

## [0.2.4] - 2026-09-28

### Added

- `parseHeaders(headers)`, plus `HookHeaders`, `HeaderSource`, and
  `MissingHookIdError`, for receivers of Ankusa's HTTP sink: parses
  `x-ankusa-id`/`x-ankusa-source`/`x-ankusa-seq`/`x-ankusa-tenant`/
  `content-type` case-insensitively, raising `MissingHookIdError` when
  `x-ankusa-id` is absent or empty. Mirrors the Python SDK's `parse_headers`.
- `createClaimCheckClient({ timeoutMs })`: a per-request deadline in
  milliseconds (default `10_000`) covering connect through the last body
  byte; exceeding it rejects with `ClaimCheckUnavailableError`. Mirrors the
  Python client's `timeout=10.0`.
- `createRoutesClient({ baseUrl, headers?, timeoutMs?, fetch? })`: the
  route-management client (`routes.admin.port`) — `health`, `listRoutes`,
  `createRoute`, `getRoute`, `replaceRoute`, `updateRoute`, `deleteRoute`,
  `getIpRules`, `putIpRules`, `testRoute` — with a `RoutesError` hierarchy
  (`RouteNotFoundError`, `RoutesRejectedError`, `RoutesUnavailableError`).
- `createAdminClient({ baseUrl, headers?, timeoutMs?, fetch? })`: the operator
  client (`admin.port`) — `health`, `metrics`, `config`, `listDeadLetters`,
  `replayDeadLetters`, `listQuarantined` — with an `AdminError` hierarchy
  (`RoleNotEnabledError`, `AdminRejectedError`, `AdminUnavailableError`).

### Changed

- `redeem` accepts only a `200`: an unfollowed `3xx` (redirects are no longer
  followed), a `2xx` other than `200`, and any other non-`200` status now
  reject with `ClaimCheckUnavailableError`, matching the Python client.
- An empty `200` body is returned as empty bytes and verified against the
  expected sha256 instead of being rejected as a gateway error. An empty `4xx`
  body is reported as `ClaimRejectedError.body === ""` rather than
  `undefined`.
- `health()` requires exactly a `200`, like the Python client.

## [0.2.1] - 2026-09-28

### Changed

- Claim-check refs are now `urn:ankusa:claim:v1:<tenant>:<claim_id>`, where
  `claim_id` is a canonical uppercase ULID, redeemed via
  `GET /v1/claims/{tenant_id}/{claim_id}`. `parseClaimRef` returns
  `{ tenantId, claimId, path }`.
- `redeem(ref, sha256)` takes the expected digest (the queue message's
  `sha256` field, 64 lowercase hex chars) and verifies the fetched bytes
  against it; a malformed `sha256` raises `InvalidClaimRefError` before any
  request is made.

## [0.1.0] - 2026-09-27

### Added

- `createClaimCheckClient({ baseUrl, headers?, fetch? })`, the first client
  this package bundles: `.redeem(ref)` parses a claim-check ref, fetches its
  bytes from the gateway, and verifies them against the ref's own declared
  size and sha256 before returning them. `.health()` for a liveness probe.
  Generated from `priv/openapi/claim_check.v1.yaml` via `openapi-typescript`
  + `openapi-fetch`.
- `parseClaimRef`, exported standalone.
- A `ClaimCheckError` hierarchy (`InvalidClaimRefError`,
  `ClaimNotFoundError`, `ClaimRejectedError`, `ClaimIntegrityError`,
  `ClaimCheckUnavailableError`), each carrying a `retryable` boolean, so a
  consumer needs exactly one bit to route a failure to dead-letter or retry.

[Unreleased]: https://github.com/jamescarr/ankusa/compare/sdk-typescript-v0.3.0...HEAD
[0.3.0]: https://github.com/jamescarr/ankusa/compare/sdk-typescript-v0.2.4...sdk-typescript-v0.3.0
[0.2.4]: https://github.com/jamescarr/ankusa/compare/sdk-typescript-v0.2.1...sdk-typescript-v0.2.4
[0.2.1]: https://github.com/jamescarr/ankusa/compare/sdk-typescript-v0.1.0...sdk-typescript-v0.2.1
[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/sdk-typescript-v0.1.0
