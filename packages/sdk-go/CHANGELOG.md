# Changelog

All notable changes to the Go module
`github.com/jamescarr/ankusa/packages/sdk-go` are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `ListDeadLettersParams` gained `Tenant`, `Sink` (`*int`: `0` is the first sink),
  `ErrorClass` and `Cursor`; the query goes out as `source_id, tenant, sink,
  error_class, since, limit, cursor`. `DLQEntry` gained `Sink` and `ErrorClass`,
  `DLQPage` gained `NextCursor`, and `ReplaySpec` gained `Tenant`, `Sink`, `ErrorClass`
  and `JobID` (the DEL-6 admin contract).

### Fixed

- **Breaking:** `DLQPage.Total` is `*int`. A node answers `total: null` on a filtered
  listing (any filter but `Tenant`), which the `int` field silently reported as `0`.

## [0.6.0] - 2026-10-28

### Added

- `Message.Traceparent` and `Message.Tracestate`: the hook's W3C trace context
  from the message, verbatim (`nil` when none); a non-string value is
  `invalid_field`.

- `verification` on parsed headers, decoded messages and received deliveries:
  how Ankusa verified the hook at ingest (`{status, flagged}`, `null` from a
  node older than the field).
- `ParseClaimRef` and `ClaimCheckClient.Redeem` accept signed, expiring v2
  refs (`urn:ankusa:claim:v2:<tenant>:<claim_id>:<exp_unix>:<signature>`):
  `ParsedClaimRef.Path` ends in `?exp=&sig=` and `Redeem` requests it as is.
  The gateway's `400 bad_signature` and `410 ref_expired` are a
  `*ClaimRejectedError`. The SDK never checks the signature or the expiry.
- `ReceiveDelivery(header, body, ReceiveOptions{Secrets, AllowUnsigned,
  ToleranceSeconds, Now})`, the receiver-side decision on one HTTP-sink
  delivery. The sink signs every delivery unless configured
  `allow_unsigned: true`, so a receiver is closed by default: with `Secrets`
  the delivery must verify (as `VerifySignature`); with none and no
  `AllowUnsigned` it returns `*InvalidSignatureError` (`invalid_secret`). The
  signature is judged before the hook id. Returns a `ReceivedDelivery` (the
  `HookHeaders` plus `Verified`).
- `Scope` on the admin and routes rejected-error types: the scope a `403
  forbidden` says the credential lacks (control planes require a credential
  since Ankusa 0.6).
- `AdminClient.ListAudit`: `GET /v1/audit`, the audit trail (newest first,
  paged by `cursor`/`limit`, filtered by `since`, `until`, `actor`, `action`,
  `tenant`).
- **Delivery metadata (CON-4).** `HookHeaders.ReceivedAt` (when the edge accepted the hook, unix milliseconds, from `x-ankusa-received-at`) and `HookHeaders.BodySHA256` (the lowercase hex SHA-256 of the body sent, from `x-ankusa-body-sha256`): nil when the header is absent, empty or malformed, never an error.

- **Wire-format evolution path (CON-2).** `DecodeMessage` accepts every `v` from
  `MinSupportedVersion` to `MaxSupportedVersion` (1 and 2) into the one
  `Message`, with `V` as the wire said it: a `v: 2` message is a `v: 1` message
  plus additive fields, so the keys it does not know are ignored.
  `Message.Instance` and `Message.Region`: the name of the Ankusa instance that
  published the message and the region it is configured with (nil when null or
  absent). `DecodeLifecycle` and `LifecycleEvent` decode the CloudEvents
  lifecycle events of the instance's lifecycle sinks.

- `Trusted()` on `Message`, `HookHeaders` and `*Verification` (nil-safe): true
  only when the node verified the hook at ingest (status `"ok"`) and did not
  flag it. A flagged, `failed` or `skipped` hook and one with no verification
  are untrusted.

## [0.5.0] - 2026-10-14

### Added

- `VerifySignature(header, body, secrets, VerifyOptions)` verifies the
  Standard Webhooks signature an HTTP sink with a `secret` adds, comparing
  with `hmac.Equal`; failures are `*InvalidSignatureError` (`Code`, `Field`,
  not retryable).
- `DecodeMessage` and the `Message` type: decode a v1 queue message with the
  body-integrity checks (`size_mismatch`, `integrity`, `tenant_mismatch`) and
  the `dedupe_key`/`replay_id`/`idempotency_key`/`headers` fields, raising
  `InvalidMessageError` (`Code`, `Field`, `Retryable() == false`) on any
  malformed input. The wire's `idempotency_key` is `Message.ShippedIdempotencyKey`
  (the field name cannot be `IdempotencyKey`, which is the helper).
- `Message.IdempotencyKey(includeReplay)` and
  `HookHeaders.IdempotencyKey(includeReplay)`: the key a consumer dedupes a
  delivery on. It is the key Ankusa shipped (`idempotency_key` /
  `x-ankusa-idempotency-key`); for a message or delivery that predates it, the
  helper computes `tenant:source_id:dedupe_key` (tenant `default` when there is
  none) when a dedupe key is set, else the id.
- `HookHeaders.DedupeKey`, `HookHeaders.ReplayID`, and
  `HookHeaders.ShippedIdempotencyKey`, read from `x-ankusa-dedupe-key`,
  `x-ankusa-replay-id`, and `x-ankusa-idempotency-key` by `ParseHeaders`.
- `AdminClient.CreateReplay`, `GetReplay`, `ListReplays`, and `UpdateReplay`,
  with the `Replay`, `ReplayList`, `ReplaySpec`, and `ReplayPatch` types. Kinds
  are `dlq`, `archive` and `quarantine`; `ReplaySpec.ID`, `Since` and `Until`
  cover `quarantine` too.

### Changed

- `Redeem` returns `*ClaimCheckUnavailableError` (retryable) for a gateway
  `408` or `429` instead of `*ClaimRejectedError`.

### Removed

- `AdminClient.ReplayDeadLetters`, `ReplayFilter`, and `Replayed`; replay is
  now the replay-job API above.

## [0.3.0] - 2026-10-01

### Added

- `ParseClaimRef`, parsing `urn:ankusa:claim:v1:<tenant>:<claim_id>` into its
  tenant id, claim id, and `/v1/claims/{tenant_id}/{claim_id}` path.
- `NewClaimCheckClient`: `Redeem(ref, sha256)` fetches a claim's bytes from the
  claim-check gateway and verifies them against the expected digest before
  returning them; `Health()` is a liveness probe.
- `NewRoutesClient`: the route-management client (`routes.admin.port`) —
  `Health`, `ListRoutes`, `CreateRoute`, `GetRoute`, `ReplaceRoute`,
  `UpdateRoute`, `DeleteRoute`, `GetIPRules`, `PutIPRules`, `TestRoute`.
- `NewAdminClient`: the operator client (`admin.port`) — `Health`, `Metrics`,
  `Config`, `ListDeadLetters`, `ReplayDeadLetters`, `ListQuarantined`.
- `ParseHeaders`, the webhook helper: reads `x-ankusa-id`,
  `x-ankusa-source`, `x-ankusa-tenant`, and `content-type` off any
  `http.Header`, case-insensitively.
- A concrete `*XxxError` for every request outcome, each implementing the
  package's `Error` interface with a `Retryable()` bit: `InvalidClaimRefError`,
  `ClaimNotFoundError`, `ClaimRejectedError`, `ClaimIntegrityError`,
  `ClaimCheckUnavailableError`, `MissingHookIdError`, `RouteNotFoundError`,
  `InvalidRouteIdError`, `RoutesRejectedError`, `RoutesUnavailableError`,
  `RoleNotEnabledError`, `AdminRejectedError`, `AdminUnavailableError`.
- Caller misuse is a plain error instead, raised before any request: a base
  URL that is not an absolute http(s) URL, or a request body that cannot be
  encoded as JSON (an unencodable `Metadata` value in `CreateRoute`,
  `ReplaceRoute`, or `UpdateRoute`).

[Unreleased]: https://github.com/jamescarr/ankusa/compare/sdk-go-v0.6.0...HEAD
[0.6.0]: https://github.com/jamescarr/ankusa/compare/sdk-go-v0.5.0...sdk-go-v0.6.0
[0.5.0]: https://github.com/jamescarr/ankusa/compare/sdk-go-v0.3.0...sdk-go-v0.5.0
[0.3.0]: https://github.com/jamescarr/ankusa/releases/tag/sdk-go-v0.3.0
