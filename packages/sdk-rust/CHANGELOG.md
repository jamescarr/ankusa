# Changelog

All notable changes to `ankusa` (the crates.io crate) are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **Delivery metadata (CON-4).** `HookHeaders::received_at` (when the edge accepted the hook, unix milliseconds, from `x-ankusa-received-at`) and `HookHeaders::body_sha256` (the lowercase hex SHA-256 of the body sent, from `x-ankusa-body-sha256`): `None` when the header is absent, empty or malformed, never an error. `HookHeaders` gained two public fields.

- **Wire-format evolution path (CON-2).** `decode_message` accepts every `v`
  from `MIN_SUPPORTED_VERSION` to `MAX_SUPPORTED_VERSION` (1 and 2, both
  exported) into the one `Message`, with `v` as the wire said it: a `v: 2`
  message is a `v: 1` message plus additive fields, so the keys it does not know
  are ignored. `Message::instance` and `Message::region`: the name of the Ankusa
  instance that published the message and the region it is configured with
  (`None` when null or absent). `decode_lifecycle` and `LifecycleEvent` decode
  the CloudEvents lifecycle events of the instance's lifecycle sinks.

- `Message::is_trusted`, `HookHeaders::is_trusted` and `Verification::is_trusted`:
  `true` only when the node verified the hook at ingest (status `ok`) and did
  not flag it. A flagged, `failed` or `skipped` hook and one with no
  verification are untrusted.

## [0.6.0] - 2026-10-24

### Added

- `Message::traceparent` and `Message::tracestate`: the hook's W3C trace
  context from the message, verbatim (`None` when none); a non-string value is
  `invalid_field`.

- `scope` on `AdminRejectedError` and `RoutesRejectedError`: the scope a
  `403 forbidden` says the credential lacks (control planes require a
  credential since Ankusa 0.6).
- `verification` on parsed headers, decoded messages and received deliveries:
  how Ankusa verified the hook at ingest (`{status, flagged}`, `null` from a
  node older than the field).
- `parse_claim_ref` and `ClaimCheckClient::redeem` accept signed, expiring v2
  refs (`urn:ankusa:claim:v2:<tenant>:<claim_id>:<exp_unix>:<signature>`):
  `ParsedClaimRef::path()` ends in `?exp=&sig=` and `redeem` requests it as
  is. The gateway's `400 bad_signature` and `410 ref_expired` are a
  `ClaimRejected` error. The SDK never checks the signature or the expiry.
- `receive_delivery(headers, body, &ReceiveOptions)`, the receiver-side
  decision on one HTTP-sink delivery, with `ReceiveOptions` (`secrets`,
  `allow_unsigned`, `tolerance_seconds`, `now`), `ReceivedDelivery` and
  `ReceiveError`. The sink signs every delivery unless configured
  `allow_unsigned: true`, so a receiver is closed by default: with `secrets`
  the delivery must verify (as `verify_signature`); with none and no
  `allow_unsigned` it returns `InvalidSignatureError` (`invalid_secret`). The
  signature is judged before the hook id.
- `AdminClient::list_audit`: `GET /v1/audit`, the audit trail (newest first,
  paged by `cursor`/`limit`, filtered by `since`, `until`, `actor`, `action`,
  `tenant`).

## [0.5.0] - 2026-10-14

### Added

- `verify_signature(headers, body, secrets, VerifyOptions)` verifies the
  Standard Webhooks signature an HTTP sink with a `secret` adds, comparing in
  constant time (`hmac`); failures are `InvalidSignatureError` (`code`,
  `field`, never retryable). New dependency: `hmac`.
- `decode_message` and `Message`, for consumers of Ankusa's queue: the inline
  (`body_base64`) and claim (`claim`) body forms, `sha256`/size/tenant
  validation, forwarded `headers`, the shipped `idempotency_key` field, and
  `Message::idempotency_key(include_replay)`, which returns it (computing
  `tenant:source_id:dedupe_key`, tenant `default` when none, else `id`, only for
  a message that predates the field).
- `HookHeaders` gains `dedupe_key`, `replay_id`, `idempotency_key` (the shipped
  `x-ankusa-idempotency-key`) and `idempotency_key(include_replay)`.
- `AdminClient` gains `create_replay`, `get_replay`, `list_replays`, and
  `update_replay`, with the `Replay`, `ReplayList`, `ReplaySpec`, and
  `ReplayPatch` models. Kinds are `dlq`, `archive` and `quarantine`;
  `ReplaySpec`'s `id`/`since`/`until` apply to `quarantine` too.

### Changed

- `ClaimCheckClient::redeem` returns `ClaimCheckError::Unavailable`
  (retryable) for a gateway `408` or `429` instead of `Rejected`.

### Removed

- `AdminClient::replay_dead_letters` and its `ReplayFilter`/`Replayed` models,
  replaced by the replay-job API above.

## [0.3.0] - 2026-10-01

### Added

- `ClaimCheckClient`: `redeem(claim_ref, sha256)` parses a claim-check ref,
  fetches its bytes from the gateway, and verifies them against the expected
  sha256 before returning them; `health()` for a liveness probe.
- `parse_claim_ref`, returning the tenant, the claim id, and the
  `/v1/claims/{tenant_id}/{claim_id}` path.
- `ClaimCheckError` (`InvalidRef`, `InvalidSha256`, `NotFound`, `Rejected`,
  `Integrity`, `Unavailable`), each answering `is_retryable()`.
- `parse_headers`, for receivers of Ankusa's HTTP sink: parses
  `x-ankusa-id`/`x-ankusa-source`/`x-ankusa-tenant`/`content-type` into
  `HookHeaders`, raising `MissingHookIdError` if `x-ankusa-id` is absent.
- `RoutesClient` for the route-management listener: `health`, `list_routes`,
  `create_route`, `get_route`, `replace_route`, `update_route`,
  `delete_route`, `get_ip_rules`, `put_ip_rules`, and `test_route`, plus the
  `Route`, `RouteInput`, `RoutePatch`, `RoutePage`, `IpRule`, `IpRules`,
  `DryRunRequest`, `DryRunResult`, and `RoutesHealth` models, and a
  `RoutesError` hierarchy (`InvalidRouteId`, `NotFound`, `Rejected`,
  `Unavailable`).
- `AdminClient` for the operator listener: `health`, `metrics`, `config`,
  `list_dead_letters`, `replay_dead_letters`, and `list_quarantined`, plus the
  `AdminHealth`, `DlqEntry`, `DlqPage`, `QuarantineEntry`, `QuarantinePage`,
  `ReplayFilter`, and `Replayed` models, and an `AdminError` hierarchy
  (`RoleNotEnabled`, `Rejected`, `Unavailable`).
- `Transport`, the injectable request hook (`ReqwestTransport` by default, so
  the crate drives its own HTTP client or none at all), and `ClientBuilder`
  for shared base URL, headers, and timeout.

[Unreleased]: https://github.com/jamescarr/ankusa/compare/sdk-rust-v0.6.0...HEAD
[0.6.0]: https://github.com/jamescarr/ankusa/compare/sdk-rust-v0.5.0...sdk-rust-v0.6.0
[0.5.0]: https://github.com/jamescarr/ankusa/compare/sdk-rust-v0.3.0...sdk-rust-v0.5.0
[0.3.0]: https://github.com/jamescarr/ankusa/releases/tag/sdk-rust-v0.3.0
