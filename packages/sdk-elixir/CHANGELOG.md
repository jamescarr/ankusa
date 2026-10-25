# Changelog

All notable changes to the `ankusa_sdk` Hex package are documented here. Format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **Delivery metadata (CON-4).** `Webhook.Headers.received_at` (when the edge accepted the hook, unix milliseconds, from `x-ankusa-received-at`) and `Webhook.Headers.body_sha256` (the lowercase hex SHA-256 of the body sent, from `x-ankusa-body-sha256`): `nil` when the header is absent, empty or malformed, never an error. `Receiver` now sets `Hook.received_at` from the header (it was always `nil` over HTTP).

- **Wire-format evolution path (CON-2).** `Ankusa.SDK.Message.decode/1` accepts
  every `v` from `Message.min_supported_version/0` to
  `Message.max_supported_version/0` (1 and 2) into the one `Message`, with `v`
  as the wire said it: a `v: 2` message is a `v: 1` message plus additive
  fields, so the keys it does not know are ignored. `Message.instance` and
  `Message.region`: the name of the Ankusa instance that published the message
  and the region it is configured with (`nil` when null or absent).
  `Ankusa.SDK.LifecycleEvent.decode/1` decodes the CloudEvents lifecycle events
  of the instance's lifecycle sinks.

- `Ankusa.SDK.Hook.trusted?/1`: `true` only when the node verified the hook at
  ingest (`status: "ok"`) and did not flag it (`flagged: false`). Works on a
  `Hook`, a decoded `Message` or the parsed `Webhook.Headers`; a flagged,
  `failed` or `skipped` hook and one with no `verification` are untrusted.

### Fixed

- `Ankusa.SDK.Receiver` now sets `verification` on the `Hook` it hands to the
  handler (it was always `nil` over HTTP, so a hook accepted after a failed
  signature looked the same as a verified one).

## [0.6.0] - 2026-10-24

### Added

- `Ankusa.SDK.Message` carries `traceparent` and `tracestate`: the hook's W3C
  trace context from the message, verbatim (`nil` when none); a non-string value
  is `invalid_field`.

- `Ankusa.SDK.ClaimRef.parse/1` and `Ankusa.SDK.ClaimCheck.redeem/3` accept
  signed, expiring v2 refs
  (`urn:ankusa:claim:v2:<tenant>:<claim_id>:<exp_unix>:<signature>`): the
  parsed `path` carries `?exp=&sig=` and `redeem/3` requests it as is. The
  gateway's `400 bad_signature` and `410 ref_expired` are an
  `Ankusa.SDK.ClaimRejectedError`. The SDK never checks the signature or the
  expiry.
- `Ankusa.SDK.Webhook.receive_delivery/3`, the receiver-side decision on one
  HTTP-sink delivery (signature, then headers) without Plug. Options
  `:secrets`, `:allow_unsigned`, `:tolerance_seconds`, `:now`.
- `verification` on parsed headers, decoded messages and received deliveries: how Ankusa verified the hook at ingest (`{status, flagged}`, `null` from a node older than the field).
- `scope` on `AdminRejectedError` and `RoutesRejectedError`: the scope a `403 forbidden` says the credential lacks (control planes require a credential since Ankusa 0.6).
- `Ankusa.SDK.Admin.list_audit/2`: `GET /v1/audit`, the audit trail (newest first, paged by `cursor`/`limit`, filtered by `since`, `until`, `actor`, `action`, `tenant`).

### Changed

- **Breaking (SEC-10):** `Ankusa.SDK.Receiver` is signed by default. The sink
  signs every delivery unless configured `allow_unsigned: true`, so the plug
  now needs the sink's `:secret` and raises at `init/1` without one; pass
  `allow_unsigned: true` to accept unsigned deliveries (a configured `:secret`
  is never weakened by it). The signature is judged before `x-ankusa-id`: a
  request with neither is `401`, not `400`.

## [0.5.0] - 2026-10-14

### Added

- `Ankusa.SDK.Signature.verify/4` verifies the Standard Webhooks signature an
  HTTP sink with a `secret` adds, comparing with `:crypto.hash_equals/2`;
  failures are `Ankusa.SDK.InvalidSignatureError` (`code`, `field`, never
  retryable). `Ankusa.SDK.Receiver` takes `:secret` (one or a list) and
  `:tolerance_seconds`, and answers `401`
  `{"error":"invalid_signature","code":...}` to a delivery that doesn't
  verify.
- `Ankusa.SDK.Message.decode/1` now carries `dedupe_key`, `replay_id`,
  `idempotency_key` and `headers`, verifies an inline body's length and sha256,
  checks a claim's tenant against `tenant_id`, and rejects a claim that isn't a
  well-formed ref. `Ankusa.SDK.InvalidMessageError` gains `code` and `field`.
- `Ankusa.SDK.Idempotency.key/2`: the idempotency key for a message, parsed
  webhook headers, or a hook. It returns the key Ankusa shipped (the message's
  `idempotency_key`, the `x-ankusa-idempotency-key` header) and, for a message
  or delivery that predates it, computes `tenant:source_id:dedupe_key` (tenant
  `default` when there is none) when a provider event key is present, else the
  hook id; `include_replay: true` appends the replay id.
- `Ankusa.SDK.Hook` carries `dedupe_key`, `replay_id`, `idempotency_key` and
  `headers`; `Ankusa.SDK.Webhook.Headers` parses `x-ankusa-dedupe-key`,
  `x-ankusa-replay-id` and `x-ankusa-idempotency-key` and keeps every request
  header.
- `Ankusa.SDK.Admin.create_replay/2`, `get_replay/2`, `list_replays/1` and
  `update_replay/3` for the replay-job API (`POST/GET/PATCH /v1/replays`;
  kinds `dlq`, `archive`, `quarantine`).

### Changed

- `Ankusa.SDK.ClaimCheck.redeem/3` returns `ClaimCheckUnavailableError`
  (retryable) for a gateway `408` or `429` instead of `ClaimRejectedError`.

### Removed

- The admin client's `replay_dead_letters` function; `POST /v1/dlq/replay` is
  gone in favour of replay jobs.

## [0.3.0] - 2026-10-01

### Added

- `Ankusa.SDK.Receiver`, a `Plug` for `Ankusa.Sink.Http` deliveries: it reads
  the raw body, parses the `x-ankusa-*` headers, calls the configured
  `Ankusa.SDK.Handler`, and answers `202`/`503`/`400`/`413` the way the
  dispatcher's retry expects. Options: `:handler`, `:path`, `:max_body_bytes`.
- `Ankusa.SDK.Hook`, the one value a handler sees whichever transport delivered
  it, and `Ankusa.SDK.Handler`, the behaviour it implements.
- `Ankusa.SDK.Webhook.parse_headers/1` for receivers built on something other
  than the Plug, plus `Ankusa.SDK.MissingHookIdError` when `x-ankusa-id` is
  absent or empty.
- `Ankusa.SDK.Message`, the `Ankusa.Sink.Message` wire format shared by the
  queue sinks: `decode/1` with an exact reason for every malformed message, and
  `to_hook/2`, which redeems the claim check when the payload rode one.
  `Ankusa.SDK.InvalidMessageError` classifies undecodable bytes.
- `Ankusa.SDK.ClaimCheck` (`redeem/3`, `health/1`) with the end-to-end sha256
  check the gateway does not run, and `Ankusa.SDK.ClaimRef.parse/1` on its own.
  The error hierarchy carries `retryable` so a consumer needs one bit to pick
  dead-letter or retry: `InvalidClaimRefError`, `ClaimNotFoundError`,
  `ClaimRejectedError`, `ClaimIntegrityError`, `ClaimCheckUnavailableError`.
- `Ankusa.SDK.Routes`: `health`, `list_routes`, `create_route`, `get_route`,
  `replace_route`, `update_route`, `delete_route`, `get_ip_rules`,
  `put_ip_rules`, `test_route`, with `InvalidRouteIdError`,
  `RouteNotFoundError`, `RoutesRejectedError`, `RoutesUnavailableError`.
- `Ankusa.SDK.Admin`: `health`, `metrics`, `config`, `list_dead_letters`,
  `replay_dead_letters`, `list_quarantined`, with `RoleNotEnabledError`,
  `AdminRejectedError`, `AdminUnavailableError`.
- `Ankusa.SDK.Sources`: `verify_version`, `server_version`, `list_sources`,
  `get_source`, `create_source`, `update_source`, `delete_source`, plus
  `Ankusa.SDK.Sources.Spec` (the writable spec) and `Ankusa.SDK.Sources.Source`
  (a stored, redacted source).
- Every client takes `:headers`, `:timeout_ms` (default 10 s) and `:req_options`
  (`:finch`, `:connect_options`, `:pool_timeout`, `:plug`). One request per
  call, no retries, and a redirect is never followed: a `3xx` is an
  `*UnavailableError`. With a `:finch` pool, `timeout_ms` bounds the response
  only, since the pool owns its connect options.
- The language-neutral conformance runner
  (`test/conformance_test.exs`), passing every vector in `conformance/cases`.

[Unreleased]: https://github.com/jamescarr/ankusa/compare/sdk-elixir-v0.6.0...HEAD
[0.6.0]: https://github.com/jamescarr/ankusa/compare/sdk-elixir-v0.5.0...sdk-elixir-v0.6.0
[0.5.0]: https://github.com/jamescarr/ankusa/compare/sdk-elixir-v0.3.0...sdk-elixir-v0.5.0
[0.3.0]: https://github.com/jamescarr/ankusa/releases/tag/sdk-elixir-v0.3.0
