# Changelog

All notable changes to the `ankusa_sdk` Hex package are documented here. Format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

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

[Unreleased]: https://github.com/jamescarr/ankusa/compare/sdk-elixir-v0.3.0...HEAD
[0.3.0]: https://github.com/jamescarr/ankusa/releases/tag/sdk-elixir-v0.3.0
