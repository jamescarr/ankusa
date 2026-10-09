# Changelog

All notable changes to `ankusa-sdk` (the RubyGems gem) are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.4.0] - 2026-10-08

### Added

- `Ankusa::Message` and `Ankusa.decode_message(data)`, plus
  `Ankusa::InvalidMessageError` (with `code`, `field` and `retryable?` false):
  decode and validate the v1 queue message every sink delivers — the inline
  (`body_base64`) and claim-check (`claim`) body forms, `sha256`,
  `dedupe_key`, `replay_id`, and the forwarded `headers`, ignoring unknown
  keys.
- `Message#idempotency_key` / `HookHeaders#idempotency_key`, the key to store
  in a processed-ids table: the key Ankusa shipped (the message's
  `idempotency_key` field, or the `x-ankusa-idempotency-key` header), and for a
  message or delivery that predates it `tenant:source_id:dedupe_key` (tenant
  `default` when there is none) when a `dedupe_key` is set, else `id`, with an
  optional `#replay:<replay_id>` suffix (`include_replay: true`). The decoded
  field stays in `to_h[:idempotency_key]`.
- `HookHeaders#dedupe_key` / `#replay_id` / `#idempotency_key`'s field, parsed
  from `x-ankusa-dedupe-key` / `x-ankusa-replay-id` / `x-ankusa-idempotency-key`.
- `AdminClient#create_replay`, `#get_replay`, `#list_replays` and
  `#update_replay` for the `POST/GET/PATCH /v1/replays` replay-job API (kinds
  `dlq`, `archive`, `quarantine`).

### Removed

- `AdminClient#replay_dead_letters`, replaced by the replay-job API above.
  `POST /v1/dlq/replay` no longer exists.

## [0.3.0] - 2026-10-01

### Added

- `Ankusa::ClaimCheckClient`: `redeem(ref, sha256)` parses a claim-check ref,
  fetches its bytes from the gateway, and verifies them against the message's
  sha256 before returning them; `health` for a liveness probe.
- `Ankusa::RoutesClient`: the route-management client
  (`routes.admin.port`) — `health`, `list_routes`, `create_route`, `get_route`,
  `replace_route`, `update_route`, `delete_route`, `get_ip_rules`,
  `put_ip_rules`, `test_route`.
- `Ankusa::AdminClient`: the operator client (`admin.port`) — `health`,
  `metrics`, `config`, `list_dead_letters`, `replay_dead_letters`,
  `list_quarantined`.
- `Ankusa::SourcesClient`, a client for `Ankusa.Admin.Router`: the tenant-scoped
  source API's `server_version`, `list_sources`, `get_source`, `create_source`,
  `update_source`, and `delete_source`, with `Ankusa::SourceSpec` (the writable
  spec, `to_request_body` omits unset fields) and `Ankusa::Source` (a stored,
  redacted source).
- `Ankusa.parse_headers(headers)`, for receivers of Ankusa's HTTP sink: parses
  `x-ankusa-id`/`x-ankusa-source`/`x-ankusa-tenant`/`content-type` into a
  `HookHeaders` value, raising `MissingHookIdError` if `x-ankusa-id` is absent
  or empty. `Ankusa.parse_claim_ref` is exported the same way.
- `Ankusa::Transport`, the injectable transport hook (`Transport::Request` in,
  `Transport::Response` out) with the default `Transport::NetHTTP`: one
  connection per request, no redirects, no retries.
- A `ClaimCheckError`/`RoutesError`/`AdminError`/`SourcesError` hierarchy under
  `Ankusa::Error`, every class carrying a `retryable?` bit (except the sources
  family, which carries `.status` and `.body` instead), so a consumer needs
  exactly one bit to route a failure to dead-letter or retry.

[Unreleased]: https://github.com/jamescarr/ankusa/compare/sdk-ruby-v0.4.0...HEAD
[0.4.0]: https://github.com/jamescarr/ankusa/compare/sdk-ruby-v0.3.0...sdk-ruby-v0.4.0
[0.3.0]: https://github.com/jamescarr/ankusa/releases/tag/sdk-ruby-v0.3.0
