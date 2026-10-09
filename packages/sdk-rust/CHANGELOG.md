# Changelog

All notable changes to `ankusa` (the crates.io crate) are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.4.0] - 2026-10-08

### Added

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

[Unreleased]: https://github.com/jamescarr/ankusa/compare/sdk-rust-v0.4.0...HEAD
[0.4.0]: https://github.com/jamescarr/ankusa/compare/sdk-rust-v0.3.0...sdk-rust-v0.4.0
[0.3.0]: https://github.com/jamescarr/ankusa/releases/tag/sdk-rust-v0.3.0
