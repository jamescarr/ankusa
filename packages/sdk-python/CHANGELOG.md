# Changelog

All notable changes to `ankusa` (the PyPI package) are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.4.0] - 2026-10-08

### Added

- `message.Message`, `message.decode_message()`, and `message.InvalidMessageError`
  (with `code`, `field` and `retryable=False`): decode and validate the v1
  queue message every sink delivers — the inline (`body_base64`) and
  claim-check (`claim`) body forms, `sha256`, `dedupe_key`, `replay_id`, and
  the forwarded `headers`, ignoring unknown keys.
- `message.idempotency_key()`, the key to store in a processed-ids table: the
  key Ankusa shipped (the message's `idempotency_key`, or the
  `x-ankusa-idempotency-key` header), and for a message or delivery that
  predates the field `tenant:source_id:dedupe_key` (tenant `default` when there
  is none) when `dedupe_key` is set, else `id`, with an optional
  `#replay:<replay_id>` suffix (`include_replay=True`).
- `Message.idempotency_key`, decoded like `replay_id`, and
  `HookHeaders.dedupe_key` / `HookHeaders.replay_id` /
  `HookHeaders.idempotency_key`, parsed from `x-ankusa-dedupe-key` /
  `x-ankusa-replay-id` / `x-ankusa-idempotency-key`.
- `AdminClient.create_replay()`, `get_replay()`, `list_replays()` and
  `update_replay()` for the `POST/GET/PATCH /v1/replays` replay-job API (kinds
  `dlq`, `archive`, `quarantine`).

### Removed

- `AdminClient.replay_dead_letters()`, replaced by the replay-job API above.
  `POST /v1/dlq/replay` no longer exists.

## [0.3.0] - 2026-10-01

### Added

- `routes.InvalidRouteIdError` (a `RoutesError`): a route id that isn't a
  string, is empty, or is exactly `.` or `..` is refused before any request is
  sent. A URL parser normalizes those away — `..` becomes `/admin/`, an empty
  id or `.` becomes the collection endpoint — so `get_route("..")` used to hand
  back the list page as if it were a route. Exported from `ankusa.routes` and
  from the package root.
- `sources.SourcesClient`, a client for `Ankusa.Admin.Router` (port 4002): the
  tenant-scoped source API's `server_version`, `list_sources`, `get_source`,
  `create_source`, `update_source`, and `delete_source`. Built on `httpx`, with
  an injectable `transport` for tests.
- `sources.SourceSpec` (the writable spec, `to_json()` omits unset fields) and
  `sources.Source` (a stored, redacted source).
- A `sources.SourcesError` hierarchy (`SourceNotFoundError`,
  `SourceConflictError`, `SourceStoreReadOnlyError`, `SourceInvalidError`,
  `SourcesUnavailableError`, `VersionMismatchError`), each carrying `.status`
  and `.body`. `expected_version` makes the first API call verify
  `GET /health ["version"]` once and cache it.
- Tenants and source names are checked against `^[A-Za-z0-9_-]{1,64}$` before
  any path is built, so a caller-supplied name cannot escape its tenant through
  URL normalization.

### Removed

- `HookHeaders.seq` and the `x-ankusa-seq` header. It was this node's WAL
  position, not a per-source sequence: in a fleet every node's log starts at
  1, so two nodes emit `1, 2, 3` for different hooks and any consumer ordering
  or deduping on it was wrong. Dedupe on `x-ankusa-id`.

### Fixed

- Route ids are percent-encoded as a single path segment, so `/`, `?`, `#`, `%`
  and space in an id travel as `%2F %3F %23 %25 %20` instead of reshaping the
  request URL.

## [0.2.4] - 2026-09-28

### Added

- `RoutesClient(base_url, headers=None, timeout=10.0)`: the route-management
  client (`routes.admin.port`) — `health`, `list_routes`, `create_route`,
  `get_route`, `replace_route`, `update_route`, `delete_route`, `get_ip_rules`,
  `put_ip_rules`, `test_route` — with a `RoutesError` hierarchy
  (`RouteNotFoundError`, `RoutesRejectedError`, `RoutesUnavailableError`).
- `AdminClient(base_url, headers=None, timeout=10.0)`: the operator client
  (`admin.port`) — `health`, `metrics`, `config`, `list_dead_letters`,
  `replay_dead_letters`, `list_quarantined` — with an `AdminError` hierarchy
  (`RoleNotEnabledError`, `AdminRejectedError`, `AdminUnavailableError`).

### Fixed

- `parse_headers` no longer raises `ValueError` for an `x-ankusa-seq` value
  that `str.isdigit()` accepts but `int()` rejects (e.g. `²`); a sequence
  number now requires all-ASCII digits and is `None` otherwise.

## [0.2.1] - 2026-09-28

### Changed

- Claim-check refs are now `urn:ankusa:claim:v1:<tenant>:<claim_id>`, where
  `claim_id` is a canonical uppercase ULID; `parse_claim_ref` returns
  `ParsedClaimRef(tenant_id, claim_id, path)` and `redeem` fetches
  `GET /v1/claims/{tenant_id}/{claim_id}`.
- `ClaimCheckClient.redeem(ref, sha256)` takes the expected sha256 (the queue
  message's `sha256` field) and verifies the returned bytes against it. A
  malformed `sha256` raises `InvalidClaimRefError` before any request.

## [0.1.0] - 2026-09-27

### Added

- `ClaimCheckClient(base_url, headers=None, timeout=10.0)`, the first client
  this package bundles: `.redeem(ref)` parses a claim-check ref, fetches its
  bytes from the gateway, and verifies them against the ref's own declared
  size and sha256 before returning them. `.health()` for a liveness probe.
  Built on [`httpx`](https://www.python-httpx.org/) against the contract in
  `priv/openapi/claim_check.v1.yaml`.
- `parse_claim_ref`, exported standalone.
- A `ClaimCheckError` hierarchy (`InvalidClaimRefError`,
  `ClaimNotFoundError`, `ClaimRejectedError`, `ClaimIntegrityError`,
  `ClaimCheckUnavailableError`), each carrying a `retryable` boolean, so a
  consumer needs exactly one bit to route a failure to dead-letter or retry.
- `parse_headers(headers)`, for receivers of Ankusa's HTTP sink: parses
  `x-ankusa-id`/`x-ankusa-source`/`x-ankusa-seq`/`x-ankusa-tenant`/
  `content-type` into a `HookHeaders` dataclass, raising
  `MissingHookIdError` if `x-ankusa-id` is absent.

[Unreleased]: https://github.com/jamescarr/ankusa/compare/sdk-python-v0.4.0...HEAD
[0.4.0]: https://github.com/jamescarr/ankusa/compare/sdk-python-v0.3.0...sdk-python-v0.4.0
[0.3.0]: https://github.com/jamescarr/ankusa/compare/sdk-python-v0.2.4...sdk-python-v0.3.0
[0.2.4]: https://github.com/jamescarr/ankusa/compare/sdk-python-v0.2.1...sdk-python-v0.2.4
[0.2.1]: https://github.com/jamescarr/ankusa/releases/tag/sdk-python-v0.2.1
[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/sdk-python-v0.1.0
