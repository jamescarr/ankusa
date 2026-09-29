# Changelog

All notable changes to `ankusa` (the PyPI package) are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.3.0] - 2026-09-28

### Added

- `admin.AdminClient`, a client for `Ankusa.Admin.Router` (port 4002): the
  tenant-scoped source API's `server_version`, `list_sources`, `get_source`,
  `create_source`, and `update_source`. Built on `httpx`, with an injectable
  `transport` for tests.
- `admin.SourceSpec` (the writable spec, `to_json()` omits unset fields) and
  `admin.Source` (a stored, redacted source).
- An `admin.AdminError` hierarchy (`SourceNotFoundError`,
  `SourceConflictError`, `SourceStoreReadOnlyError`, `SourceInvalidError`,
  `AdminUnavailableError`, `VersionMismatchError`), each carrying `.status`
  and `.body`. `expected_version` makes the first API call verify
  `GET /health ["version"]` once and cache it.

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

[Unreleased]: https://github.com/jamescarr/ankusa/compare/sdk-python-v0.3.0...HEAD
[0.3.0]: https://github.com/jamescarr/ankusa/releases/tag/sdk-python-v0.3.0
[0.2.1]: https://github.com/jamescarr/ankusa/releases/tag/sdk-python-v0.2.1
[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/sdk-python-v0.1.0
