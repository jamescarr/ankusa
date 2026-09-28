# Changelog

All notable changes to `ankusa` (the npm package) are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

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

[Unreleased]: https://github.com/jamescarr/ankusa/compare/sdk-typescript-v0.2.1...HEAD
[0.2.1]: https://github.com/jamescarr/ankusa/compare/sdk-typescript-v0.1.0...sdk-typescript-v0.2.1
[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/sdk-typescript-v0.1.0
