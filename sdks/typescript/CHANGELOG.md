# Changelog

All notable changes to `ankusa` (the npm package) are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

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

[Unreleased]: https://github.com/jamescarr/ankusa/compare/sdk-typescript-v0.1.0...HEAD
[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/sdk-typescript-v0.1.0
