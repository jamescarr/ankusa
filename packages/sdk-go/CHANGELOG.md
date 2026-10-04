# Changelog

All notable changes to the Go module
`github.com/jamescarr/ankusa/packages/sdk-go` are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

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
  with the `Replay`, `ReplayList`, `ReplaySpec`, and `ReplayPatch` types.

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

[Unreleased]: https://github.com/jamescarr/ankusa/compare/sdk-go-v0.3.0...HEAD
[0.3.0]: https://github.com/jamescarr/ankusa/releases/tag/sdk-go-v0.3.0
