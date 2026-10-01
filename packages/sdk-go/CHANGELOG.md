# Changelog

All notable changes to the Go module
`github.com/jamescarr/ankusa/packages/sdk-go` are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
