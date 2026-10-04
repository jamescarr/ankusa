# Changelog

All notable changes to `ankusa` (the Packagist package `jamescarr/ankusa`) are
documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `Message::$idempotencyKey` and `HookHeaders::$idempotencyKey`, the key Ankusa
  ships (the message's `idempotency_key`, the `x-ankusa-idempotency-key`
  header). `idempotencyKey()` on both returns it, and for a message or
  delivery that predates it computes `tenant:source_id:dedupe_key` (tenant
  `default` when there is none) when a dedupe key is set, else the id, plus
  `#replay:<replay_id>` with `includeReplay: true`.

## [0.3.0] - 2026-10-01

### Added

- `Ankusa\ClaimCheck\ClaimCheckClient`: `redeem(ref, sha256)` parses a
  claim-check ref, fetches its bytes from the gateway, and verifies them
  against the expected sha256 before returning them; `health()` for a liveness
  probe. `ParsedClaimRef::parse()` exposes the ref parsing on its own.
- The `Ankusa\ClaimCheck\ClaimCheckError` hierarchy
  (`InvalidClaimRefError`, `ClaimNotFoundError`, `ClaimRejectedError`,
  `ClaimIntegrityError`, `ClaimCheckUnavailableError`), each carrying
  `isRetryable()`, so a consumer needs exactly one bit to route a failure to
  dead-letter or retry.
- `Ankusa\Routes\RoutesClient`: the route-management listener's `health`,
  `listRoutes`, `createRoute`, `getRoute`, `replaceRoute`, `updateRoute`,
  `deleteRoute`, `getIpRules`, `putIpRules`, and `testRoute`, with a
  `RoutesError` hierarchy (`InvalidRouteIdError`, `RouteNotFoundError`,
  `RoutesRejectedError`, `RoutesUnavailableError`).
- `Ankusa\Admin\AdminClient`: the operator API's `health`, `metrics`, `config`,
  `listDeadLetters`, `replayDeadLetters`, and `listQuarantined`, with an
  `AdminError` hierarchy (`RoleNotEnabledError`, `AdminRejectedError`,
  `AdminUnavailableError`).
- `Ankusa\Sources\SourcesClient`: the tenant-scoped source API's
  `serverVersion`, `listSources`, `getSource`, `createSource`, `updateSource`,
  and `deleteSource`, plus `SourceSpec` (the writable spec) and `Source` (a
  stored, redacted source). `expectedVersion` latches the first API call to
  `GET /health["version"]`.
- `Ankusa\Webhook\HookHeaders::fromHeaders()` for receivers of Ankusa's HTTP
  sink: parses `x-ankusa-id`/`x-ankusa-source`/`x-ankusa-tenant`/`content-type`
  from a PSR-7 message or a plain header array, raising `MissingHookIdError`
  when `x-ankusa-id` is absent or empty.
- `Ankusa\AnkusaException`, implemented by every exception the SDK throws.
- Every client takes an optional PSR-18 `ClientInterface`, so a deployment can
  bring its own transport. Client headers ride on every request; a JSON body
  always goes out as `content-type: application/json`, replacing any
  caller-supplied `Content-Type` rather than duplicating it.

[Unreleased]: https://github.com/jamescarr/ankusa/compare/sdk-php-v0.3.0...HEAD
[0.3.0]: https://github.com/jamescarr/ankusa/releases/tag/sdk-php-v0.3.0
