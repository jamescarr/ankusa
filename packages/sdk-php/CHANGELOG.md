# Changelog

All notable changes to `ankusa` (the Packagist package `jamescarr/ankusa`) are
documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.6.0] - 2026-10-28

### Added

- `Message::$traceparent` and `Message::$tracestate`: the hook's W3C trace
  context from the message, verbatim (`null` when none); a non-string value is
  `invalid_field`.

- `scope` on `AdminRejectedError` and `RoutesRejectedError`: the scope a `403
  forbidden` says the credential lacks (control planes require a credential
  since Ankusa 0.6).
- `verification` on parsed headers, decoded messages and received deliveries:
  how Ankusa verified the hook at ingest (`{status, flagged}`, `null` from a
  node older than the field).
- `ParsedClaimRef::parse()` and `ClaimCheckClient::redeem()` accept signed,
  expiring v2 refs
  (`urn:ankusa:claim:v2:<tenant>:<claim_id>:<exp_unix>:<signature>`): the
  parsed `path` carries `?exp=&sig=` and `redeem()` requests it as is. The
  gateway's `400 bad_signature` and `410 ref_expired` are a
  `ClaimRejectedError`. The SDK never checks the signature or the expiry.
- `Ankusa\Webhook\Receiver::receive($headers, $body, $secrets, $allowUnsigned,
  $toleranceSeconds, $now)`, the receiver-side decision on one HTTP-sink
  delivery. The sink signs every delivery unless configured
  `allow_unsigned: true`, so a receiver is closed by default: with `$secrets`
  the delivery must verify (as `Signature::verify`); with none and no
  `$allowUnsigned` it throws `InvalidSignatureError` (`invalid_secret`). The
  signature is judged before the hook id. Returns a `ReceivedDelivery`
  (`headers`, `verified`).
- `AdminClient::listAudit()`: `GET /v1/audit`, the audit trail (newest first,
  paged by `cursor`/`limit`, filtered by `since`, `until`, `actor`, `action`,
  `tenant`).
- **Delivery metadata (CON-4).** `HookHeaders::$receivedAt` (when the edge accepted the hook, unix milliseconds, from `x-ankusa-received-at`) and `HookHeaders::$bodySha256` (the lowercase hex SHA-256 of the body sent, from `x-ankusa-body-sha256`): `null` when the header is absent, empty or malformed, never an error.

- **Wire-format evolution path (CON-2).** `Message::decode()` accepts every `v`
  from `Message::MIN_SUPPORTED_VERSION` to `Message::MAX_SUPPORTED_VERSION` (1
  and 2) into the one `Message`, with `$v` as the wire said it: a `v: 2` message
  is a `v: 1` message plus additive fields, so the keys it does not know are
  ignored. `Message::$instance` and `Message::$region`: the name of the Ankusa
  instance that published the message and the region it is configured with
  (`null` when null or absent). `LifecycleEvent::decode()` decodes the
  CloudEvents lifecycle events of the instance's lifecycle sinks.

- `Message::isTrusted()`, `HookHeaders::isTrusted()` and
  `Verification::isTrusted()`: `true` only when the node verified the hook at
  ingest (status `ok`) and did not flag it. A flagged, `failed` or `skipped`
  hook and one with no `verification` are untrusted.

### Changed

- Accepts `guzzlehttp/guzzle ^8.2` and `guzzlehttp/psr7 ^3.1` as well as
  `^7.9` and `^2.13`: an app resolves one Guzzle for everything, so the SDK
  installs next to either major. The test suite and PHPStan pass on both.

## [0.5.0] - 2026-10-14

### Added

- `Webhook\Signature::verify($headers, $body, $secrets, $toleranceSeconds = 300, $now = null)`
  verifies the Standard Webhooks signature an HTTP sink with a `secret` adds,
  comparing with `hash_equals`; failures throw
  `Webhook\InvalidSignatureError` (`$errorCode`, `$field`, never retryable).
- `Message::$idempotencyKey` and `HookHeaders::$idempotencyKey`, the key Ankusa
  ships (the message's `idempotency_key`, the `x-ankusa-idempotency-key`
  header). `idempotencyKey()` on both returns it, and for a message or
  delivery that predates it computes `tenant:source_id:dedupe_key` (tenant
  `default` when there is none) when a dedupe key is set, else the id, plus
  `#replay:<replay_id>` with `includeReplay: true`.
- `AdminClient::createReplay(array $spec)`, `getReplay(string $id)`,
  `listReplays()` and `updateReplay(string $id, array $patch)` for replay jobs
  (`kind` `dlq`, `archive` or `quarantine`).
- `Ankusa\Message\Message::decode(string $data)` and `InvalidMessageError`
  (`$errorCode`, `$field`, `isRetryable()` always false) for the v1 queue
  message (inline or claim body, `sha256`, `dedupeKey`, `replayId`,
  `headers`).
- `HookHeaders::$dedupeKey` and `$replayId`, from `x-ankusa-dedupe-key` and
  `x-ankusa-replay-id`.

### Changed

- `ClaimCheckClient::redeem()` throws `ClaimCheckUnavailableError`
  (retryable) for a gateway `408` or `429` instead of `ClaimRejectedError`.

### Removed

- `AdminClient::replayDeadLetters()`: `POST /v1/dlq/replay` no longer exists.

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

[Unreleased]: https://github.com/jamescarr/ankusa/compare/sdk-php-v0.6.0...HEAD
[0.6.0]: https://github.com/jamescarr/ankusa/compare/sdk-php-v0.5.0...sdk-php-v0.6.0
[0.5.0]: https://github.com/jamescarr/ankusa/compare/sdk-php-v0.3.0...sdk-php-v0.5.0
[0.3.0]: https://github.com/jamescarr/ankusa/releases/tag/sdk-php-v0.3.0
