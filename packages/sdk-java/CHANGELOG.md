# Changelog

All notable changes to the `io.github.jamescarr:ankusa-sdk` Maven Central
artifact are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `ClaimCheckClient`: `redeem(ref, sha256)` fetches a claim's bytes from the
  claim-check gateway and verifies them against the expected digest before
  returning them, and `health()` is a liveness probe.
- `RoutesClient`: the route-management client — `health`, `listRoutes`,
  `createRoute`, `getRoute`, `replaceRoute`, `updateRoute`, `deleteRoute`,
  `getIpRules`, `putIpRules`, and `testRoute`. Every call is one request, no
  retries, and a redirect is never followed: a `3xx` is a `RoutesUnavailableError`.
- `AdminClient`: the DLQ and quarantine client — `health`, `metrics`, `config`,
  `listDeadLetters`, `listQuarantined`, and the replay jobs `createReplay`,
  `getReplay`, `listReplays`, and `updateReplay`.
- `Message.decode`, the queue-message decoder: it validates the v1 message
  (including the shipped `idempotency_key`, the `idempotencyKey` record
  component), decodes and verifies the body, and raises `InvalidMessageError`
  (`code()`, `field()`) on a bad message. `Message.idempotencyKey(includeReplay)`
  and `HookHeaders.idempotencyKey(includeReplay)` return the key a worker dedupes
  on: the key Ankusa shipped, or for a message or delivery that predates it
  `tenant:source_id:dedupe_key` (tenant `default` when there is none) when a
  dedupe key is set, else `id`.
- `HookHeaders.parse` for receivers: the `x-ankusa-id`, `x-ankusa-source`,
  `x-ankusa-tenant`, `content-type`, `x-ankusa-dedupe-key`,
  `x-ankusa-replay-id`, and `x-ankusa-idempotency-key` headers of a delivery,
  and `MissingHookIdError` when `x-ankusa-id` is absent.
- `SourcesClient`: the tenant-scoped source API — `serverVersion`, `listSources`,
  `getSource`, `createSource`, `updateSource`, and `deleteSource`. A client built
  with an `expectedVersion` fetches `/health` once, caches the reported version,
  and raises `VersionMismatchError` on every later call that disagrees.
- `HookHeaders.parse` for receivers: the `x-ankusa-id`, `x-ankusa-source`,
  `x-ankusa-tenant`, and `content-type` headers of a delivery, and
  `MissingHookIdError` when `x-ankusa-id` is absent.
- The error hierarchy: one abstract `AnkusaException` per client family
  (`ClaimCheckError`, `RoutesError`, `AdminError`, `SourcesError`), with
  `retryable()` true only on the four `*UnavailableError` classes. Caller misuse
  is an `IllegalArgumentException` instead, raised before any request: a base URL
  that is not an absolute http(s) URL, a header the JDK client refuses to set, or
  a body that cannot be encoded as JSON.
- `ClientOptions`, carrying the request headers, the timeout (10 s by default,
  bounding connect through the last body byte), and the `Transport`. The default
  transport is the JDK `HttpClient`; inject your own to test, or to drive a
  different stack.
- The language-neutral conformance runner (JUnit Jupiter, `ConformanceTest`), passing
  every vector in `conformance/cases`.
