# Changelog

All notable changes to the `io.github.jamescarr/ankusa-clj` Clojars artifact
are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `ankusa.sdk.claim-check`: `redeem` fetches a claim's bytes from the
  claim-check gateway and verifies them against the expected digest before
  returning them, and `health` is a liveness probe. `ankusa.sdk.claim-ref/parse`
  exposes the ref parsing on its own.
- `ankusa.sdk.routes`: the route-management client — `health`, `list-routes`,
  `create-route`, `get-route`, `replace-route`, `update-route`, `delete-route`,
  `get-ip-rules`, `put-ip-rules`, and `test-route`. Every call is one request, no
  retries, and a redirect is never followed: a `3xx` is a
  `RoutesUnavailableError`.
- `ankusa.sdk.admin`: the DLQ and quarantine client — `health`, `metrics`,
  `config`, `list-dead-letters`, `list-quarantined`, and the replay jobs
  `create-replay`, `get-replay`, `list-replays`, and `update-replay` (kinds
  `dlq`, `archive`, `quarantine`).
- `ankusa.sdk.sources`: the tenant-scoped source API — `list-sources`,
  `get-source`, `create-source`, `update-source`, `delete-source`, and
  `server-version`. A client built with `:expected-version` fetches `/health`,
  and raises `VersionMismatchError` on every call that disagrees; `verify-version`
  caches the fetched version so later calls skip the probe.
- `ankusa.sdk.message`: `decode`, the queue-message decoder. It validates the v1
  message (including the shipped `idempotency_key`), decodes and verifies the
  body, and raises `InvalidMessageError` (`:code`, `:field`) on a bad message.
  `->hook` turns a decoded message into the hook a handler takes, redeeming the
  claim when the body is by reference.
- `ankusa.sdk.idempotency/key`: the key a worker dedupes on. It is the key
  Ankusa shipped, or for a message or delivery that predates it
  `tenant:source_id:dedupe_key` (tenant `default` when there is none) when a
  dedupe key is set, else `id`. `{:include-replay true}` appends
  `#replay:<replay-id>`.
- `ankusa.sdk.webhook/parse-headers` for receivers: the `x-ankusa-id`,
  `x-ankusa-source`, `x-ankusa-tenant`, `content-type`, `x-ankusa-dedupe-key`,
  `x-ankusa-replay-id`, and `x-ankusa-idempotency-key` headers of a delivery,
  from Ring's header map or a seq of pairs, and `MissingHookIdError` when
  `x-ankusa-id` is absent.
- `ankusa.sdk.errors`: every failure is an `ex-info` whose `ex-data` has a
  `:ankusa.sdk/...` `:type` and that type's keys, always present. `error-type`
  names it and `retryable?` is the dead-letter-or-retry bit, true only on the
  four `*UnavailableError` types. Caller misuse is an
  `IllegalArgumentException` instead, raised before any request.
- Client options `:headers`, `:timeout-ms` (10 s by default, bounding connect
  through the last body byte), and `:transport`. The default transport is the
  JDK `HttpClient`; inject your own to test, or to drive a different stack.
  Header values print as a placeholder, so a client never leaks a credential
  through `pr`, `pprint`, or a log line.
- The language-neutral conformance runner (`clojure.test`,
  `ankusa.sdk.conformance-test`), passing every vector in `conformance/cases`.
