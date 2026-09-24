# Changelog

All notable changes to `ankusa` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

Release process: bump `version` in the package's `mix.exs` and move the
relevant `[Unreleased]` entries under a new dated heading in the same PR, in
accordance with SemVer. A pushed `<pkg>-vX.Y.Z` git tag publishes — see
[`docs/releasing.md`](docs/releasing.md) for the mechanics.

## [Unreleased]

### Added

- Admin API (`Ankusa.Admin.Router`), started when `admin.enabled` is true
  (default `false`, port `4002`): `GET /health`, `GET /metrics`,
  `GET /v1/config` (redacted), `GET /v1/dlq`, `POST /v1/dlq/replay`,
  `GET /v1/quarantine`. Unauthenticated by design — front it with your own
  proxy or network policy. Contract:
  [`priv/openapi/admin.v1.yaml`](https://github.com/jamescarr/ankusa/blob/main/priv/openapi/admin.v1.yaml).
- `Ankusa.Metrics`: a per-instance Prometheus reporter behind `/metrics`,
  covering ingest, verification, WAL commit, dedup, load shedding,
  quarantine, dispatch, compaction, and claim check. New dependencies:
  `telemetry_metrics` and `telemetry_metrics_prometheus_core`.
- `:instance` in the metadata of `[:ankusa, :dispatch, :stop]`,
  `[:ankusa, :dispatch, :dlq]`, `[:ankusa, :claim_check, :check_in]`, and
  `[:ankusa, :claim_check, :redeem]`.
- `Ankusa.Verifier.Hmac`: a configurable HMAC signature engine driven by a
  `%Ankusa.Verifier.Hmac.Scheme{}` descriptor, with named presets in
  `Ankusa.Verifier.Schemes` for Stripe, GitHub, Standard Webhooks, Shopify,
  and Slack, and an inline `type: hmac` YAML surface for any other body-HMAC
  provider. The `shopify` and `slack` `verify.type` values are new.
- `scheme_name/1` optional callback on `Ankusa.Verifier`, a `scheme` field on
  `Ankusa.Verification`, and a `scheme` label on `ankusa.verify.failures.total`.
- `c:Ankusa.Sink.ordering_key/2`: optional sink callback naming the ordering
  scope of a delivery. Deliveries to the same sink with an equal key run one at
  a time in `seq` order, different keys run concurrently. `Sink.Http` takes
  `ordered: true` to opt in (off by default); `Sink.Kafka` uses its record key,
  `Sink.RabbitMQ` its routing key, `Sink.Log` none.

- `c:Ankusa.Sink.inline_max_bytes/1`: optional sink callback naming the
  largest body the sink sends inline. Dispatch checks a body in once, before
  any sink runs, and hands the claim reference to every sink and retry in
  `ctx.claim`.

### Removed

- `Ankusa.Verifier.Stripe`, `Ankusa.Verifier.GitHub`, and
  `Ankusa.Verifier.StandardWebhooks` — replaced by `Ankusa.Verifier.Hmac`
  presets. Elixir embedders use `{Ankusa.Verifier.Hmac, scheme: :stripe, …}`.
- Claim-check authentication: `claim_check.api_tokens` (and the YAML
  `claim_check.tokens`), tenant scopes, and the `401`/`403` responses. The
  gateway does no auth or authorization; put a proxy, mesh, or network policy
  in front of it.
- `Ankusa.ClaimCheck.Ticket`, `Ankusa.ClaimCheck.Direct`, and
  `Ankusa.ClaimCheck.Remote`, plus `claim_check.adapter`, `claim_check.remote`,
  and `claim_check.max_bytes`. Dispatch nodes write directly to the object
  store; a public write API and a credential-less write path are no longer
  supported.

### Changed

- The claim reference is now one URN string in a queue message's `claim`
  field — `urn:ankusa:claim:v1:<tenant>:<object_id>:<offset>:<length>:sha256-<hex>`
  — instead of a nested ticket object. Message `v` stays `1`. Consumers
  redeem it with `GET /v1/claims/:tenant/:object_id/:offset/:length` and check
  the sha256 themselves. Breaking for consumers of the old ticket object.
- Claims are **packed**: dispatch checks each WAL read batch's claims in per
  tenant as one uncompressed-ZIP object (plus a `manifest.json`), one `PUT`
  per tenant per batch, sized by `claim_check.pack_max_bytes` (default
  16 MiB). Previously each sink wrote each claim on every attempt.
- The default `inline_max_bytes` for the queue sinks is now 64 KiB (was
  8 KiB), and `Sink.Message.inline_max_bytes/1` is the single source of that
  default.
- Claim storage layout is now Hive-style: `claims/tenant=<t>/dt=<yyyy-mm-dd>/<object_id>`
  (was `claims/<percent-encoded tenant>/<id>`). Tenants are validated as
  `[A-Za-z0-9_-]{1,64}` at ingest and config time; a bad URL tenant is a `404`
  before anything is written.
- The claim-check gateway is read-only and cacheable: `GET` responses carry
  `cache-control: public, max-age=31536000, immutable`, and `416` is returned
  for a range past the end of an object. `PUT` is gone.
- `Ankusa.ClaimCheck.redeem/2` takes a `%Ankusa.ClaimCheck.Ref{}` or its URN
  string; `check_in/4` and `check_in_batch/2` replace the old per-ticket
  `check_in/4`.

- Dispatch is **concurrent**: up to `dispatch.concurrency` (default `32`) sink
  deliveries run at once, serialized per `c:Ankusa.Sink.ordering_key/2`, and the
  durable cursor is a watermark that never advances past an unfinished
  envelope. A slow or retrying sink no longer blocks the whole instance, and
  `dispatch.max_inflight` (4096) / `dispatch.max_inflight_bytes` (128 MiB) bound
  how much admitted-but-unfinished work it can hold. A sink that raises,
  throws, or exits is retried and dead-lettered instead of crashing the
  pipeline. `dispatch.batch` (128) now bounds one WAL read rather than one
  tick's delivery.
- `batcher.max_delay_ms` defaults to `0` and `batcher.partitions` to `2`: the
  WAL append runs in a task, so commits pipeline naturally and extra partitions
  only add contention. `max_queue` bounds buffered *and* in-flight records. A
  failed WAL append now replies `{:error, :store_unavailable}` (→ `503`) to
  every caller in the batch instead of crashing the batcher.
- `WAL.DiskLog` truncation is logical first: `truncate_through/2` records a
  durable seq floor (`<name>.truncated`) and drops index entries, and only
  rewrites the file once the dead prefix passes `:rewrite_min_bytes` (default
  64 MiB) and is as large as the live suffix. `.cursors`, `.dedup`, and
  `.truncated` are fsynced before their rename, and `read/3` walks the index
  with `:ets.next/2` instead of a full `:ets.select/3` scan per read.
- `Ankusa.Storage.Compactor` honours `storage.roll_bytes`: a backlog is written
  as several bounded segments per tick (256-record reads) instead of one
  unbounded segment, so memory stays flat after a storage outage.
- `storage.roll_ms` remains unimplemented; `storage.roll_bytes` is now the
  segment bound it always claimed to be.
- `claim_check.api_tokens` is optional. Empty (the default) leaves the
  claim-check gateway open, with authentication delegated to whatever fronts
  the port; a `:claim_check`-role node no longer fails to boot without
  tokens.
- `Ankusa.ClaimCheck.Remote`'s `:token` is optional; omit it when the gateway
  has no `api_tokens`.
- `Ankusa.Admin.Redact` also redacts `nkey_seed`, the private key a
  `Sink.NATS` deployment authenticates with, so `GET /v1/config` cannot print
  it.

### Fixed

- `WAL.DiskLog`: a restart after a full truncation resumed at `seq` 1 while the
  persisted cursors were far ahead, so every new hook was skipped by dispatch
  and then deleted by the next truncation. Seq allocation now continues from
  the truncation floor, the persisted cursors, and the last replayed frame.
- DLQ appends and storage-index appends are fsynced: a dispatch cursor could
  advance past a dead letter (or a compaction cursor past index rows) that a
  power loss then dropped, leaving the hook in neither place.
- `Ankusa.Dispatch` no longer copies the whole pipeline state into every
  delivery task, and `WAL.DiskLog` no longer scans its whole index per read —
  together those two were the dispatch throughput ceiling (see
  [`docs/testing.md`](docs/testing.md#core-bench--benchcore_benchexs)).

## [0.1.0] - 2026-09-23

### Added

- Core ingest pipeline: Bandit edge, group-commit batcher, durable WAL
  (`Ankusa.WAL.DiskLog`), idempotent receiver, dispatch pipeline, segment
  compactor.
- Pluggable behaviours: `Ankusa.RouteResolver`, `Ankusa.WAL`,
  `Ankusa.Verifier`, `Ankusa.DedupKey`, `Ankusa.SourceStore`, `Ankusa.Sink`,
  `Ankusa.RetryPolicy`, `Ankusa.BlobStore`, `Ankusa.Codec`.
- Verifiers: Standard Webhooks, Stripe, GitHub.
- Object store adapters: `BlobStore.LocalFS` (default), `BlobStore.S3`
  (AWS/MinIO/R2, SigV4 via `aws_signature`), `BlobStore.GCS`.
- Multi-tenant catch-URL routing (`RouteResolver.Path`,
  `RouteResolver.TenantPath`) with tenant-scoped dedup/storage.
- Quarantine (rate-limited durable pen for verification failures) and DLQ +
  replay for dispatch give-ups.
- Loss checker: proves every acked id survives a hard instance crash.
- `Ankusa.ClaimCheck` gateway: check bytes in, get a versioned ticket back;
  present the ticket, get the bytes back. `Ankusa.ClaimCheck.Direct` (calls
  the instance's configured `BlobStore` in-process) and
  `Ankusa.ClaimCheck.Remote` (HTTP client against a `:claim_check`-role
  node, for callers that must not hold blob-store credentials). New
  `:claim_check` role and `Ankusa.ClaimCheck.Router` HTTP API
  (`PUT`/`GET /v1/claims/:tenant_id/:id`), off by default, bearer-token
  authenticated and tenant-scoped. See
  [`docs/claim-check.md`](docs/claim-check.md).
- `Ankusa.Sink.Message`: the wire format every queue-style sink publishes
  (`Sink.RabbitMQ`, `Sink.Kafka`) — inline base64 up to `inline_max_bytes` or
  a claim ticket above it, with an additive `"v": 1` version field.
- `Sink.Http` adds `x-ankusa-tenant` to its identity headers when the
  envelope has a tenant id.
- `Ankusa.Config.parse_roles!/1` validates `ANKUSA_ROLES` against the fixed
  role set (`edge`, `dispatch`, `storage`, `claim_check`) and fails boot on
  an unknown name, instead of silently atomizing arbitrary input.
  `Ankusa.Config.new/1` rejects an unknown nested key (e.g. `batcher: %{max_queu:
  5}`) and correctly deep-merges a keyword-list section instead of
  replacing the whole defaults map.
- HTTP is `Req` throughout — the S3 and GCS blob stores, the claim-check
  `Remote` adapter, and `Sink.Http` no longer hand-roll `:httpc` plumbing
  (and `:inets` is no longer started by this package). Outbound requests go
  through `Ankusa.HttpClient`, which never follows a redirect (a followed one
  re-sends a hook as a `GET`) and takes `:req_options` from an allowlist:
  transport tuning only, so a caller cannot rewrite a URL that has already been
  signed.
- Ankusa.Application's built-in default instance is opt-in: `autostart`
  defaults to `false`, so depending on `ankusa` never binds a port as a side
  effect.

[Unreleased]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.1.0...HEAD
[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/ankusa-v0.1.0
