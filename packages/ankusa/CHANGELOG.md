# Changelog

All notable changes to `ankusa` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

Release process: bump `version` in the package's `mix.exs` and move the
relevant `[Unreleased]` entries under a new dated heading in the same PR, in
accordance with SemVer. A pushed `<pkg>-vX.Y.Z` git tag publishes. See
[`docs/releasing.md`](../../docs/releasing.md) for the mechanics.

## [Unreleased]

### Added

- `wal: :none`, the stateless ack path. Ingest verifies, then publishes to the
  source's sinks in the request (`Ankusa.Edge.Publish`) and answers `201` only
  once every sink has confirmed; a refusal is a `503` with `Retry-After` and the
  provider retries. No WAL, no batcher, no dispatch pipeline, no compactor, no
  DLQ: the node runs only the `edge` role (the WAL's reader roles are dropped
  from `config.roles` rather than rejected). `Ankusa.WAL.validate_config!/1`
  refuses a `wal: :none` config in which a statically configured source has no
  sink whose `:ok` means durable — sources created at runtime through the admin
  API are not checked.
- `c:Ankusa.Sink.durable?/1`, the per-sink promise `wal: :none` acks on; defaults
  to `true`, and `Ankusa.Sink.Log` answers `false`. `Ankusa.Sink.safe_deliver/4`
  (the raise/throw/exit-to-`{:error, reason}` wrapper hidden inside the dispatch
  pipeline) is now public, so both ack paths deliver through the same function.
- `GET /v1/wal` on the admin API: this node's `Ankusa.WAL.stats/1`, or
  `409 wal_disabled` under `wal: :none`. `Ankusa.WAL.label/1` names the
  configured adapter for boot banners and `check-config`.

### Changed

- **Breaking: the ingest `201` body is `{"status": "accepted", "id": "…"}`.** The
  `seq` field is gone: it was this node's WAL position, not a per-source
  sequence, so two nodes in a fleet both emit `1, 2, 3` and any consumer
  ordering or deduping on it was wrong. Dedupe on the envelope `id`.
- **Breaking: ingest `GET /stats` is removed** and `GET /health` is liveness
  only (`{"status": "ok", "instance": "…"}`). Per-node WAL stats are operator
  surface, not provider surface: they moved to `GET /v1/wal` on the admin port.
- **Breaking: `Ankusa.Sink.Http` no longer sends `x-ankusa-seq`.** The delivery
  contract is `x-ankusa-id`, `x-ankusa-source`, and `x-ankusa-tenant` when set.
  (The published `ankusa` npm and PyPI SDKs drop `HookHeaders.seq` in the same
  release; their changelogs say so.)
- **Breaking for custom route stores.** `Ankusa.Routes.Store`'s `insert` and
  `replace` callbacks take the version of the snapshot the caller validated
  against — `insert(instance, route, version)`, `replace(instance, route,
  version)` — and answer `{:error, :stale}`, having changed nothing, when the
  table has moved on. `Ankusa.Routes` re-reads, re-validates and retries; a
  write that keeps losing answers `503 store_unavailable`, safe to retry.
  `delete/2` and `put_ip_rules/2` are unchanged. `ankusa_redis` has to be
  released with this: an older `Ankusa.Routes.Store.Redis` does not implement the
  new arities.
- `PUT /admin/ip-rules` requires both `default` and `rules`. An omitted `default`
  used to mean `allow`, which quietly turned a deny-by-default list into an open
  one; it is now `400 invalid_ip_rules` naming the missing field.
- Client address: `X-Forwarded-For` from a trusted proxy is walked right to
  left. The first entry outside `routes.trusted_proxies` is the client, entries
  to its left are never examined, and an entry that cannot be read before that
  point denies the request. Every `x-forwarded-for` line is joined in order, and
  `1.2.3.4:5678`, `[::1]` and `[::1]:443` are accepted forms.
- `Ankusa.Routes.Cache` is keyed by a per-snapshot `epoch`, not the store's
  `version`, because versions start over (a restarted in-memory store, a flushed
  Redis) and a decision cached under one could answer for a table it was never
  made against. Only requests of at most 16 segments and 256 bytes of path are
  cached, and the cache's memory check runs every second. A longer path is still
  decided correctly, by a scan of the route table; `[:ankusa, :routes, :match]`
  gains `cacheable` (false for such a path) so that cost can be seen.
- The IP-rule and trusted-proxy parser is `Ankusa.Net.parse_cidr/1`, which never
  raises and refuses a range inside `::ffff:0:0/96` (see Fixed).

### Fixed

- Concurrent route writes could overwrite each other. `create` checked the id and
  the path against a snapshot outside the store's serialized write, so two
  creates of one id both answered `201` and the second replaced the first, and
  two enabled routes could take one path and method. A `PATCH` was an unguarded
  read-modify-write that lost concurrent updates, and one racing a `DELETE`
  brought the route back.
- An unreadable `X-Forwarded-For` entry from a trusted proxy resolved the client
  to the proxy's own address, which an allow rule for the proxy's range then
  admitted. `Net.parse/1` no longer raises on invalid UTF-8.
- A range inside `::ffff:0:0/96` as an IP rule or trusted proxy is refused at
  write time, and the error says to write the IPv4 CIDR instead. Addresses are
  matched as IPv4, so it could never match — and as a deny rule it silently let
  its own traffic through.
- A dry run (`POST /admin/routes/test`) no longer reads or fills the decision
  cache, and a store restart can no longer let a cached decision answer for the
  new table.
- A seed with a disabled route after an enabled one for the same path no longer
  fails boot, and conflict detection is linear in the size of the seed.
- Route ids and methods refuse a trailing newline; a non-string `id` is a `400`
  instead of a generated one; `Route.from_json/1` refuses a stored definition
  with no id instead of minting one per node.
- The guard's log sampler draws independently per rejection, the warning for a
  node with no route table loaded is sampled like the rest, and `routes.enabled`
  must be a boolean. A route store that does not answer is `503` with the reason
  logged instead of a crash.
- The release notes below named `Ankusa.Net.CIDR` and `stream_data` property
  tests; neither ever shipped, and the entry now says what did.

## [0.3.0] - 2026-09-28

### Added

- `Ankusa.SourceStore` gains optional write callbacks `put/5`, `get/3`, and
  `list_tenant/2`, plus matching facade functions, and a new
  `Ankusa.SourceStore.Persistent` adapter that keeps sources in ETS and
  persists the API-managed ones to `sources.json` under the instance data
  directory. Sources can now be created, listed, and updated at runtime.
- The admin API gains `GET`/`POST /v1/tenants/:tenant/sources` and
  `GET`/`PUT /v1/tenants/:tenant/sources/:name`, and `GET /health` now reports
  the application version.

## [0.2.4] - 2026-09-28

### Added

- Route management: an allowlist in front of capture. With
  `routes.enabled: true` the edge is deny-by-default — a `POST` is captured
  only if its method and normalized path match an enabled route *and* its
  client address passes the IP rules; everything else is answered `404` (`403`
  for an IP denial, configurable to `404`) and never written to the WAL. Off by
  default, which is the previous behaviour. `Ankusa.Routes` is the context
  (`authorize/4`, `authorize_path/5`, `dry_run/2`, CRUD over definitions),
  `Ankusa.Edge.RouteGuard` the plug `Ankusa.Edge.Router` calls first on the
  capture path, and `Ankusa.Routes.Store.ETS` the default node-local store with
  a hard cap (nothing is evicted) and a `routes.seed` loaded at boot.
- `Ankusa.Routes.Router`: the management API on its own listener
  (`routes.admin.port`, default 4003), never the ingest port. Unauthenticated
  by design, the same stance as `Ankusa.Admin.Router` — Ankusa doesn't manage
  users, tokens, or API keys, so front it with your own proxy or network
  policy. Route CRUD, `GET`/`PUT /admin/ip-rules`, `GET /health`, and a dry
  run (`POST /admin/routes/test`) that reports the decision, the reason, the
  route, and the rule that produced it — without capturing anything.
- `Ankusa.Net` and `Ankusa.Net.ClientIP`: IP addresses as `:inet` tuples, CIDR
  matching through the `cidr` package, and client resolution that reads
  `X-Forwarded-For` **only** from a peer inside `routes.trusted_proxies` (one
  unparseable entry discards the header whole).
- `Ankusa.Routes.Cache`: a local decision cache (`nebulex` +
  `nebulex_local`) keyed by the snapshot version, so a route change retires
  every cached decision at once.
- Telemetry: `[:ankusa, :routes, :match]` (with `:cached`),
  `[:ankusa, :routes, :reject]` (with `:reason`), and
  `[:ankusa, :routes, :changed]`.
- Dependencies: `cidr` for CIDR parsing and range membership, and `nebulex` and
  `nebulex_local` — every deployment that turns routes on wants the decision
  cache, so it lives in core; the Redis *definitions* store is the separate
  `ankusa_redis` package. `yaml_elixir` (`:test` only) backs the OpenAPI contract
  test.
- `priv/openapi/admin.v1.yaml` now documents the route-management endpoints too
  (the `routes` tag: `admin/routes`, `admin/routes/{id}`, `admin/ip-rules`,
  `admin/routes/test`), with a worked example of every request and response —
  the examples are a sequence, one route's life. The tag is served on its own
  listener, so the path items carry a `servers` override and `/health` is
  documented as a union of the two listeners' bodies.
- `test/ankusa/routes/router_openapi_test.exs`: the contract test. It builds
  every documented request from the document's own examples, sends it through
  the real router, and checks the status, the schema, and the field names
  against what the document says; it enforces the documented method matrix, 404s
  the near-misses of the documented surface, and validates every example against
  its own schema. A rename, a status change, an added or removed endpoint, or a
  stale example fails the suite instead of shipping.
- `Ankusa.Routes.ip_rules/1` falls back to the configured rules when no store has
  published a table, and a write through a store with no table is the documented
  `{:error, :store_unavailable}` rather than a crash.

### Changed

- `Ankusa.Config` gained the `:routes` section. It merges one level deeper than
  the others (`routes.cache`, `routes.ip_rules`, and `routes.admin` merge key by
  key), because `routes.cache.max_size` replacing the whole section would
  silently drop the TTLs.

## [0.2.1] - 2026-09-28

### Changed

- The claim reference is `urn:ankusa:claim:v1:<tenant>:<claim_id>`, where
  `claim_id` is a canonical (uppercase) ULID: the pack's timestamp and
  entropy, with the claim's position in its pack in the last 16 bits. The
  digest moved out of the reference into the queue message's new `sha256`
  field (lowercase hex). The gateway route is
  `GET /v1/claims/:tenant_id/:claim_id`; its `416`/`invalid_range` response is
  gone, and a claim id past the end of its pack is `404`.
- `Ankusa.ClaimCheck.redeem/3` takes the ref (or its URN) and the expected
  sha256; `read/3` takes a tenant and a claim id. `check_in/4` and
  `check_in_batch/2` return `%{ref: %Ref{}, sha256: hex}` per item, which is
  also what dispatch hands sinks in `ctx.claim`. `check_in/4`'s
  `:object_id` option is now `:pack_id`.
- Packs start with an `index.bin` entry (a big-endian `offset`, `length`
  `uint32` pair per claim) so the gateway can find a claim from its id alone.
  Claim entries are named by claim id; `manifest.json` rows gain `claim_id`.
  Pack objects live at `claims/tenant=<t>/dt=<day>/<pack_id>`.
- `[:ankusa, :claim_check, :check_in]` telemetry metadata carries `:pack_id`,
  and `:redeem` carries `:claim_id`, instead of `:object_id`.

## [0.2.0] - 2026-09-24

### Added

- `Ankusa.BlobStore.Azure`: Azure Blob Storage. Carries no credential
  dependency: a pre-generated `:sas_token` (Shared Access Signature) or a
  `:token_provider` MFA (Entra ID bearer token) is appended to each request;
  the adapter does no Shared-Key signing of its own. Local testing against the
  `floci-az` emulator in `docker-compose.yml`.
- `Ankusa.BlobStore.OCI`: Oracle Cloud Infrastructure Object Storage. OCI has
  no bearer/SAS shortcut, so this adapter signs its own requests
  (RSA-SHA256 *Signature version 1*) with OTP's `:public_key`, no dependency,
  pinned against OCI's published reference signature (computed independently
  with OpenSSL) in `test/ankusa/blob_store_oci_signing_test.exs`. Local
  testing against the `floci-oci` emulator in `docker-compose.yml`.

- Admin API (`Ankusa.Admin.Router`), started when `admin.enabled` is true
  (default `false`, port `4002`): `GET /health`, `GET /metrics`,
  `GET /v1/config` (redacted), `GET /v1/dlq`, `POST /v1/dlq/replay`,
  `GET /v1/quarantine`. Unauthenticated by design; front it with your own
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
  `Ankusa.Verifier.StandardWebhooks`, replaced by `Ankusa.Verifier.Hmac`
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
  field, `urn:ankusa:claim:v1:<tenant>:<object_id>:<offset>:<length>:sha256-<hex>`,
  instead of a nested ticket object. Message `v` stays `1`. Consumers
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
- `Ankusa.ClaimCheck`'s `redeem/2` takes a `%Ankusa.ClaimCheck.Ref{}` or its URN
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
  delivery task, and `WAL.DiskLog` no longer scans its whole index per read.
  Together those two were the dispatch throughput ceiling (see
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
  (`Sink.RabbitMQ`, `Sink.Kafka`): inline base64 up to `inline_max_bytes` or
  a claim ticket above it, with an additive `"v": 1` version field.
- `Sink.Http` adds `x-ankusa-tenant` to its identity headers when the
  envelope has a tenant id.
- `Ankusa.Config.parse_roles!/1` validates `ANKUSA_ROLES` against the fixed
  role set (`edge`, `dispatch`, `storage`, `claim_check`) and fails boot on
  an unknown name, instead of silently atomizing arbitrary input.
  `Ankusa.Config.new/1` rejects an unknown nested key (e.g. `batcher: %{max_queu:
  5}`) and correctly deep-merges a keyword-list section instead of
  replacing the whole defaults map.
- HTTP is `Req` throughout: the S3 and GCS blob stores, the claim-check
  `Remote` adapter, and `Sink.Http` no longer hand-roll `:httpc` plumbing
  (and `:inets` is no longer started by this package). Outbound requests go
  through `Ankusa.HttpClient`, which never follows a redirect (a followed one
  re-sends a hook as a `GET`) and takes `:req_options` from an allowlist:
  transport tuning only, so a caller cannot rewrite a URL that has already been
  signed.
- Ankusa.Application's built-in default instance is opt-in: `autostart`
  defaults to `false`, so depending on `ankusa` never binds a port as a side
  effect.

[Unreleased]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.2.4...HEAD
[0.2.4]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.2.1...ankusa-v0.2.4
[0.2.1]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.2.0...ankusa-v0.2.1
[0.2.0]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.1.0...ankusa-v0.2.0
[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/ankusa-v0.1.0
