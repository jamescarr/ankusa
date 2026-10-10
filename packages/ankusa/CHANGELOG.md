# Changelog

All notable changes to `ankusa` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

Release process: bump `version` in the package's `mix.exs` and move the
relevant `[Unreleased]` entries under a new dated heading in the same PR, in
accordance with SemVer. A pushed `<pkg>-vX.Y.Z` git tag publishes. See
[`docs/releasing.md`](../../docs/releasing.md) for the mechanics.

## [Unreleased]

### Fixed

- A store restored from backup (or created fresh with a backup configured)
  keeps its `RESTORE-IN-PROGRESS` marker until `reconcile_archive` has
  succeeded. A node that died, or whose reconcile failed, between the restore
  and the reconcile now reconciles on its next boot instead of opening as an
  existing store and reusing archived seqs.
- A backup refuses to run when the prefix's latest backup belongs to another
  store (`{:foreign_backup, theirs, ours}`, logged at error level): each store
  gets an id (`m:store_id`) on its first backup, carried by its checkpoints
  and recorded in every manifest. Two nodes on one prefix no longer purge each
  other's backups.
- Blob files of the hooks and quarantine column families are capped at 64 MiB
  (`blob_file_size`; RocksDB's default is 256 MiB), bounding what one backup
  upload or restore holds in memory.

### Added

- `Ankusa.Verifier.warn_unverified_shared/1`, run at boot: under a resolver
  that takes the tenant from the URL, each configured source that is shared
  (`tenant_id: "default"`) and has no verifier is named in a warning.
- An internal `SourceStore.Table` module: the ETS layout and spec handling
  `SourceStore.Persistent` and `ankusa_redis`'s `SourceStore.Redis` share.
  A source-store read while the store restarts answers
  `{:error, :unavailable}` instead of raising.

- **Store backup and restore** (`Ankusa.Store.Backup`, `backup.*`, off by
  default). Every `backup.interval_ms` (60 s) the store writes a RocksDB
  checkpoint (`Ankusa.Store.checkpoint/2`, off the store process so `/ready`
  keeps answering) and the uploader puts it in the `:backup` blob-store scope
  (`backup.blob_store`, else `storage.blob_store`, under `storage.key_prefix`):
  `.sst`/`.blob` files once each, the rest per backup, a sha256 manifest, then
  `backup/LATEST`. `backup.keep` (3) backups are kept. A store directory with
  no database restores `LATEST` at boot, every file checked, and refuses to
  start (`{:store_restore_failed, path, reason}`) when the backup can't be read
  instead of starting empty. A restored or new store is then reconciled with
  the segment store: `m:next_seq` moves past the highest archived seq and
  uncatalogued segments are catalogued from their `.idx`. New telemetry
  `[:ankusa, :backup, :stop | :state]` and metrics `ankusa_backup_runs_total`,
  `ankusa_backup_age_seconds`.

- **Readiness.** `GET /ready` on the ingest and admin listeners
  (`Ankusa.Health.ready/1`): `200` while this node's store takes a synced
  write (`Ankusa.Store.ready/1`, checked at most once a second, reopening the
  store when a write fails) and no write has failed in the last 5 s, else
  `503` with `Retry-After: 1` and
  `store: "write_failed" | "store_unavailable"`. `/health` stays liveness.
- **State gauges.** `Ankusa.Metrics.Gauges` (a `telemetry_poller`, new
  dependency) samples the store, the queue index, the quarantine pen and disk
  space every `admin.gauge_interval_ms` (15 s), and dispatch reports its
  scheduler, as `[:ankusa, :store | :queue | :quarantine | :disk | :dispatch,
  :state]` events and `/metrics` gauges (`ankusa_queue_pending`,
  `ankusa_queue_oldest_due_age_seconds`, `ankusa_disk_free_bytes`,
  `ankusa_dispatch_breakers_open`, …). `[:ankusa, :dispatch, :stop]` carries
  `sink` and `source_id`; delivery counters gain a `sink` label.
- **Per-sink isolation and circuit breakers.** Dispatch queues claimed rows
  per `{source_id, sink index, module}` and hands slots round-robin across
  them; `dispatch.sink_concurrency` caps one key's concurrent attempts. A key
  that fails `dispatch.breaker_failures` (5) times in a row is parked for
  `breaker_open_ms` (30 s), doubling up to `breaker_max_open_ms` (5 min), then
  probed; parked rows spend no attempts. `[:ankusa, :dispatch, :breaker]` on
  every transition.
- **Sink error classes.** `{:error, {:permanent, term}}` dead-letters after
  the attempt; `{:error, {:retry_after, ms, term}}` delays the next attempt
  (capped at an hour). `Ankusa.Sink.classify/1`.
- **Signed HTTP deliveries.** `Sink.Http` `:secret` (a `whsec_` secret or a
  list) adds Standard Webhooks `webhook-id`/`webhook-timestamp`/
  `webhook-signature` headers (`Ankusa.Sink.Http.Signer`).
  `:max_response_bytes` (64 KiB) caps what is read of a response.
- **Shared buckets.** `storage.key_prefix` puts every segment key under a
  per-node prefix; `claim_check.blob_store` gives claims a store of their own
  (a `LocalFS` one takes `:root`). Claim keys are never prefixed, so one
  gateway serves every node.
- **S3 credential chain.** With no static keys the S3 adapter reads
  `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`, then web
  identity (IRSA, STS `AssumeRoleWithWebIdentity`), then IMDSv2, caching
  temporary credentials until shortly before expiry; `:session_token` is
  signed when set.
- Claim gateway: `HEAD /v1/claims/{tenant}/{claim_id}`.
- `batcher.max_queue_bytes` (256 MiB): load shedding by buffered body bytes
  as well as records; `[:ankusa, :load_shed]` carries `bytes`.
- Boot-time range validation of every numeric config key (`"<key> must be
  <constraint>, got <value>"`), and of embedded sources' verifier secrets
  (`Ankusa.Verifier.validate_config!/1`, `Ankusa.Verifier.Hmac.validate_opts/1`).
- **`Ankusa.Sink.SQS`**: delivers hooks to an Amazon SQS queue with one
  SigV4-signed `SendMessage` per delivery, the same `Ankusa.Sink.Message`
  body as the other queue sinks, and the Kafka header set as message
  attributes. No new dependency (`req` + `aws_signature`). `:ok` only on a
  `200` whose `MD5OfMessageBody` matches the body sent. A `.fifo` queue gets
  `MessageGroupId` (`:message_group_id`, default `"tenant/source"`) and
  `MessageDeduplicationId` (the hook id, `id:replay:<replay_id>` on a
  replay). A message over `:max_message_bytes` (1 MiB), `InvalidMessageContents`
  and `InvalidParameterValue` are permanent; every other SQS error (a missing
  queue included: the sink never creates one) is retried. `describe/2`
  reports the AsyncAPI `sqs` channel binding. The admin config view shows
  `queue_url` and `message_group_id`.

### Changed

- `Ankusa.BlobStore.S3.Credentials` is now `Ankusa.AWS.Credentials` (ETS table
  `:ankusa_aws_credentials`), shared by `BlobStore.S3` and `Sink.SQS`. The
  chain and its options are unchanged.
- **Circuit breakers are on by default** (`dispatch.breaker_failures: 5`): a
  sink that keeps failing has its rows parked, without spending attempts,
  instead of each retrying on the policy's schedule, so they reach the DLQ
  later than before. `breaker_failures: 0` keeps the old behaviour. A replay
  job counts its rows parked behind an open breaker toward auto-pause.
- `Sink.Http` maps `400`, `401`, `403`, `404`, `410`, `413`, `422` to
  `{:permanent, {:status, s}}` (dead-letter now) and honours `Retry-After` on
  `408`, `429` and `5xx`.
- **Breaking:** `Ankusa.BlobStore` calls take a scope —
  `put(instance, :segments | :claims, key, data)` and so on — and the
  `list/3` callback returns `{:ok, keys} | {:error, reason}`, following every
  adapter's pagination (S3, GCS, Azure, OCI) to the end.
- **Breaking:** `c:Ankusa.SourceStore.fetch/2` may return
  `{:error, :unavailable}`: ingest answers `503 store_unavailable` instead of
  `404`, dispatch reschedules the row a second later with attempts unchanged
  instead of dead-lettering it as `:source_gone`, and a replay job retries
  its page.
- **Breaking:** the route snapshot lives in a per-instance ETS table
  published in generations (`Ankusa.Routes.Snapshot`), not
  `:persistent_term`; the removed `Ankusa.Routes.snapshot` function is replaced by
  `Ankusa.Routes.meta/1`.
- A source with a `tenant_id` other than `"default"` answers only routes
  naming that tenant (any other is `404`); `"default"` sources stay shared.
- A hook accepted flagged (`accept_flag`) spends its source's quarantine
  bucket.
- Single-claim pack ids are the check-in time plus 64 random bits (no longer
  derived from the hook id and its receive time); retention counts from
  check-in.
- Claim gateway: `cache-control: private, max-age=31536000, immutable`; `503`
  bodies carry only the error code; a store `403` is
  `503 store_forbidden` with `Retry-After: 60`.

### Fixed

- The claim sweeper no longer stops at a partition it cannot delete
  (`File.rm_rf/1`, logged); `claim_check.retention_days` must be `nil` or
  ≥ 1, and `sweep_interval_ms` positive.
- The S3 and Azure managed-identity credential caches are created when the
  `:ankusa` application starts; they used to belong to whichever request or task
  filled them first and vanished with it, so temporary credentials were
  fetched again on almost every call.
- The route snapshot table is owned by `Ankusa.Routes.TableOwner` and lent to
  the routes store, so a store crash no longer deletes it, and the restarted
  store resumes from it (`Ankusa.Routes.Snapshot.adopt/1`): the ETS store keeps
  API-created routes across a store crash instead of re-seeding from config.
- A `{:permanent, _}` answer to a breaker's probe closes the breaker; it used
  to leave the probe slot taken, parking the key forever.
- `storage.key_prefix` is applied when a segment is named and the catalogue
  keeps the full key, so changing the prefix no longer strands archived
  segments.

## [0.5.0] - 2026-10-08

### Added

- **Ingest dedupe.** `Ankusa.Dedupe` extracts a provider event key (a delivery
  header such as GitHub's `x-github-delivery`, or a JSON field such as
  Stripe's `"id"`) from every accepted hook, configured per source as
  `dedupe: :github | :standard_webhooks | :svix | :shopify | :stripe` or
  `dedupe: %{header: …}` / `dedupe: %{json: …}` with an optional `ttl_ms`
  (default 72 h). Keys are scoped to tenant and source. The queue writer
  collapses envelopes that share a key
  atomically in the commit batch: the retry is answered `201` with the
  original hook's id and `"duplicate": true`, nothing extra is stored. Expired
  keys are swept inside the writer. A forged, flag-accepted request never
  claims a key.
- **Replay jobs.** `POST /v1/replays` creates a durable, paced replay job:
  `kind: :dlq` re-sends dead delivery rows, `kind: :archive` re-sends archived
  hooks over a `received_at` window. Jobs drip rows into the existing due
  index at `rate` items per second, and only while dispatch's oldest-due lag
  is at most `max_lag_ms` and its in-flight window is not full — a replay uses
  only spare capacity and inherits retries, the DLQ, claim check and
  at-least-once bookkeeping. Every job is a store record whose cursor commits
  in the same batch as the rows it moves, so a restart resumes it; a job that
  keeps dead-lettering its deliveries pauses itself. `GET /v1/replays`,
  `GET|PATCH /v1/replays/{id}` manage them (`POST /v1/dlq/replay` is removed).
  `Ankusa.Replay.start/2`, `list/1`, `get/2` and `update/3` expose the same
  surface to embedders; replayed deliveries keep the hook's original `id` and
  `dedupe_key` and carry the job id as `replay_id` (also
  `x-ankusa-replay-id`, the `ankusa_replay_id` broker header, and NATS
  `Nats-Msg-Id` as `id:replay:<replay_id>`). A dead row whose delivery record
  cannot be decoded is passed over, not dropped or retried: it stays in the
  DLQ and `GET /v1/dlq` lists it with the reason `undecodable delivery row`.
- **Message identity and integrity.** Every queue message now carries
  `sha256` (also on inline bodies), `dedupe_key`, `replay_id`, and the
  forwarded provider request `headers` (per the source's `forward_headers`
  option, `:default` forwards everything except auth/framing/hop-by-hop and
  `x-ankusa-*` headers). `Sink.Http` forwards them as request headers, with
  `x-ankusa-dedupe-key`/`x-ankusa-replay-id`; RabbitMQ carries the hook id as
  AMQP `message_id` plus the dedupe/replay AMQP headers; NATS sets `Nats-Msg-Id`
  on every publish (the hook `id`, or `id:replay:<replay_id>` on a replay); Kafka and NATS carry
  them as record headers. All 8 SDKs decode the message, verify its integrity,
  and read the idempotency key (below; `#replay:<replay_id>` is appended when
  asked); the conformance suite covers it.
- **Idempotency key, computed once.** `Ankusa.Envelope.idempotency_key/1`
  returns the tenant-scoped key a consumer dedupes on:
  `tenant:source_id:dedupe_key` when the hook has a dedupe key (`default` when
  it has no tenant), else its `id`. It ships as the message's
  `idempotency_key` field (additive, still `v: 1`; `SinkMessageV1` now requires
  `sha256`, `dedupe_key`, `replay_id`, `idempotency_key` and `headers`, and
  `SinkMessageHeadersV1` gains `ankusa_dedupe_key`, `ankusa_replay_id` and
  `Nats-Msg-Id`),
  the `x-ankusa-idempotency-key` header on `Sink.Http`, and the
  `ankusa_idempotency_key` header on RabbitMQ, Kafka and NATS. Ingest dedupe
  scopes by tenant and source, so two tenants' hooks with one provider event id
  are two hooks; the tenant in the key keeps them two keys at the consumer. The
  SDK `idempotency_key` helpers read the shipped value and compute the same
  formula only for a message or delivery that predates the field.
- **Direct-mode deadline.** With `wal.type: none`, `Ankusa.Edge.Publish`
  publishes to every sink concurrently under one overall deadline
  (`direct_publish_timeout_ms`, default 8 000, configurable as
  `wal.publish_timeout_ms` in `ankusa.yml`); a sink that misses it is a 503.
- `Ankusa.Dispatch.Pipeline.pressure/1` reports the oldest-due lag and window
  state for replay pacing.
- `Ankusa.UUIDv7.min_for/1` and `max_for/1` bound a millisecond's id range.
- `Ankusa.Queue.redrive/3` commits archived hooks back into the queue in one
  synced batch with the replay job's cursor.
- **Failure domains.** `Ankusa.Instance` is now `:rest_for_one`, and dispatch,
  storage (compactor and claim-check sweeper), lifecycle, metrics and the
  admin, route-admin and claim-check listeners each run under
  `Ankusa.Instance.Isolated` with a restart budget of their own. When a
  domain's budget is exhausted it is restarted later with backoff (1 s doubling
  to 60 s, reset after a minute up) instead of rebuilding the instance, edge
  listener included. The store, the source store and the edge (routes, queue
  writer, quarantine, rate limiter, batchers, ingress listener, now one
  `rest_for_one` subtree registered as `Ankusa.via(instance, :edge)`) are the
  core: a crash there restarts what depends on it. A child that cannot start at
  boot still fails the boot. New telemetry events
  `[:ankusa, :instance, :subtree_down]` (`:delay_ms`; `:instance`, `:domain`,
  `:reason`) and `[:ankusa, :instance, :subtree_up]`.
- `Ankusa.Instance.RegistryWatch`: an instance stops, and its supervisor
  restarts it, when `Ankusa.Registry` or one of its partitions restarts. A
  restart used to leave the store and every supervisor running but
  unregistered (`Ankusa.whereis/2` returned `nil`). The application's own
  supervisor is now `:rest_for_one` for the same reason.
- **Quarantine release.** `kind: :quarantine` replay jobs (`POST /v1/replays
  {"kind":"quarantine"}`, `Ankusa.Replay.start/2`) re-verify hooks held in the
  quarantine pen against each source's current verifier, judging the
  timestamp window at the hook's receive time, and commit the ones that pass
  through the queue writer with their original `id` and `replay_id` on every
  delivery row; the rest stay in the pen. Optional `source_id`, `id`,
  `since`/`until` (on `received_at`) filters. Needs the `:edge` and
  `:dispatch` roles. `Ankusa.Queue.release/3` commits such hooks with the pen
  deletes and the job's cursor in one synced batch.
- `DELETE /v1/quarantine` (`Ankusa.Edge.Quarantine.purge/3`) deletes held
  hooks by `source_id`, `id`, `since`/`until` and `limit` and reports
  `{deleted, bytes}`. `GET /v1/quarantine` entries gain `tenant_id` and
  `size`.
- `quarantine` config section: `burst` and `rate` (one token bucket per
  source, defaults 100 and 20/s) and `max_bytes` (the pen's byte cap, default
  1 GiB). New telemetry event `[:ankusa, :quarantine, :full]` and metric
  `ankusa_quarantine_full_total`.
- Metrics `ankusa_replay_moved_total{instance,kind}` and
  `ankusa_replay_throttled_total{instance,reason}`, telemetry events
  `[:ankusa, :replay, :moved | :throttled | :state]`, and
  `outcome="duplicate"` on `ankusa_ingest_requests_total`.
- `Ankusa.Verifier.Hmac` accepts `secret: [new, old]`: every key is tried, so
  a secret rotation needs no cut-over. An empty list or an element that is not
  a usable key is `{:error, :bad_secret}`.
- `Ankusa.Verifier.check_timestamp/2` takes `now:` (Unix seconds) to judge the
  window against a fixed instant; `Ankusa.Verifier.scheme_name/2` is the
  shared scheme label.
- `config.dispatch.attempt_timeout_ms` (default `30_000`): a delivery attempt
  that has not returned after it is killed and counts as a failed attempt with
  reason `{:attempt_timeout, ms}`, so a hung sink frees its dispatch slot. It
  covers the sink call and the fallback claim check-in of an attempt, and
  `Ankusa.Lifecycle.Publisher` applies the same deadline to lifecycle sinks.
  The sink may still complete a killed delivery, so consumers dedupe on the
  idempotency key. `Ankusa.Dispatch.Pipeline.validate_config!/1` rejects a value
  that is not a positive integer.
- `Ankusa.Edge.Ingest.lookup/2`, `ingest/4` and `refused/2`. The edge looks
  the source up with `lookup/2` before it reads the body, then calls
  `ingest/4` with the source it found; `ingest/2` is `lookup/2` followed by
  `ingest/4`. `refused/2` emits the new telemetry event
  `[:ankusa, :ingest, :refused]` (`:instance`, `:reason` of
  `:unknown_source | :payload_too_large | :body_read_failed | :invalid_header`), and
  `Ankusa.Metrics` counts it as `ankusa_ingest_refused_total{instance,reason}`.
- **An `ip` option for the admin API, the route management API and the
  claim gateway.** `admin.ip`, `routes.admin.ip` and `claim_check.ip` take a
  strict IPv4 or IPv6 literal (or an `:inet` address tuple) and default to
  `"127.0.0.1"`; `Ankusa.Config.new/1` raises `ArgumentError` for anything else
  (`"10"` and `"localhost"` included), through `Ankusa.Config.listen_ip!/2`.
  The edge listener has no `ip` and stays on every interface.
- `Ankusa.Store.report_write_failure/2`: tell the store a write failed, so it
  reopens itself (at most once per 5 s) to clear RocksDB's latched error.
  `Ankusa.Store.write/3` calls it on every failed batch.

### Changed

- Delivery rows may carry `replay: replay_id`, the job a replayed delivery is
  attributed to.
- `Ankusa.Queue.enqueue/3` takes an optional `deadline` (a
  `System.monotonic_time(:millisecond)` value) and waits for the commit's
  outcome instead of timing out after 5 s. The writer refuses, without
  consuming a seq, a batch it could not start before the deadline
  (`{:error, :deadline_exceeded}`) and one whose caller died while it waited
  (`{:error, :caller_gone}`).
- `Ankusa.Edge.Batcher.commit/4`'s `timeout` bounds how long a record may wait
  before its batch *starts* committing (15 s by default), not the commit
  itself. `:infinity` still means no bound.
- `Ankusa.Sink.safe_deliver/4` returns `{:error, {:bad_return, value}}` for
  any return value other than `:ok` or `{:error, _}`. Dispatch, lifecycle
  events and the `wal: :none` ack path all deliver through it.
- **Breaking: the quarantine bucket is per source and refuses with `429`.**
  A source over its bucket answers `429 quarantine_rate_limited` with
  `Retry-After` instead of `401 verification_failed`; `401` now always means a
  failed signature. `Ankusa.Edge.Quarantine.put/3` returns
  `{:rate_limited, retry_after_ms}` or `:full` instead of `:rate_limited`, and
  `Ankusa.Edge.Ingest.ingest/2` returns
  `{:error, {:quarantine_rate_limited, ms}}` or `{:error, :quarantine_full}`
  (outcome tags `:quarantine_rate_limited` and `:quarantine_full`).
- The quarantine pen keeps the whole envelope (method, path, headers, body,
  tenant) instead of headers and body, and refuses a write that would cross
  `quarantine.max_bytes` with `503 quarantine_full` (`Retry-After: 60`); it
  never evicts a held hook. Entries held by earlier versions stay readable and
  releasable (rebuilt as `POST /`).
- **The default retry policy retries for about 6 hours.**
  `Ankusa.RetryPolicy.Exponential` defaults to `max_ms: 300_000` and
  `max_attempts: 84` (they were `30_000` and `12`, about 83 s): 100 ms doubling
  to a 5-minute cap by attempt 13, then 5 minutes apart. A retry is a delivery
  row due later and holds no slot, so a longer horizon costs nothing while a
  sink is down. Set `max_attempts` lower to dead-letter sooner. Errors are not
  classified yet, so a permanent failure (an HTTP `400`, say) also takes every
  attempt before it reaches the DLQ.
- **Breaking for dashboards: refused requests are no longer on
  `ankusa_ingest_requests_total`.** A request for a source that does not
  exist used to be counted there as `outcome="unknown_source"` under a
  `source_id` taken from the URL; it is now
  `ankusa_ingest_refused_total{reason="unknown_source"}`, as are `413`
  (`payload_too_large`) and unreadable-body `400` (`body_read_failed`)
  refusals. A panel that sums `ankusa_ingest_requests_total` for "all
  traffic" must add the refused counter. `source_id` on `[:ankusa, :ingest]`
  and on the metrics built from it is now always a configured source's id.
  `Ankusa.Edge.Ingest.ingest/2` still returns `{:error, :unknown_source}` for
  one.
- **Breaking for callers that match exhaustively: a duplicate is a new
  result.** `Ankusa.Edge.Ingest.ingest/2` and `ingest/4` may return
  `{:duplicate, env}` (besides `{:ok, env}`), `Ankusa.Edge.Batcher.commit/4`
  returns `{:duplicate, env}` besides `{:committed, env}`, and
  `Ankusa.Queue.enqueue/3` returns
  `{:ok, [{:committed, env} | {:duplicate, env}]}`.
- **Breaking for embedders: the supervision tree is split into failure
  domains.** The batcher, queue writer, quarantine, rate limiter, routes and
  ingress listener now live under `Ankusa.via(instance, :edge)`; dispatch,
  storage, lifecycle, metrics and the listeners under `Ankusa.Instance.Isolated`
  subtrees, so `Supervisor.terminate_child/2` / `which_children/1` on the
  instance root no longer finds them. See "Failure domains" under Added.
- **Breaking for HTTP-sink receivers and queue consumers: provider request
  headers are forwarded by default.** `forward_headers: :default` forwards
  every header except auth/framing/hop-by-hop and `x-ankusa-*`; set
  `forward_headers: [...]` to narrow it.
- **Breaking: the admin API (4002), route management API (4003) and claim
  gateway (4001) listen on `127.0.0.1` by default.** They used to bind every
  interface and are unauthenticated. A deployment that reaches one from another
  host or container must set `admin.ip`, `routes.admin.ip` or `claim_check.ip`
  to `"0.0.0.0"` (or one interface's address) and keep fronting it with its own
  proxy or network policy.
- **Breaking: the edge refuses header bytes no sink can carry.** A request is
  answered `400 {"error":"invalid_header","header":"<name>"}` before its body is
  read when a header name holds a byte outside `0x21..0x7E` or a value one
  outside `0x20..0x7E` and tab (RFC 9110 field-value without `obs-text`); a
  non-ASCII value such as `café` is refused too, because `Sink.Http` (Mint)
  cannot send it and queue sinks need UTF-8. `header` is `null` when the name is
  itself not printable. Such a request used to be acked `201` and then retried
  for hours and dead-lettered. `Ankusa.Edge.Ingest.ingest/2` returns
  `{:error, {:invalid_header, name | nil}}`, and the refusal is counted as
  `ankusa_ingest_refused_total{reason="invalid_header"}`. New
  `Ankusa.Edge.Ingest.check_headers/1`.
- **Breaking for anything that reads `GET /v1/config`: redaction is an
  allowlist inside adapter options.** An option of a `{module, opts}` pair
  (sinks, verifiers, blob store, source store, codec, retry policy, route
  resolver, route store, lifecycle sinks) is shown only under a known
  non-secret key (`bucket`, `region`, `topic`, `exchange`, …); every other
  value, `sas_token`, `private_key`, `jwt` and `access_key_id` included, is
  `"[REDACTED]"`. A URL (`url`, `endpoint`, `resource`) keeps its host and
  path, loses the password of `user:pass@` (or a lone `token@`), and every
  query value becomes `[REDACTED]`. The same view feeds `print-config` and the
  source lifecycle events.
- **Breaking for callers: `Ankusa.Sink.inline_max_bytes/2` returns
  `{:ok, max | nil} | {:error, reason}`** (it returned `max | nil`). A raise,
  exit, throw or invalid return from a sink's `inline_max_bytes/1` is an
  `{:error, _}`; dispatch fails that delivery with `{:inline_max_bytes, reason}`
  (the retry policy runs, the DLQ reason names it) and `wal: :none` answers
  `503`.
- `Ankusa.Fsync.mkdir_p/1` is `mkdir_p/2`: it takes the root the path may be
  created under, and fsyncs the parent of every directory from the root down on
  every call. A LocalFS put costs one extra directory fsync per level between
  its root and the object.

### Removed

- **Breaking: `POST /v1/dlq/replay`.** Replaced by `POST /v1/replays` with
  `kind: "dlq"`; `GET /v1/dlq` is unchanged.
- `Ankusa.Dispatch` (`replay/1`, `replay/2`) — replaced by
  `Ankusa.Replay.start/2`.

### Fixed

- `Ankusa.Verifier.Hmac` fails closed on an empty key. A missing, `nil` or
  empty `:secret`, an empty list, or a list element that is empty or does not
  decode is `{:error, :bad_secret}` for every hook. It used to HMAC with the
  empty key, which verified anything signed with it (a forgery anyone can
  compute); an explicit `nil` raised on every request.
- A stalled store no longer answers `503` for hooks it then commits. It used to
  answer `503` after 5 s for hooks the writer committed afterwards; now a batch
  the writer has started is waited out, one it has not is refused, and one
  whose commit task died before the writer reached it is dropped. A process
  dying while the writer is mid-commit (the commit task, the batcher, or the
  writer after its sync) can still answer `503` for a durable hook, which the
  provider's retry then stores again.
- The batcher's commit task is supervised, not linked: a commit task that dies
  fails its own batch, not the batcher and the records buffered behind it.
- A sink that returns something other than `:ok` or `{:error, _}` is a `503` on
  the `wal: :none` ack path, not a `CaseClauseError` in the request.
- A claim check whose blob store write exits, throws or returns something other
  than `:ok` or `{:error, _}` (a `:token_provider` that exits, say) is
  `{:error, {:unavailable, _}}`, a `503` under `wal: :none`, not a crashed
  request.
- A GenServer crash report (the "terminating" report Logger prints) no longer
  prints sink options, and neither does `:sys.get_status/1` on these
  processes: the dispatch pipeline, compactor, claim-check sweeper, rate
  limiter, lifecycle publisher, writable source store, edge batchers and
  `Ankusa.Instance.Isolated` redact their state, and the batchers and the
  writable source store also redact the message being handled. Supervisors
  still carry the config in their children's start arguments, so
  `:sys.get_status/1` on a supervisor (and observer) still shows it, and so
  would supervisor reports if SASL reports were enabled (Logger's
  `handle_sasl_reports`, off by default).
- Unauthenticated requests no longer mint Prometheus series: 20 000 `POST`s to
  random paths used to add 280 000 `ankusa_ingest_*` series (the lookup ran
  inside the `[:ankusa, :ingest]` span, with the URL's `source_id` as a label);
  they are now one `ankusa_ingest_refused_total` series.
- The edge answers a request it will refuse without reading its body: a
  `Content-Length` above `max_body_bytes` is `413` and an unknown source (or a
  tenant that is not valid) is `404` immediately, where each used to buffer up
  to `max_body_bytes` first. The envelope copies the body only when it is a
  slice of a larger binary, not on every request.
- The compactor backs off. After a failed tick it retries at
  `storage.interval_ms` doubled per consecutive failure (jittered, capped at
  60 s) instead of every interval, and a blob store whose `put/4` exits,
  throws or returns something other than `:ok` or `{:error, _}` fails the tick
  instead of crashing the compactor.
- Any failed store write now asks the store for a reopen (at most once per
  5 s), whichever process saw it (dispatch outcomes, the compactor,
  quarantine, the replayer, rate-limit overrides, the source store), not only
  an ingest commit: a fallback for a latched RocksDB write error left by a full
  disk. RocksDB recovered from a full disk on its own in a container drill, so
  this is a safety net, not a measured fix. `Queue.Writer` no longer reopens
  the store itself, so ingest no longer blocks on a synchronous reopen.
- Concurrent first writes into a new LocalFS directory (a claim pack's
  `tenant=*/dt=*` partition at UTC midnight) no longer return before the
  directory is durable, and a parent fsync that failed once is retried by the
  next write.
- A custom sink whose `inline_max_bytes/1` raises no longer crash-loops the
  dispatch domain, which stopped delivery for every tenant.
- A quarantine pen write that does not answer within 5 s is `503
  store_unavailable` with `Retry-After: 1`, not a crashed request.

## [0.4.0] - 2026-10-02

### Added

- **An AsyncAPI 3.0 document of the channels an instance publishes to**, served
  by the admin API at `GET /asyncapi.json` (`application/asyncapi+json`) and
  built by `Ankusa.AsyncApi.document/1` from the configured sources. Messaging
  sinks advertise their channel through the new optional
  `c:Ankusa.Sink.describe/2` callback (`Ankusa.Sink.Description`); the Kafka,
  RabbitMQ, NATS, and Redis sinks implement it. Built on the new `async_api_spex`
  package, now a dependency. [`docs/asyncapi.md`](../../docs/asyncapi.md).
- **Lifecycle events**: `config.lifecycle.sinks` (off by default) receives a
  CloudEvents 1.0 event, `io.ankusa.source.{created,updated,deleted}` or
  `io.ankusa.route.{created,updated,deleted}`, whenever `Ankusa.SourceStore.put/5`
  or `delete/3` or `Ankusa.Routes.create/2`, `replace/3`, `update/3`, or
  `delete/2` changes something. Events never touch the store: a supervised
  in-memory publisher (`Ankusa.Lifecycle.Publisher`) delivers each one to every
  lifecycle sink independently, retrying with `dispatch.retry`, off the caller's
  path. When its queue is full (10,000 pending sink deliveries), when retries run
  out, or when it isn't running, the event is dropped and counted; pending
  events are lost on restart and are not ordered. A lifecycle failure never
  fails the change. `[:ankusa, :lifecycle, :delivered]` and
  `[:ankusa, :lifecycle, :dropped]` telemetry events, and the
  `ankusa_lifecycle_delivered_total` / `ankusa_lifecycle_dropped_total` metrics.
- `Ankusa.Store`: one RocksDB database per instance at
  `<data_dir>/<instance>/store` (Hex `rocksdb`, erlang-rocksdb), holding the
  hooks, one delivery row per hook and sink, the quarantine pen, API-managed
  sources, rate-limit overrides, the segment catalogue, and the migration
  markers. It replaces `wal/ankusa.wal` (and its `.cursors`/`.truncated`
  sidecars), `dlq/dlq.log`, `quarantine/quarantine.log`, `segments/index.log`,
  `sources.json`, and `rate_limits.json`. Column families: `default`, `hooks`,
  `deliveries`, `index`, `archive`, `quarantine`.
- `Ankusa.Queue` (public): `enqueue/2` commits a batch of hooks with one synced
  store write, `hooks/3` reads them back, `stats/1` reports the store, `dead/2`
  lists the dead rows, and `validate_config!/1` / `label/1` replace the old WAL
  adapter's. `Ankusa.Queue.Writer` is the only seq assigner: one group commit is
  one synced batch holding each hook, one pending delivery row and due key per
  sink of its source (bound by sink index and module at ack), an archive
  obligation while the `storage` role runs, and the seq marker. A failed commit
  acks nothing but may consume seqs, so seqs are strictly increasing and never
  reused (gaps are allowed). A hook with no obligations is acked with a seq but
  not stored.
- `Ankusa.Edge.Quarantine.recent/2` reads the pen from the store, newest first;
  the pen survives a restart (the old 200-entry in-memory list is gone).
- An import of a 0.3 data dir on the first boot of a new store:
  `sources.json`, `rate_limits.json`, `quarantine/`, `wal/`, `dlq/`, and
  `segments/index.log` are read in that order, marked in the store, and then
  renamed `<name>.migrated-<unix seconds>` (never deleted — renaming them back
  is the rollback). Imported dead letters replay to the source's current sinks,
  and the next seq is past everything imported. Reading is chunked, so memory
  stays bounded at any file size.

### Changed

- `Ankusa.Admin.Redact.source_entry/1` takes the `Ankusa.SourceStore.stored()`
  map and builds the redacted source view itself (the admin API's source
  endpoints and the lifecycle events share it), where it used to redact a map
  the admin router had built.
- **Breaking: the WAL, DLQ, quarantine and segment-index files, and the
  `sources.json` / `rate_limits.json` state files, are gone.** One RocksDB store
  per instance owns them all; a 0.3 data dir is imported on first boot (above).
  The data volume now holds `store/`, segments at
  `segments/seg/<first_seq>-<last_seq>.seg` with a new sibling
  `seg/<first_seq>-<last_seq>.idx` per segment, and `claims/...` claim packs.
- **Breaking: `wal: {Ankusa.WAL.DiskLog, _}` raises** `ArgumentError` with the
  hint to use `wal: :disk` (the RocksDB store) or `wal: :none`. The key itself
  is unchanged: `wal: :disk | :none` in core, `wal.type: disk | none` in YAML.
- **Breaking: `Ankusa.Dispatch.replay/2` returns `{:ok, n}`** — the number of
  rows moved back to pending — and is asynchronous: it moves the rows and the
  pipeline delivers them. A replayed row leaves the DLQ, where the old replay
  left the entry in place. It needs the `dispatch` role.
- **Breaking: `GET /v1/wal` reports the store.** The body is `next_seq`
  (exact) plus `hooks` and `deliveries` (RocksDB key estimates) and
  `disk_bytes`; `wal` is `{}` when the store is unreachable, and it is still
  `409 wal_disabled` under `wal.type: none`. `503 store_unavailable` is added to
  `GET /v1/dlq`, `POST /v1/dlq/replay`, `GET /v1/quarantine`, and the
  rate-limit routes.
- **Breaking for embedders: core compiles a NIF.** Building `rocksdb` from
  source needs cmake >= 3.12, a C++20 compiler, and zstd + OpenSSL development
  headers; on Alpine, `linux-headers`, and Ubuntu CI runners need `libzstd-dev`.
- Dispatch is a scheduler over delivery rows. A commit wakes it; otherwise it
  sleeps until the earliest due row. A retry is a row due at `now + backoff`,
  so it frees its concurrency slot instead of sleeping inside a task. The
  window is `dispatch.max_inflight` claimed rows and
  `dispatch.max_inflight_bytes` (the sum of their stored hook sizes);
  `dispatch.batch` is rows claimed per store scan.
- A row's sink binding is `(index, module)` at ack, but its opts always come
  from the source as it is now, so a config fix applies to the backlog and no
  fun or secret is ever persisted. A reordered source falls back to the unique
  sink with the row's module; otherwise the row is dead-lettered with
  `{:sink_gone, index, module}`, and a deleted source with
  `{:source_gone, source_id}`.

### Removed

- **Breaking: the Ankusa.Sink.ordering_key/2 callback, Sink.ordering_key/3, and
  the http sink's `ordered` option.** Deliveries are unordered, and a broker
  key (a Kafka partition key, an AMQP routing key) only keeps the order hooks
  were published in. A consumer that needs order has to rebuild it from data it
  receives, such as the provider's event timestamp or sequence number in the
  body, and tolerate redelivery.
- **Breaking: `dispatch.poll_ms`.** Dispatch is woken by a commit or by the next
  due row; the key is now an unknown-key error.
- Ankusa.WAL, Ankusa.WAL.DiskLog, Ankusa.DurableLog, Ankusa.Storage.Index,
  Ankusa.Dispatch.DLQ.

### Fixed

- **A full disk recovers by itself.** A commit that cannot write answers `503`
  `store_unavailable` and acks nothing; the Writer asks the store to close and
  reopen (at most once every 5 s), which clears RocksDB's latched write error,
  so ingest resumes once space frees.
- **`kill -9` loses nothing acked.** Every commit is one synced store write; the
  loss suite SIGKILLs a separate BEAM and every acked hook is read back.
- **Corruption is reported, never silently truncated or shortened.**
  `paranoid_checks` plus `wal_recovery_mode: tolerate_corrupted_tail_records`
  drop a torn tail (the last, never-acked write) and refuse to open on damage
  before it (`{:store_open_failed, path, reason}`). Point reads report checksum
  failures; a scan believes only a result that reaches its end-of-range
  sentinel. The Writer, the persistent source store, and the rate limiter refuse
  to start when they cannot read their state, rather than treating an unreadable
  store as empty.
- **One damaged frame in a 0.3 WAL no longer drops the acked frames after it.**
  A frame that fails its CRC with a valid frame after it refuses to start
  (`{:damaged_legacy_wal, path, byte, later_byte}`); only a torn tail is
  skipped. An unreadable `.cursors` or `.truncated` sidecar likewise refuses to
  start (`{:corrupt_legacy_sidecar, path}`).
- **Reclamation no longer needs the archive.** A hook is deleted once its last
  obligation clears — every delivery row and, only if `storage` ran at ack
  time, the archive obligation. So a node without the `storage` role reclaims on
  delivery, and the compactor can lag or be off without blocking delivery.
- **A retry no longer holds a delivery slot**, so a failing sink cannot starve
  other sinks or sources.
- **The archive compactor no longer crash-loops.** A failed blob write ends the
  tick with an error log; nothing crashes, and the same hooks are rewritten next
  tick.
- **LocalFS blob writes are durable.** `Ankusa.BlobStore.LocalFS` writes a
  temp file, fsyncs it, renames it, and fsyncs the directory on every `put`; a
  failed write returns `{:error, reason}` instead of raising. Claim packs,
  segments, and index objects all go through it.
- **The `scheme` label on verification metrics is the scheme, from the first
  request.** Ingest asked a verifier for its scheme name without loading the
  module first, so until something else had loaded it the label (and the
  `scheme` in telemetry metadata) was the module name instead, for example
  `Ankusa.Verifier.Hmac` where `stripe` was meant.

## [0.3.0] - 2026-10-01

### Added

- `wal: :none`, the stateless ack path. Ingest verifies, then publishes to the
  source's sinks in the request (`Ankusa.Edge.Publish`) and answers `201` only
  once every sink has confirmed; a refusal is a `503` with `Retry-After` and the
  provider retries. No WAL, no batcher, no dispatch pipeline, no compactor, no
  DLQ: the node runs only the `edge` role (the WAL's reader roles are dropped
  from `config.roles` rather than rejected). Ankusa.WAL.validate_config!/1
  refuses a `wal: :none` config in which a statically configured source has no
  sink whose `:ok` means durable — sources created at runtime through the admin
  API are not checked.
- `c:Ankusa.Sink.durable?/1`, the per-sink promise `wal: :none` acks on; defaults
  to `true`, and `Ankusa.Sink.Log` answers `false`. `Ankusa.Sink.safe_deliver/4`
  (the raise/throw/exit-to-`{:error, reason}` wrapper hidden inside the dispatch
  pipeline) is now public, so both ack paths deliver through the same function.
- `GET /v1/wal` on the admin API: this node's Ankusa.WAL.stats/1, or
  `409 wal_disabled` under `wal: :none`. Ankusa.WAL.label/1 names the
  configured adapter for boot banners and `check-config`.
- Per-tenant ingest rate limits (`Ankusa.Edge.RateLimiter`, `rate_limits` in the
  config: `%{rate: hooks_per_second, burst: hooks}` per tenant, plus a
  `default`). A hook is charged **after verification and before the durable
  write**, so forged requests spend no budget; over the limit the sender gets
  `429` with `Retry-After` and nothing is stored. Enforcement is GCRA over one
  ETS row per tenant, updated by compare-and-swap, so concurrent hooks on one
  tenant cannot overshoot, and the buckets are this node's alone. The admin API
  reads and adjusts limits at runtime without a restart (`GET /v1/rate-limits`,
  `GET|PUT|DELETE /v1/tenants/:tenant/rate-limit`, `:edge` role), persisting
  overrides to `rate_limits.json` in the instance's data dir; a `PUT` or
  `DELETE` resets that tenant's bucket. Rejections emit
  `[:ankusa, :rate_limit, :rejected]` (tenant-tagged) and appear as
  `ankusa_rate_limit_rejected_total`, and the ingest `:outcome` label gains
  `:rate_limited`.
- `Ankusa.SourceStore` gains optional write callbacks `put/5`, `get/3`, and
  `list_tenant/2`, plus matching facade functions, and a new
  `Ankusa.SourceStore.Persistent` adapter that keeps sources in ETS and
  persists the API-managed ones to `sources.json` under the instance data
  directory. Sources can now be created, listed, and updated at runtime.
- The admin API gains `GET`/`POST /v1/tenants/:tenant/sources` and
  `GET`/`PUT /v1/tenants/:tenant/sources/:name`, and `GET /health` now reports
  the application version.

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
- Ankusa.Sink.ordering_key/2, an optional sink callback naming the ordering
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
  deliveries run at once, serialized per Ankusa.Sink.ordering_key/2, and the
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
- WAL.DiskLog truncation is logical first: `truncate_through/2` records a
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

- WAL.DiskLog: a restart after a full truncation resumed at `seq` 1 while the
  persisted cursors were far ahead, so every new hook was skipped by dispatch
  and then deleted by the next truncation. Seq allocation now continues from
  the truncation floor, the persisted cursors, and the last replayed frame.
- DLQ appends and storage-index appends are fsynced: a dispatch cursor could
  advance past a dead letter (or a compaction cursor past index rows) that a
  power loss then dropped, leaving the hook in neither place.
- `Ankusa.Dispatch` no longer copies the whole pipeline state into every
  delivery task, and WAL.DiskLog no longer scans its whole index per read.
  Together those two were the dispatch throughput ceiling (see
  [`docs/testing.md`](docs/testing.md#core-bench--benchcore_benchexs)).

## [0.1.0] - 2026-09-23

### Added

- Core ingest pipeline: Bandit edge, group-commit batcher, durable WAL
  (Ankusa.WAL.DiskLog), idempotent receiver, dispatch pipeline, segment
  compactor.
- Pluggable behaviours: `Ankusa.RouteResolver`, Ankusa.WAL,
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

[Unreleased]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.5.0...HEAD
[0.5.0]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.4.0...ankusa-v0.5.0
[0.4.0]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.3.0...ankusa-v0.4.0
[0.3.0]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.2.4...ankusa-v0.3.0
[0.2.4]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.2.1...ankusa-v0.2.4
[0.2.1]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.2.0...ankusa-v0.2.1
[0.2.0]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.1.0...ankusa-v0.2.0
[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/ankusa-v0.1.0
