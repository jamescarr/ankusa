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

- **Message size preflight and forwarded-header caps (DEL-5).** A queue sink
  that `use`s `Ankusa.Sink.Messaging` now checks every message against its
  `c:Ankusa.Sink.Messaging.max_message_bytes/1` (implemented for you: the
  `:max_message_bytes` option, default 1 MiB) before `publish/4`, in
  `Ankusa.Sink.Message.encode/4`. An inline body whose base64 message would not
  fit is checked in and published as a claim; a body that is over the limit by
  itself is `{:permanent, {:message_too_large, size, max}}`, dead-lettered at
  once with no broker call. `Message.max_bytes/1` and
  `default_max_message_bytes/0` are new. `Sink.GooglePubSub` (10 MB) overrides
  the default; `Sink.SQS` keeps `:max_message_bytes`.
- **`max_headers` (64) and `forward_headers_max_bytes` (16 KiB) on
  `Ankusa.Source`.** `Ankusa.Sink.Message.forwarded_headers/2` now takes a
  sink `ctx` (or a bare mode, as before) and drops a header past either cap,
  whole, emitting `[:ankusa, :sink, :headers_dropped]`
  (`ankusa_sink_headers_dropped_total{reason="count" | "bytes"}`). `Sink.Http`
  is capped too. Sources that forward more than 64 headers, or more than 16 KiB
  of them, lose the excess: raise the two keys to keep them.
- **`Ankusa.Sink.Messaging` and `Ankusa.Sink.Connections`: the shared shape of
  a queue-style sink.** `use Ankusa.Sink.Messaging` and implement `publish/4`:
  the injected `deliver/3` encodes the hook as `Ankusa.Sink.Message` (inline,
  or its claim) and hands the payload over, and `inline_max_bytes/1` is
  implemented for you; `resolve/2`/`static/1` read a "string or function of
  the hook" option, `endpoints/1` a broker list (`"host:port"`,
  `{host, port}`, or one comma-separated string).
  `Ankusa.Sink.Connections.start_link/1` starts an adapter's
  `<sink>.Supervisor` + `<sink>.Reaper` tree under `<sink>.TopSup`, and
  `ensure/5` finds or starts a connection under it and touches it in the
  reaper. `Ankusa.Sink.Message` gains `headers/2` (the `ankusa_*` transport
  headers, tenant always present), `delivery_id/2` (`id`, or
  `id:replay:<replay_id>`) and `fits/2` (the
  `{:permanent, {:message_too_large, size, max}}` gate);
  `Ankusa.HttpClient.authority/1` is public. `mise run new:adapter` scaffolds
  the new shape. Docs: `packaging.md#adding-a-new-adapter-package`.
- **Claim gateway integrity check, and gateway, sweep and archive metrics
  (CLM-1).** `claim_check.integrity_check` (`:manifest`, the default, or
  `:off`): the gateway holds every whole-claim `GET` to the sha256 its pack's
  manifest records, answers `502 integrity_mismatch` rather than send bytes
  that differ (or from a pack whose manifest does not parse or does not list
  its claims), and sends the digest on `200` and `206` as `x-ankusa-sha256`.
  The manifest is read once per pack (one ranged read at the end of the last
  claim), coalesced by `Ankusa.ClaimCheck.Reader` like any read
  (`[:ankusa, :claim_check, :read]` gains `kind: :manifest` and
  `result: :integrity_mismatch`) and kept in the pack's `Ankusa.ClaimCheck.Cache`
  entry, so it counts against `read_cache_bytes` (32 bytes a claim) and ages with
  the pack's index. `Ankusa.ClaimCheck.digest/3` is new; the location
  `locate/4` returns gains `:claims` and `:claims_end`; `Pack.count/1`,
  `manifest_header_bytes/0`, `manifest_size/1`, `digests/3` and `digest/2` are new.
  Metrics: `ankusa_claim_gateway_requests_total{status}` and
  `ankusa_claim_gateway_duration_seconds` (these replace the unreleased
  `ankusa_claim_check_gateway_*`: status is the only label, no method),
  `ankusa_claim_sweeps_total{deleted}`, `ankusa_archive_failures_total` (one per
  compactor tick that could not write a segment) and
  `ankusa_archive_oldest_unarchived_age_seconds` (from the batch the compactor
  already reads, no scan). Telemetry: `[:ankusa, :archive, :failure]`,
  `[:ankusa, :archive, :state]` (`:oldest_unarchived_age_ms`), and `:deleted`
  metadata on `[:ankusa, :claim_check, :sweep]`. See
  [`claim-check.md#integrity`](../../docs/claim-check.md#integrity).
- **Pipelined WAL sync: the fsync is off the writer's critical path (DUR-4.b).**
  `Ankusa.Queue.Writer` applies a group with `sync: false` and hands its callers'
  replies to the new `Ankusa.Queue.Syncer`, which runs one `Store.sync_wal/1`
  (RocksDB `SyncWAL()`) for every group applied so far and then answers them, so
  group N+1 is assembled and applied while group N syncs. Nothing external relies
  on a group before its fsync: dispatch does not scan its rows
  (`Syncer.visible/2`), the archive stops at the durable seq, and a retry's `201
  duplicate` waits for the fsync of the original; a failed fsync answers every
  group it covers with the error, reopens the store like a failed write, and is
  retried. Under `wal: :dual` the local half's confirm is the syncer's. New
  `[:ankusa, :wal_sync]` span and `ankusa_wal_sync_duration_seconds`;
  `[:ankusa, :commit]` now measures the write of a group into the store. ADR 0002
  amendment 17 states the invariant and the crash order.

- **Segment format header, safe index decode, dated archive layout, time-based roll
  (DUR-8).** A segment is now `"ANKSEG" <> <<version, codec_id, count::32>>`, the
  codec's frames and a sha256 trailer (`Ankusa.Storage.Segment`); its `.idx` is
  `"ANKIDX"` + version + count + `term_to_binary` + sha256 and is decoded with
  `binary_to_term(_, [:safe])` and a shape check (`Ankusa.Storage.Index`), so a
  tampered or hostile index or segment is `{:error, _}` (`Storage.fetch/2` answers
  `:error`, archive replay logs and skips it). Segments go under
  `archive/v1/dt=<yyyy-mm-dd>/<node>/seg-<first>-<last>.seg` (+ `.idx` and the
  node-day `_manifest.json`; `<node>` is `intake.node_id`, else the host name), so a
  node without the catalogue can find another node's segments: `Storage.fetch/2`
  falls back to a day-prefix scan on a catalogue miss, and the new
  `Queue.Archive.reconcile/2` rebuilds catalogue rows from the prefix (the
  boot restore reconcile now runs through it). `storage.roll_ms` is no longer dead:
  a partial segment is written once its oldest hook has waited that long, and
  `storage.max_segments_per_tick` (16) caps what one compactor tick writes.
  `Ankusa.Codec` gains an optional `id/0` callback. Segments written by earlier
  releases (`seg/…`, bare frames) stay readable. **Store format 7** (the catalogue's
  segment rows record `format`, `codec`, `node`, `dt`, `sha256`; migration
  `SegmentRows`, fixture `store-v7.tar.zst`): a format 7 store cannot be opened by an
  earlier image, and an earlier node cannot read segments of format 1. See
  [`storage.md`](../../docs/storage.md#ankusacodec-and-segment-format).

- **Store failure telemetry, `/ready` reasons and a reopen backoff (DUR-5).**
  `[:ankusa, :store, :write_failed]` (one per failed write a process reported and
  per failed readiness probe, with a `class` from the fixed set
  `Ankusa.Store.failure_class/1` returns), `[:ankusa, :store, :reopened]` (one
  per reopen attempt: `:duration` is the WAL replay, `:unavailable_ms` the outage so
  far) and `[:ankusa, :store, :unavailable]` (`:seconds`, `0` at boot and once
  open) feed `ankusa_store_write_failures_total{class}`,
  `ankusa_store_reopen_total{result}`, `ankusa_store_unavailable_seconds` and
  `ankusa_store_wal_replay_seconds`. `/ready`'s `503` body gains `reason`
  (`no_space`, `io_error`, `read_only`, `corruption`, `busy`, `timeout`,
  `not_running`, `other`). A store that fails to reopen retries by itself after
  5 s, then twice as long each time up to 60 s, each wait jittered down by up to a
  quarter (`retry_open_ms`, `retry_open_max_ms` store options). **Changed:**
  `Ankusa.Store.ready/1` answers `{:error, {:store_unavailable | :write_failed,
  class}}` instead of `{:error, :store_unavailable | :write_failed | raw_reason}`.
  **Also:** the reopen a failed write asks for now waits until a synced write of
  a small file next to the database works (`ankusa_store_reopen_total{result="deferred"}`)
  instead of closing a database that still answers reads while the disk still
  fails. The crash drills' EIO windows fail only the file data area (journal and
  metadata stay up), their harness logs to stderr (it was silent), and they
  check `/ready`'s `reason` and the `ankusa_store_*` series.
  See `docs/storage.md#durability-corruption-full-disk`.
- **Object-store adapters: conditional puts, jittered retries, checksums, multipart,
  pools, credential refresh (DUR-7).** `Ankusa.BlobStore.put_if_absent/4` (a new
  callback every store implements; S3 and Azure `If-None-Match: *`, GCS
  `ifGenerationMatch=0`, OCI `if-none-match: *`, `LocalFS` a file check) answers
  `{:error, :already_exists}` and leaves the original alone; claim packs are written
  with it, and `ClaimCheck.check_in/4` writes a refused pack again under a fresh pack
  id (3 attempts, `[:ankusa, :claim_check, :pack_collision]`). **One absolute deadline
  per blob operation** (`op_deadline_ms`, 120 s) covers every attempt, wait, multipart
  part, pack-id retry and store tried. Retries use full jitter, honour `Retry-After`
  when it fits the deadline and otherwise return `{:error, {:throttled, seconds}}` at
  once (claim check-ins surface it as `{:unavailable, {:throttled, seconds}}`); a
  breaker per `{endpoint, bucket}` (`breaker_failures`, `breaker_open_ms`,
  `breaker_slow_ms`: it also opens on p99 latency) answers `{:error, :breaker_open}`
  (`[:ankusa, :blob_store, :breaker]`). `upload_timeout_ms` (60 s) governs a request
  that carries over 1 MiB, and a body over `multipart_threshold_bytes` (8 MiB) goes up
  as an S3/OCI multipart upload, an Azure block list or a GCS resumable upload in
  `multipart_part_bytes` pieces. Every `PUT` carries a digest (`x-amz-checksum-sha256`,
  `x-goog-hash`, `Content-MD5`) and a `GET` verifies the one the store returns:
  `{:error, :integrity}`. `pool_size`, `pool_count` and `pool_timeout` size a Finch
  pool per endpoint that the instance starts (`Ankusa.BlobStore.Pool`), instead of
  Req's shared default. New token providers `Ankusa.BlobStore.GCS.MetadataServer` and
  `Ankusa.BlobStore.OCI.InstancePrincipal` (`key_provider:`), and on a `401` every
  adapter drops the provider's cached token (`invalidate/n`) and retries once with a
  fresh one. All options are range-checked at boot. Custom `Ankusa.BlobStore`
  implementations must add `put_if_absent/4`. See
  [`storage.md`](../../docs/storage.md#conditional-writes-retries-checksums-multipart-pools).
- **Cross-partition group commit (DUR-4).** `Ankusa.Queue.Writer` takes every batch
  the batcher partitions have queued and commits them as one group: one synced
  store write (and, under `wal: :dual`, one intake publish), seqs dense across it,
  every caller replied to with its own hooks. The work that needs no seq moved out
  of the writer into the batcher's commit task: the hook's binary, its encoded
  delivery rows, and a prefetch read of its dedupe keys. The writer re-validates
  that read before it assigns seqs (the first occurrence of a key in a group wins,
  a key committed since the caller read it is answered from what the writer wrote,
  and under `dual` the pending set is consulted), so the same provider key posted
  to two partitions at once still stores one hook. New keys
  `batcher.group_commit_max_bytes` (default 4 MiB: every caller waits for its whole
  group, so the cap bounds the ack latency a backlog adds) and
  `batcher.group_commit_max_records` (1 024); a partition now keeps up to
  `batcher.max_commits` (64) commits in flight instead of one, so the writer finds
  every waiting caller in its mailbox, and a saturated writer waits
  `batcher.group_commit_delay_ms` (1) for the callers it just answered to join the
  next group (never at low load); `batcher.partitions` now defaults to 4. A
  delivery row's `at` is the
  time the hook was received rather than the
  writer's stamp (the due key keeps the writer's). `bench/store_commit.exs`
  measures commits/s and ack p99 at 1, 5 and 10 ms of fsync, and a one-second
  stall's drain. See `docs/storage.md`.

- **`queue.rocksdb`: the store's RocksDB settings are config, and `index` and
  `deliveries` carry a bloom filter (DUR-3).** `block_cache_bytes` (64 MiB),
  `bloom_bits_per_key` (10; 0 = off), `max_open_files` (65 536), `compaction_style`
  (`:level` | `:universal`), `rate_limiter_bytes_per_sec` (0 = unlimited),
  `max_background_jobs` (4) and `write_buffer_bytes` per column family, each
  range-checked at `Ankusa.Config.new/1` and read back from the `OPTIONS-*` file in
  `store_test.exs`. The defaults add a whole-key bloom filter with
  `cache_index_and_filter_blocks` on the `index` and `deliveries` families, so a
  first-seen dedupe key is one filter probe per file instead of an index and data
  block read; there is no prefix extractor, because `index` also serves ordered
  scans. Filters exist only in files written after the upgrade (compaction brings
  the rest); the store format is unchanged and no migration runs. The index and
  filter blocks now live in the block cache: size `block_cache_bytes` for a store
  with tens of millions of dedupe keys. `bench/dedupe_bench.exs` measures a miss
  and a commit against a store of `KEYS` dedupe keys. See
  `docs/storage.md#rocksdb-tuning-and-the-index-filters`.
- **Claim leases, and a bounded claim sweeper (DUR-2.c).** The `LocalFS` claim
  retention sweeper no longer deletes a pack a hook still needs. Each hook that
  checks its body in leases the pack (`Keys.claim_lease/3`, rows `?l`, counter
  `claim_leases`, gauge `ankusa_claim_leases`) in the same batch; the lease is held
  while the hook is live, a dead letter included, and when the hook is reclaimed it
  runs on for `claim_check.redeem_grace_ms` (default 24 h) so the consumer of a
  queue message that carries the ref can still redeem it. `Ankusa.ClaimCheck.Sweeper`
  now runs on `:dispatch` nodes too, lists each expired `dt=` partition and deletes
  only unleased packs, at most `claim_check.sweep_max_objects` (default 10 000) a
  tick (`ankusa_claim_check_sweep_backlog`, `..._sweep_partitions_left`,
  `ankusa_claim_check_pruned_total`, `ankusa_claim_check_leases_pruned_total`), and
  deletes nothing when it cannot read the leases. Object-store claims have no lease:
  declare the bucket lifecycle rule's age in `claim_check.bucket_expiry_days` and
  boot refuses one shorter than the retry horizon (21 days) +
  `queue.dlq.retention_days` + the redeem grace. **Store format 6**: the migration
  `5 -> 6` leases the claim of every live hook, resumably;
  `test/fixtures/store-v6.tar.zst`. A node running format 6 cannot be rolled back to
  an image that writes format 5. See `docs/claim-check.md#claim-leases`.

- **`wal: :dual`: the ack waits for a replicated intake log too (DUR-1.b.a,
  [ADR 0002](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0002-dual-commit-intake-log.md)).** The core of the
  dual commit: the `Ankusa.Intake` behaviour and `Ankusa.Intake.Memory` (a
  file-backed test double with fault injection), `Queue.Writer` running the synced
  local write and the confirmed publish **in parallel** (both required for the
  `201`; the reply is a message when the publish task answers, so batches
  pipeline), a bounded pending set, the breaker (`Intake.Gate`), the live
  `Intake.Reconciler` that **carries** a half-persisted hook to completion (the
  fleet dedupe claim is held, then promoted; a retry that meets an unconfirmed
  original is `503 intake_pending`, never `201`), boot recovery
  (`Intake.Recovery`: identity, replay from `m:intake_offset` through the
  seq-and-id rule, republish of the local tail; `mix ankusa.recover`),
  `Health.ready` `intake: up | down | recovering`, `GET /v1/wal` `intake`, the
  `ankusa_intake_*` series, `503 intake_unavailable` / `intake_pending`, and
  every `intake.*` key with its range. New meta rows only (`m:intake_seq`,
  `m:intake_node`, `m:intake_partition`); the store format is unchanged. The
  Kafka adapter, the YAML keys, the fleet drill and the architecture, storage and
  deployment docs are DUR-1.b.b. `bench/intake_bench.exs` is part of `mise run
  bench:gate`.
- **Tenant erase (TEN-5).** `DELETE /v1/tenants/{tenant}?confirm={tenant}`
  (new scope `tenants:write`, which also needs `tenant:*`) and
  `Ankusa.Tenant.delete/3` erase a tenant as a resumable background job
  (`GET /v1/tenants/{tenant}/erase` reports it): the tenant is closed first
  (`503 tenant_erasing`, dispatch stops claiming its rows, running attempts
  drain), then its sources, routes, rate-limit override, pending/inflight/dead
  rows, archive obligations, quarantine rows, `claims/tenant=<t>/` objects,
  dedupe keys and lifecycle entities are deleted, and one synced batch commits
  the tombstone `m:erased:<tenant>` with a `tenant.delete` audit row, which is
  the new lifecycle event `io.ankusa.tenant.deleted`. Progress is kept in
  `m:erase:<tenant>`, so a `kill -9` resumes the job at the next boot.
  `Ankusa.Storage.fetch/2` answers `{:error, :gone}` for an erased tenant's
  archived hooks and archive and lifecycle replay skip them.
  `Queue.purge_tenant/3` accepts `:inflight`. Configured (static or seeded)
  sources naming the tenant, and `default`, are refused with `409`.
  Proof: `test/ankusa/tenant_test.exs`, `tenant_s3_integration_test.exs`.
- **DLQ, quarantine and catalogue retention; audit and lifecycle-outbox ceilings
  (DUR-2.b).** `Ankusa.Queue.DlqReaper` evicts dead rows past
  `queue.dlq.retention_days` and over `max_rows`/`max_bytes`, only while their hook
  is archived and findable; otherwise ingest answers `503 dlq_full`.
  `DELETE /v1/dlq` evicts on demand. `quarantine.retention_days`,
  `storage.catalogue_retention_days` and `archive.retention_days` bound the pen and
  the archive catalogue; `audit.max_rows`, `audit.max_bytes` and
  `audit.outbox_max_backlog` bound the trail and refuse new changes with
  `503 audit_backlog`. Store format 5 adds the `dead_bytes` counter (migration
  `4 -> 5`, fixture `store-v5`): a node running format 5 cannot be rolled back to an
  image that writes format 4. See `docs/storage.md`.

- **Dedupe ceilings, the dedupe sweeper and audit retention behind the outbox
  (DUR-2.a).** `dedupe.max_entries` (default 100 000 000) and
  `dedupe.max_entries_per_tenant` (default 30 000 000) bound the live ingest-dedupe
  keys: a hook whose key would be a new one past either is refused with
  `503 {"error":"dedupe_quota"}` and `Retry-After: 60` (`outcome="dedupe_quota"` on
  `ankusa_ingest_requests_total`), never stored without its key. The count is the new
  `dedupe` counter (field 7 of the `counters` family, so the check is one read per
  commit batch), and the expiry sweep moved out of `Ankusa.Queue.Writer` into the
  supervised `Ankusa.Queue.DedupeSweeper`, which runs back to back while full
  chunks of expired keys keep coming (the old sweep was capped at 2 000 keys per
  10 s) and has the writer apply each chunk, deleting a key only if a refresh did
  not replace it. **Store format 4**: the migration `3 -> 4` counts the `?e` keys
  of a format 3 store, resumably; `test/fixtures/store-v4.tar.zst`. A format 3
  release cannot open a format 4 store (no downgrade across this change). New
  series `ankusa_dedupe_keys`, `ankusa_dedupe_pruned_total`. The audit trail's
  retention (`audit.retention_days`) no longer deletes a row, or its lifecycle
  entity, at or above `m:lifecycle_cursor` while `lifecycle.sinks` is set, so a
  lifecycle sink that is down longer than the retention cannot lose events it has
  not seen; `ankusa_audit_retention_blocked_by_cursor` reports the expired rows
  held back. See `docs/storage.md#ingest-dedupe-keys-bounded-expired-outside-the-writer`.
- **Backup verification, signed manifests, a final backup on stop and a secondary
  region (DUR-6).** `Ankusa.Store.Backup.verify/2` reads the latest backup back
  (manifest, every file's size and sha256) without writing, on
  `backup.verify_interval_ms`, counted by `ankusa_backup_verify_failed_total{target}`.
  `backup.signing_key` (a string or a list, each at least 32 bytes) signs every
  manifest with HMAC-SHA256 (manifest version 2; version 1 is still read) and a
  restore refuses an unsigned or mis-signed one. A clean stop takes one last
  backup under `backup.final_timeout_ms` (20 s). `backup.timeout_ms` bounds each
  object-store call. `backup.max_age_ms` puts `"backup": "fresh" | "stale"` on
  `/ready`, `503` only with `backup.require_fresh`. `backup.secondary` is a second
  object store every backup is also written to, and restore falls back to. The
  manifest records the store format (a backup of a newer one is refused at
  restore) and `m:intake_offset`. `Backup.restore/2` is now `restore/3`.
- **A restore is announced (DUR-6).** A store restored from a backup writes a
  fresh `m:incarnation` and, before taking any change, emits
  `io.ankusa.instance.restored` (`incarnation`, `restored_sequence`); lifecycle
  event ids of rows from then on are keyed with the incarnation, so audit
  sequence numbers the lost node had already published are not reused as event
  ids. Rows of the restored trail keep their ids.
- **Routes at 10⁶ entries (TEN-9).** The route snapshot is indexed by the
  literal segments a pattern starts with (`{:lit, gen, prefix, priority, id}`,
  up to eight), so a request is matched against the few routes that share its
  leading literals, merged in priority order, instead of every pattern in the
  table: a miss with the decision cache out of the picture is microseconds at a
  million routes, with the same answers as the old walk (the new
  `routes/snapshot_test.exs` checks it against the old walk on random tables).
  Enabled routes are also indexed by `{compiled pattern, method}`, so the
  create/replace/update conflict check is one lookup per method, not a scan of
  every pattern; `Routes.list/2` pages from its cursor with `:ets.next/2`
  instead of sorting the table; publishing a generation inserts in batches of
  2 000 routes and no longer sorts. New `routes.memory_budget_bytes` (default
  8 GiB): at boot `max_routes` is multiplied by an estimate of what a route
  costs (`Snapshot.estimated_bytes/1`) and a cap that does not fit is an
  `ArgumentError` naming both keys; a store refuses a load that does not fit
  (`Snapshot.check_budget/2`). `bench/routes_bench.exs` (10⁶ routes over 10⁵
  paths by default; `mise run bench:gate` runs it at 10⁵) asserts boot and
  reload time, miss p99 < 1 ms, create/list flat in the table size and bytes
  per route against the estimate. The same prefix index backs
  `Snapshot.overlaps/4` (every route whose pattern shares a request path with
  a candidate, each carrying its `tenant_id`; one prefix chain, not the table)
  and `Matcher.overlap?/2`: the query a cross-tenant path-overlap check
  (TEN-4.b) needs. Nothing calls it yet.
- **Per-tenant observability with a cardinality policy (TEN-7).** `metrics.detail`
  (`:tenant` by default, `:source`, `:none`) says which identity labels the
  per-hook Prometheus series carry, and `metrics.max_series_per_metric` (default
  `5000`, a positive integer) caps how many label sets one metric keeps: the first
  `max_series_per_metric - 1` are admitted, every later one is counted under
  `tenant_id`/`source_id` `"other"` (`Ankusa.Metrics.Cardinality`, an ETS
  admission in the emitting process: one `:ets.member/2` for a known label set).
  The ingest, verify-failure, quarantine, rate-limit and delivery counters now
  carry `tenant_id`; `source_id` only under `:source`, which also puts both on
  the ingest duration histogram; `:none` carries neither and drops the two
  per-tenant gauges, so the scrape does not grow with the tenant count. A gauge
  past the ceiling drops the tenant instead of writing to `"other"`. The
  `[:ankusa, :ingest]`, `[:verify]`, `[:quarantine, *]` and `[:dispatch, :stop |
  :dlq | :breaker]` events carry `:tenant_id` metadata. `Ankusa.Metrics` is now a
  supervisor (the policy's table and the reporter restart together).
  **Behaviour change:** `source_id` is no longer a label by default (it was on the
  ingest, verify-failure, weak-hash, quarantine and dead-letter series, and the
  ingest duration histogram), and `Ankusa.Edge.Quarantine.hit` takes the tenant
  too (`instance, source_id, tenant_id`). See `docs/configuration.md#metric-cardinality`;
  tests in `metrics_test.exs`, `config_test.exs`.
- **Per-tenant HTTP destinations on shared sources (TEN-8).** `Sink.Http`'s
  `:url`, `:headers` and `:secret` accept a one-arity function of the
  `%Envelope{}` or a `{mod, fun, args}`, resolved on every attempt, so one
  shared source delivers each tenant's hooks to that tenant's receiver. The
  resolved URL goes through the egress policy on every attempt (a denied or
  rebinding target fails closed as `{:permanent, {:egress_denied, _}}`), a
  secret that resolves to nothing is `{:permanent, :unsigned_sink}` unless
  `allow_unsigned: true`, and a value of the wrong shape is `{:permanent,
  {:computed, key, :invalid}}`. `validate_opts!/1` checks the shapes at load.
  `Sink.Http.describe/2` reports `computed: [...]` and `payload: :body` in the
  new `Sink.Description` fields of those names (`payload: :message`, the
  default, is what the AsyncAPI document keeps; `host` may now be `nil`).
  Computed URLs connect through `Ankusa.Sink.Http.Pool`, one bounded Finch per
  instance started by `Ankusa.Instance` (4 connections per origin, an origin's
  pool closed after 30 s idle, new origins refused with
  `{:error, :pool_origins_exhausted}` past 4,096, gauge
  `ankusa_sink_http_pool_origins`), instead of Req's pool per origin that is
  never closed; owned pools for every outbound client are DEL-3, which replaces
  it. A computed `:url` cannot also set `:cacertfile` or `:allow_insecure_tls`.
  `finch` is now a declared dependency (already locked through Req).
  `HttpClient` honours `req_options: [finch: [name: pool]]`, which raised
  before (Req refuses `:finch` beside the `:connect_options` it always set); the
  pool then owns the connection options, and combining it with `:connect_options`,
  `:cacertfile` or `allow_insecure_tls` raises `ArgumentError`.
- **Fleet-scoped rate limits and dedupe (TEN-6).** `rate_limits.scope` and the new
  `dedupe.scope` take `:node` (the default, unchanged) or `:fleet`: the tenant quota
  (hooks and bytes per second) and the provider event key are then counted in
  `fleet.store`, a module implementing the new `Ankusa.RateLimiter.Store` and
  `Ankusa.Dedupe.Store` behaviours (`Ankusa.RateLimiter.Store.Redis` in
  `ankusa_redis`), so N edge nodes enforce one limit and a provider retry on
  another node is the first hook, not a second. `Ankusa.Edge.Fleet` is the request
  path's one entry (`Ingest` calls it in place of `RateLimiter.hit/3`): one store
  call per hook carries the tenant's charges and the dedupe claim. The claim is
  two-phase: *pending* before the commit, *committed* (with the hook's id) after
  it, off the request path in batches; a commit that fails releases it, a key
  another node holds pending is `409 dedupe_pending` with `Retry-After`
  (`outcome` `:dedupe_pending`), one it committed is `201` with the first hook's
  `id` and `"duplicate": true`. A limiter outage never refuses a hook: every call
  has a hard budget (`fleet.timeout_ms`, 1–1000, default 20), a call that fails or
  times out is answered from the node's own limiter and dedupe index, and a breaker
  (`fleet.breaker_*`) opens on the failure *rate* and probes half-open. New
  telemetry and metrics: `[:ankusa, :fleet, :degraded | :breaker | :fallback |
  :settle_dropped]`, `ankusa_fleet_limiter_degraded{kind}`,
  `ankusa_fleet_breaker_transitions_total`, `ankusa_fleet_fallback_total`,
  `ankusa_fleet_settle_dropped_total`. `per_ip`, `node` and `sources` stay per node.
  New config: `fleet` (`store`, `timeout_ms`, `pending_ttl_ms`, `breaker_window_ms`,
  `breaker_min_calls`, `breaker_failure_rate`, `breaker_open_ms`),
  `dedupe.scope`, `rate_limits.scope`; `RateLimiter.fleet_charges/3`. Tests:
  `edge_fleet_test.exs`, `config_test.exs`.
- **Dispatch fairness by tenant (TEN-3).** A scan claims from the tenants with
  work due by deficit round-robin (an equal share of the page each, a tenant
  with less due leaves the rest to the others, a tenant the page did not reach
  goes first in the next scan), and every tenant has its own window beside the
  global one: `dispatch.tenants.default` and `dispatch.tenants["acme"]` set
  `max_inflight` (default 1024), `max_inflight_bytes` (32 MiB) and
  `concurrency` (16), so a tenant whose sink is slow fills its own window and
  nobody else's. Slots go round-robin across tenants, then across a tenant's
  keys. The sink key is now `{tenant_id, source_id, sink index, module}`:
  circuit breakers and `dispatch.sink_concurrency` are per tenant. New gauge
  `ankusa_dispatch_tenant_inflight{tenant}` (claimed rows, the 100 tenants
  holding the most). `Deliveries.next_due_at/3` takes `skip:`, the tenants at
  their window. Proof: `tenant fairness` in `test/ankusa/dispatch/isolation_test.exs`
  and `bench/tenant_fairness_bench.exs`.

- **Tenant key layout, per-tenant counters and the active-tenant index
  (TEN-1.b, [ADR 0001](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0001-tenant-key-layout.md)).** Store
  format 3. Every tenant gets a dense ordinal (`tenants` column family, assigned
  once at its first committed hook, never reused); due, claimed and dead rows
  lead their keys with it (`?D`, `?F`, `?X` beside the global `?x`), so one
  tenant's rows are one contiguous range. Six counters per tenant and for the
  node (`pending`, `scheduled`, `inflight`, `dead`, `bytes`, `archive_pending`;
  `pending` is now first-attempt rows and `scheduled` retry rows, their sum
  unchanged) live in a `counters` column family written only as
  `erlang_merge_operator` `int_add` operands in the same synced batch as the
  transition, so counts and rows agree after a `kill -9`. An active-tenant index
  (`?A`, one marker per tenant per second with a due row, cleaned by
  delete-then-verify) makes a dispatch scan cost the tenants with work due, not
  all tenants; the scan keeps a cursor per tenant. New: `Queue.pending_for_tenant/2`
  and `dead_for_tenant/2` (constant time), `Queue.purge_tenant/3` (synced chunks
  of 500 rows, resumable by calling it again), `tenants.max` (default 1 000 000;
  a hook of a tenant past it is `503 tenant_limit` with `Retry-After: 300`), the
  series `ankusa_tenants`, `ankusa_dispatch_active_tenants` and
  `ankusa_tenant_pending{tenant}` (the 100 tenants with the most pending). The
  queue gauges read counters instead of folding the index, so a gauge tick costs
  the same at 10³ and 10⁶ queued rows and at 10 and 10⁵ tenants
  (`bench/gauges_bench.exs`, `bench/dispatch_tick_bench.exs`, run by `mise run
  bench:gate`); a tenant-limited `GET /v1/dlq` is one range scan of that tenant's
  rows instead of a hook read per dead row. The v2 → v3 migration re-keys the
  rows in bounded, resumable batches and seeds the counters from them; a release
  that writes format 2 refuses a format 3 store, so rolling back past this
  release needs a backup taken before the upgrade
  ([Upgrading between releases](../../docs/deployment.md#upgrading-between-releases)).
  `mix ankusa.store.fixture` writes the new layout (`store-v3.tar.zst`).

- **Admission at the edge: per tenant, per source, per address, per node
  (TEN-2).** `rate_limits` gains `per_ip` and `node` (`%{rate, burst}`, hooks
  per second per client address and for the whole node; off by default) and
  `sources.default.bytes_per_second` (body bytes per second per source, 100 MB/s
  unless changed, `default: nil` is off). They run in `Ankusa.Edge.Router`
  after the source lookup and **before the body is read**, so a forged flood
  costs neither a read nor an HMAC: `429 ip_rate_limited`, `429
  node_rate_limited`, `429 source_rate_limited`, each with `Retry-After` and
  counted on `ankusa_ingest_refused_total{reason}`. The client address is
  `Ankusa.Net.ClientIP`'s (`routes.trusted_proxies` decides whose
  `X-Forwarded-For` counts), an IPv6 address is its `/64`, and addresses hash
  into 2^20 buckets, so the table has a ceiling. The declared `Content-Length`
  is charged up front; a body with none is charged once it has been read.
  A tenant's limit (`rate_limits.tenants[t]` and `rate_limits.default`, and a
  runtime override) may now also carry `bytes_per_second` and `burst_bytes`
  (body bytes, charged with the hook after verification), `max_body_bytes`
  (`413`, checked against the declared length before the read and enforced on
  the bytes received; at most the instance's) and `max_queue` /
  `max_queue_bytes`, the hooks and body bytes the tenant may hold in this
  node's batchers at once. `Ankusa.Edge.Batcher.commit/4` counts the share in
  ETS (`Ankusa.Edge.RateLimiter.acquire_queue/3`, released when the call
  returns or exits, so a crashed batcher leaks nothing) and answers
  `{:error, {:tenant_overload, retry_after_ms}}`, a `429 tenant_overload`,
  never the global `503 overload`. New `Ankusa.Edge.RateLimiter` functions:
  `pre_read/4`, `hit_ip/2`, `hit_node/1`, `hit_source_bytes/3`,
  `body_limit/2`, `acquire_queue/3`, `release_queue/2`, and `hit/3`
  (`hit/2` is unchanged). The admin API's rate-limit routes accept and return
  the new keys (only when set), and `ingest.v1.yaml` / `admin.v1.yaml`
  describe them. New refusal reasons `:ip_rate_limited`, `:node_rate_limited`,
  `:source_rate_limited`; new ingest outcome `:tenant_overload`. Tested in
  `edge_rate_limit_test.exs` and `config_test.exs`.
- **Tenant-aware routes bound to sources (TEN-4).** A route now has a
  `tenant_id` (default `"default"`, the shared tenant) and an optional
  `source_id`, in `Route.from_attrs/2`, `to_json/1`, `from_json/1`, the
  `routes.seed` entries and the admin API, and the route guard hands the route
  it matched to ingest instead of discarding it: `conn.assigns.ankusa_route` is
  now the `Ankusa.Routes.Route` (it was its id), `Routes.capture/5` is
  `authorize_path/5` answering with the route, and `Ankusa.Edge.Ingest.lookup/3`
  holds the route's binding against the resolver. A route owned by a tenant
  admits hooks only into that tenant (another tenant in the URL, or a source
  owned by another tenant, is the `404` an unknown source gets) and, matched
  on a shared source that the URL names no tenant for, files the hook under its
  own; a route bound to a `source_id` admits only that source. The ingest span
  carries `:route_id`. Route lifecycle events and audit rows now carry the
  route's tenant instead of `"default"`, so a replay or an audit read filtered
  to a tenant sees its own routes. The route-management listener gains a tenant
  surface, `GET|POST /v1/tenants/:tenant/routes` and
  `GET|PUT|PATCH|DELETE /v1/tenants/:tenant/routes/:id`, that needs
  `tenant:<tenant>` (not `tenant:*`) and shows and changes only that tenant's
  routes, read from a `{tenant, id}` index in the snapshot table
  (`Snapshot.tenant_routes/4`); another tenant's route is `404`, a body naming
  another tenant is `400 invalid_route`, and an id or path another tenant holds is
  a `409 duplicate_route` that does not name the holder. `/admin/routes` stays
  the all-tenants operator surface (`tenant:*`), so the SDKs' routes clients are
  unchanged. `tenant_id` and `source_id` are immutable under `PATCH`.
  **Breaking for a custom `Ankusa.Routes.Store`:** `delete/2` is now
  `delete/3`, taking the tenant the delete is made for (`nil`: any owner), which
  the store checks in the delete itself. `Route.to_stored_json/1` leaves out a
  default `tenant_id` and an absent `source_id`, so a node from before them still
  reads a Redis table with no tenant routes.
- **Verifier breadth: public-key, token and client-certificate (SEC-12).** A
  provider that signs with Ed25519 or RSA, sends a static credential, or
  authenticates by mutual TLS no longer has to be a `Verifier.None` source with
  `accept_flag`. `Ankusa.Verifier.Asymmetric` verifies an Ed25519 (RFC 8032) or
  RSASSA-PKCS1-v1_5/SHA-256 signature against the provider's public key (`whpk_`
  base64, bare base64, or a PEM `PUBLIC KEY`/`RSA PUBLIC KEY`; a list for
  rotation), over the same wire templates as `Hmac`, with the preset
  `:standard_webhooks_asym` (`v1a,` entries) and a replay window; it holds no
  secret, and a signature of the wrong length is dropped before any public-key
  operation, at most 8 being tried per request. `Ankusa.Verifier.Token` accepts
  a bearer, basic, header or query credential (rotation list, comparison over
  SHA-256 digests with `secure_compare/2`) and keeps it out of everything
  stored: its header is added to the source's `store_headers` drop list.
  `Ankusa.Verifier.ClientCert` accepts a request whose mutual-TLS client
  certificate matches a pinned SHA-256 fingerprint or a SAN/CN identity; the
  edge fetches the certificate from the socket only for such a source and
  passes it, and the query string, through the verifier's `opts` (the envelope
  and the stored format are unchanged). The shared half of `Hmac` moved to
  `Ankusa.Verifier.Template` (`candidates/2`, `timestamp/4`, `signed_bytes/3`),
  and `Ankusa.Verifier.Schemes` validates `%Asymmetric.Scheme{}` too. A new
  optional `Ankusa.Verifier` callback, `validate_opts/1`, is run at boot by
  `validate_config!/1` for every verifier that has one, which now refuses a
  token secret, public key or pin it cannot use, and a `ClientCert` source on a
  listener without `tls.verify: :verify_peer`, naming the source. New failure
  reasons `:bad_key` and `:bad_pin`. `Redact` shows `location`, `fingerprints`,
  `subjects`, `param` and `algorithm`, never a secret or a public key. See
  [`docs/configuration.md`](../../docs/configuration.md#public-key-token-and-client-certificate-verifiers).

- **Claim gateway authorization and signed, expiring refs (SEC-11).** The
  gateway's built-in check (`claims:read` plus `tenant:<t>`/`tenant:*`, a
  tenant-A credential on a tenant-B path is `403`) can be replaced with
  `claim_check.authorize: {mod, fun, args}`, called before anything is read
  with `%{principal, tenant_id, claim_id, conn}`. New **v2 refs**,
  `urn:ankusa:claim:v2:<tenant>:<claim_id>:<exp_unix>:<base64url hmac-sha256>`,
  are minted when `claim_check.ref_signing_keys` (`[current | previous]`, each
  at least 16 bytes, shown as `[REDACTED]` by `/v1/config`) is set, expire
  after `claim_check.ref_ttl_ms` (default: the retention, else 30 days), and
  redeem at `GET /v1/claims/<t>/<c>?exp=&sig=`: a signature no key produced is
  `400 bad_signature`, a good one past its expiry `410 ref_expired`, and a v1
  ref `400 unsigned_ref` once keys are set (unless
  `claim_check.accept_unsigned_refs: true`; `redeem/3` applies the same rule,
  `Ankusa.ClaimCheck.verify_ref/2`). `Ankusa.ClaimCheck.Ref` parses both
  grammars and has `sign/3`, `verify/3` and `signed/4`. With no keys nothing
  changes: refs stay v1 and are served. `claim_check.max_concurrent_reads`
  (default 1024, `0` = no cap) sheds a node's excess requests as `503
  overloaded` + `Retry-After` before any read. The OpenAPI contract and the
  AsyncAPI `claim` pattern describe v2; conformance has the `claim_ref.v2` and
  `claim_check.redeem.signed` cases. A pre-SEC-11 node reads a v2 ref as
  invalid: roll the gateways and the SDK readers before setting keys on the
  writers.
- **`Ankusa.SecretProvider` and `Ankusa.SourceStore.Static.publish/2` (SEC-9).**
  The behaviour (`fetch(ref, opts)` returning `{:ok, binary} | {:error,
  :not_found | :unavailable}`) a secret manager implements; the `ankusa_server`
  image resolves `${sm:<provider>:<ref>}` through it. `publish/2` replaces the
  sources a running instance with the static source store serves, after the
  checks it boots under (durability under `wal: :none`, shared sources without a
  verifier, verifier options, egress), and returns the ids that changed; a
  refused spec applies nothing. Reads see the new spec on their next
  `fetch/2`.
- **Durable lifecycle outbox with revisions (SEC-2.b).** Lifecycle events are
  no longer queued in memory: the audit row of a source or route change is
  flagged `publish: true` and committed, with the redacted entity beside it
  (`<<?o, seq>>`; an audit row still never holds a spec), in the same synced
  batch as the change. `Ankusa.Lifecycle.Publisher` is a cursor reader over
  those rows (`m:lifecycle_cursor`, synced on every advance; a first boot
  starts at the end of the trail), so a `kill -9` right after
  `SourceStore.put/5` returns still delivers `source.created` after the
  restart, and events arrive in commit order. Events carry `revision`, `actor`,
  `instance`, `sequence` and the full redacted entity on delete; the event `id`
  is derived from the row (a keyed hash laid out as a UUID, version 8) and
  `time` is the commit time, so a redelivery after a crash, or a replay, is the
  same event and a consumer dedupes on `id`. `Ankusa.Replay` has a fourth kind,
  `:lifecycle` (`since`/`until`, and the SEC-7 `tenants` filter; `POST /v1/replays
  {"kind": "lifecycle"}`) that re-emits a window of the trail, adding `replayid`. A
  pending audit row of a route or Redis-store source (a node killed mid-change)
  is settled by comparing the subject's present state with the row's
  `before_hash`/`after_hash`: applied (`ok`, announced), not applied (`failed`,
  not announced) or neither (stays `interrupted`, announces a state-refresh
  `updated`/`deleted` event with the current entity and revision).
  `Ankusa.SourceStore` gains the optional `ready?/1` callback for it. The
  `LifecycleEventV1` schema in the AsyncAPI document names the new attributes.
  **Breaking:** the in-memory queue and its `:queue_full` / `:not_running`
  drop reasons are gone (`ankusa_lifecycle_dropped_total` has `gave_up` and
  `unreadable`), `Lifecycle.source_changed/3` and `route_changed/3` and
  `Publisher.publish/4` are removed, a route deletion's `data` is the whole
  route (it was `{"id": ...}`), and a node with lifecycle sinks but no store
  runs no publisher. The store gains the `m:lifecycle_cursor` key and the `?o`
  key family in the `audit` column family: additive, no format bump; a release
  that predates them ignores both (and leaves orphaned `?o` rows when it prunes).

- **Verifier hardening (SEC-8).** `Ankusa.Verifier.Hmac` caps `:tolerance` at
  `Ankusa.Verifier.max_tolerance_seconds/0` (3600 s): `validate_opts/1` returns
  `{:error, :bad_tolerance}` for anything but an integer from 0 to 3600,
  `Ankusa.Verifier.validate_config!/1` refuses to boot a source declared in
  config with one (naming it), and `verify/2` fails every hook closed with
  `:bad_tolerance` for a stored source written before the cap. An inline scheme
  with `hash: :sha1` still verifies (some providers only sign that way) but now
  logs one boot warning naming the source, and every verification of it
  increments the new `ankusa_verifier_weak_hash_total{instance,source_id,scheme}`
  counter (`:weak_hash` on the `[:ankusa, :verify]` span metadata);
  `Ankusa.Verifier.Hmac.weak_hash?/1` is the predicate. No named scheme uses
  SHA-1.
- **Tenant-scoped admin reads and writes (SEC-7).** The DLQ, replay jobs, the
  quarantine pen and the rate-limit listing are filtered by tenant instead of
  needing `tenant:*`: `GET /v1/dlq`, `GET|DELETE /v1/quarantine`,
  `GET /v1/rate-limits`, `GET /v1/replays[/:id]` and `PATCH /v1/replays/:id`
  take `?tenant=<id>` (`POST /v1/replays` a `tenant` body key). A principal
  limited to some `tenant:<id>` scopes gets rows for those tenants only
  (forced server-side), `403` naming `tenant:<id>` when the request names
  another tenant, and `403` naming `tenant:*` when it holds no tenant; only
  `tenant:*` reads, purges or replays unfiltered. A purge deletes, and a
  replay touches, only the caller's tenants' rows; a replay job carries a
  `tenants` filter and is `404` to a limited caller it is not scoped to.
  `Ankusa.Queue.dead/2`, `Ankusa.Edge.Quarantine.recent/3`/`purge/3` and
  `Ankusa.Replay.start/3` take `:tenants`; `Replay.list/2` and `get/3` take the
  visible tenants, `Replay.update/4` the `tenants:` option;
  `Ankusa.Admin.Auth.authorize_tenants/4` is the plug-side helper. Until the
  store keys carry the tenant (TEN-1) the DLQ filter reads the hook of each
  candidate row. Route definitions and IP rules still need `tenant:*`.
- **Audit trail (SEC-2.a).** Every change to an API-managed source, a route, the
  global IP rules, a rate-limit override, the quarantine pen (`DELETE
  /v1/quarantine`) or a replay job writes one row to the new `audit` column
  family: actor (`Ankusa.Principal`), client address, request id
  (`X-Request-Id`, echoed in the response), `action`, `tenant`, `resource`,
  keyed `before_hash`/`after_hash` (never a secret or a spec), `result` and the
  subject's `revision`. Rows are in commit order (`<<?a, seq, at>>`:
  `Ankusa.Audit` allocates `seq` and commits the batch). Changes in this node's
  store (sources, rate limits, quarantine, replays) are committed in the same
  synced batch as their row; route definitions and the Redis source store are a
  dual write (a pending row, then the outcome), and a node killed in between
  records `result: "interrupted"` at the next boot. `GET /v1/audit` (scope
  `audit:read`, newest first, `since`/`until`/`actor`/`action`/`tenant`/`cursor`/
  `limit`; a tenant-scoped reader sees only its tenants' rows),
  `audit.retention_days` (365), telemetry `[:ankusa, :audit]` and
  `[:ankusa, :audit, :pruned]`. Sources and routes carry a `revision` (also in
  their API objects). `SourceStore.put/6`, `delete/4`, `Routes.*`,
  `RateLimiter.*_override`, `Quarantine.purge/4` and `Replay.start/3`/`update/4`
  take `audit: %Ankusa.Audit.Context{}`; code that passes none is `system`.
  **The store format is now 2** (`store-v2` fixture): a node that has booted it
  cannot roll back to a format 1 release without restoring a backup, and a node
  with `admin.enabled` or a writable source store now opens the store.
- **Secrets encrypted at rest, server-side encryption on every object-store
  write (SEC-4).** `Ankusa.Crypto.Envelope` encrypts the secret-bearing fields
  of an API-managed source's spec (the ones `Ankusa.Admin.Redact` hides:
  `verify.secret`, sink credentials, header values, URLs with credentials)
  with AES-256-GCM (`<<"AENC", 1, key_id_len, key_id, iv::96, tag::128,
  ciphertext>>`, base64 behind `AENC:` in the JSON) before
  `Ankusa.SourceStore.Persistent` writes the row, so neither the node's store
  files nor any backup of them hold a secret in the clear. The key comes from
  the new `secrets` config section: `kek` (32 bytes, base64, optionally
  `<id>:<base64>`) or `kek_provider: {module, fun, args}`, and `kek_previous`
  for rotation (keys that decrypt and never encrypt). A stored row no key
  opens refuses the boot instead of being skipped; rows written before a KEK
  existed still load. `Ankusa.SourceStore.Persistent.rewrap/1` and
  `mix ankusa.secrets.rewrap` (a stopped node's data dir; keys from
  `ANKUSA_KEK` and `ANKUSA_KEK_PREVIOUS`, `--dry-run`) move every row to the
  current key, in synced batches. `Ankusa.Admin.Redact.config/1` shows the
  keys as `[REDACTED]`. The 0.3 import encrypts the sources it imports. The
  startup log now says how secrets rest: `store files rely on volume
  encryption; secrets envelope-encrypted with key <id>` (or that they are
  stored in plaintext).
- `sse:` on `Ankusa.BlobStore.S3`, `GCS`, `Azure` and `OCI` (`%{type: :aes256}`
  or `%{type: :kms, key_id: key}`): server-side encryption sent on every
  `put/4` of that store, so on segments, backups and claim packs alike
  (`x-amz-server-side-encryption` and `…-aws-kms-key-id`, signed;
  `x-goog-encryption-kms-key-name` and `kmsKeyName`; `x-ms-encryption-scope`;
  `opc-sse-kms-key-id`). `Ankusa.BlobStore.validate_config!/1` checks it at
  boot for `storage.blob_store`, `backup.blob_store` and every
  `claim_check.blob_stores` entry; a misspelled option, `:kms` without the
  `key_id` the provider needs, or `sse` on `LocalFS` is a boot error, never an
  unencrypted object. See `docs/storage.md#encryption-at-rest`.
- **Authentication and scopes on every control plane (SEC-1).** The admin API,
  the route-management API and the claim-check gateway authenticate every
  request but `GET /health` / `GET /ready` (`401` with `www-authenticate`, `403`
  naming the missing scope). New `admin.auth`, `routes.admin.auth` and
  `claim_check.auth` blocks pick the `mode`: `:token` (SHA-256-hashed bearer
  tokens with scopes), `:jwt` (`Ankusa.Admin.Auth.JWT`: OIDC access tokens
  verified with `jose` against a cached, refreshed JWKS or static keys; checks
  `iss`, `aud`, `exp`, `nbf`; scopes from `scope`, tenants from
  `ankusa_tenants`), `:mtls` (`Ankusa.Admin.Auth.MTLS`: the `verify_peer`
  client certificate, mapped by `mtls.subject_map`), or `:none`. A caller is an
  `Ankusa.Principal` (`conn.assigns.principal`) with scopes (`metrics:read`,
  `config:read`, `sources:*`, `dlq:*`, `replays:*`, `quarantine:*`,
  `rate_limits:*`, `routes:*`, `claims:read`) and tenants (`tenant:<id>`,
  `tenant:*`); resources that span tenants need `tenant:*`. `jose` is a new
  core dependency. The OpenAPI documents declare the security scheme and the
  `401`/`403` responses.

- **Breaking: `mode: :none` is refused on a non-loopback address.** A running
  admin, route-management or claim-check listener bound anywhere but loopback
  now fails `Ankusa.Config.new/1` unless its `auth.mode` is `:token`, `:jwt` or
  `:mtls`, or `auth.allow_insecure: true` says a proxy or mesh in front
  authenticates every caller. `/v1/config` shows each token's `id` and scopes
  but never its hash.

- **Stored-header policy (SEC-5).** A hook used to be stored, archived and
  forwarded with every provider header, `Authorization`, `Cookie` and
  `x-api-key` included. The new `store_headers` config key
  (`%{drop: [...], hash: [...]}`, `Ankusa.Edge.StoredHeaders`) and the
  per-source `store_headers` override are applied in `Ankusa.Edge.Ingest`
  **after** verification (the verifier and the dedupe key still read every
  header): `drop` removes a header from the hook, `hash` keeps it with the
  value replaced by `sha256:<hex>`. The default drops `authorization`,
  `proxy-authorization`, `cookie`, `set-cookie`, `x-api-key` and
  `x-auth-token`, so those are no longer in the stored hook, the archive
  segment, the quarantine pen or any sink message; set `drop: []` to keep the
  old behaviour. The `[:ankusa, :ingest]` span's stop metadata gains
  `:dropped_headers`. Hooks stored before the upgrade are not rewritten.
  See `docs/configuration.md#stored-headers`.

- **Hardened listeners, TLS/mTLS on every listener, a separate metrics
  listener (SEC-6).** The new `listener` config section sets what Bandit's
  defaults left open, on every HTTP listener the instance runs:
  `read_timeout_ms` (10 s: a connection that sends nothing, or dribbles its
  request head, is closed), `body_read_timeout_ms` (15 s) and
  `request_deadline_ms` (30 s: the ingest port answers `408`),
  `max_connections` (20 000, over `num_acceptors`, default two per scheduler),
  `max_requests_per_connection` (1 000), `max_header_count` (64),
  `max_header_length` (16 384) and `http2` (`false`). `listener.tls`,
  `admin.tls`, `routes.admin.tls`, `claim_check.tls` and `metrics.tls` take a
  TLS block (`certfile`, `keyfile`, `cacertfile`, `verify`, `versions`,
  `ciphers`, `Ankusa.Listener`); `verify: :verify_peer` demands a client
  certificate (mTLS) and makes it available as the request's peer
  certificate. `metrics: %{enabled, ip, port}` starts `Ankusa.Metrics.Router`,
  a listener that serves only `/metrics`, `/health` and `/ready`; the admin
  API keeps serving `/metrics`. `Ankusa.Http.read_body_limited/3` takes
  `:read_timeout` and `:deadline`. Every new key is range-checked at boot.
  Behaviour change: listeners are now HTTP/1.1 only and close idle connections
  after 10 s; set `listener.http2: true` / `listener.read_timeout_ms` to
  restore Bandit's behaviour.
- **Store format version, migration registry, downgrade guard.** The store
  records its layout version under `m:format_version` (this release writes
  format 1; a store without the key is format 1 and is stamped on first open).
  A release that changes the key layout registers a migration in the store's
  migration registry; on boot, before serving, migrations run in order in
  bounded synced batches, each recording its place in `m:migrating:<to>` so a
  node killed mid-migration resumes at the next boot. A store written by a
  newer release refuses to open with `{:store_format_newer, found, supported}`
  instead of being read by code that ignores its keys. New tasks:
  `mix ankusa.store.migrate [--dry-run]` shows (or applies) the plan for a
  data directory, and `mix ankusa.store.fixture <path>` packs a seeded store
  as a `test/fixtures/store-v<n>.tar.zst` upgrade fixture. Upgrade order and
  rollback: [`deployment.md`](../../docs/deployment.md#upgrading-between-releases).
- `t:Ankusa.Net.cidr/0`, the type of a parsed CIDR range, now in the specs of
  `Ankusa.Net.parse_cidr/1` and `Ankusa.Net.ClientIP.resolve/2` in place of
  `%CIDR{}`.
- **Several claim stores, with failover.** `claim_check.blob_stores` takes a
  list of `{module, opts}` (buckets, regions, providers) and
  `claim_check.placement` picks a new pack's store: `:ordered` (default) the
  first, the others only while it is down; `:hash` spreads packs over every
  store by the pack id's random bits. A failed pack write moves on to the
  next store at once, and a store whose write failed, or that could not
  answer a read, is skipped by writes and searched last by reads for
  `claim_check.store_cooldown_ms` (default 30 s); a throttled answer (`429`,
  `503`) never opens the breaker. A read searches every store under both key
  layouts, healthy stores first, so a failed-over pack is still found, and a
  store that could not answer is not asked again within the same read; a
  miss while some store could not answer is `{:unavailable, reason}`, never
  `:not_found`. A `check_in/4` that tried several stores fails with
  `{:unavailable, [{store, reason}, ...]}` in attempt order (`{:unavailable,
  reason}` after a single attempt, as before). Refs, the gateway API and the
  SDKs are unchanged: the store is a function of the ref and the config.
  `Ankusa.ClaimCheck.validate_config!/1` rejects a bad `blob_stores`,
  `placement` or `key_layout`, and `retention_days` with any non-LocalFS
  claim store. New `Ankusa.ClaimCheck.Stores`, supervised in every role;
  `Ankusa.ClaimCheck.location` gains `:store`;
  `[:ankusa, :claim_check, :check_in]` gains `:store`, and
  `[:ankusa, :claim_check, :store_breaker]` /
  `ankusa_claim_check_store_breaker_transitions_total` count breaker opens and
  closes.
- **Sharded claim keys.** `claim_check.key_layout: :sharded` writes packs
  under `claims/tenant=<t>/shard=<c>/dt=<d>/<pack>`, `c` being the pack id's
  eleventh character: 32 prefixes per tenant, so one busy tenant's writes
  spread over 32 object-store partitions instead of throttling (S3
  `503 SlowDown`) on one. Opt-in (default `:flat`, the existing layout); reads
  try both layouts whatever it says, and the LocalFS sweeper sweeps both.
  `Ankusa.ClaimCheck.Ref.object_keys/3` and `shard/1`.
- `[:ankusa, :blob_store, :retry]` on every HTTP blob-store retry, counted by
  `ankusa_blob_store_retries_total{status}`.
- **The claim gateway stays fast while an object store throttles or hangs.**
  A `:claim_check` node caches pack indexes and claims in memory
  (`Ankusa.ClaimCheck.Cache`, `claim_check.read_cache_bytes`, default
  128 MiB; a claim over `read_cache_max_entry_bytes`, default 1 MiB, is never
  cached), and reads the store through `Ankusa.ClaimCheck.Reader`: concurrent
  reads of one pack index or claim share one store request; per store, an
  adaptive limit on concurrent reads (ceiling `read_max_inflight`, default
  48, under the HTTP client's 50 connections per host) halves on a throttle
  (`429`, `503`) or an unreachable store and grows back by one per round of
  successes; reads past it queue up to
  `read_queue_max` (default 256) and the rest are shed at once; and every
  request is answered within `read_deadline_ms` (default 5 s) — a store read
  that outlives it still fills the cache. Claim reads use
  `read_timeout_ms` (3 s) and `read_retries` (1) instead of the store's own
  write timeouts. `Ankusa.ClaimCheck.locate/4`, `read/4` and `read_bytes/5`
  take a `:deadline`; `Ankusa.ClaimCheck.location` gains `:tenant_id`,
  `:pack_id` and `:index`; `Ankusa.ClaimCheck.deadline/1` and
  `Ankusa.ClaimCheck.Stores.throttled?/1` are new. Telemetry:
  `[:ankusa, :claim_check, :gateway]`, `:read`, `:read_limit` and
  `:read_state`; on `/metrics`,
  `ankusa_claim_gateway_requests_total{status}`,
  `ankusa_claim_gateway_duration_seconds` (buckets up to 10 s, past
  the deadline), `ankusa_claim_check_reads_total{kind,source,result}` and
  `ankusa_claim_check_read_limit`/`_read_inflight`/`_read_queued{store}`.
- **Egress policy: `Sink.Http` can no longer be pointed at loopback, the cloud
  metadata service or the private network** (CWE-918). `Config` gains
  `egress: %{allow_insecure_http, deny_cidrs, allow_cidrs, allow_hosts,
  deny_hosts}`; every `Sink.Http` attempt (a source's and a lifecycle sink's)
  goes through `Ankusa.HttpClient.Egress`: the name is resolved for both
  families and the delivery is refused, as `{:permanent, {:egress_denied,
  reason}}`, when any address is denied (loopback, RFC 1918, carrier-grade NAT,
  link-local, unique-local IPv6, the unspecified addresses; IPv4-mapped IPv6 and
  odd literals such as `2130706433` match as the address they are) and not
  excepted by `allow_cidrs`/`allow_hosts`, or the host is in `deny_hosts`
  (`metadata.google.internal`). `http` URLs are refused unless
  `allow_insecure_http`; an `http` delivery connects to the address that was
  checked, with the name as its `host` header. A name that does not resolve is
  retried. Reasons name the host and address, never the URL.
  [`configuration.md#egress`](../../docs/configuration.md#egress).
- **TLS verification is explicit everywhere.** Every outbound `https` request
  (`Ankusa.HttpClient`: object stores, SQS, Pub/Sub, HTTP sinks) verifies the
  peer's chain and hostname and negotiates TLS 1.2 or 1.3 only. `Sink.Http` takes
  `:cacertfile` (a private CA bundle) and `:allow_insecure_tls` (the one way to
  skip verification, per sink); a `verify: :verify_none` in `:req_options` raises
  from `HttpClient.validate!/2`, and fails the config load for a static or
  lifecycle HTTP sink (`Egress.validate_config!/1`, `Sink.Http.validate_opts!/1`).
  `castore ~> 1.0` is now a locked dependency (Mint's fallback bundle).
- `Ankusa.fetch_config/1`, `Ankusa.HttpClient.validate!/2` (public).

### Changed

- `Ankusa.Sink.SQS` and `Ankusa.Sink.GooglePubSub` are built on
  `Ankusa.Sink.Messaging`; what they send is unchanged.
- **The quarantine pen no longer serializes every flagged hook (DUR-9).** A
  source's bucket is one ETS row updated with a compare-and-swap
  (`RateLimiter.hit_bucket/4`), the pen's byte count and the number of writes
  waiting for it are `:ets.update_counter/3`, and `Quarantine.put/3` and `hit/3`
  run in the request's process; a refusal after the token was taken gives it
  back. The pen process keeps recovery, the bucket sweep, retention and
  `purge/3`, and commits the writes that queued meanwhile in **one** synced
  `Store.write` (a group commit), so a forged flood costs one fsync per group
  and never holds more than one dirty-IO scheduler. At most 1 000 writes may
  wait: the next is `{:error, :overload}` (`503`, `Retry-After: 1`), spending
  nothing (`ankusa_quarantine_pending` is the gauge; `Quarantine.held/1` gains
  `pending`). `Quarantine.hit/3` answers `{:error, :store_unavailable}` only
  while the pen restarts (its tables are gone), no longer when its process is
  busy. A refusal's `Retry-After` is now the GCRA wait for the bucket rather
  than the token-bucket arithmetic it replaced. `bench/quarantine_bench.exs`: 5 000 forged hooks/s over 500
  sources, p99 under 50 ms, a clean source's p99 within 2× of its solo p99.
  Docs: `delivery.md#quarantine`, `architecture.md`, `configuration.md`.
- `routes.max_routes` defaults to 1 000 000 (was 10 000, TEN-9); the cap is
  still hard and nothing is evicted, and it is checked against the new
  `routes.memory_budget_bytes`. The snapshot's `:pattern` rows are replaced by
  `:lit` rows, `:meta` gains `max_prefix`, and a `:conflict` index is added;
  `Snapshot.patterns/2` and `Snapshot.build/1` are gone (nothing outside the
  snapshot read them).
- **`dispatch.sink_concurrency` defaults to `8` (was `nil`), and the per-tenant
  windows of `dispatch.tenants.default` apply by default (TEN-3).** A
  deployment whose hooks all share one tenant (none, or one tenant id) is held
  to `max_inflight: 1024`, `max_inflight_bytes: 32 MiB` and `concurrency: 16`
  instead of the global limits: raise `dispatch.tenants.default` to
  `max_inflight: 4096`, `max_inflight_bytes: 134217728` and `concurrency: 32`
  to restore the old ceilings. `dispatch.sink_concurrency` may now exceed
  `dispatch.concurrency` (the global cap binds first), where it used to be
  refused; the breaker and the sink cap are per tenant. The cap is also a
  rate ceiling: one sink key delivers at most `sink_concurrency` ÷ the sink's
  latency per second (`bench/core_bench.exs`'s 5 ms sink: `end_to_end_per_s`
  3 595 → 1 140 at 512 B on one machine; ingest and its p99 unchanged), so set
  `sink_concurrency` higher, or `nil`, for a source that needs more.

- **A tenant-carrying URL scheme needs a `rate_limits.default` (TEN-2).**
  `Ankusa.RouteResolver.TenantPath` (`routing: tenant_path`) with
  `rate_limits.default: nil` now fails the boot in
  `Ankusa.Edge.RateLimiter.validate_config!/1`: the URL names the tenant, so
  every tenant the config does not list was unlimited. Give it a default.
  The limit parser now takes the instance's `max_body_bytes` as a second
  argument (`parse_limit(attrs, max_body_bytes)` replaces `parse_limit(attrs)`),
  the ceiling for a tenant's own, and a limit that is not `rate` and `burst`
  plus the optional keys above is refused with a new message.
- **Breaking (SEC-10):** `Sink.Http` is signed by default. `validate_opts!/1`
  (so boot, and the image's loader) refuses a sink with neither `:secret` nor
  `allow_unsigned: true` (`Sink.Http requires secret: …`), and a delivery
  attempt for one that skipped validation is dead-lettered
  (`{:error, {:permanent, :unsigned_sink}}`) instead of being sent unsigned.
  `allow_unsigned: true` sends no signature headers and is for a receiver that
  cannot verify; a `:secret` still signs when both are set. A deployment that
  delivers unsigned adds `allow_unsigned: true` to those sinks, or better, a
  `:secret` the receiver verifies (every SDK's `receive_delivery`).

- **Breaking:** `Sink.Http` refuses, at delivery, a URL that is not `https`
  or that resolves to a denied address, with the default policy. A deployment
  that delivers to a plain-http or private-network receiver sets `egress`
  (`allow_insecure_http: true`, and `allow_cidrs` or `allow_hosts` naming the
  receiver). A static or lifecycle sink with an unsupported `:req_options` key
  now fails at boot instead of at its first delivery.
- **Hooks and delivery rows have a versioned encoding.** `Ankusa.Envelope`
  gains `to_binary/2` (`version: 0 | 1`; v1 is `"AE"`, `1`, then the term) and
  a delivery row gets the same scheme (`"AD"`, `1`). Both are read in the old
  bare form (v0) and in v1; this release still *writes* v0, so a rollback to
  0.6 keeps reading every row, and the release after it writes v1 (see
  `docs/deployment.md`, "Rolling back a minor"). A field a newer node added is
  ignored on an envelope and kept on a row, whether or not this VM has seen
  its atom (decoding does not use `binary_to_term/2`'s `:safe`, which would
  reject it). **Breaking for callers of the internals:**
  `Ankusa.Envelope.from_binary/1`
  now returns `{:ok, envelope} | {:error, :unsupported_envelope_version |
  :undecodable}` instead of the struct or a raise. Every reader handles the
  error: dispatch dead-letters the delivery (reason
  `:unsupported_envelope_version`, `:unsupported_delivery_row_version` or
  `:undecodable`) and lists it in the DLQ, `Ankusa.Queue.hooks/3` returns
  `{:error, {:corrupt_value, :hooks, _}}`, a replay skips the hook, the
  compactor logs it, and the 0.3 import refuses to boot with
  `{:corrupt_legacy_wal_record, path, seq, reason}`.
- Requires `async_api_spex ~> 0.2` from Hex (was `~> 0.1`, which excluded
  0.2.0, the release this package is built and tested against).
- **Breaking:** `claim_check.blob_store` is now `claim_check.blob_stores`, a
  list (`blob_stores: [{mod, opts}]`); a config still naming `blob_store`
  fails `Ankusa.Config.new/1` with `unknown Ankusa.Config key:
  claim_check.blob_store`. `Ankusa.ClaimCheck`'s `claim_store/1` and the
  `:claims` scope of `Ankusa.BlobStore` are gone: claim packs go through
  `Ankusa.ClaimCheck.Stores`.
- **Breaking:** `Ankusa.ClaimCheck.Ref`'s `object_key/2` and `key/1` take a
  layout: `Ankusa.ClaimCheck.Ref.object_key/3`, `Ankusa.ClaimCheck.Ref.key/2`.
  `Ankusa.BlobStore.Retry`'s `run/2` became `run/3` (a later release added the
  breaker scope and request kind: `Ankusa.BlobStore.Retry.run/5`), the instance
  first.
- `Ankusa.BlobStore.Retry` jitters each delay to between half and all of
  200 ms / 1 s / 5 s, so nodes throttled together don't retry in lockstep.
- Metric labels with integer values (an HTTP status, a store index) are
  reported as that number instead of `other`.
- The claim gateway's `503 store_unavailable` carries `Retry-After` of a
  random 1–3 s instead of a fixed `1`, and logs throttles, sheds and missed
  deadlines at `:debug` (they are counted by
  `ankusa_claim_check_reads_total`).
- `Ankusa.HttpClient` answers `{:error, %RuntimeError{}}` when the HTTP
  client's connection pool cannot hand out a connection within its checkout
  timeout, instead of letting that raise out of the adapter: the retry and
  the breaker see it as the transport failure it is.

### Fixed

- `Ankusa.Verifier.Hmac`: a scheme with `hash: sha1` (documented in
  `configuration.md`) raised `:badarg` on every hook, because `:crypto.mac/4`
  names SHA-1 `:sha`, not `:sha1`; it now verifies.
- `Ankusa.Verifier.Hmac` answers `{:error, :bad_scheme}` for an inline
  `%Scheme{}` that fails validation (an unknown `{token}` in `signed`, a
  `{:sig, _}` timestamp without `parse: :csv_pairs`, ...), as `validate_opts/1`
  already said it would, instead of raising `ArgumentError` out of the request
  path.
- The 0.3 import treats a WAL frame header with length 0 as not a frame. A
  header whose payload never landed, followed by the zeros a file is extended
  with after a crash (length 0, CRC 0, the CRC of nothing), was "valid", was
  decoded as an envelope, and refused the boot with `legacy_import_failed`;
  it is now the torn tail it is, and every complete frame before it imports.
- `Ankusa.ClaimCheck.Pack.build/1` refuses a pack of more than 65,532 claims
  (was 65,533). A full 65,535-entry pack wrote ZIP64's `0xFFFF` marker in its
  end record, which standard readers (Erlang's `:zip`: `{:error, :bad_eocd}`)
  refuse to open. `Ankusa.ClaimCheck` already split packs at 65,000 claims, so
  only a direct caller of `Pack.build/1` could reach it.

## [0.6.0] - 2026-10-14

### Added

- Every delivery says how the hook was verified at ingest. The v1 message
  (`Ankusa.Sink.Message`, every queue sink) carries
  `verification: {"status": "ok" | "failed" | "skipped", "flagged": bool}`,
  and `Sink.Http` sends `x-ankusa-verification` always and
  `x-ankusa-verification-flagged: true` for a hook its source accepted despite
  a failed check (`on_verify_failure: :accept_flag`). `skipped` is a source
  with no verifier (`Verifier.None`) or a lifecycle event. The reason stays
  off the wire. `SinkMessageV1` requires the field; `v` stays `1`.
  `Ankusa.Sink.Message.verification/1` is the one place that decides it.
- `GET /v1/tenants/:tenant/sources[/:name]` shows `trust_url_tenant`.
- `trust_url_tenant` on `%Ankusa.Source{}` (default `false`), and
  `Ankusa.Verifier.check_shared/2`, `validate_shared!/1` and
  `warn_stored_shared/2`: see the E5 entry under Changed.
- An internal `SourceStore.Table` module: the ETS layout and spec handling
  `SourceStore.Persistent` and `ankusa_redis`'s `SourceStore.Redis` share.
  A source-store read while the store restarts answers
  `{:error, :unavailable}` instead of raising.
- `Ankusa.Queue.pending_for_source/2`: one source's pending and in-flight
  delivery rows in this node's queue.
- `Ankusa.Dispatch.Pipeline.dead_letter_source/2`: dead-letter one source's
  pending rows now, inside the pipeline.
- `Ankusa.ClaimCheck.locate/3` and `read_bytes/4`: find a claim, and read any
  range of it, without reading it whole.
- `Ankusa.BlobStore.Retry`, the retry loop the HTTP blob stores share.
- `Ankusa.Sink.Reaper`: stops a sink adapter's connection after its
  `:idle_timeout_ms` without a delivery. The broker adapters (`ankusa_rabbitmq`,
  `ankusa_kafka`, `ankusa_nats`, `ankusa_redis`) run one each.
- **`Ankusa.Sink.GooglePubSub`**: publishes hooks to a Google Cloud Pub/Sub
  topic with one `topics.publish` `POST` per delivery, the same
  `Ankusa.Sink.Message` data as the other queue sinks and the same metadata
  as message attributes. No new dependency (`req`). Auth is a caller-supplied
  `:token_provider` MFA, as for `BlobStore.GCS`; one that answers `:error`
  is `:no_credentials` (retried, no request sent). `:ok` only on a `200`
  naming the message id. A message over `:max_message_bytes` (10 MB) and
  `INVALID_ARGUMENT` are permanent; every other Pub/Sub error (a missing
  topic included: the sink never creates one) is retried. Optional
  `:ordering_key` (a string or a function of the envelope). `describe/2`
  reports the AsyncAPI `googlepubsub` bindings. The admin config view shows
  `project` and `ordering_key`.
- `Ankusa.Sink.Message.attributes/2`: the `{name, value}` metadata pairs a
  queue sink carries outside the body, now shared by `Sink.SQS` and
  `Sink.GooglePubSub`.

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

- Requires `bandit ~> 1.13` (was `~> 1.12.5`, which held every app on
  1.12.x: 1.12.5 has two published advisories, EEF-CVE-2026-105659 and
  EEF-CVE-2026-105660, fixed in 1.13.0).
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
- **Breaking (E5):** under a resolver that takes the tenant from the request
  (any but `Ankusa.RouteResolver.Path`), a source that is shared
  (`tenant_id: "default"`), has `Ankusa.Verifier.None` and does not set
  `trust_url_tenant: true` is refused: a configured one stops the instance
  from starting (`ArgumentError` naming the source, on every node whatever
  its roles), and a `SourceStore.Persistent` write of one is
  `{:error, :invalid, message}` (the admin API's `400 invalid_source`). One
  the store already held keeps serving, and is named in a warning at boot.
  Set `trust_url_tenant: true` to keep a deliberately open shared endpoint.
- **Breaking (O2):** no supervisor in the instance tree holds the
  `%Ankusa.Config{}` any more. `Ankusa.Instance.child_spec/1` and
  `start_link/1` store the config (`Ankusa.put_config/1`) and pass only the
  instance name; every child is started with `[instance: instance]` and reads
  `Ankusa.config(instance)`. A custom `Ankusa.SourceStore` or
  `Ankusa.Routes.Store` that runs a process now gets `start_link([instance:
  instance])` instead of `start_link(%Ankusa.Config{})` /
  `opts[:config]`, and reads its options from `Ankusa.config(instance)`.
  `SourceStore.Persistent.start_link/1` takes `[instance: instance]`.
  `:sys.get_status/1` of the instance's parent, the instance and its
  subtrees no longer prints sink options or verifier secrets.
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

- Under `wal: :none`, a source written at runtime (`Ankusa.SourceStore.put/5`,
  the admin API's source routes) with no durable sink is refused with
  `{:error, :invalid, message}` (`400 invalid_source`), as boot already
  refused a static one: it would have answered `201` with no durable copy
  anywhere. Every writable source store applies the new
  `Ankusa.Queue.check_durable/2` before a write, and `Ankusa.Edge.Publish`
  answers `503` for a source that reaches ingest without that check (stored
  before it, or written to a shared source store by a `wal: :disk` node)
  instead of acking it with no durable copy.
- A store restored from backup (or created fresh with a backup configured)
  keeps its `RESTORE-IN-PROGRESS` marker until `reconcile_archive` has
  succeeded. A node that died, or whose reconcile failed, between the restore
  and the reconcile now reconciles on its next boot instead of opening as an
  existing store and reusing archived seqs.
- A backup refuses to run when the prefix's latest backup belongs to another
  store (`{:foreign_backup, theirs, ours}`, logged at error level): each store
  gets an id (`m:store_id`) on its first backup, carried by its checkpoints
  and recorded in every manifest. Two nodes on one prefix no longer purge each
  other's backups. A node whose `LATEST` was moved by another writer of the
  same store (an old host beside its replacement) refuses too
  (`{:prefix_shared, latest}`).
- Blob files of the hooks and quarantine column families are capped at 64 MiB
  (`blob_file_size`; RocksDB's default is 256 MiB), bounding what one backup
  upload or restore holds in memory.
- `DELETE /v1/tenants/:tenant/sources/:name` no longer answers `204` while
  this node's queue still holds undelivered rows for the source: it is
  `409 source_has_deliveries` (`{"pending": n, "inflight": m}`) and nothing is
  deleted. `?deliveries=dead_letter` dead-letters the pending rows first, as
  `{:source_gone, source_id}` (needs the `:dispatch` role, `409
  role_not_enabled` otherwise; `503 dispatch_unavailable` if the pipeline
  cannot answer), then deletes; rows in flight settle on their own. A source
  re-created under the same name no longer inherits the old backlog.
- Delivery rows record their `source_id`, so one source's rows are found
  without reading every hook. Rows written before keep working: they are
  matched by their hook's source.
- The claim gateway serves one `Range: bytes=` range as `206` with
  `Content-Range` (`416` past the end), from one ranged store read, and every
  claim response says `Accept-Ranges: bytes`. Several ranges, other units and
  any `If-Range` get the whole claim. `HEAD` reads only the pack's index, not
  the claim.
- The S3, GCS, Azure and OCI blob stores retry a transport error or a `408`,
  `429`, `500`, `502`, `503` or `504` (`:retries`, default 2 extra attempts,
  200 ms then 1 s apart), re-signing each attempt; any other answer is
  returned at once, mapped as before. `put` sends its iodata as is instead of
  copying a segment into one binary first.
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

[Unreleased]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.6.0...HEAD
[0.6.0]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.4.0...ankusa-v0.6.0
[0.4.0]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.3.0...ankusa-v0.4.0
[0.3.0]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.2.4...ankusa-v0.3.0
[0.2.4]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.2.1...ankusa-v0.2.4
[0.2.1]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.2.0...ankusa-v0.2.1
[0.2.0]: https://github.com/jamescarr/ankusa/compare/ankusa-v0.1.0...ankusa-v0.2.0
[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/ankusa-v0.1.0
