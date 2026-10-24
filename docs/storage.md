# Storage: the local store and the object store

Two tiers, on purpose. The **local store** is the fast, small, durable tier the
ack depends on under the default `wal.type: disk`: one RocksDB database per
instance holding every committed hook, one delivery row per hook and sink, the
archive obligations and the segment catalogue. The **object store** is the
cheap, large, long-term tier: segments roll into it asynchronously, never
blocking an ack. See [`architecture.md`](architecture.md) for how this fits the
request path, and the [`wal: :none`](#none-no-queue-no-store-hooks) section
below for the mode that has none of it.

## `Ankusa.Store` and `Ankusa.Queue`

The local store is one RocksDB database per instance at
`<data_dir>/<instance>/store`, opened by `Ankusa.Store`. Everything below lives
in it: the committed hooks, their delivery rows and the archive's catalogue.
The queue's mode is named by the historical `wal` key (`wal: :disk | :dual | :none`,
YAML `wal.type`): `:disk` is the store-backed queue described here, `:dual` is
that queue plus [the intake log](#the-intake-log), `:none` is
the direct path [below](#none-no-queue-no-store-hooks).

Column families, all in that one database:

| Family | Holds |
| --- | --- |
| `default` | the next-seq marker, the next tenant ordinal (`m:next_ord`), the store format version (`m:format_version`, see [Upgrading between releases](deployment.md#upgrading-between-releases)), migration markers, API-managed sources (`s:`) and their revisions (`v:`, kept after a source is deleted so a re-created one continues the count), rate-limit overrides, replay jobs (`j:<id>`), the audit sequence (`m:audit_seq`) and the key audit hashes are made with (`m:audit_key`) |
| `hooks` | each committed hook, keyed by its `seq` |
| `deliveries` | one delivery row per hook and sink |
| `index` | due `?D`, claimed `?F` and dead `?x`/`?X` rows, active-tenant markers `?A`, archive obligations, cleared markers, claim-check refs, ingest dedupe keys (`?u`) and their expiry index (`?e`); see [Tenants, counters and the active index](#tenants-counters-and-the-active-index) |
| `archive` | the segment catalogue, and locations imported from a 0.3 node's index |
| `quarantine` | the quarantine pen: one summary and one body key per held envelope |
| `audit` | the audit trail, `<<?a, seq::64, at::64>>` → a versioned row (`<<"AA", 1, term>>`), in commit order; and the rows of changes still in flight, `<<?p, seq::64>>` (see [Architecture](architecture.md#guarantees-by-component)). Pruned by `audit.retention_days` |
| `tenants` | tenant ordinal ↔ tenant id: `<<?t, ord::32>>` → id, `<<?n, id>>` → `<<ord::32>>` |
| `counters` | per-tenant queue counters, `<<ord::32, field::8>>` → an integer, written only as merge operands; ordinal 0 is the node's total |

`Ankusa.Store` owns the DB handle; every other process calls
`Ankusa.Store.write/3`, `get/3`, `multi_get/3`, `fold/6` and `property/3`
directly (the NIF runs on dirty schedulers). There is no per-reader cursor:
`Ankusa.Queue.hooks/3` walks the `hooks` family by `seq`.

`Ankusa.Queue` is the public face of the queue:

```elixir
entry = %{envelope: envelope, sinks: [{Ankusa.Sink.Http, url: "https://example.internal"}]}
{:ok, [{:committed, envelope}]} = Ankusa.Queue.enqueue(:default, [entry])

{:ok, hooks} = Ankusa.Queue.hooks(:default, 0, 100)            # seq > 0, ascending
{:ok, %{next_seq: _, hooks: _, deliveries: _, disk_bytes: _}} = Ankusa.Queue.stats(:default)
{:ok, %{total: _, entries: _}} = Ankusa.Queue.dead(:default, source_id: "stripe", limit: 100)
```

### The commit: one synced batch

`Ankusa.Queue.Writer`, one per instance, is the only process that assigns a
`seq`. It writes **one** synced RocksDB batch containing:

- the hook, keyed by seq;
- one pending delivery row and one due key per sink of its source, bound by
  sink index and module at ack time, each due key with its active-tenant marker;
- an archive obligation, but only while the `:storage` role runs;
- the dedupe key and its expiry entry, for a hook with a dedupe key;
- the counter deltas of all of them (`pending`, `bytes`, `archive_pending`, for
  the tenant and for the node);
- the tenant's ordinal and the next-ordinal marker, for a tenant seen for the
  first time (the Writer is the only process that assigns them);
- the next-seq marker.

The batch is atomic: a commit either lands whole or nothing is acked, and a
failed commit makes the batcher answer `503 store_unavailable`. Seqs are
strictly increasing and never reused — gaps are allowed, because a failed
commit still consumes seqs. `[:ankusa, :commit, :stop]` reports `batch_size`,
`bytes` and `duration` of the write of a group into the store; the fsync that
makes it durable is `[:ankusa, :wal_sync]` (`ankusa_wal_sync_duration_seconds`).
A hook whose source has no sinks while `:storage` is
off has no obligations at all: it is acked and stamped with a seq but not
stored.

A commit is a **group**. The writer takes every batch the batcher partitions have
queued (up to `batcher.group_commit_max_bytes`, 4 MiB, and
`batcher.group_commit_max_records`) and writes them as one batch, so four
partitions at one fsync each become one fsync. The writer applies the group
without an fsync and hands its callers' replies to the **syncer**
(`Ankusa.Queue.Syncer`), which fsyncs the WAL once for every group applied so
far and then answers them: while group N syncs, group N+1 is assembled and
applied, so a group costs the larger of its CPU and its fsync, not their sum.
A group is applied before it is durable, so until its fsync returns nothing
external may rely on it: dispatch does not claim its rows, the archive does not
read them, and a retry of one of its hooks is not answered `201 duplicate`
([ADR 0002](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0002-dual-commit-intake-log.md),
amendment 17). A failed fsync answers every group it covers `503`, reopens the
store like a failed write, fences the writer and deletes the rows of those
groups before it accepts work again, so a hook the provider was told `503` is
never delivered and its retry is stored once. Everything that needs no seq is done before
the writer is called, in the batcher's commit task: the hook's binary, its
delivery rows, and a read of its dedupe keys. That read is a prefetch the
writer re-validates before it assigns seqs: the first occurrence of a key in
the group wins, and a key it wrote since the caller read it (an earlier
group, or one whose intake publish is still in flight) is answered from what
it wrote, so two partitions that saw the same provider key as absent still
store one hook (`dedupe_test.exs`, "the same key on two batcher partitions").
The cap is a latency bound: every caller waits for its whole group, so a
group is kept to what one fsync and one produce request can carry in a few
milliseconds ([`configuration.md`](configuration.md#library-configuration-elixir)).
A writer that is saturated (its last group held 64 or more hooks) waits
`batcher.group_commit_delay_ms` (1 ms) before it starts the next group, so the
callers it just answered join it instead of forming the one after: under a
closed loop of clients the groups are twice the size (`bench/store_commit.exs`),
and at low load nothing waits. Per-tenant counters stay blind merge operands in
the group's batch (ADR 0001).

### Encoding and versions

A hook (the `hooks` family, and each record of an archive segment) is an
`Ankusa.Envelope`; a delivery row (the `deliveries` family) is a map. Both are
Erlang terms and both carry a version:

| Value | v0 | v1 |
| --- | --- | --- |
| hook | the bare `term_to_binary` of the envelope's fields (every release up to 0.6) | `"AE"`, `1`, the same term |
| delivery row | the bare `term_to_binary` of the row map (every release up to 0.6) | `"AD"`, `1`, the same term |

Readers take both. A key a reader does not know is not an error: an envelope
drops it, a row keeps it (so a field a newer node added survives this node
rewriting the row on a retry). Decoding does not depend on which atoms the
reader's VM has seen, so a newer node's new field never makes a row
unreadable. A version the reader does not know, or bytes that are no value at
all, is an error value (`:unsupported_envelope_version`,
`:unsupported_delivery_row_version`, `:undecodable`), never a raise: dispatch
dead-letters the delivery with that reason (the hook stays stored, and the
dead letter lists in `GET /v1/dlq`), a replay counts the hook as skipped, the
compactor logs it and drops its archive obligation, `Ankusa.Queue.hooks/3`
returns `{:error, {:corrupt_value, :hooks, _}}`, and the 0.3 import refuses
to boot with `{:corrupt_legacy_wal_record, path, seq, reason}`.

This release writes v0. The release after it writes v1, so a rollback of
*this* release to 0.6 still reads every row; see
[Rolling back a minor](deployment.md#rolling-back-a-minor). Proof:
`envelope_test.exs`, `queue_test.exs` and `dispatch_test.exs` (see
[`testing.md`](testing.md)).

### Durability, corruption, full disk

- One fsync per group commit (`sync_wal` after a `sync: false` batch): hundreds of hooks from every partition, one fsync, answered only after it. The boot
  line says it plainly: `[ankusa] store at <path>. Durable to power loss on
  THIS host only.` `kill -9` loses no acked hook; RocksDB recovers its own WAL
  (`loss_test.exs`, and the [crash drill](testing.md#crash-consistency-drill-mise-run-testcrash),
  which also cuts power under load). Local `BlobStore.LocalFS` writes are
  durable too (see below).
- RocksDB opens with `paranoid_checks` and
  `wal_recovery_mode: tolerate_corrupted_tail_records`: a torn trailing write
  (the last, never-acked one) is dropped. Damage anywhere before it refuses to
  open (`{:store_open_failed, path, reason}`, with a log line), because a store
  this node cannot read is never treated as empty. Point reads report checksum
  failures; a scan only trusts a result that reaches the end of its range, so
  corruption surfaces as an error, never as a silently shorter list. The
  Writer, the source store and the rate limiter refuse to start when they
  cannot read their state.
- Full disk: commits fail (`503 store_unavailable`, nothing acked) and writers
  resume by themselves once space frees. Any failed store write (ingest,
  dispatch, the archive, the quarantine pen, source and rate-limit edits) also
  tells the Store, whichever process saw it, and the Store closes and reopens
  itself at most once every 5 s. That clears a latched RocksDB write error if
  one outlives the freed space. The reopen waits until a synced write of a small
  file next to the database works (`ankusa_store_reopen_total{result="deferred"}`
  counts the waits): a disk that still fails keeps its database open and
  readable instead of being closed and half-recovered. A store that cannot reopen stays closed (every
  call answers `:store_unavailable`) and retries by itself after 5 s, then
  twice as long each time up to 60 s, each wait jittered down by up to a
  quarter, so a fleet whose disks came back together does not replay its WALs
  in step. The crash drill fills a 192 MiB tmpfs under load
  and checks the `503`s, the `/ready` flip and the resume
  ([`testing.md`](testing.md#crash-consistency-drill-mise-run-testcrash)).
- A failing store is visible without reading logs. `/ready` answers `503` with
  `store` (`"write_failed"` or `"store_unavailable"`) and a `reason` from a
  fixed set: `no_space`, `io_error`, `read_only`, `corruption`, `busy`,
  `timeout`, `not_running`, `other` (`Ankusa.Store.failure_class/1`). The
  scrape carries `ankusa_store_write_failures_total{class}` (every failed
  write a process reported and every failed readiness probe),
  `ankusa_store_reopen_total{result}` (every reopen attempt, the store's own
  retries included), `ankusa_store_unavailable_seconds` (how long the store has
  been closed: `0` while it is open, and refreshed by every failed retry, so
  at most 60 s stale) and `ankusa_store_wal_replay_seconds` (the close plus the
  open of each successful reopen). The events behind them are
  `[:ankusa, :store, :write_failed | :reopened | :unavailable]`
  ([`Ankusa.Telemetry`](https://hexdocs.pm/ankusa/Ankusa.Telemetry.html)).
  The first open of a process is not a reopen: its replay is in the boot log.
  Tests: `store_test.exs` (`write failure telemetry`, `reopen backoff`),
  `metrics_test.exs`, and the crash drills, which fail unless the series rise
  and `/ready` names the reason.
- Stop an instance cleanly (supervisor shutdown, `docker stop`'s SIGTERM) and
  the store closes with it. SIGKILL keeps the data (RocksDB WAL recovery), but a
  node halted mid-write-load with the DB open can segfault at exit.

### RocksDB tuning and the index filters

The store's RocksDB settings are config, not constants: `queue.rocksdb` in
[`configuration.md`](configuration.md#library-configuration-elixir) sets the
block cache, the bloom filter, `max_open_files`, the compaction style, a
flush/compaction rate limiter, the background job budget and a memtable size
per column family. They are read when the database opens and again by every
reopen (`Store.reopen/1`). What stays fixed is everything the durability
promise above rests on: `paranoid_checks`, the WAL recovery mode, the
`sync: true` commit, `max_total_wal_size` and `db_write_buffer_size`; no
tunable touches the WAL or its fsync (the [crash drill](testing.md#crash-consistency-drill-mise-run-testcrash)
is unchanged and passes).

- **Filters where the reads are point lookups of absent keys.** The `index`
  family holds the ingest dedupe keys (`?u`, `?e`) next to the ordered
  due/inflight/dead/active rows, and `Queue.Writer` asks it for a whole batch of
  never-seen dedupe keys before every commit (`Store.multi_get/3`); `deliveries`
  is read by point too. Both get a whole-key bloom filter (`bloom_bits_per_key`,
  10 by default, about a 1 % false-positive rate) and keep their index and filter
  blocks in the block cache (`cache_index_and_filter_blocks`), so a miss costs a
  filter probe per file instead of an index and data block read, and the memory
  stays inside `block_cache_bytes`. The other families have no filter. There is
  no prefix extractor: `index` is also scanned in key order, and a prefix
  extractor would make every such iterator choose between a total-order and a
  prefix seek, for no gain over whole-key filtering on the dedupe lookup.
- **No migration.** A filter is written into the files a flush or compaction
  creates, so an upgraded store gains it as compaction rewrites its files; the
  key layout and the store format are unchanged. `bloom_bits_per_key: 0` restores
  the plain table options.
- **Size the cache for the filters.** At 10 bits a dedupe key (a `?u` and a `?e`
  key) costs about 2.5 bytes of filter, so 10⁸ keys (`dedupe.max_entries`) are
  about 250 MB of filters plus the index blocks, all competing for
  `block_cache_bytes`. The default 64 MiB cache caps the memory; it does not hold
  those blocks for a store that size, which is why the table below shows a
  raised cache helping on top of the filter.
- **Measured.** `bench/dedupe_bench.exs` fills a store with 10⁸ dedupe keys
  (the `index` family's files came to 12.6 GB), then times 2 000 batches of 256
  fresh keys, each a `multi_get` followed by a `sync: true` commit. On an
  arm64 Mac with 48 GiB RAM, so the files sat in the OS page cache and the
  numbers are a lower bound on what the filters save when blocks come from disk
  (2026-10-19, [PR #145](https://github.com/jamescarr/ankusa/pull/145)):

  | run | block cache | miss p50 / p99 (ms) | commit p50 / p99 (ms) |
  | --- | --- | --- | --- |
  | before (no filter) | 64 MiB | 34.7 / 109.7 | 40.3 / 117.0 |
  | after (defaults) | 64 MiB | 12.8 / 22.3 | 17.7 / 28.0 |
  | before, cache raised | 1 GiB | 31.0 / 184.7 | 36.7 / 212.9 |
  | after, cache raised | 1 GiB | 1.4 / 9.2 | 6.0 / 25.7 |

  The commit p99 falls by 76 % at the default cache and by 88 % at 1 GiB. A p99
  over 2 000 samples on a shared machine is noisy (a second run of the same
  setup gave 106 ms and 32 ms at the default cache); the p50 moved more than any
  noise. Proof: `bench/dedupe_bench.exs`; the settings are read back from the
  `OPTIONS-*` file by `store_test.exs` (`effective settings`; RocksDB does not
  write the rate limiter or the cache's capacity there, so those are asserted
  from the store's state and `:rocksdb.cache_info/2`).

### Reclamation: a hook is deleted by its obligations

A hook's obligations are its delivery rows (pending or dead) and, only if
`:storage` ran at ack time, its archive obligation. When the last one clears,
the hook is deleted — whatever roles the node runs. So a node without the
archive (`ANKUSA_ROLES=edge,dispatch`) reclaims on delivery, and an archive
that is behind, off or on another node never blocks delivery.

### Tenants, counters and the active index

Design and reasons: [ADR 0001](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0001-tenant-key-layout.md). Every tenant gets
a dense 32-bit **ordinal** at the first hook the store commits for it
(`default` is the tenant of a hook with none); ordinals are never reused, 0 is
the node's own aggregate, and `tenants.max` bounds how many exist (a hook of
the tenant past it is `503 tenant_limit`). Index keys lead with the ordinal, so
one tenant's rows are one contiguous range:

| Key (`index` family) | Value | What |
| --- | --- | --- |
| `<<?D, ord::32, at::64, seq::64, sink::16>>` | `<<size::32, kind::8>>` | due: `kind` 0 a first attempt, 1 a retry, 2 the first attempt of a lane head, 3 a retry of one |
| `<<?F, ord::32, seq::64, sink::16>>` | `<<size::32>>` | claimed by dispatch |
| `<<?x, at::64, seq::64, sink::16>>` | `{source_id, id, ord}` | dead, in the node-wide time order `GET /v1/dlq` lists |
| `<<?X, ord::32, at::64, seq::64, sink::16>>` | empty | dead, by tenant |
| `<<?A, second::64, ord::32>>` | empty | active: the tenant has a due row in that second |
| `<<?a, seq::64>>` | `<<size::32, ord::32>>` | archive obligation |
| `<<?O, ord::32, lane::128, seq::64, sink::16>>` | `<<size::32>>` | ordering lane member: a pending row waiting its turn, in `seq` order, with no due row (ADR [0004](adr/0004-ordering-lanes.md)) |
| `<<?H, ord::32, lane::128>>` | `<<seq::64, sink::16>>` | lane head: the one row of the lane that is in `?D` or `?F`; exists exactly while the lane is busy |
| `<<?P, ord::32, lane::128>>` | empty | lane marker: this lane may have a member and no head (promotion work) |

Store format 8 stamps these (no rewrite: a store that never enables `ordered` is
bit-identical to format 7 apart from the stamp), so a format 7 image refuses it.
Rollback boundary: the first start of a format 8 image. The counters `lanes`
(heads), `lanes_blocked` (heads waiting out a retry or park) and `lane_members`
are kept like the others.


`pending`, `scheduled`, `inflight`, `dead`, `bytes` (the stored size of every
delivery row the tenant holds), `archive_pending` and `dedupe` (live ingest-dedupe
keys, see below) are kept per tenant and per
node in the `counters` family, as blind RocksDB merge operands in the same
batch as the row change they count, so any process may change them and a crash
leaves rows and counts together or neither. `Ankusa.Queue.pending_for_tenant/2`
and `dead_for_tenant/2` and every queue gauge read them: O(1), not a scan.
`pending` is first attempts waiting and `scheduled` retries waiting (a retry
whose backoff has ended counts until it is claimed).

The active index is what makes a dispatch scan cost the tenants with work due,
not all tenants: a scan reads the markers up to the current second, claims from
each tenant named, oldest second first, and removes a marker once its second is
over and the tenant has nothing due in it (delete, then verify no commit slipped
a row in). The index order alone would serve a tenant with a long backlog before
one that arrived later in the same scan; dispatch's claim splits its page
across the tenants with work due and gives each its own in-flight window
([`delivery.md#fairness-between-tenants`](delivery.md#fairness-between-tenants)). A due row
is never written without its marker (`Deliveries.due_ops/6`), and a counter is
never changed by hand: both only through the `Deliveries` op builders.
`Ankusa.Queue.purge_tenant/3` removes a tenant's pending, in-flight or dead rows
in chunks of 500, resumable by calling it again; a tenant erase
([`multi-tenancy.md`](multi-tenancy.md#erasing-a-tenant)) calls it for all
three. An erase keeps its progress in `m:erase:<tenant>` and, when it is done,
its tombstone in `m:erased:<tenant>` (`default` family, value `<<at::64>>`):
`Ankusa.Storage.fetch/2` answers `{:error, :gone}` for a hook of that tenant
received at or before `at`, and archive replay skips it, while the segment that
holds its frame stays until the bucket's lifecycle expires it. Both claims are
tested in `test/ankusa/tenant_test.exs` (`erase leaves no trace`, `a fetch of an
erased hook is gone`).

#### Ingest dedupe keys: bounded, expired outside the writer

A source with `dedupe:` makes the writer put two index keys per new provider
event key, in the commit batch of the hook: `?u` (tenant, source, event key →
`<<expires_at::64, original_id>>`) and its expiry index `?e`. Both are bounded
(ground rule 3 of the hardening plan, `ANKUSA_HARDENING_PLAN.md`):

| | |
| --- | --- |
| **Cap** | `dedupe.max_entries` (100 000 000) live keys on the node, `dedupe.max_entries_per_tenant` (30 000 000) for one tenant: about 200 B of disk each |
| **Count** | the `dedupe` counter (field 7 of the `counters` family, per tenant and node), a merge operand in the batch that inserts or deletes the key; the check is one `multi_get` per commit batch that has a keyed hook |
| **Refusal** | a hook whose key would be a *new* live key past either cap is `503 dedupe_quota` with `Retry-After: 60`, counted as `outcome="dedupe_quota"` on `ankusa_ingest_requests_total`. It is never stored without its key (dedupe is never silently skipped); a retry of a live key is still a `duplicate`, and an expired key is replaced in place, so neither is refused at the cap |
| **Sweep** | `Ankusa.Queue.DedupeSweeper`, its own supervised process: it scans `?e` for keys past their expiry every 10 s and then back to back while full chunks of 1 000 keep coming, and hands the writer each chunk to apply. The writer deletes a key only if it still holds the expiry the sweeper read, so a refresh is never undone, and takes the `dedupe` counter down in the same batch. The scan, decode and batching never run in the writer |
| **Metric** | `ankusa_dedupe_keys` (the node counter), `ankusa_dedupe_pruned_total` |

Store format 4 added the counter: its migration counts the `?e` keys a format 3
store holds, in resumable batches of 500.

#### Dead letters, quarantine, catalogue and audit: bounded

Store format 5 added the `dead_bytes` counter (field 8 of `counters`; its
migration sums the delivery rows of the `?x` keys, resumable in batches of 500).

| | Cap | Sweep | Refusal |
| --- | --- | --- | --- |
| **DLQ** | `queue.dlq.max_rows`, `max_bytes`, `retention_days` | `Ankusa.Queue.DlqReaper`, only archived rows (an `?a` obligation is gone and a segment index holds the id) | `503 dlq_full` + `Retry-After: 60` ingest back-pressure, never a delete |
| **Quarantine** | `quarantine.max_bytes`, `retention_days` | hourly, skipping what a live replay job reaches | `503 quarantine_full` |
| **Catalogue** (`?S`, `?L`) | `storage.catalogue_retention_days`, `archive.retention_days` | hourly in the compactor's sibling process | none; a segment with an owed delivery row or a live archive replay stays |
| **Audit** (+ `?o` entities) | `audit.max_rows`, `max_bytes`, `outbox_max_backlog` | hourly, and before a change is accepted; never past `m:lifecycle_cursor` | `503 audit_backlog` + `Retry-After: 60` on admin mutations |

Tests: `queue/deliveries_test.exs` (`dlq reaper`, `dlq reaper never evicts an
unarchived hook`, `quarantine retention`, `catalogue retention`),
`admin/audit_test.exs` (`outbox ceiling`), `admin/router_test.exs`
(`DELETE /v1/dlq`, `audit_backlog`).

#### Claim leases: a pack outlives its hook's last delivery

Store format 6 added the claim leases (`?l`, one row per pack and hook, value
`<<0::64>>` while the hook is live, else the grace expiry) and their counter
`claim_leases` (field 9 of `counters`; its migration leases every `?k` claim row
of a format 5 store, resumable in batches of 500). Reclaiming a hook turns its
lease into a grace lease in the batch that deletes the hook;
`Ankusa.ClaimCheck.Sweeper` deletes leases whose grace is over and never
deletes a leased pack. See [claim-check.md](claim-check.md#claim-leases). A
format 6 store cannot be opened by a format 5 image.

Tests: `claim_check/sweeper_test.exs` (`lease blocks sweep`),
`queue/deliveries_test.exs` (the sink-down drill), `store_upgrade_test.exs`
(`the claim leases migration (format 5 -> 6)`).

#### Segment rows: how an archived segment is stored

Store format 7 added `format`, `codec`, `node`, `dt` and `sha256` to the
catalogue's segment rows (`?S`). Its migration marks every row of a format 6
store `format: 0` (bare frames under `seg/`, no node or day; `codec: nil` reads
as the configured codec), 500 rows a batch and resumable. Segments written by a
format 7 node are `format: 1` (header and sha256, the dated layout). A format 7
store cannot be opened by a format 6 image: it would read the dated segments'
frames at offsets that include a header it does not know.

Tests: `store_upgrade_test.exs` (`the segment rows migration (format 6 -> 7)`),
`codec_test.exs` (`versioned header`).

### `:none`: no queue, no store hooks

`wal: :none` is the other ack path: no hook queue, no batcher, no compactor, no
dispatch pipeline, no DLQ. Ingest verifies, publishes to the source's sinks
inside the request (`Ankusa.Edge.Publish`), and answers `201` only once every
sink has confirmed. `c:Ankusa.Sink.durable?/1` is the promise that makes a
sink's `:ok` mean "something that outlives this node accepted it" — true for
every shipped sink except `Sink.Log` and `Sink.Redis` — and a `wal: :none`
node refuses a source with no durable sink: at boot for a static one, on write
for one managed through the admin API. One durable
sink is enough to pass that check, but *every* sink still has to confirm, so a
non-durable sink that cannot (Redis with no subscriber) turns every ingest
into a `503`. Nothing is committed and no segment is written, so the two-tier
story in this document does not apply. The node still runs `Ankusa.Store` for
the quarantine pen, API-managed sources and rate-limit overrides. See
[`delivery.md#direct-mode`](delivery.md#direct-mode) and
[`config-examples/direct.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/direct.yml).

### The default, single-node

The default `wal: :disk` runs the store on local disk, with no external
dependency beyond RocksDB itself. Every store-backed role (`edge`, `dispatch`,
`storage`) reads that same database, and RocksDB is single-process, so they
must all run in **one** BEAM node; splitting them across processes or hosts is
not supported. It is durable to process crash and power loss **on that box**,
not to losing the box — unless [`backup`](#backup-and-restore) is on. See
"Scaling out" below.

```elixir
config :ankusa, wal: :disk   # the default
```

### Backup and restore

`backup.enabled` (off by default) turns on `Ankusa.Store.Backup`, which closes
the gap above to `backup.interval_ms` (60 s): what a lost host takes with it
is the hooks acked since the last backup.

- **Backup.** Every interval the store writes a RocksDB checkpoint (memtables
  flushed, files hard-linked under `<data_dir>/<instance>/store.checkpoint`,
  in a process of its own so `/ready` keeps answering), and the uploader puts
  it in the object store — `backup.blob_store`, else `storage.blob_store` —
  under `storage.key_prefix`: `backup/sst/<name>` for every `.sst` and
  `.blob` file (immutable, so each is uploaded once and shared by every
  backup that lists it), `backup/<id>/<name>` for the manifest, options, WAL
  and `CURRENT`, then `backup/<id>/manifest.json` (every file with its size
  and sha256, the store format version, the intake offset, and with
  `backup.signing_key` an HMAC-SHA256 signature over all of it), then
  `backup/LATEST`. A backup exists only once `LATEST` names
  it. After each one, backups beyond `backup.keep` (3) and every shared file
  no kept backup lists are deleted; a purge that can't list the store or read
  a kept manifest deletes nothing. A failed attempt is retried with the
  compactor's backoff and never touches the store or the edge. Each
  object-store call is bounded by `backup.timeout_ms`.
- **Signed manifests.** With `backup.signing_key` (a string or a list; the
  first signs, any verifies, so a rotation lists the new key first) the
  manifest carries `signature`, computed over a canonical JSON form of
  everything else in it. A restore, and `Backup.verify/2`, refuse a manifest
  that is unsigned or signed by no configured key
  (`{:manifest_invalid, id, :unsigned | :bad_signature}`), so whoever can
  write the bucket cannot make a node restore content it did not back up.
  `LATEST` itself is a bare id and is not signed: it can be pointed at an
  older backup this node signed, never at forged content. With no key
  configured nothing is checked (a manifest that is signed anyway is logged as
  unverified), and a backup taken before signing was turned on is refused
  once it is, until the next backup replaces it. The signing key is not
  stored in the bucket; a replacement node needs it in its config like the
  rest of the backup block, and needs `secrets.kek` too: the backup holds
  stored source secrets as ciphertext.
- **Verify.** `Ankusa.Store.Backup.verify/2` reads `LATEST` back from an
  object store, checks the manifest (signature included), then downloads every
  file it lists, one at a time, against its size and sha256, writing nothing.
  `backup.verify_interval_ms` runs it for every store on that interval;
  a failed check is logged at error level and counted by
  `ankusa_backup_verify_failed_total{target}` — alert on it, a backup nobody
  has read back is a hope.
- **A clean stop is a backup.** SIGTERM (or any `:shutdown` of the instance)
  takes one last backup after the edge and dispatch have stopped and before
  the store closes, bounded by `backup.final_timeout_ms` (20 s): the
  recovery point of a node that was drained and stopped is the stop, not the
  last periodic backup. A final backup that fails or times out is logged and
  the periodic one stays `LATEST`. A node that crashes or is killed takes
  none.
- **A second region.** `backup.secondary` is another object store (same
  `storage.key_prefix`). Every backup is written to the primary, then to it,
  each with its own purge; a run succeeds only when both hold it, and a
  failing secondary is a failed run (backoff, `ankusa_backup_runs_total{result="error"}`)
  that does not make the primary upload everything again. At restore the
  primary is tried first; unreachable, empty, or failing a check, the
  secondary is restored from instead (`restored from the SECONDARY backup` is
  logged at warning level). With both unusable the boot is refused with the
  primary's reason.
- **Freshness.** With `backup.max_age_ms`, `/ready` says `"backup": "fresh"` or
  `"stale"` (last successful backup older than that, or the uploader not
  running); a stale backup only turns the node unready with
  `backup.require_fresh: true`.
- **Restore.** A store directory with no `CURRENT` file holds no database. With
  `backup.enabled`, `Ankusa.Store` then restores `LATEST` before opening:
  every file is checked against the manifest's size and sha256 and written
  durably, `CURRENT` last, so a restore that fails part-way leaves no database
  and the next boot starts it over. No `LATEST` at all starts an empty store.
  A backup location that can't be read, a manifest that doesn't parse or a
  file that fails its checksum refuses the boot
  (`{:store_restore_failed, path, reason}`): an empty directory with a backup
  configured is never taken as empty. Database files (`MANIFEST-*`, `.sst`,
  `.blob`, `.log`) without a `CURRENT` that no interrupted restore left behind
  are refused too (`{:unrecognized_store_files, names}`), never deleted: that
  may be a damaged store worth recovering by hand.
- **The archive.** A restored (or newly created) store may be behind the
  segments in the bucket: hooks acked after the backup may already be
  archived. Before the queue writer starts, `m:next_seq` moves past the
  highest archived seq, so no new segment key overwrites one, and every
  of this node's segments without a catalogue row is catalogued from its
  `.idx` object and its header (`Archive.reconcile/2`, see
  [Finding a segment without the catalogue](#finding-a-segment-without-the-catalogue);
  with the restored hooks' archive obligations it settles), so `Ankusa.Storage.fetch/2`
  finds those hooks again. Their delivery state is gone with the host. The
  restore leaves a `RESTORE-IN-PROGRESS` marker beside `CURRENT` until this
  reconcile succeeds: a boot that finds both (the node died, or the reconcile
  failed, in between) logs "reconciling now" and reconciles before serving,
  instead of opening the database as an existing, reconciled store.
- **A restore is a new incarnation.** The restored trail resumes at the
  backup's audit `seq`, so it re-issues sequence numbers (and per-subject
  revisions) the lost node may already have published as lifecycle events.
  Before it takes any change, the restored store writes a fresh `m:incarnation`
  and announces it as `io.ankusa.instance.restored` (`incarnation`,
  `restored_sequence`); event ids from then on are keyed with it, so a
  re-issued `seq` is a different event id, and consumers that track the highest
  `sequence`/`revision` per instance reset them on that event. Events of the
  restored trail itself keep the ids they were published under (proof:
  `lifecycle_test.exs`, "opens a new incarnation"). See
  [`asyncapi.md`](asyncapi.md). A checkpoint is the whole database, every
  column family, so the audit trail, the lifecycle cursor, the counters and
  the incarnation history come back with it (only the audit trail and the
  incarnation history are asserted by a test).
- **Where to point it.** With `LocalFS` the backup lives on the same disk as the
  store, and the node logs a warning saying so. Use a bucket: `storage.type:
  s3|gcs`, or a `backup.store` of its own. Backups are per node, so nodes
  sharing a bucket need distinct `storage.key_prefix`es, exactly as for
  segments. Each store gets an id (`m:store_id`) on its first backup, carried
  by every checkpoint and recorded in each manifest; a node whose prefix
  already holds the latest backup of another store refuses to back up (no
  upload, no purge), logs `backup refused … already holds backups of store
  …` at error level, and counts it as a failed run. A replacement node takes
  over by starting with the same config on an empty volume: it restores the
  latest backup and inherits the id. If the old host comes back beside its
  replacement, both hold that id; the first of them to find `LATEST` moved
  by the other (after it has backed up once itself) refuses with
  `backup refused: … a backup of this store that this node did not write`.
  Retire one of them and restart the one you keep (it stays refused until
  then).
- **Bucket lifecycle rules.** Keep `<key_prefix>backup/` out of every
  expiration rule. Retention is `backup.keep`; a shared `backup/sst/*` file is
  listed by every later backup that still uses it, whatever its age, and an
  expired one makes the restore fail (`{:download_failed, key, _}`) and the
  node refuse to boot.
- **Costs.** An upload reads one file into memory at a time; RocksDB files here
  are bounded by the write buffers (64 MiB by default; `queue.rocksdb.write_buffer_bytes`
  raises them, and with them the memory of an upload) and the 64 MiB blob file
  size the store sets. Each backup uploads the files written since the previous
  one, plus a few small per-backup files.
- **Watching it.** `ankusa_backup_age_seconds` is the time since the last
  successful backup — the data a lost host would take with it —
  `ankusa_backup_runs_total{result}` counts attempts, and
  `ankusa_backup_verify_failed_total{target}` counts verifications that failed.

## The intake log

Under `wal.type: dual` ([ADR 0002](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0002-dual-commit-intake-log.md)) the
local store is not the only copy of an acked hook: the hook is also a record in
a replicated log (a Kafka topic, `Ankusa.Intake.Kafka` from `ankusa_kafka`), and
the `201` waits for both. What that adds to the store is four **meta rows** in
the `default` family, nothing else, so the store format is unchanged and a
release that does not know them ignores them:

| Row | Holds |
| --- | --- |
| `m:intake_offset` | the highest log position whose record is **also in this store, durably**. Every checkpoint carries it and every backup manifest records it (`intake_offset`); it never runs ahead of the checkpointed store |
| `m:intake_seq` | the watermark: the highest seq such that every seq at or below it was confirmed in the log or had no record. Boot recovery republishes the local hooks above it |
| `m:intake_node` | the node id (`intake.node_id`) the store was stamped with; a different configured id refuses boot (`{:intake_node_mismatch, stamped, configured}`) |
| `m:intake_partition` | the log partition this node writes (Kafka: `phash2(node_id) rem partition_count`, chosen once and never recomputed, so growing the topic never remaps a node) |

**The record** is the exact `hooks` column family value (`Envelope.to_binary/2`,
itself versioned) under the node's seq, with headers `ankusa_intake_v` (`1`),
`ankusa_intake_kind` (`hook`), `ankusa_intake_node`, `ankusa_intake_seq`,
`ankusa_intake_id`, `ankusa_intake_incarnation`, `ankusa_intake_tenant` and
`ankusa_intake_source`; the Kafka key is the node id. The topic therefore holds
customer payloads and needs the confidentiality of the archive bucket (TLS,
ACLs, encryption at rest on the brokers' volumes). A reader skips, and counts, a
version or kind it does not know.

**What the backup and the log each do.** The recovery point of an acked hook is
*the backup, plus the log since the backup's `intake_offset`*. The backup bounds
replay time and carries everything the log does not hold (delivery state, DLQ,
quarantine, API-managed sources, rate-limit overrides, the audit trail, the
archive catalogue, `m:incarnation`) and any backlog older than the log's
retention; the log carries every hook since. That is why `backup.enabled` and
`backup.max_age_ms` are **required** under `dual`, and why the topic's
`retention.ms` must be at least `2 x backup.max_age_ms` (`verify/1` refuses
otherwise): a restore of a backup as old as `max_age_ms`, from the secondary
bucket if the primary is gone, still finds its offset in the log.

**Recovery.** A replacement node starts with the lost node's `intake.node_id` on
an empty volume. It restores the latest backup (primary, then secondary; the
manifest signature is checked), mints a new incarnation, then replays the log
for its node id from `m:intake_offset + 1`, in chunks of 500 in one synced batch
each. A record is applied through **one rule, keyed by seq and hook id**: no row
at `hooks/<seq>` writes the hook under its original seq (delivery rows from the
source's *current* config, dedupe keys, counters, `m:next_seq` raised); the same
id is skipped; a *different* id is a seq collision, written under a fresh seq
and logged, never over the other hook. A duplicate, a reordered or a
twice-replayed record changes nothing, which is what lets the Kafka adapter live
without an idempotent producer. Then the local hooks above `m:intake_seq` are
republished and the node reports ready. The same code runs by hand against a
stopped store as `mix ankusa.recover` (the server image: `docker run … recover`),
which prints `records=… applied=… present=… collided=… unroutable=…
skipped=… republished=…`, accepts `--dry-run` (read and count, write nothing),
and changes nothing the second time.

**A truncated log.** If the log's earliest position is above the restored store's
`offset + 1` (the topic's retention ran out before this backup was restored),
boot refuses with `{:intake_truncated, wanted, earliest}` and the node stays
unready: hooks acked in that gap are not recoverable, and only an operator may
accept that. `--allow-gap` does, logs the lost positions and writes the
`intake.recover.allow_gap` row (`wanted`, `earliest`, `lost_positions`, the
node id and the actor) to the node's own audit trail. On an ordinary restart a
gap is a warning only: every hook in it is in the local store.

Operating it is [`deployment.md`](deployment.md#host-loss-and-the-intake-log);
its configuration is [`configuration.md`](configuration.md).

## Encryption at rest

What rests where, and who encrypts it:

| Data | Where | Encrypted by |
| --- | --- | --- |
| Hook bodies, delivery rows, the archive catalogue | this node's RocksDB files | **the volume** (EBS, LUKS, a CSI class with encryption): Ankusa does not encrypt bodies |
| Stored source specs: verify secrets, sink credentials, header values | the `s:` rows of that store, in every backup of it; or Redis (`source_store.type: redis`) | **Ankusa**, with `secrets.kek` (below) |
| Segments, backups, claim packs | the object store | **the object store**, asked to by `sse:` on every `put/4` (below) |

The startup log states it in one line, so an operator reads what a node
promises without reading config: `store files rely on volume encryption;
secrets envelope-encrypted with key <id>` (or `secrets are stored in plaintext
(no secrets.kek is configured)`; with `secrets.kek_provider` it names the
provider, since the key is resolved when the source store starts). Body-level
encryption is deliberately not offered: add it only if a compliance review
demands it.

**Secrets.** With a key-encryption key (`secrets.kek`, or a `kek_provider`) the
secret-bearing fields of a source spec are encrypted with AES-256-GCM before
`Ankusa.SourceStore.Persistent` writes the row (or `Ankusa.SourceStore.Redis`
the hash entry), and decrypted as the row is loaded. Which fields is not a
second list to maintain: it is the inverse of what the admin API hides
(`Ankusa.Admin.Redact`) — header values, anything under a key that is not on
its shown-key allowlist, a URL carrying credentials — so the type, topic,
region and signature header of a stored source stay readable and diffable
while `verify.secret` and a sink's password do not. A value is
`<<"AENC", 1, key_id_len, key_id, iv::96, tag::128, ciphertext>>` (base64 in
the JSON, behind `AENC:`), its key id authenticated with it
([`Ankusa.Crypto.Envelope`](https://hexdocs.pm/ankusa/Ankusa.Crypto.Envelope.html)).

- **A row no key opens refuses the boot** (`{:source_store_load_failed,
  {:secrets, source_id, reason}}`, and the log names the source). It is never
  skipped: a node that booted without it would `404` every hook for that
  source. A Redis node has the same rule at first boot; on a later reload it
  keeps its mirror and logs the entry.
- **Rows written before there was a KEK** load as they are, and
  `rewrap/1` encrypts them.
- **Rotation.** Put the new key in `secrets.kek` and the old one in
  `secrets.kek_previous` (keys that decrypt, never encrypt), roll the nodes,
  move every row (`Ankusa.SourceStore.Persistent.rewrap/1` or
  `Ankusa.SourceStore.Redis.rewrap/1` on a running node, or `mix
  ankusa.secrets.rewrap` on a stopped node's data dir with `ANKUSA_KEK` and
  `ANKUSA_KEK_PREVIOUS`), then drop the old key. A rewrap is synced in
  batches: an interrupted one leaves every row readable under one key or the
  other.
- **Backups.** Backed-up stores carry the same ciphertext, so restoring needs
  the same keys; keep them where the backup is not (a secrets manager, the
  provider's KMS), not on the volume.
- **Not covered.** Sources in the configuration file (`sources:`) are not
  stored and are not touched; they hold `${VAR}` references. A KEK is in
  memory in the node, as any key is.

**Object stores.** `sse:` in the options of an object-store adapter is sent on
every `PUT` it makes: `S3` as `x-amz-server-side-encryption: AES256 | aws:kms`
(+ `…-aws-kms-key-id`; signed with the request), `GCS` as
`x-goog-encryption-kms-key-name` (and the `kmsKeyName` upload parameter),
`Azure` as `x-ms-encryption-scope`, `OCI` as `opc-sse-kms-key-id`. Because
segments, backups and claim packs all reach the object store through that
adapter's `put/4` with the options of the store they are configured on, one
`sse:` per configured store covers them; a deployment that gives backups or
claims their own store (`backup.store`, `claim_check.stores`) sets it on each.
On GCS, Azure and OCI every object is encrypted at rest regardless, so
`type: aes256` sends nothing and only `kms` (a customer-managed key) changes
anything. A misspelled option, or `kms` without the `key_id` the provider
needs, is a boot error, never an unencrypted object. The test is
`sse header on put` in
`test/ankusa/store/backup_s3_integration_test.exs`: a segment, a backup and a
claim PUT recorded on their way to the emulator all carry the header.

## Scaling out

One node is one local store. To scale, run **N independent all-role
nodes behind a load balancer**. Each with its own data volume and its own
DLQ/admin API. Segment keys are `archive/v1/dt=<day>/<node>/seg-<first_seq>-<last_seq>.seg`
(with the sibling `.idx` object and the day's `_manifest.json`), where `<node>` is
`intake.node_id` (else the host name): nodes with distinct names never write the
same key, and a node that has lost its catalogue can find another node's
segments by day. Set `intake.node_id` on every node that shares a bucket (the
host name is the fallback, and two nodes with one host name would overwrite each
other's segments), or give each its own `storage.key_prefix` (`node-a/`,
`node-b/`), its own bucket or LocalFS root. Claims are never
prefixed — pack ids carry 64 random bits, so every node's claims can share
one place and one gateway. See [`deployment.md`](deployment.md) for the
operational shape.

Under `wal: :none` none of this applies: there is no queue, no segment story
and no data volume — replicas are freely interchangeable, and the broker (or
whatever answers the sink) is the only shared state. See
[`architecture.md#4-stateless-ingest-fleet-wal-none`](architecture.md#4-stateless-ingest-fleet-wal-none).

## `Ankusa.BlobStore`

```elixir
@callback put(instance, key :: String.t(), data :: iodata(), opts :: keyword()) :: :ok | {:error, term()}
@callback put_if_absent(instance, key, data, opts) :: :ok | {:error, :already_exists} | {:error, term()}
@callback get(instance, key, opts) :: {:ok, binary()} | {:error, term()}
@callback get_range(instance, key, offset, length, opts) :: {:ok, binary()} | {:error, term()}
@callback delete(instance, key, opts) :: :ok
@callback list(instance, prefix :: String.t(), opts) :: {:ok, [String.t()]} | {:error, term()}
```

Callers go through the facade, which names a **scope** on every call —
`Ankusa.BlobStore.put(instance, :segments | :backup, key, data)` and so on.
`:segments` resolves `storage.blob_store`. `storage.key_prefix` is applied
once, when the compactor names a new segment
(`Ankusa.BlobStore.object_key/3`), and the archive catalogue records the full
key, which every later read uses verbatim: changing the prefix moves new
segments only and never strands archived ones. `list/3` lists this node's
segments under its prefix and returns full keys. Claim packs bypass the
facade: `Ankusa.ClaimCheck.Stores` writes and reads them across
`claim_check.blob_stores` (`claim_check.stores` in YAML; failing over between stores), never prefixed.
Adapters see full keys and stay prefix-unaware.

`list/3` returns `{:ok, keys}` or `{:error, reason}` — a listing that failed
halfway is an error, never a shorter list — and every remote adapter follows
its store's pagination (S3 continuation tokens, GCS page tokens, Azure
markers, OCI `nextStartWith`) to the end.

`get_range/5` exists because the compactor never writes one object per
hook. Segments hold many records, and a single range `GET` reads exactly
one record's bytes back out.

**`:not_found` is part of the contract for every adapter**: `get/3` and
`get_range/5` return `{:error, :not_found}` for a missing key, never a
store-specific error (`:enoent`, an HTTP status). `Ankusa.ClaimCheck`
(see [`claim-check.md`](claim-check.md)) depends on this to map a missing
claim consistently regardless of which store is configured.

Two independent namespaces share one `BlobStore` by default and never
collide: `archive/v1/...` (each segment written by `Ankusa.Storage.Compactor`,
its sibling `.idx` index object and the node-day `_manifest.json`, under
`storage.key_prefix`; `seg/...` is where releases before DUR-8 wrote them) and `claims/...`
(`Ankusa.ClaimCheck`, packed per tenant per dispatch batch, one object holding
many claims under `claims/tenant=<t>/dt=<day>/<pack_id>`, or
`claims/tenant=<t>/shard=<c>/dt=<day>/<pack_id>` with
`claim_check.key_layout: :sharded`; or in stores of their own with
`claim_check.blob_stores`). Retention differs per namespace too. See
[`claim-check.md#retention`](claim-check.md#retention).

| Adapter | Deps | Notes |
| --- | --- | --- |
| `BlobStore.LocalFS` | none | Default. Durable writes: temp file fsync, rename, directory fsync; `put` returns `{:error, reason}` instead of raising. `get_range` uses `:file.pread/3`, never slurps the whole segment. |
| `BlobStore.S3` | `aws_signature` + `req` | SigV4 signing via [`aws_signature`](https://hex.pm/packages/aws_signature), the implementation behind the official aws-elixir SDK, with HTTP through `Req`. Path-style addressing works unmodified against AWS, MinIO, Cloudflare R2, and the [floci](https://floci.io) emulator. `list/3` parses `ListObjectsV2` XML via stdlib `:xmerl`. Credentials: `:access_key_id`/`:secret_access_key` (+ `:session_token`), else `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`, else web identity (IRSA: `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`, through STS `AssumeRoleWithWebIdentity`), else the EC2 instance profile (IMDSv2); temporary credentials are cached until shortly before they expire. |
| `BlobStore.GCS` | `req` | GCS JSON API. `:token_provider` opt (an MFA returning `{:ok, bearer_token}`) is required against real GCS: `{Ankusa.BlobStore.GCS.MetadataServer, :token, []}` on GCE/GKE (the metadata server's token, cached until a minute before it expires), or whatever your deployment already uses (Goth, ADC). The adapter carries no OAuth2 dependency of its own. Unauthenticated against the `floci-gcp` emulator. |
| `BlobStore.Azure` | `req` | Azure Blob REST. Carries **no credential dependency**, the same stance as GCS: a pre-generated `:sas_token` (Shared Access Signature), or a `:token_provider` MFA, including the built-in `Ankusa.BlobStore.Azure.ManagedIdentity`, the best credential for a service running on Azure (no secret, short-lived Entra ID tokens from IMDS). No Shared-Key signing of its own. Unauthenticated against the `floci-az` emulator. |
| `BlobStore.OCI` | none | OCI Object Storage. The one adapter that signs its own requests, OCI has no bearer/SAS shortcut covering arbitrary `put`/`get`/`list`, using OTP's `:public_key` (RSA-SHA256 *Signature version 1*), no dependency. Two credential shapes: a static API key (`:tenancy_ocid`/`:user_ocid`/`:key_fingerprint`/`:private_key`), or an instance principal: `key_provider: {Ankusa.BlobStore.OCI.InstancePrincipal, :credentials, [[region: "us-ashburn-1"]]}` runs the certificate federation (IMDS leaf cert, `auth.<region>.oraclecloud.com/v1/x509`, a short-lived `ST$` session token and key), caches it and refreshes it before it expires. A session token from the OCI SDK still works as `:key_id: "ST$<token>"` + `:private_key`, renewed by the caller. Signing is pinned against OCI's reference vectors in `test/ankusa/blob_store_oci_signing_test.exs`; `floci-oci` parses but never verifies the signature, so any locally generated key works there. |

### Conditional writes, retries, checksums, multipart, pools

**`put_if_absent/4` is how claim packs are written.** It is `put/4` that refuses
to replace: S3 and Azure send `If-None-Match: *`, GCS `ifGenerationMatch=0`,
OCI `if-none-match: *` (so the check and the write are one atomic request) and
the answer for an existing key is `{:error, :already_exists}`, the original
untouched. `LocalFS` checks for the file first (one node's keys are unique by
construction). An object that already holds *exactly* the bytes being written is
`:ok` (`Ankusa.BlobStore.same_object/5`, one read-back on the rare collision), so a
write whose first attempt landed and whose answer was lost is not mistaken for
another writer's. `Ankusa.ClaimCheck.check_in/4` uses it: a refused pack is
written again under a **fresh pack id** (at most 3 attempts, then
`{:unavailable, :pack_id_collision}`), so a reused or colliding id can never
overwrite claims that are already referenced
([`claim-check.md`](claim-check.md#storage)). Segments and backups keep plain
`put/4`: their keys are unique per node and rewritten on purpose.

**The HTTP adapters retry a transient failure** (`Ankusa.BlobStore.Retry`): a
transport error (refused, reset, timed out) or a `408`, `429`, `500`, `502`,
`503` or `504` is sent again after a **full-jitter** wait (a random time from none
to 200 ms, then to 1 s, then to 5 s), three attempts in all by default, and S3
and OCI re-sign each attempt. Anything else (`404`, `403`, another `4xx`) is
answered at once. The adapter option `:retries` is the number of extra attempts
(`2`; `0` for a single attempt). A `429` or `503` that carries a `Retry-After` (seconds
or an HTTP date) makes the wait at least that long **if it fits in the time
left**; if it does not, nothing sleeps and the call returns
`{:error, {:throttled, seconds}}` at once, for the caller (dispatch, which has
its own backoff) to schedule. Each call is idempotent, a `PUT` writing the same
bytes to the same key, so a timed-out write that did land is safe to repeat. `put`
hands the adapter's iodata to the HTTP client as is: a segment is not copied
into one binary first. `LocalFS` does neither: a POSIX error is not transient.

**One deadline per blob operation** (`op_deadline_ms`, default 120 s): every
attempt, every wait, every multipart part and, for `check_in/4`, every pack-id
retry and every store tried share one absolute deadline. An attempt runs under
the smaller of its timeout (`timeout_ms`, 10 s, for a request that carries at
most 1 MiB; `upload_timeout_ms`, 60 s, for one that carries more) and the time
left; nothing is sent past it (`{:error, :deadline}`). So a slow bucket costs a
dispatch task at most the deadline, never the product of the timeout, the retries,
the pack-id retries and an uncapped `Retry-After`. A claim read stops at
`claim_check.read_deadline_ms` instead.

**A breaker per `{endpoint, bucket}`** (`Ankusa.BlobStore.Breaker`, shared by every
store of the node that talks to that bucket) opens after `breaker_failures`
(5) consecutive failures **or** when the p99 of the last 100 metadata-sized
requests passes `breaker_slow_ms` (5 s), answers `{:error, :breaker_open}` without
a request for `breaker_open_ms` (30 s), then lets one probe through. A throttle
is an answer from a reachable store and never opens it; neither does a failure of
this node (its connection pool gone or exhausted). `breaker_failures: 0` turns it
off. Each transition emits `[:ankusa, :blob_store, :breaker]`.

**Checksums.** Every `PUT` carries a digest the store validates (S3
`x-amz-checksum-sha256`, GCS `x-goog-hash: md5=…`, Azure `Content-MD5` and
`x-ms-blob-content-md5`, OCI `Content-MD5`), and a whole-object `GET` compares the
digest the store sends back with the bytes received: a mismatch is `{:error,
:integrity}`. A response with no digest (or an S3 composite one) is not checked; a
range read is not (`Ankusa.BlobStore.Checksum`). The GCS header is the
documented way to have GCS validate an upload; only the floci emulator has
exercised it here.

**Multipart above `multipart_threshold_bytes`** (8 MiB): a `put` of more bytes is
cut into `multipart_part_bytes` (8 MiB; at least 5 MiB, a multiple of 256 KiB)
pieces: an S3 or OCI multipart upload (create, parts each with `Content-MD5`,
complete or commit, abort on failure), Azure Put Block per piece and one Put
Block List, a GCS resumable upload in chunks. Each part has its own
`upload_timeout_ms` inside the operation's one deadline, so a 16 MiB pack on a
1 MiB/s link is two 8 MiB requests, not one that outlives a 10 s timeout
(`test/ankusa/blob_store_multipart_test.exs`). A conditional upload that loses
the race is aborted.

**A connection pool per endpoint** (`Ankusa.BlobStore.Pool`): the instance
starts one Finch pool for each distinct endpoint of `storage.blob_store`,
`backup.blob_store`, `backup.secondary` and `claim_check.blob_stores`, sized by
`pool_size` (32 connections), `pool_count` (1) and `pool_timeout` (5 s to check
one out), instead of Req's shared default, which is the throughput ceiling and
lets one slow bucket take connections from every other client. It verifies
peers (TLS 1.2/1.3, the system trust store, or the store's `cacertfile`). A store
with its own transport (`req_options: [finch: [name: MyFinch]]`) gets none.

**Refreshing credentials.** On a `401` the adapter asks the `:token_provider` /
`:key_provider` module to drop its cached token (`invalidate/n`, when it exports
one) and sends the request once more with a fresh one. The built-in providers
are `Ankusa.BlobStore.GCS.MetadataServer`, `Azure.ManagedIdentity` and
`OCI.InstancePrincipal`; each also refreshes before the token expires.

The transport options are Elixir config on the store (see the table in
[`configuration.md`](configuration.md#library-configuration-elixir)) and, for `s3`
and `gcs`, YAML keys of the same names under `storage.s3`, `storage.gcs` and the
`s3`/`gcs` block of `claim_check.store(s)` and `backup.store`.

**The HTTP adapters take `sse:`** — server-side encryption on every `put/4`
(`%{type: :aes256}`, or `%{type: :kms, key_id: key}`): see
[Encryption at rest](#encryption-at-rest) for what each provider receives.

```elixir
# S3 / MinIO / R2
config :ankusa,
  storage: %{
    blob_store:
      {Ankusa.BlobStore.S3,
       bucket: "ankusa-segments", region: "us-east-1",
       access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
       secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY")}
       # endpoint: "http://localhost:4566"  # only for MinIO/R2/floci; omit for real AWS
  }

# GCS
config :ankusa,
  storage: %{
    blob_store: {Ankusa.BlobStore.GCS, bucket: "ankusa-segments", token_provider: {MyApp.Auth, :gcs_token, []}}
  }

# Azure Blob Storage: a pre-generated SAS ...
config :ankusa,
  storage: %{
    blob_store:
      {Ankusa.BlobStore.Azure,
       account_name: "myaccount",
       container: "ankusa-segments",
       sas_token: System.get_env("AZURE_BLOB_SAS")}
       # endpoint: "http://localhost:4577/devstoreaccount1"  # only for floci-az; omit for real Azure
  }

# ... or, on Azure, the managed identity (system-assigned, no secret at all)
config :ankusa,
  storage: %{
    blob_store:
      {Ankusa.BlobStore.Azure,
       account_name: "myaccount",
       container: "ankusa-segments",
       # user-assigned? add: client_id: System.get_env("AZURE_CLIENT_ID")
       token_provider: {Ankusa.BlobStore.Azure.ManagedIdentity, :token, []}}
  }

# OCI Object Storage: signs its own requests with an API signing key
config :ankusa,
  storage: %{
    blob_store:
      {Ankusa.BlobStore.OCI,
       region: "us-ashburn-1",
       namespace: System.get_env("OCI_NAMESPACE"),
       bucket: "ankusa-segments",
       tenancy_ocid: System.get_env("OCI_TENANCY"),
       user_ocid: System.get_env("OCI_USER"),
       key_fingerprint: System.get_env("OCI_KEY_FINGERPRINT"),
       private_key: File.read!(System.fetch_env!("OCI_KEY_FILE"))}
  }

# ... or, on OCI compute/OKE, the instance principal (the OCI SDK does the
# IMDS → x509 federation; feed its output back here)
config :ankusa,
  storage: %{
    blob_store:
      {Ankusa.BlobStore.OCI,
       region: "us-ashburn-1",
       namespace: System.get_env("OCI_NAMESPACE"),
       bucket: "ankusa-segments",
       key_id: System.get_env("OCI_SESSION_TOKEN_ID"),   # "ST$<token>"
       private_key: File.read!(System.fetch_env!("OCI_SESSION_KEY_FILE"))}
  }
```

Local dev/test emulators via [floci](https://floci.io) (no cloud account):

```sh
# floci (S3, :4566) + floci-gcp (GCS, :4588) + floci-az (Azure, :4577) + floci-oci (OCI, :4599),
# buckets/containers auto-created
docker compose -f packages/ankusa/docker-compose.integration.yml up -d
(cd packages/ankusa && mix test --only integration)
docker compose -f packages/ankusa/docker-compose.integration.yml down -v
```

Or `mise run test:integration`, which does all three.

## `Ankusa.Codec` and segment format

```elixir
@callback encode([%{key: String.t(), payload: binary()}]) :: {segment :: binary(), index :: [index_entry]}
@callback decode_record(binary()) :: {:ok, binary()} | {:error, term()}
@callback id() :: 1..255   # optional
```

`Codec.Raw` (the only shipped codec, id `1`) frames each record length-prefixed
with a per-record CRC32; `encode/1` packs many into one binary and returns the
byte offset + length of each, which is exactly what the `get_range` callback
needs. A codec only frames records: the object around them is
`Ankusa.Storage.Segment`.

**Segment object, format 1** (written since store format 7):

```text
"ANKSEG" <> <<version::8, codec_id::8, count::32>>      12 bytes
<codec frames, back to back>
sha256(everything above)                                32 bytes
```

`codec_id` is the codec's `id/0` (`0` when it has none; the catalogue row then
names the module), `count` the number of frames. The index offsets are
positions in the object, header included, so a range read of
`[offset, offset + length)` is still exactly one frame. `Storage.fetch/2` reads
one frame and relies on its CRC; whoever reads the whole object (archive
replay, `Segment.verify/2`) checks the sha256 too, and a segment that does not
verify is `{:error, _}` — a replay logs it and skips the segment.
**Format 0**, the bare frames earlier releases wrote under `seg/`, has no header
and stays readable; a format newer than this release is `unsupported_format`.
Version N reads N−1: the catalogue row's `format` field says which one a segment
is.

**Index object** (`….idx`, `Ankusa.Storage.Index`): `%{event_id => {offset,
length, seq}}` as `"ANKIDX" <> <<version::8, count::32>>`, `term_to_binary`, and
a sha256 trailer. It is decoded with `binary_to_term(bin, [:safe])` and its
shape checked (binary ids, three non-negative integers each, `count` entries),
so an index that names an atom this node has never seen, or anything that is not
an index, is `{:error, :bad_index}` instead of being interned or executed.
Format 0 indexes (a bare term) are read the same way.

Golden vectors of both formats are in `test/ankusa/codec_test.exs`
(`versioned header`, `safe idx`); a format change adds its vectors there.

## `Ankusa.Storage.Compactor`: how segments get written

A hook committed while the `:storage` role runs carries an *archive
obligation*. Each tick (default every `storage.interval_ms`, 1s) clears some:

1. Read archive obligations past the compactor's own position, in `seq` order,
   taking them until their stored sizes reach `storage.roll_bytes` (default
   16 MiB) — always at least one, so a hook larger than a segment still gets
   one. Sizes come from the obligation keys, so nothing is read and discarded
   to find out how much fits. A long backlog produces **several segments in
   one tick** (at most `storage.max_segments_per_tick`, default 16, the rest
   wait for the next tick). What is left after the full ones is a *partial*
   segment: it is written once its first obligation has waited
   `storage.roll_ms` (default 30 s; `0` writes every tick), so an idle trickle
   becomes one segment per `roll_ms` instead of one per tick. `tick/1`
   (tests, `Compactor.tick/1`) writes a partial segment at once.
2. Encode those hooks into one segment via the configured `Codec`, wrapped in
   the header and sha256 (above).
3. `PUT` the segment to the blob store under a deterministic key in the dated
   layout: `archive/v1/dt=<yyyy-mm-dd>/<node>/seg-<first_seq>-<last_seq>.seg`
   (seqs zero-padded to 20 digits; `dt` is the UTC day of the segment's oldest
   hook, from its UUIDv7 id; `<node>` is `intake.node_id`, else the host name).
4. `PUT` the segment's index object, the same key with `.idx`.
5. Rewrite the node-day's `_manifest.json`: segment count, bytes, seq and id
   range, a constant-size summary that lets a reader skip a node whose ids
   cannot hold the one it wants.
6. Write the segment's catalogue row to the store: key, index key, seq and id
   ranges, count and byte size, and `format`, `codec`, `node`, `dt`, `sha256`.
7. Clear the batch's obligations. Each cleared obligation leaves a marker; when
   a hook's last obligation is gone, the hook is deleted.

A failed blob write ends the tick without crashing: nothing moves, and the
same hooks are written again under the same keys, after `storage.interval_ms`
doubled per consecutive failure (jittered, capped at 60 s). This is why
segments are never one-object-per-hook: PUT cost amortizes over however many
records landed in one tick, and archive-tier storage (which bills a minimum
object size) stays cheap at scale.

### Finding a segment without the catalogue

The day comes before the node in the key, so a node that lost its catalogue (a
fresh node, a restore) or never had it (another node's segment in a shared
bucket) can still find a hook whose id carries a time:

- **`Storage.fetch/2` falls back to a day-prefix scan** when the catalogue has no
  segment for the id: it lists the id's day and the day before under
  `storage.key_prefix`, skips every node whose manifest says its ids cannot
  include the id, and bisects the remaining node's segments (by the highest id
  in each index) to the one or two that can hold it. That is a listing plus
  `O(log n)` index reads per fetch, not a lookup: use it for the tail, and
  `reconcile` to make it a lookup. A segment lives in the directory of its
  *oldest* hook, so a backlog segment that spans more than a day is found only
  for hooks in the first two days of it (the catalogue finds them all); an id
  that carries no time has no day and is found only through the catalogue.
- **`Queue.Archive.reconcile/2`** rebuilds catalogue rows from the
  prefix: every dated segment (`node: :all`, the default, or `:own`, or one
  node's name) and every legacy `seg/` segment that has no row at its first seq
  gets one, built from its index object and the first bytes of the segment, and
  the archive obligations it settles are cleared — only those whose hook is the
  one the segment holds for that seq, so another node's segment never clears
  this node's own obligations. A segment whose index or object is missing or
  does not decode is logged and left out. Rows are keyed by first seq, so a
  segment that starts at the same seq as a catalogued one of another node is
  counted in `conflicts` and left to the day scan; reconcile a node into a
  store before it archives, or accept the scan. The restore at boot is
  `reconcile(config, node: :own, advance_seq: true)`.

Tests: `storage_test.exs` (`dated layout`, `versioned header`, `safe idx`,
`time roll`, `storage.max_segments_per_tick`, `reconcile other node`,
`legacy seg/ layout`), `codec_test.exs` (golden vectors, tamper cases),
`store_upgrade_test.exs` (`the segment rows migration (format 6 -> 7)`).

## Replay by id

`Ankusa.Storage.fetch/2` resolves an event id through the catalogue: the
segment rows whose id range could hold it (ids are time-ordered, so that is a
segment or two), then each candidate's `.idx` object, then a range read of
exactly that record's bytes from the blob store. It decodes the frame back
into the original `%Ankusa.Envelope{}` with its `seq`. Hooks a 0.3 node
archived resolve through the legacy locations imported from its index log. It
reads no queue state, so it works on a storage node with dispatch off.

```elixir
{:ok, envelope} = Ankusa.Storage.fetch(:default, event_id)
```
