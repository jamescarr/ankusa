# ADR 0001: Tenant in the store key layout

- Status: accepted, 2026-10-18
- Plan: `TEN-1.a`; implemented by `TEN-1.b`; consumed by `TEN-3`, `TEN-5`, `TEN-7`, `DEL-6`
- Store format: 2 → 3 (`Ankusa.Store.Migrate` registry entry `{2, 3, Ankusa.Store.Migrations.TenantKeys}`)

## Context

The queue's index keys say *when* and *which hook*, never *whose*. Today (store format 2, `Ankusa.Store.Keys`):

| Family | Column family | Key | Value |
| --- | --- | --- | --- |
| hooks | `hooks` | `<<seq::64>>` | envelope |
| delivery rows | `deliveries` | `<<seq::64, sink::16>>` | row map |
| due (pending and retrying) | `index` | `<<?d, at::64, seq::64, sink::16>>` | `<<size::32>>` |
| inflight | `index` | `<<?f, seq::64, sink::16>>` | `<<size::32>>` |
| dead (the DLQ) | `index` | `<<?x, at::64, seq::64, sink::16>>` | `term({source_id, id})` |
| archive obligation | `index` | `<<?a, seq::64>>` | `<<size::32>>` |
| dedupe, expiry | `index` | `<<?u, tenant, 0, …>>`, `<<?e, at::64, tenant, 0, …>>` | |
| audit | `audit` | `<<?a, seq::64, at::64>>` (SEC-2.a; `tenant` is a row field) | |

The tenant lives only in the hook envelope (`tenant_id`, `nil` meaning `"default"`, as in `Envelope.idempotency_key/1`). So:

1. **Counting, bounding or purging one tenant's rows is a full scan plus a hook read per row.** SEC-7's tenant-scoped admin reads (`Queue.dead(tenants: …)`, the `tenant_hits` filter in `dead_page/5`, `Replay` filters) walk the whole dead family and `multi_get` every hook to learn its tenant.
2. **Gauges fold whole index families.** `Ankusa.Metrics.Gauges.queue/1` folds `:due`, `:inflight`, `:dead` and `:archive_pending` every `admin.gauge_interval_ms` (15 s): the cost grows with the backlog (about 1 µs per key), and so does the stall a backlog causes on a dirty scheduler.
3. **Dispatch cannot be fair.** `Deliveries.due/5` is one global oldest-first scan; a tenant that floods the due family is everyone's head of line. TEN-3 needs "the next due rows of tenant *t*" and "which tenants have work now" as cheap operations.
4. **A tenant cannot be erased.** TEN-5 must find every row of one tenant without reading every hook.

Constraints the design has to live with, all verified against main (`012188f`):

- **Transitions are not single-writer.** `Queue.Writer` serializes ingest commits, but the dispatch `Pipeline` (`Store.write/3` at `dispatch/pipeline.ex:777` and in its `write/2` helper at `:1445`), `Dispatch.Replayer` (the revive batch, `replayer.ex:461`), `Queue.Archive` (settling and dropping archive obligations, `archive.ex:83`), `Queue.Reclaim` (`reclaim.ex:48`), `Deliveries.recover_inflight/2` and `Deliveries.claim/2` all write index rows from their own processes. DUR-4 ("writer offload") will add more. Any per-tenant number maintained as read-modify-write on one row loses updates silently.
- **Every transition is already one atomic batch** (`deliveries.ex` module doc: "a row always has exactly one index key"). Anything the design adds must ride in that same batch, so a crash leaves rows and their derived state together or neither.
- **A scanned family needs sentinels** (`Keys.sentinels/0`, `Store.fold/6`), because the binding reports corruption mid-iteration as a clean end of the scan.
- **Format migrations are forward-only, boot-time, bounded and resumable** (F6): `Ankusa.Store.Migration.step/2` returns one batch of ops and a cursor; `Migrate` commits both in one synced write.
- **Scale targets** (plan, Assumptions): 10⁵–10⁶ tenants, 10⁴–10⁵ req/s fleet-wide, both tenant shapes first-class (one source per tenant, and shared sources with the tenant from the URL).

## Decision

### 1. Layout at a glance

`ord` is a dense 32-bit **tenant ordinal** (section 2). Families not listed are unchanged: `hooks`, `deliveries`, the claim ref `?k`, `?c` cleared markers, `?u`/`?e` dedupe, `quarantine`, `archive`, `audit`, `default`.

| Family | Column family | Key | Value |
| --- | --- | --- | --- |
| tenant by ordinal | `tenants` (new) | `<<?t, ord::32>>` | tenant id |
| ordinal by tenant | `tenants` | `<<?n, tenant_id::binary>>` | `<<ord::32>>` |
| next ordinal | `default` | `m:next_ord` | `<<next::32>>` |
| due | `index` | `<<?D, ord::32, at::64, seq::64, sink::16>>` | `<<size::32, kind::8>>` (`kind` 0 first attempt, 1 retry) |
| inflight | `index` | `<<?F, ord::32, seq::64, sink::16>>` | `<<size::32>>` |
| dead, global time order | `index` | `<<?x, at::64, seq::64, sink::16>>` (**unchanged key**) | `term({source_id, id, ord})` |
| dead, by tenant | `index` | `<<?X, ord::32, at::64, seq::64, sink::16>>` | empty |
| archive obligation | `index` | `<<?a, seq::64>>` (unchanged key) | `<<size::32, ord::32>>` |
| active tenants | `index` | `<<?A, bucket::64, ord::32>>`, `bucket = div(at, 1000)` | empty |
| counters | `counters` (new) | `<<ord::32, field::8>>` | `term(integer)` |

Prefixes `?D`, `?F`, `?X`, `?A` are new on purpose. Reusing `?d`/`?f` with a longer key would let the migration read its own output and leave a half-migrated range with two layouts in it; with new prefixes the old family only ever shrinks and the new one only grows, and the migration ends by deleting a range. (This amends TEN-1.a's *Change*, which reused `?d`.)

`hooks` and `deliveries` keep their `seq` keys. `seq` is commit order; the compactor, `Reclaim`, replay-by-id, segments and DUR-1's intake offsets all depend on it. A tenant in those keys would buy nothing the index families do not give, and would break every one of those users.

`dead` keeps its global time-ordered key because the unfiltered `GET /v1/dlq` is "newest first across the node" and `Replayer` pages it by time; a tenant-major key would turn that into a merge over up to 10⁶ iterators. The by-tenant view is a second, value-less index (`?X`), which is what `DEL-6` asks for as `dead_by_tenant`. Dead-lettering is the rare transition, so the second write is cheap.

The audit trail is not touched: `<<?a, seq::64, at::64>>` keeps `tenant` as a row field. Audit reads are filtered by scope on a bounded, retention-pruned family and nothing in this ADR's problem list depends on them. Dedupe keys keep the tenant *string* prefix: the dedupe check runs before a tenant ordinal could be resolved, and TEN-5 erases them with a `tenant\0` prefix `delete_range`.

### 2. Tenant ordinals

- `ord` 0 is reserved for the node aggregate (section 4). Real ordinals are 1, 2, 3, …, assigned in first-sight order and **never reused**, so a straggling row can never be attributed to a different tenant. TEN-5 deletes the `?n` row and the counters but leaves the `?t` row as a tombstone (value `<<>>`).
- **Only `Queue.Writer` allocates** (and the v2 → v3 migration, which runs before anything serves). The Writer resolves the distinct tenants of a batch against a bounded ETS cache (`tenant_id → ord`, 65 536 entries, cleared whole when full, written only by the Writer and only after a commit succeeds); a miss is one `Store.multi_get` of the `?n` rows of that batch's uncached tenants. A tenant with no row gets the next ordinal; the `?t`, `?n` and `m:next_ord` puts go into the **same synced commit batch** as the due rows that use the new ordinal, so an ordinal is never visible without its rows or the other way round. The request path is untouched (rule 6): no `GenServer.call`, an ETS read inside the Writer's own batch assembly.
- **Allocation stays on the commit serializer.** Counters, markers and every transition other than first sight are safe under any number of writers (sections 4 and 5); ordinal allocation is the one serialized step. DUR-4 may parallelize encoding and batching, but must keep one process that assigns ordinals, or replace it with a reservation scheme under its own ADR.
- **Bound: `tenants.max`** (default `1_000_000`, range `1..4_294_967_294`; a `configuration.md` row, not a secret, so no `/v1/config` redaction). A request whose tenant would be the `max + 1`-th is answered like the other store-side refusals in `Edge.Router`: `503` with body `{"error":"tenant_limit"}` and `Retry-After: 300`, tagged `outcome: :tenant_limit` on the ingest metrics (a new member of the fixed outcome set listed in the `Ankusa.Metrics` moduledoc). Ordinals are only allocated at commit, after verification: quarantined and rejected requests never create one.
- `wal.type: none` has no queue, no store index and so no ordinals.
- Readers go the other way through `Ankusa.Queue.Tenants`: `ord(instance, tenant_id) :: {:ok, ord} | :not_found` (a tenant with no `?n` row has no rows, so a scoped read of it is empty without a scan) and `name(instance, ord) :: {:ok, tenant_id} | :not_found`.

### 3. Index families

- Due, inflight and the dead-by-tenant index are **tenant-major then time-major**: one tenant's rows are one contiguous range (`Keys.range(<<?D, ord::32>>)`), ordered by `at` within it.
- No code outside `Deliveries` builds a `?D`, `?F`, `?X` or `?A` key. `Deliveries.due_ops/6` returns the due put **and** the active marker put (section 5); there is no way to write a due row without its marker.
- A due row's `kind` (0 first attempt, replay revival, recovery or expansion; 1 retry) is how the `pending` and `scheduled` counters stay exact without a clock (section 4). Claiming moves the row to inflight with the `ord` and `size` it already carries.
- `ord` is known at every transition without a hook read: the claimed `due`/`inflight` key carries it, the dead key's value carries it, and the archive obligation's value carries it. Only first sight (ingest, `expand_ops` for rows imported from 0.3) and the migration read an envelope.
- `Keys.family/1` gains `:due`, `:inflight`, `:dead_by_tenant`, `:active`, `:tenants`, `:tenant_ords` and `:counters` (with `lo`/`hi` sentinels; for `:counters`, `lo = <<0::40>>` and `hi = <<0xFFFFFFFF::32, 0xFF>>`, for `?n`, `hi = <<?n, 0xFF>>`, as `s:` does). `Keys.family(:due)` etc. of format 2 survive only inside the migration as `:due_v2` and `:inflight_v2`.

### 4. Counters

Six counters per tenant, one `counters` row each, **blind merge operands, never read-modify-write**:

| `field` | Meaning | `+` | `−` |
| --- | --- | --- | --- |
| 1 `pending` | due rows with `kind` 0 | ingest, replay revival, `recover_inflight`, `expand_ops` | claim of a `kind` 0 row, purge |
| 2 `scheduled` | due rows with `kind` 1, i.e. waiting out or just past a retry backoff | `retry_ops` | claim of a `kind` 1 row, purge |
| 3 `inflight` | inflight rows | claim | delivered, retry, dead-letter, orphan drop, recovery |
| 4 `dead` | dead rows | dead-letter | revival, purge |
| 5 `bytes` | Σ `size` of every delivery row that exists (due, inflight, dead); a hook with *n* sinks counts *n* times, an upper bound of the hook bytes the tenant holds | ingest, expansion | delivered, orphan drop, purge |
| 6 `archive_pending` | `?a` rows | ingest with the archive on | the compactor settling or dropping an obligation |

- **Mechanism: RocksDB merge operands.** `counters` is opened with `merge_operator: :erlang_merge_operator`. `Ankusa.Store.op()` gains `{:merge, cf, key, delta :: integer()}`, which `add_op/3` writes with `:rocksdb.batch_merge/4` and the operand `:erlang.term_to_binary({:int_add, delta})`. A merge needs no read, takes no lock and cannot race: concurrent batches from the Writer, the Pipeline, the Replayer and DUR-4's future writers all add into the same row without losing a count, and a batch's counter deltas commit atomically with its row changes. (`:counter_merge_operator` is rejected: it parses decimal strings with `std::stoi`, 32 bits, which a byte counter passes at 2 GiB. The Erlang operator is 64-bit and implements `PartialMergeMulti`, so compaction collapses operands.)
- **One helper builds them.** `Deliveries.Counters.ops([{ord, field, delta}]) :: [Store.op()]` coalesces deltas per `(ord, field)` within a batch and adds the same deltas to the node aggregate `ord = 0`; every op builder in `Deliveries` (`claim`, `delivered_ops`, `retry_ops`, `dead_ops`, `expand_ops`, `revive_ops`, `recover_inflight`, the ingest ops in `Queue.Writer`, the archive settle/drop in `Queue.Archive`) returns its deltas through it. The structural guard is a property test, not review: after any sequence of transitions the counters equal a fold of the families (the first case under "Counters and transitions" in Proof).
- **The gauge read is O(1).** `Gauges` reads the six rows of `ord 0` in one `multi_get` and emits them as `[:ankusa, :queue, :state]`; it never folds `:due`, `:inflight`, `:dead` or `:archive_pending`. `oldest_due_age_ms` comes from the active index (section 5), not from a scan. Per-tenant reads are six point reads (`Queue.pending_for_tenant/2`, `dead_for_tenant/2`).
- **Operand chains stay short.** A point read applies every unmerged operand of the key, so the `counters` column family gets a 1 MiB write buffer (the other index families get 16 MiB) and no compression; a flush collapses the memtable's operands. If `bench/gauges_bench.exs` shows the aggregate read above 50 ms at 3 000 merging batches per second, TEN-1.b shards the aggregate as `<<0::32, field::8, shard::8>>` (16 shards, summed on read). The ADR pre-approves that change; it is still format 3.
- **Gauge semantics change, deliberately.** `pending` was "due now" and `scheduled` was "due later", both computed against the wall clock at the scan; a counter cannot follow a clock. `pending` is now "first-attempt rows waiting" and `scheduled` is "retry rows waiting, including those whose backoff has just expired and are not yet claimed". `pending + scheduled` is unchanged. An alarm on backlog age keeps its exact source: `ankusa_queue_oldest_due_age_seconds`.
- **Crash consistency needs no recount.** Batches are atomic (`sync: false` ones are lost whole with a torn WAL tail, never in part), so rows, markers and deltas appear together or not at all. Counter rows are **never deleted by a sweep**: a delete racing a merge would drop the merge. They are bounded by `6 × tenants.max` rows and removed only by TEN-5's erase.
- **Top-N gauge.** `ankusa_tenant_pending{tenant}` (top 100) is computed from the first 10 000 distinct tenants in active-index order (the tenants that have waited longest), one counter read each, every gauge tick. A tenant with `pending > 0` has an active marker at or before the current bucket, so no scan of the counters family is needed. TEN-7 owns the cardinality policy; this ADR fixes only that the source is counters plus the active index.

### 5. Active tenants and the dispatch tick

Leading the due key with `ord` turns one global time-ordered scan into one scan per tenant; a tick that visits every tenant's cursor costs 10⁵ iterator seeks at 10⁵ tenants even if three have work. The **active index** makes the tick cost O(tenants with work due).

- **Invariant: every due row has a marker `<<?A, div(at, 1000)::64, ord::32>>`.** The marker is a *blind put* in the same batch as the due row, by `Deliveries.due_ops/6` only. Rows of one tenant in one second share one marker (coalesced within a batch), so markers are bounded by rows and, in practice, by `tenants-with-work × distinct due seconds`.
- **`Deliveries.active_tenants(instance, now, limit)`** scans `?A` from the start to `div(now, 1000)` inclusive and returns the first `limit` (4 096) distinct `{ord, bucket}` in marker order, i.e. oldest first. It never touches a tenant with nothing due, so a tick's cost does not depend on the number of tenants or on the size of the backlog.
- **`Deliveries.due_for_tenant(instance, ord, now, floor, limit, byte_budget)`** is the old `due/5` restricted to `Keys.range(<<?D, ord::32>>)` up to `now`. The Pipeline's single global `floor` is replaced by the markers; per-tenant cursors (`%{ord => floor_at}`, TEN-3's scan cursors) live in the Pipeline's memory for tenants that are active, are evicted when a tenant drains, and are never persisted: after a restart a tenant's cursor starts at 0, which is correct (it only skips work already known to be done).
- **Cleanup is "delete, then verify"**, done only by the Pipeline, after it has claimed from a tenant:
  1. `first = first_due(ord)`: the earliest `at` of the tenant's due range, or `nil` (one seek).
  2. The scanned markers of `ord` with `bucket < div(first.at, 1000)` (all scanned markers with `bucket < div(now, 1000)` if `first` is `nil`) are stale. Delete them (`sync: false`).
  3. `first' = first_due(ord)`. If it exists and `div(first'.at, 1000)` is one of the deleted buckets, put that marker back.

  A concurrent writer commits a due row and its marker in one atomic batch. If the batch lands before step 2's delete, its row is visible at step 3 and the marker is restored; if after, the writer's own marker stands. Because writers only ever put markers and the Pipeline is the only deleter, no interleaving leaves a due row without a marker. A marker whose bucket is the current second is never deleted, so a retry scheduled a few hundred milliseconds ahead keeps its marker. A stale marker costs one empty tenant visit and is removed by the next tick.
- **`oldest_due_age_ms`** is read from the first marker, verified against that tenant's `first_due` (skipping at most 32 stale markers, then reporting the bucket start): exact to a second, and O(1).
- **Until TEN-3**, `Pipeline` claims from active tenants in marker order, taking each tenant's rows in `at` order until `limit` or `byte_budget` is spent. Cross-tenant order is therefore "oldest second first", not strict oldest-first; there is no fairness guarantee between tenants (TEN-3 supplies deficit round-robin on this same `active_tenants` + `due_for_tenant` pair, and the `tenants` family for per-tenant limits). A `dispatch_test` case that asserts strict cross-tenant order is amended in TEN-1.b and says so in its Evidence line; cases that use one tenant are unchanged.
- **Metrics:** `ankusa_dispatch_active_tenants` (distinct tenants the last tick visited) and `ankusa_tenants` (`m:next_ord - 1`, an O(1) read).

### 6. Reads that change

| Caller | Today | After |
| --- | --- | --- |
| `Gauges.queue/1` | folds four families | six counter reads of `ord 0`, plus the first marker |
| `Queue.pending_for_source/2` | folds `due` and `inflight`, decodes every row | unchanged semantics; if the source has a fixed tenant (per-source shape) it scans only that tenant's ranges, otherwise (shared source) it is still a fold, documented as O(backlog) and out of scope here |
| `Queue.dead/2` with `tenants:` (SEC-7) | folds `?x`, reads every hook | per tenant, a reverse range scan of `?X`, keys joined to `?x` values through `at, seq, sink`; no hook read |
| `Deliveries.dead_page/5` with `tenants:` | `multi_get` of the page's hooks | filters on the `ord` in each `?x` value, resolved once through `Tenants.ord/2` |
| `Queue.pending_for_tenant/2`, `dead_for_tenant/2` (new) | | `%{pending, scheduled, inflight, bytes}` and `%{dead}` from six counter reads |
| `Queue.purge_tenant(instance, tenant, kinds)` (new; `kinds ⊆ [:pending, :dead]`) | | chunks of 500 rows from the tenant's `?D` (resp. `?X`/`?x`) range: delete the index keys, the delivery row, put the `?c` cleared marker so `Reclaim` removes hooks nobody owes, and the counter deltas, one synced batch per chunk; idempotent, so a partial run resumes by being called again; never touches inflight rows |
| `Deliveries.recover_inflight/2` | folds all inflight | unchanged cost (boot only), rewrites `?F` to `?D` with `kind` 0 and the counter deltas |

### 7. Migration, store format 2 → 3

`Ankusa.Store.Migrations.TenantKeys` (`step/2`, 500 rows per step; `Migrate` commits each step's ops and cursor in one synced batch). Opening the format 3 code first creates the `tenants` and `counters` column families (RocksDB creates a missing family on open, as for `audit` in 1 → 2) and writes the new families' sentinels. The cursor is `<<phase::8, last_old_key::binary>>`; each step seeks to `last_old_key` (never to the family's start: the keys it moved are tombstones, and reseeking from the start would make the migration quadratic).

| Phase | Reads | Writes, per row |
| --- | --- | --- |
| 1 due (`?d`) | old key and value, the hook (`multi_get`, uniq by `seq`) for the tenant | delete the `?d` key; put `?D` (`kind` 1 if `at` is later than the time the migration started, else 0) and its marker; counter merge `pending` or `scheduled` +1 and `bytes` +`size` |
| 2 inflight (`?f`) | old key and value, the hook | delete `?f`; put `?F`; `inflight` +1 |
| 3 dead (`?x`) | key, value, the delivery row (for `size`), the hook | put `?x` with `ord` added to the value; put `?X`; `dead` +1, `bytes` +`size` |
| 4 archive (`?a`) | key, value, the hook | put `?a` with `ord` added to the value; `archive_pending` +1 |
| 5 finish | | `delete_range` of the old `?d` and `?f` ranges (stray keys and the format 2 sentinels); the format stamp comes from `Migrate`, in the same batch |

- **Tenant of a row:** `env.tenant_id || "default"`; a hook that does not decode, or is gone, files its rows under `"default"` (the Pipeline drops or dead-letters such rows when they come due, through the normal counted path).
- **Ordinals** are allocated in the batch that first meets the tenant (read `m:next_ord`, put `?t`, `?n` and the new `m:next_ord` with the rows), single-threaded because nothing serves yet. If the count passes `tenants.max` the migration continues anyway (the cap refuses *new* tenants at ingest; it never strands existing rows) and the boot logs the overage.
- **Seeding is the same code path as runtime**: the counters are the merge deltas of the batch that moves the rows. Resumption is exactly-once because a step's ops and its cursor commit together: a step either landed (cursor advanced, never replayed) or did not (nothing of it applied). That is the property `resumes interrupted tenant migration without double counting` asserts.
- **Space:** rows move chunk by chunk (delete old, put new); the store never holds two copies of more than one chunk.
- A store with nothing queued (no `?d`, `?f`, `?x`, `?a` rows) migrates in one step.
- `mix ankusa.store.migrate --dry-run` lists the step as `2 -> 3: re-key due/inflight/dead by tenant ordinal, seed per-tenant counters`.
- **Fixture:** `mix ankusa.store.fixture test/fixtures/store-v3.tar.zst` writes a format 3 store; `store-v2.tar.zst` (kept from SEC-2.a) is the input of the upgrade test. The v3 fixture must hold: at least three tenants including `default` (hooks with a `nil` tenant), pending, retrying and inflight rows (a crash leaves inflight), dead rows, archive obligations, and a tenant with only dead rows.

### 8. Rollback boundary

The migration is forward-only and the format guard is the fence: a release that writes format 2 refuses a format 3 store (`{:store_format_newer, 3, 2}`), and RocksDB itself refuses a database holding column families (`tenants`, `counters`) the older release does not list. **Once a node has booted the format 3 release, going back means restoring the pre-upgrade backup**, losing what was acked since. A migration killed mid-way leaves `m:migrating:3` and the store at format 2 with some rows moved; only the format 3 release can open it, and it resumes. The format 3 release must say so in the `docs/deployment.md` "Upgrading between releases" section (a "Format 3" paragraph next to the format 2 one) and in its release notes, and recommends a backup before the first boot. A fleet upgrades node by node; stores are per node, so a mixed fleet is fine (the claim gateway, routes and sources are not store-format dependent).

### 9. Invariants later work must keep

- Counters, markers and the dead index are changed **only** through the `Deliveries` op builders and `Counters.ops/1`; a batch that puts or deletes a `?D`, `?F`, `?x`, `?X` or `?a` key without the matching deltas is a bug. (DUR-4, DEL-1's lane state, TEN-5.)
- **Ordinal allocation has one owner** (section 2).
- **Ordinals are never reused** (TEN-5 tombstones, never recycles).
- **No sweep deletes a counter row; only the Pipeline deletes markers**, by delete-then-verify.
- A new key family that is queried per tenant is keyed `ord`-first; one that is scanned by time across tenants keeps time first and gets a by-tenant companion, as `dead` does.
- A change that adds a column family or a key family registers its migration and fixture in the same PR (ground rule 9, F6).

### 10. Alternatives considered

| Alternative | Why not |
| --- | --- |
| tenant string in the key (`<<?d, tenant, 0, at, …>>`) | variable-length keys at 10⁶ tenants cost bytes in every index key and every scan comparison, tenant ids are not dense (no fixed-width range arithmetic, no cheap cursor), and an ordinal gives the reverse map and the allocation count for free |
| 32-bit hash of the tenant id | collisions silently merge two tenants' fairness, counters and purges; no way to list tenants |
| one RocksDB column family per tenant | 10⁶ column families |
| keep global time-ordered `due`, add a per-tenant secondary index | the due scan stays global (no fairness without reading every row), every transition writes two index rows instead of one, and the secondary index still needs counters |
| counters as read-modify-write rows, serialized in one process | requires every transition writer to go through it (the Pipeline, the Replayer, the compactor and DUR-4's writers would all need routing), puts a `GenServer.call` on the dispatch path, and still loses counts if one writer is ever missed; merge operands remove the whole class |
| active set derived by scanning the counters family for `pending > 0` | O(all tenants) per tick, the cost the active index exists to avoid |
| one active marker per tenant holding its earliest `at` | needs a read-modify-write to keep the earliest `at` correct under concurrent writers |

## Consequences

- **Per-tenant counts are O(1)**, per-tenant dead and pending scans are one range, the gauge tick no longer depends on backlog or tenant count, and SEC-7's hook-reading tenant filter is replaced by the index. TEN-3 gets `active_tenants` and `due_for_tenant`; TEN-5 gets a tenant-complete index to erase through; DEL-6 gets `?X`; TEN-7's top-N gauges have their source.
- **Write amplification.** Per commit batch: one blind marker put per distinct `(tenant, second)` and up to three merge operands per distinct tenant for ingest (`pending`, `bytes`, `archive_pending`), plus the same three on the aggregate. A batch of 256 hooks from 256 tenants writes about 770 merge operands next to its 1 000-odd puts; a batch from one tenant writes three plus three. Due and inflight keys grow by 4 bytes. `bench:gate` must pass on TEN-1.b with before/after numbers in the PR (rule 10); the permitted way to recover a regression is the sharded-aggregate or buffer-size knobs of section 4, not a rebaseline.
- **Read amplification.** A counter read applies the operands pending in its memtable; bounded by the 1 MiB `counters` buffer (section 4) and measured by `gauges_bench`.
- **First sight of a tenant** costs one point read in the Writer (cache miss) and two small puts in the commit; the steady state is an ETS hit.
- **Operator-visible semantics.** `pending`/`scheduled` redefined (section 4); strict cross-tenant oldest-first claim order is replaced by oldest-second-first until TEN-3; `docs/delivery.md` and `docs/architecture.md` change where they define those gauges. A new refusal `tenant_limit` and a new key `tenants.max`.
- **Format 3 is a one-way door** (section 8); the migration's time is proportional to the rows queued at upgrade, bounded per step and resumable, and TEN-1.b reports its measured rows per second in the PR.
- **Bounded structures** (ground rule 3):

  | Structure | Count ceiling | Byte ceiling | Sweep | Gauge | Refusal |
  | --- | --- | --- | --- | --- | --- |
  | `tenants` family | `tenants.max` (default 10⁶) | ≈ 80 B × cap | none (ordinals are permanent; TEN-5 tombstones) | `ankusa_tenants` | `503 tenant_limit`, `Retry-After: 300` |
  | `counters` family | `6 × tenants.max` | ≈ 40 B × 6 × cap | none by design (a delete would race a merge); TEN-5 erase | `ankusa_tenants` | as above |
  | active markers (`?A`) | one per `(tenant, due second)` ≤ due rows | ≤ 13 B × due rows | the Pipeline, every tick | `ankusa_dispatch_active_tenants` | bounded by the due rows, which the batcher and (DUR-2) the retention ceilings already bound |
  | dead-by-tenant (`?X`) | one per `?x` row | 23 B × dead rows | deleted wherever `?x` is | `dead` counter | DUR-2's DLQ ceiling counts both |
  | Writer tenant cache | 65 536 entries | ≈ 10 MB | cleared whole when full | none | n/a (a miss is a point read) |

- **Docs TEN-1.b updates in the same PR:** `docs/storage.md` (column-family and key tables, the counters and the active index), `docs/deployment.md` ("Format 3" paragraph and rollback boundary), `docs/configuration.md` (`tenants.max`), `docs/delivery.md` and `docs/architecture.md` (gauge definitions, the claim-order statement), `docs/testing.md` (the new benches and tests below).
- **Out of scope here, decided elsewhere:** per-tenant admission (TEN-2), deficit round-robin and per-tenant dispatch limits (TEN-3), the cardinality policy and `metrics.detail` (TEN-7), the erase job and its tombstones (TEN-5), per-source counters (a shared source's queue gauge stays a fold).

## Proof

Everything below is named so TEN-1.b's PR can be checked against this list; a missing or renamed case needs an amendment to this ADR in the same PR.

**Upgrade and layout** (`test/ankusa/store_upgrade_test.exs`):

- `v2 fixture upgrades to v3 with counters equal to a fold` (every family, every tenant, `default` included)
- `resumes interrupted tenant migration without double counting` (kill between every pair of steps; counters and rows equal an uninterrupted run)
- `tenant migration allocates each ordinal once`
- `a hook with no tenant or no envelope files under default`
- `a format 2 release refuses a format 3 store`
- `a restored format 2 backup migrates on boot`
- `the v3 fixture task writes a store that boots` (`mix ankusa.store.fixture test/fixtures/store-v3.tar.zst`)

**Counters and transitions** (`test/ankusa/queue_test.exs`, `test/ankusa/queue/deliveries_test.exs`):

- `counters equal a fold after any sequence of transitions` (property test over ingest, claim, retry, deliver, dead-letter, revive, recover, expand, purge, archive settle and drop)
- `concurrent writers do not lose counts` (N processes issuing transitions on one tenant through different op builders; final counters exact)
- `tenant ordinal is assigned once and never reused`
- `tenants.max refuses a new tenant with tenant_limit` and `an existing tenant is never refused at the cap`
- `no due row without a marker` (property test) and `cleanup does not lose a marker to a concurrent enqueue` (the delete-then-verify argument, driven by interleaving the cleanup between a writer's commit and its own step 3)
- `active_tenants returns only tenants with due rows`
- `pending_for_tenant, dead_for_tenant and purge_tenant touch one tenant`
- a source-grep test that `Keys.due(`, `Keys.inflight(` and the `?D`/`?F`/`?X`/`?A` constructors are called only from `deliveries.ex`

**Dispatch** (`test/ankusa/dispatch_test.exs`): existing cases pass unchanged in semantics (the strict cross-tenant order case amended as in section 5), plus `a tick visits only tenants with work` (1 000 tenants, 3 active: the tick's `tenants_visited` is 3).

**Gauges and metrics** (`test/ankusa/metrics_test.exs` or `test/ankusa/metrics/gauges_test.exs`): `queue gauges read counters` (a trace on `Ankusa.Store.fold/6` shows no fold of `:due`, `:inflight`, `:dead` or `:archive_pending` during `Gauges.measure/1`), `oldest due age comes from the active index`, `top tenants by pending`.

**Isolation** (`test/ankusa/admin/router_test.exs`): the SEC-7 cases `tenant scoped dlq`, `tenant scoped quarantine purge` and `tenant scoped replay` keep passing; `tenant scoped dlq reads one range` asserts no hook read.

**Crash** (`test/ankusa/loss_test.exs`): its cases pass unchanged in semantics, with `counters equal the index after kill -9` added (every acked hook has a due row under its tenant's ordinal; counters equal a fold).

**Benches** (self-asserting scripts run from `mise run bench:gate`; no `baseline.json` entry, so no rebaseline PR):

- `mix run bench/gauges_bench.exs` (new): the gauge tick against 10³, 10⁴, 10⁵ and 10⁶ queued rows, **and** against 10, 10³ and 10⁵ tenants with the number of active tenants fixed at 8 (rows of the others scheduled far ahead or dead). Passes only if the slowest cell is within 1.5× of the fastest and under 50 ms, with 3 000 merging batches per second running during the measurement. *Amended in TEN-1.b:* a cell is the 10th percentile of 1 000 ticks (lowest of three rounds), and the fastest cell counts as at least 0.2 ms, because the tick costs about 0.1 ms and two runs of one cell differ by a factor of two at that scale (measured: 0.094 to 0.215 ms across seven cells with no trend in either axis); a tick that grew with the queue would cost milliseconds and fail both limits.
- `mix run bench/dispatch_tick_bench.exs` (new): the dispatch tick (`active_tenants` plus `due_for_tenant` for each) over the same two axes, same pass rule.
- `mise run bench:gate` → PASS with before/after numbers in the PR description (hot-path files `queue/`, `store/`, `store.ex`, `dispatch/` change).

**Commands for TEN-1.b:** `mix test test/ankusa/store_upgrade_test.exs test/ankusa/queue_test.exs test/ankusa/dispatch_test.exs test/ankusa/loss_test.exs test/ankusa/admin/router_test.exs test/ankusa/metrics_test.exs`, `mix test` (core), both benches, `mise run bench:gate`, `mise run test:crash`, `mise run check`.

**This ADR's own proof:** the PR merges, and `mise run check:links` passes.
