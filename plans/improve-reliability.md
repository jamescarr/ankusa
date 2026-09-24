# Perf + reliability pass: fix the chaos loss, remove throughput ceilings, make perf numbers real

## Context
Chaos testing (`examples/oban-consumer/run.sh`, docs/testing.md:208-237) shows permanent loss of ~0.5–1.5% of acked hooks when `ankusa-worker-0` is killed; the root cause was never found. The request: find and fix the root cause of that and any other reliability issues, run real perf tests, and build for max performance. End state: chaos phase `missing: 0`; the other loss/stall bugs found below are fixed; dispatch is concurrent instead of one envelope at a time; the load generator reports true numbers; before/after numbers are recorded in docs/testing.md.

User decisions (fixed, do not revisit):
- Postgres ordering fix = **per-instance advisory lock around seq allocation → commit** (integer `seq` and the WAL behaviour stay as they are).
- Concurrent dispatch ordering = **sink-defined ordering key** (new optional `Ankusa.Sink.ordering_key/2`).

## Root causes found (all verified by reading the code)
1. **Chaos loss — WAL.Postgres seq is allocation-ordered, not commit-ordered.** `seq BIGSERIAL` (`ankusa_postgres/lib/ankusa/wal/postgres/migration.ex:29`) is allocated in `insert_winners/2` mid-transaction; COMMIT comes later. Readers (`read/3`, `postgres.ex:104-115`) use `seq > cursor`. Edge A takes seq 100 while edge B takes 101 and commits first → dispatch reads 101, cursor=101 → A's 100 becomes visible later and is never read. The compactor skips it too, then `truncate_through(min(compactor, dispatch))` deletes it. That matches every symptom in docs/testing.md (row absent from `ankusa_wal`, cursor past it, no DLQ). The worker kill matters because the catch-up burst loads the shared Postgres, which widens the allocation→commit window while dispatch polls at the tail [INFERENCE for the load correlation; the mechanism itself is exact].
2. **DiskLog restarts seq at 1 after a full truncation.** `replay/3` (`lib/ankusa/wal/disk_log.ex:271-278`) returns `next_seq = 1` for an empty file. `do_truncate/2` empties the file whenever dispatch and compactor are caught up, which is the normal idle state. After a restart, new hooks get seq 1.. while the persisted cursors are e.g. 5000, so dispatch and compactor skip them and the next truncate deletes them. Every restart of a caught-up single-node deployment silently loses hooks.
3. **The batcher never sheds load.** `Edge.Batcher` resets `count` on every flush and flushes at `max_batch` (256), below `max_queue` (10_000), so the shed branch can't run. `flush/1` runs `WAL.append` synchronously, so overload piles up in the unbounded mailbox until callers hit the 15 s timeout. A WAL error is a `MatchError` crash (`{:ok, results} =`, batcher.ex:104); repeated crashes can exceed the supervisor's restart intensity and take down the instance.
4. **Dispatch ceiling (~150/s measured):** `Pipeline.drain/1` delivers one envelope at a time, persists the cursor after every envelope, sleeps inside the only dispatch process during retries (head-of-line blocks every source), and crash-loops the pipeline on a raising sink.
5. **Sink/DB-side perf bug:** `backfill_dedup_seq/2` (`postgres.ex:265-280`) runs `UPDATE ankusa_wal_dedup … WHERE d.event_id = …`, but `event_id` is not indexed (PK is `(instance, tenant_id, source_id, dedup_key)`). That is a sequential scan of the ever-growing dedup ledger on every append. Once the fix above holds a lock over it, it would serialize the whole fleet behind that scan.
6. **DiskLog does O(WAL) work per compaction tick while blocking appends:** `do_truncate/2` rewrites the whole file frame by frame and dumps the whole dedup set on every compactor tick (1 s).
7. **Durability gaps that are real loss/boot paths:** `persist_term/2` (cursors, dedup snapshot) renames an un-fsynced tmp file, so after power loss it can be empty or corrupt and `binary_to_term` crashes boot. DLQ writes (`DurableLog.append`) are not fsynced, yet the dispatch cursor moves past a dead letter, so on power loss the hook is in neither the DLQ nor the (truncated) WAL. The index append before the compactor's `put_cursor`/truncate is also not fsynced.
8. **The compactor reads up to 1,000,000 envelopes into memory in one tick** (`@read_limit`, compactor.ex:21), which risks OOM after any storage-node outage. `storage.roll_bytes` is documented but not implemented.
9. **The perf numbers are wrong:** `loadgen.run`'s `maybe_pace/5` (`tools/loadgen/lib/mix/tasks/loadgen.run.ex:142-147`) staggers worker `i` by `i*C/rate` seconds, not `i/rate`. With C=64, rate=60, worker 63 starts at 67 s, so offered load ramps from 0 to 60/s (≈1.6k sent instead of 3.6k, matching the documented 23.6 accepted/s). Latency is also measured from the actual send, not the intended send (coordinated omission).
10. **Docs describe a topology that loses data:** docs/deployment.md:35-40 and docs/architecture.md:165-183 say DiskLog roles can be split across OS processes on one host. DiskLog keeps its index in-process and truncates by renaming the file, so a separate dispatch process never sees new writes and a separate storage process rewrites the file under the edge.

Scope boundary: unbounded growth of the dedup ledger and storage index, DLQ-replay semantics, and RabbitMQ's one-confirm-at-a-time publisher are real but not part of this pass.

## Approach
Order keeps the tree building and existing tests green after each step. Step 1 comes first so the baseline is captured on unchanged core. Steps 2–5 and 8 are independent of each other. Step 7 depends on step 6.

### 1. Perf harness: truthful loadgen + in-process core bench (then capture baselines)
- `tools/loadgen/lib/mix/tasks/loadgen.run.ex`:
  - `maybe_pace/5`: `target_ms = t0_ms + round((k * concurrency + i) * 1000 / rate)`, so request `n = k*C + i` fires at `n/rate`. It returns the intended start in µs (`t0_us + (k*C+i)*1_000_000/rate`), or `nil` when unpaced.
  - `perform_request/2`: when paced, latency = `t_end - intended_start_us`, not `t_end - t_start`.
  - Bound the dup pool: in the 201 clause, `bodies: Enum.take([req_body | acc.bodies], 1024)`.
  - Report: add `sent_per_s` (sent / duration_s) to the JSON and the printed table. When `rate` is set and `sent_per_s < 0.95 * rate`, print `"loadgen: generator fell behind (sent_per_s=… < rate=…); raise --concurrency"` to stderr. The exit code is unchanged.
  - Update `tools/loadgen/README.md` flag docs to say pacing is open-loop and latency is measured from the scheduled send time.
- New `bench/core_bench.exs` (repo root; add `"bench/**/*.exs"` to `.formatter.exs` inputs; not in the hex `files` list). Run with `MIX_ENV=test mix run bench/core_bench.exs`. `test` env because `config/config.exs:8` autostarts the default instance on :4000 outside test.
  - Env knobs: `N` (20000), `CONCURRENCY` (256), `SINK_LATENCY_MS` (5), `BODY_BYTES` (512).
  - `Logger.configure(level: :warning)`. Public ETS `:bench_delivered` (set).
  - Defines `Bench.Sink` (`@behaviour Ankusa.Sink`). `deliver/3` sleeps `latency_ms`, inserts `{env.id}` and returns `:ok`. It also defines a plain `def ordering_key(_env, _opts), do: nil` with no `@impl`, so the same script compiles against baseline core.
  - Starts `{Ankusa.Instance, Ankusa.Config.new(instance: :bench, data_dir: <tmp>, port: 0, roles: [:edge, :dispatch, :storage], source_store: {Ankusa.SourceStore.Static, sources: %{"bench" => [verifier: {Ankusa.Verifier.None, []}, dedup: {Ankusa.DedupKey.Rules, json: ["id"]}, on_verify_failure: :accept_flag, sinks: [{Bench.Sink, latency_ms: L}]]}})}`. Only default dispatch/batcher keys, because `Config.new` raises on unknown keys.
  - Ingest phase: `Task.async_stream(1..N, max_concurrency: CONCURRENCY, ordered: false, timeout: :infinity)`. Each task calls `Ankusa.Edge.Ingest.ingest(:bench, %{source_id: "bench", method: "POST", path: "/webhooks/bench", headers: [], body: JSON of %{"id" => unique, "pad" => BODY_BYTES x's}})` and measures µs latency. It collects `{:ok, env}` ids.
  - Drain phase: poll `:ets.info(:bench_delivered, :size)` every 10 ms until it is ≥ the acked count, or 600 s.
  - Output one JSON line and a table: `acked, ingest_per_s, ingest_p50/p95/p99/max_ms, drain_s (after ingest end), end_to_end_per_s (acked / (last delivery − ingest start))`, and `missing` = acked ids not in ETS. `System.halt(1)` if `missing > 0` or on timeout.
- Capture baselines before touching core (see Verification B1/B2).

### 2. WAL.Postgres: commit order == seq order (the chaos fix)
- In `ankusa_postgres/test/ankusa/wal_postgres_test.exs`, write the regression test **first** and confirm it fails on current code: `"a cursor-following reader never skips a commit that lands behind it"`.
  - Inside `try … after`, create a function and statement trigger that widen the allocation→commit window:
    - `CREATE OR REPLACE FUNCTION ankusa_test_slow_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(random() * 0.02); RETURN NULL; END $$`
    - `CREATE TRIGGER ankusa_test_slow_commit AFTER INSERT ON ankusa_wal FOR EACH STATEMENT EXECUTE FUNCTION ankusa_test_slow_commit()`
    - Run both via `Postgrex.query!(Ankusa.via(inst, :wal), …)`. In `after`, `DROP TRIGGER IF EXISTS … ON ankusa_wal` and `DROP FUNCTION IF EXISTS ankusa_test_slow_commit()`.
  - Reader `Task`: loop `WAL.read(inst, cursor, 1000)` and add every seq to a MapSet. Advance the cursor to the last seq read; on a `:stop` message, drain until a read returns `[]`, then return the set.
  - Writers: 8 `Task`s, each doing 25 sequential `WAL.append(inst, [entry()])` calls and returning their committed seqs.
  - After `Task.await_many(writers, 30_000)`, send `:stop` and assert `MapSet.difference(committed, seen) == MapSet.new()`.
- `postgres.ex` `append/2`:
  - Keep `claim_dedup/2` first (outside the lock).
  - Replace `insert_winners/2` + `backfill_dedup_seq/2` with `insert_winners(conn, instance, winners)`. When `winners != []` it first runs `SELECT pg_advisory_xact_lock(hashtext($1))` with `["ankusa_wal:" <> instance]`, then one statement:
    ```sql
    WITH ins AS (
      INSERT INTO ankusa_wal (event_id, instance, tenant_id, source_id, dedup_key, envelope)
      SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::bytea[])
      RETURNING event_id, instance, tenant_id, source_id, dedup_key, seq
    ), backfill AS (
      UPDATE ankusa_wal_dedup d SET seq = ins.seq FROM ins
      WHERE ins.dedup_key IS NOT NULL
        AND d.instance = ins.instance AND d.tenant_id = ins.tenant_id
        AND d.source_id = ins.source_id AND d.dedup_key = ins.dedup_key
        AND d.event_id = ins.event_id
    )
    SELECT event_id, seq FROM ins
    ```
    The PK-keyed update also fixes root cause 5. Delete `backfill_dedup_seq/2` entirely.
  - `resolve_losers/2` stays after the insert, because intra-batch losers need the backfilled seq.
  - Deadlock-free: the lock holder never waits on a non-holder's rows. Its only writes are fresh `ankusa_wal` rows and dedup rows it claimed itself.
  - The lock is released after the commit is visible, so the next holder's seqs are always greater than every visible seq.
- Docs in code: add a moduledoc section `## Seq order` that states the lock, why it exists (the race above), and its cost (fleet-wide serialized commits). Fix the migration moduledoc lines 6-9, since strictly increasing allocation alone is not enough. No DDL change.
- `lib/ankusa/wal.ex` contract (lines 17-18): replace "dense and monotonic" with: seqs are strictly increasing **in commit order** and may have gaps; once a reader has observed seq N, no record with seq ≤ N may become visible later.

### 3. WAL.DiskLog: seq floor, lazy physical truncation, fsynced metadata
All in `lib/ankusa/wal/disk_log.ex`.
- `persist_term/2`: open tmp with `[:write, :raw, :binary]`, `:file.write`, `:file.datasync`, close, then `:file.rename`. Used for `.cursors`, `.dedup` and the new `.truncated`.
- New state fields:
  - `truncated_through` (from `<path>.truncated`, default 0, via `:erlang.binary_to_term(bin, [:safe])`; missing file → 0).
  - `rewrite_min_bytes` = `Keyword.get(elem(config.wal, 1), :rewrite_min_bytes, 64 * 1024 * 1024)`.
- `replay`/`parse`: take `truncated_through` and insert into `index` only frames with `seq > truncated_through`. Dedup keys load from every valid frame, as today.
- `init`: `next_seq = Enum.max([replay_next, truncated_through + 1 | Enum.map(Map.values(cursors), &(&1 + 1))])`. Load cursors before computing it. The cursor term covers deployments upgraded from a pre-fix empty WAL.
- `handle_call({:truncate_through, seq})`:
  - No-op if `seq <= truncated_through`.
  - Otherwise `persist_term(path <> ".truncated", seq)`, then `:ets.select_delete(index, [{{:"$1", :_}, [{:"=<", :"$1", seq}], [true]}])`, then set `truncated_through`, then `maybe_rewrite/1`.
  - `stats.records` now counts only live records, so the storage tests' `records == 0` / `== 1` still hold.
- `maybe_rewrite/1`:
  - `dead` = header start of the first live frame (`off - @header_bytes` of `:ets.first`), or `write_pos` if the index is empty; `live = write_pos - dead`.
  - Rewrite only when `dead > 0 and dead >= rewrite_min_bytes and dead >= live`. Live frames are always a contiguous file suffix, so the rewrite is:
    1. `persist_dedup_snapshot` (as today).
    2. Copy `[dead, write_pos)` into `path <> ".compact"` in 8 MiB `:file.pread`/`:file.pwrite` chunks, then `datasync` and close.
    3. Close the old fd, `:file.rename`, reopen.
    4. Rewrite every index entry as `{seq, {off - dead, len}}` (`:ets.insert` overwrites by key).
    5. `write_pos = live`.
  - Delete the old per-frame `do_truncate/2`.
- Regression test in `test/ankusa/wal_disk_log_test.exs`: `"seq keeps increasing after a full truncation and restart"`.
  - Own config: `test_config(wal: {Ankusa.WAL.DiskLog, rewrite_min_bytes: 0})`, then `put_config` and start the WAL.
  - Append 3 records, `truncate_through(3)` (which physically empties the file), stop and restart the WAL, append 1.
  - Assert its seq is 4 and `WAL.read(inst, 3, 10)` returns it.
  - Fails on current code (seq 1).

### 4. DurableLog sync barrier for DLQ and index
- `lib/ankusa/durable_log.ex`: change to `def append(path, records, opts \\ [])` with a function head. List and single-record clauses keep today's semantics. With `sync: true` it does `:file.open(path, [:append, :raw, :binary])`, `:file.write(fd, frame(records))`, `:file.datasync(fd)`, and closes in `after`. The default stays unsynced. Update the moduledoc lines 46-48.
- `Dispatch.DLQ.write/3` → `DurableLog.append(path, record, sync: true)`.
- `Storage.Index.append/2` → `DurableLog.append(path(config), rows, sync: true)`.
- Quarantine is unchanged, because it already datasyncs its own fd.

### 5. Batcher: async commit, real backpressure, no idle linger
- `lib/ankusa/edge/batcher.ex`, new state: `buffer` (newest-first), `count`, `inflight` (`nil | %{ref: reference, entries: [{from, record}]}`), `timer`.
- `handle_call({:enqueue, record}, from, state)`:
  - Shed (`{:error, :overload}` + existing `[:load_shed]` telemetry, `queue:` = total) when `count + inflight_size >= max_queue`.
  - Otherwise buffer it, then `maybe_flush`:
    - commit in flight → wait (the completion flushes);
    - else `count >= max_batch or max_delay_ms == 0` → `start_commit`;
    - else arm the timer if unarmed.
- `handle_info(:flush)`: clear the timer; `start_commit` if idle and `count > 0`.
- `start_commit/1`:
  - cancel the timer;
  - take the oldest ≤ `max_batch` entries;
  - `Task.async(fn -> safe_append(instance, records) end)`, where `safe_append` wraps `WAL.append/2` in `try` → `{:error, e}` on `rescue` and on `catch :exit`;
  - store `inflight`; keep the remainder buffered.
- `handle_info({ref, result}, %{inflight: %{ref: ref}})`:
  - `Process.demonitor(ref, [:flush])`.
  - `{:ok, results}` → `GenServer.reply` each caller in order.
  - `{:error, reason}` → `Logger.warning("[ankusa] WAL append failed: #{inspect(reason)}")` and reply `{:error, :store_unavailable}` to each caller.
  - Clear `inflight`; if `count > 0`, `start_commit` immediately (no linger).
- Update the `commit/4` `@spec` to add `{:error, :store_unavailable}`. `Edge.Ingest.commit/3` (ingest.ex:91-95) gets the clause `{:error, :store_unavailable} -> {:error, :store_unavailable}`. The router already maps it to 503 (router.ex:83).
- `lib/ankusa/config.ex` defaults:
  - `batcher.partitions: 2`: both WALs serialize commits (DiskLog GenServer, Postgres lock), so more partitions only add contention now that batching is natural.
  - `batcher.max_delay_ms: 0`.
- Test in `test/ankusa/edge_test.exs`: `"sheds with 503 once max_queue is reached while a commit is in flight"`.
  - A test-local `SlowWAL` module whose `child_spec/1`, `read/3`, `get_cursor/2`, `put_cursor/3`, `truncate_through/2`, `stats/1` delegate to `Ankusa.WAL.DiskLog`, and whose `append/2` sleeps 300 ms before delegating.
  - Config `roles: [:edge], wal: {SlowWAL, []}, batcher: %{partitions: 1, max_batch: 2, max_queue: 4, max_delay_ms: 0}`, source `"demo" => []`.
  - Fire 20 concurrent `Ingest.ingest/2`.
  - Assert `overloads >= 10`, `committed + overloads == 20`, and every committed id is in `WAL.read(inst, -1, 100)`.
  - On current code nothing sheds.

### 6. `Ankusa.Sink.ordering_key/2`
- `lib/ankusa/sink.ex`: add
  - `@callback ordering_key(Envelope.t(), opts :: keyword()) :: term() | nil` and `@optional_callbacks ordering_key: 2`. Doc: deliveries with equal `{sink module, key}` run one at a time in seq order; `nil` = no ordering constraint.
  - `@spec ordering_key(module(), Envelope.t(), keyword()) :: term() | nil`, with `def ordering_key(mod, env, opts)`: `Code.ensure_loaded(mod)`, then `mod.ordering_key(env, opts)` if exported, else `{env.tenant_id, env.source_id}`.
- Implementations (`@impl true`):
  - `Sink.Http`: `{env.tenant_id, env.source_id}` when `Keyword.get(opts, :ordered, false)`, else `nil`. Document `:ordered` in the moduledoc.
  - `Sink.Log`: `nil`.
  - `Sink.Kafka` (ankusa_kafka): `key(env, opts)`, which is exactly its record key.
  - `Sink.RabbitMQ` (ankusa_rabbitmq): `routing_key(env, opts)`.
- `ankusa_server/lib/ankusa_server/config.ex`: `@http_sink_keys` += `ordered`. The http branch (≈line 737) gets `|> put_opt(:ordered, bool_opt(sink, "ordered", path))`.

### 7. Concurrent dispatch pipeline (rewrite `lib/ankusa/dispatch/pipeline.ex`)
- New `config.dispatch` defaults (config.ex):
  - `concurrency: 32` (max concurrent sink deliveries);
  - `max_inflight: 4096` (admitted, incomplete envelopes);
  - `max_inflight_bytes: 134_217_728` (sum of admitted `byte_size(env.body)`);
  - `batch` stays 128 (it bounds one read's worst-case memory);
  - `poll_ms` stays 200.
  - `ankusa_server` `@dispatch_keys` += `concurrency max_inflight max_inflight_bytes`, with `put_opt(... int_opt(...))` lines in `dispatch_section/1`.
- `init/1`:
  - `Process.flag(:trap_exit, true)`;
  - `{:ok, task_sup} = Task.Supervisor.start_link()`, which is linked, so tests that start the Pipeline alone keep working and `Instance` needs no change;
  - `cursor = WAL.get_cursor(instance, :dispatch)`, `read_seq = cursor`;
  - `pending` (`:gb_sets` of admitted, incomplete seqs), `remaining` (`%{seq => {jobs_left, body_bytes}}`), `inflight_bytes`;
  - `lanes` (`%{lane => :queue}`; key present = lane busy), `runnable` (`:queue` of jobs), `running` (`%{ref => job}`);
  - `completed` (counter), `waiters` (`[{from, completed_at_call}]`), `window_full?`;
  - schedule `:poll` after `poll_ms`.
- Job: `%{seq, env, sink: {mod, opts}, lane}`, where `lane = case Sink.ordering_key(mod, env, opts) do nil -> nil; k -> {mod, k} end`.
- `fill/1`:
  1. Stop and set `window_full?: true` when `map_size(remaining) >= max_inflight or inflight_bytes >= max_inflight_bytes`.
  2. Otherwise `WAL.read(instance, read_seq, min(batch, max_inflight - map_size(remaining)))` and admit each envelope in order.
  3. If the read returned the requested count, loop; otherwise set `window_full?: false`.
- Admit:
  - Look up sinks with `SourceStore.fetch/2`. Memoize per `fill` call in a local map keyed by `source_id`.
  - `sinks == []` → complete immediately (`completed + 1`), set `read_seq = env.seq`, add nothing to `pending`.
  - Else add to `pending`/`remaining`/`inflight_bytes`, set `read_seq = env.seq`, and enqueue one job per sink:
    - `lane == nil` → `runnable`;
    - lane busy → append to its queue;
    - else mark it busy (`:queue.new()`) and push to `runnable`.
- `start_jobs/1`: while `map_size(running) < concurrency` and `runnable` is non-empty, `Task.Supervisor.async_nolink(task_sup, fn -> deliver(job, instance, config, max_sleep) end)` and store the job by `task.ref`.
- `deliver/4` (runs in the task) is today's `deliver_with_retry/4` loop, with these changes:
  - `mod.deliver/3` is wrapped: `rescue e -> {:error, {:raised, e}}`, `catch :exit, r -> {:error, {:exit, r}}`, `catch :throw, v -> {:error, {:throw, v}}`.
  - Retries `sleep/2` in the task, so only that lane waits.
  - It emits `[:dispatch, :stop]` exactly as today and returns `:ok` or `{:dead, {:sink, mod, reason}}`. It does not write the DLQ.
- `handle_info({ref, result}, state)` for a running ref:
  - `Process.demonitor(ref, [:flush])`.
  - On `{:dead, reason}`: `DLQ.write(config, env, reason)` and emit `[:dispatch, :dlq]` with the metadata used today, so DLQ appends stay serialized in one process.
  - Release the lane: pop the lane queue's next job into `runnable`, or delete the lane if its queue is empty.
  - Decrement `remaining[seq]`. At 0: delete it from `remaining` and `pending`, subtract its bytes, `completed + 1`.
  - Then `start_jobs`; if `window_full?`, `fill` + `start_jobs`; then `maybe_reply_waiters`.
- Crash handling:
  - `{:DOWN, ref, …, reason}` for a running ref → `{:stop, {:delivery_task_crashed, reason}, state}`. The supervisor restarts from the durable cursor, which keeps at-least-once.
  - `{:EXIT, _pid, reason}` → `{:stop, reason, state}`.
- `watermark(state)`: `read_seq` if `pending` is empty, else `:gb_sets.smallest(pending) - 1`. Gaps are harmless because the step-2/3 contract guarantees no lower seq appears later.
- `persist_cursor/1`: if `watermark > cursor`, call `WAL.put_cursor(instance, :dispatch, watermark)` and update `cursor`.
- `:poll`: `fill`, `start_jobs`, `persist_cursor`, reschedule `poll_ms` (single timer ref; cancel the old one when rescheduling).
- `terminate/2`: `persist_cursor` inside `try … catch :exit, _ -> :ok`.
- `tick/1` keeps its signature and `{:ok, count}` return. `handle_call(:tick, from)` → `fill` + `start_jobs`, append `{from, completed}` to `waiters`, `maybe_reply_waiters`.
- `maybe_reply_waiters/1`: when `running`, `runnable` and `pending` are all empty, loop `fill` + `start_jobs` until either something is pending again (return; a later completion re-checks) or `read_seq` stops advancing. In the second case, `persist_cursor` and reply `{:ok, completed - c0}` to each waiter. Update the `tick/1` doc to "drain until caught up; returns envelopes fully handled during the call".
- Existing `test/ankusa/dispatch_test.exs` tests must pass unchanged. Add:
  - `"a slow envelope holds the cursor while other ordering keys proceed, and same-key deliveries stay in seq order"`:
    - `GateSink` sends `{:started, env.id, self()}` to `opts[:pid]` (the test pid), then blocks in the delivery task on `receive {:go, ^id}` (5 s timeout → `{:error, :gate_timeout}`). Its `ordering_key(env, _)` = `env.tenant_id`. The test releases a delivery by sending `{:go, id}` to the task pid it received.
    - `dispatch: %{poll_ms: 10, concurrency: 8}`; append e1 (tenant "a"), e2 ("a"), e3 ("b").
    - Assert `{:started, e1.id, p1}` and `{:started, e3.id, p3}`, and `refute_receive {:started, e2.id, _}, 100`.
    - Send `{:go, e3.id}` to `p3`, wait 50 ms, assert `WAL.get_cursor(inst, :dispatch) < e1.seq`.
    - Send `{:go, e1.id}` to `p1`, assert `{:started, e2.id, p2}`, send `{:go, e2.id}` to `p2`, then poll until the cursor == e3.seq (2 s max). The dispatch_test `build_env/1` sets no tenant, so build these envelopes with `%{build_env("src1") | tenant_id: "a"}`.
  - `"a sink that raises is retried and dead-lettered instead of crashing dispatch"`:
    - The sink raises `RuntimeError`; `max_attempts: 2`, `max_sleep_ms: 5`.
    - `Pipeline.tick/1` returns `{:ok, 1}`, and the DLQ has one entry with reason `{:sink, RaisingSink, {:raised, %RuntimeError{}}}`.

### 8. Compactor: bounded segments via `storage.roll_bytes`
- `lib/ankusa/storage/compactor.ex`:
  - Replace `@read_limit 1_000_000` with `@read_chunk 256`.
  - `compact/1` loops. It collects records from `state.cursor` in `@read_chunk` reads, converting to `%{key, payload}` as today, until the accumulated `byte_size(payload)` ≥ `config.storage.roll_bytes` or a read returns fewer than `@read_chunk`.
  - If anything was collected, it writes one segment with the existing body (encode, `BlobStore.put`, `Index.append`, `put_cursor`, `truncate_through(min(last_seq, dispatch_seq))`, telemetry) and advances `state.cursor`.
  - It repeats while the last read was full; it returns `{segments_written, state}`.
  - `tick/1` doc: "returns the number of segments written".
  - `roll_ms` stays unimplemented, as before; don't touch it.
- Test in `test/ankusa/storage_test.exs`: `"roll_bytes caps segment size: a backlog compacts into several segments"`.
  - Config `storage: %{interval_ms: 0, roll_bytes: 1}`.
  - Commit 3 envelopes and set the dispatch cursor to the last seq.
  - Assert `{:ok, 3} == Compactor.tick(inst)`, 3 keys in `BlobStore.list(inst, "seg")`, and every id round-trips through `Storage.fetch/2`.

### 9. E2E harness settings + false docs
- `examples/oban-consumer/k8s/20-consumer.yaml`: add env `POOL_SIZE: "30"`, since the Repo pool of 10 was shared by HTTP inserts and 20 Oban workers.
- `examples/oban-consumer/run.sh`: `RATE` default → `300`, and rewrite the stale lines 6-10 comment accordingly. If step V4 shows steady `drain_s > 30` or `shed > 0` at 300 on the reference machine, set the default to the highest of 200/100 that passes and say why in the comment.
- Correct docs that the changes make false:
  - docs/testing.md: the results table and the Known issue section → root cause + fix + new numbers.
  - docs/deployment.md: "Dispatch throughput" section; lines 35-40 (DiskLog requires every WAL role in one BEAM node; splitting roles needs WAL.Postgres).
  - docs/architecture.md: topology 2 (remove the DiskLog shared-volume role split) and batcher step 4.
  - docs/delivery.md: pipeline and retry sections; ordering keys and `ordered: true`; Kafka "inline retries block the dispatch batch" → "one delivery per key at a time".
  - docs/configuration.md: new keys and defaults.
  - docs/storage.md: compaction steps.
  - `ankusa_server/config-examples/reference.yml`: batcher/dispatch keys.
  - `Edge.BatcherSupervisor` moduledoc.

## Critical files & anchors
- `ankusa_postgres/lib/ankusa/wal/postgres.ex` — `append/2`, `insert_winners/2`, `backfill_dedup_seq/2` (delete): the chaos fix + dedup seq-scan fix.
- `lib/ankusa/wal/disk_log.ex` — `init/1` next_seq, `replay/3`/`parse/5`, `do_truncate/2` (delete), `persist_term/2`.
- `lib/ankusa/dispatch/pipeline.ex` — full rewrite; `tick/1` contract is what the existing tests use.
- `lib/ankusa/edge/batcher.ex` — `handle_call/3`, `flush/1` → async `start_commit/1`.
- `lib/ankusa/storage/compactor.ex` — `compact/1`, `@read_limit`.

## Verification
Prereqs: Docker. Run `docker compose up -d --wait` in `ankusa_postgres/`, `ankusa_rabbitmq/`, `ankusa_kafka/`. For e2e: `kind`, `kubectl`, `mix`.

- **B1 baseline, core bench (before steps 2–8, after step 1):** from the repo root, `MIX_ENV=test N=20000 CONCURRENCY=256 SINK_LATENCY_MS=5 mix run bench/core_bench.exs`. Record the JSON. Expect `end_to_end_per_s` ≈ ≤200 (sequential 5 ms sink) and `missing: 0`.
- **B2 baseline, e2e with the fixed loadgen (before steps 2–8):** `cd examples/oban-consumer && RATE=60 ./run.sh`. Record all three phases. Expect the chaos phase may report `missing > 0`, which reproduces root cause 1. If it happens to be 0 this run, record that and rely on V1.
- **Loadgen pacing check (step 1):** `docker run -d --rm --name ankusa-lg -p 4000:4000 jamescarr/ankusa:edge`, then `cd tools/loadgen && mix loadgen.run --url http://localhost:4000/webhooks/demo --rate 200 --duration 10`. `sent` must be 2000 ±5% and `sent_per_s` ≈ 200 (old code: ≈ half). Then `docker rm -f ankusa-lg`.
- **V1 chaos root cause (step 2):** in `ankusa_postgres/`, run `mix test test/ankusa/wal_postgres_test.exs` with the new test **before** the fix: it must fail with missing seqs. After the fix it passes, along with the other 10 tests.
- **V2 DiskLog seq floor (step 3):** the new wal_disk_log test fails before the fix (seq 1) and passes after.
- **V3 after all steps, core bench:** same command as B1. Expect `missing: 0` and `end_to_end_per_s` ≥ 10× B1: with concurrency 32 and a 5 ms sink the theoretical rate is ~6400/s. Record the numbers next to B1 in docs/testing.md.
- **V4 after all steps, e2e:**
  - `cd examples/oban-consumer && RATE=60 ./run.sh`, then `RATE=300 ./run.sh`.
  - Every phase must report `missing: 0` and `sha_mismatches: 0`, including chaos. `extra deliveries > 0` in chaos is expected and allowed (redelivery above the watermark after the worker kill).
  - Burst `drain_s` must drop far below B2's.
  - Record all numbers in docs/testing.md, replacing the table. Apply the step-9 RATE fallback rule if needed.
- **AGENTS.md gate (every package touched):**
  - `mix format --check-formatted && mix compile --warnings-as-errors && mix test` in `.`, `ankusa_postgres/`, `ankusa_rabbitmq/`, `ankusa_kafka/` (Kafka: the documented container command if CMake is missing) and `ankusa_server/`.
  - `mix compile --warnings-as-errors` in `examples/*/ingest_app`, `examples/oban-consumer/consumer_app`, `tools/loadgen`.
  - No dependency changes, so no lockfile churn.

## Assumptions & contingencies
- Req's default Finch pool must be ≥ `dispatch.concurrency` (32), or HTTP deliveries hit pool checkout timeouts. After `mix deps.get`, read the default pool size in `deps/finch/lib/finch.ex` (unverified — confirm first). If it is < 32, set the `dispatch.concurrency` default to that size.
- `pg_advisory_xact_lock(hashtext(...))` needs no extension; the test Postgres is `postgres:16-alpine`. Two instance names that hash-collide just share a lock (correct, slightly slower).
- If V3 shows `end_to_end_per_s` < 10× B1, profile before tuning defaults. Check `dispatch.batch`/`poll_ms` first (reads should loop while full). Don't lower correctness settings.
