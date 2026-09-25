# Ra-backed distributed WAL, cursor leases, and a distributed-WAL test program

## Context

`WAL.Postgres` is the only WAL that more than one node can share, and it has
structural limits for a fleet. The request is to replace it as the recommended
fleet WAL, and to plan thorough testing of distributed WAL handling: happy path,
failure (sad) path, and chaos.

Ra is Team RabbitMQ's Raft library and the engine behind quorum queues. The
original framework plan already named it as the "replicated local log"
(`plans/webhook-ingest-framework-plan.md:52,122,234,297`), and
`lib/ankusa/wal/disk_log.ex:6` already refers to "the Postgres/Kafka/Ra
adapters".

User decisions (fixed, do not revisit):
- **Backend = Ra** (not FoundationDB). This deliberately drops the "never BEAM
  distribution" rule (`docs/architecture.md:44-52,198`,
  `ankusa_postgres/lib/ankusa/wal/postgres.ex:7`) for the WAL tier.
- **Cursor leases with fencing are in scope**, so `:dispatch` and `:storage` can
  run more than one replica. That lifts the singleton rule in
  `docs/deployment.md:43-49`.
- `ankusa_postgres` stays supported. It implements the new lease contract too, so
  every adapter meets one behaviour. Whether to deprecate it is a separate call
  after Phase 7's numbers.

## Problems with the shared Postgres WAL (verified in code)

1. **Appends are serialized fleet-wide for each instance.**
   `pg_advisory_xact_lock(hashtext("ankusa_wal:" <> instance))` is held from
   before the insert until COMMIT (`postgres.ex:48-62,276-278`). One append per
   instance commits at a time, with a network round trip plus an fsync inside the
   lock. `docs/testing.md:340-343` records this as the accepted cost of the chaos
   fix.
2. **Surviving loss of the box depends on Postgres HA outside our control.** An
   async replica promoted on failover loses commits we already acked with `2xx`.
   The claim in `docs/architecture.md:24-25` ("`WAL.Postgres` is what survives
   losing the box") only holds with synchronous replication, which nothing checks
   or documents.
3. **No lease on either cursor.** Two `:dispatch` nodes deliver everything twice.
   Two `:storage` nodes compact the same ranges and write duplicate index rows
   (`docs/deployment.md:43-49`). A lost or zombie worker is only replaced when
   the orchestrator notices, and nothing fences the old one.
4. **`put_cursor` is a blind overwrite.**
   `ON CONFLICT … DO UPDATE SET seq = EXCLUDED.seq` (`postgres.ex:171-172`). A
   stale writer (the zombie in 3) can move a cursor backwards, and truncation is
   computed from those cursors (`compactor.ex:185-189`).
5. **Truncation is `DELETE … WHERE seq <= $2`** (`postgres.ex:184`). That puts
   queue-style churn on a heap table, so vacuum load grows with ingest rate.
6. **The docs don't match the code:**
   - The moduledoc says step 3 "joins `ankusa_wal_dedup` back to `ankusa_wal`"
     (`postgres.ex:30-32`). The code reads `ankusa_wal_dedup.seq` directly
     (`postgres.ex:316-341`).
   - "The lock covers the whole `claim_dedup` → `COMMIT` window"
     (`postgres.ex:57-58`) is wrong. The lock is taken in `insert_winners/3`,
     after `claim_dedup/2` (`postgres.ex:117,276`).
   - `stats/1` reports `next_seq: (max_seq || 0) + 1` (`postgres.ex:214`). After
     a full truncation that is 1, while `BIGSERIAL` continues much higher.

## How Ra addresses these

| # | Ra adapter |
|---|---|
| 1 | Seq is assigned inside the state machine's `apply/3`, which follows Raft log order. **Commit order = seq order, with no lock.** The integer `seq` contract, the 20-digit segment keys (`compactor.ex:208`) and the JSON `seq` (`edge/router.ex:69,72`) all stay unchanged. The leader batches and pipelines commands. |
| 2 | A command is acked only after a majority of members have fsynced it (Ra's `ra_log_wal`). |
| 3, 4 | Leases, fencing tokens and cursors all live in the replicated state. `put_cursor` applies `max`. A write carrying a stale token is rejected. |
| 5 | `truncate_through` removes seqs from the state. Ra v3 `live_indexes/1` compaction then drops segments that no longer hold live entries. No row churn. |

Ra facts this plan relies on. Checked against `ra-3.2.0` from hex (2026-09-01)
and `ra/docs/internals/COMPACTION.md`; Phase 0 re-proves each one:
- `ra_machine` callbacks include `apply/3`, `live_indexes/1`, `init_aux/1`,
  `handle_aux/5`, `version/0`, `which_module/1` and `tick/2`. The command meta
  carries `system_time`, `index`, `term` and `machine_version`
  (`ra_machine.erl:214-217`).
- **`ra_kv` (bundled in 3.2.0) is the reference pattern:** the machine state maps
  `key → raft index` only, and values are read back from the log with
  `ra_server_proc:read_entries/4`, locally or through `erpc` to the leader
  (`ra_kv.erl:172-226,254-272`). Payloads never sit in the machine's memory or in
  snapshots.
- `ra:start_server/5` takes `initial_members`, and `ra:restart_server/2` restarts
  a member that has already been started (`ra.erl:194-222,501-512`).
- `ra:process_command/3` returns `{:ok, reply, leader}`, `{:error, _}` or
  `{:timeout, server}` (`ra.erl:795-828`).

## Design

### Topology

- New role **`:wal`**. A node with this role runs one Ra member per Ankusa
  instance. Production shape: a 3-member (or 5-member) StatefulSet with a
  persistent volume per pod, running the same image as every other role (the
  "one image, many deployments" rule in `docs/deployment.md:20`).
- Edge, dispatch and storage nodes are **Ra clients**. They connect to the
  `:wal` nodes over Erlang distribution as **hidden nodes** with
  `-connect_all false`, so edges don't form a full mesh with each other.
- Membership is a static list in config:
  `wal: {Ankusa.WAL.Ra, members: [:"ankusa@ankusa-wal-0.ankusa-wal", …]}`. That
  gives stable identities with no discovery dependency.
- A single node with `roles: [:edge, :dispatch, :storage, :wal]` and one member
  still works: a one-member Raft cluster. `DiskLog` stays the laptop default.

### Package `ankusa_ra`

A separate adapter package, following the placement rule in `AGENTS.md` and
`docs/packaging.md`: only fleet deployments need Ra. Its `mix.exs` mirrors
`ankusa_postgres/mix.exs`, including the path/Hex `ankusa_dep/0`.
Dependencies: `{:ra, "~> 3.2"}`, and `{:stream_data, "~> 1.1", only: :test}`.
Ra is pure Erlang: no NIF, so the Alpine server image keeps working.

Modules:

- **`Ankusa.WAL.Ra`**, the `@behaviour Ankusa.WAL` adapter, registered as
  `Ankusa.via(instance, :wal)`.
  - `start_link/1` starts the client GenServer. When the node has the `:wal`
    role, it also starts or restarts the local Ra system and member:
    - Ra system name `:"ankusa_ra_#{instance}"`, data dir `Config.path(config, "ra")`.
    - Cluster name `:"ankusa_wal_#{instance}"`.
    - Server id `{:"ankusa_wal_#{instance}", node()}`.
    - Bootstrap: if the Ra directory already knows the server, call
      `restart_server`. Otherwise call `start_server` with the configured
      `initial_members`.
  - The client tracks the current leader. It sends to the cached leader and, on
    `{:timeout, _}`, `{:error, :noproc}` or `{:error, :nodedown}`, re-resolves
    the leader (`ra:members/1` against any reachable member) and retries until the
    **`append_timeout_ms`** deadline (default 10 000). That deadline must stay
    below the batcher's 15 000 ms call timeout (`edge/batcher.ex:55`); when it
    passes, the adapter returns `{:error, :timeout}` and the batcher maps that to
    `503` (`edge/batcher.ex:116-122`).
- **`Ankusa.WAL.Ra.Machine`** (`@behaviour :ra_machine`, `version/0 = 1`). Pure
  data only:
  - `next_seq`: starts at `floor + 1`, never reused.
  - `entries`: a `:gb_trees` of `seq → {raft_index, position}`.
  - `live`: `raft_index → count of live seqs`. This drives `live_indexes/1`.
  - `dedup`: `{tenant_id, source_id, dedup_key} → seq`. Permanent, the same
    contract as the other adapters. See risk R3.
  - `batches`: `batch_id → results`, bounded: entries expire once
    `meta.system_time` is 10 minutes past them. This makes retries idempotent.
  - `cursors`: `name → seq`.
  - `leases`: `name → %{holder, token, expires_at}`.
  - `floor`: the highest seq truncated so far.
- **Commands:**
  - `{:append, batch_id, [record]}`, where a record is
    `{event_id, tenant_id, source_id, dedup_key, envelope_binary}`.
    - A duplicate `batch_id` returns the stored results and writes nothing new.
      This covers a commit whose reply was lost: a retry to the new leader
      returns the original seqs.
    - Otherwise records are processed in order:
      - A collision with an existing dedup key gives `{:duplicate, seq}`.
      - A dedup key repeated inside the same batch resolves to the seq its first
        occurrence got.
      - Everything else gets `next_seq`, is inserted into `entries` pointing at
        this command's `meta.index`, and the command's `live` count goes up.
    - Reply: the per-record result list, the same shape as the behaviour's
      `{:committed, env} | {:duplicate, seq}`. The client rebuilds the
      envelopes so the reply carries no payload bytes back.
  - `{:put_cursor, name, seq, token}`: rejected with `{:error, :fenced}` if
    `token` is below the current lease token for `name`. Otherwise
    `cursors[name] = max(old, seq)`.
  - `{:truncate_through, seq, token}`: fenced by the `:storage` lease token.
    Removes entries with seq ≤ `seq`, decrements the `live` counts, raises
    `floor`, and emits `{release_cursor, …}` so Ra snapshots and compacts.
  - `{:acquire_lease, name, holder, ttl_ms}` and
    `{:renew_lease, name, holder, token, ttl_ms}`: expiry is computed from
    `meta.system_time`, so every replica decides the same way. Acquiring an
    expired or free lease increments `token`. Renewing with the wrong token
    returns `{:error, :lost}`.
  - `{:release_lease, name, holder, token}`.
  - `{:import, …}`: used only by the Phase 6 migration.
- **Reads:**
  - `read(after_seq, limit)`: a `handle_aux` query returns up to `limit` pairs of
    `{seq, raft_index, position}`. The client then calls
    `ra_server_proc:read_entries/4` for the distinct raft indexes, using the
    local member when its `last_applied` is at least the highest index (the same
    rule as `ra_kv.erl:177-196`) and the leader otherwise. It decodes the batch
    commands and picks out each position.
    - A lagging member's state is always a prefix of the log, so a stale read
      can hide the tail but can never show seq N without every seq below N.
      That is the contract in `lib/ankusa/wal.ex:17-20`.
  - `get_cursor` uses `ra:consistent_query`, so it sees the writer's own latest
    cursor.
  - `stats` uses a leader query of `overview/1`. `next_seq` is exact.
- **Payload size.** One Raft command must not carry a whole 256-record batch
  (`config.ex:27`) at up to 8 MB per body (`config.ex:16`).
  - `append/2` splits a batch into consecutive commands, each at most
    `max_command_bytes` (default 16 MiB), sent in order.
  - If an early command commits and a later one fails, the whole call returns
    `{:error, _}`. The batcher then answers `503` for every caller, so no `2xx`
    is sent for the committed part. The provider retries, dedup absorbs the
    records that have a key, and records without a key are delivered twice. That
    is the same outcome as an ambiguous Postgres COMMIT, and I10 counts it.
  - `validate_config!/1` raises at boot when
    `max_body_bytes > max_command_bytes - 64 KiB`, the same fail-fast pattern as
    `ClaimCheck.validate_config!/1` (`claim_check.ex:202-212`).

### Core contract change: leases and fencing (every adapter)

`lib/ankusa/wal.ex`:

```elixir
@type lease :: %{name: atom(), holder: String.t(), token: pos_integer(), ttl_ms: pos_integer()}
@callback acquire_lease(server(), name :: atom(), holder :: String.t(), ttl_ms :: pos_integer()) ::
            {:ok, lease()} | {:error, {:held, holder :: String.t()}}
@callback renew_lease(server(), lease()) :: {:ok, lease()} | {:error, :lost}
@callback release_lease(server(), lease()) :: :ok
@callback put_cursor(server(), name :: atom(), seq :: non_neg_integer(), token :: pos_integer()) ::
            :ok | {:error, :fenced}
@callback truncate_through(server(), seq :: non_neg_integer(), token :: pos_integer()) ::
            :ok | {:error, :fenced}
```

Two new clauses go into the contract text:
- Cursors never decrease.
- A write carrying a token lower than the current token for that lease name is
  rejected.

This is a clean cutover: the 3-arity `put_cursor` and 2-arity
`truncate_through` are removed from the behaviour, the facade and every adapter.

- **`WAL.DiskLog`:** leases are held in the GenServer state. Tokens are persisted
  in the existing fsynced `.cursors` term file (`persist_term/2`), which must
  never be torn. Since only one BEAM node can host a DiskLog, a lease there only
  guards against a supervisor restart racing a stale process.
- **`WAL.Postgres`:** new table `ankusa_wal_leases(instance, name, holder,
  token, expires_at)` in `migration.ex`. Acquire and renew are one
  `INSERT … ON CONFLICT DO UPDATE … WHERE expires_at < now() OR holder = $holder`
  using the database clock. `put_cursor` becomes
  `… DO UPDATE SET seq = GREATEST(ankusa_wal_cursors.seq, EXCLUDED.seq) WHERE $token >= (SELECT token FROM ankusa_wal_leases …)`,
  and `truncate_through` gets the same fence. This phase also fixes the doc
  errors in problem 6.
- **Callers:**
  - `Ankusa.Dispatch.Pipeline` (`pipeline.ex:75-117,551-553`) and
    `Ankusa.Storage.Compactor` (`compactor.ex:47-48,185-189`) boot in
    **standby**. They loop on `acquire_lease` every `lease.ttl_ms / 3`, and only
    after acquiring do they read the cursor and start work.
  - They renew every `ttl_ms / 3`. On `{:error, :lost}`, `{:error, :fenced}`, or
    a renew that hasn't succeeded within `ttl_ms - safety_margin_ms` by the
    local monotonic clock, they stop: cancel in-flight tasks, drop admitted
    state, and go back to standby.
  - Holder id: `"#{node()}/#{System.unique_integer}"`, so every boot is distinct.
  - Defaults: `lease: %{ttl_ms: 15_000, safety_margin_ms: 3_000}` under both
    `dispatch` and `storage` in `Config`.
  - Dispatch that is still running after it has lost the lease can only cause
    extra deliveries (at-least-once already allows those). Its cursor writes are
    fenced.
- **`Ankusa.Storage.Index` must be shared before storage can have a standby.**
  Today the index lives on the storage node's local disk (`index.ex:6-8`,
  `docs/deployment.md:48-49`), so a standby taking over would have no index.
  - Change: the compactor writes a per-segment sidecar, `seg/<first>-<last>.idx`
    (the framed rows via `Ankusa.DurableLog` encoding), to the blob store
    **before** `put_cursor`.
  - `Index.open/1` rebuilds the local ETS table and file from the sidecars at or
    after its local high-water mark, so a new holder catches up from the blob
    store.
  - The local file becomes a cache. Sidecars are deterministic and idempotent,
    the same way segments are (`docs/storage.md:253-254`).

### Other touched surfaces

- `lib/ankusa/config.ex:72`: add `:wal` to `@roles` and to the `@role_names`
  list parsed from `ANKUSA_ROLES`.
- `instance.ex:62-68`: `wal_children/2` also starts the adapter when the only
  role is `:wal`.
- `config.ex:21-24`: rewrite the batcher comment, since the Ra leader batches
  commands itself.
- `ankusa_server/lib/ankusa_server/config.ex:59-86,411-421`: add `wal` to
  `@roles`, and a `"ra"` WAL kind with `members`, `append_timeout_ms` and
  `max_command_bytes`. Add distribution settings (node name, cookie from a
  secret file, TLS distribution) to `ankusa_server/rel/`. `ankusa_server`
  path-depends on `ankusa_ra`.
- Docs:
  - `docs/storage.md`: new "Replicated Ra WAL" section. Present Postgres as the
    alternative.
  - `docs/architecture.md`: rewrite 44-52 and 194-217 so the WAL tier uses
    distribution while every other boundary stays "durable state, not RPC".
  - `docs/deployment.md:43-49`: replace the singleton section with lease
    semantics. Add the `wal` role and the TLS distribution requirement.
  - `docs/testing.md`, `docs/configuration.md`.
  - The `CHANGELOG.md` of each package whose behaviour changes.

## Test program

Every level checks the same invariants. They are implemented once in
`Ankusa.WAL.Checker` (in `ankusa_ra/test/support/`), a history checker that
takes a list of `{client, op, invoke_ts, complete_ts, result}` events.

| Id | Invariant |
|---|---|
| I1 | **Ack ⇒ durable.** Every id acked with `201` or `200` can be read from the WAL or from segments after any fault sequence, including simultaneous power loss of every member. |
| I2 | **Integrity.** Every record read back byte-matches what was sent (sha256 of the body). Nothing appears that no client sent. |
| I3 | **Commit-order prefix.** A cursor-following reader sees seqs strictly increasing. Once it has seen N, a final full scan contains no seq ≤ N that the reader missed. |
| I4 | **Seq uniqueness.** No seq belongs to two event ids. A `batch_id` retry never allocates new seqs. |
| I5 | **Dedup.** Each `(tenant, source, dedup_key)` has exactly one committed seq, forever, across truncation, snapshots and leader changes. Every `{:duplicate, s}` names that seq. |
| I6 | **Cursor monotonic and fenced.** Stored cursors never decrease. A write with a stale token never takes effect. |
| I7 | **Truncation safety.** `truncate_through(n)` removes nothing above n. Records above `min(dispatch, compactor)` are still readable. |
| I8 | **Bounded unavailability.** Without quorum, edges answer `503` within `append_timeout_ms + 1 s` and never `2xx`. After quorum returns, `2xx` resumes within 2 × the election timeout plus the client retry backoff. |
| I9 | **Single effective holder.** For each lease name, only the latest token's writes are accepted. Failover completes within `ttl_ms + ttl_ms/3`. |
| I10 | **At-least-once accounting.** Missing deliveries must be 0. Extra deliveries are reported and may only come from ambiguous commits or a fenced zombie dispatcher; the checker attributes each one to its cause. |

### Level 0: shared conformance suite (happy path, every adapter)

`Ankusa.WAL.ConformanceCase` lives in core `lib/` inside
`if Code.ensure_loaded?(ExUnit.CaseTemplate)`, so adapter packages can
`use Ankusa.WAL.ConformanceCase, adapter: …` from their own `test/`. It is
parameterized by an adapter `boot/1` callback. It runs in core against
`DiskLog`, in `ankusa_postgres` against Postgres, and in `ankusa_ra` against a
1-member cluster and a 3-member `:peer` cluster.

Cases:
- Append/read round-trip; results in input order; `append([])`.
- Seq strictly increasing across batches; gaps allowed.
- Dedup: within one batch, across batches, `nil` keys never collide, scoped per
  tenant, still enforced after `truncate_through` (the existing
  `wal_postgres_test.exs:114-126` case, generalized).
- `read` paginates at `limit`. Reading past the tail returns `[]`.
- Cursors: default 0, persist, **monotonic** (a lower `put_cursor` is a no-op).
- `truncate_through` is idempotent and never removes anything above its argument.
- Restart: no seq is reused after a full truncation followed by a restart (the
  existing DiskLog case, generalized).
- `stats` has the right shape, and `next_seq` is exact after a full truncation
  (the Postgres fix for problem 6).
- Leases: acquire, renew, release; expiry; a second holder acquires after expiry
  with `token + 1`; stale-token `put_cursor` and `truncate_through` return
  `{:error, :fenced}`.
- Concurrency:
  - 8 writers race one dedup key and exactly one commits (the existing Postgres
    case, generalized).
  - A cursor-following reader never skips a commit: 8 writers × 25 appends, the
    existing `wal_postgres_test.exs:156-242` case, run against every adapter.

### Level 1: Ra cluster happy path (`ankusa_ra/test`, `:peer` nodes)

A `ClusterCase` starts 3 (or 5) real BEAM nodes with OTP `:peer`. Each gets its
own data dir, loads `ankusa_ra`, and forms the cluster. No docker is needed.

- Bootstrap from empty. Restarting every member uses `restart_server`, not a new
  cluster.
- Appends from a client on a non-member (hidden) node, from a client on a
  follower node, and from a client on the leader node.
- Reads served by the local follower once caught up, and by the leader
  otherwise. Byte-exact bodies from 0 B up to `max_body_bytes`.
- Snapshot and compaction: write 50 000 records, truncate the first half, force
  `release_cursor`. Then check that:
  - untruncated bodies are still readable;
  - segments holding only truncated entries are deleted from disk;
  - the snapshot size does not grow with payload bytes.
- Snapshot install carries live entries: add a fresh 4th member after
  compaction. It catches up, and reads served from it return every live body.
- Membership: add and remove members, and transfer leadership, under constant
  append load, with zero errors.
- Leases: dispatch and storage run as 2 replicas each; exactly one of each is
  active; `stats` shows the tokens.
- A batch that exceeds `max_command_bytes` is split. `validate_config!/1`
  rejects `max_body_bytes` that is too large.

### Level 2: deterministic failure paths (`:peer` cluster, fault inserted at a known point)

Each scenario records its history and runs it through the Checker.

1. **Leader killed mid-append.** `:peer.stop` on the leader right after the
   client has sent the command. The client retries on the new leader with the
   same `batch_id`. Expect exactly one set of seqs (I4), or `{:error, :timeout}`
   with no ack.
2. **Reply lost after commit.** Block the reply path: disconnect the client node
   from the leader with `:erlang.disconnect_node/1` once `last_applied` covers
   the command. The retry returns the stored results, and no second copy exists.
3. **Minority loss.** Kill 1 of 3 members: appends continue and p99 stays within
   2× baseline. Kill 2 of 3: every append returns an error within the I8 bound,
   and no `2xx`. Restart them: every earlier ack is readable (I1).
4. **Follower far behind.** `:sys.suspend` one follower's Ra server, write
   100 000 records with truncation in between, then resume it. It catches up
   through snapshot install, and I3 and I5 hold on reads it serves.
5. **Torn log on a stopped member.** Truncate the last N bytes of that member's
   `ra_log_wal` file, and flip a byte inside a segment. It restarts, detects the
   CRC failure, catches up from its peers, and never serves the damaged entry
   (I2).
6. **Stale lease holder (GC-pause simulation).**
   `:sys.suspend(Ankusa.via(inst, :dispatch))` for longer than `ttl_ms`. The
   standby acquires `token + 1` and advances. After the suspended process
   resumes:
   - its `put_cursor` returns `{:error, :fenced}`;
   - it steps down within one renew interval;
   - the cursor never went backwards (I6).
   Run the same drill for the compactor, which also covers a fenced
   `truncate_through`.
7. **Lease clocks.** Change the leader after a lease is acquired, with a new
   leader whose `system_time` is skewed by ±10 s. Inject the skew with a
   test-only `time_offset_ms` in the machine config that is added to the leader's
   stamp; Phase 0 confirms where Ra takes `system_time` from. Clock skew may
   change failover time, but tokens keep I6 and I9
   intact.
8. **Distribution flaps.** Repeatedly `disconnect_node` between clients and
   members under load. Clients reconnect and retry. I1, I3 and I4 hold.
9. **Machine-version upgrade.** A mixed cluster: two members on machine v1, one
   on a test-only v2 (adds a no-op command).
   - v2 commands are not applied until all members run v2.
   - A rolling restart onto v2 loses nothing.
   - This is a regression drill for the failure mode in
     [rabbitmq-server#17504](https://github.com/rabbitmq/rabbitmq-server/issues/17504):
     new commands reaching a machine still on the old version, which caused a
     crash loop and silent loss.
10. **Oversized command, and a partial split failure.** Kill the leader between
    the first and second sub-command of a split batch. The caller gets
    `{:error, _}` (so `503`). The committed half is readable, and a resend
    through dedup yields no extra copies of records that have a key.
11. **Edge with no quorum.** Drive the real `Ankusa.Edge.Router` over HTTP with
    quorum down. Responses are `503` with `Retry-After`. The batcher queue fills
    and sheds as it does today (`edge/batcher.ex:88`), and never `2xx` (I8).

### Level 3: property / model-based (StreamData)

- A stateful property test runs against a 3-member `:peer` cluster. The command
  generator mixes:
  - appends: random batch sizes, a shared dedup key pool with collisions, `nil`
    keys, 3 tenants, bodies of 0 B to 256 KiB;
  - reads, `put_cursor` (with current and stale tokens), `truncate_through`,
    and lease acquire/renew/release;
  - faults: restart a member, kill the leader, suspend a follower.
- A pure reference model predicts each result. The postconditions check it, and
  the Checker checks I1–I7 on the whole history at the end.
- Runs: 200 per CI job. A nightly job runs `MAX_RUNS=5000` with seeds logged, so
  any failure can be replayed deterministically.
- The `Machine` alone also gets a fast property test. Its `apply/3` is pure, so
  random command sequences can be checked for model equality in-process, with
  thousands of runs per second.

### Level 4: chaos harness (docker compose, `ankusa_ra/chaos/`)

Topology:
- 3 `wal` members;
- 3 `edge` nodes behind nginx;
- 2 `dispatch` and 2 `storage` replicas using leases;
- MinIO/floci for segments;
- `tools/loadgen`;
- an HTTP sink consumer that records each delivery (`id`, sha256, seq) into
  Postgres;
- a `nemesis` container with `NET_ADMIN` that runs scenarios.

Each run:
- **Load.** Loadgen at `RATE` using a mix of 70% keyed, 20% `nil`-key and 10%
  deliberate resend traffic.
- **Cursor observer.** An observer client follows the WAL with its own cursor,
  for I3.
- **Stats sampler.** Samples `stats` every 250 ms, for I6 and I9.
- **Verify.** Afterwards, `mix loadgen.verify` (extended for I10 attribution)
  and a WAL-level Checker pass. Any violation fails the run.

Nemesis scenarios. Each runs on its own and in a `mixed` schedule, for 5 min per
scenario:

- `kill-leader`: `kill -9` the leader every 20 s.
- `kill-random`: `kill -9` a random member every 15 s.
- `pause-leader`: `SIGSTOP` the leader for 2 × the election timeout, then
  `SIGCONT`. A gray failure: the old leader wakes up believing it still leads.
- `partition-halves`: iptables majority/minority split, with the leader in the
  minority.
- `partition-leader-bridge`: the leader can reach only one follower.
- `partition-clients`: edges cut off from the leader but not from followers.
- `netem`: 100 ms ± 50 ms latency and 5% loss between members.
- `disk-full`: tmpfs size limit on one member, then on two members.
- `clock-skew`: libfaketime ±30 s on one member and on the active dispatch
  worker.
- `kill-dispatch-active` and `kill-storage-active`: `kill -9` the lease holder.
  The standby takes over. Storage must also be killed between segment PUT and
  `put_cursor`, to prove sidecar and segment idempotency.
- `zombie-dispatch`: `SIGSTOP` the active dispatcher for longer than the TTL,
  then `SIGCONT`. It must be fenced.
- `rolling-restart`: restart members one at a time under load.
- `rolling-upgrade`: the same, but on a new image carrying machine v2.
- `power-loss`: `kill -9` all 3 members at once, restart them, then check I1 at
  fleet scale. This is the loss checker (`test/ankusa/loss_test.exs`) applied to
  the replicated tier.
- `replace-member`: delete one member's volume, `remove_member`, then
  `add_member` a fresh one. It catches up through snapshot install.
- `big-bodies`: 20% of requests at 1–8 MB during `partition-halves`. This puts
  pressure on distribution buffers and checks that heartbeats don't cause false
  elections (log the election count).

Entry point: `ankusa_ra/chaos/run.sh <scenario|all>`. The report records, per
scenario:
- accepted/s, p50/p95/p99, and 503 count;
- missing, extra (with attributed cause), and each invariant as PASS or FAIL;
- the number of elections, and failover time for leader and lease changes.

### Level 5: Kubernetes end-to-end and release gate

Extend `examples/oban-consumer`:
- `k8s/01-ankusa-wal.yaml`: a 3-replica StatefulSet with a PVC per pod, a
  headless service, and a NetworkPolicy that allows the distribution ports only
  between Ankusa pods.
- `ankusa-worker` becomes 2 replicas.
- `run.sh` gains `WAL=ra`, which becomes the default, while `WAL=postgres` stays
  runnable.
- The chaos phase additionally deletes the Raft leader's pod and the active
  worker's pod.

The release gate in `AGENTS.md` stays `run.sh`, which now runs both `WAL`
modes.

### Level 6: performance

Extend `bench/core_bench.exs`, or add `ankusa_ra/bench/wal_bench.exs`, to
compare Postgres (advisory lock) with Ra:
- 1, 3 and 6 edge clients;
- body sizes 1 KiB, 64 KiB and 1 MiB;
- metrics: ack p50/p99, maximum sustained accepted/s, and leader CPU.

Record the results in `docs/testing.md` next to the existing tables. Phase 0
sets the gate numbers; Phase 7 checks them.

## Phases

The order keeps every package building and green after each phase. Phases 1 and
2 can run in parallel once Phase 0 passes. Level 1–3 tests are written together
with the code they cover, not afterwards.

### 0. Spike (go/no-go, throwaway branch)

A minimal `Machine` with `append`, `read` and `truncate`, using `live_indexes`
and `read_entries`, on 3 `:peer` nodes. Prove each item or stop:
1. Bodies are read back from the log after a snapshot, and after snapshot install
   on a new member.
2. Every node bootstraps with `start_server` using the same `initial_members`,
   and `restart_server` works after a full restart.
3. Clients on hidden nodes with `-connect_all false` work.
4. `meta.system_time` is the same on every replica.
5. Throughput: at 3 clients with 1 KiB bodies, Ra's accepted/s ≥ Postgres's. At
   8 MB bodies, there are no spurious elections.

Output: the gate numbers for Level 6, and a note on any Ra API that behaves
differently than described above.

### 1. Core lease contract, conformance suite, shared index

- Callback changes in `lib/ankusa/wal.ex`.
- `Ankusa.WAL.ConformanceCase`.
- Leases in `DiskLog` and `Postgres`, plus the Postgres doc and `next_seq` fixes.
- Standby, lease and step-down logic in `Pipeline` and `Compactor`.
- `.idx` sidecars and the index rebuild.
- Role `:wal` in `Config` and `Instance`.

Acceptance: the conformance suite passes on DiskLog and Postgres, and every
existing test still passes.

### 2. `ankusa_ra` package

- Machine, client, bootstrap, command splitting, `validate_config!/1` and
  telemetry. Telemetry events: `[:ankusa, :commit, …]` as today, plus
  `[:ankusa, :wal, :leader_change]` and `[:ankusa, :lease, :acquired | :lost]`.
- Mix tasks `ankusa.wal.members` (list, add, remove, transfer).

Acceptance: conformance passes on 1-member and 3-member clusters, and Level 1
passes.

### 3. Failure-path and property suites

Level 2 scenarios 1–11 and Level 3.

Acceptance: all green. The nightly property run finds no failure over 5 000 runs.

### 4. Chaos harness

Level 4 compose file, nemesis, Checker wiring, and the report.

Acceptance: `run.sh all` passes on the reference machine, and the report goes
into `docs/testing.md`.

### 5. Integration

- `ankusa_server` gets the YAML `wal: ra` kind, `rel/` distribution config, and
  the fleet compose file: `ankusa_server/compose/docker-compose.fleet.yml` gains
  `wal` services.
- Kubernetes example and `run.sh WAL=ra`.
- CI:
  - `.github/workflows/ci.yml` gets an `ankusa_ra` job with the same four checks
    as `ankusa_postgres` and no services, because it uses `:peer`;
  - a new nightly workflow runs `chaos/run.sh mixed` and the long property run.

### 6. Migration Postgres → Ra

`mix ankusa.wal.migrate --from-postgres URL --instance NAME` performs an offline
cutover:
1. The operator stops the edges.
2. The tool waits until the dispatch and compactor cursors reach `max(seq)`, and
   refuses to continue otherwise.
3. It writes `{:import, floor: max_seq, cursors: …}`.
4. It streams `ankusa_wal_dedup` in chunks of 10 000 as `{:import_dedup, chunk}`
   commands, keyed by dedup tuple, so the stream can be resumed.
5. It verifies counts.
6. The operator switches config.

Dedup must be copied: skipping it would re-accept provider retries of events
already delivered, violating I5. The `floor` import keeps segment keys and index
seqs from colliding.

Acceptance: a drill on the compose stack. Migrate at 1M dedup keys under a
replayed resend stream, and see zero re-accepted duplicates.

### 7. Performance and release gate

Level 6 numbers, and Level 5 `run.sh` in both modes.

Acceptance: the Phase 0 gates are met. `missing: 0` and every invariant passes
in every phase.

## Checks per phase (`AGENTS.md`)

- For every package touched: `mix format --check-formatted`,
  `mix compile --warnings-as-errors` and `mix test`. That means core,
  `ankusa_postgres` (compose up), `ankusa_ra`, and `ankusa_server`.
- Core's callback change touches every package that path-depends on core, so all
  of them need their checks run. After adding `ankusa_ra` as a dependency of
  `ankusa_server`, run `mix deps.get` there and commit `mix.lock`. CI runs
  `--check-locked` and `--check-unused`.
- The examples need `mix compile --warnings-as-errors`, and
  `npx tsc --noEmit` where they contain a TypeScript worker.

## Risks

- **R1: Distribution is now in the data path.** Mitigations:
  - hidden-node clients and `-connect_all false`;
  - TLS distribution plus a cookie from a secret (documented as required, and
    `ankusa_server` refuses to boot the `:wal` role without a cookie file);
  - `+zdbbl` sized for 8 MB messages.
  The `big-bodies` and `netem` scenarios measure false elections.
- **R2: Losing quorum stops ingest** (`503` and the provider retries). This is
  correct by the core invariant, but it is a new operational failure mode.
  Documented, with a `[:ankusa, :wal, :leader_change]` alert and an explicit
  rule: 5 members to survive 2 failures.
- **R3: The dedup ledger is permanent and in memory.** This matches `DiskLog`'s
  ETS set, but at fleet scale it grows without bound and bloats snapshots.
  - Phase 0 measures bytes per key.
  - `docs/storage.md` states the sizing, roughly 100 B/key: 100M keys is about
    10 GB per member.
  - A time-bounded dedup window would be a contract change (providers retry for
    days, not forever) and needs its own decision. It is not part of this plan.
- **R4: `ra_server_proc:read_entries/4` is not in Ra's `ra`/`ra_machine`
  semver-stable surface.** Mitigations: pin `~> 3.2`; the conformance and Level 1
  suites exercise it on every CI run. It is the same call `ra_kv` makes in the
  same package.
- **R5: Machine-version upgrades** (see Level 2 scenario 9). A `version/0` bump
  requires the `rolling-upgrade` chaos scenario to pass before release.
- **R6: Leases are safe through tokens, not clocks.** A holder that is paused
  can still deliver after its lease has expired. That is only extra deliveries,
  which I10 attributes. Documented in `docs/delivery.md`.
