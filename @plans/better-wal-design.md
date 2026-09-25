# Fix the Ra WAL review findings and run the unrun verification

Handoff plan for finishing the review of `eabd7e4` ("feat(wal): Ra-backed distributed WAL, cursor leases, and its test program"). Every design decision is already made below. Do not make new ones: if a step turns out to be impossible as written, stop and report it.

Repo root: `/Users/jamescarr/orca/workspaces/bandit_example/better-wal-than-pgsql` (called `$ROOT` below). HEAD is `d55ec41`. The reference design is `local://ra-distributed-wal-plan.md`. The full reviewer reports are at `agent://CallersStandby`, `agent://TestProgram` and `agent://IntegrationCI`, if they are still readable.

## Ground rules (read these first)

- **AGENTS.md applies.** Every package you touch must pass `mix format --check-formatted && mix compile --warnings-as-errors && mix test`. A core (`lib/`) change is not finished until `ankusa_postgres`, `ankusa_rabbitmq`, `ankusa_kafka`, `ankusa_nats` and `ankusa_ra` pass too. Never report a check as passing unless it ran in this session.
- **The local C toolchain is broken.** `ld: tapi error … libSystem.B.tbd` breaks NIF builds. Check `ankusa_kafka`, `ankusa_server` and `examples/kafka-sqs-consumer/ingest_app` with the AGENTS.md container recipe (`elixir:1.20.4-alpine`, repo root mounted at `/repo`, `MIX_BUILD_PATH=/tmp/build`), and adjust `-w` for each package. First run `rm -rf deps/crc32cer/c_build` in that package.
- **Commits must be unsigned:** `git -c commit.gpgsign=false commit …`. Use conventional commits, and keep code and docs in separate commits.
- **Do not change these semantics:**
  - The WAL has no uniqueness constraint.
  - The edge acks with 202.
  - Dedup happens only at dispatch, through `Ankusa.Dispatch.Receiver.decide/3`.
  - `Ankusa.DedupStore.record/6` returns `:deliver | :drop | {:error, reason}`, and an error stalls the pipeline.
- **`ankusa_ra` is unreleased.** Git has no `ankusa_ra-v*` tag. There is no compatibility to preserve for it, so delete compat code instead of adding it.
- **Run formatters once per package at the end of a phase, not after every edit.**

## Already done (uncommitted, in the working tree; keep all of it)

`git status` shows these 7 modified files:

- `ankusa_postgres/lib/ankusa/wal/postgres.ex`: `live_lease?/4` and the moduledoc SQL now use `FOR SHARE`. This fixes a fence race under READ COMMITTED. Verified: `ankusa_postgres` has 22 passing tests, and a mutation test showed the fence check matters.
- `lib/ankusa/wal/disk_log.ex`: moduledoc changed from `expires_at: 0` to `expires_at: nil`.
- `lib/ankusa/wal/conformance_case.ex`:
  - Case 8 now checks that a released or expired lease's own token is fenced.
  - Case 10 now checks that tokens stay monotonic across a restart, and that the old token is both fenced and `:lost`.
  - Verified: 13 passing core conformance tests.
- `ankusa_ra/test/test_helper.exs`: the probe now uses `ClusterCase.peer_config/1`, which carries the cookie. Without the cookie the probe always failed, so none of the 22 `:dist` tests ever ran.
- `ankusa_ra/test/support/cluster_case.ex`:
  - Added `peer_config/1` (`connection: :standard_io` plus the cookie), `start_peer!/1`, and a unique name prefix per `start_peers/1` call.
  - `start_cluster/2` now applies `opts[:config]` (a fun) and passes `wal_opts` to every member's boot. It stores the result in `cluster.wal_opts`.
  - `Node.boot(instance, _cluster, data_dir, wal_opts)`.
  - `Node.start_pipeline` now unlinks.
  - Added `Node.suspend_pipeline/1` and `Node.resume_pipeline/1`.
- `ankusa_ra/test/ankusa/wal_ra_cluster_test.exs` and `wal_ra_faults_test.exs`:
  - Both use a per-test `setup` instead of `setup_all`.
  - The `add_member` match is now `{:ok, _, _}`.
  - Drill 6 now uses `Peer.suspend_pipeline`.

The last full `cd ankusa_ra && mix test` gave **62 passing out of 71**, and that run happened before the unlink and suspend fixes.

---

## Phase 1: make the `ankusa_ra` test harness honest

Goal: `cd ankusa_ra && mix test` must run every `:dist` test, all green, and no assertion may be vacuous.

1. **Fix the `:badarg` from `Node.boot`.**
   - Find every call with `grep -n "boot" ankusa_ra/test`. `Node.boot` is `cluster_case.ex:321` and takes 4 args: `(instance, cluster, data_dir, wal_opts)`.
   - Every `:peer.call(peer, ClusterCase.Node, :boot, [...])` must pass `cluster.wal_opts`, or `Keyword.put(cluster.wal_opts, :members, <list>)` when the test is changing the member set, as its 4th element. It must never pass `cluster.members`.
   - Check the snapshot-install test in `wal_ra_cluster_test.exs` (around line 168) and `restart_member/2` (`cluster_case.ex:217`).
   - If `restart_member` still gets `:badarg`, print the peer's stderr: the peer uses `connection: :standard_io`, so wrap the call in `try` and `IO.inspect` the exit. Fix the argument that is actually wrong. Do not change what the helper returns.
2. **Rewrite the follower-append test** (`wal_ra_cluster_test.exs:58-70`):
   - Call `Ra.remote_command([follower], {:append, {make_ref(), 1}, rows}, timeout: 10_000)`, where `rows` comes from `Ankusa.WAL.Ra.record/1`-style binaries, the same shape `ra.ex:436` produces (`Enum.map(records, &record(&1.envelope))`).
   - If `record/1` is private, make it `@doc false def`.
   - Assert that the result is `{:ok, {:ok, [{:committed, _}, {:committed, _}]}}` and that `WAL.read` returns both records.
3. **Drill 2 duplicate check** (`wal_ra_faults_test.exs:110-111`): replace the check with `assert length(written) == 1`.
4. **Stop disconnecting peers to fake a lost reply** (drill 2, around `:95`, and drill 8, around `:319`).
   - Peers now use `:standard_io`, so `:erlang.disconnect_node/1` no longer halts them. But it does not reliably lose one reply either.
   - Replace the disconnect with `Peer.suspend_member(node)` / `Peer.resume_member(node)`, two new helpers in `ClusterCase.Node` that call `:sys.suspend/1` and `:sys.resume/1` on the local Ra server process (`:ra_directory.where_is(system, server_name)`).
   - Drill 2: suspend the leader after the command is sent, wait 20 ms, resume it, then assert that exactly one record exists.
   - Drill 8: suspend and resume the leader 5 times, 2 × the election timeout each, while appends run. Then assert that every acked seq is readable and no seq appears twice.
5. **Assert local member state instead of reading through the leader.** This covers faults drills 4 and 5 and the cluster snapshot-install test.
   - Add `ClusterCase.Node.local_state(instance)`. It returns `:ra.member_overview(server_id)`, which includes `:last_applied`, `:commit_index` and `:snapshot_index` (or the equivalent keys in the Ra version in `ankusa_ra/mix.lock`; check with `:ra.member_overview/1` in iex).
   - Assert that the restarted or new member's `last_applied >= ` the leader's commit index, polling for up to 30 s.
   - For snapshot install, also assert `snapshot_index > 0` on the new member.
   - Keep the existing `Peer.read` checks.
6. **Segment reclamation** (`wal_ra_cluster_test.exs:146-148`): record the list of segment file names under the member's data dir before the truncate. After it, assert that every file in that first list that held only truncated entries is gone. Concretely, `MapSet.disjoint?(before_old_segments, after_segments)` must hold for every segment except the newest one before the truncate.
7. **Split-command tests** (`wal_ra_cluster_test.exs:278-295` and `wal_ra_faults_test.exs:377-413`):
   - `max_command_bytes` is captured at `init` (`ra.ex:145`), so set it through `start_cluster(config: fn c -> … end)`, which now reaches the members, and give it a small value (for example 4096).
   - Then send one append whose records add up to more than 3× that size, and assert the records arrive as more than one Ra command. Check this from the leader's log via `:ra.member_overview/1` `:last_index` delta, which must be `> 1`.
   - In drill 10, delete the tautology (`:410`) and the no-duplicate-ids assertion after a resend with a new batch_id (`:412-413`). A resend is allowed to duplicate: the WAL has no dedup.
8. **Drill 6 zombie fencing** (`wal_ra_faults_test.exs:255-275`):
   - Suspend the active pipeline's node pipeline with `Peer.suspend_pipeline`, wait `ttl + ttl/3`, and assert that the standby has taken the lease (`lease_holder` differs, *and* its token is higher).
   - Resume the zombie. Then assert that a direct `WAL.put_cursor(instance, :dispatch, 1, old_token)` issued on the zombie node returns `{:error, :fenced}`.
   - Assert that within one renew interval the zombie's pipeline state has `lease: nil`: `:sys.get_state` via `Ankusa.whereis(instance, :dispatch)` on the peer.
   - Delete the `get_cursor >= cursor_after` assertion.
   - Add the same sequence for `:storage` / the compactor, and assert that `truncate_through(…, old_token)` returns `{:error, :fenced}`.
9. **`lease_holder`** (`cluster_case.ex` around line 388): return `nil` when the entry's `expires_at` is `nil` or earlier than now.
10. **Drill 3** (`wal_ra_faults_test.exs:115-142`): after the majority-loss assertions, `restart_member` both killed members and wait for a leader. Only then do the final `WAL.read` and assert that every earlier ack is readable.
11. **Drill 7** (clock skew): a uniform `time_offset_ms` cancels out, so give **one** member `time_offset_ms: 5_000` and leave the others at 0. `start_cluster` already takes the config fun, so add a per-node override in `start_cluster` with `opts[:member_config] :: %{node_index => fun}`.
    - Assert that no two holders ever hold `:dispatch` at once. Sample `lease_holder` every 50 ms for 3 × TTL, and let the leader move by killing it once.
12. **Drill 9** (machine upgrade): this belongs to Phase 2 step 6. Leave it until then.
13. **Drill 11** (503 on no quorum): the test node has no route or Batcher, so it gets a 404.
    - Start a real edge on a peer with `Ankusa.start_link` and a `Plug.Cowboy`/`Bandit` child, whichever `examples/oban-consumer/ingest_app` uses. Use a source route `/webhooks/drill11`.
    - Kill 2 of the 3 members, POST to that edge, and assert a 503 within `append_timeout_ms + 2_000`.
14. **Drill 12:** feed the Checker real events.
    - Record `put_cursor`, `truncate_through` and lease acquire/renew/lost events from the run. Use `:telemetry.attach_many` on `[:ankusa, :lease, :*]` plus wrapper calls.
    - Pass the real `final_cursors: %{dispatch: WAL.get_cursor(instance, :dispatch)}` instead of the literal 0.
15. **CI must fail when distribution is unavailable.**
    - In `ankusa_ra/test/test_helper.exs`, when `System.get_env("ANKUSA_REQUIRE_DIST") == "1"` and the probe fails, `raise "Erlang distribution unavailable; :dist suites would be skipped"`.
    - Move `Node.start` inside the `try`.
    - In `.github/workflows/ci.yml` (around lines 89-90), set `ANKUSA_REQUIRE_DIST: "1"` on the `ankusa_ra` test step.

Verify:

```sh
cd ankusa_ra
mix test test/ankusa/wal_ra_cluster_test.exs
mix test test/ankusa/wal_ra_faults_test.exs
mix test
```

The output must show `0 failures` and must not show the line `Excluding tags: [:dist]`.

Commit: `test(ra): per-test peers with a real cookie, and drills that can fail`. The Postgres `FOR SHARE` change and the conformance case edits go in a separate earlier commit: `fix(wal): fence reads lock the lease row; conformance covers expiry and restart tokens`.

## Phase 2: Ra adapter and machine (`ankusa_ra/lib/ankusa/wal/ra.ex` and `ra/machine.ex`)

1. **Make batch_id unique across restarts.** This is a blocker: without it, acked records can be lost.
   - `ra.ex:435` becomes `batch_id = :crypto.strong_rand_bytes(16)`.
   - In the machine's `{:append, {batch_id, n}, records}` clause, when a stored reply for `{batch_id, n}` exists **and** its result count differs from `length(records)`, reply `{:error, :batch_id_conflict}` and change nothing.
   - Add a machine unit test with two appends that use the same `{batch_id, n}` and different record counts.
2. **Call and command timeouts.**
   - In `ra.ex:321-348`, give every `GenServer.call` except `append` (which stays `:infinity`) an explicit timeout of `call_timeout(server)`, where `@call_slack_ms 5_000` and the timeout is `append_timeout_ms + read_timeout_ms + @call_slack_ms`. These are the defaults `@default_append_timeout_ms` and `@default_read_timeout_ms`; read the server state's values through `Ankusa.config(instance)` if they are available. Otherwise, use the module defaults plus the slack.
   - In `command/2` (the `:ra.process_command(leader, command, state.append_timeout_ms)` at `ra.ex:618`) and `aux/2` (`ra.ex:652`), pass `min(configured, max(deadline - mono_ms(), 1))` as the Ra timeout, where `deadline` is the loop's existing deadline.
   - `remote_command/4` (`ra.ex:706-720`): the `:ra.process_command(leader, command, read_timeout)` call at `:715` gets `min(read_timeout, max(deadline - mono_ms(), 1))`.
3. **Callers survive WAL call exits.** In `lib/ankusa/dispatch/pipeline.ex` and `lib/ankusa/storage/compactor.ex`, wrap the WAL calls in `try … catch :exit, _ -> …`:
   - `try_acquire`: an exit → `{:standby, standby(state)}`, the same as `{:error, {:held, _}}`.
   - `renew`: an exit → the `{:error, :lost}` branch.
   - `persist_cursor` (`pipeline.ex:692`) and the compactor `put_cursor` (`compactor.ex:342`): an exit → the `{:error, :fenced}` branch, which is a step-down.
   - The `get_cursor` read after acquire (`pipeline.ex:143,188` and `compactor.ex:208`): an exit → step down and return standby state.
4. **Bootstrap** (`ra.ex:219-239`): add the clause `{:error, {:already_started, _}} -> campaign(server_id); :ok` to the `restart_server` case, before the generic `{:error, reason}`.
5. **`aux!/2`** (`ra.ex:662`) must not raise inside the server.
   - Rename it to `aux_reply/2`, returning `{{:ok, reply} | {:error, reason}, state}`.
   - The `handle_call`s for `:get_cursor` and `:stats` reply with that tuple.
   - The client functions `get_cursor/2` and `stats/1` pattern-match: `{:ok, v} -> v`, `{:error, r} -> raise RuntimeError, "Ankusa.WAL.Ra #{op} failed: #{inspect(r)}"`.
6. **Ship the machine as version 1.**
   - `machine.ex:124` becomes `def version, do: 1`.
   - Delete the `apply(_meta, {:machine_version, 1, 2}, state)` clause (`:199-201`).
   - Fold the `dedup_state/1` defaults (`:346-350`) into `init/1`'s initial map, then delete `dedup_state/1` and every call to it.
   - `which_module/1`: map both `0` and `1` to `__MODULE__`.
   - Rename `ankusa_ra/test/support/machine_v3.ex` to `machine_v2.ex`. The module becomes `Ankusa.WAL.Ra.MachineV2`, with `version/0` returning 2 and `which_module(2) -> MachineV2`, `(0|1) -> Machine`.
   - Remove every "v1→v2 compatibility" sentence from `machine.ex` docs, `ankusa_ra/README.md` and `ankusa_ra/CHANGELOG.md`.
   - Rewrite drill 9 (`wal_ra_faults_test.exs:338-371`):
     1. Start a 3-member cluster on `Machine`.
     2. Do a rolling restart that boots `MachineV2` on each member in turn, using a `wal_opts` key `machine: MachineV2`. `Node.boot` passes it to `machine_config`; add that key in `ra.ex` bootstrap as `Keyword.get(wal_opts, :machine, Machine)`, `@doc false`.
     3. After 2 of the 3 members restart, assert that a v2-only command (which `MachineV2` defines, e.g. `{:v2_ping}` → `:pong`) is **not** applied: it returns an error or `:unsupported`. After the third, assert that it returns `:pong`.
7. **Machine `{:import, …}`** (`machine.ex:292`): when `state.next_seq != 1 or map_size(state.entries) != 0`, return `{state, {:error, :not_empty}, []}`. Otherwise keep the current behaviour. Add a machine unit test for it.
8. **`prune_batches`** (`machine.ex:369-383`): remove the `map_size(state.batches) > @max_batches or` condition and delete `@max_batches`. Pruning is by age only, with `@batch_retention_ms 600_000`, which far exceeds `append_timeout_ms` (10 s).
9. **Cache the leader for `DedupStore.Ra`.**
   - In `remote_command/3`, read the leader from `:persistent_term.get({Ankusa.WAL.Ra, :leader, members}, nil)` first.
   - After a successful `:ra.process_command`, and only when the leader it returns differs, write `{:ok, _, leader}` back.
   - This makes steady-state dedup one round trip.

Verify:

```sh
cd ankusa_ra && mix format && mix compile --warnings-as-errors && mix test
```

Also run the property suite with `MAX_RUNS=1000 mix test test/ankusa/wal_ra_property_test.exs`.

Commit: `fix(ra): restart-unique batch ids, bounded call timeouts, machine v1`.

## Phase 3: Ra tooling and docs

1. `ankusa_ra/lib/mix/tasks/ankusa.wal.migrate.ex`, `parse_members` (around lines 190-199): each comma-separated entry is a full node name, so parse it to `{cluster, String.to_atom(String.trim(entry))}`. Add a unit test for `"ankusa@a,ankusa@b"`.
2. `ankusa_ra/README.md`:
   - The Shape 1 example uses `wal.ra.members` with `ankusa@host` entries.
   - The `members add` example includes `--seed ankusa@<existing>`.

Commit (docs separately): `fix(ra): migrate parses full node names` and `docs(ra): member examples use node names`.

## Phase 4: core callers (`lib/ankusa/dispatch/pipeline.ex`, `lib/ankusa/storage/compactor.ex`, `lib/ankusa/storage/index.ex`, blob stores)

1. **Pipeline poll-loop leak** (`pipeline.ex:183-193`): the poll loop already runs during standby (`:163-165`). So line 189 becomes `{:noreply, %{state | cursor: cursor, read_seq: cursor}}` and line 192 becomes `{:noreply, standby(state)}`. Neither calls `schedule/1`.
2. **Compactor tick-loop leak** (`compactor.ex:101-105,140-145`):
   - Init standby becomes `{:ok, schedule(standby(state))}`, so the tick loop exists from init in both branches.
   - `:acquire_lease` success (`:142`) becomes `{:noreply, activate(state)}`, with no `schedule`.
3. **Standby repairs its index:** in `compactor.ex:143`, the `{:standby, state}` branch becomes `{:noreply, state |> repair() |> standby()}`. Add `defp repair(state)`, which calls `Index.repair(state.config)` inside `try/rescue` and logs a warning on failure with `Logger.warning("index repair failed: " <> Exception.message(e))`. Use `repair/1` in `activate/1` too.
4. **Pagination.** Change the callback contract in `lib/ankusa/blob_store.ex:25` to "returns **all** keys under the prefix". Implement it:
   - `s3.ex` and `oci.ex` (if it uses the S3 API, else its own `page` token): loop on `IsTruncated`/`NextContinuationToken`, passing `continuation-token`.
   - `gcs.ex`: loop on `nextPageToken` → `pageToken`.
   - `azure.ex`: loop on `<NextMarker>` → `marker`.
   - `local_fs.ex` is already complete.
   - Add an integration test (tagged `:integration`, next to the existing S3 floci tests) that puts 1 001 keys and asserts that `list` returns 1 001.
5. **Arm the lease from send time** (`pipeline.ex:706-737`, `compactor.ex:166-196`): capture `sent = mono_ms()` before `WAL.acquire_lease`/`WAL.renew_lease`, pass it as `arm(state, sent)`, and compute `lease_deadline: sent + ttl - margin` (and the same for `lease_renew_at`).
6. **Step-down answers waiters** (`pipeline.ex:749-767`): inside `step_down/1`, `Enum.each(state.waiters, fn {from, _c0} -> GenServer.reply(from, {:ok, 0}) end)` and set `waiters: []`.
7. **Emit `:lost` on every step-down:**
   - In both modules, `step_down/1` calls `LeaseHelpers.emit(:lost, state.lease)` when `state.lease != nil`, before clearing it.
   - Delete the separate `LeaseHelpers.emit(:lost, …)` in `renew/1` (`pipeline.ex:726`, `compactor.ex:186`).
8. **Seed the hwm on upgrade** (`index.ex` `repair/1`, around lines 150-165): when the hwm file is absent and the local index log exists and is non-empty, set the hwm to the max `segment_key` in the local ETS index before listing.
9. **Harden repair** (`index.ex:184-223`):
   - `sidecar_rows/2`: when the decoded row count differs from the count in the segment header (or when there is no count, from a `walk/4` of the segment), fall back to `walk/4`.
   - `walk/4`: verify each record's CRC the same way `Storage.fetch` does. On a mismatch, `raise` the error.
   - Wrap each segment fold in `try/rescue`. On an error, log it, emit `[:ankusa, :index, :repair_failed]` with `%{key: key}`, and **stop without advancing the hwm past that segment**.
10. **Core single-node tests.** In `test/ankusa/dispatch_test.exs` and `test/ankusa/storage_test.exs`, use DiskLog and hold the lease with `LeaseHelpers`/a direct `WAL.acquire_lease` under a different holder:
    - A standby pipeline's `tick` returns `{:ok, 0}`, and after 2 s with `poll_ms: 50` its message queue is not growing: `Process.info(pid, :message_queue_len)` stays `< 5`.
    - Release the other holder's lease: the pipeline acquires, and its cursor equals the stored cursor.
    - A fenced `put_cursor` (steal the lease with a new holder after the TTL) leads to `lease: nil` and a `[:ankusa, :lease, :lost]` event.
    - The compactor tick loop count stays at 1 over 3 step-down/re-acquire cycles. Count `:tick` messages in `:erlang.trace` or by the queue length, as above.
    - `Index.repair`: the sidecar path, the missing-sidecar walk, hwm skipping, and a truncated sidecar all end up in the walk.

Verify:

- Core: `mix format --check-formatted && mix compile --warnings-as-errors && mix test`, plus `mix test --include integration` with floci running.
- Every adapter package's three checks, with `ankusa_kafka` in the container.
- `ankusa_ra`: `mix test`.

Commits: `fix(dispatch): one poll loop, waiters answered on step-down, lease armed at send`, `fix(storage): standby repairs its index, repair pages and verifies`, `fix(blob_store): list returns every page`.

## Phase 5: Checker and chaos harness (`ankusa_ra/lib/ankusa/wal/checker.ex`, `ankusa_ra/chaos/**`)

1. **Checker:**
   - I6 (`checker.ex:337-362`) records a violation whenever an observed cursor goes backwards or exceeds max acked seq.
   - I7 (`checker.ex:369-398`) uses `readable` to flag any record with `seq > n` missing after a `truncate_through(n)`. It also treats a cursor with no event as 0, so a truncation past it is a violation.
   - I10 splits `extra` into `ambiguous_commit`, which is inside an I8 fault window, and `unattributed`. `unattributed` fails the check.
   - Add negative tests in `wal_checker_test.exs` for I6, I7, I9 and I10.
2. **`ankusa.chaos.verify.ex:80,102`:** `passed` is false when a fault window exists and I8 evidence is 0, or when `unattributed > 0`.
3. **Faults run from the nemesis, not inside the targets** (`chaos/scenarios/lib.sh:164-205`):
   - Make the target containers share their network namespace with a sidecar that has the tools. Concretely, run `docker run --rm --net container:<target_id> --cap-add NET_ADMIN nicolaka/netshoot iptables …` (and the same for `tc`) from the nemesis.
   - Delete every `|| true` on the inject paths. Keep `|| true` only on `heal`/`clear_latency` cleanup.
4. **Restart policy:** add `restart: "no"` explicitly, and make every kill scenario call a new `lib.sh` `revive <svc>` (`docker start <id>`) once its hold time ends.
   - `wal_leader` asks each of wal-0, wal-1 and wal-2 in turn and uses the first answer.
5. **`run.sh:156`:** replace it with `wait "$fault_pid"` and keep `fault_status=$?` under `set +e`. After the verify step, `exit 1` if `fault_status != 0`.
6. **Pre-fault warm-up:** in `run.sh`, start the load, wait for `WARMUP_S` (default 10) seconds, and only then start the fault script. Wait until `mix loadgen.run` has reported at least 100 acks.
7. **clock-skew.sh:** inject skew with `libfaketime`. Install it in the edge and wal images, set `FAKETIME=+5s` on **one** wal member with `LD_PRELOAD`, and recreate only that container. Remove the "wrong clocks, for real" claim unless this works. Verify it with `docker exec … date` in the script, and fail the script if the offset is not observed.
8. **Compaction:** set `ANKUSA_STORAGE_INTERVAL_MS` to `1000` for the `kill-storage-active` and `replace-member` scenarios only, via a per-scenario env override in `run.sh`.
   - Extend `final-scan.sh`/`Chaos.dump` to read segments too, via `Ankusa.Storage.fetch`.
   - Update chaos `README.md:94-98` to match.
9. **big-bodies.sh:** pass `--body-bytes 1048576..8388608 --big-ratio 0.2` to `loadgen.run`, adding those options to `tools/loadgen` if they are missing, and stop partitioning. Record the election count from `stats/1` samples into the report.
10. **disk-full.sh:** give each wal member a `tmpfs` data mount with `size=512m` in compose, and fill one member's tmpfs, then two, with `fallocate` inside that container.
11. **`chaos.yml`:** add `mix deps.get` in `tools/loadgen` and in `ankusa_ra/chaos/sink`. Add a step `cd ankusa_ra && MAX_RUNS=5000 mix test test/ankusa/wal_ra_property_test.exs`.
12. **Cluster property:** in `wal_ra_property_test.exs`, add a property over a 3-member cluster: random appends, leader kills and restarts, and lease acquires by 2 holders. Check it against the model. Also extend the machine property to generate 2 holders and time steps of `0..(2*ttl)`.
13. **Fix the stale comments:** `run.sh:47-53` and `chaos.yml` must no longer say SINGLE needs no distribution.

Verify:

```sh
cd ankusa_ra && mix test
cd ankusa_ra/chaos && SINGLE=1 ./run.sh power-loss   # both must exit 0
cd ankusa_ra/chaos && ./run.sh all                    # must exit 0
```

In the power-loss report, `i8 > 0`, and the pre-kill acks must be > 0.

Commit: `test(chaos): faults that land, restart what they kill, and fail on no evidence`.

## Phase 6: integration (k8s example, server, fleet configs)

1. `examples/oban-consumer/k8s/01-ankusa-wal.yaml`:
   - `WAL=ra` in the env.
   - Move `POD_NAME`/`POD_IP` above `RELEASE_NODE`. Do the same in `30-ankusa.yaml` (edge Deployment, around lines 61-66, and worker StatefulSet, around lines 171-176).
   - Add `ERL_AFLAGS: "-kernel inet_dist_listen_min 4370 inet_dist_listen_max 4372"` and expose those ports.
   - Set `publishNotReadyAddresses: true` on the headless service.
   - The readiness probe `rpc` calls `Ankusa.WAL.stats(:default)` and exits non-zero on error.
   - Delete the `ankusa-wal-external` NodePort Service.
2. `examples/oban-consumer/run.sh`:
   - `wal_leader_pod` strips the `.ankusa-wal…` suffix with `${name%%.*}`.
   - `active_worker_pod` retries up to 30 s for a live lease. For `WAL=postgres`, it reads `SELECT holder FROM ankusa_wal_leases WHERE name='dispatch' AND expires_at > now()`. Check the table name in `ankusa_postgres/lib/ankusa/wal/postgres/migration.ex`.
3. `ankusa_server` `config.ex:505-514` `members!`: raise `ArgumentError` for an empty name or host part. Add a test for it.
4. `fleet-ra-s3.yml`:
   - Remove the stale dedup claim.
   - Add `RELEASE_DISTRIBUTION=name`, `RELEASE_NODE` and the distribution ports.
   - Set `dispatch.dedup_store: ra`.
5. `reference.yml`: document `wal.type: ra`, the `wal.ra` block (members, data_dir, append_timeout_ms, read_timeout_ms, max_command_bytes) and the `wal` role.
6. `ankusa_server/README.md:209-227`: the second POST returns **202**, and the duplicate is dropped at dispatch.
7. Delete the unused `Release.wal_members/0`. Confirm first that it has no callers: run `lsp references`.

Verify:

- `ankusa_server` via the container recipe.
- `examples/oban-consumer/ingest_app` and `consumer_app`: `mix compile --warnings-as-errors`.
- `cd examples/oban-consumer && WAL=ra ./run.sh` and `WAL=postgres ./run.sh`: both must exit 0. They need kind, kubectl and docker, all of which are installed.

Commits: `fix(k8s): ra WAL members reachable over pinned distribution ports`, then a separate `docs(server): ra wal reference and 202 duplicates`.

## Phase 7: final gate

Run, and record the actual output in the final report:

1. Core: format, compile with warnings as errors, `mix test`, and `mix test --include integration` (with floci running).
2. `ankusa_postgres`, `ankusa_rabbitmq` and `ankusa_nats`: the three checks each, after `docker compose up -d --wait`.
3. `ankusa_kafka`, `ankusa_server` and `examples/kafka-sqs-consumer/ingest_app`: the container recipe.
4. `ankusa_ra`: `mix test`, with no `:dist` exclusion, and `MAX_RUNS=5000 mix test test/ankusa/wal_ra_property_test.exs`.
5. `ankusa_ra/chaos`: `SINGLE=1 ./run.sh all` and `./run.sh all`.
6. `examples/oban-consumer`: `WAL=ra ./run.sh` and `WAL=postgres ./run.sh`.
7. `examples/*/worker`: `npx tsc --noEmit`. `tools/loadgen`: `mix compile --warnings-as-errors`.
8. Every package with changed deps: run `mix deps.get`, commit its `mix.lock`, and check with `mix deps.unlock --check-unused`.

The review is "good to go" only when all 8 are green in this session. Anything that is red goes in the report with its failing output. Do not paper over it.
