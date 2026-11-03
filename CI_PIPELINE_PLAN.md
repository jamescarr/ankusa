# CI pipeline plan: faster, cheaper, more reliable

## Context

The ask: keep improving this repository's build/CI pipeline — faster, cheaper,
more reliable, better signal — without loosening a single gate. A planner loops:
measure the pipeline, research the bottleneck, add the next 2–5 items here,
hand one item at a time to an executor (own worktree, branch `ci/<ID>-<slug>`,
one improvement per PR), verify the measured delta after the merge, repeat.

Scope: `.github/workflows/*`, `.github/actions/*`, `.mise/tasks/*`, `.mise.toml`,
`.mise/conf.d/*`, `packages/ankusa_server/Dockerfile` and image build/caching,
the bench harness under `packages/ankusa/bench/`, dep/PLT/`_build` caching, the
job graph, matrix shape, runner choice, flake handling, artifact/step-summary
reporting, supply-chain pinning (`mise run lint:pins` must keep passing: every
action a commit SHA, every release-image input a digest), `hex.audit`, release
workflows, and the `docs/testing.md` / `AGENTS.md` rows for anything changed.
Product code (`packages/*/lib`), SDK source and the examples' application code
are out of scope, and every change must keep `mise run check` as the single
local entry point.

**What may not change:** what a gate asserts. Coverage floors, dialyzer,
`credo --strict`, sobelow, both crash drills, the perf gate's 1.2×/0.8× limits,
conformance and the pin lint keep their current strictness. A change that makes
a job faster by checking less is wrong, however small. A gate that fails on a
diff that cannot have caused the failure is a *bug* — fix the measurement, not
the assertion.

**What "cheaper" means here:** `jamescarr/ankusa` is a **public** repository
(`gh repo view --json isPrivate` → `false`), so standard-runner minutes are
free and runner-minutes are a *proxy* — for scheduling contention and for how
much surface a flaky job has. The costs that are real are **wall-clock minutes
per PR** (an agent waits on them), **red runs** (each costs a rerun and a wrong
attribution), and **billed** compute only for larger runners. So the loop
optimises wall clock and signal on the critical path, and treats
runner-minutes as secondary.

## Measurement

The numbers below come from the last 30 `ci.yml` runs (2026-10-26 … 2026-10-31),
job-level, via the API, plus one run on 2026-11-02. Re-measure with:

```sh
gh run list --workflow ci.yml --limit 30 --json databaseId,conclusion,headBranch,event
gh run view <id> --json jobs --jq '.jobs[] | [.databaseId,.name,.conclusion,.startedAt,.completedAt] | @tsv'
# phase timings inside one job (the task prints `==>` markers):
gh api --allow-escape-sequences repos/jamescarr/ankusa/actions/jobs/<job-id>/logs \
  | grep -E '^[0-9T:.Z-]+ (==>|Running ExUnit|Finished in|Cache (hit|restored))'
gh api repos/jamescarr/ankusa/actions/caches --paginate \
  --jq '.actions_caches[] | [.key, (.size_in_bytes/1048576|floor), .created_at] | @tsv'
```

`gh run view --log-failed` does not work while a run is in progress; per-job
`/logs` does. The per-job `steps` API returns no timestamps on this
installation, so phase splits come from the log timestamps.

### Baseline (median / p90 minutes per job, 30 runs)

| job | med | p90 | | job | med | p90 |
| --- | --- | --- | --- | --- | --- | --- |
| `ankusa` | **30.0** | **33.1** | | `crash drill (ENOSPC)` | 1.2 | 1.4 |
| `perf gate` | 9.4 | 12.8 | | `sdk-python` | 1.2 | 1.3 |
| `ankusa_kafka` | 3.3 | 5.2 | | `ankusa_server` | 1.1 | 1.8 |
| `sdk conformance` | 2.4 | 2.6 | | `sdk-clojure` | 1.1 | 1.3 |
| `ankusa_nats` | 1.7 | 2.0 | | `async_api_spex` | 1.1 | 1.3 |
| `ankusa_redis` | 1.5 | 2.2 | | `crash drill (power-loss)` | 1.0 | 1.0 |
| `examples` | 1.5 | 2.5 | | `sdk-elixir` | 1.0 | 1.1 |
| `ankusa_rabbitmq` | 1.4 | 2.1 | | `sdk-rust`, `sdk-go` | 0.9 | 1.0 |
| `core integration (floci)` | 1.3 | 1.5 | | `sdk-ruby` | 0.8 | 1.3 |
| `sdk-java` | 1.3 | 1.4 | | `sdk-php` | 0.8 | 0.9 |
| `docker.yml` (whole workflow) | 2.5 | 9 | | `tools` | 0.7 | 1.2 |
| | | | | `sdk-typescript` | 0.6 | 0.6 |
| | | | | `lint` | 0.5 | 0.6 |
| | | | | `discover packages` | 0.1 | 0.1 |

- **Wall clock per PR: 30.0 min median, 33.1 min p90** — that is the `ankusa`
  job plus ~1 min of queueing; every other job is done before it.
- **Runner-minutes per pipeline: 53** (perf gate skipped, 14 of 30 runs) to
  **63** (gate run). The `ankusa` job alone is 30.0 (48–57 %).

*State after CI-1…CI-4* (measured on `main`, run 41549538574):
**47.9 runner-minutes, 10.6 min wall clock** — `perf gate` 10.6 min, `ankusa`
10.1 min, `ankusa_kafka` 3.1 min, every other job under 1.6 min. Against the
baseline above that is −65 % wall clock (30.0 → 10.6) and −24 % runner-minutes
(63 → 47.9 with the gate, 53 → 40 without). The two long jobs then held 20.7 of
the 47.9, and 24 jobs still started on every diff however small — which is why
CI-5 and CI-7 followed.

### Current state (30 runs, 2026-11-03 03:57 … 06:29Z — after CI-5…CI-8, CI-10…CI-12)

| job | med | p90 | max | | job | med | p90 | max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **`ankusa (tests)`** | **9.8** | **10.1** | **10.4** | | `sdk-java` | 1.3 | 1.4 | 1.5 |
| `ankusa_kafka` | 3.4 | 5.2 | 5.7 | | `ankusa (static)` | 1.3 | 1.4 | 1.5 |
| `bench-tenant` (5 runs) | 3.1 | 4.0 | 4.1 | | `crash drill (ENOSPC)` | 1.3 | 1.4 | 1.4 |
| `sdk conformance` | 2.5 | 2.7 | 2.9 | | `sdk-python` | 1.2 | 1.2 | 1.5 |
| `examples` | 1.7 | 1.8 | 1.9 | | `ankusa_server` | 1.1 | 1.2 | 1.4 |
| `ankusa_nats` | 1.6 | 1.7 | 1.9 | | `sdk-clojure` | 1.1 | 1.2 | 1.6 |
| `core integration` | 1.5 | 1.7 | 1.8 | | `crash drill (power-loss)` | 1.0 | 1.1 | 1.1 |
| `ankusa_redis` | 1.4 | 1.6 | 1.7 | | `async_api_spex` | 1.0 | 1.2 | 1.3 |
| `ankusa_rabbitmq` | 1.4 | 1.5 | 1.7 | | `sdk-go`/`sdk-elixir` | 0.9 | 1.0 | 1.3 |
| `perf gate` (4 measured) | 10.0* | 16.1 | 16.1 | | `sdk-rust`/`sdk-ruby`/`sdk-php` | 0.8 | 1.0 | 1.3 |
| `lint` | 0.5 | 0.6 | 1.1 | | `tools` | 0.7 | 0.8 | 1.6 |
| `changes`/`bench-scope`/`ci-summary` | 0.1 | 0.1 | 0.1 | | `sdk-typescript` | 0.6 | 0.6 | 0.9 |

\* the gate **measured** in only 4 of the 22 non-cancelled runs it appears in
(9.0, 10.0, 10.1, 16.1 min); the other 18 are the scope decision
(`no hot-path file touched: perf gate and bench-tenant skipped`) finishing in
0.1 min. Its numbers are therefore thin, and its failure rate is 1 of 4, not
1 of 22 — see CI-14.

- **Wall clock: 11.5 min median, 14.9 p90, 20.5 max** (10.5 median over the 4
  runs where the gate did not measure). Against the 30.0 min baseline that is
  −62 %.
- **Runner-minutes: 38.9 median** (6.3 min for a PR that only ran an SDK job,
  55.6 for a full run whose gate failed), down from 47.9/63.
- **The critical path is one job.** The last job to finish is `ankusa (tests)`
  in 10 of 26 non-cancelled runs (the fence job `ci-summary`, 0.1 min, in the
  rest); on a typical PR the wall clock is `ankusa (tests)` 9.7 min + 0.2 min
  of lead (p90 lead 1.5 min — GitHub queueing, up to 9.3 min in a burst of
  eight simultaneous runs: ~25 jobs per run against the account's runner
  concurrency). Everything else is done by minute 6. **The suite is the
  pipeline** — CI-9, CI-17.
- **Failure rate: 6 of 30 runs red (20 %), 4 cancelled by `concurrency:`**
  (CI-4 doing its job), one distinct cause each:
  - **1 — `perf gate` failed its own base leg**: `INCONCLUSIVE p99_ms
    512B-c32 … span 1.28×` then `[bench:gate] ERROR task failed` on `main`
    commit `77e22b6f` (run 41586954095, CI-11's merge, nothing had changed
    since its green PR run). The rebaseline refuses a shaky sample by design
    and halts, which kills the branch leg too. CI-14.
  - **1 — `ankusa (tests)` 2079/2080**: the tenant-fairness p99. Fixed by
    CI-8 (#248).
  - **1 — `ankusa_rabbitmq` 38/39**: `assert connection(inst, exch) == nil` in
    the idle-reaper test. Open — CI-15.
  - **1 — `ankusa_kafka` 40/42**: the broker-leader `MatchError`s. Fixed by
    CI-12 (#246).
  - **1 — `core integration` 40/41**: `Ankusa.Sink.SQSIntegrationTest` "sqs
    batch order" on a FIFO group — `left: ["m02","m01","m03", …]`, i.e. the
    first two messages of one group swapped. Backlog (needs attribution).
  - **1 — `ankusa (static)`**: `mix docs --warnings-as-errors` —
    `documentation references module "Ankusa.Application" but it is hidden`.
    That run is the `docs/elixir` PR's own doc edit, not a pipeline defect;
    removed from the backlog.

### The baseline window's failures (30 runs, 2026-10-26 … 10-31)

- **Failure rate: 12 of 30 runs red (40 %)**, 21 failed jobs, by cause:
  - **11 — Docker Hub pull rate limit**
    (`toomanyrequests: You have reached your unauthenticated pull rate limit`)
    in `ankusa_redis`, `ankusa_rabbitmq`, `ankusa_nats`, `core integration`, and
    both crash drills. Two clustered runs (2026-10-28 04:41 and 04:55) account
    for 8 of them. CI-3.
  - **4 — `mix hex.audit` advisories** published 2026-10-31
    (`ssl_verify_fun` in `ankusa_server`, `phoenix_html` in `examples`): fixed
    by PR #214, which merged 2026-11-02; from now on these two jobs must be
    green.
  - **4 — one failing test**: `ankusa` 1958/1959, `ankusa_redis` 82/83,
    `ankusa_kafka` 39/40 twice (`Ankusa.Intake.KafkaTest` "earliest,
    assign_partition and ready?", `:econnrefused` to the broker — a readiness
    race in the compose fixture). Backlog.
  - **2 — `mix docs --warnings-as-errors`** in `ankusa_redis` (a warning in the
    generated docs). Backlog.
  - **1 — `perf gate` false failure** on a workflow-only diff (2026-11-02,
    run 41506556215): CI-2.

### The `ankusa` job, in phases (three runs, same shape)

| phase | run A | run B | run C |
| --- | --- | --- | --- |
| dev-env deps compile | 6:20 | 7:27 | 8:44 |
| test-env deps compile | 5:47 | 7:09 | 8:28 |
| compile + credo + PLT + dialyzer + sobelow | 2:30 | 3:00 | 3:15 |
| `mix test --cover` | 6:24 | 8:01 | 8:14 |
| docs + `hex.build` | 0:15 | 0:15 | 0:20 |

**48–56 % of the job is dependency compilation that a warm cache removes**, and
the cache is never warm — CI-1.

### The perf gate's measurements are not comparable between legs (CI-2)

Run 41506556215, PR 213, whose diff was `.github/workflows/ci.yml` only (two
cache keys and their comments — the compiled program is byte-identical to base):

| cell | base leg p99 samples (ms) | branch leg p99 samples (ms) | verdict |
| --- | --- | --- | --- |
| `512B` | 195.5, 155.6, … (median 178.0) | 22.2, 202.1, 149.6 (median 149.6) | ok (−16 %) |
| `10240B` | … (median 126.0) | 104.3, 223.6, 91.7 (median 104.3) | ok (−17 %) |
| `512B-c32` | … (median 17.1) | 13.3, 6.2, 14.0 (median 13.3) | ok (−22 %) |
| `512B-ordered` | 26.3, 25.9, 26.4 (median 26.3) | 101.0, 186.7, 56.0 + confirm 44.1, 89.9, 103.7 | **FAIL** (+68 %) |

Two facts: the branch leg's samples span 22–202 ms *within one leg*, and in the
base leg `512B-ordered` (26.3 ms) is 7× faster than `512B` (178.0 ms) while
docs/testing.md records them within 3 % of each other on the recording machine.
So the legs are measuring different states of the runner/bench store, and the
median of three such samples cannot support a 1.2× decision.

### Current state (30 runs, 2026-11-03 11:00 … 16:20Z — after CI-14…CI-19)

- **Wall clock 10.5 min median, 14.2 p90** over 26 non-cancelled runs (the
  window straddles CI-17's merge), against 11.5 min in the 03:57 window and
  30.0 min in the baseline.
- **1 genuine red of 26 (3.8 %)**: `main` 41625156213, `ankusa (tests)`, the
  `Ankusa.DrainTest` `/ready` flip — fixed by CI-19. Five other red runs are
  this loop's own proofs (CI-9's two, CI-18's two, CI-15's deadline probe), each
  exactly one failing job. Against 6 of 30 (20 %) an hour earlier and 12 of 30
  (40 %) in the baseline.
- `ankusa (tests)` 9.3 min median (n=25) through most of the window; **CI-17
  landed at the end** and the last runs show four shards — 4.5 / 2.1 / 3.8 /
  2.1 min plus `ankusa (coverage)` 41 s, **292 s from the first shard's start to
  the merge's end** on run 41679004989, merged coverage 89.22 %.
- **PR wall clock after CI-17: 5.2–6.4 min** (5.2, 5.3, 5.4, 6.2, 6.3, 6.4, 7.0
  on the last seven runs) on a 33-job pipeline. The longest job is a shard
  (4.2–4.5 min) or `ankusa_kafka` (5.3–6.6 min).
- **`perf gate` is now the outlier: 12.0 / 12.2 / 13.8 min** on the three runs
  where it measured. It runs only on a hot-path diff, so it no longer touches
  the median — but it *is* the wall clock of any hot-path PR (CR-2 and CR-3 of
  the feature plan), at 2.3× the rest of the pipeline. Job 125809478781's
  12m9s: setup 46 s, base leg 5m35s (**4 discarded warm-ups = 84 s**, 12
  measured runs ≈ 4m10s), branch leg 5m40s — 28 runs at ~21 s each. CI-20.
- Jobs per run rose from ~29 to 33 with the shards and the merge job: the queue
  note in the Backlog matters more, not less.

### Current state (8 runs, 2026-11-03 17:45 … 19:25Z — after CI-17…CI-20)

- **`main` is red, twice, on two different causes**: run 41689506568
  (`902e3dcb`) failed `ankusa (tests, 4/4)` (CI-21's tenant-fairness p99) and
  `lint` (a `mise install` 500, CI-22); run 41702699820 (`e53e3d32`, CI-20's
  merge — a `bench/gate.exs` + docs diff) failed `ankusa_kafka` 40/42
  (CI-26: two `publish_batch` tests, `{:error, :unavailable}` after a
  `{:reached_max_retries, :shutdown}` producer crash) and its `perf gate` was
  still running at 19:25Z. The next `main` run (41712366064, `7ba2866d` —
  CI-21's merge) is **green**: the fairness shard and the coverage merge pass,
  and `ankusa_kafka` passed, so CI-26 did not recur. AGENTS.md blocks a tag on a
  red `main` until diagnosed, which is what these three items are.
- **Wall clock after CI-17: 5.35 min** on a green `main` run (41688133998) and
  6.15 min on the red one; the two `main` runs are the only non-gate runs in the
  window. The CI-20 probe PRs read 11.9 / 16.7 / 12.7 / 21.4 / 12.0 min because
  they touch `packages/ankusa/bench/`, so `perf gate` measured on all five.
- **Per-shard, run 41696475603** (`ankusa (tests, k/4)`): test time 199.3 /
  43.3 / 237.1 / 69.1 s, job time 269 / 92 / **314** / 142 s — ~73 s of per-job
  overhead and a 5.5× spread in test time. On the last non-gate `main` run
  (41688133998, wall clock 5.35 min) the four shard jobs are 4.38 / 1.72 / 3.85
  / 2.22 min and **the slowest job in the run is a shard at 82 % of the wall
  clock** while another idles for 2.7 min. The assignment is not cost-aware;
  CI-23.
- **`mise by @jdx – installing 16 tools`** appears *after* the mise-action step
  in every package job — the per-kind `install_args` is undone by mise's
  task-run auto-install: `ankusa_server` 11.3 s (job 126143230677), `ankusa_kafka`
  14.5 s (126143230821), `examples` 19 tools (126143230846). That is the
  download surface that reddened `lint` in run 41689506568; CI-22.
- **`ankusa_kafka` 237 s (job 126143230821)**: 44 s before compose (checkout +
  mise, 12 s of it the redundant toolchain), 21 s `docker compose up`, 35 s
  deps/compile, **120 s suite**, 15 s teardown. On the next green `main` run the
  same job is **6.17 min** (126183566813, suite 239.6 s) and outlasts every
  shard, so the suite's own 2× variance — not the setup — is the tail: CI-26.
- **The critical path after CI-21 is `max(slowest shard, ankusa_kafka)`**: on
  run 41712366064 (wall clock 6.67 min) shard 3/4 is 5.28, shard 1/4 4.32 and
  `ankusa_kafka` **6.17**; on 41710035955 it is 4.82 / 4.68 / 3.83. So CI-23
  (balance the shards, ~2 min) and CI-26 (the kafka suite's tail) are the two
  remaining wall-clock levers for a normal PR; `ankusa (coverage)` grew from
  40 s to ~58–60 s with the extra export it now merges.
- **`perf gate` 9.5–11.7 min** on the five runs where it measured (CI-20's own
  merge run 41696475603: 9m28s) — still 2× the rest of the pipeline on any
  hot-path PR. Its two legs are independent measurements run in series on one
  runner; CI-24.

## Task list

Status: 20/26 done · closed without meeting the bar: CI-9 · next: CI-26 · last updated: 2026-11-03

`main` is **green** at `0e11bebc` (CI-22's merge, run 41718109336): the whole
graph, `perf gate` and `bench-tenant` included, and the jobs' mise lines are the
short ones (27 mise-action installs for 27 steps, none inside `mise run`). The
two earlier reds: run 41689506568 (`902e3dcb`) failed `ankusa (tests, 4/4)`
(CI-21, fixed by #272) and `lint` (CI-22, fixed by #274 — the 17-tool `mise run`
auto-install behind it is gone); run 41702699820 (`e53e3d32`) failed
`ankusa_kafka` 40/42 (CI-26 — two `publish_batch` tests after a brod
`{:reached_max_retries, :shutdown}`), which did **not** recur on the next
`main` run, so CI-26 is a flake to attribute, not a standing red.

Format: `- [ ] **ID — Title** · priority · Deps`, then *Why* (with the measured
numbers), *Change*, *Done when*, *Proof*, *Anchors*, *Evidence* (filled by the
executor in the same PR that ticks the box). **P1** is executed first; within a
priority, ID order — unless the Status line's `next:` names a different one,
which it does when a measured red run outranks a slow pipeline.

### Phase A — Stop paying twice

- [x] **CI-1 — One writer per cache key: stop the `ankusa` job recompiling its deps on every run** · P1 · Deps: —
  *Why:* the `ankusa` job compiles its Hex deps twice — once in `dev`, once in
  `test` — on **every** run, 12:07 / 14:36 / 17:12 min in the three runs
  sampled above, ~50 % of a 30.0 min median job and ~95 % of a PR's wall clock.
  It is not a cache miss: `actions/cache` reports `Cache hit for:
  mix-v17-Linux-X64-ankusa-<mise.toml>-<mix.lock>` and restores 40 MB, then the
  log shows `* Getting bandit (Hex package)` for every dep and
  `==> third-party deps compiled` in both envs. The cause is that **two jobs
  save one key**: the package job's cache step and the `sdk conformance` job's
  cache step use the same `mix-v17-…-ankusa-…` key ("whichever job saves first
  wins"). Conformance finishes in 2.4 min and saves a partial tree;
  actions/cache never re-saves a key that exists, so the package job's full
  `dev`+`test`+`prod` tree is never stored and `dev` in particular is compiled
  from source (RocksDB's C++ NIF, ~8 min on a runner) every time. The mechanism
  works when a key has one writer: `ankusa_redis` (single-writer
  `mix-v17-…-ankusa_redis-…`) restores and logs
  `==> third-party deps up to date` in both envs, 25 s each.
  *Change:*
  - `.github/workflows/ci.yml`, package job: bump the key version
    `mix-v17-` → `mix-v18-` (and the `restore-keys` prefix) so the stale
    conformance-written entries are invisible and the package job creates fresh
    ones. Replace the "whichever job saves first wins" comment with the
    invariant: **exactly one job saves a given cache key; every other job
    restores it by prefix.**
  - `.github/workflows/ci.yml`, `conformance` job: its own namespace — key
    `mix-v18-${{ runner.os }}-${{ runner.arch }}-ankusa-conformance-${{ hashFiles('.mise.toml') }}-${{ hashFiles('packages/ankusa/mix.lock') }}`,
    `restore-keys` first its own prefix, then
    `mix-v18-${{ runner.os }}-${{ runner.arch }}-ankusa-${{ hashFiles('.mise.toml') }}-`
    so it still restores the package job's (now fuller) tree.
  - Keep every other Mix key as it is (`mix-v5-…-examples-…`,
    `-ankusa-integration-`, `-ankusa-bench-`, and `mix-v17-…-<pkg>-` for the
    other packages): each already has one writer.
  - Measure the cache entry size after the first run that saves it and record it
    in the PR body; if it exceeds ~2 GB, drop `packages/ankusa/deps` from the
    cache `path` list and keep `_build` only (deps are re-fetched in ~20 s;
    `_build` is what Mix trusts), and say so.
  - If a later run is ever suspected of having saved a poisoned (partial) entry
    under the new key, bump the version again (`mix-v18-` → `mix-v19-`); the
    comment in the step documents that this is the escape hatch, because
    actions/cache never re-saves an existing key.
  *Done when:* a CI run's `ankusa` job log shows `==> third-party deps up to
  date` for the **dev** env and for the **test** env (not
  `third-party deps compiled`), and the job's duration drops to ≤ 16 min with
  `mix test --cover` still green; the `sdk conformance` job still restores a
  warm tree (`up to date`) and conformance stays green; `mise run lint:workflows`
  and `mise run lint:pins` pass.
  *Proof:* on the PR, `timeout 1800 gh pr checks <N> --watch --fail-fast`, then
  `gh run view <run> --json jobs` (job durations, before/after in the PR body)
  and `gh api --allow-escape-sequences repos/jamescarr/ankusa/actions/jobs/<job>/logs
  | grep -E 'third-party deps|Cache restored from key: mix'`. The first run of a
  new key is cold by construction: rerun the same commit
  (`gh run rerun <id>`) and cite the *second* run as the measurement.
  *Anchors:* `.github/workflows/ci.yml` (package job `cache mix deps` step and
  its long PLT/key comment; `conformance` job `cache mix deps` step),
  `.mise/lib/mix_cache.sh` (`mix_cache_warm`: the restore/`-p` timestamp and
  "existing entries are never overwritten" rules this relies on),
  `.mise/tasks/check/package` (the `for env in dev test` loop),
  `.mise/tasks/check/conformance` (`mix_cache_warm … test`).
  *Evidence:* PR #213, merged as `ae5bfe31`. In the PR: `ankusa` 33.5 min cold
  (run 41506556215 — `third-party deps compiled` in both envs, saved
  `mix-v18-Linux-X64-ankusa-…` = **59 MB**, so `path` kept `deps` + `_build`) →
  **8.3 min** warm (run 41512168322: `Cache restored from key: mix-v18-…`,
  `==> third-party deps up to date` at 19:43:57 for dev and 19:43:59 for test);
  `sdk conformance` 12.1 → 2.5 min. On `main` after the merge: the first run
  (41523194955) was cold at 33.6 min and saved the entry; the next
  (41531241640) ran the same job in **10.4 min** and the **whole pipeline in
  12.8 min** (previous main runs: 31.9 and 34.5 min for the job, 32–46 min wall
  clock). One side effect worth knowing: the key spans the matrix, so every
  package paid one cold compile and saved its own v18 entry in the first run,
  and afterwards all were warm.

### Phase B — Make red mean something

- [x] **CI-2 — Perf gate: make the base and branch legs comparable, so a diff that cannot change the runtime cannot fail the gate** · P1 · Deps: —
  *Why:* on 2026-11-02 the gate failed PR 213, whose only change was two cache
  keys and a comment in `.github/workflows/ci.yml` (compiled program identical
  to base): `FAIL p99_ms 512B-ordered: 44.10 > 1.2×26.30 (limit 31.57)` — the
  *best of six* runs, i.e. after the existing confirm round. The table above
  shows why: the branch leg's own `p99_ms` samples span 22.2–202.1 ms (`512B`)
  and 44.1–186.7 ms (`512B-ordered`), while the base leg's `512B-ordered` was
  26.3 / 25.9 / 26.4 — and in that same base leg `512B-ordered` came out 7×
  *faster* than `512B`, contradicting `docs/testing.md` ("within 3 % … on
  ingest_per_s and no slower on p99_ms"), so the legs were not measuring the
  same machine state. Two medians of a quantity with 10× spread, taken minutes
  apart, cannot support a 1.2× decision. This is not a rare corner: the gate
  runs on every PR that touches `packages/ankusa/lib/ankusa/(edge|queue|store|
  dispatch|verifier|routes)/**` — the feature plan's CR-1…CR-3 all do — and on
  every `ci.yml` change, which is every change this loop makes.
  *Change:* diagnose first, then the minimal fix; **the 1.2×/0.8× limits, the
  1.2×/0.8× gate list and `BENCH_CONFIRM` stay as they are.**
  1. Reproduce and attribute, recording every number in the PR body: run the
     gate on one unchanged commit on `main` three times; for each leg log the
     bench's data directory (where the store lives; `TMPDIR`), its size before
     and after each cell, and the per-run `p99_ms`. The candidates, in order of
     plausibility from the samples: (a) the store accumulates across cells and
     across legs and the branch leg always runs second on the fuller store;
     (b) `/tmp` is disk-backed on the runner, so fsync latency lands in the
     p99 tail and drifts with the store's size; (c) the `-ordered` cell's 64
     lanes amplify scheduling noise, so its p99 is the widest.
  2. Fix the measurement, not the gate. Preferred, in order: a fresh bench data
     directory per cell per leg (neither leg inherits the other's store); the
     same, with `TMPDIR` on `/dev/shm` for both legs on CI (a tmpfs makes the
     fsync cost the "costs nothing" regime `docs/testing.md` describes for CI);
     if neither removes the drift, alternate the legs by round so the order bias
     cancels, and say in the PR why the simpler fixes were not enough.
  3. Add the missing guard: if a leg's own `p99_ms` spread (max/min) exceeds the
     limit's headroom, the run is **inconclusive** — the harness re-measures once
     and, if still inconclusive, fails with a distinct message
     (`INCONCLUSIVE p99_ms <cell>: samples span N×`) carrying the samples. An
     inconclusive is never a pass and never a "regression".
  4. Drop `^\.github/workflows/ci\.yml$` from the `bench` job's `HOT_PATHS`: a
     workflow file cannot change the measured program, and a bad invocation is
     measured by the next `main` push within minutes. Keep
     `^packages/ankusa/bench/` and `^\.mise/tasks/bench/` (those *do* change what
     is measured) — and this item changes `bench/gate.exs`, so the gate must run
     on the PR that lands it.
  5. `docs/testing.md` "Perf gate" section: the fresh-data-directory rule, the
     inconclusive rule, and what `HOT_PATHS` now covers. `AGENTS.md`'s hot-path
     row is unchanged (it names source files, not ci.yml).
  *Done when:* three consecutive gate runs on one unchanged hot-path commit are
  green with per-cell deltas inside ±10 %; a `ci.yml`-only PR's `perf gate` job
  is a no-op (`run=false`); **and a deliberately regressed tree still fails** —
  done in a throwaway PR (below), which is the only proof that the limits still
  bite.
  *Proof:* the three-run table; then a throwaway branch `ci/perfgate-proof` that
  adds `Process.sleep(1)` at the top of `Ankusa.Edge.Ingest.ingest/2` — the gate
  must print `FAIL` for the cells `docs/testing.md` says catch a fixed delay
  (`512B-c32`) — record the failure table and **close that PR unmerged** (it
  exists only as this item's proof, and the closed branch is deleted); `mise run
  lint:workflows`; `mise run bench:gate` locally (its own numbers, not a
  verdict).
  *Anchors:* `.github/workflows/ci.yml` (`bench` job: `HOT_PATHS`, the scope
  step, "measure the base commit on this runner", `BENCH_REBASELINE`),
  `packages/ankusa/bench/gate.exs:1-120,135-160,205-250` (`medians/3`,
  `BENCH_CONFIRM`, `evaluate/3`, `report/4`), `.mise/tasks/bench/gate`,
  `packages/ankusa/bench/core_bench.exs` (where the store lives),
  `docs/testing.md:1310-1400`.
  *Evidence:* PR #217, squash `13c63e75`. Cause attributed: not an accumulated
  store (`core_bench.exs` already made a fresh one per cell — traced 0 B before
  every cell) but the runner's **disk fsync in the p99 tail**: an ingest commit
  ends in `Store.write(…, sync: true)` and a group commit carries up to 1024
  hooks, so `p50_ms` stayed within a few percent while `p99_ms` spanned 4–10×
  inside one leg. Fix: every run gets its own `TMPDIR` under `/dev/shm`
  (7.8 GiB free on the runner; `BENCH_DATA_ROOT` overrides), path and size
  logged per cell; an `INCONCLUSIVE p99_ms <cell>` verdict (with every sample)
  when the round medians still span >1.2× — never a pass, never a regression.
  `@gates`, the matrix and `BENCH_CONFIRM` untouched (`git diff` verified).
  Three green runs on `37bef1fb` (jobs 125595490540, 125611013439,
  125616200581) with per-cell gated deltas inside ±7.8 %. The gate still bites:
  throwaway PR #218 (`Process.sleep(1)` in `Ingest.ingest/2`) **FAILED**
  (`FAIL ingest_per_s 512B-c32: 10744.35 < 0.8×16711.76`), closed unmerged;
  throwaway PR #221 (a `ci.yml`-only diff) reported
  `no hot-path file touched: gate skipped` (`run=false`), closed unmerged.

- [x] **CI-3 — Keep the CI images in the Actions cache instead of pulling them from Docker Hub on every job, and pin every compose image by digest** · P1 · Deps: —
  *Why:* **11 of 21 failed jobs in 30 runs** are
  `toomanyrequests: You have reached your unauthenticated pull rate limit` —
  `ankusa_redis` (2), `ankusa_rabbitmq` (2), `ankusa_nats` (2),
  `core integration (floci)` (2), `crash drill (ENOSPC)` (1),
  `crash drill (power-loss)` (2). Authenticating does not save it: in
  `ankusa_redis`'s failed job the log shows `Login Succeeded!` at 04:57:01 and
  `toomanyrequests` at 04:57:14, i.e. the shared account's
  200-pulls-per-6-hours limit was exhausted by five jobs pulling per run across
  a day of agent traffic (the registry returns the "unauthenticated" wording
  either way), and a fork PR gets no `DOCKERHUB_TOKEN` at all. Docker Hub pulls
  per run: `floci/floci{,-gcp,-az,-oci}:latest` (4, **unpinned** — a moved
  `latest` silently changes what the integration suite runs against),
  `amazon/aws-cli:2.38.1`, `curlimages/curl:8.23.0` (×3), `apache/kafka:3.9.1`,
  `nats:2-alpine`, `rabbitmq:4-management-alpine`, `redis:8-alpine`, and
  `elixir:1.20.4-alpine@sha256:…` (the crash drills take it from
  `packages/ankusa_server/Dockerfile`'s `FROM`).
  A GHCR mirror was considered first and rejected on evidence: packages pushed
  by `GITHUB_TOKEN` are private by default on a user-owned namespace, and the
  call that makes them public needs a `packages` scope this repo's `gh` token
  does not have (`gh auth status`: `repo, workflow, gist, project, read:org,
  admin:public_key, write:gpg_key`), so every local `mise run check:package`
  would need `docker login ghcr.io`. A cache of the image tarballs needs no
  registry, no auth and no visibility, works for fork PRs, and removes the pull
  from every run after the first — the same immunity, one moving part fewer.
  *Change:*
  1. Pin every `image:` in `packages/ankusa/docker-compose.integration.yml` and
     `packages/{ankusa_redis,ankusa_rabbitmq,ankusa_nats,ankusa_kafka}/docker-compose.yml`
     by digest (`redis:8-alpine@sha256:…`), and add that rule to
     `.mise/tasks/lint/pins` (the wording already used for the Dockerfile).
     Resolve a digest without pulling: `docker buildx imagetools inspect <ref>
     --format '{{json .Manifest.Digest}}'`, or for Hub images the API
     `https://hub.docker.com/v2/repositories/<repo>/tags/<tag>` (`.digest`);
     record each resolved digest in the PR body beside the ref it replaces.
  2. New `scripts/ci/images.sh`, dependency-free (bash + docker, no new action):
     `images.sh load <dir> <ref>…` does `docker load -i` on every `*.tar` in
     `<dir>` (no-op when the directory is empty or missing) and reports which
     refs are now present; `images.sh save <dir> <ref>…` does `docker save -o
     <dir>/<slug>.tar <ref>` for the refs that are not already saved.
  3. `.github/workflows/ci.yml`: an `actions/cache` step in each image-pulling
     job (`package` for a `hex`/`docker` kind with a compose file,
     `integration`, both `crash` jobs) with `path: ${{ runner.temp }}/docker-images`,
     key `images-v1-${{ runner.os }}-${{ runner.arch }}-<package|job>-<hash of
     that package's compose file(s), or of the Dockerfile for the drills>` and a
     prefix `restore-keys`; then `scripts/ci/images.sh load` **before**
     `mise run check:package` / `test:integration` / `test:crash`, and an
     `if: always()` `scripts/ci/images.sh save` step so a cold run warms the
     entry. `docker compose up` then finds the images present and pulls nothing
     (`pull_policy` defaults to `missing`), and the drill's `docker run` likewise.
  4. Keep the `Log in to Docker Hub` steps: a cold run still pulls, and an
     authenticated pull is worth having. The cache makes cold pulls rare — one
     per compose/Dockerfile change instead of five per run.
  5. If the added cache entries total more than ~2 GB, keep only the `integration`
     and `ankusa_kafka` sets (the two biggest and the two that failed) and say so
     in the PR body with the measured sizes.
  6. `docs/testing.md` (local infra, long drills) and the `AGENTS.md` Commands
     table: images are cached between CI runs; nothing changes for a local
     `mise run check:package` / `test:integration`.
  *Done when:* on a warm run no image-pulling job's log contains a Docker Hub
  pull for its refs (grep the logs), and each such job is at least 10 s faster;
  a forced cold run (delete one package's `images-v1-…` entry, then
  `gh run rerun`) still pulls, saves and passes; `mise run check:package
  ankusa_redis`, `mise run check:package ankusa_kafka` (pinned kafka digest) and
  `mise run test:integration` pass; `mise run lint:pins` and
  `mise run lint:workflows` pass.
  *Proof:* the PR's warm-run job logs with the pull lines absent and the job
  durations before/after in the PR body;
  `gh api repos/jamescarr/ankusa/actions/caches --paginate --jq '.actions_caches[]|select(.key|startswith("images-v1"))|[.key,(.size_in_bytes/1048576|floor)]'`;
  `mise run lint:pins`; `mise run test:integration`; `timeout 1800 gh pr checks
  <N> --watch --fail-fast`.
  *Anchors:* `.github/workflows/ci.yml` (the `package` job's toolchain/compose
  steps, `integration`, both `crash` jobs), `.mise/tasks/check/package`
  (`docker compose up -d --wait`), `.mise/tasks/test/integration`,
  `scripts/drills/crash.sh:21-30`, `packages/*/docker-compose*.yml`,
  `.mise/tasks/lint/pins`, `.mise/lib/pkg.sh` (`pkg_kind`),
  `scripts/ci/images.sh`.
  *Evidence:* PR #222, merged as `cda07e3e`. Warm run 41537885469: **7 jobs with
  no pull** (kafka, rabbitmq, redis, nats, integration and both drills); the
  per-job pull window is gone (~39 s per run of pure pull time) but the point is
  the failure class: `toomanyrequests` was 11 of 21 failed jobs and is now
  unreachable on a warm cache. Cold proof: run 41533665431 attempt 2 after
  deleting the `ankusa_redis` entry (`loaded 0, pulled 2, saved 2`, re-saved,
  green) and run 41544232328 on `main` with every image cold (all pulled, saved,
  green, 13.2 min wall clock). Cache: 924 MB per scope (integration 385, kafka
  316, rabbitmq 97, crash 64, redis 44, nats 18) — under the 2 GB cut. Two
  findings worth remembering: on Docker 28's classic overlay2 store a
  `save`/`load` round trip drops the digest, so the jobs switch on the containerd
  snapshotter (`scripts/ci/images.sh containerd`) or a pinned ref re-fetches its
  manifest; and a cold run pulls from up to seven jobs at once, so `sync` retries
  a `toomanyrequests` pull twice (15 s, 30 s). `lint:pins` now fails an unpinned
  compose `image:` (verified by hand).

### Phase C — Spend less per push

- [x] **CI-4 — `concurrency:` on `ci.yml`: cancel a superseded PR run instead of running the whole pipeline twice** · P1 · Deps: —
  *Why:* `ci.yml` has no `concurrency:` block (every other workflow here does:
  `docker.yml:31`, `release*.yml`, `soak.yml`). A pushed-after-a-push PR runs
  two full pipelines: in the 30-run sample, `readme-cleanup` (3 runs),
  `fleet-replay-node-guard` (2) and `preflight-job-log` (2) — 4 superseded
  pipelines at **53–63 runner-minutes each**, with `main` pushes never
  cancelable (their result is the record for the commit). With three
  plan-workers plus a merge-all agent pushing constantly, superseded runs are
  the second-largest avoidable cost after CI-1.
  *Change:* `.github/workflows/ci.yml`: add
  ```yaml
  concurrency:
    # A newer push to the same PR supersedes the older run; a push to main (or
    # a tag) never cancels, because its result is the record for the commit.
    group: ci-${{ github.workflow }}-${{ github.ref }}
    cancel-in-progress: ${{ github.event_name == 'pull_request' }}
  ```
  Nothing else. `e2e.yml` and `soak.yml` already cancel in progress; leave them.
  *Done when:* two quick pushes to one PR leave the first run `cancelled` and the
  second running, and a push to `main` is not cancelled by a later push.
  *Proof:* a throwaway PR from `ci/concurrency-probe`, pushed twice 30 s apart,
  `gh run list --branch ci/concurrency-probe --limit 3` shows `cancelled` then
  `in_progress`/`completed` — close it without merging; `mise run lint:workflows`.
  *Anchors:* `.github/workflows/ci.yml:1-12`, `.github/workflows/docker.yml:31-33`,
  `.github/workflows/e2e.yml`.
  *Evidence:* PR #216, merged as `0ce8cec7`. Probe PR #215 (throwaway, closed
  unmerged): runs 41523518884 and 41523612780 both `cancelled` after the second
  push, latest run 41523628604 in progress — the supersede works. PR #216's own
  run 41529503570 was green (28 passed / 3 skipped) and `mise run lint:workflows`
  passed in session.

- [x] **CI-5 — A `changes` job that skips the package jobs a diff cannot have touched, with `main` always running everything** · P2 · Deps: CI-1, CI-3, CI-4
  *Why:* every run executes 25 jobs regardless of the diff; **15 of the 25
  finish in under 1.3 min**, and the file's own measurements put the
  toolchain-restore step at 8–34 s of an 18–36 s mise-action step in jobs whose
  check is 2–10 s, while the four Docker jobs pull images and start brokers for
  PRs that touch a README. Most PRs in this repository touch one package (or
  docs); the full pipeline cost is 53–63 runner-minutes for a one-line docs
  change. A skipped job reports `skipped`, which blocks nothing: `main` has no
  branch protection and no rulesets at all (`gh api
  repos/jamescarr/ankusa/branches/main/protection` → 404, `/rulesets` → `[]`),
  so the guard is the executor's own `gh pr checks --watch` and the fact that
  the *workflow* always runs — hence the filtering must stay at job level, never
  at the workflow's `paths:`.
  *Change:*
  - `.github/workflows/ci.yml`: a `changes` job (`fetch-depth: 2`,
    `git diff --name-only "$BASE" "$HEAD"` — no third-party action to pin) that
    outputs `packages` (the JSON matrix to run), `docs_only`, `examples`,
    `tools`, `conformance`, `integration`, `drills`. For a non-PR event (push to
    `main`, `workflow_call`, a release tag) every output is "run everything".
    Fail-safe: any error inside the step prints the all-true values and exits 0.
  - **The dependency rule the filter must encode** (AGENTS.md: "Every adapter
    package and `ankusa_server` path-depend on `ankusa` core, so a core change
    isn't finished until they pass too"): a diff touching `packages/ankusa/**`
    or `packages/async_api_spex/**` runs **the whole Mix group** (`ankusa`, the
    five adapters, `ankusa_server`, `async_api_spex`, `sdk-elixir`); a diff
    touching `.mise.toml`, `.mise/conf.d/**`, `.mise/lib/**`, `scripts/**` or
    `.github/workflows/**` runs **everything**; otherwise only the touched
    `packages/<pkg>` dirs run. `docs/**`, `*.md`, `marketing/**`, `static/**`
    alone ⇒ `docs_only`. `conformance` runs when any `packages/sdk-*`,
    `conformance/` or `packages/ankusa/**` (the encoder) changes; `integration`
    when `packages/ankusa/**` changes; the two crash jobs when the ack path
    (`packages/ankusa/lib/ankusa/{queue,store,edge}/**`, `store.ex`,
    `queue.ex`, `edge/batcher.ex`) or `packages/ankusa_server/Dockerfile`
    changes; `examples` when `examples/**`, `deploy/helm/**` or a package the
    example path-depends on changes.
  - An empty matrix is a workflow error (`Matrix vector 'package' does not
    contain any values`), so `packages` emits `["-"]` and the matrix job carries
    `if: needs.changes.outputs.packages != '[\"-\"]'` — the standard guard; do
    not rely on `fromJSON('[]')`.
  - Leave the `bench` job alone: it already has its own scope step (CI-2/CI-7).
    Every other job gets an `if:` that reads the `changes` outputs, with its
    existing condition kept on top.
  - A `ci-summary` job (`needs: [changes, …], if: always()`) that fails the run
    when a skip's reason is "the changes job failed", and writes the per-job
    result table to `$GITHUB_STEP_SUMMARY`, so a broken filter is loud rather
    than silent.
  - `docs/testing.md` and the `AGENTS.md` Commands table say which checks a
    single-package PR runs and that `main` always runs all of them.
  *Done when:* a docs-only PR reports its skipped jobs green with a summary line,
  under 10 runner-minutes; a PR touching one SDK runs only that SDK + `lint`; a
  PR touching `packages/ankusa/lib/ankusa/edge/**` runs the core package, the
  perf gate and both crash drills exactly as today; a push to `main` runs all 25
  jobs.
  *Proof:* three probe PRs (docs-only, `packages/sdk-go`-only, core hot-path),
  `gh run view <id> --json jobs --jq '.jobs[]|[.name,.conclusion]'` for each;
  `mise run lint:workflows`; the merged change's first `main` run is fully green.
  *Anchors:* `.github/workflows/ci.yml` (every job's `runs-on`/`needs`/`if`; the
  `bench` job's `HOT_PATHS` scope step as the pattern to follow),
  `.mise/lib/pkg.sh` (`pkg_names`, `pkg_kind`), `.mise/tasks/check/_default`
  (the "everything CI checks" contract this must not contradict).
  *Evidence:* PR #231, merged as `897e6b94`. Measured on its probe PRs:
  **a docs-only PR runs 4 jobs in 36 s** (`changes`, `lint`, the gate's scope
  job, `ci-summary`) instead of ~48 runner-minutes; an `sdk-go`-only PR runs 5
  jobs; a core hot-path PR runs 18 (the whole Mix group, the perf gate, both
  crash drills, conformance, integration, examples; `tools` skipped); a push to
  `main` runs all 28 job entries (run 41578446187, 10:03, green). `discover` was
  folded into the new `changes` job, so the serial hop is gone too. Fail-safe:
  non-PR event, empty/unreadable diff, unknown path and any trap error emit the
  all-true values and exit 0; an unlistable `packages/` fails the job rather than
  skipping everything. A probe caught a real bug before merge: the "nothing to
  run" matrix was emitted as `[-]`, which is not JSON, so `fromJSON` failed at
  job creation (run 41574114453); `["-"]` plus the job's `if:` guard is the fix.
  `ci-summary` writes the per-job table to the step summary and fails the run
  when `changes` itself failed, so a broken filter is loud.

### Phase D — Shorter critical path

- [x] **CI-6 — Split the `ankusa` job into a static-analysis lane and a test lane, each with its own cache key** · P2 · Deps: CI-1
  *Why:* after CI-1 the `ankusa` job is still the whole critical path: compile +
  `credo --strict` + PLT + dialyzer + sobelow is 2:30–3:15 and
  `mix test --cover` is 6:24–8:14, run one after the other in one job. The two
  lanes share only `deps/` and the compile, and both fit in a warm cache.
  Splitting them costs one extra runner and takes ~3 min off every PR's wall
  clock, and surfaces a static failure without waiting for the suite.
  *Change:* `.github/workflows/ci.yml` gains `ankusa-static` and `ankusa-test`,
  both `needs: [changes, discover]`, replacing `ankusa` in the matrix (the
  matrix keeps the other packages). `check:package` grows an optional lane:
  `mise run check:package ankusa --lane static` (deps.get, unlock, hex.audit,
  format, compile `--warnings-as-errors`, credo, PLT restore, dialyzer, PLT save,
  sobelow, docs, `hex.build`) and `--lane test` (deps.get, `mix test --cover`,
  the seed line in the step summary). With no flag it runs both, so
  `mise run check` and every other caller are unchanged — prove that in the PR.
  **Cache keys are per lane** (`mix-v18-…-ankusa-static-…`,
  `mix-v18-…-ankusa-test-…`), each restoring by prefix from the other: CI-1's
  rule, one writer per key.
  *Done when:* both lanes are green on the PR; `ankusa-test` ≤ 9 min and
  `ankusa-static` ≤ 6 min; the run's wall clock (last job to finish) ≤ 22 min for
  a non-hot-path PR; `mise run check:package ankusa` still runs everything;
  coverage floor and dialyzer unchanged.
  *Proof:* the PR's `gh run view <id> --json jobs` durations, before/after;
  `mise run check:package ankusa` locally (full lane); `mise run check:package
  ankusa --lane test`; `mise run lint:pins`.
  *Anchors:* `.mise/tasks/check/package`, `.github/workflows/ci.yml` (the
  `ankusa` matrix entry and its cache step), `.mise/lib/mix_cache.sh`,
  `docs/testing.md` (the "what each suite covers" table).
  *Evidence:* PR #224, merged as `c08970c5`. `ankusa` 10.4 min → `ankusa
  (static)` **1:23** / `ankusa (tests)` **9:47**, pipeline wall clock **9:54**
  (run 41565219821; earlier green run 41550052881: 1:19 / 9:38, wall clock
  9:46). `check:package` gained `--lane static|test`; no flag still runs both
  envs and every step in the original order (proved locally: both "deps
  restored" lines, `Total errors: 0`, same package checksum as `main`). Cache
  keys per lane with a prefix restore from the pre-split tree; `hex.audit` and
  its artifact live only in the static lane (one artifact name, one writer).
  The key version was `mix-v19` in the file (a feature PR had bumped it through
  the documented escape hatch), not `v18` as the item text said. **The finding
  that matters for what comes next:** the test lane is suite-bound (≈40 s of
  preamble + `mix test --cover`), so it lands 7:07–9:47 with the suite's own
  spread — splitting the job no longer shortens the pipeline, the suite does.

- [x] **CI-7 — The perf gate is now the longest job: scope `main` pushes like PRs, and split the self-asserting tenant benches into their own job** · P2 · Deps: CI-2
  *Why:* after CI-1 and CI-3 the pipeline is 13.2 min and the gate is its
  longest job — 12.6 min of run 41544232328 (23:17:46 → 23:30:21), against
  `ankusa` 10.5 min, with every other job finished by 23:21:28. Inside it: two
  legs of 4 cells × 3 runs (~3:20 each), the tenant benches (gauges 69 s +
  dispatch_tick 71 s + intake 5 s + routes 8 s = 2:33), and two compiles. It
  runs on **every push to `main`**, i.e. once per merge, which in this repo is
  dozens a day — although a main push's diff is exactly the PR diff that was
  already gated, and the PR-scope rule (`HOT_PATHS`) already encodes the
  judgement that only a hot-path diff needs it. Runner-minutes are free on a
  public repo, so the value here is less contention, a shorter wall clock on
  ordinary merges, and separating a throughput regression from a fairness one —
  not dollars.
  *Change:*
  1. The `bench` job's scope step: extend the existing `pull_request` branch to
     `push` — diff `github.event.before..github.sha` and set `run=false` when
     nothing matches `HOT_PATHS`. Keep tags skipping, keep
     `workflow_call`/`workflow_dispatch` always running, keep every error in
     the step resolving to `run=true` (fail-safe), and keep the gate's own
     files (`packages/ankusa/bench/`, `.mise/tasks/bench/`) in the pattern.
  2. New `bench-tenant` job (same runner, same warm cache, gated by the same
     scope output): `mise run bench:tenants`, a new task carrying exactly the
     three tenant benches + the routes bench that
     `.mise/tasks/bench/gate` runs today, with the same commands, the same
     `BENCH_REBASELINE` skip and the same assertions; `bench:gate` keeps
     `elixir bench/gate.exs` only. `mise run bench` and any other caller that
     ran the tenant benches keep working — `bench:gate` gains a
     `BENCH_SKIP_TENANTS=1` switch the CI job sets, rather than deleting them
     from the task (local `mise run bench:gate` must still run everything).
  3. `docs/testing.md` "Perf gate" and the long-drills table: which job runs
     what and why a non-hot-path main push skips both; `AGENTS.md`'s
     `bench:gate` row stays true (`mise run bench:gate` locally still runs
     everything).
  *Done when:* three consecutive non-hot-path pushes to `main` show `perf gate`
  and `bench-tenant` `skipped` (each ~5 s) and a pipeline wall clock under
  11 min; a push touching `packages/ankusa/lib/ankusa/dispatch/**` runs both
  unchanged; a hot-path PR still measures base-vs-branch; and an injected
  regression still fails (the throwaway-PR technique CI-2 used).
  *Proof:* `gh run view <id> --json jobs` for those main runs; the throwaway
  regression PR, closed unmerged; `mise run lint:workflows`; a local
  `mise run bench:tenants` and `mise run bench:gate` (the second must still
  include the tenant benches).
  *Anchors:* `.github/workflows/ci.yml` (`bench` job), `.mise/tasks/bench/gate`,
  `packages/ankusa/bench/{gauges_bench,dispatch_tick_bench,intake_bench,routes_bench}.exs`,
  `docs/testing.md:1310-1400`, `AGENTS.md` (Commands table, `bench:gate` row).
  *Evidence:* PR #225, merged as `3641b88e`. The scope step now covers `push`
  events (fail-safe on a zeroed/unknown `before`), the tenant benches moved to
  `mise run bench:tenants` with `bench:gate` keeping them behind
  `BENCH_SKIP_TENANTS=1` for CI, and the merge push (run 41568929419) ran both
  jobs as designed: `perf gate` 10:04, `bench-tenant` 4:16. **The defect that
  run exposed:** `bench-tenant` declares `needs: bench` (to read the scope
  job's `run` output), so it starts 2 s *after* the gate finishes — the hot-path
  wall clock went **up** to 14:24 instead of staying at ~10:04, which is the
  opposite of the item's purpose. CI-10 fixes it; do not treat the 4:16 as
  parallel time until it lands. The three non-hot-path `main` pushes the item
  asked for could not be observed yet (every merge that day was hot-path, and
  the merge of the item itself always is); CI-10's plan-only push supplies one.

### Phase G — Fix what the measurements exposed

- [x] **CI-10 — `bench-tenant` must run *beside* the perf gate, not after it** · P1 · Deps: CI-7
  *Why:* CI-7 split the tenant benches out of the gate to take 2:33–4:16 off its
  critical path, but the new job declares `needs: bench` — it reads the scope
  job's `run` output from it — so on the merge push (run 41568929419) it started
  2 s after `perf gate` finished (`perf gate` 02:54:08 → 03:04:12, `bench-tenant`
  03:04:14 → 03:08:30) and the hot-path wall clock went **up** to 14:24 from
  ~10:04. The two jobs measure different things and share no state; they must
  start together.
  *Change:* hoist the scope step into its own `bench-scope` job (it is a
  checkout, a `gh api`/`git diff`, and a `grep`: seconds), give it the `run`
  output, and make both `bench` and `bench-tenant` `needs: bench-scope` with
  `if: needs.bench-scope.outputs.run == 'true'`. Keep everything else as CI-7
  left it: same `HOT_PATHS`, same fail-safe, same tags/`workflow_call` rules,
  same two tasks. Do not merge the two benches back together.
  *Done when:* a hot-path push to `main` (or the PR that lands this, if its
  diff qualifies) shows `bench-scope`, `perf gate` and `bench-tenant` with
  overlapping start times and a wall clock within ~1 min of the gate's own
  duration (not gate + tenant); a non-hot-path push shows all three skipping in
  ~5 s; a hot-path PR still measures base-vs-branch.
  *Proof:* the run's `gh run view <id> --json jobs` start/end times; a
  non-hot-path push with the three jobs `skipped`; `mise run lint:workflows`.
  *Anchors:* `.github/workflows/ci.yml` (`bench` job's scope step and
  `outputs`, `bench-tenant` job).
  *Evidence:* 2026-11-03 · PR #230 · `mise run lint:workflows`, `mise run
  lint:pins`, `mise run check:links`. The scope step is a `bench-scope` job
  now; `perf gate` and `bench-tenant` both `needs: bench-scope` and start
  together, and `ci-summary` lists the new job. CI-5's `changes` job has no
  `hot-path` output (it is the package/drill filter), so `bench-scope` is not a
  duplicate and the `HOT_PATHS` rules stay where CI-7 left them. The
  non-hot-path case on this PR's own runs: **41573739333** (`8ed29d6e`) —
  `bench-scope` success **16 s** (03:41:57 → 03:42:13) with `perf gate` and
  `bench-tenant` both `skipped`; **41578011409** (`fa2921df`) — `bench-scope`
  **5 s** (04:25:38 → 04:25:43), both `skipped`, run wall clock **10:46**. The
  merge push is the same case on `main`. The overlap needs a hot-path run,
  which this PR's diff is not (`ci.yml` + docs + plan), so it is the first
  hot-path push after the merge — the item's own *Proof* run — and not
  manufactured here.

- [x] **CI-11 — An INCONCLUSIVE perf measurement must report, not fail** · P1 · Deps: CI-2
  *Why:* CI-2 made an unresolvable measurement an explicit verdict, which is
  right in principle and wrong in its consequence. On run 41571000120 (`main`,
  `a57976f8`, a product PR) the gate exited 1 with
  `INCONCLUSIVE p99_ms 512B-c32: samples span 1.76× across rounds (2.99, 3.18,
  3.62 | 1.81, 3.54, 1.76)` — a cell whose absolute latency is 2–3 ms, where a
  1 ms of scheduler jitter is 30 %, and therefore a cell that cannot support a
  1.2× decision at all. It painted `main` red for a reason nobody can act on,
  roughly one run in eight since CI-2 landed (2 of the 8 gate runs). "I could
  not measure this" is information, not a regression.
  *Change:* in `packages/ankusa/bench/gate.exs` only (no `ci.yml` change — the
  script writes `$GITHUB_STEP_SUMMARY` itself):
  1. When a cell's round medians span more than the limit's headroom, re-measure
     that cell for up to **2 more rounds** of `BENCH_RUNS` runs (bounded).
  2. If it is still unresolvable, **do not fail the run**: print the existing
     `INCONCLUSIVE p99_ms <cell>: samples span N× across rounds (…)` line, emit
     `::warning::INCONCLUSIVE p99_ms <cell> — not judged`, append one line to the
     step summary naming the cell, every sample, and the fact that it was not
     judged, and exclude that cell from the failure list.
  3. Never print `ok` for it: the verdict column says `inconclusive`, and a cell
     that is both unresolvable and over its limit says
     `inconclusive (over limit by X%)`. The final line counts them.
  4. Everything else unchanged: `@gates` (1.2× max p99, 0.8× min throughput), the
     matrix, `BENCH_CONFIRM`'s confirm round, `BENCH_REBASELINE`, the per-run
     `/dev/shm` data directory. A *resolvable* failure still exits non-zero.
  5. `docs/testing.md` Perf gate: one paragraph on the policy.
  *Done when:* an exercised inconclusive path exits 0 with the warning, the
  `inconclusive` verdict and the summary line; a resolvable regression still
  exits 1 with `FAIL`; three consecutive hot-path runs are green; the PR's own
  `perf gate`/`bench-tenant` jobs are green.
  *Proof:* the evidence for each of those three paths in the PR body (exit code,
  table excerpt, summary text, and for the regression a tightened
  `BENCH_BASELINE`); `mise run bench:gate` locally.
  *Anchors:* `packages/ankusa/bench/gate.exs` (`settle_spread/6`,
  `print_inconclusive/1`, the `System.halt(1)` on the inconclusive branch,
  `report/5`), `docs/testing.md:1310-1400`.
  *Evidence:* PR #245, merged as `77e22b6f`. The verdict is now bounded-retry then
  report: `@spread_retries = 2` (nine runs max for a cell), then the table says
  `inconclusive` (and `inconclusive (over limit by X%)` when it is also over),
  `::warning::INCONCLUSIVE … not judged`, and one `$GITHUB_STEP_SUMMARY` line
  with every sample — exit code 0 unless another cell has a resolvable failure.
  Proven deterministically with the documented, CI-unset
  `BENCH_SELF_TEST_INCONCLUSIVE=<cell>` switch (spans 2.17× then 1.92×, exits 0,
  a settled cell on the same run kept its real `ok`), and the failure path with
  `BENCH_RUNS=1` plus a quartered baseline (`FAIL p99_ms 512B: 34.09 > 1.2×4.78`,
  exit 1). `@gates`, the matrix, `BENCH_CONFIRM`, `BENCH_REBASELINE` (still
  refuses to record a baseline from samples that wide) and the `/dev/shm` TMPDIR
  are untouched. PR run 41585504758 green (`perf gate` 10:02 + `bench-tenant`
  both ran and passed). The inciting run was 41571000120 on `main`.

### Phase E — Make a single-test flake stop costing a pipeline

- [x] **CI-12 — `ankusa_kafka`: the test publishes before the topic's leader exists** · P1 · Deps: —
  *Why:* run 41575311702 (`main`, `9d774f83` — a docs/examples-only commit) failed
  `ankusa_kafka` 40/42 with two `** (MatchError) no match of right hand side
  value: {:error, :unavailable}` at `Kafka.publish_batch`
  (`test/ankusa/intake/kafka_test.exs:654` and `:538`), each logged immediately
  after `client :"ankusa_kafka.intake_kafka_…" connected to localhost:29092`. The
  `econnrefused 127.0.0.1` lines in the same job are a **red herring**: they come
  from the `kafka_down_*` client the tests configure with
  `brokers: ["127.0.0.1:1"]` (`kafka_test.exs:636`) to exercise the down-broker
  path. Startup is not the problem either — `docker compose up -d --wait` had
  every broker `Healthy` at 04:06:02 (its healthcheck probes
  `kafka-broker-api-versions --bootstrap-server localhost:9092`) and the tests
  began at 04:06:26. What was not ready is the **topic's leader**, and
  `publish_batch` maps that transient to `:unavailable` — correctly. The test
  then asserts `{:ok, _}` on its first publish. Two of the last three
  `ankusa_kafka` failures have this shape.
  *Change:*
  1. Put the wait in the fixture, not the assertion: the test-support helper
     that creates (or deletes and recreates) a topic polls until its partitions
     report a leader — `:brod.get_metadata` or a bounded
     `Kafka.publish_batch` retry — with a timeout that fails with the broker's
     own metadata in the message. No blanket retry around a test body, and no
     assertion changes.
  2. Harden the compose healthcheck while you are there: `x-kafka.healthcheck`
     probes the internal listener only. Probe the **external** one the tests use
     (`localhost:29092` on `kafka-1`), so a listener that has not bound cannot
     report Healthy.
  *Done when:* ten consecutive `mise run check:package ankusa_kafka` runs are
  green with no `{:error, :unavailable}` in their logs (or ten CI reruns if the
  local docker path is unavailable — say which), and
  `docker compose up -d --wait` still succeeds with the new healthcheck.
  *Proof:* the ten runs; a deliberate negative check (publish immediately after
  creating a topic, with the wait disabled → the same `:unavailable`, showing the
  wait is the fix); the compose up output with the external probe.
  *Anchors:* `packages/ankusa_kafka/test/ankusa/intake/kafka_test.exs:520-670`,
  `packages/ankusa_kafka/test/support/` (the topic helper),
  `packages/ankusa_kafka/docker-compose.yml` (`x-kafka`, `KAFKA_LISTENERS`,
  `KAFKA_ADVERTISED_LISTENERS`), `.mise/tasks/check/package`
  (`docker compose up -d --wait`).
  *Evidence:* PR #246, merged as `fc3b3b4e`. The diagnosis came out sharper than
  the item's: `create_topic/1` already polled every broker for leader + ISR, so
  the missing wait was elsewhere. Two windows mapped to `:unavailable` — (a) a
  broker serves the topic in its metadata image before the leader has applied
  its assignment (`not_leader_for_partition`, reproduced 1/150 locally with the
  existing wait), and (b) a replica is in the ISR from creation but has not
  fetched yet, so an `acks=-1` produce commits only on the followers' first
  fetch turn (~1 s idle) against the tests' **2 s** publish deadline — which
  matches the CI failure times exactly (2.00 s / 2.08 s). The fix is a bounded
  (20 s) **commit probe** on a topic of its own with the adapter's producer
  settings, run after the leader/ISR wait; a fresh brod client per attempt
  because brod caches "unknown topic" for 2 minutes. No assertion touched, and
  the probe topic keeps the tests' offsets pristine. Ten consecutive
  `mise run check:package ankusa_kafka` runs green (42 passed each, zero
  `:unavailable` in any log, 132–286 s). Negative check: with the wait stubbed
  and `kafka-3` restarted, the CI shape reproduces (`{:error, :unavailable}` at
  the same two tests); with the wait restored, three restarts are green each.
  Healthcheck now probes **both** listeners per broker (a comma list was
  measured to exit 0 through a closed port, so it cannot prove both); a live
  container reports `healthy`, the probe exits 1 on a closed port, and
  `docker compose up -d --wait` passes. PR run 41583971484: `ankusa_kafka` job
  125745049774 success, 203 s, five containers Healthy.

- [x] **CI-8 — Three flaky tests, not one assertion: make their *measurements* deterministic without touching what they assert** · P2 · Deps: —
  *Why:* in the 30-run sample plus today's runs, four red runs were a **single
  failing test**, and three distinct tests account for them:
  - `Ankusa.Dispatch.IsolationTest` "per-tenant scheduling tenant fairness"
    (`test/ankusa/dispatch/isolation_test.exs:437`): `light tenant p99
    62.843 ms against 1.159 ms alone` vs `assert mixed_p99 <= max(2 * solo_p99,
    25)` — a p99 over too short a window on a loaded runner (`solo` 1.159 ms is
    below the 25 ms floor, so the assertion degenerates to `<= 25`).
  - `Ankusa.Intake.KafkaTest` "earliest, assign_partition and ready?":
    `:econnrefused` to 127.0.0.1, `39/40 passed` — the compose broker is "up"
    before its listener accepts; there is no readiness wait.
  - `Ankusa.Sink.RabbitMQTest` "an idle connection is closed at the broker, and
    the next delivery opens a new one": `36/37 passed` on run 41546975710 on
    `main` — the idle-reaper window is raced against a `sleep`-based wait.
  - `ankusa_redis` `SourceStore.RedisTest`: "condition was still false after the
    deadline" (reported by a sibling on the 2026-11-03 runs) — an `assert
    eventually`-style wait whose deadline is too short for a loaded runner.
  Each occurrence costs a ~13 min rerun plus a wrong attribution — two of this
  loop's own executors re-ran a red run believing their change broke it.
  *Change:* for each test, first record ten consecutive local/CI runs of the
  test to see the distribution, then make the *thing it measures* stable:
  - fairness: warm up the pipeline, then measure over a window long enough to
    contain ≥1000 light-tenant deliveries with the heavy tenant at 99× load, and
    keep `assert mixed_p99 <= max(2 * solo_p99, 25)` verbatim (the bound is the
    gate — do not touch it, do not add a retry that swallows a real regression);
  - Kafka: wait for the listener, not the container (a TCP connect loop, or the
    compose healthcheck the package's other fixtures use), bounded by a timeout
    that fails with the broker's log tail;
  - RabbitMQ: replace the `sleep`-and-assert-reaped pattern with a poll for the
    reaper's own counter/telemetry (`Sink.Reaper` emits one) so the test reacts
    to the event instead of out-waiting it.
  If a fix cannot be done without changing an assertion, stop and say so in the
  PR — that is a design question, not a flake.
  *Done when:* twenty consecutive CI runs (a `ci/CI-8-flake-probe` branch, or
  ten pushes to the PR) show zero failures of these three tests; each test still
  fails when its subject is deliberately broken (say how you proved it — e.g. a
  throwaway commit that disables the reaper or removes the wait); the three
  suites' own jobs stay green; no assertion text changed except the Kafka wait's
  timeout value, which must be stated in the PR.
  *Proof:* the 20-run list (`gh run list --branch …`); the three deliberate
  breakages (throwaway commits, never merged, with the failing test output);
  `mise run check:package ankusa`, `check:package ankusa_kafka`,
  `check:package ankusa_rabbitmq`.
  *Anchors:* `packages/ankusa/test/ankusa/dispatch/isolation_test.exs:437-480`,
  `packages/ankusa_kafka/test/ankusa/intake/kafka_test.exs` (the readiness
  fixture), `packages/ankusa_kafka/docker-compose.yml`,
  `packages/ankusa_rabbitmq/test/ankusa/sink/rabbitmq_test.exs`,
  `packages/ankusa/lib/ankusa/sink/reaper.ex` (the telemetry to poll),
  `.mise/tasks/check/package` (`docker compose up -d --wait`).
  *Evidence:* two PRs, because the causes were unrelated. **#248** (squash
  `1177dc6e`) did the fairness measurement: a 20-pair warm-up window dropped
  from the p99, two steady-state gates instead of a wall clock (the heavy
  tenant's window full, its slow sink every slot busy, two waves finished), 200
  samples with the count asserted (`assert length(lags) == @measured`), the solo
  baseline measured hook-for-hook beside the mixed one, and a 1 ms heartbeat
  that drops samples overlapping a ≥ 25 ms host stall. The assertion
  (`mixed_p99 <= max(2 * solo_p99, 25)`) and the 99× ratio are unchanged. Ten
  consecutive runs: before, `mixed_p99` p50 9.2 / p90 18.3 / max 52.8 ms, 1 of
  10 failed; after, mixed/solo 1.02–1.52 on all 10, and 1.09–1.29 under 12 CPU
  burners. Proven still biting: `Process.sleep(40)` in dispatch's outcome
  handler fails with `light tenant p99 179.354 ms`; 20/20 green locally,
  `2080 passed`, coverage 89.28 %. The same PR fixed a second flake it surfaced
  — "circuit breaker opens after breaker_failures…" (3 runs in 52, `left: 1`):
  the test had asserted which *rows* took the three attempts, which the breaker
  does not promise; it now asserts three attempts, on one to three rows, the
  rest parked with a re-open time. **#246** (CI-12, squash `fc3b3b4e`) did the
  Kafka one in the fixture (leader/ISR wait plus a bounded commit probe, ten
  consecutive green package runs, the CI shape reproduced with the wait
  stubbed). CI on `1177dc6e`: `ankusa (tests)` 9:18 green, `ankusa (static)`
  green, the run green (41583819554). **Not done, carried to CI-15:** the
  RabbitMQ idle-reaper test's `sleep`-and-assert-reaped pattern and the
  `ankusa_redis` deadline — the file `packages/ankusa_rabbitmq/test/ankusa/sink/rabbitmq_test.exs`
  is untouched, and it failed again in this window (run 41575405567,
  `38/39 passed`, `assert connection(inst, exch) == nil`).

### Phase F — The last wall-clock lever: the suite itself

- [ ] **CI-9 — Profile the core suite and cut the test lane's wall-clock time** · P1 · Deps: CI-6
  *Why:* after CI-6 the `ankusa (tests)` lane **is** the pipeline: 9:47 of a 9:54
  wall clock (run 41565219821), ≈40 s of it preamble and the rest `mix test
  --cover`. A profile of the suite (`MIX_ENV=test mix test --slowest 30` in
  `packages/ankusa`, this session) says where it goes: **the top 30 tests are
  72.1 % of the total time, and four of them are 58 %** —
  `test/ankusa/property/migrate_property_test.exs`: "a torn tail is forgiven"
  386.8 s, "any bytes as the WAL" 242.3 s, "any bytes as a 0.3 term log import"
  156.0 s, "any bytes as a cursor or floor sidecar" 46.3 s (831 s of 1445 s on
  this laptop). Next: the five properties of `property/pack_property_test.exs`
  (~69 s together), `RouteRouterOpenAPITest` "every documented error response"
  (11.6 s), `Admin.RouterTest` "GET /v1/dlq clamps limit to 1000" (10.9 s),
  two `Routes.SnapshotTest` cases (10.4 + 9.7 s). Whatever those properties
  cost per *case* — a real fsync per generated case, a store boot per case, or
  gratuitously large generated payloads — is bought 51 times over on every PR.
  A local profile is indicative, not a CI verdict: the item re-measures on CI.
  *Change:*
  1. Re-measure on the CI runner (a throwaway branch with a
     `mise run check:package ankusa --lane test` plus `--slowest 30`, or a
     temporary job step) and record the same table in the PR body. The local
     table above is the expectation, not the evidence.
  2. Attribute the cost of the top properties per case: how many `max_runs`,
     whether each case writes through `Store.write(…, sync: true)`, whether each
     case boots and migrates a store, and the generated payload sizes. Put the
     per-case arithmetic in the PR body.
  3. Fix the *cost per case*, never the case count: point the properties'
     temporary stores at a tmpfs (`/dev/shm` on CI, `TMPDIR` on a laptop — the
     mechanism CI-2 established for the bench), reuse one store across the cases
     of a property where the property's own assertion allows it, and check
     whether the generator's payload ceiling is larger than the property tests
     (e.g. a torn tail needs a tail, not a megabyte). A test that needs a fresh
     store per case keeps one.
  4. If the only remaining lever is `max_runs` or deleting a case, stop and
     report it with the arithmetic — that is a coverage decision for the
     maintainer, not this loop's call.
  5. Apply the same treatment to the next tier only where the profile justifies
     it (an 11 s test that boots 20 instances buys more than shrinking a
     generator). Re-measure.
  *Done when:* the test lane's wall clock drops ≥30 % with the **same test count
  and the same `--cover` floor** (`Result: N/N passed` identical before and
  after); no property's `max_runs` is lower than before; the profile table in
  the PR body shows the change.
  *Proof:* the CI profile before/after (both `--slowest 30` tables), the two
  `Result:` lines and coverage percentages, the PR's `ankusa (tests)` job
  duration, and `mise run check:package ankusa --lane test`.
  *Anchors:* `packages/ankusa/test/ankusa/property/migrate_property_test.exs`,
  `packages/ankusa/test/ankusa/property/pack_property_test.exs`,
  `packages/ankusa/test/ankusa/routes/router_openapi_test.exs`,
  `packages/ankusa/test/ankusa/admin/router_test.exs`,
  `packages/ankusa/test/support/` (the store-boot helpers the properties use),
  `packages/ankusa/mix.exs` (`test_coverage`, `ex_unit` settings),
  `.mise/tasks/check/package` (the `--lane test` step).
  *Evidence:* **not met, not merged.** Probe PR #258 (closed unmerged, branch
  deleted) ran the lane with `TEST_ARGS="--slowest 30"` (`--slowest` implies
  `trace`, so `max_cases: 1`; both sides serialised). Before (run 41643183851):
  `Finished in 613.3 s` (268.9 async / 344.4 sync), top 30 = 422.4 s = 68.9 %,
  `Result: 2081 passed (51 properties, 2030 tests), 41 excluded`, coverage
  89.25 %, job **11:08**. The attempted change (#259, closed unmerged): every
  test store's data root on `/dev/shm` (CI-2's `BENCH_DATA_ROOT` pattern —
  `ANKUSA_TEST_DATA_ROOT` overrides, system temp dir otherwise) plus
  `TEST_ARGS` in the lane task. After (run 41648295752): `Finished in 489.6 s`
  (221.6 / 268.0), top 30 = 306.0 s, rerun of the same commit 467.1 s
  (207.6 / 259.5), coverage 89.17 %, job 8:57 — the five
  `migrate_property_test.exs` properties 136.7 s → 64.3 s (−53 %), the two
  `routes/snapshot_test.exs` cases 24.9 → 18.2 s.
  Per case, which is what this item asked for: `test/test_helper.exs` sets
  `:stream_data, :max_runs` from `PROPERTY_RUNS` (**5 000**), so a property is
  5 000 cases and the 51 properties are ~47 % of the suite. Each
  `migrate_property_test.exs` case writes 1–5 frames (23 B–4.8 KiB measured)
  and calls `Migrate.run/1`, which is ~10 **synced** `Store.write/3` calls —
  `:rocksdb.write_batch(…, sync: true)`, one fsync each: the
  `reopen_for_import` format-2 downgrade, the import's final batch + marker,
  and the 2→8 format ladder's seven migrations, one synced batch each. Measured
  71 ms/case on this laptop's APFS (7.5 ms fsync), 10.6 ms/case on a RAM disk,
  5.6 ms of the APFS case the ladder alone: the cost is the fsync, not the
  payload. `pack_property_test.exs` has no store: 3.2 ms/case, of which the
  property's own body is 0.26 ms — 92 % is `G.pack_claims()` generation
  (0.55 MB of bodies/case after its own 2 MiB budget, 0.6 ms of that SHA-256),
  so its ceiling only comes down by narrowing what is generated, which the
  guardrail forbids. The routes snapshot pair is 400/400 `Routes.create/2`
  calls, one at a time.
  The real lane (`max_cases: 8`, no profile flags), one pair each: `main`
  **394.8 s** (129.2 async / 265.5 sync), job **7:17**, 2081 passed, coverage
  89.21 % → the change **465.8 s** (193.7 / 272.0), job **8:38**, 2080/2081,
  coverage 89.17 %. The 758 RAM-backed stores bought the sync phase nothing in
  that run and the async phase grew; `--max-cases 16` on the baseline (job
  7:32) says the async half is CPU-bound, not short of ExUnit slots. The change
  also met a fixture that is not deterministic on a faster store:
  `Ankusa.Queue.DeliveriesTest` "a sink down for longer than retention keeps the
  node's footprint flat" failed 3 of 3 runner runs (43/46/45 hooks where it
  asserts 40) and passed 24 of 24 locally on both an APFS and a RAM-disk root;
  an hour of `storage.interval_ms` in place of the drill's spinning background
  compactor did not settle it.
  So the ≥30 % *Done when* is **not reachable from cost per case**: the
  profile-side win is real (fsync wait) but the real lane overlaps fsyncs with
  other tests, and the properties' remaining cost is generator CPU and the
  format ladder's RocksDB calls. The wall clock is one 8–9 min job, and **CI-17**
  (partition into three shards and merge the coverage) is what takes it to
  ~3.2 min.

- [x] **CI-17 — Partition the core suite across jobs and merge the coverage, so the pipeline stops being one 9.8 min job** · P2 · Deps: CI-9
  *Why:* `ankusa (tests)` is 9.8 min median of an 11.5 min PR wall clock and
  the last job to finish in 10 of 26 runs (2026-11-03 window); ~40 s of it is
  preamble and ~9 min is one `mix test --cover` in one ExUnit process on a
  4-vCPU runner. ExUnit already splits the suite, and since Elixir 1.18 the
  coverage objection this plan has carried is gone:
  `MIX_TEST_PARTITION=k mix test --partitions K --cover` exports each shard's
  coverage and `mix test.coverage` merges the parts into one report under the
  package's own `:test_coverage` floor — Elixir's docs call this exact case
  out ("Example: aggregating partitioned runs",
  https://hexdocs.pm/mix/1.18.4/Mix.Tasks.Test.Coverage.html). So K shards plus
  a merge step keep the 85 % floor and the same test count, with no new
  dependency (the `ex_coveralls` route the backlog named is not needed). Three
  shards on three runners should land near 3.3 min plus ~0.5 min of merge —
  ~6 min off the wall clock, the largest lever left once CI-9 has taken the
  per-case cost out.
  *Change:*
  1. Measure before building: on a throwaway branch run the suite as
     `MIX_TEST_PARTITION=k mix test --partitions 3 --cover` for k = 1, 2, 3 on
     the CI runner, then `mix test.coverage`; record the per-shard durations,
     their sum, and the merged coverage against the single-process 89.28 %
     (2080 tests, 41 excluded).
  2. `.mise/tasks/check/package`: a partition form of `--lane test` (e.g.
     `--partition 1/3`, passed to `mix test --partitions 3` with
     `MIX_TEST_PARTITION=1`) that keeps the coverage-floor step *out* of the
     shard and fails if the shard's `Result:` line is missing or zero tests ran
     (an empty shard is a workflow bug, not a pass).
  3. `.github/workflows/ci.yml`: `ankusa-test` becomes a matrix of K shards,
     each uploading its exported coverage; a new `ankusa-coverage` job
     (`needs: ankusa-test`, `if: always() && !cancelled()`) downloads them,
     runs `mix test.coverage`, and enforces the **same** 85 % floor — failing
     on a shortfall and on a missing shard artifact. `ci-summary` gains it.
     Keep one ExUnit seed line per shard in the step summary.
  4. `docs/testing.md` (the local check) and the `AGENTS.md` Commands row: how
     to run one shard locally.
  *Done when:* the merged report matches the single-process run — the shards'
    test counts sum to 2080, the merged coverage is within 0.5 points of
    89.28 %, the floor is still 85 % — and the shard jobs plus the merge take
    ≤ 60 % of the old lane's 9.8 min on the same runner class; a shard whose
    artifact is missing fails the merge job (proved with a throwaway commit).
  *Proof:* the two coverage tables and the before/after durations; the
    throwaway PR's failed merge job; `mise run check:package ankusa
    --partition 1/3` locally; `mise run lint:workflows`.
  *Anchors:* `.mise/tasks/check/package` (`--lane`, the `test` env warm),
  `packages/ankusa/mix.exs` (`test_coverage`), `.github/workflows/ci.yml`
  (`ankusa-test` job, `ci-summary`), `docs/testing.md`.
  *Evidence:* 2026-11-03 · PR #265 · four shards + a merge. Per-shard job times
  on run 41679004989 (`3b5f721f`): `ankusa (tests, 1/4)` **188 s** (142.4 s
  test, 506 passed), `2/4` **93 s** (46.8 s, 519), `3/4` **250 s** (200.7 s,
  601), `4/4` **120 s** (71.2 s, 455), `ankusa (coverage)` **40 s**, merged
  **89.22 %** — **292 s** from the first shard's start to the merge's end,
  **49.7 %** of the plan's 9.8 min lane (bar ≤ 60 %) and 67.7 % of the same
  commit's own 431 s single-process job (run 41673183340: 385.6 s of tests,
  `Result: 2081 passed (51 properties, 2030 tests), 41 excluded`, 89.20 %). The
  shards sum to exactly those counts (506+519+601+455 = 2 081, 51 properties,
  2 030 tests, 41 excluded) and the merged table is 0.02 points off. A second
  K=4 run on a noisier host ran every job ~30 % slower (`ankusa_kafka` 232 →
  321 s) and put the same shape at 355 s (60.4 %); a measured K=5 run
  (41685085654: 92/251/114/215/131 s, 297 s, 50.5 %) bought no wall clock, so
  K stays 4 — a heavy property file is whole in one shard either way and the
  ~55 s of per-shard setup is the floor. The floor runs only in
  `ankusa (coverage)` via `mix test.coverage` (exit 3, `Coverage: … /
  Threshold: 85.00 %`; a single shard's export merges to 66.97 % locally), and
  probe PR #266 (closed unmerged, branch deleted) proved both guards on CI:
  threshold 99.9 → merge job exit 3 (`Coverage: 89.25 % / Threshold: 99.90 %`),
  shard 4 dropped from the matrix → merge job exit 1 (`shard 4 uploaded no
  cover/4.coverdata`). Local `mise run check:package ankusa --partition 1/3`
  ran shard 1 (`Result: 765 passed (20 properties, 745 tests)`) and exported
  `cover/1.coverdata`; the single-process `--lane test` path is unchanged.
  `mise run lint:pins`, `mise run lint:workflows` and `mix docs
  --warnings-as-errors` clean.

- [x] **CI-18 — A failed test lane names its tests where the report survives** · P1 · Deps: —
  *Why:* on 2026-11-03 a failed `mise run check:package ankusa --lane test` exited 1 with "1 failed test" and **the failing test's name was lost**: the lane reports failures only on stdout, and stdout is what a CI log view or an agent's tool output clips. `mix test --failed` is the local recovery and CI cannot use it (a fresh VM every run). The lane already writes the ExUnit seed to `$GITHUB_STEP_SUMMARY` (`.mise/tasks/check/package`, `record_seed`, from the `$testlog` that `tee` already captures), so the names belong on the same page. The cost is real: 6 of 30 `ci.yml` runs were red in the 2026-11-03 window and two of those sites were attributed only by pulling job logs through the API, while a third (the run that motivated this item) was never named at all.
  *Change:*
  1. `.mise/tasks/check/package`, the `test` lane: after a non-zero `mix test`, append a bounded block to `$GITHUB_STEP_SUMMARY` — for up to 10 failures the `N) test <name> (<Module>)` line, its `file:line`, and the first 8 lines of its failure body (`code:`, `left:`, `right:`, `** (…)`), then a `Failed: N (showing M)` line; nothing when there are no failures. Parse from the existing `$testlog`; do not re-run anything.
  2. Print the same information as one compact stdout line at the end of the run (`Failed: 2 — <test> (<Module>) at file:line; …`), so a clipped log still names them.
  3. Locally (no `$GITHUB_STEP_SUMMARY`) only the stdout list appears, so `mise run check` grows by a line or two.
  4. `docs/testing.md`: the lane's summary page carries the failing test names, their `file:line` and the seed.
  *Done when:* a deliberately broken test (throwaway commit, never merged) makes the lane exit non-zero **and** the step summary contain that test's name, `file:line` and `code:` line plus the `Failed:` list, while stdout ends with the compact list; a green run appends only the seed line and no failure section; `mise run check:package ankusa --lane test` still runs the same tests with the same coverage floor and the same exit codes (0 green, non-zero red).
  *Proof:* the probe PR's step-summary excerpt and stdout tail (quoted in the PR body), a green run's summary page, `mise run lint:pins`, `mise run lint:workflows`.
  *Anchors:* `.mise/tasks/check/package` (`record_seed`, `testlog`, the `mix test --cover` step), `docs/testing.md` (the check:package / testing section).
  *Evidence:* PR #262. `.mise/tasks/check/package` grew `record_failures`
  beside `record_seed`: one `awk` pass over the `$testlog` that `tee` already
  captured (the suite is never re-run), called only when `mix test` exited
  non-zero, so a green run's summary is byte-for-byte the old one. It parses
  ExUnit's own block (`^ *[0-9]+\) (test|doctest) ` — doctests included — then
  the `file:line` line, then the body until a blank line), appends up to 10
  failures and ~60 excerpt lines plus `Failed: N (showing M)` to
  `$GITHUB_STEP_SUMMARY`, and always prints one compact `Failed: — …` line on
  stdout; when nothing parses it says
  `Failed: N (names unparsed — see the job log)` instead of nothing. A green
  run, with `GITHUB_STEP_SUMMARY` set, exited 0 and left exactly one line:
  `- \`async_api_spex\`: ExUnit seed \`522092\``
  (`mise run check:package async_api_spex --lane test`, local, 2026-11-03).

  Probe PR #261 — a throwaway failing test, closed unmerged, branch deleted —
  run [41657646044](https://github.com/jamescarr/ankusa/actions/runs/41657646044)
  / job 125992235684, `mise run check:package ankusa --lane test` exit code 2,
  and the `$GITHUB_STEP_SUMMARY` it wrote, printed by a probe-only `cat`
  appended to the same workflow step (the summary file is per step, so this is
  the page CI renders):

  ~~~text
  - `ankusa`: ExUnit seed `545988`
  - `ankusa`: failing tests (names, locations and the head of each failure)

  ````text
    1) test CI-18 probe: a raise names itself (Ankusa.Ci18ProbeTest)
       test/ci18_probe_test.exs:10
       ** (RuntimeError) CI-18 probe boom
       code: raise RuntimeError, "CI-18 probe boom"
       stacktrace:
         test/ci18_probe_test.exs:11: (test)

    2) test CI-18 probe: an assertion names itself (Ankusa.Ci18ProbeTest)
       test/ci18_probe_test.exs:6
       Assertion with == failed
       code:  assert 1 + 1 == 3
       left:  2
       right: 3
       stacktrace:
         test/ci18_probe_test.exs:7: (test)

  Failed: 2 (showing 2)
  ````
  ~~~

  and its stdout tail, after `mix test --cover`'s coverage table:

  ~~~text
  Failed: 2 — CI-18 probe: a raise names itself (Ankusa.Ci18ProbeTest) at test/ci18_probe_test.exs:10; CI-18 probe: an assertion names itself (Ankusa.Ci18ProbeTest) at test/ci18_probe_test.exs:6
  [check:package] ERROR task failed
  ~~~

  A real (non-probe) red run reproduces the same shape: this worktree's own
  `mise run check:package ankusa --lane test` on 2026-11-03 exited 2 on an
  `assert_receive` timeout in `Ankusa.Sink.SQSTest` and ended with
  `Failed: 1 — batched (a delivery from dispatch: ctx batch: true, an
  instance's batchers) an ordered FIFO group is one request in flight, entries
  in admission order (Ankusa.Sink.SQSTest) at test/ankusa/sink/sqs_test.exs:698`.
  The lane's `mix test --cover`, its `--cover` floor, its `tee`, its step order
  and its exit codes are untouched; the green run above and the merged PR's
  `ankusa (tests)` job are the 0 side. `mise run lint:pins` and
  `mise run lint:workflows` green.

- [x] **CI-19 — The `/ready` flip deadline: measure the flip, not the test's round trip** · P2 · Deps: —
  *Why:* `Ankusa.DrainTest` "SIGTERM with 200 requests in flight: ready flips, zero resets, every 201 survives a restart" failed on `main` (run 41625156213, `da3a7bb9` — a `docker.yml`-only diff) with `assert flip_ms <= 100, "/ready took #{flip_ms} ms to flip to draining"`, `Result: 2079/2080 passed`. Unlike every other site the measurement found, 100 ms is a **product promise** about how fast `/ready` flips after SIGTERM — the bound must not move. What is wrong is what `flip_ms` spans: `sent = System.monotonic_time(:millisecond)` → `System.cmd("kill", ["-TERM", …])` (a **fork/exec of a shell**) → the drainer → an HTTP poll of `/ready` **on the listener that holds 200 in-flight requests**, retried with no delay (`await_draining/2`). Three of those terms are the test harness's, not the drainer's, and on a loaded runner the fork alone can exceed 100 ms.
  *Change:* attribute first, on the runner, with numbers:
  1. State in the PR exactly what `flip_ms` spans today (the four terms above, with the code paths).
  2. Instrument (throwaway probe PR) the harness's and the drainer's own timestamps — SIGTERM receipt, `draining` set, `/ready` served — and collect ~20 samples of `flip_ms` and of the harness-internal span on the hosted runner.
  3. If the internal span is ≪100 ms and `flip_ms` is dominated by the fork/exec and the polled HTTP round trip, the measurement is the bug: apply `flip_ms <= 100` **verbatim** to the span that starts when the harness *receives* SIGTERM (its own marker line on the port) and keep a separate, non-timing assertion that `/ready` answers `503 draining`.
  4. If the internal span is not ≪100 ms, **stop** — that is a product defect: report the samples, do not widen or patch the bound.
  5. `docs/testing.md` only if the test's contract is documented there.
  *Done when:* the PR shows both distributions and either (a) the bound re-based onto the promise's own span, unchanged, with 20/20 green on the runner, or (b) the item closes as a product finding with the samples in *Evidence*.
  *Proof:* the two sample tables (20 rows each), the probe run ids, `mise run check:package ankusa --lane test`, `mise run lint:workflows`.
  *Anchors:* `packages/ankusa/test/ankusa/drain_test.exs` (the SIGTERM test's `flip_ms`, and `await_draining/2`), `packages/ankusa/test/support/drain_harness.exs`, `packages/ankusa/lib/ankusa/instance/drain.ex` (read-only; out of scope to change).
  *Evidence:* probe PR #263 (closed unmerged, branch deleted), run [41668142392](https://github.com/jamescarr/ankusa/actions/runs/41668142392): 20 hosted-runner samples — harness span (SIGTERM receipt → `Health.drain/1`) 3–5 ms, median 4, **20/20 under 100 ms**; the old `flip_ms` (before the `kill` → first `503 draining` answer) 11–192 ms, median 12, **2/20 over 100 ms**. `kill` fork/exec 1–2 ms; the residual 9–190 ms is the polled `/ready` round trip on the listener holding the 200 requests. The unchanged assertion `assert flip_ms <= 100, "/ready took #{flip_ms} ms to flip to draining"` now applies to the harness's own span; `/ready` answering `503 draining` is asserted separately and untimed; `docs/testing.md` says which is which. PR #264: `mise run check:package ankusa --lane test` green, `mise run lint:pins` and `mise run lint:workflows` OK.

- [ ] **CI-13 — Evaluate an arm64 runner for the Mix jobs, measured, not guessed** · P3 · Deps: CI-5
  *Why:* the repository is public, so standard runners are free either way, and
  GitHub offers `ubuntu-24.04-arm` at the same price for public repos. The
  `ankusa (tests)` lane (9:47) is the pipeline, and its heaviest inputs —
  RocksDB's C++ NIF compile and a 2 000-test BEAM suite — are exactly where
  arm64 is usually faster than the emulated/x64 runners this repo has measured
  so far. Nobody has measured it here, and a guess would be wrong in either
  direction.
  *Change:* one probe, no product changes: a throwaway branch that runs
  `ankusa (tests)` (or `mise run check:package ankusa --lane test`) on
  `ubuntu-24.04-arm` beside the same job on `ubuntu-latest`, three runs each,
  recording the job duration, the deps-compile time and the test-only time.
  If arm64 wins by ≥15 % on two of three measures, move the Mix jobs
  (`ankusa (static)`, `ankusa (tests)`, the adapters, `ankusa_server`,
  `conformance`, `integration`) to it in a follow-up PR — checking first that
  every image in the image cache has an arm64 variant at the pinned digest
  (`docker buildx imagetools inspect`), and keeping the cache keys keyed by
  `runner.arch` so the two classes never share an entry. If it does not win,
  record the numbers in *Evidence* and close the item.
  *Done when:* either the migration PR is green with its before/after numbers,
  or the item's Evidence carries the six measurements and says why not.
  *Proof:* the six runs (three per runner class, ids and durations), the
  `imagetools inspect` output for every pinned image if you migrate.
  *Anchors:* `.github/workflows/ci.yml` (`runs-on` per job, the `mix-v19-…`
  cache keys already include `runner.arch`), `packages/ankusa_server/Dockerfile`
  (the image jobs stay where they are), `docs/testing.md` (runner class).
  *Evidence:* —

### Phase H — The red runs no diff caused

- [x] **CI-14 — The perf gate's base leg fails the job when it cannot settle a cell** · P1 · Deps: CI-11
  *Why:* run 41586954095 (`main`, commit `77e22b6f`, CI-11's own merge, nothing
  changed since its green PR run) went red in `perf gate`:

  ```
  INCONCLUSIVE p99_ms 512B-c32: samples span 1.28× across rounds
    (6.12, 4.76, 6.21 | 5.47, 5.77, 6.10 | 4.78, 5.51, 4.71)
    — wider than the 1.20× the limit can resolve
  INCONCLUSIVE
  [bench:gate] ERROR task failed
  ```

  That is the **base** leg: the "measure the base commit on this runner" step
  runs `BENCH_REBASELINE=1`, and `gate.exs`'s rebaseline branch treats an
  unsettled cell as a hard refusal (`System.halt(1)`, header: "it still fails on
  one, because that is a refusal to record a baseline from samples that wide").
  The refusal is sound; failing the pipeline is not — no diff was involved, the
  job died *before* the branch leg ran (`bench` is one job with the two legs in
  sequence), and 10.6 min of red bought nothing. The samples also say *why* it
  could not settle: round medians 6.12 → 5.77 → 4.78 ms, a monotone warm-up
  trend (first round slowest), so the spread is systematic, not tail noise.
  Frequency: the gate measured in only **4 runs** in the 2026-11-03 window (the
  other 18 were scope no-ops — `no hot-path file touched`), and **1 of those 4
  failed this way**: 25 % of the measurements, on 1 of the 8 gated numbers. It
  will hit every hot-path PR — CR-2 and CR-3 of the feature plan change
  `edge/ingest.ex`, `queue.ex` and `sink/http.ex` — and every `main` push that
  touches them.
  *Change:* fix the measurement, never the limits: `@gates` stays
  `p99_ms ≤ 1.2×`, `ingest_per_s ≥ 0.8×`, the four cells stay, `BENCH_CONFIRM`
  stays 1.
  1. Diagnose the trend first: three `BENCH_RUNS=3 BENCH_TRACE=1 mise run
     bench:gate` runs on an unchanged tree, recording each cell's p99 per run
     per round. Put the table in the PR.
  2. If it is warm-up, add one **discarded** warm-up run per cell per leg —
     `collect/5` gains a round whose samples feed neither the medians nor the
     spread — and re-measure: the cell's round medians must then agree inside
     the limit's headroom on a runner where today's shape does not.
  3. Whatever remains unstable: the base leg must not *fail* on it. Write the
     settled cells to the temp baseline, omit the unsettled cell, print the same
     `INCONCLUSIVE p99_ms <cell>: …` line + `::warning::` + step summary, and
     exit 0; `evaluate/3` must treat a cell absent from the baseline as **not
     judged** (never as passed), and the branch leg then reports that cell
     `inconclusive` — the policy CI-11 already applies on the branch side.
     Every cell the measurement *can* decide keeps its judgement, and a cell
     over its limit still fails.
  4. `docs/testing.md` "Perf gate": the warm-up run, and that a cell the base
     leg cannot settle is reported and not judged (never a pass, never a red
     run).
  *Done when:* with the harness's own `BENCH_SELF_TEST_INCONCLUSIVE=512B-c32`
  (built for this), both legs are green, the job prints the INCONCLUSIVE line
  for that cell, and the step summary says not judged; with that switch **and**
  a real `Process.sleep(1)` at the top of `Ankusa.Edge.Ingest.ingest/2`, the
  same run still **FAILS** on another cell (the gate bites while one cell is
  unjudged — the proof CI-2 used); two reruns of the failing `main` commit
  produce the same verdict; `git diff` touches only `packages/ankusa/bench/gate.exs`
  and `docs/testing.md`.
  *Proof:* the self-test runs' ids with their `INCONCLUSIVE` and `FAIL` lines;
  the three `BENCH_TRACE=1` tables; `mise run bench:gate` locally;
  `mise run lint:workflows`.
  *Anchors:* `packages/ankusa/bench/gate.exs` (header, `settle_spread/6`,
  `unsettled/3`/`wide/2`/`unstable/3`, `report/5`, the rebaseline `cond`),
  `.github/workflows/ci.yml` (`bench` job, "measure the base commit on this
  runner"), `docs/testing.md` "Perf gate".
  *Evidence:* PR #250. Diagnosis: three `BENCH_RUNS=3 BENCH_TRACE=1` gate runs
  on an unchanged tree, p99 per run per round in the PR body (local bench data
  on a 4 GiB RAM disk — macOS has no `/dev/shm` — and the laptop was under load
  average 11–14 while other agents worked). That machine settles no cell in
  either shape (8 of 12 cell-runs inconclusive before, 6 of 12 after) and its
  first-round median was not systematically higher (0.78×–1.23× of the later
  rounds): the monotone trend the CI run showed is a first-run cost the busy
  machine buries, so the warm-up is the item's measured cheap discard (one run
  per cell per leg, feeding neither the medians nor the spread), and the safety
  comes from the policy below. Change: `warmup/3`; the `BENCH_REBASELINE=1`
  branch writes the settled cells, leaves an unsettled one out, prints the
  CI-11 `INCONCLUSIVE` line + `::warning::` + step summary and exits 0;
  `evaluate/3` skips a cell absent from the baseline (never a pass) and the
  branch leg reports it `inconclusive` once, naming the missing entry; every
  other cell keeps its judgement. `@gates`, `@matrix`, `@spread_retries`,
  `BENCH_CONFIRM` untouched. CI: PR #250's own `perf gate` is green twice on
  the same runner — run 41604058589 / job 125809478781 (12m9s, all four cells
  settled, deltas −5.8 %…+3.9 %) and rerun 41608411532 / job 125824087558
  (15m28s) — and the two throwaway PRs (closed unmerged, branches deleted) are
  the proofs. #251 (`BENCH_SELF_TEST_INCONCLUSIVE: 512B-c32`), run 41608424667
  / job 125824024592: base leg prints `INCONCLUSIVE p99_ms 512B-c32: samples
  span 3.10× …`, `rebaselined … without 512B-c32`, exits 0; branch leg prints
  the same line plus `— and the baseline has no entry for it, so it cannot be
  judged`, one `::warning::`, `1 cell inconclusive (512B-c32): not judged`, and
  the job is green (16m8s) with the other cells `ok` (`512B ingest_per_s
  21251.00 vs 21816.10`, `10240B p99_ms 54.50 vs 55.01`). #252 (the switch plus
  `Process.sleep(20)` at the top of `Ingest.ingest/2`), run 41611183268 / job
  125833239480: `perf gate` **fails** (21m4s) with `1 cell inconclusive
  (512B-c32)` and `FAIL p99_ms 512B: 45.90 > 1.2×29.16`, `FAIL ingest_per_s
  512B: 10615.85 < 0.8×23475.03`, `FAIL ingest_per_s 10240B`, `FAIL p99_ms
  512B-ordered`; two reruns of that commit (attempt 2, job 125845714830;
  attempt 3, job 125858306136) are the same verdict. The item's `sleep(1)`
  cannot fail a judged cell here: measured at 41604135537 / job 125810067182 it
  left `512B ingest_per_s 20575.28 vs 19647.27` (+4.7 %), and `sleep(5)` at
  41608453549 / job 125824475220 left `19399.77 vs 22047.58` (−12.0 %), both
  inside the 20 % the 0.8× limit allows — 1 ms is 8 % of a 256-worker cell's
  ~12 ms service time, and CI-2's own `sleep(1)` proof failed only
  `ingest_per_s 512B-c32`, the cell the switch unjudges; `sleep(20)` is the same
  class of regression, sized to the runner. `mise run bench:gate` locally,
  `mise run lint:pins`, `mise run lint:workflows`; `git diff` is
  `packages/ankusa/bench/gate.exs`, `docs/testing.md` and this file.

- [x] **CI-15 — The four flake sites: RabbitMQ reaper, `ankusa_redis` deadline,
  deliveries marker bucket, NATS subscription** · P2 · Deps: —
  *Why:* CI-8 fixed the tenant-fairness measurement (#248) and CI-12 the Kafka
  readiness wait (#246); the sites it left, plus the two the 2026-11-03
  measurement added, are four tests whose wait measured a different event from
  the one they assert — each cost a full rerun (~11 min) plus a wrong
  attribution:
  - `Ankusa.Sink.RabbitMQTest` "an idle connection is closed at the broker, and
    the next delivery opens a new one"
    (`packages/ankusa_rabbitmq/test/ankusa/sink/rabbitmq_test.exs:387`) failed
    run 41575405567 / job 125717852245 with `Assertion with == failed … code:
    assert connection(inst, exch) == nil`, `Result: 38/39 passed`. The test
    sweeps the reaper deterministically (`sweep_as_idle/1` + `:sys.get_state/1`
    as a barrier) and then waits for the AMQP process's `:DOWN`; it asserts the
    *registry* is empty the instant the process dies, which is a different
    event — the registry processes its own exit message after the monitor fires.
  - `ankusa_redis`: `flunk("condition was still false after the deadline")` in
    four files (`source_store/redis_test.exs:85`, `routes/store/redis_test.exs:85`,
    `routes/store/redis_scale_test.exs:94`, `rate_limiter/store/redis_test.exs:116`)
    behind `eventually(fun, deadline_ms \\ 2_000)` polling every 20 ms — a 2 s
    budget for cross-node propagation through a Redis in Docker on a loaded
    runner. Measured once in the 2026-10-31 window (`82/83`); `main` has not hit
    it since, so the deadline needs a distribution before it is changed.
  - `Ankusa.Queue.DeliveriesTest` "the active-tenant markers cleanup does not
    lose a marker to a concurrent enqueue" failed run 41591480530 / job
    125769019148 — the merge of a **docs-only** PR, so the code cannot be at
    fault — with `MatchError … no match of right hand side value: :not_found`
    at `deliveries_test.exs:550` and `2079/2080 passed`. The setup writes the
    marker at `Keys.active(div(old, 1000), ord)` and the injected commit calls
    `Deliveries.due_ops(ord, old + 5, seq, 0, 10, 0)`, whose marker key is
    `Keys.active(div(old + 5, 1000), ord)`: at `rem(old, 1000) >= 995` the
    commit's marker lands in the *next* second, the sweep legitimately deletes
    the bucket it was told about, and the assertion reads `:not_found` — two
    steps × 5/1000 ≈ 1 %.
  - `Ankusa.Sink.NATSTest` "an inline message carries the payload, subject, and
    headers, and is stored" failed run 41621508311 attempt 1 / job 125868163861
    (`22/23 passed`) with `Assertion failed, no matching message after 2000ms`
    and `The process mailbox is empty.` `Gnat.sub/3` writes SUB to the
    subscriber's socket and returns, while the sink publishes on its own
    connection: the server may read the PUB first, JetStream stores the message
    with no live subscriber (so it is never delivered), and `deliver/3` still
    answers `:ok`. The `failed to connect :econnrefused` lines beside the
    failure are the three `servers: ["127.0.0.1:1"]` tests' connection
    supervisors retrying every 2 s for the rest of the suite
    (`nats_test.exs:313,344,360`), not the sink under test.
  *Change:* the CI-8 rule — change what the test *waits on*, never what it
  asserts.
  - RabbitMQ: after `assert_receive {:DOWN, …}`, `await_unregistered/2` polls
    the same registry lookup with a 2 s deadline before the unchanged
    `assert connection(inst, exch) == nil`; a registration that outlives it
    fails with the pid it names, so a stale connection is never retried away.
  - `ankusa_redis`: all four helpers take the deadline from the CI runner's own
    distribution — **2 s / 2 s / 5 s / 3 s → 10 s**, 20× the measured p99.9
    (504 ms) and 5× the old default, still below the stores' periodic refresh
    so a dropped broadcast fails the test instead of being waited out.
  - Deliveries: the setup puts `old` 100 ms into its second
    (`div(now - 10_000, 1000) * 1000 + 100`) so the commit's `old + 5` cannot
    cross buckets. Every assertion is unchanged, including the final
    `assert :not_found`.
  - NATS: after `Gnat.sub/3`, `await_subscription/1` waits for a server round
    trip on the subscriber's own connection (a request to a subject nobody
    listens on is answered `:no_responders` only after the server has processed
    the earlier SUB; one connection is processed in order). The assertions are
    unchanged.
  *Done when:* twenty consecutive CI runs of the affected package jobs (or ten
  pushes to the PR) with zero failures of any of the four tests, and each test
  still fails when its subject is deliberately broken (a throwaway commit that
  disables the reaper / shifts the marker's second / subscribes late, output in
  the PR, never merged).
  *Proof:* ten consecutive whole-`ci.yml` runs of the PR's test-code commit
  (the "ten pushes" option; each run includes `ankusa (tests)`,
  `ankusa_rabbitmq`, `ankusa_redis` and `ankusa_nats`), and the four
  deliberate-breakage outputs.
  *Anchors:* `packages/ankusa_rabbitmq/test/ankusa/sink/rabbitmq_test.exs:380-420`,
  `packages/ankusa_redis/test/ankusa/source_store/redis_test.exs:74-91`,
  `packages/ankusa_redis/test/ankusa/routes/store/redis_test.exs:75-90`,
  `packages/ankusa_redis/test/ankusa/routes/store/redis_scale_test.exs:85-93`,
  `packages/ankusa_redis/test/ankusa/rate_limiter/store/redis_test.exs:108-120`,
  `packages/ankusa/test/ankusa/queue/deliveries_test.exs:527-560`,
  `packages/ankusa/lib/ankusa/queue/deliveries.ex` (`due_ops/6`,
  `sweep_markers/5`), `packages/ankusa/lib/ankusa/store/keys.ex:104`
  (`active/2`), `packages/ankusa_nats/test/ankusa/sink/nats_test.exs:93-120`,
  `packages/ankusa_nats/lib/ankusa/sink/nats.ex` (`connection/4`,
  `supervisor_spec/3`), `packages/ankusa_nats/docker-compose.yml`,
  `.mise/tasks/check/package` (`docker compose up -d --wait`).
  *Evidence:* four proofs, each with the failure forced on `main` and the same
  test green after the change; the `ankusa_redis` measurement came from the
  throwaway probe PR **#256** (closed unmerged, branch
  `ci/CI-15-deadline-probe` deleted): run 41628378393 / job 125892165949,
  twenty runs of the four files with a 60 s cap. **RabbitMQ reaper:** on
  `main`, with every `Ankusa.Registry.PIDPartition*` suspended for 300 ms
  around the sweep (throwaway edit), the test fails 3/3 with
  `left: #PID<0.458.0>, right: nil` —
  the assertion does read the table before the partition handles the exit; with
  `await_unregistered/2` the same forced run passes. 50 local repeats
  (`mix test --repeat-until-failure 50`), 17/17 for the package file.
  **`ankusa_redis`:** 20 runs / 945 waits with the output kept: p50 21 ms,
  p90 105 ms, p99 504 ms, p99.9 504 ms, max 504 ms — the old 2 s was only 4×
  the p99.9. Zero waits were ever *never* true (60/60 tests passed in each of
  the 20 runs, none above 504 ms), and 60/60 pass locally after the change.
  **Deliveries:** forcing the remainder on `main`
  (`old = div(now - 10_000, 1000) * 1000 + 997`) fails 3/3 with the CI's exact
  `MatchError … :not_found`; at +100 ms it passes 100/100
  (`--repeat-until-failure 100`) and the file 30/30. **NATS:** with toxiproxy
  delaying the subscriber's upstream by 500 ms, the unflushed sequence
  reproduces the CI failure exactly (`no matching message after 2000ms`,
  mailbox empty) and the flushed one delivers; 100/100 local repeats of the
  test, 16/16 for the file. The sample found a fifth occurrence of the reaper
  race, in this package's own idle-reaper test (`assert Ankusa.whereis(inst,
  {:nats_sup, :default}) == nil`, run 41630250047 attempt 7 / job
  125923501565): the same bounded poll after its `:DOWN`, forced the same way
  (fails without the wait, passes with it). **Runs:** eight whole-`ci.yml`
  reruns of the PR's test-code commit (attempts 1–8 of run 41630250047) —
  seven green on all four affected jobs, and attempt 7 red only in
  `ankusa_nats` (the sibling above, fixed in the next commit) and
  `ankusa_kafka` (`replay without a stamped partition … {:error, :unavailable}`
  after a brod producer crash — a different, pre-existing flake in a package
  this PR does not touch, not papered over). The final commit's own run is the
  ninth; the ten-run target was cut when the landing was prioritised.

### Phase I — Stop rebuilding the image for this loop's own diffs

- [x] **CI-16 — `docker.yml` must not build the image for a `ci.yml`-only diff** · P1 · Deps: —
  *Why:* `.github/workflows/docker.yml` lists `.github/workflows/ci.yml` in both
  its `push` and its `pull_request` `paths`, so **every** CI change this loop
  makes rebuilds and smoke-tests both architectures: 13 of the last 30
  `docker.yml` runs are `ci:`-titled PRs, each 3 non-skipped jobs
  (`Resolve version and tags` 0.1 min, `Build and smoke (linux/amd64)` 0.9,
  `Build and smoke (linux/arm64)` 0.9) and **2.0 min median / 3.1 p90** of wall
  clock on a workflow an agent is already blocked on — measured on the 30
  `docker.yml` runs of 2026-11-03. The diff cannot change the image: the
  workflow's only use of `ci.yml` is the `ci` job, which is
  `if: github.ref_type == 'tag'`, and path filters are not evaluated for tag
  pushes (the file says so in its own comment) — so a release still runs the
  full suite before an image ships.
  *Change:* drop `- ".github/workflows/ci.yml"` from both `paths` lists in
  `.github/workflows/docker.yml` (the `push` one under `branches: [main]` and
  the `pull_request` one). Keep `packages/ankusa*/**`,
  `packages/async_api_spex/**`, `.dockerignore` and
  `.github/workflows/docker.yml` itself. Add a one-line comment beside the
  remaining paths saying why `ci.yml` is not one of them (the `ci` job reads it
  at tag time only).
  *Done when:* a `ci.yml`-only PR shows no `docker.yml` job in its checks, while
  a `packages/ankusa/lib/...` change (or the next real core PR) still does; the
  tag path is unchanged in the file (`if: github.ref_type == 'tag'` on `ci`,
  path filters not evaluated for tags).
  *Proof:* the two PRs' check lists (a throwaway `ci.yml`-comment PR and a
  throwaway `packages/ankusa/lib` comment PR, both closed unmerged);
  `mise run lint:workflows`.
  *Anchors:* `.github/workflows/docker.yml` (`on.push.paths`,
  `on.pull_request.paths`, the `ci` job's `if`).
  *Evidence:* PR #253. `.github/workflows/ci.yml` dropped from both `paths`
  lists (the two `packages/*`, `.dockerignore` and `docker.yml` filters kept),
  with the reason folded into the "path filters are not evaluated for tag
  pushes" note above the lists and a one-line pointer in each list; the `ci`
  job's `if: github.ref_type == 'tag'`, the `build`/`merge`/`publish-meta`
  jobs, the pins and `concurrency:` are untouched. Measured on 2026-11-03:
  baseline 25 `pull_request` runs to 11:00 — 3 non-skipped jobs each (`Resolve
  version and tags` 4–5 s, `Build and smoke (linux/amd64)` ~2m5s, `(linux/
  arm64)` ~1m36s), median 135 s wall clock (e.g. run 41621508408, 138 s).
  Probe PRs, both branched off #253's head so they run the new file, both
  closed unmerged with branches deleted: #254 (only a comment in
  `.github/workflows/ci.yml`) — **no `Docker image` workflow run at all**; its
  24 checks are `ci.yml` jobs only. #255 (only a comment in
  `packages/ankusa/lib/ankusa.ex`) — run 41624301573, 152 s, 3 non-skipped
  jobs, so the package filter still fires. #253 itself edits `docker.yml`, the
  one kept self-filter: run 41624247554, 57 s, 3 non-skipped jobs (warm buildx
  cache), so that trigger still fires too. Net for the loop: a CI-only PR now
  costs 0 docker jobs and 0 docker seconds instead of 3 jobs and ~135 s.
  `mise run lint:workflows` and `mise run lint:pins` green.

### Phase J — The perf gate is now the critical path

- [x] **CI-20 — The perf gate's warm-up costs 84 s per leg, and the gate is now 2.3× the rest of the pipeline** · P2 · Deps: CI-14
  *Why:* CI-17 took the test lane from 9.8 to ~4.9 min (four shards + merge, 292 s
  on run 41679004989), so a PR whose diff touches a hot-path file is now wall
  clocked by `perf gate` alone: **12.0 / 12.2 / 13.8 min** on the three runs
  where it measured, while every other job finishes under 7. Job 125809478781's
  12m9s decomposes as setup 46 s, **base leg 5m35s** and **branch leg 5m40s**,
  each leg being 4 discarded warm-up runs (**84 s**) + 12 measured runs
  (~4m10s) at ~21 s a run for 20 000 hooks. The warm-up is CI-14's addition and
  it earned its place (it is what removes the first-round drift); its *size*
  never had to be the measured size — 84 s per leg is 23 % of the job, and 2m48s
  of the 12m9s is spent warming a code path, not measuring one.
  *Change:* fix the cost of the warm-up, never the measurement's strictness —
  `@gates`, `@matrix`, `BENCH_RUNS`, `BENCH_CONFIRM`'s default, the confirm
  round and CI-14's settle/omit policy all stay exactly as they are.
  1. Make the warm-up's size a constant of its own (`@warmup_n`, or a
     `BENCH_WARMUP_N` knob defaulting to it) instead of the measured `@n`, and
     use a small run count (the shape the item's measurement decides, e.g.
     2 000 hooks at each cell's body size and concurrency).
  2. Measure that it still does its job: three `BENCH_RUNS=3` gate runs on an
     unchanged tree with the short warm-up, per-cell round medians, against the
     same three runs with the full-size warm-up (CI-14's evidence is the
     reference for the drift it removes). A cell whose round medians are wider
     than the limit's headroom on the short warm-up means the short warm-up does
     not warm that path: keep the full size for that cell and say so.
  3. Print the warm-up's size in the run's log lines (`bench <cell> warm-up
     (discarded, N hooks)`) so the next measurement can see it.
  4. `docs/testing.md` "Perf gate": the warm-up is a fixed small run, and why
     it is smaller than the measured one.
  *Done when:* on a hot-path PR the `perf gate` job is **≥2 min faster** with
  all four cells settled and the same verdicts (each cell's deltas inside the
  limit, `PASS`), the round medians of the three-run table still agreeing inside
  the limit's headroom, and `git diff` showing no change to `@gates`, `@matrix`
  or the confirm/settle logic; or, if the short warm-up does not settle a cell,
  the item closes with the three-run tables in *Evidence* and that cell's full
  warm-up restored.
  *Proof:* the two three-run tables (full vs short warm-up) with per-cell round
  medians; the hot-path PR's `perf gate` duration next to 12m9s (job
  125809478781); `mise run bench:gate` locally; `mise run lint:workflows`.
  *Anchors:* `packages/ankusa/bench/gate.exs` (`@n`, `warmup/3`, `collect/5`,
  `@spread_retries`, the header's warm-up paragraph),
  `.github/workflows/ci.yml` (`bench` job), `docs/testing.md` "Perf gate".
  *Evidence:* PR #268. Change: `@warmup_n` 2 000 — overridable with
  `BENCH_WARMUP_N` — at each cell's own body size and concurrency; `warmup/4`
  and `run_bench/8` take the hook count, and each line names it (`==> bench
  512B warm-up (discarded, 2000 hooks)`); the measured runs keep `N=20000`.
  `@gates`, `@matrix`, `@n`, `BENCH_RUNS` and `BENCH_CONFIRM`'s defaults, the
  confirm round and CI-14's settle/omit policy are untouched (`git diff` is the
  warm-up, `run_bench`'s arity, the header and `docs/testing.md`).
  Measurement: three `BENCH_RUNS=3` gate runs per shape on throwaway probe PRs
  #269 (2 000 hooks) and #270 (20 000 hooks) — same commit, `main`'s code under
  the probe's bench scripts (both PRs closed unmerged, branches deleted). The
  two three-run tables, per-cell p99 round medians in every round, are in
  #268's body. Warm-up wall time per leg, from the gate's own timestamps (first
  warm-up line → first measured run): **full 81.8 / 82.6, 81.5 / 81.8, 82.3 /
  82.8 s; short 9.9 / 9.8, 11.5 / 11.5, 11.9 / 11.5 s** — per job 163–166 s →
  20–23 s, **2m23s–2m26s** of job 125809478781's 12m9s. Job totals on the day
  (runner speed varies by 2×, so the per-leg timestamps are the measurement):
  full 14m09s / 18m06s / 12m13s, short 10m45s / 9m57s / 16m10s; #268's own
  gates 11m31s (run 41689871611) and **9m28s** (run 41696475603, job
  126138233723) against the 12m9s reference.
  Worst round-median spread per cell over its three runs: short **512B 1.00×,
  10240B 1.14×, 512B-c32 1.19×, 512B-ordered 1.00×**; full 1.03×, 1.08×, 1.14×,
  1.00×. All four cells settled inside the 1.20× the limit can resolve in all
  six runs and all six printed `PASS`; the retry path fired in both shapes
  (512B-c32 on a slow runner: short 1.15× / 1.19×, full 1.14× / 1.11×), so the
  short warm-up adds no instability — the same policy, unchanged, absorbs it.
  `mise run bench:gate` locally runs with the 2 000-hook warm-up and exits 0
  (`4 cells inconclusive`: this laptop settles no cell under load, as CI-14's
  executor measured); `mise run lint:workflows` and `mise run lint:pins` pass.
  #268's one red job (`sdk conformance`, run 41696475603) is external:
  `org.clojure:clojure:jar:1.12.6` and `org.scala-sbt:sbt:2.0.10` unresolvable
  from Maven Central on that attempt; the same SHA's rerun is green in 2m52s.

- [x] **CI-21 — The tenant-fairness p99 fails again now that the suite is sharded** · P1 · Deps: CI-17
  *Why:* `Ankusa.Dispatch.IsolationTest` "tenant fairness" failed on `main` at `902e3dcb` (run 41689506568, `ankusa (tests, 4/4)` job 126104189792): `assert mixed_p99 <= max(2 * solo_p99, 25)`, `Result: 454/455 passed`, `Failed: 1 — per-tenant scheduling tenant fairness (Ankusa.Dispatch.IsolationTest) at test/ankusa/dispatch/isolation_test.exs:532` — the CI-8/#248 rewrite, which measured 20/20 green when the suite was one 2 080-test process. The test is `async: false` (isolation_test.exs:7) and pairs its solo and mixed measurements so ambient load cancels; what CI-17 changed is the *neighbour set* — shard 4/4 runs 455 tests in a different order, so a leftover process from an earlier test is background load the pairing cannot cancel (it inflates the mixed phase differently from the solo phase). One failure in the first ~10 shard-4 runs; CI-18's reporting change is what made the name visible at all.
  *Change:* attribute before touching the bound — `mixed_p99 <= max(2 * solo_p99, 25)` stays verbatim, and no retry may swallow a regression.
  1. Ten full `ankusa (tests)` runs of one commit (ten pushes to a probe branch) recording every printed `mixed_p99`/`solo_p99` pair per shard, so the distribution is known rather than one sample.
  2. Find the neighbour: replay shard 4 with the seed the step summary recorded (CI-18 wrote it on failure) and bisect which preceding test's leftovers move the mixed phase — a scheduler-utilisation / `Process.info(:processes)` snapshot before the drill is the cheap instrument.
  3. Fix the measurement, not the bound: the CI-8 heartbeat drops ≥25 ms host stalls, but a *continuously* busy neighbour is never a stall. Either make the drill immune (a paired idle baseline taken immediately before and after the measurement, subtracted from the light tenant's lag) or stop the neighbour (await its supervision tree) — say which and why.
  4. If a 4-way shard genuinely cannot host a latency drill, report that with the numbers and propose the smallest change to the shard assignment (e.g. an `--only`-tagged timing shard) — a plan decision, never a silent retry.
  *Done when:* ten consecutive shard-4 runs on the failing seed are green with the assertion unchanged, and a deliberate regression (`Process.sleep(40)` in dispatch's outcome handler — CI-8's own proof) still fails.
  *Proof:* the ten runs' ids with their `mixed_p99`/`solo_p99` pairs; the deliberate-regression output; `mise run check:package ankusa --partition 4/4`.
  *Anchors:* `packages/ankusa/test/ankusa/dispatch/isolation_test.exs` (the drill and its heartbeat), `.github/workflows/ci.yml` (`ankusa-test` matrix), `packages/ankusa/test/test_helper.exs`.
  *Evidence:* the shard is not the variable and the neighbour was not there:
  the excess was the store commit inside the window. Probe branch
  `probe/ci21-attribution` (run **41704242132**, throwaway, never merged) ran
  shard 4/4 ten times on ten runners with `--seed 628685` (the red run's seed,
  `max_cases: 8`) and split every sample at `Queue.enqueue/2`'s return: the
  commit, then the delivery. Every job printed
  `PROBE-CI21 SNAPSHOT before-drill processes=182 … instances=0 registry_keys=0`
  — no instance tree and no registry key left over from an earlier test — and
  `pauses=0` in every measured window. All ten were green: `455 passed
  (15 properties, 440 tests)`, 61–71 s, seeds and order identical to the red
  run, so the 108 ms p99 is a tail event rather than a reproducible order
  effect. The ten `mixed_p99`/`solo_p99` pairs, with each half's p99 (ms, over
  200 samples; "commit" is `enqueue!` returning, "delivery" is commit return →
  sink call):

  | run (job) | mixed | solo | mixed commit | mixed delivery | solo commit | solo delivery |
  | --- | --- | --- | --- | --- | --- | --- |
  | 1 (126155216884) | 1.009 | 0.887 | 0.597 | 0.536 | 0.637 | 0.293 |
  | 2 (126155216682) | 2.021 | 1.426 | 1.203 | 1.211 | 0.863 | 0.747 |
  | 3 (126155216978) | 1.706 | 1.099 | 1.036 | 0.934 | 0.642 | 0.533 |
  | 4 (126155216774) | 18.348 | 7.170 | **18.068** | 0.391 | **7.017** | 0.267 |
  | 5 (126155217132) | 1.464 | 1.403 | 0.758 | 0.902 | 1.088 | 0.626 |
  | 6 (126155217117) | 1.971 | 0.981 | 1.279 | 0.840 | 0.730 | 0.559 |
  | 7 (126155216727) | 1.592 | 1.487 | 0.838 | 0.911 | 1.179 | 0.628 |
  | 8 (126155216720) | 1.667 | 1.096 | 0.983 | 0.983 | 0.720 | 0.538 |
  | 9 (126155217229) | 1.695 | 1.184 | 1.094 | 0.829 | 0.762 | 0.540 |
  | 10 (126155217032) | 1.546 | 1.182 | 0.839 | 0.905 | 0.762 | 0.573 |

  The attribution is the commit in every row, and run 4 makes it unambiguous:
  18.068 of its 18.348 ms mixed p99 was the commit — whose max was **101.011 ms**
  — while its delivery p99 was 0.391 ms (max 1.232); solo's commit p99 was 7.017
  (max 28.055) beside a 0.267 ms delivery. The heartbeat recorded no pause in
  that window: a RocksDB write stall on a store holding 19 800 heavy-tenant rows
  beside continuous acks does not stop the VM, so it is not a stall the CI-8
  heartbeat can see. The red shard's `solo_p99 25.852` has the same shape — its
  store holds ~400 rows, so its own delivery cannot be 25 ms when ten same-shard
  runs measure 0.267–0.747 ms. Step 2's neighbour is disproved by the snapshot
  (0 live instances, 0 registry keys) and step 4's timing shard is not needed.

  *Change:* the window opens when `enqueue!/2` returns, so the store commit is
  outside it, and both halves are kept: the failure message now prints the
  commit p99 beside the delivery numbers. `assert mixed_p99 <= max(2 * solo_p99,
  25)` is textually unchanged, as are the 20-pair warm-up, the 200-pair window,
  both count assertions, the heartbeat and its 25 ms threshold; a sample whose
  *delivery* window overlaps a heartbeat pause is still dropped, and a delivery
  that beats the writer's reply counts as a zero, never negative. There is no
  neighbour tree to await; the drill is made immune to the store write instead,
  because that write is the store's cost, not the scheduler's fairness.
  `docs/testing.md` ("Dispatch, per tenant") says what the window spans and why;
  the test comment carries the red run's numbers.

  *Post-fix ten runs:* run **41705160596** (branch `probe/ci21-after`, same
  recipe and seed, window changed): **10/10 green** (`455 passed`, 57–72 s).
  Delivery p99 mixed/solo (ms): 0.841/0.598, 0.596/0.401, 0.927/0.509,
  0.955/0.510, 0.908/0.557, 1.178/0.747, 1.023/0.686, 0.476/0.290, 0.882/0.544,
  0.896/0.532 — every ratio inside the floor. The commits still spike:
  run 5's commit p99 was 25.908/9.522 ms (job 126158400573) and run 1's
  3.839/3.904, and those runs' delivery p99s stayed 0.908/0.557 and 0.841/0.598,
  which is the red failure's shape measured outside the window.

  *Proof it still bites:* a throwaway commit put `Process.sleep(40)` at the top
  of `Ankusa.Dispatch.Pipeline.outcome/4` (CI-8's proof). With the new window
  and `--seed 628685`: `light tenant p99 120.708 ms against 11.322 ms alone
  (delivery from the commit's return to the sink call; the commit p99 was
  18.464 ms mixed against 21.967 ms solo)`, `Result: 18/19 passed`. Unmodified,
  the same patch first trips the earlier invariant — `the heavy tenant never
  filled its window (15)`, because 40 ms in the outcome handler also starves the
  claim refill — and the p99 printed before that assert was 107.19 / 4.808.
  Reverted; never merged.

  *Final:* PR #272; `ankusa (tests, 4/4)` job 126176199402 green beside
  `ankusa (coverage)` in run 41710247867, and green again on the text-only plan
  push (`ankusa (tests, 4/4)`, `ankusa (coverage)` and the rest of the lane).
  Local: `mise run check:package ankusa --partition 4/4`
  green (export only, nothing else changed), `mise run lint:pins`,
  `mise run lint:workflows`, and ten file-level drill runs at `--seed 628685`
  green. `main` is red on CI-26 (`ankusa_kafka`), not on this item's site.

### Phase K — The toolchain, the shards and the gate's legs

- [x] **CI-22 — Every `mise run` installs the whole 19-tool monorepo toolchain, and one 500 from a tool download reddens the run** · P1 · Deps: —
  *Why:* mise loads `.mise/conf.d/*.toml` repo-wide, so the config declares 19
  tools in every directory, and its task runner auto-installs every missing
  declared tool before a task runs (`auto_install` / `task.run_auto_install`,
  default on). The package job's careful per-kind `install_args` (`elixir erlang
  cmake` for a Mix package, `go` for `sdk-go`, …) is therefore undone the moment
  `mise run check:package` starts. Measured on run 41700744695 (`07ea08fd`):
  `ankusa_server` job 126143230677 — the action reports `3/3 · installed 0 tools
  · 3 already installed in 194ms`, then at 19:11:09 `mise by @jdx – installing
  16 tools` completes 19:11:21 `installed 16 tools in 11.3s`; `ankusa_kafka`
  job 126143230821 — 16 tools in 14.5 s (18:44:07 → 18:44:19 of a 237 s job);
  `examples` job 126143230846 — 19 tools. Six jobs pass no `install_args` at all
  (`integration`, `conformance`, `examples`, `bench`, `bench-tenant`, `crash`:
  `ci.yml:768,828,874,1033,1101,1134`) and the action installs the whole set
  there by design. That download surface is also the flake: run 41689506568
  (`main` `902e3dcb`) `lint` job 126104074391 exited 1 in 30 s because
  `mise run lint:workflows` auto-installed 17 tools and `ruby@4.0.9` answered
  500 three times in 4 s (`github.com/jdx/ruby/releases/download/4.0.9-1/ruby-4.0.9.x86_64_linux.tar.gz`
  at 17:59:57.095, :57.235, :57.886, then `mise ERROR Failed to install
  core:ruby@4.0: HTTP status server error (500)`), while the job's own comment
  says it needs no Elixir and no SDK toolchain ("actionlint and promtool alone
  are a few MB"). mise's retry backoff is documented as roughly 200 ms, 1 s,
  4 s, 15 s with `MISE_HTTP_RETRIES` retries (default 3), so the whole budget
  fits in a 4 s window — shorter than a GitHub Releases 500 burst
  (https://mise.jdx.dev/configuration/settings.html, `http_retries`).
  *Change:*
  1. `MISE_AUTO_INSTALL: "false"` in `.github/workflows/ci.yml`'s top-level
     `env:` (every job inherits it), so `mise run` never installs anything the
     job's `install_args` did not. CI only: local `mise run check` keeps its
     on-demand installs.
  2. Complete every job's `install_args`: the five jobs above get exactly what
     their tasks use (`integration`, `bench`, `bench-tenant`, `crash`:
     `elixir erlang cmake`; `examples`: `elixir erlang cmake node uv rust helm
     kubectl kubeconform kind`). `conformance` is the exception and keeps a
     near-full set, spelled out rather than implied: `conformance/check.mjs`
     spawns every SDK's own toolchain (`node`, `uv`, `cargo`, `go`, `ruby`,
     `java` + `sbt`, `clojure`, `http:php` + `http:composer`) besides core's
     `elixir erlang cmake` — measure it from that file, do not copy the whole
     repo list (`helm`, `kind`, `kubectl`, `kubeconform` and `promtool` are not
     used there). A missing tool must fail at the command that needs it, not
     start a 19-tool download.
  3. `MISE_HTTP_RETRIES: "6"` beside it, so the downloads that remain (a
     package job's three tools) ride out a 5xx burst (~50 s of backoff) instead
     of reddening a run.
  4. `docs/testing.md`: one paragraph (CI runs with auto-install off and a
     longer retry budget; a task that needs a tool the workflow does not install
     fails loudly). `AGENTS.md`'s Commands row only if it claims `mise run`
     brings the toolchain.
  *Done when:* no CI job log contains a second `mise by @jdx – installing N
  tools` after its mise-action step (and `lint` contains none at all); the six
  previously-argument-less jobs log exactly their declared tools; a probe branch
  whose tool URL answers 500 shows 6 retry attempts with the documented backoff
  and then the failure, and the same probe with the server answering on the
  last attempt succeeds; every job's task is green.
  *Proof:* the before/after mise lines with timestamps for the three jobs above
  and for `lint`; the probe run ids; `mise run lint:workflows`,
  `mise run lint:pins`; a full green `main` run after the merge.
  *Anchors:* `.github/workflows/ci.yml` (top-level `env:`, `lint` job's
  mise-action step, the package job's `the package's toolchain` step, the six
  `uses: jdx/mise-action` steps without `install_args`), `.mise.toml`
  (`[tools]`), `.mise/conf.d/*.toml`, `docs/testing.md`, `AGENTS.md` (Commands).
  *Evidence:* PR #274. `env: MISE_AUTO_INSTALL: "false"` and
  `MISE_HTTP_RETRIES: "6"` at `ci.yml`'s top level; the six jobs that passed no
  `install_args` now name their set — `integration`, `bench`, `bench-tenant`,
  `crash`: `elixir erlang cmake` (on the `-hex` cache entry the `ankusa` package
  job keeps); `examples`: `elixir erlang cmake node uv rust helm kubectl
  kubeconform kind` (`check:examples` itself never shells out to `kind` or
  `kubectl` — it renders the fleet chart's values with helm — but the set is
  the one this item lists); `conformance`: `elixir erlang cmake node uv rust
  ruby go java sbt clojure http:php http:composer`, read off
  `.mise/tasks/check/conformance` + `conformance/sdks.json`. `docs/testing.md`
  gained "The job's toolchain, and nothing else (CI-22)"; `AGENTS.md`'s Commands
  rows make no claim about `mise run` bringing the toolchain, so they are
  unchanged. Measured on run **41715254649** (the PR's own run, every job green;
  `bench` and `bench-tenant` correctly skipped by `bench-scope`): 26 mise-action
  steps, **each logging exactly one `mise by @jdx – installing N tools` line —
  the action's own — and no job a second one** (run 41700744695 logged 32, two
  per job). `ankusa_server` `3/3 · installed 0 tools · 3 already installed in
  226ms` (was: a second `installing 16 tools` at 19:11:09.971 → `installed 16
  tools in 11.3s` at 19:11:21.251); `ankusa_kafka` `3/3 · 140ms` (was 16 tools
  in 14.5 s, 19:11:40.913 → 19:11:55.405); `examples` `10/10 · installed 1 tool ·
  9 already installed in 7.7s` (was `19/19`); `sdk conformance` `13/13 · 9.2s`;
  `integration`, `crash`, `tools` `3/3` in 197–283 ms; `lint` `2/2 · 136ms` and
  **no** second line at all (was `installing 17 tools` at 17:59:56.301 and
  `installed 16 tools · 1 failed in 11.3s` on ruby's 500, run 41689506568).
  `bench` and `bench-tenant` are outside that diff's scope, so they ran on
  throwaway probe PR #275 (closed unmerged, branch deleted): `perf gate` pass in
  8m22s (job 126192642505) and `bench-tenant` pass in 4m13s (job 126192642664),
  both `3/3 · installed 0 tools` with `MISE_AUTO_INSTALL=false` in the step env.
  Forced-500 probe (branch `probe/ci-22-mise-retries`, closed unmerged, deleted;
  run **41715076762**, mise 2026.10.12): a local server answers 500 to a one-file
  `http:` tool and then serves it. With `MISE_HTTP_RETRIES=6` and a URL that
  always answers 500 mise makes six retries — `attempt 1 … retrying in 173.1ms`,
  `attempt 2 … 804.0ms`, `attempt 3 … 2.6s`, `attempt 4 … 14.6s`, `attempt 5 …
  14.1s`, `attempt 6 … 9.4s` (20:42:56.425 → 20:43:28.660) — then fails at
  20:43:38.067 with `1/1 · installed 0 tools · 1 failed in 41.7s`, **41.7 s and
  7 requests**; with the default (`MISE_HTTP_RETRIES` unset) the same URL fails
  after `attempt 3` in **3.5 s** (4 requests). With the server answering 200 on
  the seventh request the same install **succeeds**: `http:probe@1.0.0  37.0s`,
  `1/1 · installed 1 tool`, and the tool runs (`probe-tool-ok`). The same probe with
  `MISE_AUTO_INSTALL=false`: `mise run lint:workflows` green with no `installing`
  line, and `mise run` on a task calling a declared but uninstalled tool fails at
  the command — `[needs-probe] $ probe-tool --version` / `sh: 1: probe-tool: not
  found` / `ERROR task failed`, exit 127, no download attempted.
  `mise run lint:workflows` and `mise run lint:pins` pass locally.
  `release*.yml`, `e2e.yml` and `soak.yml` keep their own mise config and are
  out of this item's scope.

- [ ] **CI-23 — Cost-balance the four shards: one shard carries 5.5× another's test time and is the run's critical path** · P2 · Deps: CI-17
  *Why:* `mix test --partitions K` assigns whole test *files* without knowing
  what they cost, so on run 41696475603 (`06875752`) the four shards finished
  their tests in **199.3 / 43.3 / 237.1 / 69.1 s** and their jobs in 269 / 92 /
  **314** / 142 s — a 5.5× spread in test time. On the last non-gate `main` run
  (41688133998) the shard jobs are 4.38 / 1.72 / 3.85 / 2.22 min and the slowest
  job of the whole run is a shard at **82 % of the 5.35 min wall clock**, while
  shard 2/4 finishes with 2.7 min to spare. Evenly divided the test time is
  137 s, so a shard's job would be ~210 s: **~100 s off every core-touching PR**
  (5.35 → ~3.7 min) with the same tests, the same 85 % floor and the same
  merged coverage. The imbalance is structural, not noise: a 137 s file
  (`test/ankusa/property/migrate_property_test.exs`, CI-9's measurement) lands
  whole in one shard, and CI-17 already found that a fifth shard buys nothing
  while the assignment stays hash-based.
  *Change:*
  1. Measure per file on the runner first (a probe branch with a per-file timing
     pass, or one `--slowest 200` run grouped by file) and put the table in the
     PR: the assignment must be made from measured cost, not from file size.
  2. Replace `MIX_TEST_PARTITION`/`--partitions` in `.mise/tasks/check/package`'s
     `--partition` form with an explicit assignment: a committed map
     (`packages/ankusa/test/.shard-map`, or a list in the task) from shard to
     test files, run as `mix test --cover --export-coverage <n> <files…>` —
     Mix's own export name, which `mix test.coverage` already merges.
  3. Add the **completeness guard**: the lane enumerates `test/**/*_test.exs`
     and fails naming any file that is in no shard or in two. A file silently
     missing from the map must be a red run, never a shorter suite.
  4. Balance against the measured file times; where one property file is the
     only reason a shard is long, split that file into two (same cases, same
     `max_runs`, same assertions) rather than lowering anything. If a single
     file alone exceeds the target, say so and accept that floor.
  5. `ankusa-coverage` stays as it is (four exports, the 85 % floor enforced
     once); update its comments and `docs/testing.md` + the AGENTS.md
     `check:package` row: how the map is maintained, and that the guard names a
     new file that is missing from it.
  *Done when:* on a run whose predecessor had 314 s for shard 3/4, the four
  shard jobs are within 15 % of each other and the slowest is ≤ 240 s; the
  shards' `Result:` counts sum to the single-process total (2 081 today, and
  the merge job proves coverage within 0.5 points of the single-process
  89.2 %); a probe branch that drops one file from the map fails the guard with
  that file's name; the 85 % floor still runs only in `ankusa (coverage)`.
  *Proof:* the before/after per-shard tables with run ids, the per-file timing
  table, the dropped-file guard probe (closed unmerged), `mise run
  check:package ankusa --partition 3/4` locally, `mise run lint:workflows`.
  *Anchors:* `.mise/tasks/check/package` (`--partition` block, the test lane's
  `mix test --cover`), `.github/workflows/ci.yml` (`ankusa-test` matrix,
  `PARTITIONS`, `ankusa-coverage`), `packages/ankusa/test/**/*_test.exs`,
  `packages/ankusa/mix.exs` (`test_coverage`), `docs/testing.md`, `AGENTS.md`
  (Commands).
  *Evidence:* —

- [ ] **CI-24 — Run the perf gate's two legs on two runners and judge in a third, so a hot-path PR stops paying them in series** · P2 · Deps: CI-20
  *Why:* after CI-17 and CI-20 a hot-path PR is wall-clocked by `perf gate`
  alone: 9m28s on run 41696475603, 11.7 min median over the five runs where it
  measured (max 16.2), against 5.35 min for the rest of the pipeline. The job is
  two independent measurements in series — setup 46 s, base leg (4 discarded
  warm-ups at 2 000 hooks, then 12 measured runs × 4 cells), branch leg (the
  same) — and the second waits for the first only because the base leg writes
  `$RUNNER_TEMP/base-baseline.json` for the branch leg to read. Both legs must
  stay on one runner class and neither may share a runner (they would contend
  and corrupt each other's samples), so the fix is two parallel jobs plus a
  verdict job, not two steps in one job.
  *Change:* `@gates` (1.2× p99, 0.8× throughput), the four cells,
  `BENCH_RUNS`, `BENCH_CONFIRM`, CI-20's warm-up size and CI-14's
  settle/omit policy are untouched.
  1. `perf gate (base)`: the existing base-leg step (checkout the base commit's
     sources and deps, `BENCH_REBASELINE=1 mise run bench:gate`), uploading
     `base-baseline.json` **and** its raw samples as artifacts.
  2. `perf gate (branch)`: its own runner, `mise run bench:gate` against the
     base baseline when the artifact is available (else the checked-in
     `baseline.json`, as today), uploading `last.json` and its samples. The
     confirm round lives here: measure the cells that decide, and when a cell
     is within the limit's headroom of failing, collect its confirm samples in
     the same job (the branch job knows its own numbers; the base is only
     needed for the verdict).
  3. `perf gate (verdict)`: downloads both, applies the same `evaluate/3`
     decisions (no new thresholds, no new pass), writes the step summary and
     fails on a resolvable regression or a missing artifact by name.
  4. `bench-scope` gates both legs; `ci-summary` lists the three jobs; the
     `bench-last` artifact keeps its name so docs and probes still find it.
  *Done when:* on a hot-path PR the wall clock from `perf gate (base)` start to
  `perf gate (verdict)` end is ≤ 60 % of the 9m28s–11m39s reference on the same
  runner class, with the same verdicts on the same commit; a deliberately
  regressed tree (`Process.sleep(20)` in `Ankusa.Edge.Ingest.ingest/2`, CI-14's
  proof) still fails, naming its cell; a missing baseline artifact fails the
  verdict job by name; a `main` push with no base commit still falls back to the
  checked-in baseline.
  *Proof:* the before/after job tables with run ids, the deliberate-regression
  run, the missing-artifact probe (closed unmerged), `mise run bench:gate`
  locally, `mise run lint:workflows`, `mise run lint:pins`.
  *Anchors:* `.github/workflows/ci.yml` (`bench` job's two legs, `bench-scope`,
  `bench-tenant`, `ci-summary`), `packages/ankusa/bench/gate.exs` (`evaluate/3`,
  `report/5`, `settle_spread/6`, `warmup/4`, `@gates`, `BENCH_CONFIRM`),
  `.mise/tasks/bench/gate`, `docs/testing.md` "Perf gate".
  *Evidence:* —

- [ ] **CI-25 — `ci-summary` prints the run, not its critical path: add the wall clock, the last job and the queue lead** · P2 · Deps: —
  *Why:* every measurement this loop makes starts with `gh run list` plus one
  `gh run view --json jobs` per run (30 calls) to answer three questions: how
  long the run took, which job ended last, and how long the queue held it.
  `ci-summary` already fetches the same jobs API and prints the table; the
  arithmetic is three lines. The lead is not decoration: with 33 jobs per run
  and eight runs in flight it reached 2–9.3 min (median 0.2, p90 1.5 in the
  2026-11-03 window), which on a 5.35 min pipeline is a visible share, and the
  shard imbalance above is only visible if the last job is named.
  *Change:* in `ci-summary`'s existing step, compute from the same `gh api
  …/jobs` response — first `started_at` to last `completed_at` as the wall
  clock, the job with the latest `completed_at` as the last to finish (a
  skipped leg never wins), and `created_at` of the run (`gh api
  repos/$REPO/actions/runs/$RUN_ID`, the same token) to the first `started_at`
  as the queue lead. Print one line above the table, e.g. `wall clock 5.4 min
  (queued 0.1 min; last to finish: ankusa (tests, 3/4) 5.2 min)`. Keep the
  table, the `changes` reason line and the failure block unchanged.
  *Done when:* a run with a skipped matrix leg and a run with every job show the
  right wall clock, lead and last-job name against a hand-computed `gh run view
  --json jobs`, and the job's exit condition is unchanged.
  *Proof:* the two summary pages quoted in the PR beside the hand-computed
  numbers; `mise run lint:workflows`.
  *Anchors:* `.github/workflows/ci.yml` (`ci-summary` job), `docs/testing.md`
  (the summary page, if documented there).
  *Evidence:* —

### Phase L — `main`'s kafka red

- [ ] **CI-26 — `ankusa_kafka`: a 2× suite-time tail, and `:unavailable` after the restart drills** · P1 · Deps: CI-12
  *Why (the tail):* the job is now the pipeline's critical path on a green `main`
  run: run 41712366064 (`7ba2866d`) finished in 6.67 min with `ankusa_kafka`
  **6.17 min** (job 126183566813) — slower than the slowest shard (5.28) — and
  its suite alone took **239.6 s** (`Result: 42 passed`) against **120.0 s** for
  the same 42 tests on another runner (job 126143230821, 3.95 min). The rest of
  that 6.17 min is setup: mise 20:25:43→20:26:18 (35 s, a chunk of it CI-22's
  redundant 16-tool install), `docker compose up` 18 s, deps/compile 34 s. So
  the variance is inside the suite — the broker stop/restart drills and their
  bounded waits, which stretch when a loaded runner re-forms the cluster slowly.
  The 2026-11-03 window's per-job medians agree: 3.93 min median over 27 runs
  with a 5.88 max, and 6.17 on the latest green `main` run.
  *Why (the flake):* run 41702699820 (`main` `e53e3d32`, CI-20's bench/docs
  merge) `ankusa_kafka` job 126150001619 failed **40/42**: `assert {:ok,
  %{position: 2}} = Kafka.publish_batch(…)` at `kafka_test.exs:455` and
  `… position: 0` at `:492`, both `right: {:error, :unavailable}`, each preceded
  by `GenServer #PID<0.3227.0> terminating ** (stop) {:reached_max_retries,
  :shutdown}` in `brod_producer_buffer.rebuffer_or_crash/3` with `Last message:
  {:DOWN, …, :shutdown}`. The test's own clients *had* connected
  (`:ankusa_intake_probe_… connected to localhost:29094`,
  `intake_kafka_… connected to localhost:29092` at 19:26:34.5) and
  `create_topic/1` had polled leader + ISR, so the cluster was not simply "down";
  with `max_retries: 0` (deliberate — kafka_test.exs:192: "produce that fails
  kills its producer") a single attempt against a leader/connection that is
  still re-forming reports unavailability instead of retrying. CI-12 (#246)
  fixed exactly this shape at *startup* with a bounded commit probe, and this is
  the same state entered mid-suite after the outage drills
  (`compose(["restart" | all])`, `kafka_test.exs:340-400`), where nothing
  re-probes. It is a flake rather than a standing red (it did not recur on
  `7ba2866d`), but not a one-off: CI-15's evidence found the same class once
  ("`ankusa_kafka` (`replay without a stamped partition … {:error,
  :unavailable}` after a brod producer crash)", run 41630250047 attempt 7) and
  deliberately did not paper over it.
  *Change:* the CI-12 rule — make the *wait* right, never the assertion:
  1. Attribute the tail first: run the suite on the CI runner twice, one of them
     under load, with per-test timings (a probe branch with ExUnit's `--slowest`
     or a `--trace`-free per-test log), and name the tests that carry the extra
     120 s. If it is a drill's bounded wait reaching its limit, say which
     signal took how long.
  2. Attribute the flake: ten reruns of the failing job on the same commit (or a
     probe branch), recording for each whether the failure follows a restart
     drill (`compose(["restart" | all])`), and the state at failure
     (`:brod.get_metadata` leader/ISR for the new topic, which broker the leader
     was on, the crashed producer's last batch). One sample is not a diagnosis.
  3. If the flake follows a restart: after every restart/stop drill, re-establish
     the produce path before the next test — a bounded (20 s) leader + ISR wait
     plus CI-12's commit probe on a topic of its own — in the drill's teardown,
     or lazily in `create_topic/1` when the helper sees a new restart generation.
     No retry inside a test body, and the probe keeps `max_retries: 0`.
  4. If the tail is a drill's wait: make the drill react to the same signal
     (controller elected, every replica in sync, a commit probe passed) instead
     of a fixed budget, so the fast case and the loaded case converge. If a
     broker genuinely needs the time under load, record the timings and hand it
     to the maintainer as a product finding; do not widen a test.
  *Done when:* twenty consecutive `mise run check:package ankusa_kafka` runs (or
  ten CI reruns — say which) are green with no `reached_max_retries` in any log;
  the failure still reproduces when the re-probe is stubbed out and a broker is
  restarted between tests; the suite's duration on a loaded runner is within
  20 % of its fast case (239.6 s → ≤ 145 s against the 120 s run), so the job
  stops being the run's last non-fence job; every assertion is unchanged.
  *Proof:* the rerun list with the log excerpt, the per-test timing tables (fast
  and loaded), the stubbed negative check, `docker compose up -d --wait`,
  `mise run check:package ankusa_kafka`, and a green `main` run after the merge
  with the job's duration beside 6.17 min.
  *Anchors:* `packages/ankusa_kafka/test/ankusa/intake/kafka_test.exs:180-200,340-400,450-500`
  (the `max_retries: 0` comment, `compose/1`, the leader/ISR waits,
  `create_topic/1`), `packages/ankusa_kafka/lib/ankusa/intake/kafka.ex` (the
  `:unavailable` mapping), `packages/ankusa_kafka/docker-compose.yml`,
  `.mise/tasks/check/package` (the `docker compose up -d --wait` step).
  *Evidence:* —

## Backlog (not yet an item)

- **SQS FIFO batch order** — `Ankusa.Sink.SQSIntegrationTest` "sqs batch order:
  one FIFO group arrives in the order its messages were admitted" failed once in
  the 2026-11-03 window (run 41575205472, `40/41 passed`): `left: ["m02", "m01",
  "m03", …]` against `["m01", "m02", …]` — the first two of one group swapped
  and every later pair in order. One sample decides nothing. Attribution first:
  run the test 20× on the CI runner (or in a loop with `floci`) and record the
  distribution; if the batcher can emit two flushes for one lane out of order
  that is a *product* finding and leaves this loop's scope (this loop fixes
  measurements, not ordering guarantees); if the harness's own batching is what
  reorders, fix the wait.
- **Runner concurrency during a burst** — ~25 jobs per run against the account's
  concurrent-job limit, now **33 jobs** after CI-17's four shards and the merge
  job: with eight runs in flight the queue lead went to 2–9.3 min (median 0.2,
  p90 1.5 over 26 runs), and one `main` push waited 9.3 min before its first job
  started. Not an item yet, and CI-25 now prints the lead on every run's
  summary so the next measurement is free. Candidates if it becomes one: folding
  the nine single-minute SDK jobs into three, and dropping `bench-scope` into
  `changes`. Note that CI-22 and CI-23 both shrink job *time*, which is the
  other half of the same problem.
- **`lint` failed on a mise download** — **promoted to CI-22**, which measured
  the cause: the job does not need ruby at all, and `mise run` was installing 17
  tools for it (`main` 41689506568, job 126104074391).
- **`ankusa_kafka` setup, after CI-22** — job 126143230821 decomposes as 44 s
  before compose (checkout + mise, ~12 s of it CI-22's redundant toolchain),
  21 s `docker compose up` (five containers), 35 s deps/compile, 120–240 s
  suite, 15 s teardown. With CI-22 removing the toolchain install, the setup is
  ~85 s and the suite is the variable — **promoted to CI-26** together with the
  `:unavailable` flake, because on the latest green `main` run the job (6.17 min)
  outlasted every shard.
- **`Ankusa.BlobStore.PoolTest`** — one local observation (`ankusa (tests)`,
  2026-11-03, a pool-timing test): fails in a full run, passes in isolation and
  passed in the CI lane on the same commit. Not enough samples to attribute;
  collect ten full-lane runs before touching it.
- **`ci-summary` as the measuring tool** — **promoted to CI-25**.
- **`mix docs --warnings-as-errors`** — closed, not a pipeline defect: the one
  failure in the 2026-11-03 window (run 41575322460, `ankusa (static)`) is
  `warning: documentation references module "Ankusa.Application" but it is
  hidden`, from the `docs/elixir` PR's own doc edit. If it recurs on `main`,
  reopen it with the offending doc line.

## Note for the maintainer (no action from this loop)

`main` has **no branch protection** — `gh api
repos/jamescarr/ankusa/branches/main/protection` returns 404 and
`/rulesets` returns `[]` (re-checked 2026-11-03). Nothing forces a merge to wait
for CI and no check is required, so a red `main` is only visible in the run
list, and every agent here merges on its own judgement of the green checks.
Adding protection would change the merge semantics this loop and the feature
loop rely on (a required-check list, a merge queue, or "branches must be up to
date"), so it is a decision for you, not a patch from this loop. The related
measurement: in the 2026-11-03 window **2 of 7 `main` pushes were red** (the
perf gate's own refusal on `77e22b6f`, run 41586954095, and the Kafka readiness
flake before #246 landed), and nothing in the repository makes a red `main`
visible beyond the run list — a required check on the two lane jobs would have
shown both on the PR that caused them, where the cost is a rerun instead of a
bisect.

## Verification of this plan

- `grep -cE '^- \[[ x]\] \*\*CI-' CI_PIPELINE_PLAN.md` → 26 (the number of
  items; fewer ticked as they land, and the count matches the `Status:` line's
  denominator).
- Every *Anchors* path exists at the commit the item is executed on (re-read the
  file: line numbers are hints, not contracts).
- Each executed item's PR carries the measured before/after numbers and the run
  ids, and ticks its box with an *Evidence* line in the same PR.
