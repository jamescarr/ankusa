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

*Current state* (measured on `main`, run 41549538574, after CI-1/CI-2/CI-3/CI-4):
**47.9 runner-minutes, 10.6 min wall clock** — `perf gate` 10.6 min, `ankusa`
10.1 min, `ankusa_kafka` 3.1 min, every other job under 1.6 min. Against the
baseline above that is −65 % wall clock (30.0 → 10.6) and −24 % runner-minutes
(63 → 47.9 with the gate, 53 → 40 without). The two long jobs now hold 20.7 of
the 47.9, and 24 jobs still start on every diff however small — which is why
CI-5 and CI-7 are next. Re-measure after each lands; do not quote these as
current after a change to the job graph.

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

## Task list

Status: 7/10 done · last updated: 2026-11-02

Format: `- [ ] **ID — Title** · priority · Deps`, then *Why* (with the measured
numbers), *Change*, *Done when*, *Proof*, *Anchors*, *Evidence* (filled by the
executor in the same PR that ticks the box). **P1** is executed first; within a
priority, ID order.

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
  *Evidence:* PR #231. Three probes, each a branch into the PR's own base and
  closed unmerged: docs-only PR #232 (run 41574599194 — `changes`, `lint`, the
  perf gate's scope step and `ci-summary` ran, every other job `skipped`, 0:36
  wall clock, under one runner-minute); `packages/sdk-go`-only PR #233 (run
  41574117567 — `sdk-go`, `lint`, `changes`, `ci-summary`; the rest `skipped`);
  core hot path PR #234 (run 41574120125 — the whole Mix group, `core
  integration`, `sdk conformance`, both crash drills and `examples` ran exactly
  as before; `tools` skipped). The PR's own run takes the all-true path — its
  diff is `.github/workflows/ci.yml` plus docs — and runs every job.
  `mise run lint:workflows` and `mise run lint:pins` passed in session; the
  first push to `main` after the merge is recorded in the PR.

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

- [ ] **CI-10 — `bench-tenant` must run *beside* the perf gate, not after it** · P1 · Deps: CI-7
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
  *Evidence:* —

### Phase E — Make a single-test flake stop costing a pipeline

- [ ] **CI-8 — Three flaky tests, not one assertion: make their *measurements* deterministic without touching what they assert** · P2 · Deps: —
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
  *Evidence:* —

### Phase F — The last wall-clock lever: the suite itself

- [ ] **CI-9 — Profile the core suite and cut the test lane's wall-clock time** · P2 · Deps: CI-6
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
  *Evidence:* —

## Backlog (not yet an item)

- **Flaky tests that block merges** (each hit costs a full rerun; the `ankusa`
  job is one of the two longest, so a rerun is ~10 min):
  - `Ankusa.Dispatch.IsolationTest` "per-tenant scheduling tenant fairness"
    (`test/ankusa/dispatch/isolation_test.exs:437`): `assert mixed_p99 <=
    max(2 * solo_p99, 25)` failed with `light tenant p99 62.843 ms against
    1.159 ms alone` on run 41532198386; the same job has failed `1958/1959` and
    `1960/1961` on other days. It is a latency assertion inside a loaded
    runner. Fix the *measurement* (a longer probe window, a warm-up, a
    steady-state sample) — never the bound.
  - `Ankusa.Intake.KafkaTest` "earliest, assign_partition and ready?"
    (`ankusa_kafka` 39/40 twice, `:econnrefused` to 127.0.0.1): a broker
    readiness race in the package's compose fixture; fix the wait, not the test.
  - `ankusa_redis` 82/83 (once) — the failing test still needs naming.
- **`mix docs --warnings-as-errors` in `ankusa_redis`** — 2 of 21 failures; the
  log says a warning in the generated docs, which is a signal bug worth its own
  diagnosis.
- **ExUnit partitioning** — only worth it with *merged* coverage: `mix test
  --cover --partitions K` writes K `cover/` trees, so every shard fails the
  85 % floor, and shards-plus-one-full-coverage-job leaves the long job on the
  critical path. Doing it properly means ex_coveralls-style merging (a new dev
  dependency in `packages/ankusa`) or a merged `.coverdata` step — evaluate only
  if CI-9's profile shows the suite is still the critical path afterwards.
- **Pipeline report** — a `ci-summary` job writing per-job durations to
  `$GITHUB_STEP_SUMMARY` so the next measurement needs no API poking.
- **`discover` hop** — 0.1 min but serial ahead of every package job; fold it
  into `changes` when CI-5 lands.
- **Branch protection** — `main` has none (`gh api
  repos/jamescarr/ankusa/branches/main/protection` → 404; `/rulesets` → `[]`).
  Nothing forces a merge to wait for CI, and no check is required. This is an
  observation for the maintainer, not an item: adding protection changes the
  merge semantics every agent here relies on, so it needs a decision, not a
  patch from this loop.
- **Runner class** — larger/arm64 runners are a cost decision, not a speed one
  for the critical path until `ankusa` is a single verified number.

## Verification of this plan

- `grep -cE '^- \[[ x]\] \*\*CI-' CI_PIPELINE_PLAN.md` → 10 (the number of
  items; fewer ticked as they land, and the count matches the `Status:` line's
  denominator).
- Every *Anchors* path exists at the commit the item is executed on (re-read the
  file: line numbers are hints, not contracts).
- Each executed item's PR carries the measured before/after numbers and the run
  ids, and ticks its box with an *Evidence* line in the same PR.
