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

Status: 5/6 done · last updated: 2026-11-02

Format: `- [ ] **ID — Title** · priority · Deps`, then *Why* (with the measured
numbers), *Change*, *Done when*, *Proof*, *Anchors*, *Evidence* (filled by the
executor in the same PR that ticks the box). **P1** is executed first; within a
priority, ID order.

### Phase A — Stop paying twice

- [ ] **CI-1 — One writer per cache key: stop the `ankusa` job recompiling its deps on every run** · P1 · Deps: —
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
  *Evidence:* —

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
  *Evidence:* PR #217. Cause: the runner's disk, not an accumulated store —
  `core_bench` already made a fresh data dir per cell (`0 B before` each with
  the new `BENCH_TRACE=1`); `p50_ms` stayed within a few percent while `p99_ms`
  spanned 4–10× inside one leg (base `512B` 155.56/177.99/195.47 ms, branch
  `512B` 22.20/149.55/202.08, `512B-ordered` 44.10–186.70 after the confirm
  round), because an ingest commit is `Store.write(..., sync: true)` and one
  group commit carries up to 1 024 hooks, so the slowest fsyncs *are* p99: in
  one base leg `-ordered` was 7× faster than `512B` with the same p50 for that
  reason alone. Fix: `gate.exs` gives every run its own `TMPDIR` under
  `/dev/shm` (7.8 GiB free on the runner; `BENCH_DATA_ROOT` overrides) and logs
  the path and size before/after each cell; `HOT_PATHS` no longer lists
  `ci.yml`. Guard: a cell whose three runs span >1.2× is measured once more
  (samples added), and if the two rounds' medians still span >1.2× the run
  fails as `INCONCLUSIVE p99_ms <cell>` with its samples (base leg included;
  never a pass, never a regression). Three consecutive `perf gate` runs of
  `37bef1fb` (jobs 125595490540, 125611013439, 125616200581): all green, every
  gated delta within ±7.8 % (p99 +6.2/−3.4/−5.0/+1.7, +7.8/−1.9/+6.2/+4.9,
  +5.1/+2.5/+1.3/−1.1; ingest within ±5.2 %), within-leg p99 spread ≤1.20×
  except two cells re-measured by the guard. Proof: throwaway PR #218 (closed
  unmerged, branch deleted) added `Process.sleep(1)` to
  `Ankusa.Edge.Ingest.ingest/2` and the gate **failed**:
  `FAIL ingest_per_s 512B-c32: 10744.35 < 0.8×16711.76` with `p50_ms +78.9 %`
  and every other cell inside its limit. A `ci.yml`-only PR (#221, closed,
  branch deleted) reported `no hot-path file touched: gate skipped`
  (`run=false`). `mise run lint:workflows` passes; `mise run bench:gate`
  locally reports the laptop's fsync-bound numbers (three cells INCONCLUSIVE,
  `512B` FAIL on both gates), not a verdict.

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
  `.mise/tasks/lint/pins`, `.mise/lib/pkg.sh` (`pkg_kind`).
  *Evidence:* PR #222. Warm run **41533665431 attempt 2** (green): six of the
  seven image-pulling jobs log `images: loaded N, pulled 0, saved 0` with no
  `Pulling` line at all (`ankusa_nats` 2, `ankusa_rabbitmq` 2, `ankusa_kafka`
  5, `core integration` 8, both `crash` drills 1 ref); attempt 1 was cold for
  `ankusa_nats`/`ankusa_rabbitmq`/`core integration` and each pulled once,
  saved its entry and passed. **Forced cold run:** deleted
  `images-v1-Linux-X64-ankusa_redis-64a8…` (44 MB), `gh run rerun
  41533665431` → attempt 2 logs `images: loaded 0, pulled 2, saved 2`,
  re-created the entry and passed. What the cache replaces, per job (pull
  window in baseline run 41531241640 → the `load` step in attempt 2):
  `ankusa_redis` 13.1 s → 4, `ankusa_rabbitmq` 13.9 → 3, `ankusa_kafka`
  18.6 → 11, `core integration` 10.9 → 10, `crash` ENOSPC 9.2 → 2,
  `crash` power-loss 2.4 → 1, `ankusa_nats` 2.7 → 1 — ~38 s a run in total,
  and the 7 job durations differ by ±15 s run to run, so what this removes is
  the `toomanyrequests` failure class (11 of 21 failed jobs in 30 runs), not a
  provable 10 s per job. Entries: integration 385 MB, kafka 316, rabbitmq 97,
  crash 64, redis 44, nats 18 = **924 MB**, so every job keeps its entry
  (§5's cut not needed). `lint:pins` (the new compose rule fails a
  hand-unpinned `image:`) and `lint:workflows` pass; `check:package
  ankusa_redis`, `check:package ankusa_kafka` and `test:integration` pass
  locally against the pins, and all four compose files' `up -d --wait` ran
  against them.

### Phase C — Spend less per push

- [ ] **CI-4 — `concurrency:` on `ci.yml`: cancel a superseded PR run instead of running the whole pipeline twice** · P1 · Deps: —
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
  *Evidence:* —

- [ ] **CI-5 — A `changes` job that skips the package jobs a diff cannot have touched, with `main` always running everything** · P2 · Deps: CI-1, CI-3, CI-4
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
    outputs `docs_only`, the package list (filter `pkg_names`), and the booleans
    `sdk_or_conformance`, `examples`, `tools`, `hot_path`, `needs_docker`. For a
    non-PR event (push to `main`, `workflow_call`, a release tag) every output is
    "run everything". Fail-safe: any error inside the step prints the all-true
    JSON and exits 0.
  - `if:` on the `package` matrix, `integration`, `conformance`, `examples`,
    `tools`, `bench`, and the two crash jobs, keeping their existing scope rules
    on top (the `bench` job's `HOT_PATHS`, the drills' own paths).
  - A `ci-summary` job (`needs: [changes, …], if: always()`) that fails the run
    when a skip's reason is "the changes job failed", so a broken filter is loud
    rather than silent.
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
  *Evidence:* —

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
  *Evidence:* PR #224. Run 41550052881 (both lanes green on their first run):
  `ankusa (static)` **1:19** — dialyzer `Total errors: 0`, docs and
  `hex.build` included — and `ankusa (tests)` **9:38** (suite 8:47), against
  the 10.4 min single `ankusa` job of run 41531241640. Wall clock (first job
  start → last job end) **9:46**; the gate skips a diff with no hot-path file.
  Both lanes warmed on that first run through their second restore prefix
  (`Cache hit for restore-key: mix-v19-Linux-X64-ankusa-…`, the pre-split
  full tree); on the merged head (run 41554012789) each restored its own key
  instead (`mix-v19-…-ankusa-test-…`): `ankusa (static)` **1:17**,
  `ankusa (tests)` **8:41** red on one pre-existing latency assertion
  (`Ankusa.Edge.FleetTest`, 213 ms against a 200 ms budget) and **7:07** green
  on `gh run rerun --failed`. The test lane is suite-bound — ~40 s of preamble
  plus `mix test --cover` — so its floor tracks the suite, not the split.
  Locally: `--lane static` 1:09; `--lane test` green (seed 126436); the bare
  `mise run check:package ankusa` green (seed 982202, 21:08 on a loaded
  laptop: both envs warmed, every step in the original order, same package
  checksum as main's). Two earlier local runs failed only on the known
  `Ankusa.Dispatch.IsolationTest` flakes. `mise run lint:workflows` and
  `lint:pins` pass.

## Backlog (not yet an item)

- **The four real test failures** — `ankusa_kafka` 39/40 twice
  (`Ankusa.Intake.KafkaTest`, `:econnrefused`, a broker-readiness race in the
  package's compose fixture), `ankusa` 1958/1959, `ankusa_redis` 82/83: name the
  failing test from the run logs and fix the cause in the fixture or the test
  (never by loosening an assertion).
- **`mix docs --warnings-as-errors` in `ankusa_redis`** — 2 of 21 failures; the
  log says a warning in the generated docs, which is a signal bug worth its own
  diagnosis.
- **Perf gate cost** — 9.4 min median, 12.8 p90; inside it, 12 bench runs per
  side (~3:20) + tenant benches (2:33) + two full compiles. After CI-2 makes the
  numbers trustworthy, look at the order of the legs and at moving the
  self-asserting tenant benches to their own job. Do not touch the comparison
  rule (base measured on this runner).
- **ExUnit partitioning** — `mix test --partitions` cuts the 6–8 min suite, but
  `--cover` with a coverage floor per partition needs a merged-coverage step;
  research `mix test --cover --partitions` semantics first.
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

- `grep -c '^- \[ \] \*\*' CI_PIPELINE_PLAN.md` → 6 unchecked items (fewer as
  items land).
- Every *Anchors* path exists at the commit the item is executed on (re-read the
  file: line numbers are hints, not contracts).
- Each executed item's PR carries the measured before/after numbers and the run
  ids, and ticks its box with an *Evidence* line in the same PR.
