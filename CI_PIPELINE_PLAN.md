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

Status: 1/6 done · last updated: 2026-11-02

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
  *Evidence:* PR #213. `ankusa` job **33.5 min cold** (run 41506556215, which
  saved `mix-v18-…-ankusa-…`, 59 MB) → **8.3 min warm** (run 41512168322,
  `Cache restored from key: mix-v18-…-ankusa-…`, then `==> third-party deps up
  to date` at 19:43:57 for dev and 19:43:59 for test; `mix test --cover`
  green, `sdk conformance` green). `sdk conformance` restored its own
  `…-ankusa-conformance-…` entry, logged `==> third-party deps up to date`, and
  finished in 2.5 min. The entry is 59 MB, far under the 2 GB threshold, so
  `path` still carries `deps` and `_build`. The package job's key spans the
  whole matrix, so the version bump also re-keyed the other packages' entries:
  each paid one cold compile and saved its own v18 entry in run 41506556215,
  and all were green and warm in 41512168322. `mise run lint:workflows` and
  `mise run lint:pins` pass.

### Phase B — Make red mean something

- [ ] **CI-2 — Perf gate: make the base and branch legs comparable, so a diff that cannot change the runtime cannot fail the gate** · P1 · Deps: —
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
  *Evidence:* —

- [ ] **CI-3 — Pull every CI image from a GHCR mirror, pinned by digest: remove the 52 % share of failures** · P1 · Deps: —
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
  per run: `floci/floci{,-gcp,-az,-oci}:latest` (4, unpinned — a moved `latest`
  silently changes the integration suite), `amazon/aws-cli:2.38.1`,
  `curlimages/curl:8.23.0` (×3), `apache/kafka:3.9.1`, `nats:2-alpine`,
  `rabbitmq:4-management-alpine`, `redis:8-alpine`, `elixir:1.20.4-alpine@sha256:…`
  (the crash drills take it from `packages/ankusa_server/Dockerfile`'s `FROM`),
  and `moby/buildkit` in `docker.yml`. GHCR is part of the same infrastructure
  as Actions (free, authenticated by `GITHUB_TOKEN`, no third-party limit) and
  `docker.redpanda.com` + `ghcr.io/shopify/toxiproxy` already prove it: no job
  has ever failed on those pulls.
  *Change:*
  - New `.github/workflows/mirror-images.yml`: `on: schedule` (daily),
    `workflow_dispatch`, and `push` to itself; `permissions: packages: write,
    contents: read`; one matrix over `.github/mirror-images.txt`
    (`<source-ref> <ghcr-name>` per line), each step
    `docker buildx imagetools create -t ghcr.io/jamescarr/ankusa/mirror/<ghcr-name>:<tag> <source-ref>`
    (registry-to-registry: no layer crosses the runner), then
    `docker buildx imagetools inspect --format '{{json .Manifest.Digest}}'`
    written to the job summary so the digests are copy-pasteable.
  - Point every consumer at the mirror **by digest**:
    `packages/ankusa/docker-compose.integration.yml`,
    `packages/{ankusa_redis,ankusa_rabbitmq,ankusa_nats,ankusa_kafka}/docker-compose.yml`,
    `packages/ankusa_server/Dockerfile` (`elixir`, mirror repository + the digest
    it already carries, so `scripts/drills/crash.sh`'s `FROM` regex follows
    automatically) and `docker.yml`'s `moby/buildkit` digest. Leave
    `docker.redpanda.com` and `ghcr.io/shopify/toxiproxy` alone.
  - Registry auth: each job that pulls replaces its `Log in to Docker Hub` step
    (four in `ci.yml`, they go away) with `docker/login-action` and
    `registry: ghcr.io`, `username: ${{ github.actor }}`,
    `password: ${{ github.token }}` — the one action already pinned here, so
    `lint:pins` needs nothing new. A fork PR gets a read-only `GITHUB_TOKEN`,
    which is all a pull needs.
  - Package visibility: packages created by `GITHUB_TOKEN` are private, and the
    compose files are also used locally. Make them public (`gh api --method
    PATCH /user/packages/container/<name> -f visibility=public`; if the CLI
    token lacks the scope, record the exact one-time click path in the PR body
    and add the `docker login ghcr.io` line for local runs to
    `docs/testing.md`). CI authenticates either way, so no *CI* step is manual.
  - `.mise/tasks/lint/pins`: every `image:` in `packages/*/docker-compose*.yml`
    and every source in `.github/mirror-images.txt` must carry
    `@sha256:<64 hex>` (the rule already applied to the Dockerfile), so a mirror
    line and its consumer cannot drift.
  - `docs/testing.md` (local infra) and the `AGENTS.md` Commands table note where
    the images come from.
  *Done when:* the mirror workflow has run and its summary lists a digest per
  image; every compose file and the Dockerfile name a GHCR mirror with a digest;
  `mise run test:integration` and `mise run check:package ankusa_redis` pull from
  the mirror (name it in the log) and pass; `mise run lint:pins` and
  `mise run lint:workflows` pass; a full `ci.yml` run on the PR is green with no
  Docker Hub pull in any job log.
  *Proof:* `gh workflow run mirror-images.yml` then `gh run view <id> --log`
  (digests); `mise run lint:pins`; `mise run test:integration`; on the PR
  `timeout 1800 gh pr checks <N> --watch --fail-fast` and
  `gh api …/jobs/<id>/logs | grep -c 'from docker.io'` = 0.
  *Anchors:* `.github/workflows/ci.yml` (the four `Log in to Docker Hub` steps
  and every `docker compose up`), `.github/workflows/docker.yml:129-160`,
  `packages/ankusa/docker-compose.integration.yml`,
  `packages/*/docker-compose.yml`, `packages/ankusa_server/Dockerfile:16,53`,
  `scripts/drills/crash.sh:21-30`, `.mise/tasks/lint/pins`,
  `.mise/tasks/check/package`, `.mise/tasks/test/integration`.
  *Evidence:* —

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
  change. A skipped job reports `skipped`, which satisfies branch protection for
  a required check — but only if the *workflow* runs, so the filtering must stay
  at job level and every job must be able to prove it may skip.
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

- [ ] **CI-6 — Split the `ankusa` job into a static-analysis lane and a test lane, each with its own cache key** · P2 · Deps: CI-1
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
  *Evidence:* —

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
- **Runner class** — larger/arm64 runners are a cost decision, not a speed one
  for the critical path until `ankusa` is a single verified number.

## Verification of this plan

- `grep -c '^- \[ \] \*\*' CI_PIPELINE_PLAN.md` → 6 unchecked items (fewer as
  items land).
- Every *Anchors* path exists at the commit the item is executed on (re-read the
  file: line numbers are hints, not contracts).
- Each executed item's PR carries the measured before/after numbers and the run
  ids, and ticks its box with an *Evidence* line in the same PR.
