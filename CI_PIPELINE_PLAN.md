# CI pipeline plan: faster, cheaper, more reliable

## Context

The ask: keep improving this repository's build/CI pipeline — faster, cheaper,
more reliable, better signal — without loosening a single gate. A planner loops:
measure the pipeline, research the bottleneck, add the next 2–5 items here,
hand one item at a time to an executor (own worktree, branch `ci/<ID>-<slug>`,
one improvement per PR), verify the measured delta after the merge, repeat.

Scope: `.github/workflows/*`, `.github/actions/*`, `.mise/tasks/*`, `.mise.toml`,
`.mise/conf.d/*`, `packages/ankusa_server/Dockerfile` and image build/caching,
dep/PLT/`_build` caching, the job graph, matrix shape, runner choice, flake
handling, artifact/step-summary reporting, supply-chain pinning (`mise run
lint:pins` must keep passing: every action a commit SHA, every release-image
input a digest), `hex.audit`, release workflows, and the `docs/testing.md` /
`AGENTS.md` rows for anything changed. Product code, SDK source and the
examples' application code are out of scope, and every change must keep
`mise run check` as the single local entry point.

**What may not change:** what a gate asserts. Coverage floors, dialyzer,
`credo --strict`, sobelow, both crash drills, the perf gate, conformance and the
pin lint keep their current strictness. A change that makes a job faster by
checking less is wrong, however small.

## Measurement

The numbers below come from the last 30 `ci.yml` runs (2026-10-26 … 2026-10-31),
job-level, via the API. Re-measure with:

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
| | | | | `tools` | 0.7 | 1.2 |
| | | | | `sdk-typescript` | 0.6 | 0.6 |
| | | | | `lint` | 0.5 | 0.6 |
| | | | | `discover packages` | 0.1 | 0.1 |

- **Wall clock per PR: 30.0 min median, 33.1 min p90** — that is the `ankusa`
  job plus ~1 min of queueing; every other job is done before it.
- **Runner-minutes per pipeline: 53** (perf gate skipped, 14 of 30 runs) to
  **63** (gate run). The `ankusa` job alone is 30.0 (48–57 %).
- **Failure rate: 12 of 30 runs red (40 %)**, 21 failed jobs, by cause:
  - **11 — Docker Hub unauthenticated/account rate limit**
    (`toomanyrequests: You have reached your unauthenticated pull rate limit`)
    in `ankusa_redis`, `ankusa_rabbitmq`, `ankusa_nats`, `core integration`, and
    both crash drills. Two clustered runs (2026-10-28 04:41 and 04:55) account
    for 8 of them.
  - **4 — `mix hex.audit` advisories** published 2026-10-31
    (`ssl_verify_fun` in `ankusa_server`, `phoenix_html` in `examples`): known
    environmental, being fixed by the lock bump.
  - **4 — one failing test**: `ankusa` 1958/1959, `ankusa_redis` 82/83,
    `ankusa_kafka` 39/40 twice.
  - **2 — `mix docs --warnings-as-errors`** in `ankusa_redis`.

### The `ankusa` job, in phases (three runs, same shape)

| phase | run A | run B | run C |
| --- | --- | --- | --- |
| dev-env deps compile | 6:20 | 7:27 | 8:44 |
| test-env deps compile | 5:47 | 7:09 | 8:28 |
| compile + credo + PLT + dialyzer + sobelow | 2:30 | 3:00 | 3:15 |
| `mix test --cover` | 6:24 | 8:01 | 8:14 |
| docs + `hex.build` | 0:15 | 0:15 | 0:20 |

**48–56 % of the job is dependency compilation that a warm cache removes**, and
the cache is never warm — see CI-1.

## Task list

Status: 0/5 done · last updated: 2026-11-02

Format: `- [ ] **ID — Title** · priority · Deps`, then *Why* (with the measured
numbers), *Change*, *Done when*, *Proof*, *Anchors*, *Evidence* (filled by the
executor in the same PR that ticks the box). **P1** is executed first.

### Phase A — Stop paying for the same work

- [ ] **CI-1 — One writer per cache key: stop the `ankusa` job recompiling its deps on every run** · P1 · Deps: —
  *Why:* the `ankusa` job compiles its Hex deps twice — once in `dev`, once in
  `test` — on **every** run, 12:07 / 14:36 / 17:12 min in the three runs
  sampled above, ~50 % of a 30.0 min median job and ~95 % of a PR's wall clock.
  It is not a cache miss: `actions/cache` reports `Cache hit for:
  mix-v17-Linux-X64-ankusa-<mise.toml>-<mix.lock>` and restores 40 MB, then the
  log shows `* Getting bandit (Hex package)` for every dep and
  `==> third-party deps compiled` in both envs. The cause is that **two jobs
  save one key**: the package job (`ci.yml`, `cache mix deps`, key
  `mix-v17-…-ankusa-…`) and the `sdk conformance` job (key `mix-v17-…-ankusa-…`,
  "whichever job saves first wins"). Conformance finishes in 2.4 min and saves a
  test-env-only tree; actions/cache never re-saves a key that exists, so the
  package job's full `dev`+`test`+`prod` tree is *never* stored and `dev` in
  particular is compiled from source (RocksDB's C++ NIF, ~8 min on a runner)
  every single time. The mechanism works when a key has one writer:
  `ankusa_redis` (single-writer `mix-v17-…-ankusa_redis-…`) restores and logs
  `==> third-party deps up to date` in both envs, 25 s each.
  *Change:*
  - `.github/workflows/ci.yml`, package job: bump the key version
    `mix-v17-` → `mix-v18-` (and the `restore-keys` prefix) so the stale
    conformance-written entries are invisible and the package job creates fresh
    ones. Replace the "whichever job saves first wins" comment with the
    invariant: **exactly one job saves a given cache key; every other job
    restores it by prefix.**
  - `.github/workflows/ci.yml`, `conformance` job: its own namespace —
    key `mix-v18-${{ runner.os }}-${{ runner.arch }}-ankusa-conformance-${{ hashFiles('.mise.toml') }}-${{ hashFiles('packages/ankusa/mix.lock') }}`,
    `restore-keys` first its own prefix, then
    `mix-v18-${{ runner.os }}-${{ runner.arch }}-ankusa-${{ hashFiles('.mise.toml') }}-`
    so it still restores the package job's (now fuller) tree.
  - Keep every other Mix key as it is (`mix-v5-…-examples-…`, `-ankusa-integration-`,
    `-ankusa-bench-`, `mix-v17-…-<pkg>-` for the other packages): each already
    has one writer.
  - Measure the cache entry size after the first main run
    (`gh api repos/jamescarr/ankusa/actions/caches`) and record it in the PR; if
    it exceeds ~2 GB, drop `packages/ankusa/deps` from `path` and keep `_build`
    only (deps are re-fetched in ~20 s and `_build` is what Mix trusts), and say
    so in the PR body and the comment.
  - If a later run is ever suspected of having saved a poisoned (partial) entry
    under the new key, bump the version again (`mix-v18-` → `mix-v19-`); the
    comment in the step documents that this is the escape hatch, because
    actions/cache never re-saves an existing key.
  *Done when:* a CI run's `ankusa` job log shows `==> third-party deps up to
  date` for the **dev** env and for the **test** env (not
  `third-party deps compiled`), and the job's duration drops to ≤ 16 min with
  `mix test --cover` still green; the `sdk conformance` job still restores a
  warm tree (`up to date`) and `mise run check:conformance` stays green;
  `mise run lint:workflows` and `mise run lint:pins` pass.
  *Proof:* on the PR, `timeout 1800 gh pr checks <N> --watch --fail-fast`, then
  `gh run view <run> --json jobs` (job durations, before/after in the PR body)
  and `gh api --allow-escape-sequences repos/jamescarr/ankusa/actions/jobs/<job>/logs
  | grep -E 'third-party deps|Cache restored from key: mix'`. Cite the run ids.
  *Anchors:* `.github/workflows/ci.yml` (package job `cache mix deps` step and
  its long PLT/key comment; `conformance` job `cache mix deps` step),
  `.mise/lib/mix_cache.sh` (`mix_cache_warm`: the restore/`-p` timestamp and
  "existing entries are never overwritten" rules this relies on),
  `.mise/tasks/check/package` (the `for env in dev test` loop),
  `.mise/tasks/check/conformance` (`mix_cache_warm … test`).
  *Evidence:* —

- [ ] **CI-2 — Pull every CI image from a GHCR mirror, pinned by digest: remove the 52 % share of failures** · P1 · Deps: —
  *Why:* **11 of 21 failed jobs in 30 runs** are
  `toomanyrequests: You have reached your unauthenticated pull rate limit` —
  `ankusa_redis` (2), `ankusa_rabbitmq` (2), `ankusa_nats` (2),
  `core integration (floci)` (2), `crash drill (ENOSPC)` (1),
  `crash drill (power-loss)` (2). Authenticating does not save it: in
  `ankusa_redis`'s failed job the log shows `Login Succeeded!` at 04:57:01 and
  `toomanyrequests` at 04:57:14, i.e. the shared account's
  200-pulls-per-6-hours limit was exhausted by five jobs pulling per run across
  a day of agent traffic (the registry returns the "unauthenticated" wording
  either way), and a fork PR gets no `DOCKERHUB_TOKEN` at all. Docker Hub pulls per run: `floci/floci{,-gcp,-az,-oci}:latest` (4,
  unpinned — a moved `latest` silently changes the integration suite),
  `amazon/aws-cli:2.38.1`, `curlimages/curl:8.23.0` (×3), `apache/kafka:3.9.1`,
  `nats:2-alpine`, `rabbitmq:4-management-alpine`, `redis:8-alpine`, and
  `elixir:1.20.4-alpine@sha256:…` (the crash drills take it from
  `packages/ankusa_server/Dockerfile`'s `FROM`). GHCR is part of the same
  infrastructure as Actions (free, authenticated by `GITHUB_TOKEN`, no
  third-party limit) and `docker.redpanda.com` + `ghcr.io/shopify/toxiproxy`
  already prove it: no job has ever failed on those pulls.
  *Change:*
  - New `.github/workflows/mirror-images.yml`: `on: schedule` (daily),
    `workflow_dispatch`, and `push` to itself; `permissions: packages: write,
    contents: read`; one matrix over a list held in `.github/mirror-images.txt`
    (`<source> <ghcr-name>` per line), each step
    `docker buildx imagetools create -t ghcr.io/jamescarr/ankusa/mirror/<ghcr-name>:<tag> <source>`
    (registry-to-registry: no layer crosses the runner), then
    `docker buildx imagetools inspect --format '{{.Manifest.Digest}}'` written to
    the job summary so the digests are copy-pasteable. Every image built by
    `docker.yml`/`crash.sh` must be action-pinnable — `imagetools` is in the
    stock buildx image, no new pin.
  - Point every consumer at the mirror **by digest**:
    `packages/ankusa/docker-compose.integration.yml`,
    `packages/{ankusa_redis,ankusa_rabbitmq,ankusa_nats,ankusa_kafka}/docker-compose.yml`,
    and `scripts/drills/crash.sh`'s `FROM` regex stays as it is but
    `packages/ankusa_server/Dockerfile` moves to the mirror's `elixir` copy
    (`ghcr.io/jamescarr/ankusa/mirror/elixir:1.20.4-alpine@sha256:…` — the
    Dockerfile already carries the digest, so only the repository part changes,
    and `docker.yml` + both crash drills follow it automatically). Leave
    `docker.redpanda.com` and `ghcr.io/shopify/toxiproxy` alone. Also mirror
    `moby/buildkit:buildx-stable-1@sha256:…`, which `docker.yml` pulls on every
    PR that touches `packages/ankusa*/**` (the second-most-frequent job here).
  - Registry auth: each CI job that pulls (replacing the four Docker Hub
    `Log in to Docker Hub` steps, which go away) logs in to GHCR with
    `docker/login-action` and `registry: ghcr.io`, `username: ${{ github.actor }}`,
    `password: ${{ github.token }}` — that is the one action already pinned in
    this repo, so `lint:pins` needs nothing new. Fork PRs get a read-only
    `GITHUB_TOKEN`, which is all a pull needs.
  - Package visibility: packages created by `GITHUB_TOKEN` are private, and the
    compose files are also used locally. The PR must make the mirror packages
    public (`gh api --method PATCH /user/packages/container/<name> -f
    visibility=public`; if the CLI token lacks the scope, say so in the PR body
    with the exact one-time click path, and add the
    `docker login ghcr.io`-for-local-runs line to `docs/testing.md` so a local
    run is never a mystery). Do not leave CI depending on a manual step: CI
    authenticates either way.
  - `.mise/tasks/lint/pins`: every `image:` in `packages/*/docker-compose*.yml`
    and `.github/mirror-images.txt`'s source must carry `@sha256:<64 hex>` (the
    rule already applied to the Dockerfile), so a mirror line and its consumer
    cannot drift.
  - `docs/testing.md` (local infra section) and the `AGENTS.md` Commands table
    note: CI pulls these images from GHCR; a human running the compose files
    locally needs `docker login ghcr.io` only if the mirror packages are
    private (they are public, so no login).
  *Done when:* the mirror workflow has run and its job summary lists a digest
  per image; every compose file and the Dockerfile name a GHCR mirror with a
  digest; `mise run test:integration` and `mise run check:package ankusa_redis`
  pull from the mirror (prove it by name in the log) and pass; `mise run
  lint:pins` and `mise run lint:workflows` pass; a full `ci.yml` run on the PR
  is green with no Docker Hub pull in any log.
  *Proof:* `gh workflow run mirror-images.yml`, then `gh run view <id> --log`
  (digests); `mise run lint:pins`; `mise run test:integration`; on the PR
  `timeout 1800 gh pr checks <N> --watch --fail-fast`; grep every job's log for
  `from docker.io` / `toomanyrequests` (zero).
  *Anchors:* `.github/workflows/ci.yml` (each job's `Log in to Docker Hub` step —
  they become unnecessary once nothing pulls from Hub and should be removed in
  this PR), `packages/ankusa/docker-compose.integration.yml`,
  `packages/*/docker-compose.yml`, `packages/ankusa_server/Dockerfile:16`,
  `scripts/drills/crash.sh:21-30`, `.mise/tasks/lint/pins`,
  `.mise/tasks/check/package` (`docker compose up -d --wait`),
  `.mise/tasks/test/integration`.
  *Evidence:* —

- [ ] **CI-3 — `concurrency:` on `ci.yml`: cancel a superseded PR run instead of running the whole pipeline twice** · P1 · Deps: —
  *Why:* `ci.yml` has no `concurrency:` block (every other workflow here does:
  `docker.yml:31`, `release*.yml`, `soak.yml`). A pushed-after-a-push PR runs
  two full pipelines: in the 30-run sample, `docs/npm-trusted-publishing` +
  `readme-cleanup` (3 runs), `fleet-replay-node-guard` (2) and
  `preflight-job-log` (2) — 4 superseded pipelines at **53–63 runner-minutes
  each**, with `main`-branch pushes never cancelable (they must not be). With
  three plan-workers and a merge-all agent pushing constantly, superseded runs
  are the second-largest avoidable cost after CI-1.
  *Change:* `.github/workflows/ci.yml`: add
  ```yaml
  concurrency:
    # A newer push to the same PR supersedes the older run; a push to main (or
    # a tag) never cancels, because its result is the record for the commit.
    group: ci-${{ github.workflow }}-${{ github.ref }}
    cancel-in-progress: ${{ github.event_name == 'pull_request' }}
  ```
  Nothing else. `e2e.yml` and `soak.yml` already cancel in progress; leave them.
  *Done when:* two quick pushes to one PR leave the first run `cancelled` and
  the second running, and a push to `main` is not cancelled by a later push.
  *Proof:* open a throwaway PR from a `ci/concurrency-probe` branch, push twice
  30 s apart, `gh run list --branch ci/concurrency-probe --limit 3` shows
  `cancelled` then `in_progress`/`completed` — close the PR without merging;
  `mise run lint:workflows`.
  *Anchors:* `.github/workflows/ci.yml:1-12`, `.github/workflows/docker.yml:31-33`
  (the existing shape), `.github/workflows/e2e.yml`.
  *Evidence:* —

### Phase B — Do not start work the diff cannot need

- [ ] **CI-4 — A `changes` job that skips the package jobs a diff cannot have touched, with `main` always running everything** · P2 · Deps: CI-1, CI-2, CI-3
  *Why:* every run executes 25 jobs regardless of the diff; **15 of the 25
  finish in under 1.3 min**, and the file's own measurements put the
  toolchain-restore step at 8–34 s of an 18–36 s mise-action step in jobs whose
  check is 2–10 s, while the four Docker jobs pull images and start brokers for
  PRs that touch a README.
  Most PRs in this repository touch one package (or docs); the full pipeline
  cost is 53–63 runner-minutes for a one-line docs change. A skipped job
  reports `skipped`, which satisfies branch protection for a required check —
  but only if the *workflow* runs, so the filtering must stay at job level and
  every job must be able to prove it may skip.
  *Change:*
  - `.github/workflows/ci.yml`: a `changes` job (checkout with
    `fetch-depth: 2`, `git diff --name-only "$BASE" "$HEAD"` — no third-party
    action to pin) that outputs `docs_only`, `packages` (the package names to
    run, from `pkg_names`), `sdk_or_conformance`, `examples`, `tools`,
    `hot_path` and `needs_docker`. It is **declared `needs: changes`** by each
    gated job, and its output for a non-PR event (push to `main`, `workflow_call`,
    a release tag) is "run everything". Fail-safe: any error inside the step
    makes it print the all-true JSON and exit 0.
  - `if:` on the `package` matrix (per-package `fromJSON` list — the matrix is
    already discovered, so gate the *value* by filtering the discovered list in
    `discover`, keeping the matrix one-dimensional), `integration`,
    `conformance`, `examples`, `tools`, `bench`, and the two crash jobs. The
    crash drills and the perf gate keep their existing scope rules on top
    (`HOT_PATHS`, the drill's own file list) — this only removes the runs whose
    diff cannot possibly affect them.
  - A `ci-summary` job (`needs: [changes, …], if: always()`) that fails the run
    when a skipped job's reason is "changes job failed", so a broken filter is
    loud rather than silent.
  - `docs/testing.md` "Long drills run in CI" and the `AGENTS.md` Commands
    table say which checks a single-package PR runs and that `main` always runs
    all of them.
  *Done when:* a docs-only PR reports every skipped job green and a
  `changes`-explained summary, with runner-minutes under 10; a PR that touches
  one SDK runs only that SDK + `lint`; a PR that touches
  `packages/ankusa/lib/ankusa/edge/**` runs the core package, the perf gate and
  both crash drills, exactly as today; a push to `main` runs all 25 jobs; and a
  PR that breaks the core suite while touching only `docs/` is still caught —
  by construction that is a `main` push, and the item says so in the PR.
  *Proof:* three probe PRs (docs-only, `packages/sdk-go`-only, core hot-path),
  `gh run view <id> --json jobs --jq '.jobs[]|[.name,.conclusion]'` for each;
  `mise run lint:workflows`; the merged change's first `main` run is fully
  green.
  *Anchors:* `.github/workflows/ci.yml` (every job's `runs-on`/`needs`/`if`,
  the `bench` job's `HOT_PATHS` scope step as the pattern to follow),
  `.mise/lib/pkg.sh` (`pkg_names`, `pkg_kind`), `.mise/tasks/check/_default`
  (the "everything CI checks" contract this must not contradict).
  *Evidence:* —

### Phase C — Shorter critical path

- [ ] **CI-5 — Split the `ankusa` job into a static-analysis lane and a test lane, each with its own cache key** · P2 · Deps: CI-1
  *Why:* after CI-1 the `ankusa` job is still the whole critical path:
  compile + `credo --strict` + PLT + dialyzer + sobelow is 2:30–3:15 and
  `mix test --cover` is 6:24–8:14, run one after the other in one job. The two
  lanes share nothing but `deps/` and the compile, and both fit in a warm cache.
  Splitting them costs one extra runner but takes ~3 min off every PR's wall
  clock and surfaces a static failure without waiting for the suite.
  *Change:* `.github/workflows/ci.yml` gains `ankusa-static` and `ankusa-test`,
  both `needs: [changes, discover]`, replacing the `ankusa` entry in the matrix
  (the matrix keeps the other packages). `check:package` grows an optional
  lane: `mise run check:package ankusa --lane static` (deps.get, unlock,
  hex.audit, format, compile `--warnings-as-errors`, credo, PLT restore,
  dialyzer, PLT save, sobelow, docs, `hex.build`) and `--lane test`
  (deps.get, `mix test --cover`, the seed line in the step summary). The
  default (no flag) runs both, so `mise run check` and every other caller are
  unchanged — prove that in the PR.
  **Cache keys are per lane** (`mix-v18-…-ankusa-static-…`, `mix-v18-…-ankusa-test-…`),
  each restoring by prefix from the other: CI-1's rule, one writer per key.
  *Done when:* both lanes are green on the PR; `ankusa-test` is ≤ 9 min and
  `ankusa-static` ≤ 6 min; the run's wall clock (last job to finish) is ≤ 22 min
  for a non-hot-path PR; `mise run check:package ankusa` still runs everything
  (and `mise run check` still passes locally); coverage floor and dialyzer
  unchanged.
  *Proof:* the PR's `gh run view <id> --json jobs` durations, before/after;
  `mise run check:package ankusa` locally (full lane); `mise run
  check:package ankusa --lane test`; `mise run lint:pins`.
  *Anchors:* `.mise/tasks/check/package`, `.github/workflows/ci.yml` (the
  `ankusa` matrix entry and its cache step), `.mise/lib/mix_cache.sh`,
  `docs/testing.md` (the "what each suite covers" table).
  *Evidence:* —

## Backlog (not yet an item)

- **Perf gate cost** — 9.4 min median, 12.8 p90, 63 % of pipelines; inside it,
  12 bench runs per side (~3:20) + tenant benches (2:33) + two full compiles.
  Options to research: measure the base first and the branch second (one
  compile order instead of two), cache the base's compiled tree by base SHA,
  move the self-asserting tenant benches to their own job. Do not touch the
  comparison rule (base measured on this runner) without an ADR-grade argument.
- **ExUnit partitioning** — `mix test --partitions` cuts the 6–8 min suite, but
  `--cover` with a coverage floor per partition needs a merged-coverage step;
  research `mix test --cover --partitions` semantics before proposing it.
- **The four real test failures** — `ankusa_kafka` 39/40 twice on different
  days, `ankusa` 1958/1959, `ankusa_redis` 82/83: name the failing test from the
  run logs and fix the flake in the test (never by loosening an assertion).
- **`mix docs --warnings-as-errors` in `ankusa_redis`** — 2 of 21 failures;
  the log says a warning in the generated docs, which is a signal bug worth
  its own diagnosis.
- **Pipeline report** — a `ci-summary` job writing per-job durations to
  `$GITHUB_STEP_SUMMARY` so the next measurement needs no API poking.
- **`discover` hop** — 0.1 min but serial ahead of every package job; fold it
  into `changes` when CI-4 lands.
- **Runner class** — larger/arm64 runners are a cost decision, not a speed one
  for the critical path until `ankusa` is a single verified number.

## Verification of this plan

- `grep -c '^- \[ \] \*\*' CI_PIPELINE_PLAN.md` → 5 unchecked items.
- Every *Anchors* path exists at the commit the item is executed on (re-read
  the file: line numbers are hints, not contracts).
- Each executed item's PR carries the measured before/after numbers and the
  run ids, and ticks its box with an *Evidence* line in the same PR.
