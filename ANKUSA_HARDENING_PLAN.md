# Ankusa → Salesforce-grade inbound webhook tier: hardening task list

## Context

The ask: assess Ankusa as a principal engineer who has run webhook ingestion at Zapier/Workato/Salesforce scale, and produce (1) a checkable task list that makes it rock solid as the ingestion tier for *all* Salesforce inbound webhooks, and (2) ground rules for evolving it without losing correctness or performance. An autonomous agent will loop over the list, planning and executing one task at a time.

End state of *this* plan: one new repo document, `docs/hardening.md`, holding the ground rules, the agent loop protocol and the task list, copied verbatim from the block between `<!-- BEGIN docs/hardening.md -->` and `<!-- END docs/hardening.md -->` below; `docs/README.md` and `AGENTS.md` link to it. No product code changes land in this plan — every code change is a task in the list, executed later by the loop.

Baseline established this session (nine read-only subsystem reviews over `packages/ankusa`, adapters, SDKs, CI): `docs/critical-review.md` closed all Critical/High of its 59 findings against a *single-box* bar. What remains open for a Salesforce bar: an acked hook survives only its host (60 s checkpoint backup is the only cross-host copy); control planes have no authentication, no audit, no TLS; tenant is not a scheduling key anywhere (queue, dispatch, routes, keys, metrics); secrets are plaintext in RocksDB and in backups; the HTTP sink can be pointed at loopback/IMDS; several structures grow without ceiling (DLQ, dedupe index, archive catalogue, quarantine age); there is no tracing, no drain on SIGTERM, no perf gate, no automated crash/fleet drill; consumer SDKs drop the verification signal. Decisions already taken with the user: host-loss durability = **dual-commit intake log** (ack after local fsync *and* confirmed publish to a replicated broker intake topic); per-key ordering = **opt-in lanes**; control-plane auth = **pluggable, shipping OIDC/JWT + mTLS**.

## Approach

1. Create `docs/hardening.md` with exactly the content between the BEGIN/END markers below (title line included). Do not edit the task text; the loop owns it from then on.
2. `docs/README.md`: add a row to the **Contributing** table (`docs/README.md:33-39`) after the `critical-review.md` row: `| [`hardening.md`](hardening.md) | the Salesforce-scale hardening checklist the agent loop executes, and the ground rules every change must keep |`.
3. `AGENTS.md` **Docs index** table: add the row `|[`hardening.md`](docs/hardening.md)|the hardening checklist and ground rules; read before any change to the ack path, store keys, dispatch, control planes or wire formats|` after the `critical-review.md` row.
4. `AGENTS.md`, end of **Run the checks before calling work done**: append the paragraph `Working the hardening list? Follow the loop protocol in [docs/hardening.md](docs/hardening.md): one task per PR, check a box only with an Evidence line, never reword acceptance criteria.`
5. Create `docs/adr/README.md` (one paragraph: ADRs are required by ground rule G14; file name `NNNN-<slug>.md`; sections `Context`, `Decision`, `Consequences`, `Proof`) so design-gate tasks have a home.

No other files change.

## Critical files & anchors

- `docs/README.md:33-39` — Contributing table where the new row goes.
- `AGENTS.md` — "Docs index" table and "Run the checks before calling work done" section (both exist; add, don't restructure).
- `.mise/tasks/check/_default` and `.mise/tasks/check/package` — referenced by many tasks as the place CI gates are added (read by the loop, not by this plan).
- `.github/workflows/ci.yml` — job list referenced by Phase 0 tasks.

## Verification

Run from the repo root after the edits:

- `mise run check:links` → passes (the new doc's relative links resolve; CI runs the same task in the `lint` job, `.github/workflows/ci.yml:26-36`).
- `grep -c '^- \[ \] \*\*' docs/hardening.md` → `64` (one unchecked box per task; the count is the number of `**ID — Title**` bullets in the Task list below).
- `grep -c '^  \*Evidence:\* —$' docs/hardening.md` → `64`.
- `grep -n 'hardening.md' docs/README.md AGENTS.md` → three hits (README row, AGENTS index row, AGENTS checks paragraph).
- `ls docs/adr/README.md` → exists.
- Open `docs/hardening.md` in the GitHub preview (or `glow`): headings render as `##`/`###`, task bullets render as checkboxes.

## Assumptions & contingencies

- Scale targets used throughout (override by editing the SLO table in the doc, nothing else depends on the numbers): 10⁴–10⁵ req/s fleet-wide, ≥100 edge nodes, 10⁵–10⁶ sources, 10⁵–10⁶ tenants, mean body 10 KB, max 10 MB, fsync 1–3 ms.
- Tenant model: both shapes are first-class — one source per (tenant, integration) *and* shared sources with the tenant taken from the URL (`TenantPath`). Every task that adds a tenant dimension must work for both; if a task finds the two shapes conflict, the per-source shape wins and the shared-source case gets a `tenant_id` resolved by the route (TEN-4).
- The intake-log adapter ships for Kafka first (`ankusa_kafka` exists, `acks=all`), JetStream second; if the Kafka client (`brod`) cannot give a confirmed-at-quorum publish within the attempt deadline, DUR-1 falls back to JetStream as the first adapter and records why in the ADR.
- `jose` is added to core for JWT verification (benefits every user; packaging rule in `docs/packaging.md`). If the maintainer rejects a new core dep, SEC-1 keeps the behaviour in core and moves `Ankusa.Admin.Auth.JWT` into `ankusa_server`.
- CI runners cannot run device-mapper fault injection; F2 does ENOSPC and SIGKILL-under-load in CI and ships the `dm-flakey` drill as a script run by `release:preflight` on a privileged runner. If no privileged runner exists, the script stays documented as a manual pre-release step and the task's Evidence says so.
- Encryption at rest: application-level envelope encryption for secrets (SEC-4); hook bodies rely on encrypted volumes plus SSE-KMS on buckets, stated in the startup log. Body-level encryption is deliberately not a task; add one only if a compliance review demands it.
- If `mise run check:links` rejects the `docs/adr/` link because the directory is new, create `docs/adr/README.md` first (step 5) — it is listed above for that reason.

<!-- BEGIN docs/hardening.md -->
# Hardening Ankusa for a Salesforce-scale inbound webhook tier

This document is executed, not read: an autonomous agent loops over the **Task list**, one task per PR, following the **Agent loop protocol**, and every change it (or anyone) makes must keep the **Ground rules**. Findings behind the tasks come from the 2026-10-17 subsystem assessment; the older per-finding review is [`critical-review.md`](critical-review.md).

## Ground rules

Each rule has a **Check** — the thing a reviewer (human or agent) does to confirm a change keeps it. A change that cannot pass its checks is wrong, however small.

1. **Never `2xx` before durable, and say what durable means.** "Durable" is the configured durability mode's promise (`wal.type: disk` = this host's fsync; `wal.type: dual` = this host *and* the intake log; `wal.type: none` = every sink's confirm). The startup log states the loss window in one line. **Check:** each mode has a test that fails if an acked hook can be lost under that mode's promise (`loss_test.exs`, `edge_direct_test.exs`, the DUR-1 fleet drill).
2. **Errors are values at every I/O boundary.** The only place that may crash on I/O is opening the store at boot. Every boundary returns one of four classes — `not_found | unavailable | throttled | permanent` — with an optional `retry_after`. **Check:** no `:ok =` / `!` call on a file, socket, store or HTTP result outside a boot-critical `init/1`; new callers map all four classes.
3. **Everything is bounded.** Every queue, buffer, ETS table, cache, column family and index has a count ceiling *and* a byte ceiling, a pruner, a gauge, and a documented refusal (`429`/`503` with `Retry-After`). **Check:** for every new table/CF/map/pool: name the cap, the sweep, the metric and the refusal in the PR description.
4. **Tenant is a scheduling key.** Any resource shared across tenants (slots, byte windows, pools, breakers, buckets, caches, scan cursors) is partitioned by tenant or carries a per-tenant cap; a tenant can only exhaust its own share. **Check:** a two-tenant test at 99:1 load where the light tenant's p99 stays within 2× of its solo p99.
5. **Idempotency travels end to end.** Provider dedupe key → hook `id` → `idempotency_key` → the broker's native id (`Nats-Msg-Id`, AMQP `message_id`, Kafka header, SQS dedup id, HTTP `webhook-id`). Nothing mints a new identity on retry or replay without a `replay_id`. **Check:** the conformance idempotency cases pass; a replay drill shows `extra_deliveries` bounded by `idempotency_key`.
6. **The request path owns no process but its own.** Between accept and `2xx` the only serialization points are the batcher partition and the store commit. No `GenServer.call` to a singleton, no `Logger` at `:info` or above, no JSON decode of a body (dedupe JSON paths are capped), no `:persistent_term.put`, iodata instead of binary concatenation, `:binary.copy/1` only on a sub-binary. **Check:** the hot-path bench gate (rule 10) and a grep of the diff for `GenServer.call`, `Logger.info`, `Jason`/`JSON.decode`, `<>` on bodies.
7. **Control planes are default-deny.** Authenticated, tenant-scoped, audited in the same synced batch as the mutation, rate-limited, TLS when crossing a host. No secret in a log line, crash report, API response or plaintext at rest. **Check:** every new admin route has a 401 test, a 403 cross-tenant test and an audit-row test; `/v1/config` redaction test covers every new secret-bearing key.
8. **Observability is structural.** Every hook carries W3C trace context; every stage emits a span and the SLI metrics in the table below; state gauges come from maintained counters, never from scans; no metric label comes from unauthenticated input; the cardinality policy (`metrics.detail`, series cap) is enforced in code. **Check:** `metrics_test.exs` asserts the series set and that series count is independent of tenant count.
9. **Every persisted or wire format carries a version.** Store format, envelope, delivery row, message, segment, claim ref, config, admin API. Changes are additive; version N and N−1 read each other's data within a minor; migrations are forward-only, fixture-tested, with the rollback boundary written down. **Check:** the upgrade test boots current code on every checked-in fixture; a format bump ships its migration and fixture in the same PR.
10. **Performance is a budget, not a hope.** The bench gate (`mise run bench:gate`) runs on every PR touching a hot-path file (`packages/ankusa/lib/ankusa/{edge,queue,store,dispatch,verifier,routes}/**`, `store.ex`, `queue.ex`, `dedupe.ex`, `envelope.ex`, `http.ex`, `sink/message.ex`, `sink/http.ex`, `route_resolver.ex`); a p99 or throughput regression > 20 % against `bench/baseline.json` fails. A hot-path change states before/after numbers in its PR. Baselines change only in a PR titled `perf: rebaseline <reason>`. **SLOs** (production, per node unless stated):

   | SLI | Target | Metric |
   | --- | --- | --- |
   | Ack latency (steady, 10 KB bodies, 1 k req/s/node) | p99 ≤ 50 ms | `ankusa_ingest_duration_seconds` |
   | Ack latency during chaos (pod kills) | p99 ≤ 250 ms, zero `missing` | loadgen chaos phase |
   | Ingest throughput (1 KB bodies, in-process bench) | ≥ 10 k/s/node | `bench/core_bench.exs` |
   | Delivery latency, healthy sinks (received → confirmed) | p99 ≤ 5 s | `ankusa_delivery_latency_seconds` |
   | Backlog age (oldest due row) | alert at 5 min | `ankusa_queue_oldest_due_age_seconds` |
   | Memory growth, 1 h soak at target rate | ≤ 5 % RSS | soak job |
   | `/metrics` size | ≤ 10 k series/node, independent of tenant count | metrics test |
   | Drain on SIGTERM | zero connection resets, every in-flight request answered | OPS-4 test |

11. **Proof over prose.** A durability, isolation or security claim exists only with a test that fails when the claim is false; drills are automated; a doc sentence without a test says "(unverified)". **Check:** the PR links the test for each claim it adds to `architecture.md`, `delivery.md` or `storage.md`.
12. **Dependencies by placement.** Take the dependency when it is the right tool; put it in core only if every deployment benefits, otherwise in an adapter package ([`packaging.md`](packaging.md)). After a core dep change run `mise run deps` and commit every lock.
13. **Zero-downtime operations.** SIGTERM drains (readiness flips first); reloadable keys reload without restart; rolling upgrades N→N+1 and rollback N+1→N are safe within a minor; every new config key has a default, a range check, a `configuration.md` row and a `/v1/config` redaction decision. **Check:** `config_test.exs` has a range test for every new numeric key; `configuration.md` diff shows the row.
14. **Design gates.** A change to the ack path, the store key layout, a wire format, the isolation model or the durability mode needs an ADR in `docs/adr/NNNN-<slug>.md` (sections: Context, Decision, Consequences, Proof) merged before code. Tasks marked `[design-gate]` are split into `.a` (ADR) and `.b` (implementation).

## Agent loop protocol

1. **Select.** Take the first unchecked task in document order whose `Deps` are all checked and that has no open `Blocked:` line. Never skip ahead for convenience; never work two tasks at once.
2. **Plan.** Read the task, every `Anchors` location (line numbers are hints; re-read the file), the ground rules, and `AGENTS.md`. Write a short plan: files, symbols, new config keys, tests, the exact Proof commands. For a `[design-gate]` task the first PR is the ADR alone (`docs/adr/`); the `.b` sub-task depends on the `.a` box being checked.
3. **Implement.** Branch `hardening/<ID>-<slug>`, one task per PR, draft PR per the repo's git rules. Update the docs the behaviour touches in the same PR (`configuration.md` keys, the guarantees table in `architecture.md`, `testing.md` coverage, `delivery.md`/`storage.md` claims). Keep every ground rule; if the task as written would break one, fix the task text's *Change* (not its *Done when*) and say so in the Evidence line.
4. **Prove.** Run the task's Proof commands exactly as written; run `mise run check:package <pkg>` for every touched package (all adapters and `ankusa_server` for a core change); `mise run bench:gate` if a hot-path file changed; `mise run check:conformance` if `conformance/` or any `packages/sdk-*` changed; `mise run check:examples` if `examples/` changed.
5. **Record.** Check the box only if every Proof passed in this session. Replace `*Evidence:* —` with `*Evidence:* <YYYY-MM-DD> · PR #<n> · <commands run> · <2–5 key output lines>`. Never reword a task's *Done when* or *Proof*; never delete a task; never check a task whose proof was skipped or hand-written.
6. **Blocked.** If a proof fails or a prerequisite is missing, leave the box unchecked, append `Blocked (<date>): <what failed> · <what was tried>`, and if the fix is a distinct piece of work append a new task at the end of the same phase with the next free ID in that phase. Move on to the next eligible task. A later run clears the `Blocked:` line when it resolves it.
7. **Split.** A task that needs more than one PR is split in place into `<ID>.a`, `<ID>.b`, … ; the original *Done when* and *Proof* move to the last sub-task; earlier sub-tasks get their own narrower proof. Splitting never narrows the original acceptance.
8. **Rebaseline.** A perf baseline changes only in a dedicated PR titled `perf: rebaseline <reason>` with the before/after numbers in the description.
9. **Every tenth completed task:** run `mise run check` in full, then update the `Status:` line at the top of the Task list.
10. **Never** mark a task done while its own acceptance fails, report a check that did not run, or narrow scope silently; say what failed and what is open.

## Task list

Status: 0/64 done · P0 open: 29 · last updated: 2026-10-17

Format: `- [ ] **ID — Title** · priority · Deps`, then *Why* (the gap, with evidence), *Change* (what to build; module/config names are decided, not suggestions), *Done when* (observable acceptance), *Proof* (commands and expected output), *Anchors* (where to read first), *Evidence* (filled by the loop). Priorities: **P0** blocks a Salesforce deploy (loss, isolation, security, no recovery); **P1** needed before GA (ops, observability, scale ceilings); **P2** hardening.

### Phase 0 — Gates (make every later task provable)

- [x] **F1 — Perf regression gate** · P0 · Deps: —
  *Why:* `bench/core_bench.exs` exists but has no baseline and no CI gate; `tools/loadgen` is manual; nothing stops a hot-path regression (`docs/testing.md:463-481`, `.mise/tasks/bench`, `.github/workflows/ci.yml`).
  *Change:* `.mise/tasks/bench/gate`: runs `core_bench.exs` for the matrix `BODY_BYTES ∈ {512, 10240}` × `N=20000` × `CONCURRENCY=256`, three runs each, median per cell, writes `packages/ankusa/bench/last.json`; compares to `packages/ankusa/bench/baseline.json` (`{cell: {ingest_per_s, p50_ms, p99_ms, end_to_end_per_s}, runner: "<class>"}`); fails if any cell's `p99_ms > 1.2 × baseline` or `ingest_per_s < 0.8 × baseline`. Extend `core_bench.exs` to print `p50_ms`/`p99_ms` in its JSON line if absent. CI job `bench` in `ci.yml` runs on PRs whose diff touches a ground-rule-10 hot-path file (`paths:` filter) and on `main`. Nightly workflow `.github/workflows/soak.yml`: `DURATION=3600` loadgen run against the `docker:smoke` image sampling `/metrics` every 60 s, failing on `missing > 0` or RSS growth > 5 %.
  *Done when:* a PR adding `Process.sleep(2)` to `Ankusa.Edge.Ingest.ingest/2` fails the `bench` job; reverting it passes; `baseline.json` is checked in with the runner class recorded.
  *Proof:* `mise run bench:gate` → `PASS` table with per-cell deltas; the sleep-PR run shows `FAIL p99_ms 512B: <x> > 1.2×<baseline>`.
  *Anchors:* `packages/ankusa/bench/core_bench.exs:134-157`, `.mise/tasks/bench`, `tools/loadgen/`, `.github/workflows/ci.yml:38-73`.
  *Evidence:* 2026-10-18 · PR #97 · `mise run bench:gate` (local: PASS table; with `Process.sleep(10)` FAIL lines, exit 1), the CI `perf gate` job on `ubuntu-latest` (a run, a `Process.sleep(2)` commit, the revert), `DURATION=600 mise run bench:soak`, `mise run lint:workflows`, `mise run check:links`, `mise run check:package ankusa` · sleep(2) in `Ingest.ingest/2`, CI: `FAIL ingest_per_s 512B-c32: 6334.17 < 0.8×10317.57 (limit 8254.06)`, job red; an earlier CI run of the sleep commit against the checked-in baseline also gave `FAIL p99_ms 512B-c32: 10.79 > 1.2×5.93 (limit 7.12)`. Reverted: job green, `PASS`, 512B p99 18.11 vs 17.72, 10240B p99 32.18 vs 34.07, 512B-c32 ingest/s 11782.64 vs 11946.43 · *Change* amended: (1) a third cell `512B-c32` (CONCURRENCY=32), because at CONCURRENCY=256 latency is 256/throughput (Little's law) and the sleep moved neither gated number there; (2) CI compares with the PR's base commit measured on the same runner, because two runs of identical code on `ubuntu-latest` ran at 23k and 50k hooks/s and failed a checked-in absolute baseline (`baseline.json`, recorded from CI, `runner: github-linux-x64`, stays the reference for local runs and the fallback); (3) a cell over a limit is re-run and judged on its best of six runs, since noise only adds latency · soak, 600 s at 200 req/s: `accepted 120000 delivered 120000 missing 0`, `soak OK`; RSS is a sawtooth, so growth compares window floors, not medians

- [x] **F2 — Crash-consistency drill in CI (ENOSPC, EIO, kill under load)** · P0 · Deps: —
  *Why:* the only crash test is `kill -9` of a child BEAM (`packages/ankusa/test/ankusa/loss_test.exs:40`); ENOSPC was a hand-run container probe (`docs/critical-review.md:171-177`); fsync order is checked by `strace`, not in CI (`docs/testing.md:113-118`).
  *Change:* extract the child-BEAM harness from `loss_test.exs` into `packages/ankusa/test/support/crash_harness.exs` (start instance on `DATA_DIR`, ingest at `RATE`, print `ACK <id>` per 201). `.mise/tasks/test/crash`: in `elixir:<pinned>-alpine` with `--tmpfs /data:size=96m`, run the harness, fill the disk with a filler file at +5 s, assert `503 store_unavailable` and `/ready` 503 while full, delete the filler, assert 201 and `/ready` 200 resume, `kill -9` at +20 s under load, reopen the store with `Ankusa.Store` only and assert every printed `ACK` id reads back. `scripts/drills/flakey.sh`: the same harness on a `dm-flakey` loop device (drop writes for 2 s windows) — run by `release:preflight` on a privileged runner when available, otherwise documented. CI job `crash` in `ci.yml`.
  *Done when:* `crash` job is green on `main`; the drill fails if `Queue.Writer` is patched to skip `sync: true` (prove once, revert).
  *Proof:* `mise run test:crash` → `acked=<n> recovered=<n> missing=0 enospc_503=<k> resumed=true`; with the `sync: false` patch → `missing>0`.
  *Anchors:* `packages/ankusa/test/ankusa/loss_test.exs`, `packages/ankusa/lib/ankusa/queue/writer.ex:500-511`, `packages/ankusa/lib/ankusa/store.ex:345-350,593-594`.
  *Evidence:* 2026-10-17 · PR #96 · `mise run test:crash`, `scripts/drills/flakey.sh`, both again with `sync: false` in `queue/writer.ex` (reverted), `mise run check:package ankusa`, `mise run lint:workflows`, `mise run check:links`, and the PR's CI jobs `crash` and `crash-powerloss` · local `test:crash` → `acked=14940 recovered=14940 missing=0 enospc_503=1 resumed=true`; local `flakey.sh` (ext4 on device-mapper: two 2 s I/O-error windows, then a power-loss cut) → `acked=6562 recovered=6562 missing=0 eio_503=17 resumed=true`; local `flakey.sh` with `sync: false` → `acked=9344 recovered=5600 missing=3744`, exit 1. CI on GitHub's `ubuntu-latest`: job `crash` → `acked=12265 recovered=12265 missing=0 enospc_503=9 resumed=true`; job `crash-powerloss` → `acked=9307 recovered=9307 missing=0 eio_503=18 resumed=true`. The tmpfs drill with the `sync: false` patch still gives `missing=0` (`kill -9` loses nothing the kernel already holds), so the `sync: false` proof is the device-mapper drill, which runs in CI as `crash-powerloss` and in `release:preflight` for a core tag. The I/O-error windows use the device-mapper `error` target (the test kernel has no `dm-flakey` module).

- [x] **F3 — Static analysis and coverage gates** · P1 · Deps: —
  *Why:* `.mise/tasks/check/package:33-37` runs format, warnings-as-errors, tests, `hex.audit` only; no dialyzer, credo, sobelow or coverage floor.
  *Change:* add `dialyxir` (PLT cached on `mix.lock` + OTP in CI), `credo --strict`, `sobelow --exit` (core and `ankusa_server`), and `mix test --cover` with `coveralls.json`/`test_coverage: [threshold: 85]` for core (adapters 80) to `.mise/tasks/check/package`; record the ExUnit seed in `$GITHUB_STEP_SUMMARY`.
  *Done when:* an injected wrong `@spec` fails CI; coverage below the floor fails CI; the suite passes clean.
  *Proof:* `mise run check:package ankusa` → dialyzer `0 errors`, credo `0 issues`, sobelow `No findings`, coverage ≥ 85 %.
  *Anchors:* `.mise/tasks/check/package`, `packages/ankusa/mix.exs`.
  *Evidence:* 2026-10-17 · PR #98 · `mise run check:package` for ankusa, ankusa_kafka, ankusa_nats, ankusa_rabbitmq, ankusa_redis, ankusa_server, async_api_spex, sdk-elixir (all exit 0); `mise run check:examples`, `check:tools`, `check:links`, `lint:workflows`, `check:conformance` (all exit 0); `@spec min_for(...) :: atom()` injected into `Ankusa.UUIDv7`, `mix dialyzer`, reverted · core: `found no issues` (credo), `Total errors: 0 … done (passed successfully)` (dialyzer), `SCAN COMPLETE` with no finding (sobelow `--exit --skip`), `87.51% Total` against the 85 floor · wrong spec: `The @spec for the function does not match the success typing … (non_neg_integer()) :: binary() / … :: atom()`, dialyzer exit 2 · floor: `ankusa_rabbitmq` first failed its 80 floor at 76.79 % (`Coverage test failed, threshold not met`, task exit 3) until broker-free callback tests lifted it; core at the default 90 floor failed at 87.70 %. The injected-spec and coverage failures were run locally; no red CI run was made.

- [x] **F4 — Property tests for parsers and codecs** · P1 · Deps: —
  *Why:* no `StreamData`/fuzz anywhere; `Ref.parse/1`, `Pack.build/read`, `Envelope` round-trip, header validation, CIDR parsing and the 0.3 frame scanner are example-tested only.
  *Change:* `{:stream_data, "~> 1.1", only: :test}` in core; `test/ankusa/property/` with properties (≥ 5 000 runs each in CI): `Ref.parse(Ref.to_string(r)) == r`; `Pack.read(Pack.build(items)) == items` for 1–65 000 items of 0–1 MiB; `Envelope.from_binary(Envelope.to_binary(e)) == {:ok, e}`; `Ingest.check_headers/1` never raises on arbitrary header lists; `Net.parse_cidr/1` returns `:error` or a valid range for arbitrary strings; `Store.Migrate` frame scanner never raises on arbitrary bytes; `Verifier.Hmac` template engine never raises on arbitrary templates/headers; `Dedupe.key/2` never raises on arbitrary bodies. A checked-in corpus `test/fixtures/corrupt_frames/` of torn/bit-flipped frames that all return `{:error, _}`.
  *Done when:* properties run in `mix test`; any property that finds a counterexample is fixed in the same PR with the shrunk case added as a regression test.
  *Proof:* `mix test test/ankusa/property` → all pass; `PROPERTY_RUNS=50000 mix test test/ankusa/property` passes once (recorded in Evidence).
  *Anchors:* `packages/ankusa/lib/ankusa/claim_check/{ref,pack}.ex`, `envelope.ex:78-87`, `edge/ingest.ex:155-164`, `net.ex`, `store/migrate.ex`.
  *Evidence:* 2026-10-18 · PR #100 · `mix test test/ankusa/property` (inside `mise run check:package ankusa`), `PROPERTY_RUNS=50000 mix test test/ankusa/property`, `mise run check` (repo-wide), `mise run check:links` · check:package ankusa: `Result: 831 passed (47 properties, 784 tests), 25 excluded`; soak at 50 000 runs: `Finished in 2840.5 seconds` / `Result: 70 passed (47 properties, 23 tests)`; `mise run check` exit 0. Found and fixed (shrunk cases kept as example tests): `hash: sha1` raised `:badarg` in `:crypto.mac/4` (SHA-1 is `:sha`), an invalid inline `%Scheme{}` raised out of `Hmac.verify/2` instead of `{:error, :bad_scheme}`, `Pack.build/1` at 65 535 entries wrote ZIP64's `0xFFFF` and `:zip.unzip/2` answered `{:error, :bad_eocd}` (ceiling now 65 534 entries), and a torn WAL header zero-extended into a length-0 CRC-0 "frame" raised in `Envelope.from_binary/1` and refused the 0.3 import. Change text differs from the bullet's wording: there is no `Pack.read/1` (reader = `Pack.locate/2` + `:zip`), `Envelope.from_binary/1` returns the struct (F7 wraps it later), items run 1–600 claims with the 65 532-claim ceiling a deterministic test, and a CRC-valid frame around a non-envelope payload still raises by design (`Store.run_migration/1` turns it into a refused boot).

- [x] **F5 — Fleet end-to-end in CI (nightly)** · P0 · Deps: —
  *Why:* the only multi-node zero-loss proof is the manual kind+Oban drill (`docs/testing.md:483-537`); nothing exercises LB → N nodes → shared claim bucket → gateway → consumer automatically.
  *Change:* `examples/fleet-e2e/`: compose with 2 `ankusa_server` nodes (Redis route+source store from `ankusa_redis`, MinIO claims bucket, distinct `storage.key_prefix`), 1 `claim_check`-role node, nginx round-robin, a consumer (copy the `examples/rabbitmq-consumer` worker, extended to upsert into a Postgres `processed_webhooks` table so `mix loadgen.verify` works unchanged), and a `run.sh` driving `tools/loadgen` steady/chaos (`docker kill` one node mid-run)/burst phases then `loadgen.verify`. Workflow `.github/workflows/e2e.yml`: nightly + `workflow_dispatch` + on tags, running `examples/fleet-e2e/run.sh` and `mise run e2e`. Chaos must also include a provider retry sent to the *other* node (same dedupe key) and record `extra_deliveries`.
  *Done when:* nightly is green with `missing: 0`, `sha_mismatches: 0`; a fat hook from node A is redeemed through the gateway; `extra_deliveries` is reported (allowed > 0 until TEN-6, then must be 0).
  *Proof:* `examples/fleet-e2e/run.sh` → `verify: missing=0 sha_mismatches=0 extra_deliveries=<n> claim_redeemed_cross_node=true`.
  *Anchors:* `examples/rabbitmq-consumer/`, `examples/oban-consumer/run.sh`, `tools/loadgen/`, `packages/ankusa_redis/lib/ankusa/routes/store/redis.ex`.
  *Evidence:* 2026-10-18 · PR #99 · `examples/fleet-e2e/run.sh` (local, twice), `mise run lint:workflows`, `mise run check:links`, `mise run check:examples` (exit 0, includes `tsc --noEmit` of `examples/fleet-e2e/consumer`), and the PR's CI job `fleet end to end (compose)` (`.github/workflows/e2e.yml`, run via its `pull_request` path filter) · local → `verify: missing=0 sha_mismatches=0 extra_deliveries=4 claim_redeemed_cross_node=true` (first run) and `extra_deliveries=2` (second); steady 9000/9000 processed, chaos 9000/9000 processed with node a killed at +10 s and started at +20 s, burst 28464/28464 · CI → `verify: missing=0 sha_mismatches=0 extra_deliveries=2 claim_redeemed_cross_node=true`; the retried event `default:demo:retry-…` is one row with 2 deliveries from two hook ids (the dedupe index is per node, so the retry on the other node is a second hook; reported, allowed until TEN-6) · the first CI run failed (RabbitMQ's `ping` healthcheck is green before the AMQP listener is up, so the consumer died at boot, never declared its queue, and every publish was unroutable); fixed with a port-connectivity healthcheck, a connect retry in the consumer, and a wait for the consumer's queue in `run.sh`, and the rerun was green. Not exercised: the nightly schedule, `workflow_dispatch` and tag triggers (they run the same job, and fire only after this merges), and the `kind` job / `mise run e2e` on an amd64 runner (its `--platform linux/arm64` build now follows the Docker host's architecture; the `kind` job is skipped on PRs, so that change is unverified until the first non-PR run). The `Change` text names MinIO: the `minio/minio` Docker Hub image is no longer pullable, so the stack uses `cgr.dev/chainguard/minio`.

- [ ] **F6 — Store format version, migration registry, downgrade guard** · P0 · Deps: —
  *Why:* the RocksDB layout has no version marker (`store/keys.ex:17-21`); `Store.Migrate` only imports 0.3 artifacts (`store/migrate.ex:31,59,82`); a newer image's keys are silently ignored by an older image after rollback; later tasks (TEN-1, SEC-2, DUR-2, DUR-8, DEL-1) change the layout.
  *Change:* `Keys.format_version/0` → `"m:format_version"`; current = `1`, written `sync: true` on first open of an unversioned store. `Ankusa.Store.Migrate` gains `@migrations [{from, to, module}]`, applied in order at boot before serving, each migration in bounded synced batches with a resumable marker `m:migrating:<to>`; boot refuses `{:store_format_newer, found, supported}`. `mix ankusa.store.migrate --dry-run` prints the plan. `mix ankusa.store.fixture <path>` produces `test/fixtures/store-v<version>.tar.zst` from a seeded instance (hooks, pending/dead rows, quarantine, sources, catalogue, dedupe); check in the v1 fixture now. `test/ankusa/store_upgrade_test.exs` boots current code on every fixture and asserts counts and `Storage.fetch` by id. `deployment.md`: upgrade order (backup → one pod at a time) and that rollback past a format bump restores the pre-upgrade backup.
  *Done when:* a store stamped `format_version = 2` refuses to open on current code naming both versions; v1 fixture upgrades cleanly; a mid-migration kill resumes at the next boot.
  *Proof:* `mix test test/ankusa/store_upgrade_test.exs` → pass, including `refuses newer format` and `resumes interrupted migration`.
  *Anchors:* `packages/ankusa/lib/ankusa/store/keys.ex:17-21`, `store/migrate.ex:1-60,151-158`, `store.ex:138-159`, `test/ankusa/store_migrate_test.exs`, `test/support/legacy_data_dir.ex`.
  *Evidence:* —

- [ ] **F7 — Versioned envelope and delivery-row encoding** · P0 · Deps: —
  *Why:* `Envelope.to_binary/from_binary` is bare `term_to_binary` + `struct/2` (`envelope.ex:78-87`) and delivery rows are `term_to_binary` maps (`queue/writer.ex:466-480`); adding a field (OPS-1 trace, DEL-2 `first_attempt_at`) has no N−1 compatibility story.
  *Change:* `Envelope.to_binary/1` → `<<"AE", 1::8, term::binary>>`; `from_binary/1` accepts the legacy unprefixed form (v0) and v1, decodes with `binary_to_term(bin, [:safe])`, builds the struct via `struct(Envelope, Map.take(map, known_keys))` so unknown additive fields are ignored; unknown version → `{:error, :unsupported_envelope_version}`. Same scheme for delivery rows (`"AD"`, v1) in `Queue.Deliveries`. Any reader that gets `{:error, _}` dead-letters/logs instead of raising.
  *Done when:* a row written with an extra field by "N+1" decodes on N; a v0 row decodes; a `"AE", 9` row returns the error and the pipeline dead-letters it with reason `:unsupported_envelope_version`.
  *Proof:* `mix test test/ankusa/envelope_test.exs test/ankusa/queue_test.exs` → pass with the three new cases.
  *Anchors:* `packages/ankusa/lib/ankusa/envelope.ex:78-87`, `queue/writer.ex:466-480`, `queue/deliveries.ex:120-138`.
  *Evidence:* —

### Phase 1 — Security and control planes

- [ ] **SEC-6 — Listener hardening, TLS/mTLS on every listener, separate metrics listener** · P0 · Deps: —
  *Why:* Bandit gets only `plug/scheme/port` (`instance.ex:195-199,352-358`), so defaults apply: 60 s header read timeout, 15 s per body recv, unlimited keep-alive, 16 384 connections × 100 acceptors, HTTP/2 on, no TLS anywhere; `/metrics` shares the mutating admin listener (`admin/router.ex:84-90`).
  *Change:* `Config` key `listener: %{tls: %{certfile, keyfile, cacertfile, verify: :verify_peer | :verify_none, versions: [:"tlsv1.2", :"tlsv1.3"], ciphers}, max_connections: 20_000, num_acceptors: System.schedulers_online() * 2, read_timeout_ms: 10_000, body_read_timeout_ms: 15_000, request_deadline_ms: 30_000, max_requests_per_connection: 1_000, max_header_count: 64, max_header_length: 16_384, http2: false}` applied in `Instance.edge_children/2` via `thousand_island_options`/`http_1_options`/`http_2_options`; `admin.tls`, `routes.admin.tls`, `claim_check.tls` with the same shape in `bandit_child/4`; `verify: :verify_peer` + `cacertfile` makes the peer certificate available to SEC-1's mTLS mode. `metrics: %{enabled, ip, port}` starts `Ankusa.Metrics.Router` serving only `/metrics`, `/health`, `/ready`; the admin listener keeps serving `/metrics` unchanged (SEC-1 later restricts it to the `metrics:read` scope). `Http.read_body/2` takes `read_timeout` and enforces `request_deadline_ms` across chunks. Range checks for every new key; YAML keys in `ankusa_server/config.ex`.
  *Done when:* a raw socket idle past `read_timeout_ms` is closed; a TLS edge with `verify_peer` rejects a client without a cert and accepts the pinned one; the metrics port answers 404 for `/v1/*`.
  *Proof:* `mix test test/ankusa/edge_test.exs test/ankusa/instance_test.exs test/ankusa/config_test.exs` → new cases `closes idle header read`, `tls listener requires client cert`, `metrics listener serves only metrics`; `openssl s_client -connect :4000 -cert client.pem` drill added to `packages/ankusa_server/scripts/smoke.sh`.
  *Anchors:* `packages/ankusa/lib/ankusa/instance.ex:180-214,286-301,351-363`, `http.ex:21-27`, `config.ex:16,84-87,114-115,142`, `packages/ankusa_server/lib/ankusa_server/config.ex`.
  *Evidence:* —

- [ ] **SEC-1 — `Ankusa.Admin.Auth`: OIDC/JWT, mTLS and scoped tokens on every control plane** · P0 · Deps: SEC-6
  *Why:* admin, route and claim-gateway routers read no credential by design (`admin/router.ex:13-22`, `routes/router.ex:11-19`, `claim_check/router.ex:7-11`); loopback bind is the only control; no principal means no RBAC and no attributable audit. CWE-306.
  *Change:* behaviour `Ankusa.Admin.Auth` with `c:authenticate(conn, opts) :: {:ok, %Ankusa.Principal{id, scopes, tenants}} | {:error, :unauthenticated}`; implementations `Auth.JWT` (`jose`; JWKS URL with cache/refresh or static keys; checks `iss`, `aud`, `exp`, `nbf`; claims `scope` and `ankusa_tenants` → principal), `Auth.MTLS` (peer cert from SEC-6; SAN/CN → principal via `subject_map`), `Auth.Token` (sha256-hashed tokens with scopes, dev/CI only). Scopes: `metrics:read`, `config:read`, `config:write`, `sources:read`, `sources:write`, `dlq:read`, `dlq:write`, `replays:write`, `quarantine:write`, `routes:read`, `routes:write`, `rate_limits:write`, `claims:read`, `audit:read`, plus `tenant:<id>` or `tenant:*`. Config `admin.auth: %{mode: :jwt | :mtls | :token | :none, ...}`, same for `routes.admin.auth` and `claim_check.auth`; `Config.new/1` refuses `mode: :none` unless the listener binds loopback. Plug mounted before `:match` in all three routers; `/health`, `/ready` exempt; `conn.assigns.principal` set for every handler; 401 with `www-authenticate`, 403 for missing scope. `jose` added to core (`mise run deps`).
  *Done when:* every documented admin/route/claim operation answers 401 without a credential and 403 with the wrong scope; a tenant-scoped JWT cannot read another tenant's source; mTLS identity works against the SEC-6 test CA; non-loopback bind with `mode: :none` fails boot.
  *Proof:* `mix test test/ankusa/admin test/ankusa/routes/router_test.exs test/ankusa/claim_check/router_test.exs test/ankusa/config_test.exs` → pass with `401`/`403`/`tenant scope`/`mtls principal`/`refuses none on 0.0.0.0` cases; `router_openapi_test.exs` updated for the security scheme.
  *Anchors:* `packages/ankusa/lib/ankusa/admin/router.ex:13-22,62-183`, `routes/router.ex:11-19`, `claim_check/router.ex:7-11`, `config.ex:84-87,115,142`, `test/ankusa/admin/router_test.exs:87`.
  *Evidence:* —

- [ ] **SEC-2.a — Audit trail written with the mutation** · P0 · Deps: SEC-1, F6
  *Why:* no audit of who changed what; lifecycle events carry no actor/IP and are best-effort in memory (`source_store.ex:121,177`, `routes.ex:273,542`, `lifecycle.ex:16-31`, `lifecycle/publisher.ex:4-24`). CWE-778.
  *Change:* new column family `audit` (format bump via F6). Row key `<<?a, at::64, seq::64>>`, value `%{actor, remote_ip, action, tenant, resource, before_hash, after_hash, request_id, result, revision}`, written in the **same** `Store.write(sync: true)` batch as the mutation in `SourceStore.Persistent` put/delete, `Routes` write/delete, rate-limit override put/delete, `Quarantine.purge`, `Replay` create/cancel, `PUT /admin/ip-rules`. `revision` is a per-subject monotonic integer stored with the source/route. `GET /v1/audit?since&until&actor&action&tenant&cursor&limit` (scope `audit:read`), `audit.retention_days` (default 365) pruned by the housekeeping tick, telemetry `[:ankusa, :audit]`. `Ankusa.Principal` from SEC-1 supplies `actor`; `ClientIP` supplies `remote_ip`.
  *Done when:* every mutating route produces exactly one audit row that survives `kill -9` immediately after the response; the row never contains a secret; `GET /v1/audit` filters work.
  *Proof:* `mix test test/ankusa/admin/audit_test.exs test/ankusa/store_upgrade_test.exs` → pass (`audit row survives kill`, `no secret in audit`, `filters`), fixture `store-v2` checked in.
  *Anchors:* `packages/ankusa/lib/ankusa/source_store/persistent.ex:170-190`, `routes.ex:170-250,540-560`, `admin/router.ex:145-253`, `store.ex:43`, `store/keys.ex`.
  *Evidence:* —

- [ ] **SEC-2.b — Durable lifecycle outbox with revisions** · P1 · Deps: SEC-2.a
  *Why:* lifecycle events drop on full queue/restart/give-up with no replay and no ordering (`lifecycle/publisher.ex:130-138`); consumers cannot detect stale updates (`lifecycle.ex:118-146`).
  *Change:* `Lifecycle.Publisher` becomes a cursor reader over `audit` rows flagged `publish: true` (cursor `m:lifecycle_cursor`, synced on advance); events carry `revision`, `actor` (redacted to id), `instance`, and the full redacted entity on delete; `Ankusa.Replay` kind `:lifecycle` (`since`/`until`/`tenant`); in-memory queue removed. AsyncAPI schema updated.
  *Done when:* `kill -9` right after `SourceStore.put/5` returns still delivers `source.created` after restart; three rapid updates arrive in revision order; replay re-emits a window.
  *Proof:* `mix test test/ankusa/lifecycle_test.exs test/ankusa/replay_test.exs` → new cases `survives kill`, `ordered by revision`, `lifecycle replay`.
  *Anchors:* `packages/ankusa/lib/ankusa/lifecycle.ex:16-56,118-146`, `lifecycle/publisher.ex`, `dispatch/replayer.ex:454-460`.
  *Evidence:* —

- [ ] **SEC-3 — Egress policy: SSRF deny-list, DNS pinning, TLS verification for every outbound HTTP** · P0 · Deps: —
  *Why:* `HttpClient` passes any URL (`http_client.ex:79-95`); an HTTP sink URL may be `http://127.0.0.1:4003/...` with the webhook body as payload (`ankusa_server/config.ex:1224-1226`, `sink/http.ex:118-124`); IMDS is reachable; `verify_none` is accepted; `castore` is not a locked dep. CWE-918.
  *Change:* `Ankusa.HttpClient.Egress.validate_target(url, policy) :: :ok | {:error, {:egress_denied, reason}}`; policy from `Config` `egress: %{allow_insecure_http: false, deny_cidrs: [127.0.0.0/8, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 100.64.0.0/10, 169.254.0.0/16, ::1/128, fc00::/7, fe80::/10], allow_cidrs: [], allow_hosts: [], deny_hosts: ["metadata.google.internal"]}`; resolve with `:inet.getaddrs/2` for both families and deny if any address is denied and not allowed; pin the checked address for the connection (`connect_options: [hostname: host]` with the IP as the connect target if Req/Mint support it; otherwise re-validate on every attempt and document the TOCTOU window); applied to `Sink.Http` (static and TEN-8 computed URLs), lifecycle sinks, and `AnkusaServer.Config.sinks!` at load (`https` required unless `allow_insecure_http`). TLS: `verify: :verify_peer` always, `cacertfile` option, `{:castore, "~> 1.0"}` locked in core, min TLS 1.2; `verify_none` rejected by `HttpClient.validate!/1` unless `allow_insecure_tls: true` per sink.
  *Done when:* sinks to `127.0.0.1`, `169.254.169.254`, `::1`, RFC1918 and a rebinding stub all fail closed with `{:egress_denied, _}`; an allow-listed internal CIDR passes; `verify_none` fails config load.
  *Proof:* `mix test test/ankusa/http_client_test.exs test/ankusa/sink/http_test.exs packages/ankusa_server/test` → cases `denies loopback`, `denies metadata`, `denies rebinding`, `allows allow_cidrs`, `rejects verify_none`; `mix deps.tree | grep castore` shows it locked.
  *Anchors:* `packages/ankusa/lib/ankusa/http_client.ex:29-37,79-138`, `sink/http.ex:86-124`, `net.ex` (`parse_cidr/1`), `packages/ankusa_server/lib/ankusa_server/config.ex:1224-1226`.
  *Evidence:* —

- [ ] **SEC-4 — Secrets encrypted at rest; SSE-KMS on every object-store write** · P0 · Deps: —
  *Why:* API-managed source specs with `verify.secret` and sink credentials are `JSON.encode!`-ed plaintext in RocksDB (`source_store/persistent.ex:179`, Redis `source_store/redis.ex:37`) and therefore in every backup; no SSE headers on any PUT (`blob_store/s3.ex:69-77`, backup `store/backup.ex:440-448`). CWE-312.
  *Change:* `Ankusa.Crypto.Envelope`: AES-256-GCM via `:crypto.crypto_one_time_aead/7`, value format `<<"AENC", 1::8, key_id_len::8, key_id::binary, iv::96, tag::128, ct::binary>>`; KEK from `secrets.kek` (32-byte base64, `${ENV}`) or `secrets.kek_provider: {mod, fun, args}`; `secrets.kek_previous: [...]` for rotation; `mix ankusa.secrets.rewrap`. Encrypt the secret-bearing fields (reuse the inverse of `Admin.Redact`'s allowlist to find them) before persisting in `Persistent` and `SourceStore.Redis`; decrypt on load; boot refuses when encrypted rows exist and no KEK is configured. Blob stores: option `sse: %{type: :aes256 | :kms, key_id}` → `x-amz-server-side-encryption(-aws-kms-key-id)`, GCS `x-goog-encryption-kms-key-name`, Azure `x-ms-encryption-scope`, OCI `opc-sse-kms-key-id`, sent on every PUT (backups, claims, segments). Startup log: `store files rely on volume encryption; secrets envelope-encrypted with key <id>`.
  *Done when:* a raw `Store.get` of a source row contains no plaintext secret; restart decrypts; rotation keeps old rows readable; the S3 emulator test sees the SSE header on backup, claim and segment PUTs.
  *Proof:* `mix test test/ankusa/source_store_persistent_test.exs test/ankusa/crypto_test.exs` and `mise run test:integration` → cases `no plaintext at rest`, `rewrap`, `sse header on put`.
  *Anchors:* `packages/ankusa/lib/ankusa/source_store/persistent.ex:170-190`, `packages/ankusa_redis/lib/ankusa/source_store/redis.ex:30-50`, `admin/redact.ex:6-44`, `blob_store/{s3,gcs,azure,oci}.ex` put functions, `store/backup.ex:440-448`.
  *Evidence:* —

- [ ] **SEC-5 — Stored-header policy** · P0 · Deps: —
  *Why:* every provider header, `Authorization`/`Cookie`/`x-api-key` included, is stored verbatim in the hook and archived forever; `@never_forwarded` only governs forwarding (`edge/ingest.ex:312-320`, `sink/message.ex:50-53`).
  *Change:* `Config` `http.store_headers: %{drop: ["authorization", "proxy-authorization", "cookie", "set-cookie", "x-api-key", "x-auth-token"], hash: []}` and per-source `store_headers` override; applied in `Ingest.build_envelope/3` **after** verification (the verifier keeps the raw list in memory); hashed names stored as `sha256:<hex>`; dropped count on telemetry metadata.
  *Done when:* a POST with `authorization` + `cookie` verifies, and the header is absent from the stored hook, the segment object and the claim pack; the message to sinks never carries it.
  *Proof:* `mix test test/ankusa/edge_test.exs test/ankusa/storage_test.exs` → cases `drops stored headers`, `segment has no authorization header`.
  *Anchors:* `packages/ankusa/lib/ankusa/edge/ingest.ex:179-220,312-331`, `envelope.ex:11-29`, `sink/message.ex:50-53,145-161`.
  *Evidence:* —

- [ ] **SEC-7 — Tenant-scoped admin reads and writes** · P0 · Deps: SEC-1
  *Why:* DLQ, quarantine, replays and rate-limit routes are node-global with only a `source_id` filter (`admin/router.ex:117-135,188-253`); any reader sees every tenant. CWE-284.
  *Change:* `tenant` filter on `Queue.dead/2`, `Quarantine.recent/purge`, `Replay` filters and rate-limit overrides; a principal with `tenant:<id>` scopes gets rows only for those tenants (filter forced server-side, `403` if the request names another tenant); `tenant:*` required for unfiltered reads. Until TEN-1 lands, tenant filtering scans rows' `tenant_id`; TEN-1 replaces the scan with the index.
  *Done when:* a tenant-A principal listing `/v1/dlq` sees only tenant-A rows; naming tenant B is 403; an operator sees all.
  *Proof:* `mix test test/ankusa/admin/router_test.exs` → cases `tenant scoped dlq`, `tenant scoped quarantine purge`, `tenant scoped replay`.
  *Anchors:* `packages/ankusa/lib/ankusa/admin/router.ex:117-135,165-253`, `queue/deliveries.ex:509-575`, `edge/quarantine.ex:38-44`, `replay.ex:40-49,129-170`.
  *Evidence:* —

- [ ] **SEC-8 — Supply chain: digest pins, SBOM, provenance, signing; small verifier hardening** · P1 · Deps: —
  *Why:* base images pinned by tag (`packages/ankusa_server/Dockerfile:10,48`), actions by major tag (`ci.yml:22-31`), no SBOM/provenance/signature in `docker.yml`; `tolerance_seconds` has no range check (`ankusa_server/config.ex:1061`); custom `hmac` permits `sha1` silently (`verifier/schemes.ex:54`).
  *Change:* `FROM …@sha256:` for both stages; pin every GitHub action to a commit SHA; `docker/build-push-action` with `sbom: true`, `provenance: mode=max`; `cosign sign --yes` keyless and `cosign verify` in `scripts/smoke.sh`; `mix hex.audit` output uploaded as a CI artifact; `tolerance ≤ 3600` in `Hmac.validate_opts/1` and the YAML loader; `sha1` logs a boot warning and increments `ankusa_verifier_weak_hash_total`.
  *Done when:* a tag build publishes SBOM + attestation and `cosign verify` passes in smoke; `tolerance: 3601` is a config error.
  *Proof:* `docker.yml` run log shows attestation + SBOM artifacts; `mise run docker:smoke` → `cosign verify: OK`; `mix test packages/ankusa_server/test/ankusa_server/config_test.exs` → `tolerance range` case.
  *Anchors:* `packages/ankusa_server/Dockerfile`, `.github/workflows/docker.yml`, `.github/workflows/ci.yml`, `packages/ankusa/lib/ankusa/verifier/hmac.ex:276-294`, `verifier/schemes.ex:54`.
  *Evidence:* —

- [ ] **SEC-9 — Secret sources and rotation without redeploy** · P1 · Deps: —
  *Why:* secrets enter only via `${VAR}` (`ankusa_server/config.ex:10-14,222-257`); no file or secret-manager source; static-source rotation needs a redeploy.
  *Change:* `${file:/path}` interpolation and `secret_file:` siblings for `secret:`; behaviour `Ankusa.SecretProvider` with `c:fetch(ref) :: {:ok, binary} | {:error, :unavailable | :not_found}`; `${sm:<provider>:<ref>}` resolved by providers registered in `ankusa_server` (`AWS Secrets Manager`, `Vault` KV v2; both via `Req`, with cache TTL); re-resolution on reload (OPS-6); `SourceStore.Static` re-publishes updated specs; secret lists (`secret: [new, old]`) remain the rotation window.
  *Done when:* a secret file swap + reload verifies with the new secret while the old stays valid until removed; a provider outage at boot is a clear `ConfigError`, at reload a warning that keeps the old secret.
  *Proof:* `mix test packages/ankusa_server/test` → cases `file secret`, `sm ref`, `rotation via reload`.
  *Anchors:* `packages/ankusa_server/lib/ankusa_server/config.ex:10-14,222-257`, `packages/ankusa/lib/ankusa/source_store.ex:208-224`.
  *Evidence:* —

- [ ] **SEC-10 — HTTP hand-off signed by default, verified by every SDK receiver** · P0 · Deps: —
  *Why:* `Sink.Http` signs only with `:secret` and `Ankusa.SDK.Receiver` accepts anything without one (`sink/http.ex` `signature/2`, `sdk-elixir/lib/ankusa/sdk/receiver.ex:7-10,117-119`); any host that reaches a consumer URL can inject hooks for any tenant.
  *Change:* `Sink.Http` requires `secret` unless `allow_unsigned: true` (boot/`400 invalid_source` otherwise); every SDK receiver (check each `packages/sdk-*` for one) requires a secret unless `allow_unsigned: true` and answers 401 on a bad/missing signature; conformance feature `webhook.require_signature` with cases (unsigned → 401, signed → 2xx, `allow_unsigned` → 2xx).
  *Done when:* an unsigned POST to any SDK receiver is 401 by default; `mise run check:conformance` passes for all nine SDKs.
  *Proof:* `mise run check:conformance` → all SDKs pass `webhook.require_signature`; `mix test test/ankusa/sink/http_test.exs` → `requires secret`.
  *Anchors:* `packages/ankusa/lib/ankusa/sink/http.ex`, `packages/sdk-elixir/lib/ankusa/sdk/receiver.ex`, `conformance/features.json`, `conformance/cases/`.
  *Evidence:* —

- [ ] **SEC-11 — Claim gateway authorization and signed, expiring refs** · P0 · Deps: SEC-1
  *Why:* the gateway has no auth/TLS/rate limit and refs are unexpiring bearer strings with 64 random bits whose tenant is only a path segment (`claim_check/router.ex:1-45`, `claim_check/ref.ex:71-88`); a leaked queue message grants reads until retention.
  *Change:* `claim_check.authorize: {mod, fun, args}` called before `locate` with `%{principal, tenant_id, claim_id, conn}`; default (when `claim_check.auth.mode != :none`) requires `claims:read` and `tenant:<t>`/`tenant:*`. Ref v2: `urn:ankusa:claim:v2:<tenant>:<claim_id>:<exp_unix>:<base64url hmac-sha256>` signed with `claim_check.ref_signing_keys: [current, previous]`, TTL `claim_check.ref_ttl_ms` (default = retention); `Ref.parse/1` accepts v1 only with `claim_check.accept_unsigned_refs: true`; expired → `410`; `claim_check.max_concurrent_reads` shedding with `503` + `Retry-After`. SDKs parse v2 (conformance `ref.v2` cases).
  *Done when:* tenant-A credential on a tenant-B path is 403; an expired signed ref is 410; a tampered signature is 400; all nine SDKs parse v2.
  *Proof:* `mix test test/ankusa/claim_check` and `mise run check:conformance` → pass with the new cases.
  *Anchors:* `packages/ankusa/lib/ankusa/claim_check/router.ex`, `claim_check/ref.ex:30-130`, `claim_check.ex:200-240`, `conformance/cases/redeem.json`.
  *Evidence:* —

- [ ] **SEC-12 — Verifier breadth: asymmetric, token/basic, client-certificate** · P0 · Deps: SEC-6
  *Why:* only `Verifier.Hmac` and `Verifier.None` exist (`verifier/schemes.ex:8-11`); providers using Ed25519/RSA (Standard Webhooks `v1a,`), static bearer/basic auth, or mTLS client certs force `None` + `accept_flag`, i.e. unverified data.
  *Change:* extract `Ankusa.Verifier.Template` (`candidates/2`, `timestamp/4`, `signed_bytes/3`) from `Hmac`; add `Verifier.Asymmetric` (`:crypto.verify/5` with `:eddsa`/`:rsa` + sha256, keys as PEM/base64, key list for rotation, preset `standard_webhooks_asym`), `Verifier.Token` (header/basic/query token compared with `secure_compare/2`), `Verifier.ClientCert` (peer cert from SEC-6: SAN or fingerprint allowlist via `raw_request.peer_cert`). YAML schemes in `ankusa_server/config.ex`; conformance case for the asymmetric vector.
  *Done when:* the Standard Webhooks asymmetric reference vector verifies; a wrong key, stale timestamp and tampered body fail; a client-cert source rejects a request without the pinned cert.
  *Proof:* `mix test test/ankusa/verifier_test.exs test/ankusa/edge_test.exs` → cases per verifier; `mise run check:conformance` passes.
  *Anchors:* `packages/ankusa/lib/ankusa/verifier/hmac.ex:74-93,240-250`, `verifier/schemes.ex:16-60`, `verifier.ex:45-50,181-191`, `edge/ingest.ex:179-196`.
  *Evidence:* —

### Phase 2 — Tenant isolation at 10⁶ tenants

- [ ] **TEN-1.a — ADR: tenant in the store key layout** · P0 · Deps: F6 `[design-gate]`
  *Why:* hooks/deliveries/due/inflight/dead/archive keys carry no tenant (`store/keys.ex:30-43,48-78`); counting, bounding or purging one tenant's rows is a full scan; gauges fold whole index families every 15 s (`metrics/gauges.ex:105-124`).
  *Change:* ADR deciding: dense tenant ordinal (`tenants` CF: `ord → tenant_id`, `tenant_id → ord`, assigned by the single writer at first sight), due key `<<?d, ord::32, at::64, seq::64, sink::16>>`, inflight/dead likewise, a `counters` row per tenant (`pending, scheduled, inflight, dead, bytes, archive_pending`) maintained in every transition batch, per-tenant scan cursors for TEN-3, migration v2→v3 rewriting index rows in bounded resumable batches, and the rollback boundary.
  *Done when:* `docs/adr/0001-tenant-key-layout.md` merged with Proof section naming TEN-1.b's tests.
  *Proof:* PR merged; `mise run check:links` passes.
  *Anchors:* `packages/ankusa/lib/ankusa/store/keys.ex`, `queue/deliveries.ex:66-93,180-250,342-352`, `metrics/gauges.ex:7-21,105-124`.
  *Evidence:* —

- [ ] **TEN-1.b — Implement the tenant key layout and counters** · P0 · Deps: TEN-1.a
  *Change:* as decided in the ADR; `Queue.pending_for_tenant/2`, `dead_for_tenant/2`, `purge_tenant/3`; `Gauges` read counters instead of folding; migration registered in F6 with fixture `store-v3`; `metrics` gain `ankusa_tenant_pending{tenant}` for the top-100 tenants by pending.
  *Done when:* per-tenant counts are O(1); the gauge tick cost is independent of backlog size; the fixture upgrade passes; `loss_test`, `dispatch_test`, `queue_test` pass unchanged in semantics.
  *Proof:* `mix test` → pass; `mix run bench/gauges_bench.exs` (new) → tick time flat from 10³ to 10⁶ rows; `mise run bench:gate` → PASS.
  *Anchors:* ADR 0001, `queue/deliveries.ex`, `queue/writer.ex:466-511`, `store/migrate.ex`.
  *Evidence:* —

- [ ] **TEN-2 — Per-tenant, per-source, per-IP and node-global admission at the edge** · P0 · Deps: —
  *Why:* rate limits are hooks/s per tenant only, charged after the body is read and verified (`edge/rate_limiter.ex:4-24`, `ingest.ex:197-220`); default unlimited (`config.ex:160`); batcher queues (`10k`/`256 MiB` per partition) and dispatch windows are global, so one tenant fills them and everyone gets `503 overload` (`batcher.ex:149-162`, `router.ex:170-176`); forged 8 MB POSTs cost a full read + HMAC before any limiter (`ingest.ex:179-196`).
  *Change:* pre-read limiters in `Router.capture` after `Ingest.lookup/2`: per-IP GCRA (`rate_limits.per_ip: %{rate, burst}`, key from `ClientIP`), node-global (`rate_limits.node`), per-source bytes/s ceiling (`rate_limits.sources.default.bytes_per_second`, generous default 100 MB/s, pre-verification so documented as CPU protection, not quota); post-verification quota per tenant gains `bytes_per_second`, `burst_bytes`, `max_body_bytes`, `max_queue`, `max_queue_bytes` under `rate_limits.tenants[t]` and `rate_limits.default`; per-tenant queue counters in ETS (`RateLimiter.hit_bucket/4` CAS pattern) charged in `Batcher.commit` and released on reply; exhaustion is `429 tenant_overload` + `Retry-After`, never the global `503`; a finite `rate_limits.default` is required when `routing: tenant_path`. Chunked bodies charge bytes at read end.
  *Done when:* tenant A flooding past its queue share keeps getting `429` while tenant B gets `201`; the per-IP limiter answers `429` before the body is read; `max_body_bytes` per tenant yields `413`.
  *Proof:* `mix test test/ankusa/edge_rate_limit_test.exs test/ankusa/edge_test.exs` → cases `tenant overload isolates`, `per ip pre-read`, `tenant body cap`; `mise run bench:gate` → PASS.
  *Anchors:* `packages/ankusa/lib/ankusa/edge/router.ex:56-110,170-176`, `edge/rate_limiter.ex:26-46,233-295`, `edge/batcher.ex:76-83,149-162`, `edge/ingest.ex:179-220`, `config.ex:27-42,158-160`.
  *Evidence:* —

- [ ] **TEN-3 — Dispatch fairness by tenant** · P0 · Deps: TEN-1.b
  *Why:* claims are a global oldest-due scan (`queue/deliveries.ex:67-83`) under global `concurrency 32` / `max_inflight 4096` / `128 MiB` (`config.ex:48-70`); the scheduling key `{source_id, sink index, module}` has no tenant (`dispatch/pipeline.ex:627`); `sink_concurrency` default `nil`.
  *Change:* deficit round-robin across tenants from the `tenants` CF with per-tenant due cursors; `dispatch.tenants.default` and `dispatch.tenants[t]`: `max_inflight`, `max_inflight_bytes`, `concurrency`; key becomes `{tenant_id, source_id, sink_index, module}` so breakers and `sink_concurrency` (new default 8) are per tenant; gauges `ankusa_dispatch_tenant_inflight{tenant}` top-100.
  *Done when:* 99:1 load across two tenants with tenant A's sink slow: tenant B's p99 delivery lag ≤ 2× its solo p99 and A never exceeds its window.
  *Proof:* `mix test test/ankusa/dispatch/isolation_test.exs` → `tenant fairness` case; `mix run bench/tenant_fairness_bench.exs` (new) prints both tenants' p99.
  *Anchors:* `packages/ankusa/lib/ankusa/dispatch/pipeline.ex:501-512,627,936-1140`, `queue/deliveries.ex:66-93`, `config.ex:47-70`.
  *Evidence:* —

- [ ] **TEN-4 — Tenant-aware routes bound to sources** · P0 · Deps: —
  *Why:* routes have no tenant or source field (`routes/route.ex:40-49`); the guard's `route_id` is discarded by ingest (`edge/route_guard.ex:125-130`, `edge/router.ex:76-84`); route lifecycle events hardcode tenant `"default"` (`lifecycle.ex:46-47,126-133`); any route caller can write routes for the whole fleet.
  *Change:* `tenant_id` (default `"default"`) and optional `source_id` on `Route` (`from_attrs/to_json/from_json`), in snapshot keys and the `{tenant, id}` ordered index; routes API paths become `/v1/tenants/:tenant/routes[...]` (OpenAPI updated, `router_openapi_test.exs` regenerated); `RouteGuard` passes `route_id`/`tenant_id`/`source_id` into `Ingest`; a resolver result that disagrees with the route's binding is `404`; lifecycle route events carry the route's tenant; SEC-1 scopes enforce `tenant:<t>` on the route API.
  *Done when:* a route owned by tenant A cannot admit a hook into tenant B's source; route events name the tenant; cross-tenant route CRUD is 403.
  *Proof:* `mix test test/ankusa/routes test/ankusa/edge_route_guard_test.exs test/ankusa/lifecycle_test.exs` → pass with `route binds source`, `tenant route event`.
  *Anchors:* `packages/ankusa/lib/ankusa/routes/route.ex:40-105`, `routes/snapshot.ex:24-45,120-132`, `routes/router.ex`, `edge/route_guard.ex:120-140`, `edge/router.ex:56-84`, `lifecycle.ex:39-56`.
  *Evidence:* —

- [ ] **TEN-5 — Tenant erase** · P0 · Deps: TEN-1.b, SEC-2.a
  *Why:* no tenant-delete; source delete only dead-letters rows (`admin/router.ex:365-410`); quarantine, dedupe, claims, overrides and routes are never purged by tenant (`quarantine.ex:38-44`, `claim_check/sweeper.ex:9-13`).
  *Change:* `Ankusa.Tenant.delete/3` + `DELETE /v1/tenants/:tenant?confirm=<tenant>` (scope `tenant:*`): resumable job with progress in `m:erase:<tenant>` deleting sources, pending/inflight/dead rows via the tenant index (reclaiming hooks), quarantine rows, `claims/tenant=<t>/` prefix, dedupe keys (`tenant\0` prefix `delete_range`), rate-limit overrides, routes; writes a tombstone set `m:erased:<tenant>` consulted by `Storage.fetch` and archive replay (segments are multi-tenant and expire by lifecycle); one audit row + lifecycle `io.ankusa.tenant.deleted`.
  *Done when:* after erase, no store key, claim object or route references the tenant; a fetch of an erased hook id is `410`; the job resumes after `kill -9`.
  *Proof:* `mix test test/ankusa/tenant_test.exs` → `erase leaves no trace`, `resumes`; `mise run test:integration` → claims prefix deleted on S3.
  *Anchors:* `packages/ankusa/lib/ankusa/admin/router.ex:145-182,365-410`, `queue/reclaim.ex`, `edge/quarantine.ex:38-44`, `claim_check/sweeper.ex`, `dedupe.ex`.
  *Evidence:* —

- [ ] **TEN-6 — Fleet-scoped rate limits and dedupe (`ankusa_redis`)** · P0 · Deps: TEN-2
  *Why:* limits and dedupe are node-local, so N replicas admit N× the quota and a provider retry on another node is a duplicate commit (`rate_limiter.ex:29-31`, `docs/configuration.md:514-517`).
  *Change:* behaviours `Ankusa.RateLimiter.Store` (ETS default; `Ankusa.RateLimiter.Store.Redis` GCRA in Lua, same semantics) and `Ankusa.Dedupe.Store` (`Redis` `SET NX PX` ahead of the writer's local check); config `rate_limits.scope: :node | :fleet`, `dedupe.scope: :node | :fleet`; degraded mode when Redis is unavailable: fall back to node scope, set gauge `ankusa_fleet_limiter_degraded{kind}` = 1, log once per minute — never refuse ingest for a limiter outage.
  *Done when:* two nodes enforce one tenant's 10 rps combined; the same dedupe key posted to each node yields one hook; Redis down degrades, never 503s.
  *Proof:* `mise run check:package ankusa_redis` → `fleet limiter`, `fleet dedupe`, `degrades without redis` cases; F5 nightly reports `extra_deliveries: 0`.
  *Anchors:* `packages/ankusa/lib/ankusa/edge/rate_limiter.ex`, `dedupe.ex`, `queue/writer.ex:330-341`, `packages/ankusa_redis/lib/ankusa/source_store/redis.ex` (Lua/pubsub patterns).
  *Evidence:* —

- [ ] **TEN-7 — Per-tenant observability with a cardinality policy** · P1 · Deps: TEN-1.b
  *Why:* metrics are tagged by `source_id` with no cap (`metrics.ex:86-110,140-178`); the only tenant tag is `rate_limit.rejected`; at 10⁶ sources the scrape is unbounded.
  *Change:* `metrics.detail: :tenant | :source | :none` (default `:tenant`); `tenant_id` on ingest/commit/verify/dispatch/quarantine metadata; `metrics.max_series_per_metric` (default 5 000) with an `other` bucket; `source_id` only under `:source`; top-N tenant gauges from TEN-1 counters.
  *Done when:* 10 000 sources × 1 request each keep series ≤ the cap and scrape < 100 ms; `metrics_test` asserts `tenant_id` present and `source_id` absent by default.
  *Proof:* `mix test test/ankusa/metrics_test.exs` → `bounded series under 10k sources`; `curl /metrics | wc -l` in the soak job ≤ 10 000.
  *Anchors:* `packages/ankusa/lib/ankusa/metrics.ex:84-312`, `telemetry.ex:9-46`.
  *Evidence:* —

- [ ] **TEN-8 — Per-tenant HTTP destinations on shared sources** · P1 · Deps: SEC-3
  *Why:* `Sink.Http` URL/headers are static (`sink/http.ex:89-120`), so a shared source cannot route tenants to their own endpoints without one source per tenant.
  *Change:* `:url` and `:headers` accept `{mod, fun, args}` or a 1-arity fun of `%Envelope{}` (as `GooglePubSub` does for `ordering_key`); resolved per attempt; every resolved target passes SEC-3; `Sink.Description` reports `computed`.
  *Done when:* a shared source with two tenants delivers each hook to its tenant's receiver; a computed target in a denied CIDR fails closed.
  *Proof:* `mix test test/ankusa/sink/http_test.exs` → `computed url per tenant`, `computed url denied`.
  *Anchors:* `packages/ankusa/lib/ankusa/sink/http.ex:86-124`, `sink/google_pubsub.ex:159-165`, `sink/description.ex`.
  *Evidence:* —

- [ ] **TEN-9 — Routes and source stores at 10⁶ entries** · P1 · Deps: TEN-4
  *Why:* a cache miss is a linear ETS walk over every pattern (`routes/snapshot.ex:485-514`); Redis load is one `HGETALL` under a 5 s timeout and one giant `:ets.insert` (`ankusa_redis/.../routes/store/redis.ex:466-485,202-203`, `snapshot.ex:120-132`); `max_routes` 10 000 (`config.ex:122-125`); create and list are O(n) (`routes.ex:125-152,600-607`).
  *Change:* first-literal-segment index `{{:seg0, gen, literal}, [priority_key]}` plus a param/wildcard list; `HSCAN COUNT 1000` streaming load with batched inserts and yields; `routes.store.timeout_ms` (load uses its own larger timeout); `{compiled_pattern, method}` conflict index; `list` via `:ets.select` continuation; `max_routes` default raised to 1 000 000 behind `routes.memory_budget_bytes` checked at boot; same streaming for `SourceStore.Redis`.
  *Done when:* 1 M routes boot in < 60 s and reload without a store timeout; miss p99 < 1 ms with the cache disabled; create/list time does not grow with n.
  *Proof:* `mix run bench/routes_bench.exs` (new; 10⁶ routes, 10⁵ distinct paths) prints boot/reload time and miss p99 within budget; `mise run check:package ankusa_redis` passes.
  *Anchors:* `packages/ankusa/lib/ankusa/routes/snapshot.ex:120-132,485-528`, `routes.ex:125-152,452-470,600-607`, `routes/cache.ex:2-3,60-72`, `packages/ankusa_redis/lib/ankusa/routes/store/redis.ex:202-203,466-485`.
  *Evidence:* —

### Phase 3 — Durability and capacity

- [ ] **DUR-6 — Backup verification, signed manifests, final checkpoint on stop, cross-region secondary** · P0 · Deps: —
  *Why:* restore happens only at boot (`store.ex:138-159`); the manifest is unauthenticated JSON (`store/backup.ex:714-746`); no verify job, no age threshold, no shutdown checkpoint (`backup.ex:75-137` has no `terminate/2`), one bucket, one region.
  *Change:* `Backup.verify/2` (list `LATEST`, stream objects, check size+sha256, no writes) on `backup.verify_interval_ms` emitting `ankusa_backup_verify_failed_total`; HMAC-SHA256-signed manifest with `backup.signing_key` (list for rotation), verified on restore; `terminate/2` runs one final checkpoint under `backup.final_timeout_ms` (default 20 s); `backup.timeout_ms` per upload; `backup.max_age_ms` → `/ready` label `backup: stale` (unready only with `backup.require_fresh: true`); `backup.secondary` (second blob store, dual-write, read fallback at restore); the checkpoint records `m:intake_offset` for DUR-1.
  *Done when:* a flipped byte fails verify; restore refuses an unsigned/mis-signed manifest; a clean stop leaves `LATEST` newer than the last periodic checkpoint; restore from the secondary bucket reproduces every acked id.
  *Proof:* `mix test test/ankusa/store/backup_test.exs` and `mise run test:integration` → cases `verify detects corruption`, `manifest signature`, `final checkpoint on stop`, `secondary restore`.
  *Anchors:* `packages/ankusa/lib/ankusa/store/backup.ex:75-137,314-345,551-590,608-665,714-746`, `store.ex:138-159`, `config.ex:101-111`.
  *Evidence:* —

- [ ] **DUR-1.a — ADR: dual-commit intake log (`wal.type: dual`)** · P0 · Deps: F5, DUR-6 `[design-gate]`
  *Why:* an acked hook survives only its host; with `backup.enabled` a lost host loses everything acked since the last checkpoint (`architecture.md:37-44`, `store.ex:224-231`). Decision taken: ack only after local fsync *and* a confirmed publish to a replicated broker intake topic.
  *Change:* ADR deciding: behaviour `Ankusa.Intake` (`c:publish_batch(instance, [{seq, bin}], opts) :: :ok | {:error, class}`, `c:replay(instance, from, fun)`, `c:ready?/1`); commit order = intake publish (seqs pre-assigned by the writer) → local synced write → `2xx`, local failure after intake success answers `503` and recovery reconciles idempotently by hook id; offset checkpointing `m:intake_offset` in every backup; recovery `mix ankusa.recover --node <id> --from <offset>` into a replacement node; first adapter `Ankusa.Intake.Kafka` (`ankusa_kafka`, topic `ankusa.intake.<fleet>`, key = node id, `acks=all`, seq in headers), second `Ankusa.Intake.JetStream` (`ankusa_nats`, R3 stream, `Nats-Msg-Id = <node>:<seq>`); readiness includes `intake`; the startup log sentence; loss window per mode in `architecture.md`.
  *Done when:* `docs/adr/0002-intake-log.md` merged.
  *Proof:* PR merged; `mise run check:links` passes.
  *Anchors:* `packages/ankusa/lib/ankusa/queue/writer.ex:99-150,500-511`, `queue.ex:46-77`, `store/backup.ex`, `packages/ankusa_kafka/lib/ankusa/sink/kafka.ex`, `packages/ankusa_nats/lib/ankusa/sink/nats.ex`.
  *Evidence:* —

- [ ] **DUR-1.b — Implement `wal.type: dual` with the Kafka intake adapter and recovery** · P0 · Deps: DUR-1.a
  *Change:* as decided in the ADR; metrics `ankusa_intake_publish_duration_seconds`, `ankusa_intake_failures_total{class}`; `Health.ready` includes `intake`; `recover` mix task; docs (`architecture.md` core invariant, `storage.md`, `deployment.md`).
  *Done when:* in the F5 fleet, with `wal.type: dual`, `docker kill` of a node under 1 k req/s followed by a replacement restored from backup + `recover` yields every `201` id in the consumer table, `extra_deliveries` bounded by `idempotency_key`; the intake broker down → `503 intake_unavailable`, nothing acked.
  *Proof:* `examples/fleet-e2e/run.sh --wal dual --kill-node` → `missing=0`; `mix test test/ankusa/queue_dual_test.exs` → `no ack without intake confirm`.
  *Anchors:* ADR 0002 and the DUR-1.a anchors.
  *Evidence:* —

- [ ] **DUR-2 — Ceilings and retention for every unbounded structure** · P0 · Deps: F6
  *Why:* DLQ rows are never deleted and have no config (`queue/deliveries.ex:343-352`); dedupe keys grow at ingest rate × 72 h with a 2 000-key/10 s sweep inside the writer (`queue/writer.ex:358-435`); the archive catalogue is only ever put (`queue/archive.ex:66-70`); quarantine has a byte cap but no age (`config.ex:164`); claim packs can be swept while a queued message still references them (`queue/reclaim.ex:75-80`, `claim_check/sweeper.ex:75-100`).
  *Change:* `queue.dlq: %{retention_days: 30, max_rows: 10_000_000, max_bytes: 50 GiB}` enforced by `Ankusa.Queue.DlqReaper` on the housekeeping tick (oldest-first, hook reclaim in the same batch) and `DELETE /v1/dlq?tenant&source_id&since&until&limit`; `quarantine.retention_days: 14`; `storage.catalogue_retention_days` deleting `?S`/`?L` rows (and LocalFS segments via `archive.retention_days`); dedupe: own supervised `Ankusa.Queue.DedupeSweeper`, expiry keys bucketed by hour so expiry is one `delete_range` per bucket, `ingest.dedupe_max_entries` (global and per tenant) answering `503 dedupe_quota` (never silently skipping dedupe); claim leases `Keys.claim_lease(tenant, pack_id)` set at check-in and cleared when the last referencing row is delivered or dead, the sweeper deleting only unleased packs; gauges `ankusa_dlq_rows`, `ankusa_dedupe_keys`, `ankusa_claim_leases`, counters `*_pruned_total`. The bucketed expiry keys and the lease keys are a key-layout change: register the migration and check in its fixture through F6's registry.
  *Done when:* a "sink down 6 h at 10 k/s" simulation keeps disk flat after retention; the sweep never runs in the writer; a leased pack survives its date cutoff and is deleted after the lease clears.
  *Proof:* `mix test test/ankusa/queue_test.exs test/ankusa/dedupe_test.exs test/ankusa/claim_check/sweeper_test.exs` → `dlq reaper`, `dedupe bucket sweep`, `lease blocks sweep`; soak job shows flat `ankusa_dlq_rows` after retention.
  *Anchors:* `packages/ankusa/lib/ankusa/queue/deliveries.ex:342-352,452-505`, `queue/writer.ex:358-435`, `queue/archive.ex:66-70`, `queue/reclaim.ex:75-80`, `edge/quarantine.ex:16-18,358`, `claim_check/sweeper.ex:75-130`, `store/keys.ex:44-57`.
  *Evidence:* —

- [ ] **DUR-3 — RocksDB tunables and index-family filters** · P1 · Deps: —
  *Why:* every RocksDB option is hard-coded (`store.ex:45,67-79,596-632`): 64 MiB block cache, no bloom filter, no `max_open_files`, no compaction style or rate limiter; dedupe `multi_get` misses hit several SSTs per key at 10⁸ keys.
  *Change:* `store.rocksdb: %{block_cache_bytes, bloom_bits_per_key: 10, max_open_files: 65_536, compaction_style, rate_limiter_bytes_per_sec, max_background_jobs, write_buffer_bytes: %{hooks, deliveries, index, ...}}` threaded through `descriptors/1` and `@db_opts`; defaults add bloom + `whole_key_filtering` + `cache_index_and_filter_blocks` on `index` and `deliveries`, a prefix extractor on dedupe keys; `store_test` reads the `OPTIONS-*` file back for each.
  *Done when:* with 10⁸ pre-seeded dedupe keys commit p99 improves vs. baseline and the number is recorded; every option round-trips through `OPTIONS-*`.
  *Proof:* `mix test test/ankusa/store_test.exs` → `effective settings` extended; `mix run bench/dedupe_bench.exs` (new) prints p99 before/after.
  *Anchors:* `packages/ankusa/lib/ankusa/store.ex:45,67-79,596-632`, `test/ankusa/store_test.exs:318-345`, `config.ex`.
  *Evidence:* —

- [ ] **DUR-4 — Cross-partition group commit and writer offload** · P1 · Deps: F1
  *Why:* each partition's batch is one serialized `multi_get` + row encoding + `sync: true` write inside one `Queue.Writer` GenServer (`queue/writer.ex:99-150,330-341,466-511`); a second partition only queues behind the first's fsync; no throughput measurement above 512 B bodies.
  *Change:* the batcher's task performs the dedupe `multi_get` and row encoding and hands prebuilt ops to the writer; the writer drains its mailbox, merges all queued batches into one synced write (cap `batcher.group_commit_max_bytes`, default 64 MiB), assigns seqs, replies to every caller; `batcher.partitions` default 4. `bench/store_commit.exs` measures commits/s and ack p99 at 1/5/10 ms simulated fsync.
  *Done when:* ≥ 2× ingest throughput at 10 KB bodies with 4 partitions vs. the F1 baseline; seqs stay dense and increasing; `loss_test` and F2 pass.
  *Proof:* `mise run bench:gate` → PASS with the new numbers in the PR; `mix test test/ankusa/queue_test.exs test/ankusa/loss_test.exs` pass; `mise run test:crash` → `missing=0`.
  *Anchors:* `packages/ankusa/lib/ankusa/queue/writer.ex:99-150,330-341,466-511`, `edge/batcher.ex:172-200,260-300`, `queue.ex:46-77`.
  *Evidence:* —

- [ ] **DUR-5 — Store failure telemetry and reopen backoff** · P1 · Deps: —
  *Why:* `handle_cast({:write_failed, …})` and `reopen_db/1` emit nothing (`store.ex:415-445,508-525`); a store closed for hours is visible only through `/ready` and ingest counters.
  *Change:* events `[:ankusa, :store, :write_failed]`, `:reopened`, `:unavailable` with reason class; metrics `ankusa_store_write_failures_total{class}`, `ankusa_store_reopen_total`, `ankusa_store_unavailable_seconds`, `ankusa_store_wal_replay_seconds`; reopen backoff 5 s → 60 s with jitter; `/ready` reason labels.
  *Done when:* the F2 ENOSPC drill shows the counters rising and `/ready` naming the reason, then recovery.
  *Proof:* `mix test test/ankusa/store_test.exs test/ankusa/metrics_test.exs` → `write failure telemetry`, `reopen backoff`; `mise run test:crash` log shows the series.
  *Anchors:* `packages/ankusa/lib/ankusa/store.ex:48-62,345-350,415-445,508-525,593-594`, `metrics.ex:113-128`.
  *Evidence:* —

- [ ] **DUR-7 — Object-store adapters: conditional puts, jittered retries, checksums, multipart, pools, credential refresh** · P0 · Deps: —
  *Why:* packs are written with an unconditional PUT so a reused/colliding pack id overwrites referenced claims (`blob_store/s3.ex:69-77`, `claim_check/ref.ex:71-81`); retries are fixed 200 ms/1 s/5 s with no jitter, no `Retry-After`, no breaker (`blob_store/retry.ex:22-23,46-53`); one 10 s timeout covers a 16 MiB upload with no multipart (`claim_check.ex:88-97`); no checksum headers; Req's default pool is the throughput ceiling; OCI session tokens never refresh and GCS has no metadata-server token source.
  *Change:* `BlobStore.put_if_absent/4` (S3 `If-None-Match: *`, GCS `ifGenerationMatch=0`, Azure `If-None-Match: *`, OCI `if-none-match: *`) → `{:error, :already_exists}`; `check_in/4` retries with a fresh pack id (max 3); `Retry-After` honoured via `request_with_headers/6`, full jitter, per-`{endpoint, bucket}` breaker (`blob_store.breaker_failures`); `upload_timeout_ms` (default 60 s) and multipart/resumable/block-list above `multipart_threshold_bytes` (default 8 MiB); `x-amz-checksum-sha256`/`Content-MD5`/`x-goog-hash` on PUT and verified on GET; adapter options `pool_size`, `pool_count`, `pool_timeout` creating a named Finch pool per endpoint under the instance; `BlobStore.OCI.InstancePrincipal` and `BlobStore.GCS.MetadataServer` token providers with refresh on 401.
  *Done when:* a pre-existing key makes `check_in` pick a new id with the original intact; `Retry-After: 2` is honoured; a 16 MiB pack uploads via multipart under a 1 MiB/s stub; a corrupted GET is `{:error, :integrity}`; expired OCI/GCS tokens refresh transparently.
  *Proof:* `mix test test/ankusa/blob_store_retry_test.exs test/ankusa/blob_store_*_test.exs test/ankusa/claim_check_test.exs` and `mise run test:integration` → the five new cases.
  *Anchors:* `packages/ankusa/lib/ankusa/blob_store.ex`, `blob_store/retry.ex:22-60`, `blob_store/s3.ex:62-160`, `blob_store/{gcs,azure,oci}.ex`, `claim_check.ex:61,88-141`, `aws/credentials.ex`.
  *Evidence:* —

- [ ] **DUR-8 — Segment format header, safe index decode, dated archive layout, time-based roll** · P1 · Deps: F6
  *Why:* segments are `<<len::32, crc32::32, payload>>` with no magic/version/codec (`codec/raw.ex:20-47`); `.idx` is `term_to_binary` read with bare `binary_to_term/1` (`storage/compactor.ex:195-205`, `storage.ex:42-56`); the shipped layout is `seg/<first>-<last>.seg` under a node prefix, so a fresh node cannot find another node's segments without the local catalogue; `storage.roll_ms` is dead config (`replay.ex:260-265`).
  *Change:* segment header `"ANKSEG" <> <<version::8, codec_id::8, count::32>>` and trailing sha256; catalogue row records version/codec (a row-format change: register the migration and fixture through F6's registry); `.idx` as versioned JSON (or `binary_to_term(bin, [:safe])` with a version prefix); layout `archive/v1/dt=<yyyy-mm-dd>/<node>/seg-<first>-<last>.seg` (+ `.idx`, `_manifest.json`); `Archive.reconcile/1` rebuilds catalogue rows from the prefix (reuse the restore reconcile); `Storage.fetch` falls back to a day-prefix scan when the catalogue misses; implement time-based roll (`storage.roll_ms`) and `storage.max_segments_per_tick`; legacy `seg/` segments stay readable.
  *Done when:* golden vectors per version decode; a tampered segment/idx returns `{:error, _}`; a second node reconciles the first's archive and serves `Storage.fetch`; a partial segment rolls after `roll_ms`.
  *Proof:* `mix test test/ankusa/storage_test.exs test/ankusa/codec_test.exs` → `versioned header`, `safe idx`, `reconcile other node`, `time roll`.
  *Anchors:* `packages/ankusa/lib/ankusa/codec/raw.ex`, `storage/compactor.ex:90-97,150-205`, `storage.ex:42-56`, `store/backup.ex` (reconcile), `replay.ex:260-265`.
  *Evidence:* —

- [ ] **DUR-9 — Quarantine off the serial GenServer** · P1 · Deps: —
  *Why:* every quarantined or `accept_flag` hook is a `GenServer.call` plus a synced `Store.write` inside one process (`edge/quarantine.ex:84-102,421-429`): ~1/fsync per node for all sources, and a forged flood stalls `hit/2` for every flagged source.
  *Change:* bucket map → ETS CAS (reuse `RateLimiter.hit_bucket/4`); the pen write happens in the caller via `Store.write`; byte accounting via `:ets.update_counter`; the GenServer keeps only recovery/purge.
  *Done when:* 5 k quarantined/s across 500 sources with p99 < 50 ms; existing quarantine tests pass unchanged.
  *Proof:* `mix test test/ankusa/edge_test.exs` → quarantine cases; `mix run bench/quarantine_bench.exs` (new) prints p99.
  *Anchors:* `packages/ankusa/lib/ankusa/edge/quarantine.ex:84-102,295,314-375,421-429`, `edge/rate_limiter.ex:233-271`.
  *Evidence:* —

- [ ] **CLM-1 — Gateway integrity check, caching and metrics** · P1 · Deps: SEC-11
  *Why:* the gateway never verifies a claim's digest although the manifest carries it (`claim_check/pack.ex:180-200`, `claim_check/router.ex:35-39`); every GET is two store reads with no cache or single-flight (`claim_check.ex:200-240`); sweeps, gateway requests and compactor failures have no metrics (`metrics.ex:212-225`, `storage/compactor.ex:150-170`).
  *Change:* `claim_check.integrity_check: :off | :manifest` (read the pack tail once, cache verified manifests per pack id in a byte-bounded ETS LRU, compare sha256, `502 integrity_mismatch`, `x-ankusa-sha256` header); single-flight per `{pack_id, index}`; metrics `ankusa_claim_gateway_requests_total{status}` + duration histogram, `ankusa_claim_sweeps_total{deleted}`, `ankusa_archive_failures_total`, `ankusa_archive_oldest_unarchived_age_seconds`.
  *Done when:* a flipped body byte is `502`; 100 concurrent HEADs for one claim cause one store read; the series exist.
  *Proof:* `mix test test/ankusa/claim_check test/ankusa/metrics_test.exs` → `integrity mismatch`, `single flight`, `gateway metrics`.
  *Anchors:* `packages/ankusa/lib/ankusa/claim_check.ex:200-240`, `claim_check/router.ex:35-100`, `claim_check/pack.ex:180-200`, `metrics.ex:212-260`.
  *Evidence:* —

### Phase 4 — Delivery engine

- [ ] **DEL-2 — Retry horizon, per-source policy, explicit error classification** · P1 · Deps: F7
  *Why:* the retry policy is global with 84 attempts and no wall-clock deadline (`retry_policy/exponential.ex:8-30`, `config.ex:70`); `Retry-After` is capped at 1 h by a module attribute (`dispatch/pipeline.ex:80-81`); SQS `AccessDenied`/`QueueDoesNotExist` and Pub/Sub `PERMISSION_DENIED`/`NOT_FOUND` are retried for hours (`sink/sqs.ex:270-279`, `sink/google_pubsub.ex:226-233`).
  *Change:* `retry: %{max_elapsed_ms}` (default 24 h) and per-source `retry:` on `Ankusa.Source`; `dispatch.max_retry_after_ms` (default 1 h); `first_attempt_at` persisted in the delivery row (F7 versioning); auth/config errors → `{:permanent, {:config, _}}` with `ankusa_dispatch_config_errors_total{sink}`; AWS `Throttling`/429/503 and Pub/Sub `RESOURCE_EXHAUSTED` → `{:retry_after, ms}` with full jitter; HTTP 409/425/451 classified explicitly and documented.
  *Done when:* a property test shows no schedule exceeds the horizon; a 30 h sink outage dead-letters at 24 h, not 6 h; per-source override is honoured across a restart.
  *Proof:* `mix test test/ankusa/retry_policy_exponential_test.exs test/ankusa/dispatch_test.exs test/ankusa/sink/{sqs,google_pubsub}_test.exs` → the new cases.
  *Anchors:* `packages/ankusa/lib/ankusa/retry_policy/exponential.ex`, `dispatch/pipeline.ex:80-81,1302-1315`, `source.ex:8-33`, `sink/sqs.ex:252-279`, `sink/google_pubsub.ex:220-233`.
  *Evidence:* —

- [ ] **DEL-3 — Owned, bounded HTTP connection pools** · P1 · Deps: —
  *Why:* Req's default Finch gives one pool per origin, 50 connections, `pool_max_idle_time: :infinity`, `conn_max_idle_time: :infinity`, HTTP/1 only; at 10⁵ tenant origins that is 10⁵ pools and millions of sockets with nothing reaping them.
  *Change:* `Ankusa.HttpClient.Finch` started in `Ankusa.Application` with `default: [size: 8, count: 1, pool_max_idle_time: 60_000, conn_max_idle_time: 30_000, protocols: [:http1, :http2]]`; per-sink `:pool` override; an idle-pool reaper (mirror `Sink.Reaper`); pools closed on source delete; gauge `ankusa_http_pools`.
  *Done when:* 10⁵ synthetic origins keep FDs and RSS bounded with idle pools reaped; HTTP/2 negotiates against a test server; throughput at 1 origin is unchanged.
  *Proof:* `mix test test/ankusa/http_client_test.exs test/ankusa/sink/http_test.exs` → `pool reaped`, `http2`; `mix run bench/pools_bench.exs` (new) prints FD count at 10⁵ origins.
  *Anchors:* `packages/ankusa/lib/ankusa/http_client.ex`, `application.ex`, `sink/reaper.ex`, `docs/deployment.md:197-214`.
  *Evidence:* —

- [ ] **DEL-4 — SQS and Pub/Sub batching workers** · P1 · Deps: —
  *Why:* one HTTP request per message (`sink/sqs.ex:214-250`, `sink/google_pubsub.ex:107-130`); `SendMessageBatch` (10) and Pub/Sub's 1 000-message `publish` are unused.
  *Change:* `Sink.SQS.Batcher` (linger 20 ms, max 10 entries / 256 KiB, bounded queue) and `Sink.GooglePubSub.Batcher` (max 1 000 / 10 MB); per-entry results mapped back (`Successful`/`Failed`, each classified); a delivery is answered only after its batch's response; rows stay claimed so a worker crash retries them.
  *Done when:* 10 k messages/s make ~10× / ~1 000× fewer requests; a partial failure dead-letters exactly the failed entries; a killed batcher loses nothing.
  *Proof:* `mise run test:integration` → `sqs batch partial failure`, `pubsub batch`; `mix run bench/sqs_batch_bench.exs` (new) prints requests per 10 k messages.
  *Anchors:* `packages/ankusa/lib/ankusa/sink/sqs.ex:106-131,158-250`, `sink/google_pubsub.ex:98-130,159-233`.
  *Evidence:* —

- [ ] **DEL-5 — Message size preflight and forwarded-header caps** · P1 · Deps: —
  *Why:* only SQS/Pub/Sub enforce size; Kafka/NATS rely on the broker and retry an oversize body for hours; `forwarded_headers/2` has no count or byte cap, so a large-header provider can push a small body over a broker limit (`sink/message.ex:40-46,143-161`).
  *Change:* `Sink.Message.max_bytes/1` per sink (from `c:max_message_bytes/1`, default per adapter) checked before publish → `{:permanent, :message_too_large}` or claim-check when the body alone fits; `forward_headers_max_bytes` (16 KiB) and `max_headers` (64) on `Source`/YAML, enforced in `forwarded_headers/2` with `ankusa_sink_headers_dropped_total`; document the `inline_max_bytes × 4/3 + metadata` budget.
  *Done when:* a 1 MiB header value still yields a message under the SQS limit with a counted drop; a > 1 MiB inline body to Kafka is permanent without a broker call.
  *Proof:* `mix test test/ankusa/sink/message_test.exs` and `mise run check:package ankusa_kafka ankusa_nats` → the new cases.
  *Anchors:* `packages/ankusa/lib/ankusa/sink/message.ex:40-61,143-161`, `sink.ex`, `packages/ankusa_kafka/lib/ankusa/sink/kafka.ex`, `packages/ankusa_nats/lib/ankusa/sink/nats.ex`.
  *Evidence:* —

- [ ] **DEL-1.a — ADR: opt-in ordering lanes** · P1 · Deps: TEN-3 `[design-gate]`
  *Why:* delivery is unordered by design (`sink.ex:24-31`); decision taken: bring back per-key ordering as an opt-in per source/sink.
  *Change:* ADR deciding: `ordered: true` + `ordering_key: {mod, fun, args} | {:header, name} | {:json, path}` on a source's sink entry; lane = `{tenant, source, sink, key}` single-flight; retry blocks the lane only; lane state in a store index so restarts preserve it (a key-layout change registered through F6's registry, with fixture); SQS FIFO `MessageGroupId` = key, Pub/Sub `orderingKey` = key, Kafka record key = key, RabbitMQ/NATS/Redis documented as consumer-side; HTTP sequential per lane; replay respects lanes; metrics `ankusa_dispatch_lanes_blocked`.
  *Done when:* `docs/adr/0003-ordering-lanes.md` merged.
  *Proof:* PR merged; links pass.
  *Anchors:* `packages/ankusa/lib/ankusa/sink.ex:24-31`, `dispatch/pipeline.ex:1094-1140`, `sink/sqs.ex:195-211`, `sink/google_pubsub.ex:118-120,159-165`.
  *Evidence:* —

- [ ] **DEL-1.b — Implement ordering lanes** · P1 · Deps: DEL-1.a
  *Change:* as decided in the ADR; `docs/delivery.md` ordering section rewritten.
  *Done when:* 100 hooks for one key delivered with a mid-stream retry arrive in order at the receiver; other keys stay concurrent; lanes survive a restart.
  *Proof:* `mix test test/ankusa/dispatch/ordering_test.exs` (new) → `ordered lane`, `other keys concurrent`, `survives restart`; `mise run bench:gate` → PASS.
  *Anchors:* ADR 0003.
  *Evidence:* —

- [ ] **DEL-6 — Fleet replay with tenant/sink/error filters and bounded DLQ listing** · P1 · Deps: TEN-1.b
  *Why:* replay filters are `source_id`/`id`/time only (`replay.ex:47-48,140-169`); `GET /v1/dlq` folds the whole dead family (`queue/deliveries.ex:509-575`); jobs are node-local (`dispatch/replayer.ex:34-39`).
  *Change:* filters `tenant_id`, `sink`, `error_class`; `dead_by_tenant` index from TEN-1; cursor pagination on `GET /v1/dlq`; `docker-entrypoint replay --nodes <dns-or-list> --filter …` in `ankusa_server` fanning out `POST /v1/replays` to every node with the same idempotent job id and aggregating status (no in-process clustering).
  *Done when:* a 10 M-row DLQ lists a page in < 100 ms; a tenant-filtered fleet replay touches only that tenant's rows on every node and resumes after a node kill.
  *Proof:* `mix test test/ankusa/replay_test.exs test/ankusa/admin/router_test.exs` → `tenant filter`, `cursor page`; F5 run with `--replay-drill` → `replayed=<n> duplicates=0`.
  *Anchors:* `packages/ankusa/lib/ankusa/replay.ex:40-49,129-170`, `dispatch/replayer.ex:34-39,318-400`, `queue/deliveries.ex:509-575`, `packages/ankusa_server/lib/ankusa_server/cli.ex`.
  *Evidence:* —

- [ ] **DEL-7 — Broker fault injection for every adapter** · P1 · Deps: —
  *Why:* adapter suites test "unreachable" and one RabbitMQ channel close only (`rabbitmq_test.exs:202,244,335`, `kafka_test.exs:146`, `nats_test.exs:239`); no broker restart mid-publish, no latency or partition injection.
  *Change:* toxiproxy in each adapter's `docker-compose.yml`; tests: broker killed between publish and confirm with `max_inflight` outstanding (RabbitMQ), `produce_timeout_ms` expiry mid-flight (Kafka), JetStream ack loss (NATS), latency above the attempt deadline (all); assert `deliver/3` never returns `:ok` for a message the broker did not retain and the row stays retryable.
  *Done when:* each suite has the four scenarios green and the F5 chaos phase includes a broker restart with `missing: 0`.
  *Proof:* `mise run check:package ankusa_rabbitmq ankusa_kafka ankusa_nats ankusa_redis` → new cases pass.
  *Anchors:* `packages/ankusa_rabbitmq/test`, `packages/ankusa_kafka/test`, `packages/ankusa_nats/test`, each package's `docker-compose.yml`.
  *Evidence:* —

### Phase 5 — Observability and operations

- [ ] **OPS-1 — OpenTelemetry tracing end to end** · P1 · Deps: F7
  *Why:* no OpenTelemetry anywhere; `traceparent` is stored as an opaque header and forwarded but never parsed or linked (`telemetry.ex:9-46`, `edge/ingest.ex:312-320`, `sink/message.ex:96-99`); dispatch has no span, no duration.
  *Change:* `opentelemetry_api` in core (no-op without an SDK); `ankusa_server` adds `opentelemetry` + `opentelemetry_exporter` behind `tracing: %{enabled, otlp_endpoint, sample_ratio}`; `Edge.Router` extracts `traceparent`/`tracestate` into `Envelope.trace` (F7 field); spans `ankusa.ingest` (child of the provider's), `ankusa.verify`, `ankusa.commit`, `ankusa.dispatch.attempt` (linked to the ingest span from the stored context), `ankusa.claim_check`, `ankusa.replay`; propagation: `traceparent` header on `Sink.Http`, `traceparent`/`tracestate` fields plus the broker-native header in `Sink.Message`, CloudEvents `traceparent` extension on lifecycle events; conformance decode case for the fields.
  *Done when:* a `traceparent` sent to the edge reaches an SDK consumer unchanged and the in-memory exporter shows ingest → commit → dispatch spans sharing the trace id.
  *Proof:* `mix test test/ankusa/tracing_test.exs` (new, in-memory exporter) → `one trace across stages`; `mise run check:conformance` passes `message.traceparent`.
  *Anchors:* `packages/ankusa/lib/ankusa/telemetry.ex`, `edge/router.ex:56-74`, `edge/ingest.ex:184-188,312-331`, `dispatch/pipeline.ex:746-752,1273-1278`, `sink/message.ex:72-129`, `sink/http.ex:196-198`, `lifecycle.ex:39-56`.
  *Evidence:* —

- [ ] **OPS-2 — Structured JSON logs with correlation, runtime log level** · P1 · Deps: SEC-1
  *Why:* plain-text logs with `metadata: []`, no hook/tenant/source correlation, no runtime level change (`ankusa_server/config/config.exs:4`, `ankusa_server/application.ex:35-37`).
  *Change:* `AnkusaServer.LogFormatter` (one JSON object per line: `time, level, msg, instance, hook_id, tenant_id, source_id, sink, trace_id`); `Logger.metadata` set in `Edge.Router.capture/2`, `Ingest.ingest/4`, dispatch attempts, lifecycle publish, replay; `log: %{format: :json | :text, level}`; `Ankusa.LogLevel.set/1` + `PUT /v1/log-level` (scope `config:write`); the formatter redacts values of keys matching `secret|token|password|authorization|cookie`.
  *Done when:* a failing commit's log line parses as JSON with `source_id`/`tenant_id`; a request line carries `trace_id`; the level changes at runtime without restart and is audited.
  *Proof:* `mix test packages/ankusa_server/test/ankusa_server/log_test.exs` (new) and `test/ankusa/admin/router_test.exs` → `json line`, `runtime level`.
  *Anchors:* `packages/ankusa_server/lib/ankusa_server/application.ex:27-50`, `packages/ankusa_server/config/config.exs`, `packages/ankusa/lib/ankusa/edge/router.ex:56-74`.
  *Evidence:* —

- [ ] **OPS-3 — SLI metrics: delivery latency, attempt duration, failure counters** · P1 · Deps: —
  *Why:* `[:ankusa, :dispatch, :stop]` has no measurements (`telemetry.ex:24`, `pipeline.ex:1273-1278`); no end-to-end latency, no compactor/claim/backup failure counters, no SLO definitions.
  *Change:* measurements `attempt_ms` and `since_received_ms` on dispatch stop; histograms `ankusa_delivery_latency_seconds{tenant_id,sink,result}` (buckets to 60 s) and `ankusa_dispatch_attempt_duration_seconds{sink,result}`; `tenant_id` on `ankusa_ingest_duration_seconds`; counters `ankusa_compact_failures_total`, `ankusa_claim_check_failures_total{op}`, `ankusa_backup_failures_total`; the ground-rule SLO table referenced from `architecture.md#telemetry`.
  *Done when:* a `GateSink` delay shows up in the histogram `_sum`; every series in the SLO table exists on `/metrics`.
  *Proof:* `mix test test/ankusa/metrics_test.exs` → `delivery latency histogram`, `slo series present`.
  *Anchors:* `packages/ankusa/lib/ankusa/telemetry.ex:24-30`, `dispatch/pipeline.ex:746-752,1273-1278`, `metrics.ex:46-47,159-179,251-271`.
  *Evidence:* —

- [ ] **OPS-4 — Graceful drain on SIGTERM** · P0 · Deps: DUR-6
  *Why:* neither application defines `prep_stop/1`; readiness never flips; the ThousandIsland `shutdown_timeout` (15 s) equals the batcher's 15 s commit-start deadline, so a commit that starts late is killed mid-fsync and the provider sees a reset for a committed hook (`ankusa_server/application.ex:27-50`, `instance.ex:180-205`, `health.ex:3-22`).
  *Change:* `prep_stop/1` in `Ankusa.Application` and `AnkusaServer.Application`: `Ankusa.Health.drain(instance)` (`/ready` → `503 {"status":"draining"}`), sleep `listener.drain_delay_ms` (default 5 s), stop the edge listener with `shutdown_timeout = batcher.deadline_ms + commit p99 budget + 5 s` computed from config, wait until every batcher's in-flight count is 0 (bounded by `listener.drain_timeout_ms`, default 30 s), let dispatch flush outcomes, run the DUR-6 final checkpoint, then let the tree stop. `deployment.md`: `terminationGracePeriodSeconds ≥ drain_delay + drain_timeout + 10`.
  *Done when:* SIGTERM with 200 in-flight POSTs against a slow sink yields every request a `201` or `503`, zero connection resets, `/ready` 503 within 100 ms of the signal, and every `201` present after restart.
  *Proof:* `mix test test/ankusa/drain_test.exs` (new, separate OS process like `loss_test`) → `zero resets`, `ready flips`; `packages/ankusa_server/scripts/smoke.sh` gains a `docker stop` drill asserting the same.
  *Anchors:* `packages/ankusa/lib/ankusa/application.ex`, `packages/ankusa_server/lib/ankusa_server/application.ex:27-50`, `instance.ex:180-214`, `health.ex:3-50`, `edge/batcher.ex:76-83`.
  *Evidence:* —

- [ ] **OPS-5 — Readiness that reflects every dependency** · P1 · Deps: —
  *Why:* `/ready` consults only the local store; a `wal: none` node is ready with every broker down; no disk floor or backlog signal despite the gauges existing (`health.ex:38-50`).
  *Change:* `Ankusa.Health.Check` registry: `store`, `intake` (DUR-1 when present), `sinks` (last publish error per sink within `health.sink_error_window_ms` under `wal: none`), `disk` (`node.min_free_bytes`), `backlog` (`queue.max_oldest_due_ms`, label only, never unready by default), `backup` (DUR-6); `/ready` body lists each with `ok | failing | draining`; unready for `store`, `intake`, `disk`, `sinks` (direct mode).
  *Done when:* a direct-mode instance with a failing sink is 503; a tmpfs below the floor is 503; a backlog over threshold shows `backlog: failing` with 200.
  *Proof:* `mix test test/ankusa/health_test.exs` (new) → the three cases.
  *Anchors:* `packages/ankusa/lib/ankusa/health.ex`, `store.ex:345-355`, `metrics/gauges.ex:44-51`, `edge/publish.ex`.
  *Evidence:* —

- [ ] **OPS-6 — Config reload and config schema version** · P1 · Deps: SEC-1, SEC-9
  *Why:* no key reloads without restart; no schema version; unknown root keys reject (`ankusa_server/config.ex:65,328`).
  *Change:* root key `config_version: 1` with a migration map in the loader; `SIGHUP` and `POST /v1/config/reload` (scope `config:write`) re-read the file, re-resolve secrets (SEC-9), and apply the declared reloadable set — `log`, `rate_limits`, static `sources` (secret rotation), `routes` seed, `dispatch.concurrency`, `dispatch.sink_concurrency`, `dispatch.tenants`, `metrics.detail`, `admin.gauge_interval_ms` — refusing with `409` and the list of non-reloadable keys that changed; one audit row per reload.
  *Done when:* changing `log.level` and a tenant rate limit then reloading takes effect without restart; changing `port` is refused naming the key.
  *Proof:* `mix test packages/ankusa_server/test/ankusa_server/reload_test.exs` (new) → `applies reloadable`, `refuses non-reloadable`.
  *Anchors:* `packages/ankusa_server/lib/ankusa_server/config.ex:60-70,200-260,320-340`, `packages/ankusa/lib/ankusa/config.ex`, `source_store.ex:208-224`.
  *Evidence:* —

- [ ] **OPS-7 — Alert rules, dashboards, runbooks, checked in CI** · P1 · Deps: OPS-3
  *Why:* nothing ships; `architecture.md:486-498` only asserts the metrics are "enough to alert".
  *Change:* `deploy/monitoring/prometheus-rules.yaml` (ack error ratio > 1 %, ack p99 > 1 s, `oldest_due_age > 300`, `dispatch_breakers_open > 0` for 5 m, `backup_age > 300`, `disk_free < 10 %`, `dlq_rows` growth, `subtree_down`, `intake_failures`, `tenant_overload` rate, `store_write_failures`), `deploy/monitoring/grafana/ankusa.json`, `docs/runbooks/{disk-full,dlq-growth,breaker-open,backup-stale,node-unready,intake-down,tenant-overload,store-write-failed}.md` each with symptom → metric → action → verification; `.mise/tasks/check/monitoring` running `promtool check rules` and verifying every alert's `runbook_url` resolves; wired into `check/_default` and `ci.yml`.
  *Done when:* CI fails on a broken rule or a missing runbook; every SLO row has an alert.
  *Proof:* `mise run check:monitoring` → `rules: OK runbooks: OK`.
  *Anchors:* `docs/architecture.md:472-495`, `packages/ankusa/lib/ankusa/metrics.ex`, `.mise/tasks/check/_default`.
  *Evidence:* —

- [ ] **OPS-8 — Helm chart, hardened pod spec, topology and multi-region guidance** · P1 · Deps: OPS-4, OPS-5
  *Why:* no chart/PDB/HPA/PVC guidance; the demo chart uses `emptyDir` for a store node and probes readiness on `/health`; no `securityContext` anywhere; multi-region is undefined.
  *Change:* `deploy/helm/ankusa`: StatefulSet for `wal.type: disk | dual`, Deployment for `wal.type: none`; `startupProbe`/`/health`/`/ready`; PDB `maxUnavailable: 1`; HPA on `ankusa_queue_pending` and CPU for stateless; `terminationGracePeriodSeconds` from OPS-4; `topologySpreadConstraints`; `securityContext` (non-root, read-only rootfs, `drop: [ALL]`, seccomp `RuntimeDefault`); NetworkPolicy restricting 4001–4003 to the operator namespace; PVC sizing table (`bytes/s × mean body × backlog goal × 2`); `helm lint`/`helm template` in `check:examples`. `docs/deployment.md` multi-region section: independent regional fleets, tenant → region pinning at the global LB, one intake topic per region, `backup.secondary` cross-region, no cross-region store.
  *Done when:* `helm template` renders for all three modes and passes `kubeconform`; the F5 fleet can run from the chart on kind via `mise run e2e`.
  *Proof:* `mise run check:examples` → `helm lint OK`, `kubeconform OK`; `mise run e2e` → `missing: 0`.
  *Anchors:* `examples/jetstream-k8s/`, `examples/oban-consumer/`, `docs/deployment.md`, `packages/ankusa_server/Dockerfile`.
  *Evidence:* —

### Phase 6 — Consumer contract

- [ ] **CON-1 — SDKs expose the verification signal** · P0 · Deps: —
  *Why:* every queue message and HTTP delivery carries `verification`/`flagged`, but the Elixir SDK drops it (`sdk-elixir/lib/ankusa/sdk/message.ex:164-175`, `hook.ex:24-45`, `webhook.ex:46-56`) and no conformance case covers it; a hook accepted after a failed signature is indistinguishable from a verified one.
  *Change:* `verification: %{status, flagged}` on `Message`, `Hook` and `Webhook.Headers` (parse `x-ankusa-verification`/`-flagged`), `Hook.trusted?/1`; conformance features `message.verification` and `webhook.verification`; every SDK (check each `packages/sdk-*`) updated.
  *Done when:* a `flagged: true` message decodes as untrusted in all nine SDKs.
  *Proof:* `mise run check:conformance` → all SDKs pass the two features.
  *Anchors:* `packages/sdk-elixir/lib/ankusa/sdk/{message,hook,webhook}.ex`, `packages/ankusa/lib/ankusa/sink/message.ex:84-85,205-221`, `conformance/features.json`.
  *Evidence:* —

- [ ] **CON-2 — Wire-format evolution path and producer-side conformance** · P1 · Deps: OPS-1
  *Why:* decoders accept `v == 1` only, vectors are decoder-only, and nothing proves core's encoder matches the vectors (`sdk-elixir message.ex:158-160`, `conformance/features.json:2-30`); a `v: 2` would break all nine SDKs at once.
  *Change:* SDKs accept `min_supported ≤ v ≤ max_supported` (both exposed) and map each version to the current struct; a producer vector file generated by `mix ankusa.conformance.gen` from `Sink.Message.encode/3`, with `check.mjs` asserting produced JSON equals the vector; `decode_lifecycle` operation; `message.decode.v2_additive` cases (v2 = v1 + additive fields) that every SDK must accept; additive origin fields `instance` and `region` (from `Config`) on messages and an `x-ankusa-region` AsyncAPI extension.
  *Done when:* a field added to `encode/3` without a vector update fails `check:conformance`; all SDKs decode the v2-additive cases.
  *Proof:* `mise run check:conformance` → pass; a throwaway encoder change fails it (recorded, reverted).
  *Anchors:* `conformance/check.mjs`, `conformance/cases/message.json`, `packages/ankusa/lib/ankusa/sink/message.ex:72-105`, `packages/sdk-elixir/lib/ankusa/sdk/message.ex`.
  *Evidence:* —

- [ ] **CON-3 — Salesforce dedupe preset and no-dedupe boot warning** · P1 · Deps: —
  *Why:* without `dedupe:` a provider retry is a new `idempotency_key` (`envelope.ex:61-64`); presets cover github/standard_webhooks/svix/shopify/stripe only (`dedupe.ex:40-46`).
  *Change:* preset `salesforce` in `Ankusa.Dedupe.@presets` keyed on the delivery id Salesforce sends (confirm the header or JSON path against Salesforce's outbound-webhook documentation before coding — unverified; if none is stable, ship the preset as `{:json, "id"}` and say so in `configuration.md`); boot warning `source <id> has no dedupe:` with `ankusa_sources_without_dedupe` gauge; `configuration.md` row.
  *Done when:* a source with `dedupe: salesforce` collapses a retry to the original id; a source without `dedupe` logs the warning once.
  *Proof:* `mix test test/ankusa/dedupe_test.exs test/ankusa/config_test.exs` → `salesforce preset`, `warns without dedupe`.
  *Anchors:* `packages/ankusa/lib/ankusa/dedupe.ex:40-48,111-158`, `envelope.ex:61-64`, `docs/configuration.md` (sources).
  *Evidence:* —

- [ ] **CON-4 — Consumer failure policy, header symmetry, lifecycle delete payload** · P2 · Deps: CON-1
  *Why:* poison-message guidance stops at `retryable: false` (`sdk-elixir/README.md:205-209`); HTTP deliveries omit `received_at`/`size`/`sha256` (`sdk hook.ex:3-10`); `SinkMessageHeaders` requires `ankusa_tenant_id` that `attributes/2` may drop (`sink/message.ex:116-131` vs `async_api/schemas.ex:123-160`).
  *Change:* `docs/delivery.md` "Consumer failure policy" (decode errors and version mismatch are permanent → terminal queue, counted); conformance cases asserting `retryable: false` for `invalid_json`/`unsupported_version` in all SDKs; optional `x-ankusa-received-at` and `x-ankusa-body-sha256` on `Sink.Http` parsed by SDKs; `ankusa_tenant_id` optional in the header schema; per-transport header sets described in `Ankusa.AsyncApi`.
  *Done when:* conformance passes the new cases; the AsyncAPI document validates with the schema change.
  *Proof:* `mise run check:conformance`; `mix test test/ankusa/async_api_test.exs test/ankusa/sink/http_test.exs`.
  *Anchors:* `packages/ankusa/lib/ankusa/sink/http.ex:196-198`, `sink/message.ex:116-131`, `async_api/schemas.ex:123-160`, `docs/delivery.md`.
  *Evidence:* —

### Phase 7 — Hot-path polish

- [ ] **PERF-1 — Allocation cuts on the request path** · P2 · Deps: F1
  *Why:* `Hmac.signed_bytes` materialises a second body copy via `Regex.replace` (`verifier/hmac.ex:240-250`); `Envelope.header/2` re-downcases every lookup (`envelope.ex:51-57`); the `{:json, path}` dedupe preset decodes the whole body on the request process (`dedupe.ex:125-134`); the rate-limiter sweep is a full `select_delete` every 60 s (`rate_limiter.ex:105-114`).
  *Change:* build the signing template as an iolist once at `resolve_scheme` and pass iodata to `:crypto.mac/4`; `Envelope.header/2` compares against already-lowercased names; `dedupe.max_json_bytes` (default 1 MiB) → key `nil` above it; shard the rate-limiter table by `phash2(tenant, 16)` and sweep one shard per tick.
  *Done when:* allocations per verify of an 8 MB body halve; a 9 MB JSON body yields a nil key with unchanged p99; a 1 M-row sweep takes < 50 ms.
  *Proof:* `mise run bench:gate` → PASS with improved `p99_ms 10240B`; `mix test test/ankusa/verifier_test.exs test/ankusa/dedupe_test.exs test/ankusa/edge_rate_limit_test.exs` pass.
  *Anchors:* `packages/ankusa/lib/ankusa/verifier/hmac.ex:74-93,240-250`, `envelope.ex:51-57`, `dedupe.ex:125-134`, `edge/rate_limiter.ex:45-46,105-114`.
  *Evidence:* —

- [ ] **PERF-2 — Bounded sweeper and claim batch error handling** · P2 · Deps: DUR-2
  *Why:* the LocalFS claim sweeper globs every `tenant=*/dt=*` directory per tick inside a GenServer (`claim_check/sweeper.ex:122-130`); `check_in_batch` matches only `{:ok, _}` from `Task.async_stream`, so an `{:exit, _}` raises in the dispatch packing task, and `Pack.build` raises above its limits unguarded (`claim_check.ex:142`, `pack.ex:70-75`).
  *Change:* paginated sweep with a persisted cursor (one tenant prefix per tick); handle `{:exit, reason}` as per-item `{:error, {:unavailable, reason}}`; move the entry/size limits into `split/2` so `Pack.build` cannot raise.
  *Done when:* 10 k partitions sweep in bounded ticks and resume; a raising store or an over-limit item yields per-item errors without a crash.
  *Proof:* `mix test test/ankusa/claim_check` → `bounded sweep`, `batch exit handled`.
  *Anchors:* `packages/ankusa/lib/ankusa/claim_check/sweeper.ex:75-130`, `claim_check.ex:129-142`, `claim_check/pack.ex:45-77`.
  *Evidence:* —

- [ ] **PERF-3 — Scale fixtures in CI: 10⁵ sources and tenants** · P2 · Deps: TEN-7, TEN-9
  *Why:* no test runs with more than a handful of sources; `GET /asyncapi.json` build time, `SourceStore` fetch p99 and metrics series count at 10⁵ are unmeasured.
  *Change:* `bench/scale_bench.exs` generating 10⁵ sources/tenants (10⁶ nightly) into `Static`, `Persistent` and `Redis` stores; measures `fetch/2` p99, document build time, `/metrics` series count and scrape time; thresholds in `baseline.json`; run in `bench:gate` (10⁵) and the soak job (10⁶).
  *Done when:* fetch p99 < 1 ms, document build < 5 s at 10⁵, series count independent of n.
  *Proof:* `mise run bench:gate` → `scale: PASS` row.
  *Anchors:* `packages/ankusa/lib/ankusa/source_store.ex:208-224`, `async_api.ex`, `metrics.ex`.
  *Evidence:* —
<!-- END docs/hardening.md -->
