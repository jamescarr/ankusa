# Remove the Postgres WAL and ingest-side dedup; plan DLQ-as-a-sink

## Context

Ankusa currently dedups at ingest. `Ankusa.DedupKey` extracts a provider event id, the WAL rejects a repeat with `{:duplicate, seq}`, and the edge answers `200 {"status":"duplicate"}`. The Postgres WAL package (`ankusa_postgres`) exists mostly to make that dedup, and a shared log, work across nodes. The owner's call is that ingest dedup is complexity nobody uses. After this change:

- The only WAL is the single-process `Ankusa.WAL.DiskLog`.
- Every accepted POST is stored and answered `201`.
- There is no `DedupKey`, no `dedup:` config, no `duplicate` response, and no dedup metric.
- The docs say ingest is at-least-once and consumers are idempotent receivers.

This file also carries a second, not-yet-executed plan (Part B: the dead-letter queue becomes a configured sink). Step 0 saves both parts under `plans/`.

The working tree already has `ankusa_postgres/` deleted (uncommitted, 11 `D` entries in `git status`). That is the owner's own change: keep it.

## Part A — Approach

### Step 0. Save the plans into the repo
- Write the whole of this file's **Part A** (Context through Assumptions) to `plans/remove-ingest-dedup.md`.
- Write the whole of **Part B** to `plans/dlq-as-sink.md`.
- Copy the text verbatim, headings demoted one level where needed. `plans/*.md` is the repo's existing convention.
- Do not edit any other file in `plans/`. They are historical.

### Step 1. Core: the WAL contract has no duplicates
- `lib/ankusa/wal.ex`:
  - `@type result :: {:committed, Envelope.t()}`.
  - Delete the moduledoc sentences about `dedup_key` collisions (lines ~13–16) and "the `dedup_key` is read from the envelope" (line ~24). Keep "Adapters set `envelope.seq`".
- `lib/ankusa/wal/disk_log.ex`:
  - Delete the `## Dedup durability` moduledoc section.
  - Rewrite line 6 to: "It does **not** survive loss of the box." Drop the Postgres/Kafka/Ra mention.
  - `init/1`: delete the `dedup` ETS table, the `load_dedup_snapshot/2` call, and `dedup:` in state.
  - `replay/4` → `replay(fd, index, truncated_through)`, and `parse/6` → `parse/5`. Drop the `dedup` arg, the `dedup_key_of/1` call and its insert. Replay no longer decodes every payload.
  - `handle_call({:append, …})`:
    - Destructure `{results, iodata, inserts, next_seq, bytes, pos}`.
    - Delete the `if iodata == []` "all duplicates" branch. `records` is never empty: the batcher only flushes non-empty batches (confirm in `lib/ankusa/edge/batcher.ex` before deleting). If an empty list can arrive, keep the guard as `records == [] -> {:reply, {:ok, []}, state}`.
    - Delete the `dedup_inserts` insert.
  - `build_batch/2` becomes a reduce with accumulator `{results, iodata, inserts, seq, bytes, pos}`. Every record becomes `{:committed, %{env | seq: seq}}`; frame, index entry and byte accounting are unchanged.
  - Delete `dedup_lookup_key/1`, `committed_seq/3`, `dedup_key_of/1`, `load_dedup_snapshot/2`, `persist_dedup_snapshot/2`, and the snapshot call and comment at the top of `rewrite/3`.
  - A leftover `<name>.dedup` file from an older version is simply ignored. No migration code.
- `test/ankusa/wal_disk_log_test.exs`:
  - `env/2` drops the `dedup_key` param and field.
  - Delete the tests `"dedup: a repeated (source, dedup_key) …"` and `"dedup collisions within a single batch are absorbed"`.
  - Rewrite `"truncate_through drops a prefix but keeps dedup coverage"` as `"truncate_through drops a prefix and seqs continue"`: append 3, `truncate_through(1)`, read gives `[2, 3]`, the next append commits seq 4.

### Step 2. Core: remove dedup from ingest, config types, telemetry, API spec
- Delete `lib/ankusa/dedup_key.ex`, `lib/ankusa/dedup_key/{rules,stripe,git_hub}.ex`, and `test/ankusa/dedup_key_test.exs`.
- `lib/ankusa/envelope.ex`: remove `:dedup_key` from `defstruct` and `@type t`.
  - Old WAL/DLQ frames still decode: `from_binary/1` uses `struct/2`, which drops unknown keys.
- `lib/ankusa/source.ex`: remove the `dedup` field, its comment, its type entry, and the `Map.get(opts, :dedup, …)` line. Rewrite the moduledoc's "how to dedup," clause and the `tenant_id` comment to "the storage/retention scope".
- `lib/ankusa/edge/ingest.ex`:
  - Remove `{:duplicate, Envelope.t()}` from `@type result`.
  - Remove line 107 (`dedup_key` assignment) and the `{:duplicate, seq}` case arm.
  - Delete `dedup_key/2` and its comment, and `tag({:duplicate, _})`.
  - Rename the section comment to `# ── commit ──`.
  - Moduledoc: drop "extract the dedup key" and `/ {:duplicate, _}`.
- `lib/ankusa/edge/batcher.ex`: remove `{:duplicate, …}` from the `commit/4` `@doc` and `@spec`; `@doc` first line becomes "…until it is durably committed (or shed)."
- `lib/ankusa/edge/router.ex`: delete the `respond(conn, {:duplicate, env})` clause and any moduledoc mention of `200 duplicate`.
- `lib/ankusa/metrics.ex`: delete the `"ankusa.dedup.hits.total"` counter and `:duplicate` from the documented `:outcome` values.
- `lib/ankusa/telemetry.ex`: delete the `[:ankusa, :dedup, :hit]` row and `:duplicate` from the outcome doc.
- Comment-only edits:
  - `lib/ankusa.ex`: delete the `Ankusa.DedupKey` bullet.
  - `lib/ankusa/admin/redact.ex:71`: "dedup keys, " goes.
  - `lib/ankusa/uuid_v7.ex:11`: "a unique dedup/index key" → "a unique index key".
  - `lib/ankusa/route_resolver.ex:14`: "verify/dedup/sinks" → "verify/sinks".
  - `lib/ankusa/config.ex:21-24` and `lib/ankusa/edge/batcher_supervisor.ex:4-5`: drop the Postgres advisory-lock clause ("the DiskLog GenServer serializes commits").
  - `lib/ankusa/instance.ex:61`: drop "(e.g. a Postgres connection)".
- `priv/openapi/ingest.v1.yaml`:
  - Remove the `200 duplicate` sentence in the description and both `'200': $ref: '#/components/responses/Duplicate'` entries.
  - Remove the `Duplicate` schema and response components.
  - Change "it scopes dedup and storage" to "it scopes storage".
- `bench/core_bench.exs:63`: delete the `dedup:` line.
- Tests:
  - `test/ankusa/edge_test.exs:47-65`: replace with `"the same body posted twice is stored twice"`. Source `"demo"` with `Verifier.None`, body `{"id":"evt_123"}` posted twice. Both are `201`, the two response `id`s differ, the `seq`s are 1 and 2, and `WAL.stats(...).records == 2`.
  - `test/ankusa/route_resolver_test.exs:65-86`: rename to `"the URL tenant is threaded onto the envelope"`.
    - Source `"stripe" => []`.
    - Post the same body to acme, globex, acme: all three are `201`.
    - `WAL.read` returns 3 envelopes: tenants `acme` ×2 and `globex` ×1, all `source_id == "stripe"`.
  - `test/ankusa/admin/router_test.exs:229-260` (redaction):
    - Drop the `wal:` line.
    - Put the URL password in a sink instead: add `sinks: [{Ankusa.Sink.Http, url: "https://hooks:leakhunter@sink.internal/h"}]` to the `"stripe"` source.
    - Replace the assertion with `assert conn.resp_body =~ "https://hooks:[REDACTED]@sink.internal/h"`.
- Acceptance for steps 1–2: `grep -rn "dedup\|DedupKey\|:duplicate\|WAL.Postgres" lib test priv bench` returns nothing. Word-boundary false positives such as `String.duplicate` are fine.

### Step 3. Delete the `ankusa_postgres` package plumbing
- Keep the working-tree deletion of `ankusa_postgres/` as it is.
- `.github/workflows/ci.yml`: delete the `ankusa_postgres:` job (lines ~34–67).
- `.github/workflows/release.yml`: delete the `"ankusa_postgres-v*"` tag and `ankusa_postgres|` from the `case` on line ~42.
- `.mise.toml`:
  - Delete the `[tasks."check:postgres"]` task and its `mise run check:postgres` line in the aggregate check task.
  - Remove `ankusa_postgres` from every `for` loop (lines ~34, 53, 66, 287, 387, 448, 477).
  - Change the `infra:up` description to "Start RabbitMQ / Redpanda / NATS for the adapter suites".
- `.gitignore`: drop `ankusa_postgres/,` from the line-2 comment (list `ankusa_rabbitmq/` first) and delete `ankusa_postgres-*.tar`.
- `AGENTS.md`: delete the `ankusa_postgres` table row.
- `ankusa_kafka/mix.exs` and `ankusa_nats/mix.exs` (lines ~62–63), and `ankusa_rabbitmq/mix.exs` (line ~63): replace the "See ankusa_postgres/mix.exs…" comment with this block, verbatim from the deleted file:
  ```
  # Path dep for local monorepo development/test; the Hex-published version
  # is what a consumer installing from Hex.pm actually resolves — Hex
  # rejects packages with path/git deps, so this "poncho project" split is
  # required for this package to be publishable at all. Mix rejects two
  # entries for the same app regardless of :only, so this has to be a
  # single conditional entry, not a duplicate-with-disjoint-:only pair.
  ```
  `ankusa_rabbitmq/mix.exs:7`: "Same forced-split pattern as ankusa_postgres:" becomes "Forced split:".

### Step 4. `ankusa_server`: no Postgres WAL, no `dedup:` key
- `ankusa_server/lib/ankusa_server/config.ex`:
  - `@wal_keys ~w(type)`. Delete `@postgres_keys`, `@postgres_discrete_keys`, `@dedup_keys` and `@dedup_types`.
  - Remove `dedup` from `@source_keys`, so a `dedup:` key now raises `unknown key "dedup"`. That is the intended clean cutover; no users exist.
  - `wal_section/1` is `wal = section!(doc, "wal", @wal_keys, [])` followed by `"disk" = enum!(wal["type"] || "disk", ~w(disk), ["wal", "type"])` and `[wal: {Ankusa.WAL.DiskLog, []}]`. `wal.type` stays as a one-value enum so the WAL remains a named, pluggable choice.
  - Delete `postgres_opts!/1`, `postgres_url_opts!/2` and `userinfo_parts/1` (confirm nothing else calls them first; if something does, keep the helper and delete only the Postgres callers).
  - Delete the `ANKUSA_WAL_POSTGRES_URL` env override and keep `ANKUSA_WAL_TYPE`.
  - Delete the `|> put_opt(:dedup, …)` line in `source_opts!/2`, `dedup_key/2`, and `json_path/2` if its only caller was `dedup_key/2`.
- `ankusa_server/test/ankusa_server/config_test.exs`:
  - Delete the four Postgres tests (lines ~271–312) and the `wal/1` helper if it becomes unused.
  - Delete the block at ~86–93 that loads `fleet-postgres-s3.yml`. If it sits inside an "every shipped example loads" loop, just drop that file from the list.
  - Delete the `dedup:` YAML line (~346) and the `source.dedup` assertion (~370–371).
  - Add a test: a source containing `dedup: {type: stripe}` raises `ConfigError` whose message contains `unknown key "dedup"`.
- `ankusa_server/mix.exs`: delete the `{:ankusa_postgres, …}` line. `ankusa_server/Dockerfile`: delete `COPY ankusa_postgres ./ankusa_postgres`, and drop "Postgres WAL," and `../ankusa_postgres` from the header comment.
- `ankusa_server/scripts/smoke.sh`: delete the second-POST/duplicate block (lines ~71–74) and fix any comment that mentions dedup. The smoke test still asserts the first POST returns `201`.
- `ankusa_server/config-examples/`:
  - Delete `fleet-postgres-s3.yml`.
  - Delete every `dedup:` line (single-node ×2, kafka-fanout, multi-tenant, nats-fanout, rabbitmq-fanout, reference ×3).
  - `reference.yml`: delete the `wal.postgres` block and the `ANKUSA_WAL_POSTGRES_URL` header var. The `wal.type` comment becomes `# [env ANKUSA_WAL_TYPE] disk — local append-only log; survives process crash and power loss on this host, not the loss of the host.` The batcher comment "Both WALs serialize…" becomes "The WAL serializes commits itself…". The `tenant` comment drops "dedup and".
  - `multi-tenant.yml` header: "it scopes dedup and storage" becomes "it scopes storage", and "independent dedup scopes" becomes "independent storage scopes".
- Compose:
  - Rename `ankusa_server/compose/docker-compose.fleet.yml` to `docker-compose.proxy.yml`. It becomes one all-role `ankusa` service behind nginx:
    - `image: ${ANKUSA_IMAGE:-jamescarr/ankusa:edge}`, a named volume `ankusa-data:/var/lib/ankusa`, no published ports, `restart: unless-stopped`.
    - No postgres service, no `ANKUSA_WAL_POSTGRES_URL`, no config mount (the image's baked demo config).
  - Its header comment describes "one node behind nginx: ingest open, admin API behind basic auth". The `proof:` line becomes "POST /webhooks/demo returns 201; :4002 returns 401 without credentials".
  - `nginx.conf`: the header says `docker-compose.proxy.yml`, and both `set $edge`/`set $worker` become `set $ankusa http://ankusa:4000;` / `http://ankusa:4002;`. The ingest comment drops "Round-robined across the edge replicas".
  - Update the references in `compose/htpasswd:1` and `compose/docker-compose.yml:17`.

### Step 5. Examples
- `examples/rabbitmq-consumer/ingest_app/.../application.ex:69` and `examples/kafka-sqs-consumer/ingest_app/.../application.ex:54`: delete the `dedup:` line.
- `examples/oban-consumer` becomes an independent-node fleet on DiskLog. Keep the e2e gate and its chaos phase.
  - `ingest_app/mix.exs`: delete the `ankusa_postgres` dep and its comment. Keep `{:ankusa, path: "../../..", override: true}`, but reword the comment to "path dep on core; `override: true` keeps it authoritative over any Hex resolution".
  - `ingest_app/Dockerfile`: delete `COPY ankusa_postgres ./ankusa_postgres` and fix the header comment.
  - `application.ex`:
    - Drop `wal:` (DiskLog is the default), delete `wal_opts/0`, and drop `wal_host=…` from the log line.
    - Delete the `dedup:` line.
    - Moduledoc: "with the default disk-backed WAL on a per-pod volume".
  - Delete `ingest_app/lib/ankusa_example/ingest/release.ex`.
  - `k8s/10-migrate.yaml`: delete the `ankusa-migrate` Job and keep `consumer-migrate`.
  - `k8s/01-postgres.yaml` is unchanged: it still hosts the consumer DB that `loadgen.verify` reads.
  - `k8s/30-ankusa.yaml` is replaced by:
    - a `StatefulSet` `ankusa` (`serviceName: ankusa`, `replicas: 3`, `podManagementPolicy: Parallel`, label `app: ankusa`);
    - its container keeps the existing `preStop sleep 5` and health probes, with env `ANKUSA_ROLES=edge,dispatch,storage`, `PORT=4000`, `DATA_DIR=/data`, `CONSUMER_URL=http://consumer/deliveries`;
    - `volumeClaimTemplates` `data` of 1Gi mounted at `/data`;
    - a `Service` `ankusa` of type NodePort (port 80 → 4000, `nodePort: 30080`, selector `app: ankusa`).
  - Each pod is a self-contained node. A killed pod comes back with the same PVC and drains its own WAL.
  - `run.sh`:
    - Wait only for `job/consumer-migrate`, and replace the two ankusa rollout waits with `rollout status statefulset/ankusa --timeout=180s`.
    - Chaos: at +10 s `delete pod ankusa-0`, at +20 s `delete pod ankusa-1`, at +30 s kill a consumer pod as today. Log lines: `killing ankusa pod ankusa-0` and `killing ankusa pod ankusa-1`.
  - `README.md`: rewrite the topology diagram and bullets as three self-contained ankusa pods (each with its own WAL on a PVC) calling the consumer over HTTP. Delete every mention of the shared WAL, `ankusa_postgres` and the migrate Job.
- `tools/loadgen`: delete `--dup-ratio` from the option parsing and the README table.
  - In `loadgen.run.ex`: `dup_ratio`, the `{:dup, …}` branch in `build_body/3` (always generate fresh), the `bodies` pool, the `duplicates` counter and report field, `duplicate_response?/1`, and the `200` classify clause (a `200` now counts as an error like any other unexpected status).
  - Update the README's report field list and status list.
  - `loadgen.verify` is unchanged.

### Step 6. Docs: no dedup claims, no Postgres WAL
Rewrite every hit of `grep -rniE "dedup|duplicate|WAL\.Postgres|ankusa_postgres|shared (postgres|wal|log)|fleet-postgres|docker-compose\.fleet" README.md docs examples/README.md examples/*/README.md ankusa_server/README.md`. Files: `README.md`, `docs/{architecture,configuration,delivery,deployment,elixir,integrations,multi-tenancy,packaging,quickstart,releasing,storage,testing}.md`, `examples/README.md`, `examples/kafka-sqs-consumer/README.md`, `ankusa_server/README.md`.

Hits that stay:
- consumer-side statements ("dedupe on `x-ankusa-id`" in `quickstart/worker.py`, `docs/integrations.md`, `docs/delivery.md`);
- SQS FIFO / `Nats-Msg-Id` dedup text in the kafka-sqs example and `nats.ex`;
- `String.duplicate`.

Required statements:
- **Ingest contract** (README "How it works", `docs/architecture.md` guarantees table, `docs/elixir.md:117`, `docs/quickstart.md:148`):
  - A `2xx` is returned only after the WAL fsync. `201 accepted` is the only committed response. There is no `200`.
  - Every accepted POST is a new hook with a new `id`. A provider retry after a lost ack is stored and delivered again.
  - Ingest does no deduplication.
- **Consumer contract** (`docs/delivery.md`, `docs/integrations.md`, `ankusa_server/README.md` "Dedupe on x-ankusa-id"):
  - Consumers are idempotent receivers.
  - `x-ankusa-id` / message `id` identifies one stored hook: dedupe dispatch redeliveries on it.
  - Provider retries arrive as distinct hooks. Dedupe those on the provider's event id in the body (e.g. Stripe `id`).
  - The original request headers (e.g. `X-GitHub-Delivery`, `webhook-id`) are not forwarded by `Sink.Http` or `Sink.Message` today, so header-borne ids are not available downstream. State that plainly.
- Remove the quickstart duplicate drill (`docs/quickstart.md:~40-50`), `README.md:36-37`, `ankusa_server/README.md:31-32`, and both `# 200 duplicate` curl lines.
- **Scaling** (`README.md` "Grow into a fleet", `docs/deployment.md` roles section and "Scaling the ingest fleet", `docs/architecture.md` topologies 2–3 and the "compose" paragraph):
  - `WAL.DiskLog` needs every WAL role (`edge`, `dispatch`, `storage`) in one BEAM node. Splitting roles across processes or hosts is not supported; `claim_check` still runs anywhere.
  - Scale out by running N independent all-role nodes behind a load balancer. Each has its own data volume and its own DLQ/admin API.
  - Each node needs **its own bucket** (or LocalFS) for segments, because segment keys are `seg/<first_seq>-<last_seq>.seg` and remote blob stores ignore the instance, so nodes sharing a bucket overwrite each other.
  - Delete architecture topology 3 ("Multi-node fleet, shared Postgres WAL") and the "3 and 4 together" paragraph. Rewrite topology 2 as the single-node role split being unsupported.
  - `docs/delivery.md:187`: drop "across a fleet sharing a `WAL.Postgres`,".
- `docs/storage.md`: delete the `WAL.Postgres` section, the `.dedup` snapshot paragraph, and the duplicate result sentence. `docs/multi-tenancy.md:99-107`: tenant is the storage/retention scope only. `docs/configuration.md`: delete the `dedup.type` row, the `DedupKey` behaviour-table row, and the Postgres `wal` row alternative; the `batcher.partitions` note becomes "the DiskLog GenServer serializes commits". `docs/elixir.md:15`: delete the `ankusa_postgres` dep line, plus the `dedup:` config line.
- `docs/packaging.md`, `docs/releasing.md`, `docs/testing.md`: drop `ankusa_postgres` from package lists and tables ("four packages" wherever "five" was counted). In `docs/testing.md`, delete the `ankusa_postgres` suite section and the dedup test descriptions. Keep the historical chaos-loss write-up but add a one-line note that the Postgres WAL was removed.

## Part A — Critical files & anchors
- `lib/ankusa/wal/disk_log.ex`: `init/1`, `handle_call({:append…})`, `build_batch/2`, `replay/4`/`parse/6`, `rewrite/3`. This is the only non-trivial code rewrite; the seq floor, cursors and truncation logic must stay byte-identical.
- `ankusa_server/lib/ankusa_server/config.ex`: `@source_keys`, `wal_section/1` (~406), `source_opts!/2` (~588), `dedup_key/2` (~687). The unknown-key rejection comes from `check_keys!/3` (~878).
- `examples/oban-consumer/k8s/30-ankusa.yaml` and `run.sh:53-65,107-119`: the release e2e gate topology.
- `test/ankusa/admin/router_test.exs:229`: the redaction test currently depends on a `WAL.Postgres` URL.

## Part A — Verification
Prereqs: Docker. Run `docker compose up -d --wait` in `ankusa_rabbitmq/`, `ankusa_kafka/` and `ankusa_nats/`. For the e2e: `kind`, `kubectl`, `mix`.
1. **Lockfiles:** run `mix deps.get && mix deps.unlock --unused` in `ankusa_server/` and `examples/oban-consumer/ingest_app/`. `postgrex`/`db_connection` must leave both locks; `consumer_app` and `tools/loadgen` keep theirs. Then run `mix deps.get --check-locked && mix deps.unlock --check-unused` in every package listed in AGENTS.md.
2. **AGENTS.md gate:**
   - `mix format --check-formatted && mix compile --warnings-as-errors && mix test` in `.`, `ankusa_rabbitmq/`, `ankusa_nats/`, `ankusa_kafka/` (use the documented container command if CMake is missing) and `ankusa_server/`.
   - `mix compile --warnings-as-errors` in `examples/*/ingest_app`, `examples/oban-consumer/consumer_app` and `tools/loadgen`.
   - `npx tsc --noEmit` in `examples/*/worker`.
3. **New behaviour, core:** `mix test test/ankusa/edge_test.exs test/ankusa/route_resolver_test.exs test/ankusa/wal_disk_log_test.exs`. The new "stored twice" test passes: two `201`s, distinct ids, seqs 1 and 2.
4. **New behaviour, live node:** from the repo root, `mix run --no-halt` (dev autostart on :4000), then in a second shell:
   ```sh
   curl -s -XPOST localhost:4000/webhooks/demo -d '{"id":"evt_1"}'   # 201 {"status":"accepted",...,"seq":N}
   curl -s -XPOST localhost:4000/webhooks/demo -d '{"id":"evt_1"}'   # 201, different id, seq N+1
   ```
   - If `demo` is not a dev source, use whatever source `config/config.exs` defines; check first.
   - Stop the node, create `data/default/wal/ankusa.wal.dedup` containing garbage bytes (`printf 'x' > …`), and restart. It boots, logs `recovered … record(s)`, and the next POST gets seq N+2.
5. **Server config:** `cd ankusa_server && mix test`. Also run `docker run --rm -v "$PWD/bad.yml:/etc/ankusa/ankusa.yml:ro" jamescarr/ankusa:dev check-config` with a `bad.yml` containing a source with `dedup: {type: stripe}`: it exits 78 and prints `unknown key "dedup"`. Build the image first with `mise run docker:build`.
6. **Image smoke:** `mise run docker:smoke` passes without the duplicate assertions.
7. **Proxy compose:** `ANKUSA_IMAGE=jamescarr/ankusa:dev docker compose -f ankusa_server/compose/docker-compose.proxy.yml up -d --wait`.
   - `curl -XPOST localhost:4000/webhooks/demo -d '{"id":"f1"}'` returns 201.
   - `curl -s localhost:4002/v1/dlq` returns 401.
   - `curl -s -u admin:change-me localhost:4002/v1/dlq` returns `{"total":0,…}`.
   - Then `down -v`.
8. **E2E release gate:** `cd examples/oban-consumer && ./run.sh`. All three phases print `PASS` with `missing 0`, including chaos with `ankusa-0` and `ankusa-1` killed.
9. **Residue:** `grep -rn "ankusa_postgres\|WAL.Postgres\|DedupKey\|dedup:" --include='*.ex' --include='*.exs' --include='*.yml' --include='*.yaml' --include='*.toml' --include='*.sh' --include='Dockerfile' . | grep -v '^./plans/'` returns nothing.

## Part A — Assumptions & contingencies
- `ankusa_postgres-v0.1.0` is tagged. Retiring it on Hex is an owner action outside this change: `mix hex.retire ankusa_postgres 0.1.0 deprecated --message "Removed; Ankusa ships DiskLog only"`. Do not run it.
- If the oban e2e chaos phase reports `missing > 0`, suspect PVC reattachment timing. Raise `loadgen.verify --timeout` for chaos to 600 before changing topology. Do not reintroduce a shared WAL.
- If `mix run` in the repo root has no `demo` source, use the Docker image (`docker run --rm -p 4000:4000 jamescarr/ankusa:dev`) for check 4's POSTs. Skip its restart sub-check and rely on the DiskLog unit tests.

---

# Part B — The dead-letter queue becomes a configured sink (plan only; not executed by Part A)

## Context
Today every give-up is written to a node-local file, `<data_dir>/<instance>/dlq/dlq.log`, by `Ankusa.Dispatch.DLQ.write/3`, called in the pipeline process. `GET /v1/dlq` and `POST /v1/dlq/replay` read that file.

In a real deployment the dead-letter destination should be infrastructure the operator already runs: a Kafka topic, a NATS subject, a RabbitMQ exchange, or an HTTP endpoint. Every `Ankusa.Sink` already means "durably accepted" on `:ok`:
- HTTP `2xx`;
- Kafka `acks=all`;
- JetStream ack;
- publisher confirm.

So any sink can hold dead letters. End state:
- The instance names one sink as its dead-letter destination, `dispatch.dead_letter`.
- The local file is just the default sink implementation, `Ankusa.Sink.DeadLetterLog`.
- Listing and replay work only when that local sink is configured.

## Approach

### B1. Config
- `lib/ankusa/config.ex`: add `dead_letter: {Ankusa.Sink.DeadLetterLog, []}` to the `dispatch` defaults, with the comment `# {module, opts} implementing Ankusa.Sink: where give-ups go`. `Config.new/1` already deep-merges `:dispatch`.
- Instance-level only. There is no per-source override: one dead-letter destination per instance keeps replay and alerting in one place.

### B2. `Ankusa.Sink.DeadLetterLog` replaces `Ankusa.Dispatch.DLQ`
- Move `lib/ankusa/dispatch/dlq.ex` to `lib/ankusa/sink/dead_letter_log.ex`, module `Ankusa.Sink.DeadLetterLog`, `@behaviour Ankusa.Sink`.
  - `deliver(env, %{dead_letter: dl} = ctx, _opts)` appends `%{envelope: env, reason: {:sink, dl.sink, dl.reason}, at: dl.at}` with `sync: true` to `Config.path(Ankusa.config(ctx.instance), "dlq/dlq.log")`.
    - The on-disk record shape and path are unchanged, so existing files stay readable and the admin JSON is unchanged.
    - Wrap the append in `:global.trans({{__MODULE__, path}, self()}, fn -> DurableLog.append(…) end, [node()])`. Delivery now runs in concurrent dispatch tasks, and appends must stay serialized; the old code serialized them by running in the pipeline process.
    - Returns `:ok`.
  - `deliver(_env, _ctx, _opts)` without `:dead_letter` returns `{:error, :dead_letter_only}`.
  - `ordering_key(_env, _opts)` returns `nil`.
  - `entries(Config.t())` is unchanged (`DurableLog.read(path, safe: false)`).
- Callers to migrate: `lib/ankusa/dispatch.ex:9,24`, `lib/ankusa/admin/router.ex:93`, `lib/ankusa/dispatch/pipeline.ex:160` (removed in B3), `test/ankusa/dispatch_test.exs:160,177,242`, and `test/ankusa/admin/router_test.exs:93-184`.
  - The router tests seed entries by calling `DeadLetterLog.deliver(env, %{instance: inst, source_id: env.source_id, tenant_id: env.tenant_id, attempt: 1, dead_letter: %{sink: Ankusa.Sink.Http, reason: r, attempts: 1, at: System.system_time(:millisecond)}}, [])`. Put this in a private `dead_letter(config, env, reason)` test helper.
  - `grep -rn "Dispatch.DLQ\|DLQ\." lib test` must return nothing afterwards.

### B3. Dispatch delivers give-ups to the configured sink, inside the job
- `lib/ankusa/sink.ex` `@type ctx`: add `optional(:dead_letter) => %{sink: module(), reason: term(), attempts: pos_integer(), at: integer()}`. Document it as "present only when this delivery is a dead letter".
- `lib/ankusa/dispatch/pipeline.ex` `deliver/5`, `:give_up` branch: after the existing `[:dispatch, :stop]` emit (`result: :dlq`), call `dead_letter(job, instance, config, max_sleep, {mod, reason}, attempt, 1)` and return its result.
- New private `dead_letter/7`:
  1. `{dmod, dopts} = config.dispatch.dead_letter`.
  2. `ctx = ctx(job, instance, 1) |> Map.put(:dead_letter, %{sink: mod, reason: reason, attempts: attempts, at: System.system_time(:millisecond)})`. `at` is computed once, before the loop.
  3. `safe_deliver(dmod, job.env, ctx, dopts)`:
     - `:ok` → return `{:dead, {:sink, mod, reason}}`.
     - `{:error, e}` → emit `[:dispatch, :dead_letter_error]` with metadata `%{instance: instance, source_id: job.env.source_id, sink: dmod, reason: e}`, then `sleep(min(1_000 * 2 ** (n - 1), 30_000), max_sleep)` and retry with `n + 1`, **forever**.
  - The job's lane and the cursor stay held until the dead-letter sink accepts. A dead-letter outage is backpressure, never loss. No fallback to the local file.
  - Claim check: `ctx/3` already carries `job.claim` when present. Queue sinks without one check the body in themselves via `Message.encode/3`.
- `handle_info({ref, {:dead, reason}}, …)`: delete the `DLQ.write/3` call and its comment and keep the `[:dispatch, :dlq]` telemetry emit. Update the `deliver/4` comment to "Returns `:ok` or `{:dead, reason}` once the dead letter is durably accepted".
- `lib/ankusa/metrics.ex`: add counter `"ankusa.dispatch.dead_letter_errors.total"` on `[:ankusa, :dispatch, :dead_letter_error]`, tags `[:instance, :source_id, :sink]`. `lib/ankusa/telemetry.ex`: add the matching doc row.

### B4. Sinks carry dead-letter metadata
- `lib/ankusa/sink/http.ex`: when `ctx[:dead_letter]` is present, append these headers:
  - `x-ankusa-dead-letter-sink` = `inspect(sink)`;
  - `x-ankusa-dead-letter-reason` = `String.slice(inspect(reason), 0, 512)`;
  - `x-ankusa-dead-letter-attempts`;
  - `x-ankusa-dead-lettered-at` (unix ms).
  Document them in the moduledoc.
- `lib/ankusa/sink/message.ex` `encode/3`: when `ctx[:dead_letter]` is present, add `dead_letter: %{sink: inspect(sink), reason: inspect(reason), attempts: n, at: ms}` to `base`. `v` stays `1`, since adding a field is non-breaking per the moduledoc. Document it in the moduledoc example. The Kafka, NATS and RabbitMQ sinks inherit this through `Message.encode/3`; confirm each calls it with the ctx it receives.

### B5. Admin API and replay are local-only
- `lib/ankusa/admin/router.ex`: `GET /v1/dlq` and `POST /v1/dlq/replay`, after the role check, require `elem(config.dispatch.dead_letter, 0) == Ankusa.Sink.DeadLetterLog`. Otherwise they return `409 {"error":"dlq_not_local","sink":"<inspect module>"}`. Same status family as `role_not_enabled`; document it next to that paragraph in the moduledoc and in `priv/openapi` (admin spec).
- `Ankusa.Dispatch.replay/2` keeps reading `DeadLetterLog.entries/1` and delivering to the source's current sinks. Its semantics are unchanged: it re-delivers to every current sink, not only the failed one.

### B6. Server YAML
- `ankusa_server/lib/ankusa_server/config.ex`:
  - Add `dead_letter` to `@dispatch_keys`.
  - In `dispatch_section/1`, `|> put_opt(:dead_letter, dead_letter(dispatch["dead_letter"]))`.
  - `dead_letter(nil)` → `nil`, so the core default applies.
  - A map with `type: local` (and no other keys; otherwise `unknown key`) → `{Ankusa.Sink.DeadLetterLog, []}`.
  - Any other map → `sink!(map, ["dispatch", "dead_letter"])`, the same parser and types as source sinks, so `http | log | rabbitmq | kafka | nats` all work.
- `reference.yml`: under `dispatch:` add `dead_letter: {type: local}` with the comment `# where give-ups go: local (default; enables /v1/dlq + replay) or any sink type, e.g. {type: kafka, brokers: [...], topic: ankusa.dlq}`.

### B7. Docs
- `docs/delivery.md`, the DLQ section, says:
  - the dead-letter destination is a sink;
  - the default local log and its path;
  - the ctx/header/`dead_letter` field contract;
  - "an unavailable dead-letter sink stalls dispatch; it never drops";
  - listing and replay exist only for the local sink, and an external destination is replayed with that system's own tooling.
- `docs/configuration.md` gets a `dispatch.dead_letter` row.
- `docs/quickstart.md` is unchanged (the default is local).

## Critical files & anchors
- `lib/ankusa/dispatch/pipeline.ex`: `deliver/5` `:give_up` (~465), `handle_info({ref, result}…)` (~151), `ctx/3` (~483), `safe_deliver/4` (~496).
- `lib/ankusa/dispatch/dlq.ex` becomes `lib/ankusa/sink/dead_letter_log.ex`; this is where append serialization moves.
- `lib/ankusa/admin/router.ex`: `dlq_index/1`, `dlq_replay/1`, `require_role/3` (~200).
- `ankusa_server/lib/ankusa_server/config.ex`: `@dispatch_keys` (~65), `dispatch_section/1` (~373), `sink!/2` (~739).

## Verification
- `mix test test/ankusa/dispatch_test.exs`: the existing give-up/replay/raise tests pass against `DeadLetterLog.entries/1`. New tests:
  1. `"give-ups go to the configured dead-letter sink with failure metadata"`:
     - `dispatch: %{dead_letter: {CapturingSink, pid: self()}}`, an `AlwaysFail` source sink, `max_attempts: 2`.
     - Receives the env, and `ctx.dead_letter` has `sink: AlwaysFail`, `reason: :always`, `attempts: 2`.
     - `DeadLetterLog.entries(config) == []`, and the cursor advances.
  2. `"a failing dead-letter sink holds the cursor until it accepts"`: a dead-letter sink that fails twice then succeeds. After `Pipeline.tick`, it was called 3 times, `[:ankusa, :dispatch, :dead_letter_error]` fired twice, and the cursor advanced.
- `mix test test/ankusa/admin/router_test.exs`: the existing DLQ tests pass using the helper. New test: `dispatch: %{dead_letter: {Ankusa.Sink.Http, url: "http://x"}}` makes `GET /v1/dlq` return 409 `dlq_not_local`.
- `test/ankusa/sink/http_test.exs`: a ctx with `dead_letter` sends the four headers. `test/ankusa/sink/message_test.exs`: the encoded JSON has `dead_letter.attempts`.
- `ankusa_server` `config_test.exs`:
  - `dispatch.dead_letter: {type: kafka, brokers: ["k:9092"], topic: ankusa.dlq}` gives `{Ankusa.Sink.Kafka, …}`;
  - `{type: local}` gives `DeadLetterLog`;
  - absent gives the core default;
  - `{type: local, url: x}` raises `unknown key "url"`.
- End to end: `examples/quickstart` DLQ drill (`docs/quickstart.md`) still yields `{"total":1,…}` and `{"replayed":1}`. Then set `dispatch.dead_letter: {type: http, url: http://worker:8000/dlq}` in `examples/quickstart/ankusa.yml` temporarily and repeat the drill. The worker log shows a POST carrying `x-ankusa-dead-letter-sink`, and `/v1/dlq` returns 409. Revert the yml.
- AGENTS.md gate for core, `ankusa_server`, and the three adapter packages (they compile against `Ankusa.Sink`).

## Assumptions & contingencies
- One dead-letter sink per instance, with no per-source override. The owner can ask for per-source later; that would be a `Source.dead_letter` field falling back to the instance setting.
- Dead-letter retries never give up (a capped 30 s backoff). If the owner prefers bounded retries with a local-file fallback, B3 changes in `dead_letter/7` only.
- If `:global.trans/3` shows contention in `bench/core_bench.exs` (a dead-letter-heavy run), replace it with a per-instance `GenServer` registered at `Ankusa.via(instance, :dead_letter_log)` and started by `Ankusa.Instance` under the `:dispatch` role.
