# Ankusa 0.1.0 release readiness: review fixes, tag-driven Hex publish, k8s + Oban end-to-end, load harness

## Context

Get the four packages (`ankusa`, `ankusa_postgres`, `ankusa_rabbitmq`, `ankusa_kafka`) ready for their first Hex.pm publish at 0.1.0. That means five things: fix the pre-release review findings; publish on per-package git tags, the Elixir norm, instead of on every push to `main`; build a load-test harness; prove a real deployment on local Kubernetes (kind) where an Oban worker fleet processes the ingested webhooks with zero loss, including under pod kills; and document how users of Oban, Celery or any other job framework feed from Ankusa.

**Hard constraint: no coupling.** No published package may reference Oban, Celery or any job framework. The integration goes through the existing framework-neutral seams (`Ankusa.Sink`, `Ankusa.Sink.Http`, the queue sinks). Oban code lives only in `examples/oban-consumer/`. Celery appears only in docs.

Canonical repo URL: `https://github.com/jamescarr/ankusa`. None of the four package names is taken on Hex (the Hex API returned 404 for each, checked this session). Latest stable versions at the time of checking: ex_doc 0.40.4, oban 2.24.1, ecto_sql 3.14.0.

## Approach

Steps 1–5 change the published packages and CI, and must land first. Step 6 depends on steps 1 and 2. Step 7 is independent. Step 8 depends on step 6, because it quotes code from it. Step 9 depends on steps 6 and 7. Per AGENTS.md, every package a step touches must pass `mix format --check-formatted && mix compile --warnings-as-errors && mix test`; the table there also covers the adapter packages and the examples.

### 1. Harden core config and startup (`lib/ankusa/config.ex`, `lib/ankusa/application.ex`)

- Add `@roles [:edge, :dispatch, :storage, :claim_check]` to `Ankusa.Config`.
- Add `@spec parse_roles!(String.t()) :: [atom()]` with a `@doc`. It splits on `","`, trims each part and drops empty strings. It maps each part through the literal map `%{"edge" => :edge, "dispatch" => :dispatch, "storage" => :storage, "claim_check" => :claim_check}`. It must never call `String.to_atom`.
  - Unknown name: `raise ArgumentError, "unknown Ankusa role #{inspect(name)}; expected one of: edge, dispatch, storage, claim_check"`.
  - Empty result: `raise ArgumentError, "ANKUSA_ROLES must name at least one role"`.
- In `Config.new/1`:
  - `:roles` must be a list whose members are all in `@roles`. Otherwise `raise ArgumentError, "unknown Ankusa role(s) #{inspect(bad)} in :roles"`.
  - For the nested keys `:batcher`, `:dispatch`, `:storage` and `:claim_check`, accept a map, or a keyword list via `Keyword.keyword?/1`. Any other value raises `ArgumentError, "Ankusa.Config #{k} must be a map or keyword list"`. This fixes a real bug: a keyword list currently falls through to `Map.put` and replaces the whole defaults map.
  - Raise on a nested key that isn't among that section's defaults: `raise ArgumentError, "unknown Ankusa.Config key: #{k}.#{nk}"`. A typo like `batcher: %{max_queu: 5}` is silently accepted today.
- `Ankusa.Application`:
  - Change `Application.get_env(:ankusa, :autostart, true)` to default `false`. A library must not bind port 4000 just because it's a dependency.
  - Replace the `String.to_atom` roles parsing (currently line 58) with `Ankusa.Config.parse_roles!/1`.
  - Update the comment on lines 17–18 to say autostart is opt-in (the repo's own `config/config.exs` turns it on outside `:test`).
- Clean cutover for callers:
  - Replace the `roles/0` bodies in `examples/rabbitmq-consumer/ingest_app/lib/ankusa_example/ingest/application.ex` (lines 59–64) and `examples/kafka-sqs-consumer/ingest_app/lib/ankusa_example/ingest/application.ex` (the `String.to_atom` pipeline around line 51) with `Ankusa.Config.parse_roles!(System.get_env("ANKUSA_ROLES", "edge,dispatch,storage"))`.
  - Delete the now-redundant `config :ankusa, autostart: false` line from `ankusa_postgres/config/config.exs`, `ankusa_rabbitmq/config/config.exs`, `ankusa_kafka/config/config.exs`, and both examples' `ingest_app/config/config.exs`. If a file is left with nothing but `import Config` and comments, delete it, and delete `config/` if it's then empty.
  - `grep -rn autostart docs README.md` must then show only accurate text. Rewrite `docs/packaging.md` lines 55–59 and 124–126 and `docs/testing.md` line 163: adapter packages no longer need any config, because autostart is off by default.
  - In `docs/configuration.md`: `## Runtime environment overrides` (line 132) documents that `ANKUSA_ROLES` is validated and an unknown role fails boot. Add an `autostart` row (default `false`) to the Config table or to the overrides section.
- New `test/ankusa/config_test.exs` (`async: true`) with three tests:
  - `parse_roles!("edge, dispatch")` returns `[:edge, :dispatch]`, and `parse_roles!("edgee")` raises `ArgumentError`.
  - `Config.new(batcher: %{max_queu: 1})` raises `ArgumentError` with a message containing `batcher.max_queu`.
  - `Config.new(batcher: [max_queue: 5]).batcher` keeps `max_batch: 256` and has `max_queue: 5`.

### 2. Add `x-ankusa-tenant` to `Ankusa.Sink.Http`

- In `lib/ankusa/sink/http.ex` `deliver/3`, append `{"x-ankusa-tenant", env.tenant_id}` to the identity headers when `is_binary(env.tenant_id)`. Update the moduledoc sentence listing the identity headers.
- In `test/ankusa/sink/http_test.exs`, the test "forwards the body verbatim, with the identity headers and content type" (line 51) gains `assert h["x-ankusa-tenant"] == "acme"`. The fixture already sets `tenant_id: "acme"`.
- In `docs/delivery.md`, update the Sink.Http header list to match.

### 3. Hex package metadata, ExDoc, and docs that render on hexdocs

The same pattern applies to all four `mix.exs` files. `<pkg>` is the app name, and `<prefix>` is `""` for core or `"<pkg>/"` for adapters.

- Top of the module: `@version "0.1.0"` (exactly this line shape, 2-space indent; the release workflow greps it) and `@source_url "https://github.com/jamescarr/ankusa"`.
- `project/0`: `version: @version`, `source_url: @source_url`, `homepage_url: @source_url`, `docs: docs()`.
- `package`:
  ```
  [licenses: ["Apache-2.0"],
   links: %{"GitHub" => @source_url, "Changelog" => "https://hexdocs.pm/<pkg>/changelog.html"},
   files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)]
  ```
  Core adds `priv`, which holds `priv/openapi/claim_check.v1.yaml`. Delete the commented `links` placeholder in root `mix.exs` line 26.
- deps: `{:ex_doc, "~> 0.40", only: :dev, runtime: false}`. It is dev-only because docs are published from the `:dev` env (step 4).
- Core `docs/0`:
  - `main: "readme"` and `source_ref: "ankusa-v#{@version}"`.
  - `extras:` exactly, in this order: `README.md`, `docs/quickstart.md`, `docs/architecture.md`, `docs/configuration.md`, `docs/multi-tenancy.md`, `docs/integrations.md` (created in step 8), `docs/storage.md`, `docs/delivery.md`, `docs/claim-check.md`, `docs/deployment.md`, `docs/packaging.md`, `docs/testing.md`, `CHANGELOG.md`. Exclude `docs/README.md` (its id would collide with `readme`) and `plans/`.
  - `groups_for_extras: [Guides: ~r{^docs/}]`.
  - `groups_for_modules:` a single group `"Internals (no stability guarantee)"` listing exactly `Ankusa.Edge.Router, Ankusa.Edge.Ingest, Ankusa.Edge.Batcher, Ankusa.Edge.BatcherSupervisor, Ankusa.Edge.Quarantine, Ankusa.Storage.Compactor, Ankusa.Storage.Index, Ankusa.Dispatch.Pipeline, Ankusa.Dispatch.DLQ, Ankusa.ClaimCheck.Router, Ankusa.ClaimCheck.Sweeper, Ankusa.DurableLog, Ankusa.Http, Ankusa.HttpClient, Ankusa.UUIDv7`. Everything else stays ungrouped and so reads as public API.
- Adapter `docs/0`:
  - `main: "readme"`, `extras: ["README.md", "CHANGELOG.md"]`, `source_ref: "<pkg>-v#{@version}"`.
  - `source_url_pattern: "#{@source_url}/blob/<pkg>-v#{@version}/<pkg>/%{path}#L%{line}"`, because the source lives in a subdirectory.
  - `deps: [ankusa: "https://hexdocs.pm/ankusa"]`, so that `Ankusa.*` references link across to core's docs.
- Adapter READMEs: create `ankusa_postgres/README.md` and `ankusa_rabbitmq/README.md`, shaped like `ankusa_kafka/README.md`: one-paragraph purpose, an install snippet `{:<pkg>, "~> 0.1"}`, a config snippet copied from the module's moduledoc, and a link to `https://hexdocs.pm/ankusa`.
  - `ankusa_postgres/README.md` also carries a **"Multi-node deployments"** section. Every node calls `Migration.run!` at start by default (`migrate: true`), and concurrent `CREATE TABLE IF NOT EXISTS` across nodes can race in Postgres. For fleets: run `Ankusa.WAL.Postgres.Migration.run!/1` once (release task or Job), then set `migrate: false` on every node. Link to `examples/oban-consumer` as the worked example.
- Links that break on hexdocs:
  - In `README.md` and every `docs/*.md`, rewrite each relative link that targets something outside `README.md` and `docs/*.md` to an absolute GitHub URL. Directories use `https://github.com/jamescarr/ankusa/tree/main/<path>`; files use `.../blob/main/<path>`. Targets include `examples/…`, `ankusa_*/…`, `.github/…`, `priv/…`, `lib/…`, `test/…` and `LICENSE`. Find them with `grep -nE '\]\((\.\./)?(examples|ankusa_|\.github|priv|lib|test|plans|LICENSE)' README.md docs/*.md`.
  - Links between `README.md` and `docs/*.md` stay relative; ExDoc rewrites links between extras. In `docs/README.md`, the `../README.md` link can stay as it is, since that file isn't an extra.
- `README.md` `## Test` block (line 123): remove the hard-coded "98 tests" and "+8" counts, which are already stale (the suite has 123). The prose sentence about adapter test counts goes too.
- CHANGELOGs: nothing has been published, so each package's first public entry becomes `## [0.1.0] - <tag date>`, placed below an empty `## [Unreleased]`.
  - Root `CHANGELOG.md` merges its current `[Unreleased]` content (line 12 onward) into the `[0.1.0]` section (line 72) as one `### Added` list describing shipped features. Drop the Changed/Fixed items that describe edits to never-published code.
  - `ankusa_rabbitmq/CHANGELOG.md`: same treatment. Drop the "Changed (breaking)" rollout note.
  - `ankusa_kafka/CHANGELOG.md`: add the dated `[0.1.0]` section.
  - Replace every compare/release link with the `jamescarr/ankusa` repo and per-package tags: `[0.1.0]: https://github.com/jamescarr/ankusa/releases/tag/<pkg>-v0.1.0` and `[Unreleased]: https://github.com/jamescarr/ankusa/compare/<pkg>-v0.1.0...HEAD`. Core's tag is `ankusa-v0.1.0`, not `v0.1.0`.
  - Rewrite the root CHANGELOG release-process note (lines 7–10) to describe the tag flow from step 4.
  - `<tag date>` is filled with the real date in the PR that immediately precedes tagging.
- `.dockerignore`: change `_build/`, `deps/`, `node_modules/` to `**/_build`, `**/deps` and `**/node_modules`. Replace `hook-*.tar` with `**/*.tar`. Today the adapter directories' `deps/` and `_build/` leak into Docker build contexts.
- Lock files: run `mix deps.get` in root, `ankusa_postgres`, `ankusa_rabbitmq`, `ankusa_kafka` and both existing `examples/*/ingest_app`, then commit the lock changes (AGENTS.md rule).

### 4. Tag-driven release workflow

- `.github/workflows/ci.yml`:
  - Add `workflow_call:` under `on:`, keeping `push: branches: [main]` and `pull_request:`.
  - Add a final `mix docs --warnings-as-errors` step to each of the `ankusa`, `ankusa_postgres`, `ankusa_rabbitmq` and `ankusa_kafka` jobs. The kafka job already has CMake for crc32cer.
  - Add `mix hex.build` as a final step of the `ankusa` job only. Adapter builds need `ankusa` on Hex first; they are checked at publish.
- Replace `.github/workflows/release.yml` entirely:
  - Trigger: `on: push: tags: ["ankusa-v*", "ankusa_postgres-v*", "ankusa_rabbitmq-v*", "ankusa_kafka-v*"]`. The glob `ankusa-v*` does not match `ankusa_postgres-v…`.
  - `concurrency: { group: release-${{ github.ref }}, cancel-in-progress: false }` and `permissions: { contents: write }`.
  - Job `resolve` (ubuntu-latest) has outputs `pkg`, `dir`, `version`. It runs one bash step:
    ```
    TAG="$GITHUB_REF_NAME"; PKG="${TAG%-v*}"; VERSION="${TAG##*-v}"
    case "$PKG" in ankusa) DIR=. ;; ankusa_postgres|ankusa_rabbitmq|ankusa_kafka) DIR="$PKG" ;; *) echo "::error::unknown package in tag $TAG"; exit 1 ;; esac
    MIX_VERSION=$(sed -n 's/^  @version "\(.*\)"$/\1/p' "$DIR/mix.exs")
    [ "$MIX_VERSION" = "$VERSION" ] || { echo "::error::tag $VERSION != mix.exs $MIX_VERSION"; exit 1; }
    grep -q "^## \[$VERSION\]" "$DIR/CHANGELOG.md" || { echo "::error::no CHANGELOG entry for $VERSION"; exit 1; }
    ```
    It then writes the three values to `$GITHUB_OUTPUT`.
  - Job `ci`: `uses: ./.github/workflows/ci.yml`. It runs the full suite for all packages, so a core tag can't ship something that breaks an adapter.
  - Job `publish`: `needs: [resolve, ci]`, `env: { OTP_VERSION: "29.0", ELIXIR_VERSION: "1.20.4" }`. Steps: `actions/checkout@v4`, then `erlef/setup-beam@v1`, then, in `working-directory: ${{ needs.resolve.outputs.dir }}` with `HEX_API_KEY: ${{ secrets.HEX_API_KEY }}`:
    1. `mix deps.get`
    2. `MIX_ENV=prod mix deps.get`
    3. `MIX_ENV=prod mix hex.publish package --yes` (under `:prod`, adapters resolve `{:ankusa, "~> 0.1"}` from Hex instead of the path dep Hex rejects)
    4. `mix hex.publish docs --yes` (`:dev`, where ex_doc lives; the path dep is fine for building docs)
  - Final `publish` step, with `GH_TOKEN: ${{ github.token }}`: extract the notes and create the GitHub release.
    ```
    awk -v v="$VERSION" '$0 ~ "^## \\[" v "\\]" {f=1; next} f && /^## \[/ {exit} f' CHANGELOG.md > "$RUNNER_TEMP/notes.md"
    gh release create "$GITHUB_REF_NAME" --title "$GITHUB_REF_NAME" --notes-file "$RUNNER_TEMP/notes.md"
    ```
- Rewrite `docs/deployment.md` `## Releasing` (lines 162–194):
  1. In a PR: bump `@version` and move `[Unreleased]` entries under a dated heading.
  2. Merge.
  3. Run `git tag <pkg>-vX.Y.Z && git push origin <pkg>-vX.Y.Z` from `main`.
  4. The first release is ordered: tag `ankusa-v0.1.0` and wait for its publish job to finish green before tagging the adapters. Their `:prod` deps resolve `ankusa` from Hex; an adapter tag pushed early fails at `MIX_ENV=prod mix deps.get` and is re-run from the Actions UI after core lands.
  5. `HEX_API_KEY` is a repository secret from a Hex key scoped to `api:write`.
  6. The pre-tag gate is `examples/oban-consumer/run.sh` passing locally (step 6).
  - Also delete the sentence "no tag to remember to push".

### 5. Document operational limits found in review

- `docs/deployment.md`, `## Roles and topologies`, next to the multi-machine constraint (line 40): add **"`:dispatch` and `:storage` are singletons per instance."** Neither cursor has a lease. Two `:dispatch` nodes on one `WAL.Postgres` deliver every hook twice, and two `:storage` nodes compact the same ranges and write duplicate index rows. Scale `:edge` horizontally; run `:dispatch` and `:storage` as exactly one replica each (in Kubernetes, a 1-replica StatefulSet). Also note that `Ankusa.Storage.Index` lives on the `:storage` node's local disk (`segments/index.log`), which needs a persistent volume.
- `docs/deployment.md`: add a short **"Dispatch throughput"** paragraph. `Ankusa.Dispatch.Pipeline` delivers one envelope at a time and writes the cursor after each (`drain/1`, `deliver_with_retry/4`). A retrying sink therefore blocks the instance's whole pipeline, and throughput is bounded by sink latency. Link to the measured numbers recorded in `docs/testing.md` (step 9).
- `README.md` `## Not yet implemented` (line 152): add "concurrent dispatch, and a lease so `:dispatch`/`:storage` can run hot-standby replicas".

### 6. `examples/oban-consumer/`: Ankusa on kind feeding an Oban worker fleet over HTTP

Topology: provider → `ankusa-edge` (Deployment, 3 replicas, `ANKUSA_ROLES=edge`) → `WAL.Postgres` → `ankusa-worker` (StatefulSet, 1 replica, `dispatch,storage`) → `Sink.Http` → `consumer` (Deployment, 2 replicas: Plug endpoint + Oban) → Oban jobs → `processed_webhooks` table. One Postgres StatefulSet holds two databases: `ankusa` for the WAL and `consumer` for Oban and the ground truth. Ankusa knows nothing about Oban; the consumer knows only the HTTP handoff contract.

**`ingest_app/`** (app `:ankusa_example_ingest`, module `AnkusaExample.Ingest.Application`, the same names as the other examples):
- `mix.exs`: deps `{:ankusa, path: "../../..", override: true}` and `{:ankusa_postgres, path: "../../../ankusa_postgres"}`, plus `releases: [ingest: [include_executables_for: [:unix]]]`. Add `.formatter.exs`.
- `config/config.exs`: logger format only (copy the other examples).
- `application.ex` builds `Ankusa.Config.new/1` from env vars, mirroring `examples/rabbitmq-consumer/ingest_app/.../application.ex` (the `env/2`, `env_int/2` and `parse_int/2` helpers):
  - `port: env_int("PORT", 4000)`, `data_dir: env("DATA_DIR", "./data")`, `roles: Ankusa.Config.parse_roles!(env("ANKUSA_ROLES", "edge,dispatch,storage"))`.
  - `wal: {Ankusa.WAL.Postgres, wal_opts() ++ [migrate: false]}`, where `wal_opts/0` returns `[hostname: env("WAL_DB_HOST","localhost"), port: env_int("WAL_DB_PORT",5432), username: env("WAL_DB_USER","ankusa"), password: env("WAL_DB_PASSWORD","ankusa"), database: env("WAL_DB_NAME","ankusa"), pool_size: env_int("WAL_DB_POOL_SIZE",10)]`.
  - Source `"demo"`: `verifier: {Ankusa.Verifier.None, []}`, `dedup: {Ankusa.DedupKey.Rules, json: ["id"]}`, `on_verify_failure: :accept_flag`, `sinks: [{Ankusa.Sink.Http, url: env("CONSUMER_URL", "http://localhost:4200/deliveries"), timeout_ms: 5_000}]`.
  - Log one startup line with roles and port.
- `lib/ankusa_example/ingest/release.ex` (`AnkusaExample.Ingest.Release`) has `migrate/0`: `{:ok, _} = Application.ensure_all_started(:postgrex)`, `{:ok, conn} = Postgrex.start_link(wal_opts)`, `Ankusa.WAL.Postgres.Migration.run!(conn)`. Make `wal_opts/0` public on `AnkusaExample.Ingest.Application` with `@doc false` and reuse it here.
- `Dockerfile`, with the repo root as build context:
  - Build stage `FROM elixir:1.20.4-alpine AS build`: `mix local.hex --force && mix local.rebar --force`, `ENV MIX_ENV=prod`. `WORKDIR /repo`; `COPY` `mix.exs mix.lock`, `lib`, `config` and `priv` into `/repo`, then `ankusa_postgres/` and `examples/oban-consumer/ingest_app/` at the same relative paths. `WORKDIR /repo/examples/oban-consumer/ingest_app`, `RUN mix deps.get --only prod && mix release ingest`.
  - Runtime stage `FROM elixir:1.20.4-alpine` (the same image, so ERTS/musl compatibility is guaranteed): `COPY --from=build …/_build/prod/rel/ingest /app`, `ENV RELEASE_DISTRIBUTION=none`, `EXPOSE 4000`, `CMD ["/app/bin/ingest","start"]`.

**`consumer_app/`** (app `:ankusa_example_consumer`, namespace `AnkusaExample.Consumer`; Hex deps only, no Ankusa dependency):
- deps: `{:bandit, "~> 1.12"}`, `{:plug, "~> 1.18"}`, `{:ecto_sql, "~> 3.14"}`, `{:postgrex, "~> 0.19"}`, `{:oban, "~> 2.24"}`. Release `consumer`. Add `.formatter.exs`.
- `config/config.exs`: `config :ankusa_example_consumer, ecto_repos: [AnkusaExample.Consumer.Repo]` and the logger format.
- `config/runtime.exs`:
  - `config :ankusa_example_consumer, AnkusaExample.Consumer.Repo, url: System.get_env("DATABASE_URL", "ecto://ankusa:ankusa@localhost:5432/consumer"), pool_size: String.to_integer(System.get_env("POOL_SIZE", "10"))`.
  - `config :ankusa_example_consumer, Oban, repo: AnkusaExample.Consumer.Repo, queues: [webhooks: 20], plugins: [{Oban.Plugins.Lifeline, rescue_after: :timer.seconds(30)}]`. Lifeline is required: it rescues jobs orphaned when the chaos phase kills a consumer pod mid-`perform`.
  - `config :ankusa_example_consumer, port: String.to_integer(System.get_env("PORT", "4200"))`.
- `Repo`: `use Ecto.Repo, otp_app: :ankusa_example_consumer, adapter: Ecto.Adapters.Postgres`.
- Migrations in `priv/repo/migrations/`:
  - `…_add_oban.exs`: `def up, do: Oban.Migration.up()` and `def down, do: Oban.Migration.down(version: 1)`.
  - `…_create_processed_webhooks.exs`: table `processed_webhooks` with `ankusa_id text PRIMARY KEY`, `source_id text NOT NULL`, `tenant_id text`, `body_sha256 text NOT NULL`, `deliveries integer NOT NULL DEFAULT 1`, `processed_at timestamptz NOT NULL`.
- `Router` (`Plug.Router`), `post "/deliveries"`:
  - Read the whole body, looping on `{:more, …}` with an 8,000,000-byte cap. Over the cap → 413.
  - Missing `x-ankusa-id` → 400 `{"error":"missing x-ankusa-id"}`.
  - Otherwise, `WebhookWorker.new(%{"ankusa_id" => id, "source_id" => header "x-ankusa-source", "tenant_id" => header "x-ankusa-tenant", "content_type" => header "content-type", "body_base64" => Base.encode64(body)}, unique: [period: :infinity, keys: [:ankusa_id]]) |> Oban.insert()`.
  - `{:ok, job}` → 202 `{"job_id": job.id, "duplicate": job.conflict?}`. `{:error, _}` → 503.
  - This is the contract: 2xx only after the enqueue commits. Any failure makes Ankusa retry.
  - `get "/health"` → 200 `{"status":"ok"}`; `match _` → 404. JSON via Elixir's built-in `JSON`.
- `WebhookWorker`: `use Oban.Worker, queue: :webhooks, max_attempts: 10`. `perform/1` decodes the body and computes the lowercase hex sha256, then runs `Repo.query!("INSERT INTO processed_webhooks (ankusa_id, source_id, tenant_id, body_sha256, deliveries, processed_at) VALUES ($1,$2,$3,$4,1,now()) ON CONFLICT (ankusa_id) DO UPDATE SET deliveries = processed_webhooks.deliveries + 1", [...])`, logs `processed ankusa_id=…`, and returns `:ok`.
- `Application` children in order: `Repo`, `{Oban, Application.fetch_env!(:ankusa_example_consumer, Oban)}`, `{Bandit, plug: Router, port: Application.fetch_env!(:ankusa_example_consumer, :port)}`.
- `Release.migrate/0`: the standard Ecto pattern (`Application.load/1`, then `Ecto.Migrator.with_repo(repo, &Ecto.Migrator.run(&1, :up, all: true))` for each of `ecto_repos`).
- `Dockerfile`, with `consumer_app/` as build context: multi-stage `elixir:1.20.4-alpine` → `mix release consumer`. Runtime: the same image, `RELEASE_DISTRIBUTION=none`, `CMD ["/app/bin/consumer","start"]`.
- Contingency: if Postgrex or Oban demands `Jason` at compile or run time, add `{:jason, "~> 1.4"}` and nothing else.

**`k8s/`**: plain manifests, applied in stages by `run.sh`. All objects are in namespace `ankusa-e2e`, with images `ankusa-oban-ingest:e2e` and `ankusa-oban-consumer:e2e` and `imagePullPolicy: IfNotPresent`.
- `kind.yaml`: one control-plane node, `extraPortMappings` 30080→8080 (edge) and 30432→15432 (Postgres).
- `00-namespace.yaml`.
- `01-postgres.yaml`:
  - ConfigMap `postgres-init` with `create-consumer.sql` containing `CREATE DATABASE consumer;`.
  - StatefulSet `postgres`, `postgres:16-alpine` (matches CI): env `POSTGRES_USER=ankusa`, `POSTGRES_PASSWORD=ankusa`, `POSTGRES_DB=ankusa`; args `["-c","max_connections=300"]`; the ConfigMap mounted at `/docker-entrypoint-initdb.d`; readiness `pg_isready -U ankusa -d ankusa`; a 2Gi volumeClaimTemplate.
  - Service `postgres` (ClusterIP 5432) and Service `postgres-external` (NodePort 30432).
- `10-migrate.yaml`:
  - Job `ankusa-migrate`: ingest image, command `["/app/bin/ingest","eval","AnkusaExample.Ingest.Release.migrate()"]`, with `WAL_DB_HOST=postgres` and the other `WAL_DB_*` values at their defaults.
  - Job `consumer-migrate`: consumer image, command `["/app/bin/consumer","eval","AnkusaExample.Consumer.Release.migrate()"]`, `DATABASE_URL=ecto://ankusa:ankusa@postgres:5432/consumer`.
  - Both use `backoffLimit: 6`.
- `20-consumer.yaml`: Deployment `consumer`, 2 replicas. Env `DATABASE_URL` as above and `PORT=4200`. Readiness and liveness probe httpGet `/health` on 4200. Service `consumer` port 80 → 4200.
- `30-ankusa.yaml`:
  - Deployment `ankusa-edge`, 3 replicas, label `app: ankusa-edge`. Env `ANKUSA_ROLES=edge`, `PORT=4000`, `DATA_DIR=/data` (emptyDir; only the quarantine log lands here, and the demo source never quarantines), `WAL_DB_HOST=postgres`, `CONSUMER_URL=http://consumer/deliveries`. Readiness and liveness httpGet `/health` on 4000.
  - Service `ankusa-edge`: NodePort, port 80 → 4000, nodePort 30080.
  - StatefulSet `ankusa-worker`, `replicas: 1`, with a comment that it is a singleton by design (see `docs/deployment.md`). Env `ANKUSA_ROLES=dispatch,storage`, the same WAL and `CONSUMER_URL` env, `DATA_DIR=/data` on a 1Gi volumeClaimTemplate. No probes (no HTTP listener).

**`run.sh`** (bash, `set -euo pipefail`, run from anywhere; it `cd`s to the repo root via `git rev-parse --show-toplevel`):
- Env knobs: `CLUSTER=ankusa-e2e`, `RATE=100`, `DURATION=60`, `BURST_SECONDS=15`, `CONCURRENCY=64`, `KEEP=0`, `OUT_DIR=examples/oban-consumer/.e2e-out`.
- Requires `kind`, `kubectl`, `docker` and `mix` on PATH; any missing → print the missing tool and exit 2. `kind` is not installed on the author's machine today (only `kubectl` and `docker` are), so the README lists `brew install kind`.
- Sequence:
  1. `kind create cluster --name $CLUSTER --config examples/oban-consumer/k8s/kind.yaml`, skipped if `kind get clusters` already lists it.
  2. `docker build -t ankusa-oban-ingest:e2e -f examples/oban-consumer/ingest_app/Dockerfile .` and `docker build -t ankusa-oban-consumer:e2e examples/oban-consumer/consumer_app`.
  3. `kind load docker-image … --name $CLUSTER` for both images.
  4. Apply `00` and `01`, then `kubectl -n ankusa-e2e rollout status statefulset/postgres --timeout=180s`.
  5. Apply `10`, then `kubectl -n ankusa-e2e wait --for=condition=complete job/ankusa-migrate job/consumer-migrate --timeout=180s`.
  6. Apply `20` and `30`; wait for `rollout status` of `deployment/consumer`, `deployment/ankusa-edge` and `statefulset/ankusa-worker`.
  7. Poll `curl -sf localhost:8080/health` until it answers (60s cap).
- Phases, each writing `$OUT_DIR/<phase>.csv` and `$OUT_DIR/<phase>-report.json` via the loadgen from step 7 (run with `mix` inside `tools/loadgen`):
  1. **steady**: `loadgen.run --url http://localhost:8080/webhooks/demo --rate $RATE --duration $DURATION`, then `loadgen.verify --timeout 300`.
  2. **chaos**: the same run in the background. At +10s `kubectl delete pod` the first `app=ankusa-edge` pod; at +20s `ankusa-worker-0`; at +30s the first `app=consumer` pod. Then `wait` for the loadgen and run `loadgen.verify --timeout 300`.
  3. **burst**: `loadgen.run --concurrency $CONCURRENCY --duration $BURST_SECONDS` (no rate cap), then `loadgen.verify --timeout 900`. The verify's reported drain time is the dispatch throughput measurement.
- Verify always uses `--database-url postgres://ankusa:ankusa@localhost:15432/consumer`.
- Exit status is non-zero if any verify fails. A `trap` deletes the cluster on exit unless `KEEP=1`.
- Add `examples/oban-consumer/.e2e-out/` to `.gitignore`.

**`README.md`** for the example: the topology diagram, what each piece proves, prerequisites, `./run.sh`, the knobs, how to read the report, and the explicit statement that Oban appears only in `consumer_app/`, which talks to Ankusa over HTTP.

### 7. `tools/loadgen/`: load generator and loss verifier (unpublished Mix project)

- `mix.exs`: app `:loadgen`, deps `{:req, "~> 0.7"}`, `{:postgrex, "~> 0.19"}`. Add `.formatter.exs`. Both tasks call `Mix.Task.run("app.start")`.
- `Mix.Tasks.Loadgen.Run`:
  - Options (`OptionParser` strict): `--url` (required), `--concurrency` (default 64), `--duration` (seconds, default 60), `--rate` (total req/s; absent means closed loop), `--dup-ratio` (default 0.05), `--body-bytes` (default 512), `--out` (default `acked.csv`), `--report` (default `loadgen-report.json`).
  - Start `{Finch, name: Loadgen.Finch, pools: %{default: [size: concurrency]}}` and spawn `concurrency` workers that loop until the deadline.
  - With `--rate`, worker `i` fires at `t0 + i*(concurrency/rate) + k*(concurrency/rate)` seconds, which paces the fleet to `rate`.
  - Each request is `POST` with `content-type: application/json` and body `{"id": <fresh 16-byte hex>, "n": <counter>, "pad": <"x" × body-bytes>}`. With probability `dup-ratio` it instead resends a body this worker already got a 201 for. The source's `DedupKey.Rules json: ["id"]` makes that a duplicate.
  - Record latency in microseconds for every request.
  - Classify responses:
    - 201: parse `"id"` from the JSON response and record `id,sha256hex(body)`.
    - 200 whose JSON has `"status":"duplicate"`: a duplicate.
    - 503: shed.
    - Any other status or a transport error: an error. Errors are expected during chaos; only acked ids must survive.
  - Write the acked lines to `--out`. Write `--report` JSON with the keys `sent, accepted, duplicates, shed, errors, duration_s, accepted_per_s, latency_ms: {p50,p95,p99,max}`, and print the same numbers as a table.
  - Exit 1 only if `accepted == 0`.
- `Mix.Tasks.Loadgen.Verify`:
  - Options: `--acked` (required), `--database-url` (required, parsed into Postgrex opts with `URI.parse/1`), `--timeout` (seconds, default 300), `--report` (default `verify-report.json`).
  - Every 2s, query `SELECT ankusa_id, body_sha256, deliveries FROM processed_webhooks WHERE ankusa_id = ANY($1)` in chunks of 5,000 ids, until every acked id is present or the timeout expires.
  - Report `acked, processed, missing` (with up to 10 example ids), `sha_mismatches`, `extra_deliveries` (sum of `deliveries - 1`) and `drain_s` (seconds from start until the last id appeared).
  - Also report `unacked_processed = count(*) FROM processed_webhooks` minus acked. These are hooks committed but whose response was lost when a pod died; the count is informational.
  - Exit 1 if `missing > 0` or `sha_mismatches > 0`.
- `tools/loadgen/README.md`: both commands and their pass/fail meaning.

### 8. `docs/integrations.md`: using Ankusa with a job framework without coupling to one

Sections, in order:
1. **The seam.** Ankusa never knows your job system. The handoff is an `Ankusa.Sink`, and delivery is at-least-once. Quote the callback `@callback deliver(Envelope.t(), ctx(), opts :: keyword()) :: :ok | {:error, term()}` from `lib/ankusa/sink.ex`.
2. **HTTP handoff (any language).** The `Ankusa.Sink.Http` config snippet. The contract: the raw body plus the headers `x-ankusa-id`, `x-ankusa-source`, `x-ankusa-tenant`, `x-ankusa-seq` and `content-type`. Respond 2xx only after the job is durably enqueued. A non-2xx response or timeout is retried per `dispatch.retry`, then dead-lettered. Dedupe on `x-ankusa-id`.
3. **Oban.** Verbatim excerpts of `consumer_app`'s `/deliveries` route and `WebhookWorker` from step 6, with a link to `examples/oban-consumer`. Then an "in-process" subsection for apps that embed Ankusa: a custom sink `defmodule MyApp.ObanSink do @behaviour Ankusa.Sink; @impl true def deliver(env, ctx, _opts)` that calls `Oban.insert/1` with the same `unique: [period: :infinity, keys: [:ankusa_id]]`, maps `{:ok, _}` → `:ok` and `{:error, r}` → `{:error, r}`, and is configured as `sinks: [{MyApp.ObanSink, []}]`.
4. **Celery.** A Flask endpoint. `POST /deliveries` returns 400 if `x-ankusa-id` is missing. Otherwise it calls `process_webhook.apply_async(args=[ankusa_id, source, tenant, content_type, base64(body)], task_id=ankusa_id)` and returns `202`. `apply_async` raising (broker down) yields a 5xx, which Ankusa retries. The task is declared with `acks_late=True`. Recommend `broker_transport_options={"confirm_publish": True}` on RabbitMQ. Note that Celery does not dedupe on `task_id`, so the task body must be idempotent on `ankusa_id`.
5. **Queue handoff.** Consumers already on RabbitMQ or Kafka can take `Ankusa.Sink.RabbitMQ`/`Ankusa.Sink.Kafka` and the `Ankusa.Sink.Message` format. Link to `delivery.md` and the two existing examples.

Link the new page from the `README.md` Documentation table and from `docs/README.md`. It is already in the ExDoc extras from step 3.

### 9. CI and agent notes for the new projects

- `.github/workflows/ci.yml`: add job `oban-example` (no services), with a matrix over `examples/oban-consumer/ingest_app`, `examples/oban-consumer/consumer_app` and `tools/loadgen`.
  - Each entry runs `mix deps.get --check-locked`, `mix deps.unlock --check-unused`, `mix format --check-formatted`, then `MIX_ENV=prod mix compile --warnings-as-errors`, except `tools/loadgen`, which uses plain `mix compile --warnings-as-errors`.
  - Generate and commit each project's `mix.lock`.
- `AGENTS.md` check table: add rows for `examples/oban-consumer/ingest_app` and `consumer_app` (`mix compile --warnings-as-errors`), `tools/loadgen` (the same), and the e2e gate `examples/oban-consumer/run.sh` (before any release tag).
- `docs/testing.md`: add a `## Load and end-to-end (kind + Oban)` section explaining `run.sh`. It includes a results table from the verification run: machine, `RATE`, `DURATION`, and per phase: accepted/s, p50/p95/p99, shed, errors, missing, extra deliveries, drain_s.

## Critical files & anchors

- `lib/ankusa/config.ex` — `new/1` (lines 62–77): the nested-map merge and unknown-key logic being tightened.
- `lib/ankusa/application.ex` — `default_instance/0` (line 20: autostart default) and `roles/0` (line 58: `String.to_atom`).
- `lib/ankusa/dispatch/pipeline.ex` — `drain/1` (77–85) and `deliver_with_retry/4` (97–125): the source of the serial-dispatch and head-of-line limits that step 5 documents. Do not change them.
- `ankusa_postgres/lib/ankusa/wal/postgres.ex` — `start_link/1` (54–66): `migrate: true` by default runs DDL on every node, which is why the example migrates once via a Job and sets `migrate: false`.
- `.github/workflows/release.yml` — replaced wholesale. The old push-to-`main` publish logic must not survive anywhere.

## Verification

Everything runs from the repo root unless a directory is given. Run each item after its step.

1. **Step 1.**
   - `mix test test/ankusa/config_test.exs` passes, and the full core `mix format --check-formatted && mix compile --warnings-as-errors && mix test` passes.
   - Boot check: `ANKUSA_ROLES=edgee mix run -e ''` exits non-zero with `unknown Ankusa role "edgee"`, and `mix run -e ''` still boots the default instance on :4000 (`config/config.exs` enables autostart in `:dev`).
   - In `ankusa_postgres/` (after `docker compose up -d --wait`), `mix test` passes with the `config/` line removed, which shows that Registry-only startup works without autostart config.
   - Repeat for `ankusa_rabbitmq`, and for `ankusa_kafka` via the AGENTS.md container recipe.
2. **Step 2.** `mix test test/ankusa/sink/http_test.exs` passes with the new tenant assertion.
3. **Step 3.**
   - In each of the four packages: `mix docs --warnings-as-errors` exits 0. (If that flag is rejected by ex_doc 0.40, run `mix docs 2>&1 | grep -i warning` and require empty output.)
   - `open doc/index.html` shows the README with working Guides links.
   - `mix hex.build --unpack` in the root lists only `lib/ priv/ .formatter.exs mix.exs README.md LICENSE CHANGELOG.md`.
   - `MIX_ENV=prod mix hex.build` in each adapter reports requirement `ankusa ~> 0.1` and no path dependency. If it errors only because `ankusa` isn't on Hex yet, record that as expected; it is re-checked at publish.
4. **Step 4.**
   - `actionlint .github/workflows/*.yml` is clean (or `docker run --rm -v "$PWD":/repo -w /repo rhysd/actionlint:latest`).
   - Dry-run the resolve script locally: with `GITHUB_REF_NAME=ankusa_postgres-v0.1.0` it resolves `DIR=ankusa_postgres VERSION=0.1.0`, and with `GITHUB_REF_NAME=ankusa-v9.9.9` it exits 1 with the version-mismatch error.
   - Dry-run the awk notes extraction against the root `CHANGELOG.md` for `0.1.0`: it prints exactly that section.
5. **Steps 6–7, the main proof.**
   - `examples/oban-consumer/run.sh` exits 0.
   - For `steady` and `chaos`, `verify-report.json` shows `missing: 0` and `sha_mismatches: 0`.
   - The chaos loadgen report shows nonzero `accepted` after each kill time, and the kubectl delete lines appear in the output.
   - `burst` shows `missing: 0` and records `drain_s`.
   - Paste the numbers into the `docs/testing.md` results table (step 9).
   - Then run `KEEP=1 examples/oban-consumer/run.sh`, and `kubectl -n ankusa-e2e get pods` shows 3 edge, 1 worker, 2 consumer and 1 postgres pod `Running`. `curl -XPOST localhost:8080/webhooks/demo -H 'content-type: application/json' -d '{"id":"manual-1"}'` returns 201. Within 10s, `psql postgres://ankusa:ankusa@localhost:15432/consumer -c "select ankusa_id, deliveries from processed_webhooks order by processed_at desc limit 1"` shows the returned id with `deliveries = 1`. Tear down with `kind delete cluster --name ankusa-e2e`.
6. **Step 8, doc snippets.**
   - Throwaway (not committed): paste `MyApp.ObanSink` into `consumer_app/lib/`, add `{:ankusa, path: "../../.."}` temporarily, and run `mix compile --warnings-as-errors`. It compiles. Revert both.
   - Celery throwaway:
     - `docker run -d --name celery-redis -p 6379:6379 redis:7-alpine`.
     - Run the doc's Flask app plus `celery -A app worker` in a `python:3.13-slim` container (`pip install flask celery redis`, `CELERY_BROKER_URL=redis://host.docker.internal:6379/0`) on host port 5055.
     - Start Ankusa: `mix run -e 'Ankusa.Instance.start_link(Ankusa.Config.new(instance: :celery_smoke, port: 4100, data_dir: "/tmp/ankusa-celery-smoke", source_store: {Ankusa.SourceStore.Static, sources: %{"demo" => [sinks: [{Ankusa.Sink.Http, url: "http://localhost:5055/deliveries"}]]}})); Process.sleep(:infinity)'`.
     - `curl -XPOST localhost:4100/webhooks/demo -d '{"id":"c1"}'` returns 201 with an id, and the Celery worker log shows the task executed with that same id. Clean up the containers and `/tmp/ankusa-celery-smoke`.
7. **Decoupling proof.** `grep -rniE 'oban|celery' lib ankusa_postgres/lib ankusa_rabbitmq/lib ankusa_kafka/lib mix.exs ankusa_*/mix.exs` returns nothing.
8. **Every lockfile is current.** In each Mix project (root, three adapters, three `examples/*/ingest_app`, `consumer_app`, `tools/loadgen`), `mix deps.get --check-locked && mix deps.unlock --check-unused` passes.

## Assumptions & contingencies

- The user publishes. The implementer never pushes a tag and never runs `mix hex.publish`. The first release order is in step 4's docs: `ankusa-v0.1.0` first, adapters after it is live on Hex.
- `HEX_API_KEY` is configured as a repository secret by the user before tagging.
- If `kind load` or image builds fail on Apple Silicon because of architecture, add `--platform linux/arm64` to both `docker build` commands in `run.sh`. Do not add emulation.
- If the steady phase shows a growing backlog at `RATE=100` (the verify times out with `missing > 0` while dispatch is still progressing), lower the `run.sh` default `RATE` to half the measured burst drain rate (`accepted / drain_s`) and record that ceiling in `docs/testing.md`. Do not change `Ankusa.Dispatch.Pipeline`; concurrent dispatch is out of scope for 0.1.0 and listed under Not yet implemented.
- If concurrent `CREATE DATABASE`/DDL issues appear anyway, the fix stays inside the example (Jobs plus `migrate: false`). Core and `ankusa_postgres` code are unchanged apart from the README guidance.
