# Ankusa server: Docker image, config file, operator API

## Context

Ship Ankusa as a standalone server that people run without knowing Elixir, the way Elasticsearch is run without knowing Java: `docker run ankusa/ankusa`, configured by a mounted YAML file plus env vars, operated over HTTP. Outcome: multi-arch images on Docker Hub at `ankusa/ankusa`, a `<pkg>-vX.Y.Z` tag scheme (`ankusa_server-vX.Y.Z`) with matching mise preflight/tag tasks, and a GitHub Actions workflow that builds, smoke-tests, and pushes. Non-Elixir operators also need HTTP access to things that are IEx-only today (DLQ list/replay, quarantine, metrics). No client SDKs.

Decisions already made (do not revisit): image `ankusa/ankusa`; one image with all adapters (Postgres WAL, RabbitMQ, Kafka); YAML config at `/etc/ankusa/ankusa.yml` with `${VAR}` interpolation and `ANKUSA_*` env overrides; admin API + Prometheus `/metrics` built into **core** (so embedded users get it); independent versioning via a new unpublished Mix project `ankusa_server/` with its own `@version`, CHANGELOG, and `ankusa_server-vX.Y.Z` tag. CI is GitHub Actions (the repo has no GitLab config; "gitlab action" in the request means the GitHub workflow).

**Auth model: bring your own.** Ankusa does not manage users, API keys, or tokens. The only built-in request authentication is provider signature verification on ingest (Stripe/GitHub/Standard Webhooks), which is part of webhook semantics, not access control. The admin API has no authentication. The claim-check gateway's existing bearer tokens become optional: none configured means an open gateway. Operators protect ports 4001/4002 with their own reverse proxy (nginx, oauth2-proxy, an API gateway) or network policy, the way Elasticsearch was run before it shipped security. Every surface is on its own port so it can be firewalled on its own. Do not add any token, API-key, or user setting to the server config beyond the optional claim-check tokens that already exist in core.

## Approach

Steps 1–4 change core and must leave every package green before step 5 starts. Steps 5–8 build `ankusa_server/`. Steps 9–10 are release plumbing and depend on 5–8.

### 1. Core: `admin` config section; optional claim-check tokens

- `lib/ankusa/config.ex`: add a fifth deep-merged section to the `defstruct`, after `claim_check`:
  ```elixir
  # operator HTTP API + Prometheus /metrics, unauthenticated; off by default for embedded use
  admin: %{enabled: false, port: 4002}
  ```
  Add `:admin` to the section list in `new/1` (`k in [:batcher, :dispatch, :storage, :claim_check, :admin]`) and to the `new/1` `@doc`.
- `lib/ankusa/claim_check.ex` `validate_config!/1`: delete the `if cc.api_tokens == %{}` raise (~lines 147–151) and its moduledoc bullet "a `:claim_check`-role node with no `api_tokens`" (~line 128).
- `lib/ankusa/claim_check/router.ex` `authenticate/2` (~118–128): add a first clause. When `api_tokens == %{}`, return `{:ok, :all}` without reading the header. Comment: `# No tokens configured: authentication is delegated to whatever fronts this port.` When tokens are configured, behavior is exactly as today (401 on a missing or unknown token, tenant scoping via `authorize/2`).
- `lib/ankusa/claim_check/remote.ex`: make `:token` optional. In `request/6` (~89–97), add the `authorization` header only when `Keyword.get(opts, :token)` is non-nil. Change the moduledoc line `:token — required bearer token` to `:token — optional bearer token; omit when the gateway has no api_tokens`.
- `lib/ankusa/instance.ex` `claim_check_children/2`: when the role is enabled and `config.claim_check.api_tokens == %{}`, log `Logger.warning("[ankusa] claim-check API on :#{port} has no api_tokens and is unauthenticated; protect it with your own proxy or network policy")`.
- Tests:
  - In `test/ankusa/claim_check_test.exs`, replace the test "rejects a :claim_check-role node with no api_tokens" (~166–172) with one asserting that `validate_config!/1` returns `:ok` for `roles: [:claim_check]` and no tokens.
  - In `test/ankusa/claim_check/router_test.exs`, add a test with a separately built config (`api_tokens` left at `%{}`): a PUT with no `authorization` header returns 201, and a GET of the same id returns the bytes.
  - The existing 401 tests stay as they are; their setup configures tokens.

### 2. Core: Prometheus metrics

- `mix.exs` (core) deps: add `{:telemetry_metrics, "~> 1.2"}` and `{:telemetry_metrics_prometheus_core, "~> 1.2"}` (latest stable on Hex: 1.2.0 / 1.2.1). Core is the right place because every deployment needs metrics (AGENTS.md dependency rule).
- New `lib/ankusa/metrics.ex`, `Ankusa.Metrics`:
  - `@spec reporter_name(atom()) :: atom()` → `:"ankusa_metrics_#{instance}"` (bounded: one atom per configured instance).
  - `@spec child_spec(keyword()) :: Supervisor.child_spec()` delegating to `{TelemetryMetricsPrometheus.Core, name: reporter_name(instance), metrics: metrics(), start_async: false}`, with `id: {__MODULE__, instance}`.
  - `@spec scrape(atom()) :: String.t()` → `TelemetryMetricsPrometheus.Core.scrape(reporter_name(instance))`.
  - `metrics/0` defines exactly these metrics (`import Telemetry.Metrics`). Tag values go through `tag_values: &normalize/1`, which maps any tag value that is not an atom or binary to its leading atom (`{tag, _}` → `tag`, anything else → `:other`) and module atoms to `inspect/1` strings, so labels stay bounded:

    | Metric name | Type | event_name / measurement | Tags | Options |
    | --- | --- | --- | --- | --- |
    | `ankusa.ingest.requests.total` | counter | `[:ankusa, :ingest, :stop]` | instance, source_id, outcome | |
    | `ankusa.ingest.duration.seconds` | distribution | `[:ankusa, :ingest, :stop]` / `:duration` | instance, source_id | `unit: {:native, :second}`, buckets `[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5]` |
    | `ankusa.verify.failures.total` | counter | `[:ankusa, :verify, :stop]`, `keep: &(&1.status == :failed)` | instance, source_id, provider | |
    | `ankusa.wal.commit.duration.seconds` | distribution | `[:ankusa, :commit, :stop]` / `:duration` | instance | same unit/buckets |
    | `ankusa.wal.commit.batch.size` | sum | `[:ankusa, :commit, :stop]` / `:batch_size` | instance | |
    | `ankusa.dedup.hits.total` | counter | `[:ankusa, :dedup, :hit]` | instance, source_id | |
    | `ankusa.load_shed.total` | counter | `[:ankusa, :load_shed]` | instance | |
    | `ankusa.quarantine.rate_limited.total` | counter | `[:ankusa, :quarantine, :rate_limited]` | instance, source_id | |
    | `ankusa.dispatch.deliveries.total` | counter | `[:ankusa, :dispatch, :stop]` | result | |
    | `ankusa.dispatch.dead_lettered.total` | counter | `[:ankusa, :dispatch, :dlq]` | source_id, sink | |
    | `ankusa.compact.records.total` | sum | `[:ankusa, :compact, :stop]` / `:records` | instance | |
    | `ankusa.compact.bytes.total` | sum | `[:ankusa, :compact, :stop]` / `:bytes` | instance | `unit: :byte` |
    | `ankusa.claim_check.operations.total` | counter | `[:ankusa, :claim_check, :check_in]` and a second def for `:redeem` | adapter, result | name the redeem one `ankusa.claim_check.redeems.total` |

    Only tags listed in `lib/ankusa/telemetry.ex`'s table for that event are used (the dispatch events carry no `:instance`).
- Update the `lib/ankusa/telemetry.ex` moduledoc with one line pointing at `Ankusa.Metrics` as the built-in Prometheus mapping.

### 3. Core: admin HTTP API

- New `lib/ankusa/admin/redact.ex`, `Ankusa.Admin.Redact.config(%Ankusa.Config{}) :: map()`. It returns a JSON-encodable map of the whole config:
  - Every `{module, opts}` tuple becomes `%{"module" => inspect(mod), "opts" => map}`.
  - Keyword lists become maps and atoms become strings.
  - The value of any key named `secret`, `password`, `secret_access_key`, `token`, `api_tokens`, or `sasl` becomes `"[REDACTED]"`.
  - Any string that parses as a URI with userinfo `user:pass` is rewritten to `user:[REDACTED]`.
  - Functions render as `"#Function"`.
  - Sources come from `SourceStore.Static`'s `sources:` opts.
- New `lib/ankusa/admin/router.ex`, `Ankusa.Admin.Router` (`use Plug.Router, copy_opts_to_assign: :ankusa_opts`, the same shape as `Ankusa.ClaimCheck.Router`). Respond with `Ankusa.Http.send_json/3`. Routes:

  | Method/path | Role needed | Behavior |
  | --- | --- | --- |
  | `GET /health` | any | `200 {"status":"ok","instance":"<name>","roles":["edge",...]}` |
  | `GET /metrics` | any | `200`, `content-type: text/plain; version=0.0.4`, body `Ankusa.Metrics.scrape(instance)` |
  | `GET /v1/config` | any | `200` with `Ankusa.Admin.Redact.config(Ankusa.config(instance))` |
  | `GET /v1/dlq?source_id=&since=&limit=` | `:dispatch` | Reads `Ankusa.Dispatch.DLQ.entries/1`, applies the same filter semantics as `Ankusa.Dispatch.replay/2`, and sorts newest first by `at`. `limit` defaults to 100 and is capped at 1000. Returns `{"total": <matching count>, "entries": [{"id","source_id","tenant_id","seq","received_at","dead_lettered_at","size","content_type","reason"}]}`, where `reason` is `inspect/1` of the stored reason. Bodies are never returned. |
  | `POST /v1/dlq/replay` | `:dispatch` | JSON body with optional `source_id` (string), `id` (string), `since` (integer unix ms). Calls `Ankusa.Dispatch.replay(instance, filter)` and returns `200 {"replayed": n}`. An empty body replays everything. |
  | `GET /v1/quarantine?limit=` | `:edge` | `Ankusa.Edge.Quarantine.recent(instance)` (this node's in-memory recent list, up to 200), `limit` default 100. Returns `{"entries":[{"id","source_id","received_at","reason"}]}` with `reason` as `inspect/1`. |
  | anything else | any | `404 {"error":"not_found"}` |

  No route authenticates or reads the `authorization` header. Redaction still applies to `/v1/config`, because an operator's proxy may let many people read it.

  Error bodies:
  - `409 {"error":"role_not_enabled","role":"dispatch"}` (or `"edge"`) when the route needs a role this node doesn't run.
  - `400 {"error":"invalid_filter","field":"since"}` for a non-integer `since`/`limit` or a non-JSON replay body. Read the replay body with `Http.read_body_limited(conn, 65_536)`.
- `lib/ankusa/instance.ex`: add `admin_children(config, opts)` to the `init/1` child list (last). When `config.admin.enabled` is true it returns `[{Ankusa.Metrics, opts}, Supervisor.child_spec({Bandit, plug: {Ankusa.Admin.Router, [instance: config.instance]}, scheme: :http, port: config.admin.port}, id: Ankusa.Admin.Router)]`, and `[]` otherwise. The metrics reporter goes first so no events are missed. When enabled, log once at init: `Logger.warning("[ankusa] admin API on :#{port} is unauthenticated; do not expose it publicly, front it with your own proxy or network policy")`.
- New `priv/openapi/admin.v1.yaml`: an OpenAPI 3.1 spec for the table above, in the same style as the existing `priv/openapi/claim_check.v1.yaml`. Also new `priv/openapi/ingest.v1.yaml`, documenting the edge API exactly as `lib/ankusa/edge/router.ex` implements it: `POST {prefix}/{source_id}` and `{prefix}/{tenant_id}/{source_id}`; 201/200/202/400/401/404/413/503 with `Retry-After`; and `GET /health`, `GET /stats`.

### 4. Core tests and lockfiles

- New `test/ankusa/admin/router_test.exs`, in the `Plug.Test` + `Router.call` style of `test/ankusa/claim_check/router_test.exs`. Use `Ankusa.TestHelpers.test_config/1` with `admin: %{enabled: true}`. Cover:
  - Requests carry no `authorization` header and still get 200: the API has no auth.
  - `/v1/dlq` on a config with `roles: [:edge]` → 409 with `"role":"dispatch"`.
  - Two DLQ records written with `Ankusa.Dispatch.DLQ.write/3`: `GET /v1/dlq?source_id=a` returns only `a`'s entry, with no `body` key.
  - `POST /v1/dlq/replay` with `{"id": ...}` returns `{"replayed":1}`, and the source's `Sink.Http` target (a `Req.Test` plug, the pattern existing sink tests use) receives the body.
  - `/v1/config` never contains a configured Stripe secret or a URL password.
- New `test/ankusa/metrics_test.exs`: start an instance with admin enabled (as the existing instance tests do), ingest one hook to the `demo`-style source, and assert that `Ankusa.Metrics.scrape(inst)` contains `ankusa_ingest_requests_total{` with `outcome="committed"`.
- After the core dep change, run `mix deps.get` and commit `mix.lock` in `.`, `ankusa_postgres`, `ankusa_rabbitmq`, `ankusa_kafka`, `examples/rabbitmq-consumer/ingest_app`, `examples/kafka-sqs-consumer/ingest_app`, `examples/oban-consumer/ingest_app`, and `tools/loadgen` if it path-depends on core. Check its `mix.exs` first; if it doesn't, skip it. CI runs `--check-locked` everywhere.

### 5. `ankusa_server/` Mix project and release

- `ankusa_server/mix.exs`: `app: :ankusa_server`, `@version "0.1.0"` (the `  @version "..."` line format that the mise/release `sed` extracts), `elixir: "~> 1.20"`, `start_permanent: Mix.env() == :prod`, `application: [extra_applications: [:logger], mod: {AnkusaServer.Application, []}]`. Deps:
  ```elixir
  {:ankusa, path: "..", override: true},
  {:ankusa_postgres, path: "../ankusa_postgres"},
  {:ankusa_rabbitmq, path: "../ankusa_rabbitmq"},
  {:ankusa_kafka, path: "../ankusa_kafka"},
  {:yaml_elixir, "~> 2.12"}
  ```
  Use `override: true` for the same reason as `examples/kafka-sqs-consumer/ingest_app/mix.exs`: the adapters pick Hex `:ankusa` in `:prod`. Add `releases: [ankusa: [include_executables_for: [:unix], applications: [ankusa_server: :permanent]]]`. This project is never published to Hex, so there is no `package:`.
- `ankusa_server/config/config.exs`: `import Config` and `config :logger, :console, format: "$time [$level] $message\n", metadata: []`. Do **not** set `:ankusa, autostart`. It defaults to false, and `AnkusaServer.Application` owns the instance.
- `ankusa_server/.formatter.exs` (same as root), `ankusa_server/.gitignore` (`/_build/`, `/deps/`), `ankusa_server/CHANGELOG.md` with a `## [Unreleased]` and a `## [0.1.0]` section (the preflight greps for `^## \[0.1.0\]`), and `ankusa_server/test/test_helper.exs`.
- `lib/ankusa_server/application.ex`, `AnkusaServer.Application.start/2`:
  1. Call `AnkusaServer.Config.load!()`. On `AnkusaServer.ConfigError`, write `"ankusa: invalid configuration\n  " <> message` to stderr and call `System.halt(78)`, so the container exits cleanly with EX_CONFIG instead of printing a crash dump.
  2. Call `Logger.configure(level: loaded.log_level)`.
  3. Log `"[ankusa] ankusa_server #{version} roles=#{inspect(config.roles)} http=#{config.port} admin=#{admin_port_or_off} wal=#{inspect(elem(config.wal, 0))} sources=#{Enum.join(source_ids, ",")}"`.
  4. Call `Supervisor.start_link([{Ankusa.Instance, config}], strategy: :one_for_one, name: AnkusaServer.Supervisor)`. `Ankusa.Registry` is already started by core's `Ankusa.Application` (see `lib/ankusa/application.ex:11`).

### 6. Config loader: YAML schema, interpolation, env overrides

New `lib/ankusa_server/config.ex`, `AnkusaServer.Config`, and `lib/ankusa_server/config_error.ex` (`defexception [:message]`).

`@spec load!(keyword()) :: %{config: Ankusa.Config.t(), log_level: Logger.level()}`. Options: `env:` (map, default `System.get_env()`) and `path:` (overrides `ANKUSA_CONFIG`). Pipeline:

1. **Locate the file.** If `env["ANKUSA_CONFIG"]` (or `path:`) is set, read it; a missing file is a `ConfigError`: `"config file #{path} not found (ANKUSA_CONFIG)"`. Otherwise try `/etc/ankusa/ankusa.yml`, then `./ankusa.yml`. If neither exists, use `%{}` and log a warning: `"no config file found; starting with defaults and no sources"`. Parse with `YamlElixir.read_from_file/1`. A parse error becomes a `ConfigError` whose message includes the file path and yamerl's line/column text. An empty file becomes `%{}`.
2. **Interpolate.** Walk every string value recursively, replacing `${NAME}` and `${NAME:-default}`, where `NAME` matches `[A-Z0-9_]+`, with `env[NAME]` or the default. If a referenced variable is unset and has no default, raise `ConfigError` with the dotted key path: `"sources.stripe.verify.secret: ${STRIPE_WHSEC} is not set"`. There is no escape syntax.
3. **Apply env overrides.** Env wins over the file. Values stay strings and are coerced in step 4. The table:

   | Env var | YAML path |
   | --- | --- |
   | `ANKUSA_ROLES` | `node.roles` (comma-separated) |
   | `ANKUSA_DATA_DIR` | `node.data_dir` |
   | `ANKUSA_LOG_LEVEL` | `log.level` |
   | `ANKUSA_HTTP_PORT`, else `PORT` | `http.port` |
   | `ANKUSA_ADMIN_PORT` | `admin.port` |
   | `ANKUSA_CLAIM_CHECK_PORT` | `claim_check.port` |
   | `ANKUSA_WAL_TYPE` | `wal.type` |
   | `ANKUSA_WAL_POSTGRES_URL` | `wal.postgres.url` |
   | `ANKUSA_STORAGE_TYPE` | `storage.type` |
   | `ANKUSA_S3_BUCKET`, `ANKUSA_S3_REGION`, `ANKUSA_S3_ENDPOINT` | `storage.s3.bucket/region/endpoint` |
   | `ANKUSA_GCS_BUCKET` | `storage.gcs.bucket` |

   Sources are not env-overridable; operators inject secrets into them through `${VAR}`.
4. **Validate and translate.** Use a small hand-written schema walker: one private function per section, each taking `(map, path)`. Any key not in the schema raises `ConfigError` `"<path>: unknown key \"<k>\""`. A wrong type raises `"<path>: expected <type>, got <inspect>"`. An enum miss raises `"<path>: unknown value \"<v>\"; expected one of <a>, <b>"`. Integers accept YAML ints or digit strings (from env). Schema and translation into `Ankusa.Config.new/1` keywords (every key optional; defaults in brackets):

   ```yaml
   node:
     roles: [edge, dispatch, storage]     # list or "a,b" string → roles: via Ankusa.Config.parse_roles!/1
     data_dir: ./data                     # → data_dir:
   log:
     level: info                          # debug|info|warning|error → log_level
   http:
     port: 4000                           # → port:
     max_body_bytes: 8000000              # → max_body_bytes:
     routing: path                        # path → Ankusa.RouteResolver.Path, tenant_path → TenantPath
     prefix: /webhooks                    # → opts prefix: String.split(p, "/", trim: true)
   admin:
     enabled: true                        # server default TRUE (core default is false) → admin.enabled
     port: 4002                           # unauthenticated; protect with your own proxy/network policy
   batcher: {partitions, max_batch, max_delay_ms, max_queue}        # 1:1 → batcher:
   dispatch:
     poll_ms: 200
     batch: 128
     retry: {base_ms: 100, max_ms: 30000, max_attempts: 12, jitter: true}  # → {Ankusa.RetryPolicy.Exponential, [...]}
   wal:
     type: disk                           # disk → {Ankusa.WAL.DiskLog, []}; postgres → {Ankusa.WAL.Postgres, opts}
     postgres:
       url: postgres://user:pass@host:5432/db   # parsed with URI.parse → hostname/port(5432)/username/password/database
       # or discrete: host, port, username, password, database (url and discrete keys are mutually exclusive → ConfigError)
       pool_size: 10
       ssl: false
       migrate: true
   storage:
     type: local                          # local → BlobStore.LocalFS; s3 → BlobStore.S3; gcs → BlobStore.GCS
     roll_bytes: 16777216
     roll_ms: 30000
     s3: {bucket, region, endpoint, access_key_id, secret_access_key}  # bucket+region required when type: s3;
                                          # omit keys → adapter falls back to AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY
     gcs:
       bucket: ...                        # required when type: gcs
       endpoint: https://storage.googleapis.com
       auth: metadata                     # metadata | token | none
       token: "${GCS_TOKEN}"              # required iff auth: token
   claim_check:
     port: 4001
     max_bytes: 8000000
     retention_days: null
     tokens:                              # OPTIONAL. Omitted/empty → api_tokens: %{} → open gateway (bring your own auth)
       - token: "${CLAIM_CHECK_TOKEN}"    # → api_tokens: %{token => :all | [tenant...]}
         tenants: all                     # "all" | [list]
     remote: {url, token}                 # present → adapter {Ankusa.ClaimCheck.Remote, url:, token:}; token optional
                                          # (omitted → no :token opt); absent block → Direct
   sources:
     <source_id>:
       tenant: default                    # → tenant_id:
       on_verify_failure: reject          # reject|quarantine|accept_flag
       verify: {type: none}               # none | stripe | github | standard_webhooks; secret required for the last three;
                                          # tolerance_seconds (stripe, standard_webhooks) → tolerance:
       dedup: {type: rules}               # rules | stripe | github; rules takes header: "<name>", json_path: "data.id" (split on ".")
       sinks:                             # required, non-empty list
         - {type: log}
         - {type: http, url, method: post, headers: {K: V}, timeout_ms: 5000}
         - {type: rabbitmq, url, exchange, exchange_type: topic, routing_key, inline_max_bytes: 8192}
         - {type: kafka, brokers: [h:p], topic, key, inline_max_bytes: 8192,
            ssl: false, sasl: {mechanism: plain|scram_sha_256|scram_sha_512, username, password}}
   ```

   Translation details the implementer must not improvise:
   - Module mapping for sources:
     - Verifiers: `Ankusa.Verifier.{None, Stripe, GitHub, StandardWebhooks}`.
     - Dedup: `Ankusa.DedupKey.{Rules, Stripe, GitHub}`.
     - Sinks: `Ankusa.Sink.{Log, Http, RabbitMQ, Kafka}`.
   - HTTP sink: `method` becomes the atom `:post | :put | :patch`, and `headers` becomes `[{k, v}]`.
   - Kafka sink:
     - `sasl` becomes `{mechanism_atom, username, password}`, passed through as `:sasl` (the Kafka sink forwards `:ssl`/`:sasl` to brod verbatim; `ankusa_kafka/lib/ankusa/sink/kafka.ex:126-134`).
     - `ssl: true` passes `ssl: true`.
     - `key`/`routing_key` accept static strings only.
   - GCS `auth: metadata` sets `token_provider: {AnkusaServer.GcsToken, :metadata, []}`, and `auth: token` sets `{AnkusaServer.GcsToken, :static, [token]}`. New `lib/ankusa_server/gcs_token.ex`:
     - `static/1` returns `{:ok, token}`.
     - `metadata/0` GETs `http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token` with header `Metadata-Flavor: Google` via `Req`. It caches `{token, expires_at}` in `:persistent_term` under `{AnkusaServer.GcsToken, :token}` and refreshes when fewer than 60 s remain. It returns `:error` on any failure.
   - `sources` absent means `%{}`, with the warning `"no sources configured; every POST will return 404"`.
   - The server config has no auth settings except the optional `claim_check.tokens`/`remote.token` above. An unknown key such as `admin.tokens` fails with the standard unknown-key error.
5. Build with `Ankusa.Config.new(kw)` and call `Ankusa.ClaimCheck.validate_config!/1`, rescuing `ArgumentError` into `ConfigError` so role/claim-check misconfiguration fails at load time, in `check-config` too.

New `lib/ankusa_server/cli.ex`, `AnkusaServer.CLI`, run through `bin/ankusa eval`. Each function first calls `Application.ensure_all_started(:yaml_elixir)` and loads config through `load!/1`; each halts via `System.halt/1`:
- `check_config/0` prints `config OK: roles=... sources=... wal=... storage=...` and exits 0, or prints the error and exits 78.
- `print_config/0` prints `JSON.encode!(Ankusa.Admin.Redact.config(config))` and exits 0 (78 on error).
- `version/0` prints `Application.spec(:ankusa_server, :vsn)` and the core version.

### 7. Shipped configs and loader tests

- `ankusa_server/rel/ankusa.yml` is the default baked into the image. It contains `admin: {enabled: true}` and one `demo` source (`verify: {type: none}`, `on_verify_failure: accept_flag`, `sinks: [{type: log}]`), mirroring the root `config/config.exs` demo. A header comment points at `config-examples/reference.yml` and says in one line that ports 4001/4002 are unauthenticated and must not be published publicly.
- `ankusa_server/config-examples/` holds real, loadable files. Each starts with a comment block naming the env vars it expects and the topology it implements:
  - `reference.yml`: every key above, commented, with its default.
  - `single-node.yml`: disk WAL, local storage, Stripe and GitHub sources with an HTTP sink.
  - `fleet-postgres-s3.yml`: `wal.type: postgres` via `${ANKUSA_WAL_POSTGRES_URL}` and S3 storage. Meant to run as edge nodes with `ANKUSA_ROLES=edge` plus one `dispatch,storage` node. It includes the same `demo` source as `rel/ankusa.yml` (log sink), so the fleet compose in step 8 can be exercised with no provider credentials.
  - `kafka-fanout.yml` and `rabbitmq-fanout.yml`: queue sinks plus a claim-check block.
  - `multi-tenant.yml`: `routing: tenant_path`.
- `ankusa_server/test/ankusa_server/config_test.exs`, all through `load!(path: ..., env: %{...})` with temp files:
  - Every file in `config-examples/` plus `rel/ankusa.yml` loads with a fixture env map that supplies all its `${VAR}`s, and yields an `%Ankusa.Config{}`.
  - A missing `${STRIPE_WHSEC}` raises with message `sources.stripe.verify.secret: ${STRIPE_WHSEC} is not set`, and `${X:-fallback}` resolves to `fallback`.
  - Precedence: `http.port: 5000` in YAML is overridden by `PORT=6000`, which is overridden by `ANKUSA_HTTP_PORT=7000`, giving 7000.
  - An unknown key `sources.a.sinks[0].urll` raises and the message contains that path.
  - `verify: {type: strip}` raises the enum message listing the valid types.
  - Postgres `url` parses to hostname/port/username/password/database, and `url` plus `host` together raises.
  - `print_config` output (call the redaction on a loaded config) does not contain the fixture secret values.

### 8. Docker image

- `ankusa_server/Dockerfile`. The build context is the repo root, the same as `examples/oban-consumer/ingest_app/Dockerfile`, so the path deps resolve.
  ```dockerfile
  # syntax=docker/dockerfile:1.7
  FROM elixir:1.20.4-alpine AS build
  RUN apk add --no-cache build-base cmake git && mix local.hex --force && mix local.rebar --force
  ENV MIX_ENV=prod
  WORKDIR /repo
  COPY mix.exs mix.lock ./
  COPY lib ./lib
  COPY config ./config
  COPY priv ./priv
  COPY ankusa_postgres ./ankusa_postgres
  COPY ankusa_rabbitmq ./ankusa_rabbitmq
  COPY ankusa_kafka ./ankusa_kafka
  COPY ankusa_server ./ankusa_server
  WORKDIR /repo/ankusa_server
  RUN mix deps.get --only prod && mix release ankusa

  FROM alpine:3.24
  RUN apk add --no-cache libstdc++ libgcc ncurses-libs openssl ca-certificates tini curl \
   && addgroup -S ankusa && adduser -S -G ankusa -h /var/lib/ankusa ankusa \
   && mkdir -p /var/lib/ankusa /etc/ankusa && chown ankusa:ankusa /var/lib/ankusa
  COPY --from=build --chown=ankusa:ankusa /repo/ankusa_server/_build/prod/rel/ankusa /opt/ankusa
  COPY ankusa_server/rel/ankusa.yml /etc/ankusa/ankusa.yml
  COPY --chmod=0755 ankusa_server/rel/docker-entrypoint.sh /usr/local/bin/docker-entrypoint
  USER ankusa
  ENV ANKUSA_CONFIG=/etc/ankusa/ankusa.yml ANKUSA_DATA_DIR=/var/lib/ankusa \
      RELEASE_DISTRIBUTION=none LANG=C.UTF-8 HOME=/var/lib/ankusa
  VOLUME /var/lib/ankusa
  EXPOSE 4000 4001 4002
  HEALTHCHECK --interval=10s --timeout=3s --start-period=15s \
    CMD curl -fsS "http://127.0.0.1:${ANKUSA_ADMIN_PORT:-4002}/health" || exit 1
  ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/docker-entrypoint"]
  CMD ["start"]
  ```
  `alpine:3.24` matches the `elixir:1.20.4-alpine` base (`/etc/alpine-release` = 3.24.2, checked). If the release fails to start with a shared-library error, switch the runtime stage to `FROM elixir:1.20.4-alpine` (what the oban example uses) rather than chasing libraries.
- `ankusa_server/rel/docker-entrypoint.sh` (POSIX sh, `set -e`):
  - `start` → `exec /opt/ankusa/bin/ankusa start`
  - `check-config` → `exec /opt/ankusa/bin/ankusa eval 'AnkusaServer.CLI.check_config()'`
  - `print-config` → `exec /opt/ankusa/bin/ankusa eval 'AnkusaServer.CLI.print_config()'`
  - `version` → `exec /opt/ankusa/bin/ankusa eval 'AnkusaServer.CLI.version()'`
  - anything else → `exec "$@"`
- The root `.dockerignore` already excludes `**/_build` and `**/deps`. Add `ankusa_server/config-examples/` only if it bloats the context; leave it otherwise.
- `ankusa_server/scripts/smoke.sh IMAGE` (bash) is used by both mise and CI. It:
  1. Runs `docker run -d --name ankusa-smoke -p 127.0.0.1:4000:4000 -p 127.0.0.1:4002:4002 IMAGE` and waits up to 30 s for `GET :4002/health` to return 200.
  2. `POST :4000/webhooks/demo` with `{"id":"evt_smoke"}` must return 201 with `"status":"accepted"`; the same request again must return 200 with `"status":"duplicate"`.
  3. `GET :4002/metrics` must contain `ankusa_ingest_requests_total`.
  4. `GET :4002/v1/config` with no `authorization` header must return 200, and its body must contain `"demo"`.
  5. `docker run --rm -e ANKUSA_CONFIG=/nope IMAGE check-config` must exit 78.
  6. Always runs `docker rm -f ankusa-smoke` on exit, and prints container logs on failure.
- `ankusa_server/compose/docker-compose.yml`: single node, the image, a named volume on `/var/lib/ankusa`, ports `"4000:4000"` and `"127.0.0.1:4002:4002"`. The admin port is bound to loopback only, with a comment explaining why.
- `ankusa_server/compose/docker-compose.fleet.yml` is the reference for bringing your own auth:
  - `postgres:16-alpine`.
  - `edge` running `ANKUSA_ROLES=edge` with `deploy.replicas: 2`.
  - `worker` running `ANKUSA_ROLES=dispatch,storage`.
  - None of the Ankusa services publish ports. Only `nginx:alpine` does, using `compose/nginx.conf`, which has two `server` blocks:
    - `listen 4000`: round-robins to `edge:4000`, with no auth (providers authenticate through signatures).
    - `listen 4002`: proxies to `worker:4002` behind `auth_basic "ankusa admin"` with `auth_basic_user_file /etc/nginx/htpasswd`.
  - `compose/htpasswd` holds one line for user `admin` with password `change-me`. Generate it with `docker run --rm httpd:2.4-alpine htpasswd -nbB admin change-me` and commit the output. A comment in the compose file says to replace it.
  - Both Ankusa services mount `../config-examples/fleet-postgres-s3.yml`, with `ANKUSA_STORAGE_TYPE=local` so the demo needs no S3.
  - `ANKUSA_WAL_POSTGRES_URL=postgres://ankusa:ankusa@postgres:5432/ankusa`.

### 9. mise tasks (`.mise.toml`)

- `deps`: add `ankusa_server` to the directory loop.
- `check:server`: runs in a container because `crc32cer` needs cmake. Copy `check:kafka`'s `docker run` form with `-w /repo/ankusa_server`, `MIX_BUILD_PATH=/tmp/build-server`, and no network. Commands: `deps.get --check-locked`, `deps.unlock --check-unused`, `format --check-formatted`, `compile --warnings-as-errors`, `mix test`.
- Add `mise run check:server` to the `check` aggregate.
- `docker:build`: `docker build -f ankusa_server/Dockerfile -t ankusa/ankusa:dev .` from the repo root.
- `docker:smoke`: depends on `docker:build`; runs `ankusa_server/scripts/smoke.sh ankusa/ankusa:dev`.
- `release:preflight-server`: depends on `release:guard`, following `release:preflight-core`'s shape:
  - Read `@version` from `ankusa_server/mix.exs`.
  - `TAG=ankusa_server-v$VERSION`.
  - Require `^## \[$VERSION\]` in `ankusa_server/CHANGELOG.md`.
  - The tag must not exist locally or on origin.
  - `gh secret list` must show `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN`.
  - `curl https://hub.docker.com/v2/namespaces/ankusa/repositories/ankusa/tags/$VERSION` must return 404, else fail with "ankusa/ankusa:$VERSION already on Docker Hub".
- `release:tag-server`: depends on `release:preflight-server`; tags and pushes `ankusa_server-v$VERSION`, then prints `mise run release:watch-server`.
- `release:watch-server`: the same as `release:watch` with `--workflow=docker.yml`.
- `release:verify-server`: checks that the tag endpoint above returns 200 for the current version and for `latest`.

### 10. GitHub Actions

- `.github/workflows/ci.yml`: add an `ankusa_server` job modeled on the `ankusa_kafka` job (runner cmake handles `crc32cer`) with `working-directory: ankusa_server`. Steps: `deps.get --check-locked`, `deps.unlock --check-unused`, `format --check-formatted`, `compile --warnings-as-errors`, `mix test`. No services are needed.
- New `.github/workflows/docker.yml`:
  - Triggers:
    - `push` of tags `ankusa_server-v*`.
    - `push` to `main`, and `pull_request`, both with `paths` filter `lib/**`, `priv/**`, `mix.*`, `ankusa_*/**`, and `.github/workflows/docker.yml`.
  - `permissions: contents: write`.
  - `concurrency: docker-${{ github.ref }}`.
  - `env: IMAGE: ankusa/ankusa`.
  - Job `resolve` outputs `version` and `tags`:
    - **Tag run:** `VERSION=${GITHUB_REF_NAME#ankusa_server-v}`. It must equal the `@version` in `ankusa_server/mix.exs` and have a CHANGELOG heading (same checks as `release.yml`'s resolve).
    - **Tag list for `X.Y.Z` with no `-`:** `X.Y.Z`, `X.Y`, `latest`, plus `X` only when `X >= 1`.
    - **Tag list for a prerelease (contains `-`):** only `X.Y.Z-pre`.
    - **Push to `main`:** `edge` and `sha-<first 7 of GITHUB_SHA>`.
    - **PR:** empty, with `push=false`.
  - On tag runs only, job `ci` does `uses: ./.github/workflows/ci.yml` (the same reuse as `release.yml`).
  - Job `build` is a matrix of `{platform: linux/amd64, runner: ubuntu-latest}` and `{platform: linux/arm64, runner: ubuntu-24.04-arm}`, native builds with no QEMU. It `needs` `resolve` (and `ci` when on a tag). Steps:
    1. `docker/setup-buildx-action@v3`.
    2. `docker/build-push-action@v6` with `load: true`, `tags: ankusa/ankusa:test`, `cache-from/to: type=gha,scope=${{ matrix.platform }}`, `file: ankusa_server/Dockerfile`, `context: .`.
    3. `ankusa_server/scripts/smoke.sh ankusa/ankusa:test`.
    4. When `push`: `docker/login-action@v3` with `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN`, then a second build-push with `outputs: type=image,name=ankusa/ankusa,push-by-digest=true,name-canonical=true,push=true`.
    5. Write the digest to a file and `actions/upload-artifact@v4` it as `digest-<arch>`.
  - Job `merge` (push runs only; `needs: [resolve, build]`) logs in, downloads the digests, and runs `docker buildx imagetools create $(for t in $TAGS; do printf -- '-t ankusa/ankusa:%s ' "$t"; done) $(printf 'ankusa/ankusa@sha256:%s ' *)`, then `docker buildx imagetools inspect ankusa/ankusa:<first tag>`.
  - Job `publish-meta` (tag runs only; `needs: merge`):
    - Runs `peter-evans/dockerhub-description@v4` with `repository: ankusa/ankusa` and `readme-filepath: ankusa_server/README.md`.
    - Creates the GitHub release with the same `awk` CHANGELOG extraction as `release.yml`, run in `ankusa_server/`.
- Leave `release.yml` alone. Its tag filter already excludes `ankusa_server-v*`.

### 11. `ankusa_server/README.md`

This is the Docker Hub description (pushed in step 10), so it is part of the interface:
- Quick start: `docker run -p 4000:4000 -p 127.0.0.1:4002:4002 ankusa/ankusa`, then the curl to `/webhooks/demo`.
- Mounting a config: `-v ./ankusa.yml:/etc/ankusa/ankusa.yml`.
- `check-config` and `print-config`.
- The env override table from step 6.
- Ports: 4000 ingest, 4001 claim check (`claim_check` role), 4002 admin/metrics.
- A "Security" section, a few sentences in the product voice:
  - Ankusa verifies provider signatures on ingest and does no other authentication.
  - Publish only port 4000.
  - Put 4002 (admin/metrics) and 4001 (claim check) behind your own proxy, SSO, or network policy.
  - `compose/docker-compose.fleet.yml` shows nginx basic auth in front of the admin API.
  - Claim-check bearer tokens are optional, for setups that want them.
- The data volume `/var/lib/ankusa`.
- Links to `config-examples/`, the compose files, and the OpenAPI specs.
- Image tags: `X.Y.Z`, `X.Y`, `X`, `latest`, `edge`.

Keep it in the same voice as the root README.

## Critical files & anchors

- `lib/ankusa/config.ex`: `defstruct` sections and the `new/1` section list (lines ~22–52, ~110). The new `admin` section must deep-merge like `claim_check`.
- `lib/ankusa/instance.ex`: `init/1` child list and `claim_check_children/2` (~lines 37–86). This is the pattern for the admin Bandit listener and the metrics reporter.
- `lib/ankusa/claim_check/router.ex`: `authenticate/2` (~118–128), which gets the empty-tokens clause, and the router shape `Ankusa.Admin.Router` copies (without its auth). `lib/ankusa/claim_check.ex` `validate_config!/1` (~135–151) loses the empty-tokens raise.
- `examples/oban-consumer/ingest_app/Dockerfile`: the working multi-stage `mix release` built from the repo root with path deps and `override: true`.
- `.mise.toml`: the `check:kafka` container invocation (~116–140) and `release:preflight-core`/`release:tag-core` (~283–376), templates for `check:server` and the server release tasks.

## Verification

1. Core, from the repo root: `mix format --check-formatted && mix compile --warnings-as-errors && mix test`.
   - The new `test/ankusa/admin/router_test.exs` and `test/ankusa/metrics_test.exs` pass.
   - In `claim_check/router_test.exs`, the new no-token test gets 201 without an `authorization` header and the existing token tests still return 401. The rewritten `claim_check_test.exs` validation test passes.
   - Then `mise run infra:up`, and run the same three commands in `ankusa_postgres/` and `ankusa_rabbitmq/`, plus `mise run check:kafka`, `mise run check:examples`, `mise run check:oban` (lockfiles regenerated in step 4).
2. Server: `mise run check:server`. `config_test.exs` passes, including the precedence case (`ANKUSA_HTTP_PORT=7000` wins) and the missing-var message.
3. Image, from the repo root:
   - `mise run docker:smoke` builds `ankusa/ankusa:dev` and passes every assertion in `smoke.sh`: 201 then 200 duplicate, metrics present, `/v1/config` returns 200 with no credentials, and `check-config` exits 78.
   - Manually, `docker run --rm -e STRIPE_WHSEC=whsec_x -v $PWD/ankusa_server/config-examples/single-node.yml:/etc/ankusa/ankusa.yml ankusa/ankusa:dev print-config` prints JSON in which `whsec_x` does not appear.
   - `docker image inspect ankusa/ankusa:dev --format '{{.Config.User}}'` prints `ankusa`.
4. Fleet topology: `docker compose -f ankusa_server/compose/docker-compose.fleet.yml up -d --wait`. Then:
   - `curl -XPOST localhost:4000/webhooks/demo -d '{"id":"f1"}'` returns 201.
   - Repeating it returns 200 `duplicate`, which proves dedup is shared across the two edge replicas via Postgres.
   - `curl -s localhost:4002/v1/dlq` returns 401 from nginx (no credentials).
   - `curl -s -u admin:change-me localhost:4002/v1/dlq` returns `{"total":0,...}` from the worker.
   - `docker compose -f ankusa_server/compose/docker-compose.fleet.yml exec edge curl -s localhost:4002/v1/dlq` returns `{"error":"role_not_enabled","role":"dispatch"}`. It goes straight to an edge node, bypassing nginx.
   - Finish with `docker compose ... down -v`.
5. Workflows: `mise run lint:workflows` passes, and a PR run of `docker.yml` builds and smoke-tests both architectures without pushing. The first real push is `mise run release:tag-server` after `release:preflight-server` passes. `mise run release:verify-server` returns 200 for `0.1.0` and `latest`, and `docker buildx imagetools inspect ankusa/ankusa:0.1.0` lists `linux/amd64` and `linux/arm64`.

## Assumptions & contingencies

- The Docker Hub org `ankusa` exists and repo secrets `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN` are set before step 10's first push. Preflight enforces this. Until then, PR runs still build and smoke-test.
- If `ubuntu-24.04-arm` runners are unavailable for the repo (private repo on a plan without them), build arm64 on `ubuntu-latest` with `docker/setup-qemu-action@v3` and `platforms: linux/arm64` in that matrix leg. Nothing else changes.
- Workload-identity credentials (AWS IRSA/instance profiles) are not supported by `BlobStore.S3` today. The image supports static keys and `AWS_*` env vars only; do not add credential-chain support in this change.
- The DLQ and quarantine endpoints are node-local by design: the DLQ file lives on the `dispatch` node's disk and quarantine's recent list lives in the edge node's memory. The 409 response tells an operator they hit the wrong node. Aggregating across nodes is out of this change.
