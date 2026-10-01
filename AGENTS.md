# Agent notes

Ankusa is a self-hosted webhook receiver (Elixir/OTP): write every hook to a
durable WAL before answering `2xx`, then dispatch it to HTTP/RabbitMQ/Kafka/NATS
sinks with retries, DLQ, and replay. Full pipeline and guarantees:
[`docs/architecture.md`](docs/architecture.md).

```mermaid
flowchart LR
    P[Provider] --> E[Edge: Router/Ingest]
    E --> B[Batcher] --> W[(WAL)]
    W --> C[Compactor] --> S[(Object store)]
    W --> D[Dispatch] --> SK[Sinks/DLQ]
```

## Repo map

| Path | What |
| --- | --- |
| `packages/ankusa` | Core Mix project: edge/WAL/storage/dispatch machinery + every zero-external-dep default adapter. No adapter deps (`bandit`, `plug`, `cidr`, `req`, `aws_signature`, `telemetry_metrics`/`telemetry_metrics_prometheus_core`, `nebulex`/`nebulex_local` only). |
| `packages/ankusa_rabbitmq`, `ankusa_kafka`, `ankusa_nats` | One sink adapter each (`Sink.RabbitMQ`/`Kafka`/`NATS`), path-depend on `ankusa` + one broker client (`amqp`/`brod`/`gnat`). Own `docker-compose.yml` for local broker infra. |
| `packages/ankusa_redis` | Redis adapters: the route store (`Ankusa.Routes.Store.Redis` — definitions in Redis, shared by every edge node) and the pub/sub sink (`Ankusa.Sink.Redis`). Path-depends on `ankusa` + one client (`redix`). Own `docker-compose.yml` (Redis on `:6399`). |
| `conformance/` | Language-neutral SDK vectors (`features.json`, `cases/*.json`) and the checker (`check.mjs`) every `packages/sdk-*` must pass; `mise run check:conformance`. |
| `packages/ankusa_server` | The `jamescarr/ankusa` Docker image: core + every adapter, driven entirely by YAML (`config.ex` is the loader). Not published to Hex. |
| `packages/sdk-typescript`, `sdk-python`, `sdk-rust`, `sdk-ruby` | Published client SDKs (npm `ankusa`, PyPI `ankusa`, crates.io `ankusa`, RubyGems `ankusa-sdk`) for writing worker consumers. |
| `examples/*` | Runnable Docker-composed demos, one per delivery transport; see [`examples/README.md`](examples/README.md). |
| `tools/loadgen` | Load generator used by the `oban-consumer` example. |
| `docs/*` | Prose docs, see table below. Index: [`docs/README.md`](docs/README.md). |
| `.mise/tasks/*` | Every CI/dev task, one file each (see Commands below). |

Why the package split (and when a new adapter earns its own package):
[`docs/packaging.md`](docs/packaging.md).

### `packages/ankusa/lib/ankusa` — core module map

| Stage | Modules |
| --- | --- |
| Edge (ingress) | `edge/router.ex`, `edge/ingest.ex`, `edge/batcher.ex` + `batcher_supervisor.ex`, `edge/quarantine.ex`, `edge/route_guard.ex`, `route.ex`, `route_resolver.ex`, `verifier.ex` + `verifier/{hmac,none,schemes}.ex` |
| WAL | `wal.ex`, `wal/disk_log.ex`, `durable_log.ex` |
| Storage (compaction + blobs) | `storage.ex`, `storage/compactor.ex`, `storage/index.ex`, `blob_store.ex`, `blob_store/{local_fs,s3,gcs,azure,oci}.ex` |
| Dispatch (sinks, retries, DLQ) | `dispatch.ex`, `dispatch/pipeline.ex`, `dispatch/dlq.ex`, `sink.ex`, `sink/{log,http,message}.ex`, `retry_policy.ex`, `retry_policy/exponential.ex` |
| Claim check (large payloads) | `claim_check.ex`, `claim_check/{pack,ref,router,sweeper}.ex` |
| Route management | `routes.ex`, `routes/{route,matcher,snapshot,cache,router,store}.ex`, `routes/store/ets.ex`, `net.ex`, `net/client_ip.ex` |
| Ops / cross-cutting | `application.ex`, `config.ex`, `instance.ex`, `source.ex`, `source_store.ex`, `envelope.ex`, `codec.ex` + `codec/raw.ex`, `admin/router.ex`, `admin/redact.ex`, `telemetry.ex`, `metrics.ex`, `http.ex`, `http_client.ex`, `ulid.ex`, `uuid_v7.ex` |

`packages/ankusa_server/lib/ankusa_server`: `application.ex`, `cli.ex`,
`config.ex` (YAML → core config), `config_error.ex`, `gcs_token.ex`.

### Docs index

| Doc | Read it when |
| --- | --- |
| [`quickstart.md`](docs/quickstart.md) | run it with a worker, break the worker, replay |
| [`configuration.md`](docs/configuration.md) | every YAML key, and the Elixir config |
| [`deployment.md`](docs/deployment.md) | roles, the container, fleets |
| [`architecture.md`](docs/architecture.md) | the guarantees, the request path |
| [`delivery.md`](docs/delivery.md) | sinks, retries, DLQ, quarantine |
| [`storage.md`](docs/storage.md) | the log and object stores |
| [`claim-check.md`](docs/claim-check.md) | large payloads to queue workers |
| [`multi-tenancy.md`](docs/multi-tenancy.md) | catch URLs per customer |
| [`integrations.md`](docs/integrations.md) | Oban, Celery, queues |
| [`elixir.md`](docs/elixir.md) | embed the library in your own app |
| [`testing.md`](docs/testing.md) | running the suites, local infra, what each one covers |
| [`packaging.md`](docs/packaging.md) | why adapters are separate packages, and how to add one |
| [`releasing.md`](docs/releasing.md) | releasing any package: Hex, the server image, npm, PyPI, crates.io, or RubyGems |

Exact callback signatures and options: module docs on [HexDocs](https://hexdocs.pm/ankusa).

## Commands

`mise install` once for the pinned toolchain (`.mise.toml`). All tasks live in
`.mise/tasks/` (one file each); `mise tasks ls` lists every one; CI runs the
same tasks this table does.

| Task | What |
| --- | --- |
| `mise run check` | everything CI checks, every package/example/tool |
| `mise run check:package <pkg>` | one package: format, warnings-as-errors, tests, docs; starts/stops that package's own `docker-compose.yml` |
| `mise run check:examples` | `examples/*` |
| `mise run check:tools` | `tools/*` |
| `mise run check:conformance` | validate `conformance/` and run every `packages/sdk-*` against its vectors |
| `mise run test:integration` | `ankusa` core's object-store adapters vs. the floci emulators |
| `mise run e2e` | kind + Oban end-to-end gate (needs `docker`; `kind`/`kubectl` from `.mise.toml`) |
| `mise run format` | `mix format` across every package — run before pushing, not after CI complains |
| `mise run deps` | refresh every `mix.lock` after adding/removing a core dependency |
| `mise run new:adapter <name>` | scaffold `packages/ankusa_<name>/` |
| `mise run status` | every package's version, last tag, published or not |
| `mise run docker:smoke` | build `jamescarr/ankusa:dev`, run `packages/ankusa_server/scripts/smoke.sh` against it |

## Run the checks before calling work done

Format, warnings-as-errors, and tests — for **every** package the change
touches, not just the one you edited. Every adapter package and
`ankusa_server` path-depend on `ankusa` core, so a core change isn't finished
until they pass too.

| What changed | Run |
| --- | --- |
| a package under `packages/` | `mise run check:package <pkg>` for every touched package (all of them for a core change) |
| `examples/*` | `mise run check:examples` |
| `tools/*` | `mise run check:tools` |
| an SDK (`packages/sdk-*`) or `conformance/` | `mise run check:conformance` (also part of `mise run check`) |
| core's object-store adapters | `mise run test:integration` |
| anything, before tagging a release | `mise run e2e` |
| everything | `mise run check` |

Changing core's dependencies touches every package that path-depends on it, so
after adding or removing one, run `mise run deps` and commit every `mix.lock`
it changes. CI runs `mix deps.get --check-locked` and
`mix deps.unlock --check-unused` everywhere — including the examples — so a
`deps.get` that rewrites a stale lock, or a lock entry for a dependency nobody
declares, fails the build instead of passing quietly.

What each suite covers: [`docs/testing.md`](docs/testing.md).

Working on something that needs a browser or a running system? Exercise the
real thing (or a throwaway script against it) — see the verification
requirements in the project brief.

Two rules that follow from all of the above:

- **Never report a check as passing unless it ran in this session.** "It
  should work", "the code looks right", and a hand-written log line are not
  evidence.
- **Never mark a task done while its own acceptance criteria fail** — a
  step that crashed, or a drill that was never run, is not done. Say what
  failed and what's still open.

## Dependencies

**Take the dependency when it is the right tool.** Judge it on merit: is the
library more correct, better tested, and less surface for us to own and be wrong
about than the code it replaces? Then use it. "Zero dependencies" is not a goal,
and refusing a good library to keep a count at zero is not a principle — it is
how you end up maintaining hand-rolled SigV4 signing.

The rule that *does* hold is about placement: a dependency only some deployments
need goes in its own adapter package, so nobody else's `mix deps.get` compiles
it. A dependency every user benefits from belongs in `ankusa` core. Full decision
rule, and the worked examples: [`docs/packaging.md`](docs/packaging.md).
