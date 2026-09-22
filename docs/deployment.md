# Deployment

See [`architecture.md#deployment-topologies`](architecture.md#deployment-topologies)
for the four shapes this section explains how to actually run.

## Roles and topologies

`Hook.Instance.init/1` starts children conditionally on `config.roles`:

```elixir
defp edge_children(config, opts), do: if Config.role?(config, :edge), do: [...], else: []
defp dispatch_children(config, opts), do: if Config.role?(config, :dispatch), do: [...], else: []
defp storage_children(config, opts), do: if Config.role?(config, :storage), do: [...], else: []
```

One Mix release, many deployments — the same compiled artifact runs
all-in-one on a laptop or as split fleets, because *which* children start is
a runtime config decision, never a build-time one.

```sh
HOOK_ROLES=edge,dispatch mix run --no-halt    # this node: edge + dispatch, no compactor
HOOK_ROLES=storage mix run --no-halt          # this node: compactor only
```

`Hook.Application` reads `HOOK_ROLES` (comma-separated) and `PORT` on top of
whatever `config.exs` sets — see
[`configuration.md#runtime-environment-overrides`](configuration.md#runtime-environment-overrides).

**Important constraint:** splitting roles across different *processes on
one host* works with any WAL, because they can share a local disk path.
Splitting roles across different *machines* requires a WAL every role can
reach over the network — that's `WAL.Postgres` (see
[`storage.md`](storage.md)), not `WAL.DiskLog`.

## Docker

There's no single canonical "the" Dockerfile shipped at the repo root —
deployment shape is a choice the operator makes, so the worked example
([`examples/rabbitmq-consumer/ingest_app/Dockerfile`](../examples/rabbitmq-consumer/ingest_app/Dockerfile))
shows the pattern rather than prescribing one image for every use case:

- **Dev-mode image** (what the example uses): `elixir:1.20.4-alpine`,
  `mix deps.get && mix compile`, `CMD ["mix", "run", "--no-halt"]`. Simpler,
  faster to build, right-sized for an example or a low-traffic deployment.
- **Release image** (what a real production deployment should build
  instead): multi-stage, `mix release` in the build stage, a slim runtime
  base in the final stage. Not shipped here — the framework doesn't
  prescribe a release config because that's genuinely deployment-specific
  (env-var vs. `runtime.exs`-based config, which roles per image, etc.).

Either way, a deployable wrapper app (like
[`ingest_app/`](../examples/rabbitmq-consumer/ingest_app)) is the intended
pattern: a tiny Mix project that depends on `hook` (+ whichever adapter
packages it needs), reads its own env vars, and calls `Hook.Config.new/1` +
`Hook.Instance.start_link/1` directly. `hook` core stays a library; the
wrapper is where "how do I actually deploy this" config lives.

### Mono-repo Docker builds (path deps)

Because `hook_postgres`/`hook_rabbitmq` are path-dependencies during local
development, a Dockerfile building an app that depends on them needs a
build **context** wide enough to see the whole slice, with the relative
paths preserved so the same `mix.exs` files resolve identically inside the
container as they do on disk:

```dockerfile
# examples/rabbitmq-consumer/ingest_app/Dockerfile
WORKDIR /repo
COPY mix.exs mix.lock ./          # hook core
COPY lib ./lib
COPY config ./config
COPY hook_rabbitmq ./hook_rabbitmq
COPY examples/rabbitmq-consumer/ingest_app ./examples/rabbitmq-consumer/ingest_app
WORKDIR /repo/examples/rabbitmq-consumer/ingest_app
RUN mix deps.get && mix compile
```

```yaml
# docker-compose.yml
services:
  ingest:
    build:
      context: ../..                                            # repo root
      dockerfile: examples/rabbitmq-consumer/ingest_app/Dockerfile
```

## Scaling the ingest fleet

```sh
docker compose up --build --scale ingest=3
```

Three independent ingest containers, each with its own local WAL (if using
`DiskLog`) or sharing one Postgres WAL (if configured with `WAL.Postgres`),
all publishing to the same RabbitMQ exchange and writing to the same
bucket. Nothing about `Hook.Sink.RabbitMQ` or `Hook.BlobStore.S3` changes —
this is the "durable state, not RPC" rule holding at the fleet level exactly
like it holds between the edge/dispatch/storage roles inside one instance.
You'd need a load balancer in front of the ingest port at that point; that's
a deployment concern the framework doesn't solve for you (nothing in
`hook`'s job description is "be a load balancer").

## The worked example

[`examples/rabbitmq-consumer/`](../examples/rabbitmq-consumer/) is the full
picture: dockerized ingest → RabbitMQ exchange (fat payloads offloaded to
S3, small ones inlined) → a real TypeScript consumer that owns its own
queue/binding, fetches, and prints. `docker compose up --build`, then:

```sh
curl -XPOST localhost:4000/hooks/demo -H 'content-type: application/json' -d '{"id":"evt_1"}'
docker compose logs -f worker
```

Its own README documents the architecture diagram, both the inline and
fat-payload code paths, and what's intentionally left as a stub (the
worker's own business logic — everything around it: topology declaration,
decode, blob fetch, ack/nack, is real working code).
