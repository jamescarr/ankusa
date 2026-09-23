# Deployment

See [`architecture.md#deployment-topologies`](architecture.md#deployment-topologies)
for the four shapes this section explains how to actually run.

## Roles and topologies

`Ankusa.Instance.init/1` starts children conditionally on `config.roles`:

```elixir
defp edge_children(config, opts), do: if Config.role?(config, :edge), do: [...], else: []
defp dispatch_children(config, opts), do: if Config.role?(config, :dispatch), do: [...], else: []
defp storage_children(config, opts), do: if Config.role?(config, :storage), do: [...], else: []
defp claim_check_children(config, opts), do: if Config.role?(config, :claim_check), do: [...], else: []
```

One Mix release, many deployments — the same compiled artifact runs
all-in-one on a laptop or as split fleets, because *which* children start is
a runtime config decision, never a build-time one.

```sh
ANKUSA_ROLES=edge,dispatch mix run --no-halt    # this node: edge + dispatch, no compactor
ANKUSA_ROLES=storage mix run --no-halt          # this node: compactor only
ANKUSA_ROLES=claim_check mix run --no-halt      # this node: claim-check gateway only
```

`Ankusa.Application` reads `ANKUSA_ROLES` (comma-separated) and `PORT` on top of
whatever `config.exs` sets — see
[`configuration.md#runtime-environment-overrides`](configuration.md#runtime-environment-overrides).

**`:claim_check` is a fourth, opt-in role**, absent from the default
`roles` list (`[:edge, :dispatch, :storage]`) because it opens an
authenticated port. A node running it alone needs no WAL — only blob-store
credentials and `claim_check.api_tokens` — and can be scaled independently
from ingest/dispatch/storage exactly like any other role. See
[`claim-check.md`](claim-check.md) for the full contract and the worked
`examples/rabbitmq-consumer/` deployment (an `ingest` service plus a
separate `claim-check` service, same image, different `ANKUSA_ROLES`).

**Important constraint:** splitting roles across different *processes on
one host* works with any WAL, because they can share a local disk path.
Splitting roles across different *machines* requires a WAL every role can
reach over the network — that's `WAL.Postgres` (see
[`storage.md`](storage.md)), not `WAL.DiskLog`. This constraint doesn't
apply to `:claim_check`, which never touches the WAL at all.

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
pattern: a tiny Mix project that depends on `ankusa` (+ whichever adapter
packages it needs), reads its own env vars, and calls `Ankusa.Config.new/1` +
`Ankusa.Instance.start_link/1` directly. `ankusa` core stays a library; the
wrapper is where "how do I actually deploy this" config lives.

### Mono-repo Docker builds (path deps)

Because `ankusa_postgres`/`ankusa_rabbitmq` are path-dependencies during local
development, a Dockerfile building an app that depends on them needs a
build **context** wide enough to see the whole slice, with the relative
paths preserved so the same `mix.exs` files resolve identically inside the
container as they do on disk:

```dockerfile
# examples/rabbitmq-consumer/ingest_app/Dockerfile
WORKDIR /repo
COPY mix.exs mix.lock ./          # ankusa core
COPY lib ./lib
COPY config ./config
COPY ankusa_rabbitmq ./ankusa_rabbitmq
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

**Depending on `ankusa` directly *and* transitively through an adapter
package needs `override: true`.** `ankusa_postgres`/`ankusa_rabbitmq`'s own
`mix.exs` picks its Hex entry for `:ankusa` (`~> 0.1`) whenever Mix
evaluates it as a nested dependency — Mix builds dependencies under `:prod`
by default regardless of *your* project's `Mix.env()`, so the adapter
package's dev/test-only path-dep branch never gets hit there. A wrapper app
like `ingest_app` that depends on both `ankusa` (path) and
`ankusa_rabbitmq` (path, which transitively wants `ankusa` from Hex) hits a
real conflict — `mix deps.get` refuses with "the dependency ankusa in
mix.exs is overriding a child dependency." Fix: mark your direct entry
`override: true` so Mix uses it everywhere in the tree:

```elixir
defp deps do
  [
    {:ankusa, path: "../../..", override: true},
    {:ankusa_rabbitmq, path: "../../../ankusa_rabbitmq"}
  ]
end
```

## Scaling the ingest fleet

```sh
docker compose up --build --scale ingest=3
```

Three independent ingest containers, each with its own local WAL (if using
`DiskLog`) or sharing one Postgres WAL (if configured with `WAL.Postgres`),
all publishing to the same RabbitMQ exchange and writing to the same
bucket. Nothing about `Ankusa.Sink.RabbitMQ` or `Ankusa.BlobStore.S3` changes —
this is the "durable state, not RPC" rule holding at the fleet level exactly
like it holds between the edge/dispatch/storage roles inside one instance.
You'd need a load balancer in front of the ingest port at that point; that's
a deployment concern the framework doesn't solve for you (nothing in
`ankusa`'s job description is "be a load balancer").

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

## Releasing

Three independently versioned Hex packages (`ankusa`, `ankusa_postgres`,
`ankusa_rabbitmq`), each with its own `mix.exs` `version` and
`CHANGELOG.md`. The release flow is the standard Elixir/Hex one — no bot,
no separate changeset files, no tag to remember to push:

1. In your PR, bump `version` in the package's `mix.exs` (follow
   [SemVer](https://semver.org/)) and move the relevant entries from that
   package's `CHANGELOG.md` `[Unreleased]` section under a new dated
   heading.
2. Merge to `main`. [`.github/workflows/release.yml`](../.github/workflows/release.yml)
   runs each package's tests, then checks Hex for that exact version; if
   it's not there yet, publishes it (`mix hex.publish --yes`). A package
   whose version didn't change in that push is a no-op — nothing publishes
   twice.
3. `ankusa_postgres` and `ankusa_rabbitmq` publish only after `ankusa`
   (core) does, since their published package declares a real Hex
   dependency on it (`{:ankusa, "~> 0.1"}`) — not the path dependency local
   development uses. See their `mix.exs` for why a plain `{:ankusa, path:
   "..", only: [:dev, :test]}` alongside a hex entry doesn't work (Mix
   rejects duplicate entries for the same app regardless of `:only`); the
   working pattern is a single `Mix.env()`-conditional entry.

Requires a `HEX_API_KEY` repository secret — generate one from the Hex.pm
dashboard (Keys → scoped to `api:write`, ideally limited to these package
names) and add it under repo Settings → Secrets and variables → Actions.

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs the same
tests (plus `mix format --check-formatted` and
`mix compile --warnings-as-errors`) on every PR and push, independent of
the release workflow — a red CI check is a merge blocker regardless of
whether anything's being released.
