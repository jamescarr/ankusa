# Packaging: why this is a mono-repo of separate Mix projects

## The decision rule

Two questions, deliberately kept separate:

**1. Should we depend on this at all?** Judge it on merit: does the library do
the job better than the code we would otherwise own — more correct, better
tested, less surface for us to maintain and be wrong about? Then take it. *Zero
dependencies is not a goal here*; the goal is the least code we have to be right
about, and a well-maintained library replacing a hand-rolled protocol
implementation is usually the cheaper side of that trade.

**2. Where should it live?** That is the packaging question:

> **Split a package when, and only when, an adapter introduces an external
> dependency the default deployment shouldn't compile. Everything else
> stays in core. Role separation (edge/dispatch/storage) is runtime config,
> not packaging.**

A dependency every user benefits from belongs in `ankusa` core — no split, no
ceremony. A dependency only one adapter needs belongs in that adapter's package.

This isn't a stylistic preference — it's the direct consequence of the
project being library-first (embeds in an existing Phoenix/Bandit app) as
well as a deployable release. A library embedder pays for every dependency
their host app compiles, whether they use it or not. A laptop user running
`mix deps.get` on `ankusa` alone should never fetch `postgrex` or `amqp`
because they exist in the ecosystem, only because they were actually
configured.

## Layout

```
bandit_example/            ankusa — core. mix.exs deps: {bandit, plug, req,
                              aws_signature}. No adapter deps.
  lib/ankusa/…                behaviours, envelope, config, registry, telemetry,
                              edge/dispatch/storage machinery, and every
                              zero-external-dep default adapter
                              (WAL.DiskLog, BlobStore.{LocalFS,S3,GCS},
                              Codec.Raw, all Verifiers, DedupKey.*,
                              Sink.{Log,Http}, RetryPolicy.Exponential,
                              RouteResolver.{Path,TenantPath})
  ankusa_postgres/            path-dep on ankusa + postgrex. Ankusa.WAL.Postgres.
  ankusa_rabbitmq/            path-dep on ankusa + amqp. Ankusa.Sink.RabbitMQ.
  ankusa_kafka/               path-dep on ankusa + brod. Ankusa.Sink.Kafka.
  ankusa_nats/                path-dep on ankusa + gnat. Ankusa.Sink.NATS.
  examples/                 deployable demos; not published packages
```

`ankusa_postgres` and `ankusa_rabbitmq` each depend on `ankusa` via `{:ankusa, path:
".."}` for local development, and would become normal Hex dependencies once
published. Each ships its **own** `docker-compose.yml` for local dev/test
infra (`ankusa_postgres/` → Postgres on `:5433`; `ankusa_rabbitmq/` → RabbitMQ
on `:5673`/`:15673`; `ankusa_kafka/` → Redpanda on `:19092`; `ankusa_nats/` →
NATS with JetStream on `:4223`/`:8223`). Every adapter
package test suite needs `Ankusa.Registry` running (started by `ankusa`'s own
Application); none needs any config to get it, since Ankusa.Application's
built-in default instance is off (`autostart: false`) by default and only the
root project's own `config/config.exs` turns it on.

## Why S3/GCS stayed in-tree but Postgres/RabbitMQ/Kafka/NATS didn't

This is a dependency-weight split, not a position on hand-rolling.

`Ankusa.BlobStore.S3` and `Ankusa.BlobStore.GCS` are in core rather than in
their own packages because their dependencies are small and focused:
`aws_signature` (signing only) and `Req` (HTTP, with its own Finch pool).
Neither drags a credential stack or a framework along, so a `LocalFS` user's
`deps.get` stays cheap. GCS bundles nothing credential-shaped at all — it takes
a `:token_provider` callback and leaves token acquisition (Goth, ADC) to the
deployment.

### Resolved: the hand-rolled SigV4 signing is gone

`Ankusa.BlobStore.S3` used to hand-roll canonical-request / string-to-sign /
HMAC-chain code — ~80 lines, security-sensitive, and covered only by
`:integration`-tagged tests. That is precisely the profile where a focused
library wins, so it now calls `aws_signature` (the implementation behind the
official aws-elixir SDK) through the same `Req` client the other adapters use.
Core shrank by ~50 lines, and the part we would most regret getting subtly wrong
is no longer ours to get wrong.

Worth knowing when reading the tests: `floci` does **not** validate SigV4 — a
bogus signature, no signature at all, and a wrong-secret signature all return
`200` — so the integration suite could never have caught a signing bug.
`test/ankusa/blob_store_s3_signing_test.exs` does, from both ends: it
reproduces AWS's published reference signatures, and it reconstructs the
adapter's own signing call from a captured request to pin S3's "sign the path as
sent" rule.

`Ankusa.WAL.Postgres` needs `postgrex` (which pulls `db_connection`,
`decimal`). `Ankusa.Sink.RabbitMQ` needs `amqp` (which pulls `amqp_client`,
`rabbit_common` — real NIF/native-adjacent Erlang libraries).
`Ankusa.Sink.Kafka` needs `brod`, which pulls `crc32cer`: a C++ NIF that
compiles from source on every `mix deps.compile`, so that package carries a
build-toolchain requirement (CMake ≥ 3.16 plus a C++ compiler) that no
`ankusa` core user should be forced to satisfy. `Ankusa.Sink.NATS` needs
`gnat`, which pulls `jason`, `nkeys` (+ `ed25519`/`kcl`), `nimble_parsec`, and
`connection` — pure Elixir, but four libraries nobody running an HTTP-,
Kafka-, or RabbitMQ-only deployment has any use for, which is the same test
with a lighter dependency. Those are
genuine external dependencies the laptop/standalone user shouldn't pay to
compile, so each got its own package the moment it was built — not before.
`ankusa_postgres` didn't exist until the Postgres WAL adapter was actually
written; there was nothing to split prematurely.

## What deliberately did *not* get split

The plan document that predates this codebase proposed separate
applications per **role** (`hook_edge`, `hook_storage`, `hook_dispatch`,
`hook_dashboard`). That didn't happen, on purpose: role separation is a
config concern (`config.roles` / `ANKUSA_ROLES`), not a dependency-weight
concern. Splitting them into packages would buy package-management overhead
(version matrix, release coordination) for zero dependency-isolation
benefit — every role's code has the same (zero) external deps as core. One
release, many roles, config decides what boots. See
[`deployment.md`](deployment.md).

## Adding a new adapter package

1. Decide whether it needs a dependency at all, on merit (question 1 above). If a
   library is the right tool, take it — then put the adapter in its own package
   so deployments that don't configure it never compile it. If no dependency is
   warranted, the adapter belongs in `ankusa` core next to the dependency-free
   ones (`BlobStore.LocalFS`, `WAL.DiskLog`, `Codec.Raw`, the verifiers) — not
   because hand-rolling is preferred, but because a dependency that buys nothing
   is a liability.
2. Scaffold a sibling directory: `mix.exs` with `{:ankusa, path: ".."}` plus
   the real dependency; a `docker-compose.yml` if the adapter needs live infra
   to test against. No config is needed to keep `ankusa`'s built-in demo
   instance from booting — `autostart` defaults to `false`.
3. Implement the behaviour. Register any supervised process (a connection
   pool, a channel) through `Ankusa.Registry`/`Ankusa.via/2` exactly like the
   framework's own processes do — this is what lets the facade
   (`Ankusa.WAL.append/2`, `Ankusa.Sink`'s `deliver/3` call sites) dispatch to
   your adapter without `ankusa` core knowing your package exists.
4. **Verify against real infrastructure, not mocks.** Both `ankusa_postgres`
   and `ankusa_rabbitmq` are tested against real Postgres/RabbitMQ containers
   — a hand-rolled protocol implementation (SigV4 signing, a SQL dedup
   ledger, AMQP publisher confirms) that "looks right" is exactly the kind
   of thing that's subtly wrong until proven against the real thing. Both
   adapters in this repo caught genuine bugs this way during development
   (see `ankusa_postgres`'s moduledoc on why the dedup ledger carries its own
   `seq` instead of joining back to `ankusa_wal`).
5. Document it: a row in the behaviour table in
   [`configuration.md`](configuration.md), and a section in
   [`storage.md`](storage.md) or [`delivery.md`](delivery.md) depending on
   which behaviour it implements.

## Building an app against the path deps

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
