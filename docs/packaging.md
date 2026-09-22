# Packaging: why this is a mono-repo of separate Mix projects

## The decision rule

> **Split a package when, and only when, an adapter introduces an external
> dependency the default deployment shouldn't compile. Everything else
> stays in core. Role separation (edge/dispatch/storage) is runtime config,
> not packaging.**

This isn't a stylistic preference — it's the direct consequence of the
project being library-first (embeds in an existing Phoenix/Bandit app) as
well as a deployable release. A library embedder pays for every dependency
their host app compiles, whether they use it or not. A laptop user running
`mix deps.get` on `hook` alone should never fetch `postgrex` or `amqp`
because they exist in the ecosystem, only because they were actually
configured.

## Layout

```
bandit_example/            hook — core. mix.exs deps: {bandit, plug}. Zero adapter deps.
  lib/hook/…                 behaviours, envelope, config, registry, telemetry,
                              edge/dispatch/storage machinery, and every
                              zero-external-dep default adapter
                              (WAL.DiskLog, BlobStore.{LocalFS,S3,GCS},
                              Codec.Raw, all Verifiers, DedupKey.*,
                              Sink.{Log,Http}, RetryPolicy.Exponential,
                              RouteResolver.{Path,TenantPath})
  hook_postgres/            path-dep on hook + postgrex. Hook.WAL.Postgres.
  hook_rabbitmq/            path-dep on hook + amqp. Hook.Sink.RabbitMQ.
  examples/                 deployable demos; not published packages
```

`hook_postgres` and `hook_rabbitmq` each depend on `hook` via `{:hook, path:
".."}` for local development, and would become normal Hex dependencies once
published. Each ships its **own** `docker-compose.yml` for local dev/test
infra (`hook_postgres/` → Postgres on `:5433`; `hook_rabbitmq/` → RabbitMQ
on `:5673`/`:15673`) and its **own** `config/config.exs` setting `config
:hook, autostart: false` — every adapter package test suite needs
`Hook.Registry` running (started by `hook`'s own `Application`) but must not
let `hook`'s built-in demo instance boot as a side effect of `hook` being a
transitive OTP application dependency.

## Why S3/GCS stayed in-tree but Postgres/RabbitMQ didn't

`Hook.BlobStore.S3` and `Hook.BlobStore.GCS` needed real signing (AWS SigV4)
and a real HTTP client — but stdlib already provides both (`:crypto` for
HMAC, `:httpc`/`:inets` for HTTP, `:xmerl` for the one bit of XML parsing
S3's `ListObjectsV2` needs). Zero *external* dependency, so they stayed in
`hook` core alongside `BlobStore.LocalFS`.

`Hook.WAL.Postgres` needs `postgrex` (which pulls `db_connection`,
`decimal`). `Hook.Sink.RabbitMQ` needs `amqp` (which pulls `amqp_client`,
`rabbit_common` — real NIF/native-adjacent Erlang libraries). Those are
genuine external dependencies the laptop/standalone user shouldn't pay to
compile, so each got its own package the moment it was built — not before.
`hook_postgres` didn't exist until the Postgres WAL adapter was actually
written; there was nothing to split prematurely.

## What deliberately did *not* get split

The plan document that predates this codebase proposed separate
applications per **role** (`hook_edge`, `hook_storage`, `hook_dispatch`,
`hook_dashboard`). That didn't happen, on purpose: role separation is a
config concern (`config.roles` / `HOOK_ROLES`), not a dependency-weight
concern. Splitting them into packages would buy package-management overhead
(version matrix, release coordination) for zero dependency-isolation
benefit — every role's code has the same (zero) external deps as core. One
release, many roles, config decides what boots. See
[`deployment.md`](deployment.md).

## Adding a new adapter package

1. Confirm it actually needs an external dependency `hook` core shouldn't
   carry. If it doesn't (stdlib covers it), it belongs in `hook` core next
   to the existing zero-dep adapters — see `Hook.BlobStore.S3` for the
   pattern (hand-roll what stdlib can do rather than pull a dependency for
   convenience).
2. Scaffold a sibling directory: `mix.exs` with `{:hook, path: ".."}` plus
   the real dependency; `config/config.exs` with `config :hook, autostart:
   false`; a `docker-compose.yml` if the adapter needs live infra to test
   against.
3. Implement the behaviour. Register any supervised process (a connection
   pool, a channel) through `Hook.Registry`/`Hook.via/2` exactly like the
   framework's own processes do — this is what lets the facade
   (`Hook.WAL.append/2`, `Hook.Sink`'s `deliver/3` call sites) dispatch to
   your adapter without `hook` core knowing your package exists.
4. **Verify against real infrastructure, not mocks.** Both `hook_postgres`
   and `hook_rabbitmq` are tested against real Postgres/RabbitMQ containers
   — a hand-rolled protocol implementation (SigV4 signing, a SQL dedup
   ledger, AMQP publisher confirms) that "looks right" is exactly the kind
   of thing that's subtly wrong until proven against the real thing. Both
   adapters in this repo caught genuine bugs this way during development
   (see `hook_postgres`'s moduledoc on why the dedup ledger carries its own
   `seq` instead of joining back to `hook_wal`).
5. Document it: a row in the behaviour table in
   [`configuration.md`](configuration.md), and a section in
   [`storage.md`](storage.md) or [`delivery.md`](delivery.md) depending on
   which behaviour it implements.
