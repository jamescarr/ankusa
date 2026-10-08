# Packaging: why this is a mono-repo of separate Mix projects

## The decision rule

Two questions, deliberately kept separate:

**1. Should we depend on this at all?** Judge it on merit: does the library do
the job better than the code we would otherwise own: more correct, better
tested, less surface for us to maintain and be wrong about? Then take it. *Zero
dependencies is not a goal here*; the goal is the least code we have to be right
about, and a well-maintained library replacing a hand-rolled protocol
implementation is usually the cheaper side of that trade.

**2. Where should it live?** That is the packaging question:

> **Split a package when, and only when, an adapter introduces an external
> dependency the default deployment shouldn't compile. Everything else
> stays in core. Role separation (edge/dispatch/storage) is runtime config,
> not packaging.**

A dependency every user benefits from belongs in `ankusa` core: no split, no
ceremony. A dependency only one adapter needs belongs in that adapter's package.

This isn't a stylistic preference. It's the direct consequence of the
project being library-first (embeds in an existing Phoenix/Bandit app) as
well as a deployable release. A library embedder pays for every dependency
their host app compiles, whether they use it or not. A laptop user running
`mix deps.get` on `ankusa` alone should never fetch `amqp` or `brod`
because they exist in the ecosystem, only because they were actually
configured.

## Layout

```
packages/
  ankusa                      core. mix.exs runtime deps: {bandit, plug, cidr,
                              req, aws_signature, telemetry_metrics,
                              telemetry_metrics_prometheus_core, nebulex,
                              nebulex_local, rocksdb, async_api_spex}. No adapter
                              deps.
    lib/ankusa/…              behaviours, envelope, config, registry, telemetry,
                              edge/dispatch/storage machinery, the store
                              (Ankusa.Store, on erlang-rocksdb), and every
                              zero-external-dep default adapter
                              (BlobStore.{LocalFS,S3,GCS,Azure,OCI},
                              Codec.Raw, all Verifiers,
                              Sink.{Log,Http}, RetryPolicy.Exponential,
                              RouteResolver.{Path,TenantPath})
  ankusa_rabbitmq             path-dep on ankusa + amqp. Ankusa.Sink.RabbitMQ.
  ankusa_kafka                path-dep on ankusa + brod. Ankusa.Sink.Kafka.
  ankusa_nats                 path-dep on ankusa + gnat. Ankusa.Sink.NATS.
  ankusa_redis                path-dep on ankusa + redix.
                              Ankusa.Routes.Store.Redis (route definitions in
                              Redis, shared across edge nodes) and
                              Ankusa.Sink.Redis (delivered hooks published to
                              a pub/sub channel).
  ankusa_server               the jamescarr/ankusa Docker image: core + every
                              adapter, configured by YAML. Not on Hex.
  async_api_spex              generic AsyncAPI 3.0 library: document structs,
                              `use AsyncApiSpex.Schema`/`Message`/`Channel`/`Spec`
                              (decorate an app's own structs and publishers), a
                              validator, a Plug, `mix async_api_spex.gen`. No
                              Ankusa code; core depends on it for
                              `Ankusa.AsyncApi`. Any Elixir app can use it on
                              its own.
  sdk-typescript              the `ankusa` npm client SDK.
  sdk-python                  the `ankusa` PyPI client SDK.
  sdk-rust                    the `ankusa` crates.io client SDK.
  sdk-ruby                    the `ankusa-sdk` RubyGems client SDK.
  sdk-php                     the `jamescarr/ankusa` Packagist client SDK,
                              published through the read-only mirror
                              jamescarr/ankusa-php (Packagist reads
                              composer.json at a repository root).
  sdk-elixir                  the `ankusa_sdk` Hex client SDK: a `Plug`
                              receiver for HTTP-sink deliveries and a decoder
                              for the queue-sink message format, plus clients
                              for the operator APIs. A pure HTTP client — it
                              never depends on `ankusa` core.
  sdk-java                    the `io.github.jamescarr:ankusa-sdk` Maven
                              Central client SDK, built with sbt 2 and
                              published as Java 17 bytecode. Pure Java on
                              the JDK's HttpClient plus Jackson 3 and the
                              JSpecify annotations; a pure HTTP client like
                              the other SDKs.
examples/                     deployable demos; not published packages
tools/loadgen/                load generator for the examples
conformance/                  language-neutral SDK conformance vectors + checker
```

`ankusa_rabbitmq`, `ankusa_kafka`, `ankusa_nats`, and `ankusa_redis` each
depend on `ankusa` via `{:ankusa, path: "../ankusa"}` for local development,
and on the Hex package once published. Each ships its **own**
`docker-compose.yml` for local dev/test infra (`packages/ankusa_rabbitmq/` →
RabbitMQ on `:5673`/`:15673`; `packages/ankusa_kafka/` → Redpanda on `:19092`;
`packages/ankusa_nats/` → NATS with JetStream on `:4223`/`:8223`;
`packages/ankusa_redis/` → Redis on `:6399`), which `mise run check:package
<pkg>` brings up and tears down around the suite. Every adapter package test
suite needs `Ankusa.Registry` running (started by `ankusa`'s own Application);
none needs any config to get it, since Ankusa.Application's built-in default
instance is off (`autostart: false`) by default and only the core project's
own `packages/ankusa/config/config.exs` turns it on.

Core itself depends on `async_api_spex` the same way (`path:` in dev/test, the
Hex release otherwise). Mix loads a path dependency's `mix.exs` under `:prod`,
so a project that path-depends on core would resolve the Hex `async_api_spex`,
which is not published until a release. Each such project (the four adapters'
dev/test deps, `ankusa_server`, the example ingest apps) therefore also pins
`{:async_api_spex, path: "../async_api_spex", override: true}`, the same way
the examples pin `:ankusa`.

## Why S3/GCS/Azure/OCI stayed in-tree but RabbitMQ/Kafka/NATS didn't

This is a dependency-weight split, not a position on hand-rolling.

`Ankusa.BlobStore.{S3,GCS,Azure,OCI}` are in core rather than in their own
packages because their dependencies are small and focused:
`aws_signature` (signing only) and `Req` (HTTP, with its own Finch pool).
Neither drags a credential stack or a framework along, so a `LocalFS` user's
`deps.get` stays cheap. GCS and Azure bundle nothing credential-shaped at all:
each takes a pre-obtained credential (`:token_provider`, or Azure's
`:sas_token`) and leaves acquisition (Goth, ADC, the Azure CLI) to the
deployment. OCI carries **no dependency at all**: its RSA-SHA256 *Signature
version 1* signing is implemented with OTP's own `:public_key`, pinned against
OCI's reference vectors (see the `### Resolved` note below for why that's an
exception to the no-hand-rolling rule, and `test/ankusa/blob_store_oci_signing_test.exs`
for the proof).

### Resolved: the hand-rolled SigV4 signing is gone

`Ankusa.BlobStore.S3` used to hand-roll canonical-request / string-to-sign /
HMAC-chain code, ~80 lines, security-sensitive, and covered only by
`:integration`-tagged tests. That is precisely the profile where a focused
library wins, so it now calls `aws_signature` (the implementation behind the
official aws-elixir SDK) through the same `Req` client the other adapters use.
Core shrank by ~50 lines, and the part we would most regret getting subtly wrong
is no longer ours to get wrong.

Worth knowing when reading the tests: `floci` does **not** validate SigV4, a
bogus signature, no signature at all, and a wrong-secret signature all return
`200`, so the integration suite could never have caught a signing bug.
`test/ankusa/blob_store_s3_signing_test.exs` does, from both ends: it
reproduces AWS's published reference signatures, and it reconstructs the
adapter's own signing call from a captured request to pin S3's "sign the path as
sent" rule.

### The OCI exception: signing with no library to lean on

`Ankusa.BlobStore.OCI` signs its own requests, which *looks* like a regression
to the hand-rolling the S3 note above removed. It isn't the same call: OCI has
no *mature, focused* Elixir signing library to take (unlike `aws_signature` for
SigV4). `ex_oci_sdk` exists but is a whole SDK at v0.2 with ~700 downloads,
not the proven, single-purpose signing dependency `aws_signature` is, and OCI
offers no bearer/SAS shortcut an Object Storage adapter could ride instead. So
the rule becomes "hand-roll only when there is no library and no credential-free
path, and pin it against the provider's own reference vectors", which is what
`test/ankusa/blob_store_oci_signing_test.exs` does, reproducing the RSA-SHA256
signature of OCI's published test string (computed independently with OpenSSL)
and reconstructing the signing string from a captured request.

`rocksdb` is the one native dependency core takes on its own, and the rule
above says why: it is the node's store, not an adapter. Every role that reads
or writes hooks (`edge`, `dispatch`, `storage`) opens it, and so does a
writable source store; only a claim-check-only node runs without one. It is
compiled into every build either way, so there is no default deployment to
spare. It is built from source on `mix deps.compile` and needs cmake ≥ 3.12, a
C++20 compiler, and
the zstd and OpenSSL development headers (plus `linux-headers` on Alpine). The
`ankusa_server` image installs all of them and builds RocksDB in its own cached
layer, so a source change does not rebuild it; Ubuntu CI runners need
`libzstd-dev`.

The cost is paid per Mix project, not once: each adapter package,
`ankusa_server`, and every example app has its own `_build`, so each one
compiles its own copy of RocksDB (minutes, not seconds, on a cold cache) once
per checkout or lockfile change. CI caches `deps` and `_build` per package (and
once for all the examples), keyed on the lockfile and the pinned toolchain, so
a warm run skips it. An embedder that already has RocksDB installed can link
the NIF against it instead of building the bundled copy (the NIF itself still
compiles); `deps/rocksdb/CUSTOMIZED_BUILDS.md` in the Hex package lists the
options.

`Ankusa.Sink.RabbitMQ` needs `amqp` (which pulls `amqp_client`,
`rabbit_common`: real NIF/native-adjacent Erlang libraries).
`Ankusa.Sink.Kafka` needs `brod`, which pulls `crc32cer`: a C++ NIF that
compiles from source on every `mix deps.compile`, so that package carries its
own build-toolchain requirement (CMake ≥ 3.16 plus a C++ compiler) on top of
core's. The split is not "no native code"; it is "no native code a deployment
doesn't need": an HTTP-only deployment compiles `rocksdb` and not `crc32cer`.
`Ankusa.Sink.NATS` needs
`gnat`, which pulls `jason`, `nkeys` (+ `ed25519`/`kcl`), `nimble_parsec`, and
`connection`, pure Elixir, but four libraries nobody running an HTTP-,
Kafka-, or RabbitMQ-only deployment has any use for, which is the same test
with a lighter dependency. `ankusa_redis` carries `redix` for its two
adapters — the shared route store and `Ankusa.Sink.Redis` — so only a
deployment that configures one of them compiles it. Those are genuine
external dependencies the laptop/standalone user shouldn't pay to compile,
so each got its own package the moment it was built, not before.

## What deliberately did *not* get split

The plan document that predates this codebase proposed separate
applications per **role** (`hook_edge`, `hook_storage`, `hook_dispatch`,
`hook_dashboard`). That didn't happen, on purpose: role separation is a
config concern (`config.roles` / `ANKUSA_ROLES`), not a dependency-weight
concern. Splitting them into packages would buy package-management overhead
(version matrix, release coordination) for zero dependency-isolation
benefit: every role's code uses the same dependencies core already pulls in
(the store included). One
release, many roles, config decides what boots. See
[`deployment.md`](deployment.md).

## Adding a new adapter package

1. Decide whether it needs a dependency at all, on merit (question 1 above). If a
   library is the right tool, take it, then put the adapter in its own package
   so deployments that don't configure it never compile it. If no dependency is
   warranted, the adapter belongs in `ankusa` core next to the dependency-free
   ones (`BlobStore.LocalFS`, `Codec.Raw`, the verifiers), not
   because hand-rolling is preferred, but because a dependency that buys nothing
   is a liability.
2. Scaffold it: `mise run new:adapter <name> [--module Mod]` creates
   `packages/ankusa_<name>/` (an `Ankusa.Sink.<Mod>` stub, its mix project
   path-depending on `../ankusa`, README, CHANGELOG, and one failing test),
   picked up by `mise run check`, CI, and the release tasks with no further
   wiring. Add the real dependency, and a `docker-compose.yml` if the adapter
   needs live infra to test against. No config is needed to keep `ankusa`'s
   built-in demo instance from booting: `autostart` defaults to `false`.
3. Implement the behaviour. Register any supervised process (a connection
   pool, a channel) through `Ankusa.Registry`/`Ankusa.via/2` exactly like the
   framework's own processes do: this is what lets the facade
   (`Ankusa.Sink`'s `deliver/3` call sites, for example) dispatch to
   your adapter without `ankusa` core knowing your package exists.
4. **Verify against real infrastructure, not mocks.** `ankusa_rabbitmq`,
   `ankusa_kafka`, `ankusa_nats`, and `ankusa_redis` are tested against real
   RabbitMQ/Redpanda/NATS/Redis containers: a protocol implementation (AMQP
   publisher confirms, a Kafka produce path) that "looks right" is exactly
   the kind of thing that's subtly wrong until proven against the real thing.
5. Document it: a row in the behaviour table in
   [`configuration.md`](configuration.md), and a section in
   [`storage.md`](storage.md) or [`delivery.md`](delivery.md) depending on
   which behaviour it implements.

## Building an app against the path deps

Because `ankusa_rabbitmq`/`ankusa_kafka`/`ankusa_nats` are path-dependencies
during local development, a Dockerfile building an app that depends on them
needs a build **context** wide enough to see the whole slice, with the relative
paths preserved so the same `mix.exs` files resolve identically inside the
container as they do on disk:

```dockerfile
# examples/rabbitmq-consumer/ingest_app/Dockerfile
WORKDIR /repo
COPY packages/ankusa ./packages/ankusa                   # ankusa core
COPY packages/async_api_spex ./packages/async_api_spex
COPY packages/ankusa_rabbitmq ./packages/ankusa_rabbitmq
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
package needs `override: true`.** Each adapter package's own
`mix.exs` picks its Hex entry for `:ankusa` (`~> 0.5` for rabbitmq/kafka/nats, `~> 0.4` for redis) whenever Mix
evaluates it as a nested dependency: Mix builds dependencies under `:prod`
by default regardless of *your* project's `Mix.env()`, so the adapter
package's dev/test-only path-dep branch never gets hit there. A wrapper app
like `ingest_app` that depends on both `ankusa` (path) and
`ankusa_rabbitmq` (path, which transitively wants `ankusa` from Hex) hits a
real conflict: `mix deps.get` refuses with "the dependency ankusa in
mix.exs is overriding a child dependency." Fix: mark your direct entry
`override: true` so Mix uses it everywhere in the tree:

```elixir
defp deps do
  [
    {:ankusa, path: "../../../packages/ankusa", override: true},
    {:async_api_spex, path: "../../../packages/async_api_spex", override: true},
    {:ankusa_rabbitmq, path: "../../../packages/ankusa_rabbitmq"}
  ]
end
```
