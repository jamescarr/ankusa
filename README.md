# Hook — a loosely coupled, high-throughput webhook ingestion framework

Built on [Bandit](https://github.com/mtrudel/bandit) + Plug. Implements the plan in
[`webhook-ingest-framework-plan.md`](webhook-ingest-framework-plan.md).

## Core invariant

**Never return 2xx until the hook is durably stored.** The edge acks only after the
group-commit batcher's WAL commit (one `fsync`) returns.

- Crash **before** commit: no 2xx sent; the provider retries.
- Crash **after** commit, before response: the provider retries; dedup absorbs it.
- Store slow/down: `503` with `Retry-After`. Never ack what you haven't saved.

The single-node default (disk WAL + local segments) survives process crash and power
loss **on that box** — not loss of the box. The startup log says so, in one line.

## Architecture

```
Provider → Bandit edge → group-commit batcher → durable WAL ─┬→ segment compactor → object store
                                                             └→ dispatch pipeline → sinks / DLQ
```

Ingest and dispatch are fully decoupled. Compaction and dispatch are competing
consumers reading the WAL by `seq` cursor — never RPC callers. Kill the compactor or
dispatch fleet and the edge keeps acking; the WAL grows, nothing is lost.

## Pluggable behaviours (every layer)

| Behaviour | Job | Default | Also shipped |
| --- | --- | --- | --- |
| `Hook.RouteResolver` | Catch-URL scheme → `%Route{tenant_id, source_id}` | `RouteResolver.Path` (`/hooks/:source_id`) | `RouteResolver.TenantPath` (`/hooks/:tenant/:source`) |
| `Hook.WAL` | Durable ack, ordered log, truncation | `WAL.DiskLog` (fsync group commit) | `WAL.Postgres` (shared, multi-node — separate `hook_postgres` package) |
| `Hook.Verifier` | Signature/timestamp checks | `Verifier.None` | StandardWebhooks, Stripe, GitHub |
| `Hook.DedupKey` | Extract provider event id | `DedupKey.Rules` (header/JSON path) | Stripe, GitHub |
| `Hook.SourceStore` | Source config, secrets, policy | `SourceStore.Static` | — |
| `Hook.Sink` | What happens to a delivered hook | `Sink.Log` | `Sink.Http` (forward via `:httpc`), `Sink.RabbitMQ` (exchange publish — separate `hook_rabbitmq` package) |
| `Hook.RetryPolicy` | Backoff / give-up | `RetryPolicy.Exponential` (jitter) | — |
| `Hook.BlobStore` | Segment PUT / range GET / delete | `BlobStore.LocalFS` | `BlobStore.S3` (+R2/MinIO), `BlobStore.GCS` |
| `Hook.Codec` | Segment record framing | `Codec.Raw` (len-prefixed, CRC32) | — |

Swapping is one line of config: `wal: {Hook.WAL.Postgres, hostname: "...", database: "..."}`.

## Guarantees, by component

- **`Hook.WAL.DiskLog`** — append-only, length-prefixed, CRC32-per-record log. One
  `fsync` per batch. Replay validates every CRC and **drops a torn trailing frame**
  (a commit that never `fsync`'d), so no un-acked write is ever surfaced. Dedup keys
  are snapshotted at truncation so `(source_id, dedup_key)` uniqueness survives
  compaction *and* restart. Seqs are 1-based; cursor `0` means "nothing consumed".
- **Group-commit batcher** — one process per partition (default: one per scheduler)
  under a supervisor. Callers block until commit. Bounded queue; a full queue sheds
  load as `503`.
- **Idempotent receiver** — duplicates still get a `2xx` (`{"status":"duplicate"}`).
- **Dispatch** — tails the WAL in `seq` order, at-least-once delivery to every source
  sink, exponential backoff with jitter, dead-letter on give-up, durable cursor.
- **Compactor** — packs many WAL records into one immutable segment (never one object
  per hook), writes an index row per hook, truncates the WAL through
  `min(compactor, dispatch)` so undispatched records are never dropped.

## Instance model

Every process is registered through `Hook.Registry` with a `via` tuple keyed by
instance name — no global names, so two instances run in one VM and tests are async.
Config is a `%Hook.Config{}` struct passed down the tree and cached in
`:persistent_term` for read-mostly access. Roles (`:edge`, `:dispatch`, `:storage`)
boot independently; the same release runs all-in-one on a laptop or as split fleets
(`HOOK_ROLES=edge,dispatch`).

## Quickstart — run locally and ingest a webhook

Requires Elixir 1.20+ / OTP 29 (check with `elixir --version`).

**1. Install deps and start the server:**

```sh
mix deps.get
iex -S mix               # or: mix run --no-halt
```

You'll see the durability banner and the listener come up:

```
[hook] starting instance default roles=[:edge, :dispatch, :storage] port=4000 data_dir=./data
[hook] DiskLog WAL at ./data/default/wal/hook.wal: recovered 0 record(s), next_seq=1. Durable to power loss on THIS host only.
Running Hook.Edge.Router with Bandit 1.12.5 at 0.0.0.0:4000 (http)
```

A zero-config `demo` source is preconfigured (accepts anything, logs it). Override the
port with `PORT=4055 iex -S mix` or `config :hook, port: <n>`.

**2. Ingest a webhook** (from another terminal):

```sh
curl -XPOST localhost:4000/hooks/demo -H 'content-type: application/json' \
  -d '{"id":"evt_1","event":"push"}'
# => {"id":"01a0...","status":"accepted","seq":1}
```

The `201` returns only *after* the payload is `fsync`'d to the WAL. The dispatch
pipeline then delivers it, which you'll see in the server log:

```
[info] hook delivered id=01a0... source=demo attempt=1
```

**3. Idempotency** — replay the same event id; it's absorbed but still `2xx`:

```sh
curl -XPOST localhost:4000/hooks/demo -d '{"id":"evt_1"}'
# => {"status":"duplicate","seq":1}
```

**4. Inspect state:**

```sh
curl localhost:4000/health
# => {"status":"ok","instance":"default","wal":{"records":..,"cursors":{"dispatch":..}}}

# durable state on disk (the WAL is truncated as the compactor rolls segments):
find data/default -type f
# data/default/wal/hook.wal        data/default/segments/index.log
# data/default/segments/seg/00000000000000000001-...seg
```

**5. Point a real provider at it** — expose the port with any tunnel, then set the
provider's webhook URL to `<tunnel>/hooks/<source_id>` and configure that source (see
[Configuring a source](#configuring-a-source)). Verification runs inline before the ack.

```sh
ngrok http 4000     # or cloudflared / tailscale funnel / etc.
```

State lives under `./data/<instance>/` (`wal/`, `segments/`, `quarantine/`, `dlq/`).

## Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `POST` | *(catch URL)* | Ingest. Path scheme is set by the configured `Hook.RouteResolver` (default `/hooks/:source_id`; `TenantPath` gives `/hooks/:tenant/:source`). Raw body kept verbatim; verified + deduped inline; committed before ack. `201` accepted / `200` duplicate / `202` quarantined / `401` verification failed / `404` unknown source / `413` too large / `503` overloaded. |
| `GET` | `/health` | Liveness + WAL stats. |
| `GET` | `/stats` | WAL stats. |

## Configuring a source

```elixir
config :hook,
  sources: %{
    "stripe" => [
      verifier: {Hook.Verifier.Stripe, secret: System.get_env("STRIPE_WHSEC")},
      dedup: {Hook.DedupKey.Stripe, []},
      on_verify_failure: :quarantine,          # :reject | :quarantine | :accept_flag
      sinks: [{Hook.Sink.Http, url: "https://example.internal/stripe"}]
    ]
  }
```

`tenant_id` on a source (default `"default"`) is the dedup/storage/retention
scope, so `(tenant_id, source_id, dedup_key)` is unique — one tenant's `evt_1`
never collides with another's. For many tenants over one path scheme, set
`route_resolver: {Hook.RouteResolver.TenantPath, []}` and carry the tenant in the
URL; the resolver's `tenant_id` wins over the source's.

## Object store adapters (S3, GCS)

`Hook.BlobStore.S3` and `Hook.BlobStore.GCS` are zero-dependency adapters
(`:httpc` + `:crypto` only — S3 requests are signed with real AWS SigV4).
`Hook.BlobStore.S3` also covers MinIO, Cloudflare R2, and any other
S3-compatible endpoint; just point `:endpoint` at it.

Local dev/test emulators via [floci](https://floci.io) — no cloud account:

```sh
docker compose up -d          # starts floci (S3, :4566) + floci-gcp (GCS, :4588)
                               # and creates the `hook-segments-dev` bucket in each
mix test --include integration
docker compose down -v        # tear down; drops emulator state
```

```elixir
# S3 / MinIO / R2
config :hook,
  storage: %{
    blob_store:
      {Hook.BlobStore.S3,
       bucket: "hook-segments-dev",
       region: "us-east-1",
       endpoint: "http://localhost:4566",   # omit for real AWS
       access_key_id: "test",
       secret_access_key: "test"}
  }

# GCS
config :hook,
  storage: %{
    blob_store: {Hook.BlobStore.GCS, bucket: "hook-segments-dev", endpoint: "http://localhost:4588"}
  }
```

Against real GCS, pass `:token_provider` (an MFA returning `{:ok, bearer_token}`)
— the adapter carries no OAuth2 dependency of its own, so wire up whatever your
deployment already uses (Goth, ADC, …).

## Shared Postgres WAL (multi-node fleets)

`WAL.DiskLog` is one BEAM node, one local file — the right default for a
laptop or a single edge node, but it cannot be the shared log a *fleet* of
ingest servers coordinates through. `Hook.WAL.Postgres` is: every node runs
its own local `Postgrex` pool against the same database; coordination between
nodes happens entirely through Postgres (row locks resolve concurrent dedup
races deterministically), never through BEAM distribution — the same
"durable state, not RPC" rule the rest of the framework already follows.

It ships as a **separate package**, `hook_postgres/` in this repo (path-dep
on `hook`, own `postgrex` dependency) — not a module in `hook` itself. That
keeps `hook`'s own `mix.exs` at zero external deps: a laptop user who never
configures Postgres never fetches or compiles `postgrex`. This is the forced
split point from the packaging model — an adapter earns its own package
exactly when it introduces a dependency the default deployment shouldn't
carry; nothing else in the framework needed to change for this to work,
because `wal: {mod, opts}` was already the seam.

```sh
cd hook_postgres
docker compose up -d --wait   # local Postgres on :5433, for dev/test only
mix deps.get
mix test                      # 10 tests, incl. concurrent-writer dedup races
                               # and a dedup-survives-truncation regression
```

```elixir
config :hook,
  wal: {Hook.WAL.Postgres,
        hostname: "localhost", port: 5433,
        username: "hook", password: "hook", database: "hook_dev",
        pool_size: 10}
```

Dedup is permanent — a separate ledger table, never touched by
`truncate_through/2`, so a duplicate of an already-compacted-away event is
still caught (mirrors `WAL.DiskLog`'s persisted `.dedup` snapshot, just
durable in the same database instead of a sidecar file).

## Test

```sh
mix test
```

59 tests cover the WAL (group commit, dedup, crash-replay, truncation), the edge
(accept/duplicate/verify/quarantine/load-shed/oversize, pluggable route
resolvers, tenant-scoped dedup), dispatch (retry + DLQ), storage (compaction
round-trip), and a **loss checker** that acks 500 hooks concurrently,
hard-kills the instance, and proves every acked id survives replay. Zero
tolerance. 8 further integration tests exercise `BlobStore.S3`/`BlobStore.GCS`
against the live emulators above and are excluded by default
(`mix test --include integration`).

## Examples

`examples/rabbitmq-consumer/` — a full deployed topology: dockerized ingest
fleet → RabbitMQ exchange (fat payloads offloaded to S3, small ones inlined)
→ a TypeScript worker that owns its own queue/binding, fetches, and prints.
`docker compose up --build` runs the whole thing.

## Not yet implemented (deferred adapters from the plan)

Kafka/Ra WAL adapters, zstd codec, Broadway-backed dispatch, LiveView
dashboard, `mix hook.new` generator, `SourceStore.Ecto` + a catch-URL
control-plane API, and per-source envelope encryption. Each is an adapter
behind an existing behaviour — the ingest guarantees above do not change when
they land.
