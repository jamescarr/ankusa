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
| `Hook.WAL` | Durable ack, ordered log, truncation | `WAL.DiskLog` (fsync group commit) | — |
| `Hook.Verifier` | Signature/timestamp checks | `Verifier.None` | StandardWebhooks, Stripe, GitHub |
| `Hook.DedupKey` | Extract provider event id | `DedupKey.Rules` (header/JSON path) | Stripe, GitHub |
| `Hook.SourceStore` | Source config, secrets, policy | `SourceStore.Static` | — |
| `Hook.Sink` | What happens to a delivered hook | `Sink.Log` | `Sink.Http` (forward via `:httpc`) |
| `Hook.RetryPolicy` | Backoff / give-up | `RetryPolicy.Exponential` (jitter) | — |
| `Hook.BlobStore` | Segment PUT / range GET / delete | `BlobStore.LocalFS` | — |
| `Hook.Codec` | Segment record framing | `Codec.Raw` (len-prefixed, CRC32) | — |

Swapping is one line of config: `wal: {Hook.WAL.Postgres, repo: MyApp.Repo}`.

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

## Test

```sh
mix test
```

51 tests cover the WAL (group commit, dedup, crash-replay, truncation), the edge
(accept/duplicate/verify/quarantine/load-shed/oversize), dispatch (retry + DLQ),
storage (compaction round-trip), and a **loss checker** that acks 500 hooks
concurrently, hard-kills the instance, and proves every acked id survives replay.
Zero tolerance.

## Not yet implemented (deferred adapters from the plan)

Postgres/Kafka/Ra WAL adapters, S3/GCS/R2 blob stores, zstd codec, Broadway-backed
dispatch, LiveView dashboard, `mix hook.new` generator, and per-source envelope
encryption. Each is an adapter behind an existing behaviour — the ingest guarantees
above do not change when they land.
