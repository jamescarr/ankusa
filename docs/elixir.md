# Ankusa for Elixir

This is the container's pipeline running inside your own supervision tree: the
durable store, verification, dispatch, the sinks, and the operator API. The code
is the same. What changes is that your application owns the config and the
process tree, so you decide which roles run and when they stop.

If you would rather run Ankusa as a container, read the
[repository README](https://github.com/jamescarr/ankusa#ankusa) instead. Every
page in this documentation set is indexed in [`README.md`](README.md).

## Install

```elixir
def deps do
  [
    {:ankusa, "~> 0.7"},
    # add these as you scale out:
    {:ankusa_kafka, "~> 0.7"},     # deliver to Kafka
    {:ankusa_rabbitmq, "~> 0.7"}   # deliver to RabbitMQ
  ]
end
```

`ankusa` depends on `rocksdb`, a NIF built from source when you run
`mix deps.compile`. That build needs cmake ≥ 3.12, a C++20 compiler, and the
zstd and OpenSSL development headers. On Alpine, add `linux-headers`.

## Configure a source

```elixir
config :ankusa,
  autostart: true,
  sources: %{
    "stripe" => [
      verifier: {Ankusa.Verifier.Hmac, scheme: :stripe, secret: System.get_env("STRIPE_WHSEC")},
      sinks: [{Ankusa.Sink.Http, url: "https://example.internal/stripe", secret: System.get_env("SINK_WHSEC")}]
    ]
  }
```

Stripe now posts to `/webhooks/stripe`, and every accepted hook is delivered to
your sinks at least once. `autostart: true` boots that `default` instance with
the application, which is what you want in a single-purpose app; leave it off
and start instances yourself if your app runs more than one.

## Try it from a checkout

Requires Elixir 1.20+ and OTP 29 (`elixir --version`).

### 1. Install deps and start the server

```sh
mix deps.get
iex -S mix               # or: mix run --no-halt
```

The durability banner comes up, then the listener:

```
[ankusa] starting instance default roles=[:edge, :dispatch, :storage] port=4000 data_dir=./data
[ankusa] store at ./data/default/store. Durable to power loss on THIS host only.
Running Ankusa.Edge.Router with Bandit 1.14.1 at 0.0.0.0:4000 (http)
```

Core's own project config supplies a `demo` source that accepts anything and
logs it, so you have an endpoint before you have any provider credentials.
Override the port with `PORT=4055 iex -S mix` or `config :ankusa, port: <n>`.

### 2. Ingest a webhook

From another terminal:

```sh
curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' \
  -d '{"id":"evt_1","event":"push"}'
# => {"id":"01a0...","status":"accepted"}
```

The `201` returns only after the payload is committed to the store in one
synced batch. That is [the core invariant](architecture.md#the-core-invariant).
The dispatch pipeline then delivers the hook, which shows up in the server log:

```
[info] hook delivered id=01a0... source=demo attempt=1
```

Under `wal.type: dual` the same `201` also means the hook was confirmed in the
Kafka intake log. Under `wal.type: none` it means every sink confirmed inside
the request: there is no queue, no dispatch pipeline, and the provider's retry
is the retry.

### 3. Every accepted POST is a new hook

Without a source [`dedupe:` setting](configuration.md#sources), posting the
same body again gives you a new hook with a new `id`:

```sh
curl -XPOST localhost:4000/webhooks/demo -d '{"id":"evt_1"}'
# => {"id":"01b1...","status":"accepted"}
```

A provider retry after a lost ack lands the same way: stored and delivered
again. Consumers are idempotent receivers, so absorbing the redelivery is their
job; see [`integrations.md`](integrations.md). A source with `dedupe:` collapses
the hooks that share a provider event key within the TTL. The repeat then gets
`201` with the original `id` and `"duplicate": true`, and nothing new is stored.

### 4. Inspect state

```sh
curl localhost:4000/health
# => {"status":"ok","instance":"default"}

curl localhost:4002/v1/wal      # admin.enabled: true
# => {"instance":"default","wal":{"next_seq":..,"hooks":..,"deliveries":..,"disk_bytes":..}}
```

`hooks` and `deliveries` are RocksDB key estimates; `next_seq` is exact. `wal`
is `{}` when the store cannot be read, and `/v1/wal` answers `409 wal_disabled`
under `wal.type: none`.

The durable state on disk is the store plus the segments the compactor rolls out
of it:

```sh
find data/default -type f
# data/default/store/...        # RocksDB: hooks, delivery rows, catalogue
# data/default/segments/archive/v1/dt=2026-10-19/<node>/seg-00000000000000000001-...seg
# data/default/segments/archive/v1/dt=2026-10-19/<node>/seg-00000000000000000001-...idx
```

State lives under `./data/<instance>/`. `store/` holds the queue, the DLQ, the
quarantine pen, sources and rate limits; `segments/` holds the object store's
segment and index objects.

## Endpoints

The ingest listener serves the catch URL and one read-only endpoint. The catch
URL's shape comes from the configured `Ankusa.RouteResolver`: `/webhooks/:source_id`
by default, or `/webhooks/:tenant/:source` with `TenantPath` (see
[`multi-tenancy.md`](multi-tenancy.md)). The raw body is kept verbatim and
verified inline, and the `2xx` waits for the durable accept described above.
`201 accepted` is the only committed response; there is no `200`.

| Method | Path | Description |
| --- | --- | --- |
| `POST` | *(catch URL)* | Ingest. `201` accepted or duplicate, `202` quarantined, `400` body unreadable or `invalid_header`, `401` verification failed, `403` route guard (when `routes.ip_denied_status` is `403`), `404` unknown source, `413` too large, `429` `rate_limited` or `quarantine_rate_limited` (with `Retry-After`), `503` overloaded, undeliverable, or `quarantine_full`. |
| `GET` | `/health` | Liveness. Always `200` while the listener is up. |

The operator API is a separate port, and `/metrics`, the dead-letter queue,
replay, and quarantine all live behind `admin.enabled: true`. See the admin API
section in [`configuration.md`](configuration.md#the-admin-api).

## Roles from code

`Ankusa.Instance`'s `init/1` starts children conditionally on `config.roles`.
The store and the queue writer are the two to know:

```elixir
@doc """
Whether this instance runs the node-local store (`Ankusa.Store`) and the audit
trail written into it (`Ankusa.Audit`): any of the `:edge`, `:dispatch` or
`:storage` roles, the admin API (every change it makes is audited), or a
source store that accepts writes.
"""
def store?(%Config{} = config) do
  Enum.any?([:edge, :dispatch, :storage], &Config.role?(config, &1)) or config.admin.enabled or
    writable_source_store?(config)
end

# The one process that assigns seqs and commits hooks, in the edge subtree
# (only an `:edge` node writes hooks). Under `wal: :disk` and `wal: :dual` (which
# adds the intake log); under `wal: :none` ingest acks on a sink's confirm and
# nothing is committed.
defp writer_children(%Config{wal: wal}, opts) when wal in [:disk, :dual],
  do: [{Ankusa.Queue.Writer, opts}]

defp writer_children(_config, _opts), do: []
```

The writer is not role-checked itself. It lives in the `:edge` subtree, which
only starts when the `edge` role is on, and only under `wal: :disk` or
`wal: :dual`. The `edge`, `dispatch`, `storage` and `claim_check` children all
follow the same `if Config.role?(config, role)` shape.

One Mix release covers many deployments. The same compiled artifact runs
all-in-one on a laptop or as one node of a fleet, because which children start
is a runtime config decision, never a build-time one.

`edge`, `dispatch`, and `storage` share the one local RocksDB store at
`<data_dir>/<instance>/store`, and RocksDB is single-process, so every
store-backed role has to run in the **same BEAM node**. You cannot split them
across processes or hosts. `claim_check` is stateless and can run anywhere:

```sh
ANKUSA_ROLES=edge,dispatch,storage mix run --no-halt   # the store-backed shape
ANKUSA_ROLES=claim_check mix run --no-halt             # claim-check gateway, any node
```

`Ankusa.Application` reads `ANKUSA_ROLES` (comma-separated) and `PORT` on top of
whatever `config.exs` sets. See
[`configuration.md#runtime-environment-overrides`](configuration.md#runtime-environment-overrides).

## The queue from code

The default `wal: :disk` commits every accepted hook to `Ankusa.Queue`;
`wal: :dual` runs the same commit and a confirmed intake-log publish. The
batcher does that on the ingest path, and you can read the queue and manage the
DLQ directly:

```elixir
# one entry per hook: the envelope plus the source's sinks at ack time
{:ok, [{:committed, env}]} =
  Ankusa.Queue.enqueue(:default, [%{envelope: env, sinks: [{Ankusa.Sink.Http, url: "..."}]}])

{:ok, hooks} = Ankusa.Queue.hooks(:default, 0, 100)     # seq > 0, ascending
{:ok, stats} = Ankusa.Queue.stats(:default)             # %{next_seq, hooks, deliveries, disk_bytes}
{:ok, %{total: n, entries: entries}} = Ankusa.Queue.dead(:default, source_id: "stripe", limit: 100)

# A durable, paced replay job instead of a one-shot flip:
{:ok, :created, job} = Ankusa.Replay.start(:default, kind: :dlq, source_id: "stripe", rate: 1_000)
{:ok, job} = Ankusa.Replay.get(:default, job.id)        # watch moved/delivered/state
{:ok, job} = Ankusa.Replay.update(:default, job.id, state: :paused)   # or :cancelled / a new :rate
```

`Ankusa.Replay.start/2` creates a job that drips dead rows back to pending at
`rate` items per second, only while dispatch has spare capacity, and the
pipeline delivers them asynchronously. A row that fails again is dead-lettered
again. The job is durable, because its cursor commits with the rows it moves,
and it is resumable. It also pauses itself if its deliveries keep
dead-lettering. Under `wal.type: none` nothing is committed, so there is no
queue and no DLQ.

## Deploying your own wrapper app

If you embed Ankusa rather than run the published image, the deployment shape is
yours to choose. The worked example
([`examples/rabbitmq-consumer/ingest_app/Dockerfile`](https://github.com/jamescarr/ankusa/blob/main/examples/rabbitmq-consumer/ingest_app/Dockerfile))
shows the pattern instead of prescribing one image for every use case.

The example builds a dev-mode image: `elixir:1.20.4-alpine`, then
`mix deps.get && mix compile`, with `CMD ["mix", "run", "--no-halt"]`. That is
simpler and faster to build, and right-sized for an example or a low-traffic
deployment. A real production deployment should build a release image instead:
multi-stage, `mix release` in the build stage, a slim runtime base in the final
stage. Nothing here ships a release config, because it is genuinely
deployment-specific: whether config comes from env vars or `runtime.exs`, and
which roles each image carries.

Either build stage needs the RocksDB toolchain from [Install](#install): cmake,
a C++20 compiler, and the zstd and OpenSSL headers. Those are the same packages
the shipped example installs.

A deployable wrapper app, like
[`ingest_app/`](https://github.com/jamescarr/ankusa/tree/main/examples/rabbitmq-consumer/ingest_app),
is the intended pattern. It is a small Mix project that depends on `ankusa` plus
whichever adapter packages it needs, reads its own env vars, and calls
`Ankusa.Config.new/1` and `Ankusa.Instance.start_link/1` directly. `ankusa` core
stays a library; the wrapper is where the "how do I actually deploy this"
config lives.

Stopping the node on SIGTERM is the wrapper's job too. `:ankusa`'s own
`prep_stop/1` drains every instance in its supervision tree: the autostarted
default instance, or one you start with
`Supervisor.start_child(Ankusa.Supervisor, {Ankusa.Instance, config})`. An
instance your app supervises itself is not reachable from there. Call
`Ankusa.Instance.Drain.run(instance)` from your application's `prep_stop/1`
instead, which answers `/ready` with `503 draining`, waits
`listener.drain_delay_ms`, closes the ingest listener after the requests in
flight, and waits for the batchers (see [Deployment](deployment.md)). Set the
pod's `terminationGracePeriodSeconds` to cover it.

If that wrapper depends on `ankusa` directly *and* transitively through an
adapter package, mark your direct entry `override: true`. The reason, and the
Dockerfile and compose context that go with it, is in
[`packaging.md#building-an-app-against-the-path-deps`](packaging.md#building-an-app-against-the-path-deps).

## Where next

[`configuration.md`](configuration.md) has every config key, struct and YAML key.
For Ankusa and Oban in one app, read
[`integrations.md#in-process-embedding-ankusa-and-oban-in-the-same-app`](integrations.md#in-process-embedding-ankusa-and-oban-in-the-same-app).
[`storage.md`](storage.md) covers the local store and the object stores, and
[`delivery.md`](delivery.md) covers sinks, retries and dead letters. Module
reference and callback signatures are on
[HexDocs](https://hexdocs.pm/ankusa).
