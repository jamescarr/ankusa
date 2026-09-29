# Ankusa for Elixir

The same pipeline the container runs, durable WAL, verification,
dispatch, sinks, as a library in your own supervision tree. If you would
rather run Ankusa as a container, or you don't write Elixir, start at the
[README](../README.md).

## Install

```elixir
def deps do
  [
    {:ankusa, "~> 0.2"},
    # add these as you scale out:
    {:ankusa_kafka, "~> 0.2"},     # deliver to Kafka
    {:ankusa_rabbitmq, "~> 0.2"}   # deliver to RabbitMQ
  ]
end
```

## Configure a source

```elixir
config :ankusa,
  autostart: true,
  source_store:
    {Ankusa.SourceStore.Static,
     sources: %{
       "stripe" => [
         verifier: {Ankusa.Verifier.Hmac, scheme: :stripe, secret: System.get_env("STRIPE_WHSEC")},
         sinks: [{Ankusa.Sink.Http, url: "https://example.internal/stripe"}]
       ]
     }}
```

Stripe now posts to `/webhooks/stripe`, and every accepted hook is delivered
to your sinks at least once.

## Try it from a checkout

Requires Elixir 1.20+ / OTP 29 (`elixir --version`).

### 1. Install deps and start the server

```sh
mix deps.get
iex -S mix               # or: mix run --no-halt
```

You'll see the durability banner and the listener come up:

```
[ankusa] starting instance default roles=[:edge, :dispatch, :storage] port=4000 data_dir=./data
[ankusa] DiskLog WAL at ./data/default/wal/ankusa.wal: recovered 0 record(s), next_seq=1. Durable to power loss on THIS host only.
Running Ankusa.Edge.Router with Bandit 1.12.5 at 0.0.0.0:4000 (http)
```

A zero-config `demo` source is preconfigured (accepts anything, logs it).
Override the port with `PORT=4055 iex -S mix` or `config :ankusa, port: <n>`.

### 2. Ingest a webhook

From another terminal:

```sh
curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' \
  -d '{"id":"evt_1","event":"push"}'
# => {"id":"01a0...","status":"accepted"}
```

The `201` returns only *after* the payload is `fsync`'d to the WAL, that's
the [core invariant](architecture.md#the-core-invariant), not a formality.
The dispatch pipeline then delivers it, visible in the server log:

```
[info] hook delivered id=01a0... source=demo attempt=1
```

Under `wal.type: none` the same `201` means the sinks confirmed inside the
request instead: there is no log, no dispatch pipeline, and the provider's
retry is the retry.

### 3. Every accepted POST is stored

Ingest does no deduplication, so posting the same body again is a new hook
with a new `id`:

```sh
curl -XPOST localhost:4000/webhooks/demo -d '{"id":"evt_1"}'
# => {"id":"01b1...","status":"accepted"}
```

A provider retry after a lost ack lands the same way, stored and delivered
again. Consumers are idempotent receivers, so absorbing the redelivery is
their job; see [`integrations.md`](integrations.md).

### 4. Inspect state

```sh
curl localhost:4000/health
# => {"status":"ok","instance":"default"}

curl localhost:4002/v1/wal      # admin.enabled: true
# => {"instance":"default","wal":{"records":..,"cursors":{"dispatch":..}}}
```

Durable state on disk (the WAL is truncated as the compactor rolls
segments):

```sh
find data/default -type f
# data/default/wal/ankusa.wal        data/default/segments/index.log
# data/default/segments/seg/00000000000000000001-...seg
```

State lives under `./data/<instance>/` (`wal/`, `segments/`, `quarantine/`,
`dlq/`).

## Endpoints

The ingest listener serves the catch URL, plus one read-only endpoint:

| Method | Path | Description |
| --- | --- | --- |
| `POST` | *(catch URL)* | Ingest. Path scheme is set by the configured `Ankusa.RouteResolver` (default `/webhooks/:source_id`; `TenantPath` gives `/webhooks/:tenant/:source`, see [`multi-tenancy.md`](multi-tenancy.md)). Raw body kept verbatim; verified inline; the `2xx` is returned only after the durable accept — the WAL fsync, or every sink's confirm under `wal.type: none`: `201 accepted` is the only committed response (there is no `200`). Every accepted POST is a new hook with a new `id`; ingest does no deduplication, so a provider retry after a lost ack is stored and delivered again. `201` accepted / `202` quarantined / `400` body unreadable / `401` verification failed / `404` unknown source / `413` too large / `503` overloaded or undeliverable. |
| `GET` | `/health` | Liveness. Always `200` while the listener is up. |

The operator API on its own port, `/metrics`, the dead-letter queue, replay,
quarantine, is `admin.enabled: true`; see the admin API section in
[`configuration.md`](configuration.md#ankusa-config).

## Roles from code

`Ankusa.Instance`'s `init/1` starts children conditionally on `config.roles`:

```elixir
defp edge_children(config, opts), do: if Config.role?(config, :edge), do: [...], else: []
defp dispatch_children(config, opts), do: if Config.role?(config, :dispatch), do: [...], else: []
defp storage_children(config, opts), do: if Config.role?(config, :storage), do: [...], else: []
defp claim_check_children(config, opts), do: if Config.role?(config, :claim_check), do: [...], else: []
```

One Mix release, many deployments: the same compiled artifact runs
all-in-one on a laptop or as a node in a fleet, because *which* children
start is a runtime config decision, never a build-time one.

`edge`, `dispatch`, and `storage` share the one local `WAL.DiskLog` file, so
every WAL role has to run in the **same BEAM node**: you can't split them
across processes or hosts. `claim_check` is stateless and can run anywhere:

```sh
ANKUSA_ROLES=edge,dispatch,storage mix run --no-halt   # the WAL-bearing shape
ANKUSA_ROLES=claim_check mix run --no-halt             # claim-check gateway, any node
```

Ankusa.Application reads `ANKUSA_ROLES` (comma-separated) and `PORT` on top of
whatever `config.exs` sets, see
[`configuration.md#runtime-environment-overrides`](configuration.md#runtime-environment-overrides).

## Deploying your own wrapper app

If you embed Ankusa rather than run the published image, deployment shape is a
choice the operator makes, so the worked example
([`examples/rabbitmq-consumer/ingest_app/Dockerfile`](https://github.com/jamescarr/ankusa/blob/main/examples/rabbitmq-consumer/ingest_app/Dockerfile))
shows the pattern rather than prescribing one image for every use case:

- **Dev-mode image** (what the example uses): `elixir:1.20.4-alpine`,
  `mix deps.get && mix compile`, `CMD ["mix", "run", "--no-halt"]`. Simpler,
  faster to build, right-sized for an example or a low-traffic deployment.
- **Release image** (what a real production deployment should build
  instead): multi-stage, `mix release` in the build stage, a slim runtime
  base in the final stage. Not shipped here: the framework doesn't
  prescribe a release config because that's genuinely deployment-specific
  (env-var vs. `runtime.exs`-based config, which roles per image, etc.).

Either way, a deployable wrapper app (like
[`ingest_app/`](https://github.com/jamescarr/ankusa/tree/main/examples/rabbitmq-consumer/ingest_app)) is the intended
pattern: a tiny Mix project that depends on `ankusa` (+ whichever adapter
packages it needs), reads its own env vars, and calls `Ankusa.Config.new/1` +
`Ankusa.Instance.start_link/1` directly. `ankusa` core stays a library; the
wrapper is where "how do I actually deploy this" config lives.

If that wrapper depends on `ankusa` directly *and* transitively through an
adapter package, mark your direct entry `override: true`: the reason, and the
Dockerfile/compose context that goes with it, is in
[`packaging.md#building-an-app-against-the-path-deps`](packaging.md#building-an-app-against-the-path-deps).

## Where next

- Every config key, struct and YAML: [`configuration.md`](configuration.md)
- Embedding Ankusa and Oban in one app: [`integrations.md`](integrations.md#in-process-embedding-ankusa-and-oban-in-the-same-app)
- WAL and object stores: [`storage.md`](storage.md)
- Sinks, retries, dead letters: [`delivery.md`](delivery.md)
- Module reference and callback signatures: [HexDocs](https://hexdocs.pm/ankusa)
