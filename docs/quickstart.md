# Quickstart

Requires Elixir 1.20+ / OTP 29 (`elixir --version`).

## 1. Install deps and start the server

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

## 2. Ingest a webhook

From another terminal:

```sh
curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' \
  -d '{"id":"evt_1","event":"push"}'
# => {"id":"01a0...","status":"accepted","seq":1}
```

The `201` returns only *after* the payload is `fsync`'d to the WAL — that's
the [core invariant](architecture.md#the-core-invariant), not a formality.
The dispatch pipeline then delivers it, visible in the server log:

```
[info] hook delivered id=01a0... source=demo attempt=1
```

## 3. Idempotency

Replay the same event id — it's absorbed but still gets a `2xx`:

```sh
curl -XPOST localhost:4000/webhooks/demo -d '{"id":"evt_1"}'
# => {"id":"01a0...","status":"duplicate","seq":1}
```

## 4. Inspect state

```sh
curl localhost:4000/health
# => {"status":"ok","instance":"default","wal":{"records":..,"cursors":{"dispatch":..}}}

curl localhost:4000/stats
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

## 5. Point a real provider at it

Expose the port with any tunnel, then set the provider's webhook URL to
`<tunnel>/webhooks/<source_id>` and configure that source (see
[`configuration.md`](configuration.md#configuring-a-source)). Verification
runs inline, before the ack.

```sh
ngrok http 4000     # or cloudflared / tailscale funnel / etc.
```

## Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `POST` | *(catch URL)* | Ingest. Path scheme is set by the configured `Ankusa.RouteResolver` (default `/webhooks/:source_id`; `TenantPath` gives `/webhooks/:tenant/:source` — see [`multi-tenancy.md`](multi-tenancy.md)). Raw body kept verbatim; verified + deduped inline; committed before ack. `201` accepted / `200` duplicate / `202` quarantined / `400` body unreadable / `401` verification failed / `404` unknown source / `413` too large / `503` overloaded. |
| `GET` | `/health` | Liveness + WAL stats. |
| `GET` | `/stats` | WAL stats. |

## Where next

- Configure a real provider (Stripe, GitHub, Standard Webhooks): [`configuration.md`](configuration.md).
- Multi-tenant catch URLs: [`multi-tenancy.md`](multi-tenancy.md).
- Object storage / shared Postgres WAL for a fleet: [`storage.md`](storage.md).
- Forward to HTTP or publish to RabbitMQ: [`delivery.md`](delivery.md).
- See a full deployed example (Docker + RabbitMQ + S3 + a TypeScript
  consumer): [`examples/rabbitmq-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/rabbitmq-consumer/).
