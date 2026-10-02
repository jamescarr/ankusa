# Ankusa

<p align="center">
  <img src="./static/ankusa.png" alt="Ankusa" width="300">
</p>

[![Docker pulls](https://img.shields.io/docker/pulls/jamescarr/ankusa.svg)](https://hub.docker.com/r/jamescarr/ankusa)
[![CI](https://github.com/jamescarr/ankusa/actions/workflows/ci.yml/badge.svg)](https://github.com/jamescarr/ankusa/actions/workflows/ci.yml)
[![Hex version](https://img.shields.io/hexpm/v/ankusa.svg)](https://hex.pm/packages/ankusa)
[![License: Apache 2.0](https://img.shields.io/hexpm/l/ankusa.svg)](https://github.com/jamescarr/ankusa/blob/main/LICENSE)

_**Don't fight the traffic. Steer it.**_

Ankusa is a self-hosted webhook receiver. Point Stripe, GitHub, or any provider
at it: every hook is written to a durable local store before Ankusa answers
`2xx`, every accepted POST is stored under a fresh `id`, and each hook is
delivered to your own worker over HTTP, RabbitMQ, Kafka, NATS JetStream, or
Redis pub/sub, with retries, a dead-letter queue, and replay. Start with one
container. Grow into a fleet by changing config, not code.

## Quickstart

### 1. Run it

```sh
docker run -d --name ankusa \
  -p 4000:4000 -p 127.0.0.1:4002:4002 \
  -v ankusa-data:/var/lib/ankusa \
  jamescarr/ankusa:edge

# the image ships a healthcheck: wait for it rather than racing the listener
until [ "$(docker inspect --format '{{.State.Health.Status}}' ankusa)" = healthy ]; do sleep 1; done

curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_1"}'
# => {"id":"01a0...","status":"accepted"}   (returned only after the store fsync)
```

Done? `docker rm -f ankusa`

That is a complete, durable webhook receiver. The image ships a `demo` source
that accepts anything, so it works before you have any provider credentials:
4000 is ingest, 4002 is the operator API (`/health`, `/metrics`), and
`/var/lib/ankusa` holds the only state worth keeping.

### 2. Deliver to your own worker

Ankusa POSTs each hook to your endpoint with the raw body intact and
`x-ankusa-id` for idempotency; short outages are retried, long ones land in a
dead-letter queue you can replay with one call.

```sh
git clone https://github.com/jamescarr/ankusa
cd ankusa/examples/quickstart
docker compose up --build -d --wait
curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_1","type":"invoice.paid"}'
sleep 1 && docker compose logs worker
# received id=01a0... source=demo bytes=36 body={"id":"evt_1","type":"invoice.paid"}
```

The wiring is one sink in `ankusa.yml`:

```yaml
sources:
  demo:
    verify: {type: none}
    on_verify_failure: accept_flag
    sinks:
      - type: http
        url: http://worker:8080/hooks
        timeout_ms: 5000
```

The worker is
[a small FastAPI app](https://github.com/jamescarr/ankusa/blob/main/examples/quickstart/worker.py)
built with [`uv`](https://docs.astral.sh/uv/), using the `ankusa` Python SDK
([`packages/sdk-python`](https://github.com/jamescarr/ankusa/tree/main/packages/sdk-python));
replace it with your own service. [The quickstart guide](docs/quickstart.md)
walks through outages, dead letters, replay, and pointing a real provider at it.

## How it works

Ankusa never answers `2xx` until the hook is durably accepted, and which system
accepts it is one config key. By default (`wal.type: disk` — the queue's mode,
named for the log it replaced) the hook is written to the node's RocksDB store,
the fsync lands first, and `201 accepted` is the only committed response —
there is no `200`. If a node dies before the write, the provider never got an
ack and retries; that retry is a new hook with a new `id`, stored and delivered
again, because ingest does no deduplication and delivery is at-least-once.
Under `wal.type: none` the node keeps no store at all: it publishes to the
source's sinks inside the request and acks on the broker's confirm. Either way,
when the destination slows down,
Ankusa answers `503` with `Retry-After`, so providers back off and try again
instead of losing events. You get that guarantee on day one, on one machine,
and you keep it when you run a hundred.

[`docs/architecture.md`](docs/architecture.md) walks the full pipeline and shows
why each guarantee holds.

```mermaid
flowchart LR
    P[Provider] -->|POST catch URL| E[Edge]
    E --> B[Group-commit Batcher]
    B -->|one fsync| W[(Store)]
    W -->|ack| P
    W --> C[Compactor]
    W --> D[Dispatch]
    C --> S[(Object store)]
    D --> SK[Sinks]
```

## What you get

- Signature verification via a configurable HMAC engine, with named schemes for Stripe, GitHub, Standard Webhooks, Shopify, and Slack, plus any body-HMAC scheme you describe in config
- Multi-tenant catch URLs, with a pluggable resolver for custom schemes
- Delivery over HTTP, RabbitMQ, Kafka, NATS JetStream, and Redis pub/sub, with
  retries, backoff, a dead letter queue, and replay
- Quarantine for hooks that fail verification, so nothing is silently dropped
- Archiving to S3, GCS, Azure Blob Storage, OCI Object Storage, Cloudflare R2, or MinIO
- Telemetry on every stage of the pipeline
- A clean handoff to job frameworks like Oban and Celery

Every piece is a swappable module, so when your needs outgrow a default you
replace that piece and keep the rest.

## Grow into a fleet

When one box is not enough, run N independent all-role nodes behind a load
balancer. Each node has its own data volume, its own store, and its own
DLQ/admin API. Archive to S3 or GCS, and fan out to Kafka, NATS, RabbitMQ, or
Redis pub/sub.
A disk-mode node keeps `edge`, `dispatch`, and `storage` in one BEAM node, so a
node is the unit of scale: add nodes, not roles. Give each node **its own
bucket** (or LocalFS) for segments. Segment keys are
`seg/<first_seq>-<last_seq>.seg`, and remote blob stores ignore the instance,
so nodes sharing a bucket overwrite each other's segments.

```mermaid
flowchart LR
    P[Stripe, GitHub, ...] --> LB[Load balancer]
    LB --> E1[Ankusa node 1]
    LB --> E2[Ankusa node 2]
    LB --> E3[Ankusa node N]
    E1 --> ST1[("own store\n+ volume")]
    E2 --> ST2[("own store\n+ volume")]
    E3 --> ST3[("own store\n+ volume")]
    ST1 & ST2 & ST3 --> Q[Kafka / NATS / RabbitMQ / HTTP]
    ST1 & ST2 & ST3 --> S[(S3 / GCS)]
    Q --> W[Your workers]
```

Your workers can be written in anything. A TypeScript, Python, Ruby, or Go
consumer reads from the queue and fetches large payloads over HTTP without ever
holding storage credentials.

Roles, the container, and fleets: [`docs/deployment.md`](docs/deployment.md).

## Examples

| Example | Delivers via | Worker | Shows |
| --- | --- | --- | --- |
| [quickstart](https://github.com/jamescarr/ankusa/tree/main/examples/quickstart/) | HTTP | Python | retries, dead letters, replay |
| [rabbitmq-consumer](https://github.com/jamescarr/ankusa/tree/main/examples/rabbitmq-consumer/) | RabbitMQ | TypeScript | consumer-owned queues, large payloads via the claim-check gateway |
| [kafka-sqs-consumer](https://github.com/jamescarr/ankusa/tree/main/examples/kafka-sqs-consumer/) | Kafka → SQS FIFO | TypeScript | SQS FIFO handoff, failure drills |
| [nats-consumer](https://github.com/jamescarr/ankusa/tree/main/examples/nats-consumer/) | NATS JetStream | Rust | a consumer-owned stream, claim-check redemption with the published Rust crate |
| [oban-consumer](https://github.com/jamescarr/ankusa/tree/main/examples/oban-consumer/) | HTTP → Oban on Kubernetes | Elixir | a fleet of independent nodes, each with its own on-disk store, load-tested with pods killed mid-run |

How to pick one: [examples/README.md](https://github.com/jamescarr/ankusa/blob/main/examples/README.md)

## Documentation

- [Quickstart](docs/quickstart.md)
- [Configuration](docs/configuration.md)
- [Deployment and scaling](docs/deployment.md)
- [Delivery, retries, and dead letters](docs/delivery.md)
- [Integrations: Oban, Celery, queues](docs/integrations.md)
- [Architecture and guarantees](docs/architecture.md)
- [Storage](docs/storage.md)
- [Multi-tenancy](docs/multi-tenancy.md)
- [Claim check](docs/claim-check.md)

Ankusa is also an Elixir library you can embed in your own app:
[docs/elixir.md](docs/elixir.md) · [HexDocs](https://hexdocs.pm/ankusa).

## The name

अंकुश (aṅkuśa) is Sanskrit for "hook" or "goad": the curved tool a mahout uses
to steer an elephant. Its root, aṅka, means "to bend" or "curve".

Webhook traffic behaves like the elephant. It is large, it arrives on its own
schedule, and it will not wait for you. So the framework takes the same shape:
absorb the traffic durably and fast through the store and batcher, then steer
where it goes next through routing and dispatch.

## License

Apache 2.0. See [LICENSE](https://github.com/jamescarr/ankusa/blob/main/LICENSE).
