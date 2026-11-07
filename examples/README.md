# Examples

Nine runnable demonstrations of the same receiver with a different last hop.
Each one ends in a worker you could replace with your own service; read the
[quickstart](quickstart/) first, then pick the transport you actually run.

![One provider posts to an Ankusa node that stores each hook before 201 and delivers it four ways: HTTP POST to a worker, a RabbitMQ exchange to a worker-owned queue, Kafka bridged into an SQS FIFO queue, and a NATS JetStream stream; queue workers redeem large bodies at the claim-check gateway.](../docs/diagrams/examples-overview.png)

## Pick one

| Example | Delivers via | Worker | Needs | Run |
| --- | --- | --- | --- | --- |
| [quickstart](quickstart/) | Kafka → SQS FIFO | Python (boto3, uv) | docker | `docker compose up --build -d --wait` |
| [http-consumer](http-consumer/) | HTTP | Python (FastAPI, uv) | docker | `docker compose up --build -d --wait` |
| [rabbitmq-consumer](rabbitmq-consumer/) | RabbitMQ | TypeScript | docker | `docker compose up --build` |
| [kafka-sqs-consumer](kafka-sqs-consumer/) | Kafka → SQS FIFO | TypeScript | docker | `docker compose up --build -d --wait` |
| [event-routing](event-routing/) | Kafka topic named by event type, and a filtered HTTP sink | TypeScript | docker | `docker compose up --build -d --wait && ./run.sh` |
| [nats-consumer](nats-consumer/) | NATS JetStream | Rust | docker | `docker compose up --build -d --wait` |
| [oban-consumer](oban-consumer/) | HTTP → Oban | Elixir | docker, kind, kubectl | `./run.sh` |
| [fleet-e2e](fleet-e2e/) | RabbitMQ, two nodes behind nginx (or, from the Helm chart, on kind) | TypeScript (Postgres) | docker (kind, kubectl, helm for `./kind.sh`) | `./run.sh` |
| [jetstream-k8s](jetstream-k8s/) | NATS JetStream, two autoscaling fleets (kind + Helm + Istio) | TypeScript | docker, kind, kubectl, helm | `./run.sh` |

`quickstart`, `http-consumer`, and `nats-consumer` run the published image.
`rabbitmq-consumer`, `kafka-sqs-consumer`, and `oban-consumer` build their
ingest app from this repo's Elixir source, so you can see the library in use.
`fleet-e2e` builds the server image from this checkout; `jetstream-k8s` does
too unless you set `ANKUSA_IMAGE` to a published tag. `event-routing` builds the
server image from this checkout too, because it uses keys newer than the last
release; `ANKUSA_IMAGE` replaces that build with a published tag that has them.

## quickstart

The published image produces each hook to a Kafka topic on Redpanda, a
Redpanda Connect bridge carries the records into an SQS FIFO queue in floci,
and a Python worker polls the queue with `boto3`, using the `ankusa` Python SDK
([`packages/sdk-python`](../packages/sdk-python)) to decode each message and
redeem a large body from the node's claim-check gateway. No object store. Keep
it as the shape to copy when your workers consume from a queue.

![curl posts to ankusa on 4000, which fsyncs the hook, answers 201 and produces it to ankusa.events; a Redpanda Connect bridge moves records into the ankusa-worker.fifo queue, and worker.py long-polls it, redeems bodies over 8 KiB at the claim check on 4001 and dead-letters permanent failures.](../docs/diagrams/example-quickstart.png)

[`quickstart/`](quickstart/)

## http-consumer

The smallest HTTP deployment: the published image receives a hook and POSTs
it to a small [FastAPI](https://fastapi.tiangolo.com/) worker built with `uv`,
using the `ankusa` Python SDK ([`packages/sdk-python`](../packages/sdk-python))
to parse the delivery's headers. No broker, no object store. Keep it as the
shape to copy when you write your own receiving service.

![curl posts to ankusa on 4000, which fsyncs the hook and answers 201; the HTTP sink posts the raw body with x-ankusa-* headers to worker.py's /hooks, retries six times, then dead-letters the hook, which you replay through the admin API on 4002.](../docs/diagrams/example-http-consumer.png)

[`http-consumer/`](http-consumer/)

## rabbitmq-consumer

Ingest publishes to a RabbitMQ exchange and a TypeScript worker declares and
binds its own queue: the consumer owns the topology, and the framework never
touches a queue. A body over the sink's `inline_max_bytes` is checked in to S3
and the message carries a claim ref URN, which the worker redeems through the
claim-check gateway.

![The ingest node fsyncs each hook, answers 201 and publishes it to the ankusa.events topic exchange; the TypeScript worker declares and binds its own queue and redeems bodies over 8 KiB at the claim-check gateway on 4001 without holding storage credentials.](../docs/diagrams/example-rabbitmq-consumer.png)

[`rabbitmq-consumer/`](rabbitmq-consumer/)

## kafka-sqs-consumer

The same topology one transport over, plus the hop RabbitMQ does not need: a
Redpanda Connect bridge carries records from a Kafka topic into an SQS FIFO
queue with `message_group_id` set from the Kafka key (`tenant/source`), so one
source's records land in one group. Its README documents three failure drills:
bridge down, worker down, poison claim.

![Ingest fsyncs each hook and produces it keyed tenant/source to ankusa.events; Redpanda Connect copies records into ankusa-worker.fifo with the Kafka key as group and ankusa_id as dedup id, and the worker long-polls, redeems claims and dead-letters permanent failures, while redrive catches the rest after 5 receives.](../docs/diagrams/example-kafka-sqs-consumer.png)

[`kafka-sqs-consumer/`](kafka-sqs-consumer/)

## event-routing

One source routed by what each hook says it is, with no code: the source reads
an event type from the JSON body (`event_type: {json: type}`), a Kafka sink names
its topic by it (`topic: "events.{{ event_type ?? other }}"`), and an HTTP sink
gets only `invoice.paid` (`filter: {event_type: [invoice.paid]}`). A TypeScript
worker consumes both topics, and a TypeScript receiver verifies the HTTP
deliveries' signatures. `run.sh` posts three hooks and asserts where each
landed, including a `201 …"filtered":true` for a hook that no sink selects.

![A hook to /webhooks/billing has its event_type read once; the Kafka sink renders its topic from a template (events.invoice.paid, else events.other) and the HTTP sink's filter passes only invoice.paid to the receiver, while an unselected hook gets 201 filtered:true.](../docs/diagrams/example-event-routing.png)

[`event-routing/`](event-routing/)

## nats-consumer

The published image publishes each hook to a NATS JetStream subject, and a Rust
worker built on the published `ankusa` crate pulls them through a durable
consumer. The worker creates its own stream, redeems bodies over
`inline_max_bytes` through the claim-check gateway with `ClaimCheckClient`,
dedupes on the hook id, `nak`s a failure a retry can fix (the gateway
unreachable), and `term`s one it cannot (bad JSON, a sha256 mismatch). No object
store: the claim-check gateway runs on the same node as everything else.

![curl posts to ankusa on 4000, which fsyncs the hook, publishes it to subject ankusa.demo and waits for JetStream's ack; the Rust worker owns the ANKUSA stream and its durable consumer, dedupes on id, redeems large bodies on 4001, and naks or terms failures.](../docs/diagrams/example-nats-consumer.png)

[`nats-consumer/`](nats-consumer/)

## oban-consumer

A real Kubernetes (`kind`) deployment: three self-contained Ankusa nodes, each
with its own on-disk store on a persistent volume, calling the consumer over
HTTP, with Oban doing the actual work. `tools/loadgen` drives three load
phases — steady, chaos with pods killed mid-run, and a closed-loop burst — and
verifies every acknowledged hook is delivered and processed.

![loadgen posts to a three-pod Ankusa StatefulSet whose pods each fsync to their own PVC, then deliver over HTTP to two consumer replicas that record the hook and enqueue an Oban job in one Postgres transaction; loadgen.verify checks every acked id was processed.](../docs/diagrams/example-oban-consumer.png)

[`oban-consumer/`](oban-consumer/)

## fleet-e2e

The multi-node proof in plain Compose, run nightly in CI: nginx round-robins
over two Ankusa nodes that keep their own stores and share route and source
definitions (Redis) and a bucket (MinIO, distinct `storage.key_prefix` per
node). A claim-check gateway serves fat bodies to a TypeScript consumer that
records every hook in Postgres, and `run.sh` drives `tools/loadgen` through
steady, chaos (a node killed, a provider retry sent to the other node, the
consumer killed) and burst phases, then verifies that every acknowledged hook
was processed. `--wal dual --kill-node` runs the same drill under the intake log
and restores a replacement node from backup.

![loadgen posts through nginx to two all-role nodes that share Redis definitions and one MinIO bucket under separate prefixes and publish to RabbitMQ; the consumer binds its own queue, redeems large bodies at the claim-check node and upserts processed_webhooks, which loadgen.verify reads.](../docs/diagrams/example-fleet-e2e.png)

[`fleet-e2e/`](fleet-e2e/)

## jetstream-k8s

A `kind` cluster installed with Helm, with an Istio ingress gateway (Envoy)
load-balancing webhooks across two fleets: stateless ankusa pods in direct mode
(`wal.type: none`, HPA on CPU) and a three-pod durable fleet (`wal.type: disk`,
a PVC each). Both publish to a three-node NATS JetStream cluster, and an
autoscaling TypeScript consumer (KEDA on JetStream lag) pulls from it. A Phoenix
LiveView control panel fires synthetic webhooks (rate, burst, payload size),
draws the flow live, tails the last 30 consumed hooks, and breaks things on
request: kill pods, cut a connection with an Istio `AuthorizationPolicy`,
inject `503`s or delay at the gateway. Bodies over the inline limit take the
claim-check path through an S3 emulator. Prometheus scrapes every ankusa pod,
and a provisioned Grafana dashboard shows ingest, store, dispatch, claim-check
and lifecycle metrics at `/grafana/`.

![The control panel posts through the Istio gateway to a direct fleet (wal: none, scaled by HPA, 201 on JetStream's ack) or a durable fleet (wal: disk, 201 after the commit); both publish into the replicated ANKUSA stream, and a KEDA-scaled consumer acks, dead-letters to ANKUSA_DLQ and redeems claims.](../docs/diagrams/example-jetstream-k8s.png)

[`jetstream-k8s/`](jetstream-k8s/)
