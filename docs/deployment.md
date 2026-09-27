# Deployment

See [`architecture.md#deployment-topologies`](architecture.md#deployment-topologies)
for the shapes this section explains how to actually run. Every topology
below runs the same image, `jamescarr/ankusa:edge`; which parts of the pipeline
a container runs is decided at startup.

## Roles and topologies

Four roles — `edge` (ingest), `dispatch` (delivery), `storage` (compaction and
archiving), and `claim_check` (the large-payload gateway) — chosen per container
with `ANKUSA_ROLES`:

```sh
docker run -e ANKUSA_ROLES=edge ...              jamescarr/ankusa:edge   # ingest only
docker run -e ANKUSA_ROLES=dispatch,storage ...  jamescarr/ankusa:edge   # delivery + archiving
docker run -e ANKUSA_ROLES=claim_check ...       jamescarr/ankusa:edge   # large-payload gateway only
```

One image, many deployments — *which* children start is a runtime config
decision, never a build-time one. Embedding the library instead?
[`elixir.md#roles-from-code`](elixir.md#roles-from-code) shows the same switch
inside your own supervision tree.

**`:claim_check` is a fourth, opt-in role**, absent from the default
`roles` list (`[:edge, :dispatch, :storage]`) because it opens a port that
serves stored payloads. A node running it alone needs no WAL — only
blob-store credentials — and can be scaled independently
from ingest/dispatch/storage exactly like any other role. It does no
authentication: put a proxy, mesh, or network policy in front of it. See
[`claim-check.md`](claim-check.md) for the full contract and the worked
`examples/rabbitmq-consumer/` deployment (an `ingest` service plus a
separate `claim-check` service, same image, different `ANKUSA_ROLES`).

**Important constraint:** `WAL.DiskLog` keeps its record index in-process and
reclaims space by renaming the log file, so **every role that touches the WAL
must run in one BEAM node** — that is the topology a single container or
`mix run` gets you. Splitting `edge`, `dispatch`, and `storage` across
processes, containers, or hosts is not supported: there is no network-reachable
WAL to point them at, and a second OS process would never see writes it didn't
make. The constraint does not apply to `:claim_check`, which never touches the
WAL at all and can run on its own node.

**`:dispatch` and `:storage` are singletons per node.** Within a node there is
exactly one `Ankusa.Dispatch.Pipeline` and one `Ankusa.Storage.Compactor`
reading that node's WAL, each with an unleased cursor. Two dispatch pipelines
over the same WAL would deliver every hook twice, and two compactors would
compact the same ranges and write conflicting index rows. Scale out by adding
whole nodes, not by adding dispatch or storage replicas. Also note that
`Ankusa.Storage.Index` lives on the `:storage` node's local disk
(`segments/index.log`), which needs a persistent volume.

## Running the container

| Port | What | Who can reach it |
| --- | --- | --- |
| 4000 | ingest | publish it: providers post here |
| 4001 | claim check gateway (`claim_check` role) | your own proxy or network policy |
| 4002 | admin API + `/metrics` | your own proxy or network policy |

Every surface is on its own port so it can be firewalled on its own.

`/var/lib/ankusa` holds the WAL, the quarantine log, the dead-letter queue, and
local segments. Losing it loses un-dispatched hooks, so give it a volume and back
it — or move segments to S3/GCS, where they are not your problem anymore.

Config lives at `/etc/ankusa/ankusa.yml` (mount yours over it) or wherever
`ANKUSA_CONFIG` points — every key, plus the env overrides:
[`configuration.md`](configuration.md).

The image reports `healthy` via `:4002/health`, so orchestrators can gate on it
instead of racing the listener.

Both compose files are worked examples:

- Single node: [`ankusa_server/compose/docker-compose.yml`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/compose/docker-compose.yml)
- All-role node behind nginx basic auth: [`ankusa_server/compose/docker-compose.proxy.yml`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/compose/docker-compose.proxy.yml)

```sh
docker compose -f docker-compose.proxy.yml up -d --wait
curl -XPOST localhost:4000/webhooks/demo -d '{"id":"f1"}'    # 201
curl localhost:4002/v1/dlq                                   # 401 (nginx)
curl -u admin:change-me localhost:4002/v1/dlq                # {"total":0,"entries":[]}
```

Image tags:

| Tag | Means |
| --- | --- |
| `X.Y.Z` | an exact release (`ankusa_server-vX.Y.Z` in the repo) |
| `X.Y` | the newest release in that minor line |
| `X` | the newest release in that major line (from 1.0 on) |
| `latest` | the newest release |
| `edge` | the tip of `main`; not a release, may be broken |

`linux/amd64` and `linux/arm64`. Until the first release, use `edge`.

## Scaling the ingest fleet

```sh
docker compose -f ankusa_server/compose/docker-compose.proxy.yml up -d --wait
```

One all-role node behind nginx, with ingest open and the admin API behind basic
auth. To scale, run N independent nodes like it behind your load balancer: each
node runs every role, keeps its own data volume and WAL, and exposes its own
DLQ/admin API — a hook lands on one node and stays there.

Two things to get right once you run more than one node:

- **Every WAL role in one node.** `WAL.DiskLog` is local to a BEAM node, so
  `edge`, `dispatch`, and `storage` cannot be split across hosts (see above).
  `:claim_check` is the exception and can run anywhere.
- **Its own bucket per node.** Segment keys are `seg/<first_seq>-<last_seq>.seg`
  and remote blob stores ignore the instance, so nodes sharing one bucket
  overwrite each other's segments. Give each node its own bucket (or its own
  LocalFS directory).

You'd need a load balancer in front of the ingest port at that point; that's
a deployment concern the framework doesn't solve for you (nothing in
`ankusa`'s job description is "be a load balancer").

### Dispatch throughput

`Ankusa.Dispatch.Pipeline` delivers up to `dispatch.concurrency` envelopes at
once (default 32), each in its own task, and moves its durable cursor as a
**watermark**: it never advances past an envelope that isn't fully handled.
Deliveries to the same sink with an equal `c:Ankusa.Sink.ordering_key/2` run one
at a time, in `seq` order; different keys run concurrently, so one slow
destination or one retrying envelope no longer stalls the whole instance.

Throughput is therefore bounded by `dispatch.concurrency`, not by sink
latency, and `dispatch.max_inflight` / `dispatch.max_inflight_bytes` bound how
much admitted-but-unfinished work a stalled destination can hold. Raise
`concurrency` to push more requests at the destination — for `Sink.Http`, keep
Req's Finch pool (default 50 connections) at least that large, or deliveries
queue on pool checkout.

Measured numbers from a full ingest → dispatch → consumer run are recorded in
[`testing.md`](testing.md#load-and-end-to-end-kind--oban).

## Worked examples

Four runnable topologies — HTTP, RabbitMQ, Kafka → SQS FIFO, and Oban on
Kubernetes — each with its own README and failure drills:
[`examples/README.md`](https://github.com/jamescarr/ankusa/blob/main/examples/README.md).
They are also the fastest way to see how much of a real deployment is Ankusa
config and how much is your worker.
