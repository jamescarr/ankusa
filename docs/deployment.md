# Deployment

See [`architecture.md#deployment-topologies`](architecture.md#deployment-topologies)
for the four shapes this section explains how to actually run. Every topology
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
blob-store credentials, plus `claim_check.api_tokens` if you want bearer
tokens rather than your own proxy in front — and can be scaled independently
from ingest/dispatch/storage exactly like any other role. See
[`claim-check.md`](claim-check.md) for the full contract and the worked
`examples/rabbitmq-consumer/` deployment (an `ingest` service plus a
separate `claim-check` service, same image, different `ANKUSA_ROLES`).

**Important constraint:** splitting roles across different *processes on
one host* works with any WAL, because they can share a local disk path.
Splitting roles across different *machines* requires a WAL every role can
reach over the network — that's `WAL.Postgres` (see
[`storage.md`](storage.md)), not `WAL.DiskLog`. This constraint doesn't
apply to `:claim_check`, which never touches the WAL at all.

**`:dispatch` and `:storage` are singletons per instance.** Neither cursor
has a lease. Two `:dispatch` nodes on one `WAL.Postgres` deliver every hook
twice, and two `:storage` nodes compact the same ranges and write duplicate
index rows. Scale `:edge` horizontally; run `:dispatch` and `:storage` as
exactly one replica each (in Kubernetes, a 1-replica StatefulSet). Also note
that `Ankusa.Storage.Index` lives on the `:storage` node's local disk
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
it — or move the WAL to Postgres and segments to S3/GCS, where it is not your
problem anymore.

Config lives at `/etc/ankusa/ankusa.yml` (mount yours over it) or wherever
`ANKUSA_CONFIG` points — every key, plus the env overrides:
[`configuration.md`](configuration.md).

The image reports `healthy` via `:4002/health`, so orchestrators can gate on it
instead of racing the listener.

Both compose files are worked examples:

- Single node: [`ankusa_server/compose/docker-compose.yml`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/compose/docker-compose.yml)
- Fleet behind nginx basic auth: [`ankusa_server/compose/docker-compose.fleet.yml`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/compose/docker-compose.fleet.yml)

```sh
docker compose -f docker-compose.fleet.yml up -d --wait
curl -XPOST localhost:4000/webhooks/demo -d '{"id":"f1"}'    # 201
curl -XPOST localhost:4000/webhooks/demo -d '{"id":"f1"}'    # 200 duplicate
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
docker compose -f ankusa_server/compose/docker-compose.fleet.yml up -d --wait
```

Two `edge` replicas on a shared Postgres WAL, plus one `dispatch,storage`
worker. Add `edge` replicas for ingest capacity — any replica can absorb any
hook, because dedup lives in the shared WAL, not in a node's memory. `dispatch`
and `storage` stay at one replica each (see above): they are singletons. For a
shared WAL across ingest nodes instead of N independent local ones, that is the
`WAL.Postgres` config in [`storage.md`](storage.md).

You'd need a load balancer in front of the ingest port at that point; that's
a deployment concern the framework doesn't solve for you (nothing in
`ankusa`'s job description is "be a load balancer").

### Dispatch throughput

`Ankusa.Dispatch.Pipeline` delivers one envelope at a time and writes the
cursor after each (`drain/1`, `deliver_with_retry/4`). A retrying sink
therefore blocks the instance's whole pipeline, and throughput is bounded by
sink latency — there's no concurrent dispatch (see
[`delivery.md`](delivery.md)).
Measured numbers from a full ingest → dispatch → consumer run are recorded
in [`testing.md`](testing.md#load-and-end-to-end-kind--oban), including an
open finding: killing the singleton `:dispatch`/`:storage` node mid-load can
rarely drop a hook permanently — see that section's "Known issue" for what's
been ruled out and what hasn't.

## Worked examples

Four runnable topologies — HTTP, RabbitMQ, Kafka → SQS FIFO, and Oban on
Kubernetes — each with its own README and failure drills:
[`examples/README.md`](https://github.com/jamescarr/ankusa/blob/main/examples/README.md).
They are also the fastest way to see how much of a real deployment is Ankusa
config and how much is your worker.
