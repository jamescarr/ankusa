# Deployment

See [`architecture.md#deployment-topologies`](architecture.md#deployment-topologies)
for the shapes this section explains how to actually run. Every topology
below runs the same image, `jamescarr/ankusa:edge`; which parts of the pipeline
a container runs is decided at startup.

## Roles and topologies

Four roles, `edge` (ingest), `dispatch` (delivery), `storage` (compaction and
archiving), and `claim_check` (the large-payload gateway), chosen per container
with `ANKUSA_ROLES`:

```sh
docker run -e ANKUSA_ROLES=edge ...              jamescarr/ankusa:edge   # ingest only
docker run -e ANKUSA_ROLES=dispatch,storage ...  jamescarr/ankusa:edge   # delivery + archiving
docker run -e ANKUSA_ROLES=claim_check ...       jamescarr/ankusa:edge   # large-payload gateway only
```

One image, many deployments: *which* children start is a runtime config
decision, never a build-time one. Embedding the library instead?
[`elixir.md#roles-from-code`](elixir.md#roles-from-code) shows the same switch
inside your own supervision tree.

**`:claim_check` is a fourth, opt-in role**, absent from the default
`roles` list (`[:edge, :dispatch, :storage]`) because it opens a port that
serves stored payloads. A node running it alone needs no store, only
blob-store credentials, and can be scaled independently
from ingest/dispatch/storage exactly like any other role. It does no
authentication: put a proxy, mesh, or network policy in front of it. See
[`claim-check.md`](claim-check.md) for the full contract and the worked
`examples/rabbitmq-consumer/` deployment (an `ingest` service plus a
separate `claim-check` service, same image, different `ANKUSA_ROLES`).

**Important constraint:** the queue is a RocksDB store on the node's own disk,
so **every role that reads or writes hooks must run in one BEAM node**: that is
the topology a single container or `mix run` gets you. Splitting `edge`,
`dispatch`, and `storage` across processes, containers, or hosts is not
supported: there is no network-reachable store to point them at. The constraint
does not apply to `:claim_check`, which only reads the blob store and can run
on its own node.

This whole paragraph is about `wal.type: disk`, the default. Under
`wal.type: none` no hook is committed: the node runs the `edge` role with the
queue's readers (`dispatch`, `storage`) dropped from `roles` automatically
(`GET /health` on the admin port shows the effective list), needs no queue
volume and no `StatefulSet`, and replicas are freely interchangeable. See
[`architecture.md#4-stateless-ingest-fleet-wal-none`](architecture.md#4-stateless-ingest-fleet-wal-none)
and [`delivery.md#direct-mode`](delivery.md#direct-mode).

**`:dispatch` and `:storage` are singletons per node.** Within a node there is
exactly one `Ankusa.Dispatch.Pipeline` and one `Ankusa.Storage.Compactor`
reading that node's store. Two dispatch pipelines over the same store would
claim and deliver every hook twice, and two compactors would pack the same
ranges into the same segment keys. Scale out by adding whole nodes, not by
adding dispatch or storage replicas. The archive's catalogue rows live in the
same store and its segments in the blob store (`segments/` for LocalFS), so the
node's data directory needs a persistent volume.

## Running the container

| Port | What | Who can reach it |
| --- | --- | --- |
| 4000 | ingest | publish it: providers post here |
| 4001 | claim check gateway (`claim_check` role) | your own proxy or network policy |
| 4002 | admin API + `/metrics` | your own proxy or network policy |
| 4003 | route management API (`routes.admin.ip`, default `127.0.0.1`; only when `routes.enabled`) | your own proxy or network policy |

Every surface is on its own port so it can be firewalled on its own. 4001/4002/4003 listen on `127.0.0.1` by default; inside a container set `ANKUSA_ADMIN_IP` / `ANKUSA_CLAIM_CHECK_IP` (or `admin.ip` / `claim_check.ip`, and `routes.admin.ip` for 4003) to `0.0.0.0` before a published port can reach them.

`/var/lib/ankusa` holds the node's store (`store/`: hooks, delivery rows, the
quarantine pen, API-managed sources, rate-limit overrides, the archive
catalogue) and the local archive (`segments/`, unless you moved segments to
S3/GCS). Losing it loses un-dispatched hooks and the local archive, so give it
a volume and back it, or move segments to S3/GCS, where they are not your
problem anymore. Under `wal.type: none` no hook is committed, but the store
directory still holds the quarantine pen, API-managed sources and rate-limit
overrides, so mount a volume if you use any of those; without one they are lost
on restart. A full volume fails commits with `503 store_unavailable` and acks
nothing, and ingest resumes with no restart once space frees. A write the store
refuses, from any process, also asks it to reopen itself (at most every 5 s),
which clears a latched RocksDB write error if one is left.

Config lives at `/etc/ankusa/ankusa.yml` (mount yours over it) or wherever
`ANKUSA_CONFIG` points. Every key, plus the env overrides:
[`configuration.md`](configuration.md).

**Building from source.** Core compiles `rocksdb`, a NIF, on
`mix deps.compile`, so the build environment needs cmake >= 3.12, a C++20
compiler, and zstd + OpenSSL development headers (Ubuntu: `libzstd-dev`;
Alpine: `build-base cmake git linux-headers openssl-dev zstd-dev`). The
`jamescarr/ankusa` image installs those and builds RocksDB in its own cached
layer; you only meet this if you build your own image or run core from source.

**Shutdown and backups.** Stop the node with SIGTERM (`docker stop` sends it):
the supervisor lets `Ankusa.Store` close the database cleanly. SIGKILL is safe
for data — the store recovers from its write-ahead log and loses no acked hook
— but a VM killed under write load with the database open can segfault on the
way out. To back up, stop the node and copy the whole `<data_dir>`;
`segments/` is immutable and can be copied while it runs. If segments live in
S3/GCS, the store directory is all you need.

**Liveness and readiness are two probes.** `GET /health` says the process
answers. `GET /ready` says this node can take a hook: its store accepted a
synced write in the last second and no write has failed in the last 5 s
(`200`, `{"status":"ready","store":"ok",…}`), or not (`503`,
`Retry-After: 1`, `store` = `write_failed` or
`store_unavailable` — a full volume, a latched RocksDB error, a store that is
reopening). A node with no store (`wal.type: none`, a gateway) is ready while
it runs (`store: "none"`). Both
are on the ingest port and on the admin port. Point a Kubernetes
`livenessProbe` at `/health` and the `readinessProbe` at `:4000/ready` (a
probe from outside the pod hits the pod address, and `:4002` binds loopback
unless `admin.ip` says otherwise), so a node with a full disk leaves the
load balancer instead of being restarted in a loop. The image's
`HEALTHCHECK` runs `docker-entrypoint healthcheck`, which asks `/ready` on the
ingest port and falls back to the admin port for a node without `:edge`, so
`docker ps` shows `unhealthy` while the store refuses writes and `healthy`
again once space frees.

**Attaching to a running node.** `docker exec -it <ctr> docker-entrypoint
remote` opens an IEx shell inside the node; `docker exec <ctr>
docker-entrypoint rpc 'IO.inspect(Ankusa.Health.ready(:default))'` evaluates
one expression. Distribution is on, and both it and epmd listen on
`127.0.0.1` only, so nothing outside the container's network namespace can
connect. The release cookie is baked into the image, so anything that shares
that namespace (a sidecar in the same pod) and knows the image could attach:
set `RELEASE_COOKIE` per deployment, or `RELEASE_DISTRIBUTION=none` to turn
distribution off (and lose `remote`/`rpc`).

Both compose files are worked examples:

- Single node: [`packages/ankusa_server/compose/docker-compose.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/compose/docker-compose.yml)
- All-role node behind nginx basic auth: [`packages/ankusa_server/compose/docker-compose.proxy.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/compose/docker-compose.proxy.yml)

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
docker compose -f packages/ankusa_server/compose/docker-compose.proxy.yml up -d --wait
```

One all-role node behind nginx, with ingest open and the admin API behind basic
auth. To scale, run N independent nodes like it behind your load balancer: each
node runs every role, keeps its own data volume and queue, and exposes its own
DLQ/admin API: a hook lands on one node and stays there.

Two things to get right once you run more than one node:

- **Every queue role in one node.** The store is local to a BEAM node, so
  `edge`, `dispatch`, and `storage` cannot be split across hosts (see above).
  `:claim_check` is the exception and can run anywhere.
- **Distinct segment keys per node.** Segment keys are
  `seg/<first_seq>-<last_seq>.seg` and remote blob stores ignore the
  instance, so nodes writing the same keys overwrite each other's segments.
  Give each node its own `storage.key_prefix` (`node-a/`, `node-b/`; env
  `ANKUSA_STORAGE_KEY_PREFIX`) and they can share one bucket — or give each
  its own bucket or LocalFS directory. Claims are never prefixed (claim ids
  are unique across nodes), so one claim-check gateway serves the claims of
  every node; see [`claim-check.md`](claim-check.md#a-dedicated-claim-store).

If per-node state is what you want to get rid of, `wal.type: none` moves the
durable copy to the broker: replicas then share no queue at all — no hook
store, no bucket, no DLQ — and you scale them like any stateless web app. The
costs are in [`delivery.md#direct-mode`](delivery.md#direct-mode): no retry
policy, no replay, and the provider must retry on `503`.

You'd need a load balancer in front of the ingest port at that point; that's
a deployment concern the framework doesn't solve for you (nothing in
`ankusa`'s job description is "be a load balancer").

### Dispatch throughput

`Ankusa.Dispatch.Pipeline` delivers up to `dispatch.concurrency` hooks at once
(default 32), each in its own task. Deliveries are not ordered: two hooks for
one sink may run concurrently, or finish in either order. A retry does not sit
in a slot: the row is written back with a due time of `now + backoff`, the slot
is freed, and the retry returns later, so one slow destination or one failing
hook no longer stalls the rest of the instance.

Throughput is therefore bounded by `dispatch.concurrency`, not by sink
latency, and `dispatch.max_inflight` / `dispatch.max_inflight_bytes` bound how
much claimed, unfinished work a stalled destination can hold. Raise
`concurrency` to push more requests at the destination: for `Sink.Http`, keep
Req's Finch pool (default 50 connections) at least that large, or deliveries
queue on pool checkout.

Measured numbers from a full ingest → dispatch → consumer run are recorded in
[`testing.md`](testing.md#load-and-end-to-end-kind--oban).

## Upgrading from 0.3

0.3 kept hand-rolled on-disk formats — `wal/`, `dlq/`, `quarantine/`,
`segments/index.log`, `sources.json`, `rate_limits.json`. The first boot of the
new store imports them into `<data_dir>/<instance>/store`, in this order:
`sources.json`, `rate_limits.json`, `quarantine/`, `wal/`, `dlq/`,
`segments/index.log`.

Each artifact is imported, a marker is written to the store, and then it is
renamed `<name>.migrated-<unix seconds>` — never deleted. The renamed files are
the rollback: stop the node, rename them back, and start 0.3 again. That only
works before hooks are acked into the store: a hook committed after the first
new boot is invisible to 0.3, so a rollback loses it.

Stop the 0.3 node before starting the new one — two nodes must not run against
one data directory — and back the directory up first. The import reads a WAL
that could be large, but in chunks, so memory stays bounded. A torn final WAL
frame (a write that was never acked) is ignored. Dead letters import as dead
rows and replay to the source's current sinks; the next `seq` is past
everything imported.

Under `wal.type: none` the queue artifacts (`wal/`, `dlq/`,
`segments/index.log`) are left in place with a warning and imported later, once
the node runs `wal.type: disk`; everything else imports on first boot.

Two 0.3 config keys now fail as unknown keys: `dispatch.poll_ms` (dispatch no
longer polls — a commit wakes it, and it otherwise sleeps until the earliest
due row) and an HTTP sink's `ordered` (deliveries are not ordered). In core,
`wal: {Ankusa.WAL.DiskLog, _}` raises `ArgumentError` with a hint to use
`wal: :disk`; any other value but `:disk` or `:none` raises too.

## When a node refuses to start

The store never treats a database it cannot read as empty. Two refusals name
the file and what to do:

- **`{:store_open_failed, path, reason}`** — RocksDB damage before the torn
  tail, or an unreadable directory. Check the disk; restore the volume from
  backup; or move the data directory aside to start with an empty store,
  losing everything in it.
- **`{:damaged_legacy_wal, path, byte, later_byte}`** / **`{:corrupt_legacy_sidecar, path}`** —
  a 0.3 WAL with valid frames after the damage, or an unreadable `.cursors` or
  `.truncated` sidecar. Acked hooks after the damage would be lost, so the
  node will not guess. Move the 0.3 directory aside to start without it.

## Worked examples

Four runnable topologies, HTTP, RabbitMQ, Kafka → SQS FIFO, and Oban on
Kubernetes, each with its own README and failure drills:
[`examples/README.md`](https://github.com/jamescarr/ankusa/blob/main/examples/README.md).
They are also the fastest way to see how much of a real deployment is Ankusa
config and how much is your worker.
