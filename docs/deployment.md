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
| 4004 | metrics listener: only `/metrics`, `/health`, `/ready` (`metrics.enabled`, default off; `metrics.ip`, default `127.0.0.1`) | your Prometheus / kubelet network rule |

Every surface is on its own port so it can be firewalled on its own. 4001/4002/4003/4004 listen on `127.0.0.1` by default; inside a container set `ANKUSA_ADMIN_IP` / `ANKUSA_CLAIM_CHECK_IP` / `ANKUSA_METRICS_IP` (or `admin.ip` / `claim_check.ip` / `metrics.ip`, and `routes.admin.ip` for 4003) to `0.0.0.0` before a published port can reach them. Every listener, ingest included, can terminate TLS, and demand a client certificate (mTLS) with `verify: verify_peer`: see [Listeners: limits, TLS and mTLS](configuration.md#listeners-limits-tls-and-mtls). The admin, route-management and claim-check APIs authenticate their callers (`admin.auth`, `routes.admin.auth`, `claim_check.auth`: a bearer token, an OIDC access token, or a client certificate, each with scopes): off loopback they refuse to boot with `mode: none` unless `allow_insecure: true` says something in front authenticates every caller. The image takes `ANKUSA_ADMIN_TOKEN` / `ANKUSA_CLAIM_CHECK_TOKEN` / `ANKUSA_ROUTES_ADMIN_TOKEN` for the simple case; see [Control-plane authentication](configuration.md#control-plane-authentication).

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
way out. For a host you can lose, turn on [`backup`](configuration.md#backup)
with segments (or `backup.store`) in a bucket: the node uploads a checkpoint
every `backup.interval_ms` (60 s) while it runs, and a replacement started on
an empty volume with the same config (same `storage.key_prefix`) restores the
latest one before it accepts a hook — losing at most the hooks acked since that
backup. A node that cannot read the backup location refuses to start rather
than start empty; watch `ankusa_backup_age_seconds`. Details:
[`storage.md`](storage.md#backup-and-restore). Without `backup`, stop the node
and copy the whole `<data_dir>`; `segments/` is immutable and can be copied
while it runs. If segments live in S3/GCS, the store directory is all you need.

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
ingest port and falls back to the admin port, then the metrics port, for a node
without `:edge` or whose ingest listener terminates TLS (the probe is plain
HTTP), so
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
curl -u admin:change-me localhost:4002/v1/dlq                # {"total":0,"entries":[]} (nginx adds the node's token)
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

## Upgrading between releases

The store carries a **format version** (`m:format_version` in its `default`
column family; this release writes format 1). A store without the key is
format 1 — every store written before the marker existed — and is stamped on
its first open. A release that changes the key layout adds a migration to the
store's migration registry; on boot, before the node serves, the
migrations from the store's format up to the release's run in order, each in
bounded synced batches. A migration records its place in `m:migrating:<to>`
with every batch, so a node killed mid-migration resumes at the next boot
instead of starting over, and the new format is stamped only in the batch that
finishes it. Migrations are forward-only.

Preview what a boot would do, without opening the store (read-only, safe
beside a running node):

```sh
mix ankusa.store.migrate --dry-run --data-dir /var/lib/ankusa --instance default
```

It prints the store's format, the migrations still to run and, for an
interrupted one, the cursor it resumes at. Without `--dry-run` the task
migrates the store and exits; stop the node that owns it first.

Upgrade in this order:

1. **Back up.** Let a backup complete (`backup.enabled`) or copy the store
   directory while the node is stopped. This is the rollback.
2. **One pod at a time.** Replace a node, wait for `/ready`, then the next.
   Each node migrates its own store; nodes share no store. N and N−1 read each
   other's data within a minor release, so a fleet is safe mid-rollout.
3. **Do not roll back past a format bump.** An older release refuses a newer
   store at boot (`{:store_format_newer, found, supported}`, see below)
   rather than ignore the keys it does not know. To go back, stop the node and
   restore the pre-upgrade backup; the hooks acked after the upgrade are in
   the newer format and are lost with it, so roll forward when you can.

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

## Rolling back a minor

Hooks and delivery rows are stored in a versioned encoding (v0 = the bare Erlang
term, v1 = a two-byte tag and a version byte in front of it; see
[`storage.md`](storage.md#encoding-and-versions)). That version is separate
from the store's format version in
[Upgrading between releases](#upgrading-between-releases), which the boot
guard checks. A release reads the versions it knows, and a release only starts
*writing* a version once the release before it can read it. That makes a
rollback of one minor safe, and puts the boundary here:

| Running | Reads | Writes | Roll back to |
| --- | --- | --- | --- |
| 0.6 and earlier | v0 | v0 | n/a |
| the first release with versioned encodings | v0, v1 | v0 | 0.6: safe, every row it wrote is v0 |
| the release after it | v0, v1 | v1 | the first release with versioned encodings: safe; 0.6: **not supported** |

0.6 reads a v1 row with a bare `binary_to_term` and raises, so once a node has
run the release that writes v1, do not roll back past the release that reads
it. Upgrade 0.6 to the release that writes v1 *through* the release that
reads it, never directly. If a node must go below the boundary anyway, restore
the data directory from a backup taken before the first v1 write; the hooks
acked after it are lost with it. The change that flips the write default to v1
bumps the store format in the same release, so the guard refuses the older
image at boot (`{:store_format_newer, found, supported}`) instead of that
release raising on rows one at a time.

## When a node refuses to start

The store never treats a database it cannot read as empty. These refusals name
the file or version and what to do:

- **`{:store_open_failed, path, reason}`** — RocksDB damage before the torn
  tail, or an unreadable directory. Check the disk; restore the volume from
  backup; or move the data directory aside to start with an empty store,
  losing everything in it.
- **`{:damaged_legacy_wal, path, byte, later_byte}`** / **`{:corrupt_legacy_sidecar, path}`** —
  a 0.3 WAL with valid frames after the damage, or an unreadable `.cursors` or
  `.truncated` sidecar. Acked hooks after the damage would be lost, so the
  node will not guess. Move the 0.3 directory aside to start without it.
- **`{:corrupt_legacy_wal_record, path, seq, reason}`** — a 0.3 WAL frame that
  passes its checksum but is no envelope. The node will not guess; move the
  0.3 directory aside to start without it.
- **`{:store_format_newer, found, supported}`** — the store was written by a
  newer release (`found`) than this one understands (`supported`), typically
  after a rollback. Run the newer release again, or stop the node and restore
  the backup taken before the upgrade (see
  [Upgrading between releases](#upgrading-between-releases)). The node
  imports and migrates nothing before refusing.
- **`{:store_migration_failed, to, reason}`** / **`{:missing_migration, found, target}`** —
  a format migration raised or returned an error, or this release has no
  migration leaving the store's format. A failed migration resumes at the last
  batch it committed on the next boot; `mix ankusa.store.migrate --dry-run`
  shows where.

## Worked examples

Four runnable topologies, HTTP, RabbitMQ, Kafka → SQS FIFO, and Oban on
Kubernetes, each with its own README and failure drills:
[`examples/README.md`](https://github.com/jamescarr/ankusa/blob/main/examples/README.md).
They are also the fastest way to see how much of a real deployment is Ankusa
config and how much is your worker.
