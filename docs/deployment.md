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
docker run -e ANKUSA_ROLES=edge,dispatch,storage ...  jamescarr/ankusa:edge   # a store-backed node
docker run -e ANKUSA_ROLES=claim_check ...            jamescarr/ankusa:edge   # gateway only
```

The first three share one local store and therefore one BEAM node (see the
constraint below); `claim_check` is the only role that can run as its own
container. Under `wal.type: none`, `edge` alone is a complete node.

One image, many deployments: *which* children start is a runtime config
decision, never a build-time one. Embedding the library instead?
[`elixir.md#roles-from-code`](elixir.md#roles-from-code) shows the same switch
inside your own supervision tree.

**`:claim_check` is a fourth, opt-in role**, absent from the default
`roles` list (`[:edge, :dispatch, :storage]`) because it opens a port that
serves stored payloads. A node running it alone needs no store, only
blob-store credentials, and can be scaled independently
from ingest/dispatch/storage exactly like any other role. It authenticates its
readers (`claim_check.auth`: a bearer token, OIDC access token or client
certificate, holding `claims:read` and the claim's tenant), and `mode: none`
is accepted only on loopback or with `allow_insecure: true`; a proxy or network
policy in front is still a good idea. See
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

This whole paragraph is about the store-backed modes, `wal.type: disk` (the
default) and `dual`. Under
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

**Draining on SIGTERM.** `docker stop` and a Kubernetes pod deletion send
SIGTERM, and the node answers it in order, so a deploy resets no connection:

1. `/ready` answers `503` with `"status": "draining"` (and a `drain` key) at
   once, and the load balancer takes the node out.
2. It keeps serving for `listener.drain_delay_ms` (5 s): a balancer needs a
   probe interval or two to notice, and a request it had already routed must
   find the listener open.
3. The ingest listener closes. It stops accepting and waits for every request
   it is serving, up to the batcher's 15 s record deadline plus the store
   commit and a 5 s margin (22 s; 13 s under `wal.type: none`, 24 s under
   `dual`), so a commit that starts late is not killed mid-fsync. Each request
   is answered `201` (stored) or `503` (refused, nothing stored).
4. It waits until every batcher holds nothing, within `listener.drain_timeout_ms`
   (30 s) from the end of the delay.
5. The tree stops: dispatch writes the outcomes it holds, the backup takes its
   final checkpoint (below), and the store closes.

A drain that cannot finish says so in the log and lets the stop go on. Give the
orchestrator time for all of it: `terminationGracePeriodSeconds` of at least
`(drain_delay_ms + drain_timeout_ms) / 1000 + 10` (45 with the defaults), plus
`backup.final_timeout_ms` when the node backs up, and `docker stop -t` the
same (Docker's default of 10 s is shorter than the drain). Proof:
`test/ankusa/drain_test.exs` (200 requests in flight, SIGTERM, zero resets,
every `201` present after a restart) and the `docker stop` drill of
`mise run docker:smoke`.

**Shutdown and backups.** After the drain the supervisor lets `Ankusa.Store`
close the database cleanly. SIGKILL is safe
for data — the store recovers from its write-ahead log and loses no acked hook
— but a VM killed under write load with the database open can segfault on the
way out. For a host you can lose, turn on [`backup`](configuration.md#backup)
with segments (or `backup.store`) in a bucket: the node uploads a checkpoint
every `backup.interval_ms` (60 s) while it runs, and a replacement started on
an empty volume with the same config (same `storage.key_prefix`) restores the
latest one before it accepts a hook — losing at most the hooks acked since that
backup, and none after a clean stop (SIGTERM takes one last backup, bounded by
`backup.final_timeout_ms`). A node that cannot read the backup location, or
whose backup fails its checks, refuses to start rather than start empty; set
`backup.signing_key` so a restore only accepts manifests you signed,
`backup.secondary` for a second region, and watch `ankusa_backup_age_seconds`
and `ankusa_backup_verify_failed_total` (`backup.verify_interval_ms`). Details:
[`storage.md`](storage.md#backup-and-restore). Without `backup`, stop the node
and copy the whole `<data_dir>`; `segments/` is immutable and can be copied
while it runs. If segments live in S3/GCS, the store directory is all you need.

**Liveness and readiness are two probes.** `GET /health` says the process
answers. `GET /ready` says this node can take a hook, by running every check
that applies to its config and listing each one's state:

```json
{"status": "ready", "instance": "default", "store": "ok", "backlog": "failing",
 "reasons": {"backlog": "over_threshold"}}
```

A state is `ok`, `failing` or `draining`; `reasons` appears only while a check
is not `ok` and names why, from a fixed set. A check that does not apply (no
store, no threshold set, another `wal` mode) has no key. The node answers `503`
(`status: "unavailable"`, `Retry-After: 1`; `"draining"` once a stop has begun) when a check that **gates** is not
`ok`, `200` otherwise:

| Check | Applies when | Failing means (`reasons.<check>`) | `503`? |
| --- | --- | --- | --- |
| `drain` | the node is being stopped (SIGTERM) | `draining` (reason `shutting_down`): the node is closing its listeners and will exit; `status` is `draining` | yes |
| `store` | the node runs the local store (any `edge`, `dispatch` or `storage` role, the admin API, or a persistent source store: every node but a claim-check-only gateway, `wal.type: none` included) | the store did not take a synced write in the last second, or a write failed in the last 5 s: a full volume, a latched RocksDB error, a store that is reopening. Reason: `no_space`, `io_error`, `read_only`, `corruption`, `busy`, `timeout`, `not_running` or `other` | yes |
| `intake` | `wal.type: dual` | `down` (the breaker is open and the log is not answering) or `recovering` (boot replay is running) | yes |
| `sinks` | `wal.type: none` | `publish_failed`: a sink failed its last publish within `health.sink_error_window_ms` (the provider's retry is the only retry in this mode, so a broker that refuses everything leaves the load balancer). A sink that publishes again clears itself at once | yes |
| `disk` | `node.min_free_bytes` is set | `below_floor` (the file system holding `data_dir` has fewer bytes free; the node drains before the store hits `ENOSPC`) or `unmeasured` (no `:os_mon`, or `df` took over 2 s) | yes |
| `backlog` | `queue.max_oldest_due_ms` is set | `over_threshold`: the oldest due delivery has waited longer. A label, so a node that is behind stays in the load balancer and the alert (`ankusa_queue_oldest_due_age_seconds`) says so | no |
| `backup` | `backup.enabled` and `backup.max_age_ms` | `stale`: the last successful backup is older than `max_age_ms` | only with `backup.require_fresh` |

A node with none of these checks (a claim-check-only gateway: no store)
is ready while it runs. Both probes are on the ingest port and on the admin
port. Point a Kubernetes `livenessProbe` at `/health` and the `readinessProbe`
at `:4000/ready` (a probe from outside the pod hits the pod address, and `:4002`
binds loopback unless `admin.ip` says otherwise), so a node with a full disk
leaves the load balancer instead of being restarted in a loop. The image's
`HEALTHCHECK` runs `docker-entrypoint healthcheck`, which asks `/ready` on the
ingest port and falls back to the admin port, then the metrics port, for a node
without `:edge` or whose ingest listener terminates TLS (the probe is plain
HTTP), so
`docker ps` shows `unhealthy` while the store refuses writes and `healthy`
again once space frees. The body's key names are the ones in the table; only the
status code is for a probe to act on.

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
  `archive/v1/dt=<day>/<node>/seg-<first_seq>-<last_seq>.seg`, where `<node>` is
  `intake.node_id` (else the host name), and remote blob stores ignore the
  instance, so two nodes with one name write the same keys and overwrite each
  other's segments. Give each node its own `intake.node_id` (or its own
  `storage.key_prefix` (`node-a/`, `node-b/`; env `ANKUSA_STORAGE_KEY_PREFIX`))
  and they can share one bucket, and a node that lost its catalogue finds the
  others' segments by day — or give each
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
`egress.pools.pool_size` (default 32, per receiver address) at least that large, or
deliveries to one receiver queue on pool checkout (boot warns when it is smaller).
Pools are bounded per node (`egress.pools.max_pools`, default 1,024) and closed when
idle; keep `max_pools × pool_size` under `ulimit -n`. See
[Outbound connection pools](configuration.md#outbound-connection-pools).

Measured numbers from a full ingest → dispatch → consumer run are recorded in
[`testing.md`](testing.md#load-and-end-to-end-kind--oban).

## Host loss and the intake log

With `wal.type: disk` an acked hook survives its **process** and a power cut, not
its **host**: a lost box loses everything acked since the last backup
(`backup.interval_ms`, 60 s). `wal.type: dual`
([ADR 0002](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0002-dual-commit-intake-log.md)) removes that window: the `201`
waits for the hook to be fsynced here **and** confirmed in a replicated Kafka
topic, the *intake log*. An acked hook is then lost only if the host **and** every
in-sync replica of the log's partition are lost together. The price is a hop and a
dependency: ack latency is the larger of the fsync and the broker confirm, and a
node that cannot confirm in the log refuses (`503 intake_unavailable`) instead of
acking on its disk alone.

**1. Provision the log.** One topic for the fleet (`ankusa.intake.<fleet>`, or
`intake.topic`), created by you; the node never creates or alters one and refuses
to boot on a topic that does not keep the promise:

| Setting | Needs | Why |
| --- | --- | --- |
| replication factor | 3 or more | a confirm is every in-sync replica |
| `min.insync.replicas` | 2 or more | the broker refuses a produce (`not_enough_replicas`) when fewer are in sync, so a confirm is never one copy |
| `unclean.leader.election.enable` | `false` | a replica that missed a confirmed record must never lead |
| `retention.ms` | at least `2 x backup.max_age_ms` (or `-1`) | a restore of a backup as old as `max_age_ms`, even from the secondary bucket, must find its offset in the log |
| `max.message.bytes` | at least `intake.max_record_bytes` (10 000 000) | a hook the node accepts (`http.max_body_bytes` plus framing) must fit |

```sh
kafka-topics.sh --bootstrap-server kafka-1:9092 --create --topic ankusa.intake.default \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=2 \
  --config unclean.leader.election.enable=false --config retention.ms=86400000 \
  --config max.message.bytes=11000000
```

Size it as `records/s x mean hook size x retention` before replication (the plan's
10 KB mean body at 1 000 records/s per node is about 10 MB/s of produce per node,
times the node count, times three for the replicas). The topic holds customer
payloads: give it the confidentiality of the archive bucket (TLS and ACLs between
nodes and brokers, encryption at rest on the brokers' volumes), and zone- or
rack-aware replicas: `acks=all` replicates before it fsyncs, so two brokers
losing power in the same instant lose the record. Each node writes **one
partition** (`phash2(node_id) rem partition_count`, chosen at its first dual boot
and never recomputed, so growing the topic never remaps a node); give the topic at
least as many partitions as you have nodes where you can, and read
[One partition per node](#one-partition-per-node) for what a partition carries.

**2. Configure the nodes.** Every node needs a stable identity, the same on its
replacement (a StatefulSet pod name, an instance tag), the backup (required: it
carries what the log does not, and bounds replay), and a `dedupe:` on every source
(it is what turns the provider's retry of a `503` into the original hook,
[`delivery.md`](delivery.md#idempotent-receivers)):

```yaml
wal: {type: dual}
intake:
  type: kafka
  node_id: "${POD_NAME}"            # or ANKUSA_INTAKE_NODE_ID
  kafka: {brokers: ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]}
backup:
  enabled: true
  interval_ms: 60000
  max_age_ms: 300000
  secondary: {type: s3, s3: {bucket: ankusa-backups-dr, region: eu-west-1}}
```

Boot refuses `dual` without `intake.type`, `intake.node_id`, `backup.enabled` and
`backup.max_age_ms`, and (with `verify_topic`) on a topic that fails the table
above. The startup log says what you have, in one line (`Durable to power loss on
this host AND confirmed in the intake log <topic> (acks=all, min.insync.replicas=2,
replicas=3) before every 2xx: …`). Every key is in
[`configuration.md`](configuration.md).

**3. Watch it.** `GET /ready` reports `intake`: `ok`, or `failing` with
`reasons.intake` `down` (breaker open and the log not answering: the load
balancer drains the node) or `recovering` (boot replay
running). `GET /v1/wal` has an `intake` object (`node`, `partition`, `offset`,
`watermark`, `pending`, `state`). `ankusa_intake_publish_duration_seconds`,
`ankusa_intake_failures_total{class}`, `ankusa_intake_pending_records`,
`ankusa_intake_reconcile_stuck` and `ankusa_intake_ready`; alert on
`reconcile_stuck > 0` (a half-persisted hook the reconciler cannot finish),
`ankusa_intake_ready == 0` for more than a probe interval, and the
`503 intake_unavailable` rate at the edge (the availability cost of the log).

**4. Replace a lost host.** A replacement starts with the **same `intake.node_id`**
on an empty volume and the same config; there is nothing else to do:

1. It restores the latest backup (the primary bucket, then the secondary; the
   manifest signature is checked) and mints a new incarnation.
2. It replays the log for its node id from the backup's `m:intake_offset`: every
   hook acked since the backup is written back under its original seq, once
   (a duplicate or reordered record changes nothing), and the hooks above the
   watermark are republished.
3. It reports ready and serves. Until then `/ready` is 503 (`intake: failing`, reason `recovering`).

Hooks the lost node had already delivered after that backup are delivered again by
the replacement (same hook id, same `idempotency_key`): consumers dedupe, and the
count is bounded by what was acked since the backup. If the log no longer reaches
back to the backup's offset (retention ran out) boot refuses with
`{:intake_truncated, wanted, earliest}`: the hooks in that gap are not
recoverable, and only you decide to go on, with `recover --allow-gap` below.

Do not start a second node with a node id that is still alive: two live nodes on
one id interleave two seq spaces. The replay never overwrites a hook with a
different id (it is written under a fresh seq and logged), and a backup prefix shared
by two stores refuses to back up, but there is no broker-side fence; fence the old
host before the new one starts.

**5. `recover` by hand.** The same code runs against a **stopped** store, for a
node you want recovered before it serves (`intake.recover_on_boot: false`), a dry
run, or a log with a gap:

```sh
docker run --rm -v ankusa-a-data:/var/lib/ankusa -v ./ankusa.yml:/etc/ankusa/ankusa.yml:ro \
  jamescarr/ankusa recover --dry-run                  # read and count, write nothing
docker run --rm … jamescarr/ankusa recover            # restore if the volume is empty, then replay
docker run --rm … jamescarr/ankusa recover --allow-gap   # accept a log that no longer reaches the backup
mix ankusa.recover --instance default --node ankusa-a  # the same, from a Mix project
```

It prints `records=… applied=… present=… collided=… unroutable=… skipped=…
republished=…`, exits `0`, and a second run changes nothing. `--node` asserts the
id, `--from backup|earliest|N` chooses where to start. `--allow-gap` writes an
`intake.recover.allow_gap` row to the node's audit trail (the lost positions, the
node id, the actor) before it goes on.

**6. Going back to `disk`.** A durability downgrade you choose: the promise above
ends from that moment and the startup line changes. Drain the pending set first
(`GET /v1/wal` shows `intake.pending`; the hooks in it are in the store but not
in the log). Going `disk` to `dual` again is safe: boot recovery republishes every
hook above the watermark, so the hooks acked under `disk` become covered at that
boot.

### One partition per node

The plan's rates are fleet-wide: 10⁴–10⁵ req/s over 100 nodes or more, so about
1 000 req/s per node at the 10 KB mean body, or 10 000 at 1 KB, either way about
10 MB/s of produce per node. `packages/ankusa_kafka/bench/partition_bench.exs`
measures one partition of a three-broker, replication-factor-3,
`min.insync.replicas=2` cluster through the adapter at that rate and flat out.
On a laptop (the three brokers in docker beside the bench, so read it as an order of
magnitude): at the per-node rate, 997 records/s of 10 000 bytes (9.5 MiB/s), the
confirm took p50 5.1 ms and p99 17.0 ms; flat out, two publishers with 256-record
batches in flight carried 12 551 records/s (119.7 MiB/s) through **one** partition
at a confirm p50 of 36.9 ms, p99 136.7 ms. One partition per node therefore has
about 12x headroom at the plan's per-node rate, and the explicit partition per
node stands; several nodes may share a partition (the records carry the node id and
replay filters by it) as long as their sum stays well under what it carries.

### Proof

`examples/fleet-e2e/run.sh --wal dual --kill-node` kills `ankusa-a` **and deletes
its volume** under 1 000 req/s with the consumer's broker paused (so the acked hooks
are undelivered), starts a replacement with the same node id, stops the three intake
brokers (every request is `503 intake_unavailable`, none is acked) and then
restores the node from the secondary backup alone with `recover`: `missing=0`,
`duplicate_hooks=0`. `--wal disk --kill-node` runs the same host loss without
the log and must **lose** the hooks acked since the last backup, so the drill
proves the difference. See [`examples/fleet-e2e`](https://github.com/jamescarr/ankusa/tree/main/examples/fleet-e2e/).

## Kubernetes: the Helm chart

[`deploy/helm/ankusa`](https://github.com/jamescarr/ankusa/tree/main/deploy/helm/ankusa)
runs the image in its three durability modes. `wal.type` picks the workload:

| `wal.type` | Workload | Scale |
| --- | --- | --- |
| `disk` (default), `dual` | a **StatefulSet**, one RocksDB store on one PVC per pod (`dual`: `intake.node_id` is the pod name, the identity a replacement keeps) | `replicas`, by hand: a pod holds hooks, so nothing scales it away |
| `none` | a **Deployment** of interchangeable pods, an `emptyDir` for the data directory | an HPA on CPU (and, with a custom-metrics adapter, on `ankusa_queue_pending`) |

```sh
helm install ankusa deploy/helm/ankusa -f my-values.yaml
```

The server's YAML goes under `config:` in your values (merged over what the chart
derives: `wal.type`, `listener.drain_*`, JSON logs, the admin and claim-check
listeners); secrets stay out of it as `${VAR}` references filled from `extraEnv` /
`extraEnvFrom`. `claimCheck.enabled` adds the gateway as a Deployment of its own
(`ANKUSA_ROLES=claim_check`). `deploy/helm/ankusa/ci/{disk,dual,none}-values.yaml`
are one worked example per mode.

**The pod spec is the restricted Pod Security Standard.** The pod runs as uid 100 /
gid 101 (`runAsNonRoot`), `readOnlyRootFilesystem: true` (the writable paths are the
data volume and a 256 MiB `/tmp`, which is the release's `RELEASE_TMP`),
`allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`, the runtime's default
seccomp profile, no service-account token, no service-link environment variables. The
`kind` run of the fleet (below) installs the chart into a namespace that *enforces*
`restricted`. Every one of these is a value (`podSecurityContext`, `securityContext`),
so a cluster policy that needs another uid changes the chart's input, not its templates.

**Probes.** `startupProbe` and `livenessProbe` are `GET /health` (the process answers;
the startup probe allows 10 minutes, for a store's boot migrations), `readinessProbe`
is `GET /ready` with `failureThreshold: 1`: a full disk, a reopening store, an intake
log that is down, or a drain takes the pod out of the Service within one period
([Liveness and readiness](#running-the-container)). A claim-check pod has no `/ready`,
so all three probes are `GET /health` on 4001.

**`terminationGracePeriodSeconds` is computed from the drain.** The chart writes
`drain.delayMs` / `drain.timeoutMs` into `listener.drain_delay_ms` /
`drain_timeout_ms` and sets the grace period to `(delay + timeout) / 1000 + 10`, plus
`backup.final_timeout_ms` (20 s) when `config.backup.enabled`: 45 s with the defaults,
65 s for a backing-up node. A `drain` set under `config.listener` is the one it follows.
There is no `preStop` sleep: the node flips `/ready` itself on SIGTERM and keeps serving
for `drain_delay_ms`, which is the endpoint-removal margin
([Draining on SIGTERM](#running-the-container)).

**Disruption and placement.** A PodDisruptionBudget (`maxUnavailable: 1`) per workload,
and `topologySpreadConstraints` over zones (`ScheduleAnyway`) and nodes
(`DoNotSchedule`), `maxSkew: 1`, selecting the workload's own pods. A rolling update
of the StatefulSet replaces one pod at a time and waits for its `/ready`; the
Deployment surges one pod and never goes below its replicas.

**The network.** A NetworkPolicy selects every pod of the release: port 4000 is open to
every client (providers), 4001-4004 (claim check, admin and `/metrics`, route
management, the metrics listener) only to the namespaces in
`networkPolicy.operatorNamespaces` (the release's own by default). The admin API and the
gateway bind their pod address only when `admin.expose` / `claimCheck.enabled` ask, and
the chart refuses to render one without a token Secret (`tokenSecret.name`, which becomes
`ANKUSA_ADMIN_TOKEN` / `ANKUSA_CLAIM_CHECK_TOKEN`) or `allowInsecure: true`. Egress is
open: sinks, the intake log and the object store are wherever your config says.

**Sizing the PVC.** The volume holds everything the node has acked and not yet
compacted and delivered, so size it for the backlog you promise to ride out
(a sink outage, a paused broker):

```
PVC bytes = hooks/s × mean body bytes × backlog goal (s) × 2
```

The `× 2` is headroom for row overhead, the write-ahead log and compaction's working
set. Keep `node.min_free_bytes` (the `disk` readiness check) at 10 % of the volume so a
node leaves the load balancer before `ENOSPC`. Per node:

| hooks/s | mean body | backlog goal | PVC bytes | `persistence.size` |
| --- | --- | --- | --- | --- |
| 100 | 10 KiB | 5 min | 0.6 GiB | `5Gi` (the practical floor: the RocksDB, the quarantine pen, the local archive) |
| 1 000 | 10 KiB | 5 min | 5.7 GiB | `8Gi` |
| 1 000 | 10 KiB | 1 h | 68.7 GiB | `80Gi` |
| 10 000 | 1 KiB | 5 min | 5.7 GiB | `8Gi` |
| 10 000 | 1 KiB | 1 h | 68.7 GiB | `80Gi` |
| 1 000 | 100 KiB | 5 min | 57.2 GiB | `64Gi` |

A body over `inline_max_bytes` of a claim-checking sink leaves the volume for the claim
bucket, so it counts for its claim reference only. Segments belong in S3/GCS
(`storage.type`), where they are not the volume's problem; with LocalFS they live on it
too. A StatefulSet that is deleted or scaled down keeps its volumes
(`persistence.retentionPolicy`).

**Proof.** `mise run check:examples` renders the chart for every mode and validates the
manifests with `kubeconform`. `mise run e2e` (or `mise run e2e fleet`) runs the fleet of
[`examples/fleet-e2e`](https://github.com/jamescarr/ankusa/tree/main/examples/fleet-e2e/)
from this chart on kind: two StatefulSet pods and a gateway in a namespace that enforces
`restricted`, one pod deleted (a drain) and one `SIGKILL`ed under load, `missing=0`.

## Multi-region

A region is a failure domain, so the unit of deployment is a **regional fleet**: one
release of the chart with everything it needs, in the region, and nothing shared with
another region.

- **Independent regional fleets.** Each region has its own Kubernetes cluster, StatefulSet,
  Redis (the route, source and fleet stores), segment/claim bucket, consumers and
  intake log. Nothing replicates between them, so a region that fails takes down only
  its own tenants. `dedupe.scope: fleet` and `rate_limits.scope: fleet` are *per
  region*: the same provider event delivered to two regions is two hooks, and a tenant's
  quota is per region.
- **Tenant → region pinning at the global load balancer.** Pin every tenant to one home
  region and route on it before the request reaches a fleet: the tenant segment of the
  catch URL (`/webhooks/<tenant>/<source>`, [`multi-tenancy.md`](multi-tenancy.md)) or a
  per-tenant hostname, in the global LB (GeoDNS, an anycast L7 proxy, a mesh gateway).
  Keep the pin in one table that also provisions the tenant's routes and sources in
  its home region. A region outage is the provider's retry (they retry a `5xx`) against
  a LB that keeps answering `503`, or a **deliberate** change of the pin; moving a tenant
  re-points new hooks only, and what its old region had acked stays there until it returns
  or is restored.
- **One intake topic per region.** `wal.type: dual` confirms every hook in a Kafka
  cluster inside the region, across its zones (replication factor 3,
  `min.insync.replicas=2`, rack-aware): `intake.fleet` names the region
  (`ankusa.intake.eu-west-1`), and no cluster stretches across regions, because a confirm
  that waits for another region's replicas puts a WAN round trip into every ack and
  makes the region's availability depend on its neighbour's.
- **`backup.secondary` is the cross-region copy.** Set the primary bucket in the region
  and `backup.secondary` in another one: every backup goes to both, and a restore falls
  back to the secondary. That is the *only* state that leaves a region, and it is why a
  region lost entirely (hosts, volumes, the intake log, the primary bucket) costs the
  hooks acked since the last backup (`backup.interval_ms`, the recovery point); `dual`'s
  zero-loss promise covers a lost host, not a lost region. Restoring into another
  region means installing the chart there with the same node ids and `backup` pointed
  at the surviving bucket: each pod restores its store from it before it takes a hook.
- **No cross-region store.** No RocksDB, Redis, claim bucket or Kafka cluster spans
  regions, and consumers redeem claims at their own region's gateway. Sinks may be
  anywhere (TLS, the SSRF-safe egress rules apply), but the hook is durable in its home
  region before any of it leaves.

## Upgrading between releases

The store carries a **format version** (`m:format_version` in its `default`
column family; this release writes format 3). A store without the key is
format 1 — every store written before the marker existed — and is stamped on
its first open. A release that changes the key layout adds a migration to the
store's migration registry; on boot, before the node serves, the
migrations from the store's format up to the release's run in order, each in
bounded synced batches. A migration records its place in `m:migrating:<to>`
with every batch, so a node killed mid-migration resumes at the next boot
instead of starting over, and the new format is stamped only in the batch that
finishes it. Migrations are forward-only.

**Format 2 (the audit trail).** The store gains the `audit` column family and
every API-managed source a revision (`v:<tenant>\0<name>`; sources a format 1
store held become revision 1, in bounded synced batches). A format 1 release
cannot open a format 2 store: it refuses at the format guard, and RocksDB
itself refuses a database holding a column family the release does not list.
That is the rollback boundary: **once a node has booted format 2, going back
means restoring the pre-upgrade backup**, losing what was acked since and
every audit row. A node's audit rows are its own; a fleet reads each node's
trail with `GET /v1/audit` (scope `audit:read`) per node.

**Format 3 (tenant ordinals and counters, [ADR 0001](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0001-tenant-key-layout.md)).**
The store gains the `tenants` and `counters` column families. Every tenant
gets a dense ordinal, due and claimed rows are re-keyed under it (`?d`/`?f`
become `?D`/`?F`), the dead index gains a by-tenant companion (`?X`), a due
row gets an active-tenant marker (`?A`), and the per-tenant queue counters
are seeded from the rows. The migration runs on boot, before the node serves,
in synced batches of 500 rows (due, claimed, dead, then archive obligations)
and is proportional to the rows queued at the upgrade: a node with an empty
queue migrates in a moment, one with millions of queued rows takes minutes
and logs each step; it resumes where a kill left it and counts nothing twice.
The gauges `ankusa_queue_pending` and `_scheduled` change meaning here:
`pending` is first attempts waiting and `scheduled` is retries waiting
(including one whose backoff has just ended); their sum is what it was, and
`ankusa_queue_oldest_due_age_seconds` is still the alarm for a backlog. A
format 2 release cannot open a format 3 store (`{:store_format_newer, 3, 2}`,
and RocksDB refuses the families it does not list). That is the rollback
boundary: **once a node has booted format 3, going back means restoring the
pre-upgrade backup**, losing what was acked since, so take a backup before
the first boot. A migration killed part-way leaves `m:migrating:3` and the
store at format 2 with some rows moved; only a format 3 release can open it,
and it resumes. A fleet upgrades node by node: stores are per node. A new
key, `tenants.max` (default 1 000 000), refuses the hook of the tenant past
it with `503 tenant_limit` and `Retry-After: 300`; tenants already known are
never refused.

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
`wal: :disk`; any other value but `:disk`, `:dual` or `:none` raises too.

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
