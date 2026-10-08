# Ankusa

<p align="center">
  <img src="https://raw.githubusercontent.com/jamescarr/ankusa/main/static/ankusa.png" alt="Ankusa" width="280">
</p>

[![Docker pulls](https://img.shields.io/docker/pulls/jamescarr/ankusa.svg)](https://hub.docker.com/r/jamescarr/ankusa)
[![CI](https://github.com/jamescarr/ankusa/actions/workflows/ci.yml/badge.svg)](https://github.com/jamescarr/ankusa/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/hexpm/l/ankusa.svg)](https://github.com/jamescarr/ankusa/blob/main/LICENSE)

_**Don't fight the traffic. Steer it.** Durable webhook ingestion for any volume._

Run Ankusa without knowing Elixir, the way you run Elasticsearch without knowing
Java. One image, one YAML file, HTTP in and HTTP out. Every hook is written to a
durable local store before it is acked, so a provider retry is either a hook
that was durably stored (and will be delivered again) or one that genuinely
never arrived.

## Quick start

```sh
docker run -d --name ankusa \
  -p 4000:4000 -p 127.0.0.1:4002:4002 -e ANKUSA_ADMIN_IP=0.0.0.0 \
  -v ankusa-data:/var/lib/ankusa \
  jamescarr/ankusa:edge

# the image ships a healthcheck: wait for it rather than racing the listener
until [ "$(docker inspect --format '{{.State.Health.Status}}' ankusa)" = healthy ]; do sleep 1; done

curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_1"}'
# => {"id":"01a0...","status":"accepted"}   (returned only after the store fsync)
```

That is a complete, durable webhook receiver. The `demo` source accepts anything
and logs it, so it works before you have any provider credentials.

```sh
curl localhost:4002/health    # {"status":"ok","instance":"default","roles":[...]}
curl localhost:4002/metrics   # Prometheus text format
```

## Configure it

Mount a YAML file at `/etc/ankusa/ankusa.yml` (or point `ANKUSA_CONFIG` at it):

```sh
docker run -d --name ankusa \
  -p 4000:4000 -p 127.0.0.1:4002:4002 -e ANKUSA_ADMIN_IP=0.0.0.0 \
  -v "$PWD/ankusa.yml:/etc/ankusa/ankusa.yml:ro" \
  -v ankusa-data:/var/lib/ankusa \
  -e STRIPE_WHSEC -e GITHUB_WEBHOOK_SECRET -e SINK_URL \
  jamescarr/ankusa:edge
```

```yaml
node:
  roles: [edge, dispatch, storage]

sources:
  stripe:
    verify: {type: stripe, secret: "${STRIPE_WHSEC}", tolerance_seconds: 300}
    on_verify_failure: quarantine
    sinks:
      - {type: http, url: "${SINK_URL}", method: post, timeout_ms: 5000}
```

Secrets never go in the file: reference them as `${VAR}` and pass the values as
environment variables. A `${VAR}` with no value and no default stops the
container at startup, naming the field: an empty secret would otherwise accept
everything an attacker signs.

Starting points, all loadable as-is:

| File | What it is |
| --- | --- |
| [`config-examples/reference.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/reference.yml) | every key, at its default, with the alternatives |
| [`config-examples/single-node.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/single-node.yml) | one box: on-disk store, Stripe + GitHub, HTTP sink |
| [`config-examples/kafka-fanout.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/kafka-fanout.yml), [`rabbitmq-fanout.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/rabbitmq-fanout.yml), [`nats-fanout.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/nats-fanout.yml) | queue fan-out, with the claim-check gateway (`claim_check` role included) |
| [`config-examples/multi-tenant.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/multi-tenant.yml) | one instance, many tenants, tenant in the URL |
| [`config-examples/direct.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/direct.yml) | stateless edge (`wal.type: none`): no hook store, broker confirm is the ack |

### Check it before you run it

```sh
docker run --rm -v "$PWD/ankusa.yml:/etc/ankusa/ankusa.yml:ro" \
  -e STRIPE_WHSEC jamescarr/ankusa:edge check-config
# => config OK: roles=[:edge, :dispatch, :storage] sources=stripe wal=disk storage=Ankusa.BlobStore.LocalFS

docker run --rm -v "$PWD/ankusa.yml:/etc/ankusa/ankusa.yml:ro" \
  -e STRIPE_WHSEC jamescarr/ankusa:edge print-config
# => the effective config as JSON, with every secret redacted
```

`check-config` exits `78` (`EX_CONFIG`) on a bad file, which is also how the
container fails at startup: a message, not a crash dump. `version` prints the
server and core versions.

### Env overrides

Anything you would normally read from the platform, so a container can be
reconfigured without a new file. Env wins over the file.

| Variable | Field |
| --- | --- |
| `ANKUSA_ROLES` | `node.roles`, comma-separated (`edge`, `dispatch`, `storage`, `claim_check`) |
| `ANKUSA_DATA_DIR` | `node.data_dir` |
| `ANKUSA_LOG_LEVEL` | `log.level` |
| `ANKUSA_HTTP_PORT`, else `PORT` | `http.port` |
| `ANKUSA_ADMIN_PORT` | `admin.port` |
| `ANKUSA_CLAIM_CHECK_PORT` | `claim_check.port` |
| `ANKUSA_ADMIN_IP` | `admin.ip` |
| `ANKUSA_CLAIM_CHECK_IP` | `claim_check.ip` |
| `ANKUSA_ROUTES_ENABLED` | `routes.enabled` (`true`/`false`) |
| `ANKUSA_ROUTES_STORE_URL` | `routes.store.url` |
| `ANKUSA_WAL_TYPE` | `wal.type` (`disk` \| `none`) |
| `ANKUSA_STORAGE_TYPE` | `storage.type` (`local`, `s3`, `gcs`) |
| `ANKUSA_S3_BUCKET`, `ANKUSA_S3_REGION`, `ANKUSA_S3_ENDPOINT` | `storage.s3.bucket/region/endpoint` |
| `ANKUSA_GCS_BUCKET` | `storage.gcs.bucket` |

Route definitions live in `routes.store`, chosen by `type`: `ets` (each node's
own memory, the default) or `redis` (shared by every edge node); a `url` with
no `type` means `redis`. `url`, `namespace` and `tick_ms` are only valid with
`type: redis` — setting one alongside `type: ets` stops the container with a
config error naming the key, instead of leaving every node with its own
definitions while you believed the fleet shared them.

`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` are read by the S3 adapter
directly when the config does not name static keys.

## Deliver to your worker

An HTTP sink forwards every hook to your own service, raw body and all:

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

- The raw body verbatim, with the provider's `content-type`.
- `x-ankusa-id`, `x-ankusa-source`, and `x-ankusa-tenant` when set.
- `x-ankusa-idempotency-key` always; `x-ankusa-dedupe-key` and `x-ankusa-replay-id` when present.
- The provider's own request headers, per the source's `forward_headers` (by default every header except auth, framing, hop-by-hop and `x-ankusa-*`).
- `2xx` means delivered. Anything else, a timeout, or a redirect is retried, then dead-lettered.
- **Dedupe on `x-ankusa-idempotency-key`.** Delivery is at-least-once, so consumers are
  idempotent receivers. The key is `tenant:source:dedupe_key` when the source
  has `dedupe:` set, else the hook id, and every redelivery of the hook (a
  retry, a DLQ replay, a restart) carries the same key. `x-ankusa-id`
  identifies one stored hook. Without a source `dedupe:` setting, a provider
  retry is a *different* stored hook with a different id; with one, ingest
  collapses hooks that share the provider's event key within the TTL.

A runnable version, the image plus a Python worker, one `docker compose up`, is
[`examples/quickstart/`](https://github.com/jamescarr/ankusa/tree/main/examples/quickstart/);
the walkthrough with outages, dead letters, and replay is
[`docs/quickstart.md`](https://github.com/jamescarr/ankusa/blob/main/docs/quickstart.md).

## Ports

| Port | What | Who can reach it |
| --- | --- | --- |
| 4000 | ingest | publish it: providers post here |
| 4001 | claim check gateway (`claim_check` role) | your own proxy or network policy |
| 4002 | admin API + `/metrics` | your own proxy or network policy |
| 4003 | route management API (`routes.enabled` on an `edge` node) | your own proxy or network policy |

Every surface is on its own port so it can be firewalled on its own. 4001/4002/4003 listen on `127.0.0.1` by default; inside a container set `ANKUSA_ADMIN_IP` / `ANKUSA_CLAIM_CHECK_IP` (or `admin.ip` / `claim_check.ip`, and `routes.admin.ip` for 4003) to `0.0.0.0` before a published port can reach them. 4003 only
listens when `routes.enabled` is set and the node runs the `edge` role; without
both, nothing binds it.

### What the admin API is for

Operating Ankusa without a shell:

```sh
curl localhost:4002/v1/config                 # the effective config, secrets redacted
curl localhost:4002/v1/wal                    # this node's store stats (409 under wal.type: none)
curl 'localhost:4002/v1/dlq?limit=10'         # dead-lettered hooks (metadata only)
curl -XPOST localhost:4002/v1/replays -d '{"kind":"dlq","id":"<id from GET /v1/dlq>","rate":100}'
curl localhost:4002/v1/replays                # every replay job, newest first
curl -XPATCH localhost:4002/v1/replays/<job-id> -d '{"state":"cancelled"}'
curl localhost:4002/v1/quarantine             # hooks held after a failed verification
curl -XPOST localhost:4002/v1/replays -d '{"kind":"quarantine"}'   # release the ones that verify now
curl -XDELETE 'localhost:4002/v1/quarantine?source_id=stripe'      # purge what never will
curl localhost:4002/metrics                   # Prometheus
```

The DLQ, WAL, and quarantine endpoints are node-local and read this node's
store: the DLQ is its dead delivery rows, the WAL endpoint its store stats,
and quarantine the edge node's pen. They answer `409 role_not_enabled` (or,
for the WAL, `409 wal_disabled`) when you ask the wrong node, and
`503 store_unavailable` when the store cannot be read. Metrics are node-local
too: an edge node exports ingest series, a worker exports dispatch and
compaction series. Scrape every node.

The HTTP contracts are in
[`priv/openapi/admin.v1.yaml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/priv/openapi/admin.v1.yaml),
[`priv/openapi/ingest.v1.yaml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/priv/openapi/ingest.v1.yaml),
and
[`priv/openapi/claim_check.v1.yaml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/priv/openapi/claim_check.v1.yaml).

## Security

Ankusa verifies provider signatures on ingest (`stripe`, `github`, `standard_webhooks`,
`shopify`, `slack`, and `hmac` for any body-HMAC provider), and does no other
authentication. It does not manage users, API keys,
or tokens; that is your identity provider's job, and pretending otherwise would
be worse.

So:

- **Publish only port 4000.** Providers authenticate themselves by signing the
  body, and Ankusa verifies that signature against the source's secret.
- **Put 4002 (admin/metrics) behind your own proxy, SSO, or network policy.** It
  ships unauthenticated on purpose: it cannot know your identity provider, and a
  half-authentication scheme is worse than none. `GET /v1/config` is redacted
  anyway, because a proxy may let many people read it.
- **Put 4001 (claim check) behind your proxy, mesh, or network policy, the
  same as 4002.** The claim-check API performs no authentication of its own:
  it exists so non-BEAM consumers can fetch large payloads without holding
  storage credentials. Keep it internal and decide who may read what in the
  layer in front of it.
- **Put 4003 (route management) behind your proxy or network policy, the same
  as 4002.** It edits the route definitions the edge enforces and
  authenticates nobody, by the same deliberate decision. It only listens when
  `routes.enabled` is set on an `edge` node, and publishing it is your call.
- **Point Prometheus at 4002 through your proxy, with read-only credentials.**

All three listeners bind `127.0.0.1` by default. A proxy or a Prometheus in
another container, pod or host reaches them only after `admin.ip`,
`claim_check.ip` or `routes.admin.ip` is set to an address it can route to
(`0.0.0.0` inside a container); the bind decides who can connect, and the
proxy in front still decides who may.

The proxy compose file is the worked example:
[`compose/docker-compose.proxy.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/compose/docker-compose.proxy.yml)
runs one all-role node with no published ports, and an nginx in front that
leaves ingest open and puts HTTP basic auth on the admin API.

## Data

`/var/lib/ankusa` holds the node's RocksDB store (hooks, the dead-letter queue,
the quarantine pen, API-managed sources, rate-limit overrides, the segment
catalogue) and local segments. Losing it loses un-dispatched hooks, so give it
a volume and back it, or move segments to S3/GCS, where they are not your
problem anymore. Under `wal.type: none` there is no hook there to lose: run the
image as a plain `Deployment` and let the broker hold the durable copy. The
store still holds API-managed sources, rate-limit overrides and the quarantine
pen, so mount a volume if those must survive a restart. See
[`config-examples/direct.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/direct.yml)
and [`docs/delivery.md#direct-mode`](https://github.com/jamescarr/ankusa/blob/main/docs/delivery.md#direct-mode).

## Compose

- Single node: [`compose/docker-compose.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/compose/docker-compose.yml)
- All-role node behind nginx basic auth: [`compose/docker-compose.proxy.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/compose/docker-compose.proxy.yml)

```sh
docker compose -f docker-compose.proxy.yml up -d --wait
curl -XPOST localhost:4000/webhooks/demo -d '{"id":"f1"}'    # 201
curl localhost:4002/v1/dlq                                   # 401 (nginx)
curl -u admin:change-me localhost:4002/v1/dlq                # {"total":0,"entries":[]}
```

## Image tags

| Tag | Means |
| --- | --- |
| `X.Y.Z` | an exact release (`ankusa_server-vX.Y.Z` in the repo) |
| `X.Y` | the newest release in that minor line |
| `X` | the newest release in that major line (from 1.0 on) |
| `latest` | the newest release |
| `edge` | the tip of `main`; not a release, may be broken |

`linux/amd64` and `linux/arm64`.

## Where to read more

The image is the framework; the documentation covers what it does and how it
scales: [quickstart](https://github.com/jamescarr/ankusa/blob/main/docs/quickstart.md),
[configuration](https://github.com/jamescarr/ankusa/blob/main/docs/configuration.md),
[architecture](https://github.com/jamescarr/ankusa/blob/main/docs/architecture.md),
[deployment](https://github.com/jamescarr/ankusa/blob/main/docs/deployment.md),
[storage](https://github.com/jamescarr/ankusa/blob/main/docs/storage.md),
[delivery](https://github.com/jamescarr/ankusa/blob/main/docs/delivery.md),
[multi-tenancy](https://github.com/jamescarr/ankusa/blob/main/docs/multi-tenancy.md),
[claim check](https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md),
[examples](https://github.com/jamescarr/ankusa/blob/main/examples/README.md).

Apache 2.0. See [LICENSE](https://github.com/jamescarr/ankusa/blob/main/LICENSE).
