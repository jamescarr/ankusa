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
durable log before it is acked, so a provider retry is either absorbed as a
duplicate or genuinely never arrived.

## Quick start

```sh
docker run -d --name ankusa \
  -p 4000:4000 -p 127.0.0.1:4002:4002 \
  -v ankusa-data:/var/lib/ankusa \
  jamescarr/ankusa:edge

# the image ships a healthcheck: wait for it rather than racing the listener
until [ "$(docker inspect --format '{{.State.Health.Status}}' ankusa)" = healthy ]; do sleep 1; done

curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_1"}'
# => {"id":"01a0...","status":"accepted","seq":1}   (returned only after the WAL fsync)
curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_1"}'
# => {"status":"duplicate",...}                     (the retry is absorbed, not stored twice)
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
  -p 4000:4000 -p 127.0.0.1:4002:4002 \
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
    dedup: {type: stripe}
    on_verify_failure: quarantine
    sinks:
      - {type: http, url: "${SINK_URL}", method: post, timeout_ms: 5000}
```

Secrets never go in the file: reference them as `${VAR}` and pass the values as
environment variables. A `${VAR}` with no value and no default stops the
container at startup, naming the field — an empty secret would otherwise accept
everything an attacker signs.

Starting points, all loadable as-is:

| File | What it is |
| --- | --- |
| [`config-examples/reference.yml`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/config-examples/reference.yml) | every key, at its default, with the alternatives |
| [`config-examples/single-node.yml`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/config-examples/single-node.yml) | one box: disk WAL, Stripe + GitHub, HTTP sink |
| [`config-examples/fleet-postgres-s3.yml`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/config-examples/fleet-postgres-s3.yml) | edge replicas on a shared Postgres WAL, segments in S3 |
| [`config-examples/kafka-fanout.yml`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/config-examples/kafka-fanout.yml), [`rabbitmq-fanout.yml`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/config-examples/rabbitmq-fanout.yml), [`nats-fanout.yml`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/config-examples/nats-fanout.yml) | queue fan-out, with the claim-check gateway (`claim_check` role included) |
| [`config-examples/multi-tenant.yml`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/config-examples/multi-tenant.yml) | one instance, many tenants, tenant in the URL |

### Check it before you run it

```sh
docker run --rm -v "$PWD/ankusa.yml:/etc/ankusa/ankusa.yml:ro" \
  -e STRIPE_WHSEC jamescarr/ankusa:edge check-config
# => config OK: roles=[:edge, :dispatch, :storage] sources=stripe wal=Ankusa.WAL.DiskLog storage=Ankusa.BlobStore.LocalFS

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
| `ANKUSA_WAL_TYPE` | `wal.type` (`disk` or `postgres`) |
| `ANKUSA_WAL_POSTGRES_URL` | `wal.postgres.url` |
| `ANKUSA_STORAGE_TYPE` | `storage.type` (`local`, `s3`, `gcs`) |
| `ANKUSA_S3_BUCKET`, `ANKUSA_S3_REGION`, `ANKUSA_S3_ENDPOINT` | `storage.s3.bucket/region/endpoint` |
| `ANKUSA_GCS_BUCKET` | `storage.gcs.bucket` |

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
- `x-ankusa-id`, `x-ankusa-source`, `x-ankusa-seq`, and `x-ankusa-tenant` when set.
- `2xx` means delivered. Anything else, a timeout, or a redirect is retried, then dead-lettered.
- Dedupe on `x-ankusa-id`: delivery is at-least-once.

A runnable version — the image plus a Python worker, one `docker compose up` — is
[`examples/quickstart/`](https://github.com/jamescarr/ankusa/tree/main/examples/quickstart/);
the walkthrough with outages, dead letters, and replay is
[`docs/quickstart.md`](https://github.com/jamescarr/ankusa/blob/main/docs/quickstart.md).

## Ports

| Port | What | Who can reach it |
| --- | --- | --- |
| 4000 | ingest | publish it: providers post here |
| 4001 | claim check gateway (`claim_check` role) | your own proxy or network policy |
| 4002 | admin API + `/metrics` | your own proxy or network policy |

Every surface is on its own port so it can be firewalled on its own.

### What the admin API is for

Operating Ankusa without a shell:

```sh
curl localhost:4002/v1/config                 # the effective config, secrets redacted
curl 'localhost:4002/v1/dlq?limit=10'         # dead-lettered hooks (metadata only)
curl -XPOST localhost:4002/v1/dlq/replay -d '{"id":"<id from GET /v1/dlq>"}'
curl -XPOST localhost:4002/v1/dlq/replay -d '{"source_id":"stripe"}'
curl localhost:4002/v1/quarantine             # hooks held after a failed verification
curl localhost:4002/metrics                   # Prometheus
```

The DLQ and quarantine endpoints are node-local — the DLQ is the dispatch node's
disk, quarantine is the edge node's memory — and answer
`409 role_not_enabled` when you ask the wrong node. Metrics are node-local too:
an edge node exports ingest series, a worker exports dispatch and compaction
series. Scrape every node.

The HTTP contracts are in
[`priv/openapi/admin.v1.yaml`](https://github.com/jamescarr/ankusa/blob/main/priv/openapi/admin.v1.yaml),
[`priv/openapi/ingest.v1.yaml`](https://github.com/jamescarr/ankusa/blob/main/priv/openapi/ingest.v1.yaml),
and
[`priv/openapi/claim_check.v1.yaml`](https://github.com/jamescarr/ankusa/blob/main/priv/openapi/claim_check.v1.yaml).

## Security

Ankusa verifies provider signatures on ingest — Stripe, GitHub, and Standard
Webhooks — and does no other authentication. It does not manage users, API keys,
or tokens; that is your identity provider's job, and pretending otherwise would
be worse.

So:

- **Publish only port 4000.** Providers authenticate themselves by signing the
  body, and Ankusa verifies that signature against the source's secret.
- **Put 4002 (admin/metrics) behind your own proxy, SSO, or network policy.** It
  ships unauthenticated on purpose: it cannot know your identity provider, and a
  half-authentication scheme is worse than none. `GET /v1/config` is redacted
  anyway, because a proxy may let many people read it.
- **Put 4001 (claim check) behind the same.** It exists so non-BEAM consumers
  can fetch large payloads without holding storage credentials; bearer tokens
  are optional and only worth configuring if your setup wants them.
- **Point Prometheus at 4002 through your proxy, with read-only credentials.**

The fleet compose file is the worked example:
[`compose/docker-compose.fleet.yml`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/compose/docker-compose.fleet.yml)
runs two edge replicas and a worker with no published ports, and an nginx in
front that leaves ingest open and puts HTTP basic auth on the admin API. It also
demonstrates the guarantee the shared WAL buys: post the same hook twice, land on
different replicas, and the second is absorbed as a duplicate.

## Data

`/var/lib/ankusa` holds the WAL, the quarantine log, the dead-letter queue, and
local segments. Losing it loses un-dispatched hooks, so give it a volume and back
it — or move the WAL to Postgres and segments to S3/GCS, where it is not your
problem anymore.

## Compose

- Single node: [`compose/docker-compose.yml`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/compose/docker-compose.yml)
- Fleet behind nginx basic auth: [`compose/docker-compose.fleet.yml`](https://github.com/jamescarr/ankusa/blob/main/ankusa_server/compose/docker-compose.fleet.yml)

```sh
docker compose -f docker-compose.fleet.yml up -d --wait
curl -XPOST localhost:4000/webhooks/demo -d '{"id":"f1"}'    # 201
curl -XPOST localhost:4000/webhooks/demo -d '{"id":"f1"}'    # 200 duplicate
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
scales — [quickstart](https://github.com/jamescarr/ankusa/blob/main/docs/quickstart.md),
[configuration](https://github.com/jamescarr/ankusa/blob/main/docs/configuration.md),
[architecture](https://github.com/jamescarr/ankusa/blob/main/docs/architecture.md),
[deployment](https://github.com/jamescarr/ankusa/blob/main/docs/deployment.md),
[storage](https://github.com/jamescarr/ankusa/blob/main/docs/storage.md),
[delivery](https://github.com/jamescarr/ankusa/blob/main/docs/delivery.md),
[multi-tenancy](https://github.com/jamescarr/ankusa/blob/main/docs/multi-tenancy.md),
[claim check](https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md),
[examples](https://github.com/jamescarr/ankusa/blob/main/examples/README.md).

Apache 2.0. See [LICENSE](https://github.com/jamescarr/ankusa/blob/main/LICENSE).
