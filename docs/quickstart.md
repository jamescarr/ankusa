# Quickstart

Ankusa is a webhook receiver. It verifies each hook against the provider's
signature, commits it to local disk, answers `201`, and only then delivers it to
a broker your workers already read from. The provider's `2xx` never waits on a
broker or a worker.

This page builds that whole path on one machine: Ankusa, a Redpanda topic, an
SQS FIFO queue, and a Python worker. You then stop the worker, send a hook, stop
the broker, send another, and watch what happens to hooks Ankusa has already
acknowledged. You need Docker with Compose v2 and `curl`; nothing else.

## 1. Start Ankusa, a broker, and a worker

```sh
git clone https://github.com/jamescarr/ankusa
cd ankusa/examples/quickstart
docker compose up --build -d --wait
```

```mermaid
flowchart LR
    P[Provider / curl] -->|POST /webhooks/demo :4000| A[ankusa]
    A -->|written to disk, then 201| P
    A -->|produce ankusa.events, acks=all| T[(Redpanda topic)]
    T -->|consumer group| B[bridge]
    B -->|SendMessage, group = key| Q[(SQS FIFO, floci)]
    Q -->|long poll| W[worker.py]
    W -.->|large body: redeem claim :4001| A
    O[You] -->|/health /metrics /v1/dlq :4002| A
```

[`ankusa.yml`](https://github.com/jamescarr/ankusa/blob/main/examples/quickstart/ankusa.yml)
declares one source, `demo`, with one Kafka sink producing to the
`ankusa.events` topic.
[`docker-compose.yml`](https://github.com/jamescarr/ankusa/blob/main/examples/quickstart/docker-compose.yml)
creates that topic before Ankusa starts, because the sink never creates one.

A Redpanda Connect bridge
([`bridge/bridge.yaml`](https://github.com/jamescarr/ankusa/blob/main/examples/quickstart/bridge/bridge.yaml))
carries each record into an SQS FIFO queue in floci, using the record key
(tenant and source) as the FIFO group. One source's hooks therefore reach the
worker in the order Ankusa produced them. That is not the order they arrived
in: dispatch runs attempts concurrently, and a hook that was retried goes out
after hooks that arrived later. For per-key order, set `ordered: true` and an
`ordering_key` on the sink ([Ordering lanes](delivery.md#ordering-lanes)).

[`worker.py`](https://github.com/jamescarr/ankusa/blob/main/examples/quickstart/worker.py)
polls that queue with `boto3`, decodes each message with the `ankusa` Python
SDK, and prints what it receives.

## 2. Send a webhook

```sh
curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_1","type":"invoice.paid"}'
# => {"id":"01a0...","status":"accepted"}

sleep 2 && docker compose logs worker
# received id=01a0... source=demo via=inline bytes=36 body={"id":"evt_1","type":"invoice.paid"}
```

A body larger than the sink's `inline_max_bytes` (8 KiB in this config) never
reaches the broker. The node stores it and the message carries a claim
reference, which the worker redeems from the claim-check gateway:

```sh
printf '{"id":"evt_2","pad":"%s"}' "$(head -c 20000 /dev/zero | tr '\0' x)" |
  curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' --data-binary @-
sleep 2 && docker compose logs worker | grep evt_2
# received id=01a0... source=demo via=claim bytes=20023 body={"id":"evt_2","pad":"xxxx...
```

The `201` comes back only after the hook is durably accepted, which here means
the node's store has fsynced it. That is
[the core invariant](architecture.md#the-core-invariant). Neither the broker nor
the worker sits on the request path, so a slow or dead worker cannot cost the
provider an acknowledgement.

## 3. A provider retry is a new hook

Unless a source sets [`dedupe:`](configuration.md#sources), ingest collapses
nothing. Send the identical request again and it is a new hook with a new `id`,
stored and delivered a second time:

```sh
curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_1","type":"invoice.paid"}'
# => {"id":"01b1...","status":"accepted"}
```

That is at-least-once on purpose. The provider's retry contract is satisfied
without an ingest-side dedup table, and your worker is the idempotent receiver
(see [Replay is safe](#replay-is-safe) and
[`delivery.md`](delivery.md#idempotent-receivers)).

If your provider sends a stable event id, set `dedupe:` on the source instead.
The second request then answers
`201 {"status":"accepted","id":"01a0...","duplicate":true}` and stores nothing
new.

## 4. When your worker goes down

```sh
docker compose stop worker
curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_3"}'
curl -s localhost:4002/v1/dlq
# => {"total":0,"entries":[],"next_cursor":null}
docker compose start worker
until docker compose logs worker | grep evt_3; do sleep 2; done
```

Nothing was retried and nothing was dead-lettered. Ankusa produced the hook to
the topic and the bridge carried it into the FIFO queue while the worker was
away, where the queue held it. A worker outage is the queue's to absorb.

Allow it about 30 seconds to reappear. The stopped worker's long poll may
already have received the message, and SQS hides a received message for the
queue's visibility timeout (30 s here) whether or not its receiver lived to
process it. Then the next poll gets it.

## 5. When the broker goes down

### Dead-lettered, then replayed

```sh
docker compose stop redpanda
curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_4"}'
# => {"id":"01a0...","status":"accepted"}
until curl -s localhost:4002/v1/dlq | grep -q '"total":1'; do sleep 2; done
curl -s localhost:4002/v1/dlq
# => {"total":1,"entries":[{"id":"01a0...",...,"source_id":"demo",...}],"next_cursor":null}
docker compose up -d --wait redpanda    # back, and healthy
curl -XPOST localhost:4002/v1/replays -d '{"kind":"dlq","source_id":"demo","rate":100}'
# => 202 {"id":"01a0...","kind":"dlq","state":"running","filter":{"source_id":"demo"},...}
until docker compose logs worker | grep evt_4; do sleep 2; done
```

The provider still got its `201` with the broker gone, because the ack never
depended on the broker. Ankusa held the hook and retried the produce until the
retry budget ran out, then dead-lettered it. This config caps retries at 6
attempts (roughly 8–15 s, jitter included) and turns the sink's circuit breaker
off, so the drill takes seconds.

The defaults behave differently. They retry for 24 hours
(`dispatch.retry.max_elapsed_ms`), backing off to 5 minutes, and after 5
failures in a row they park the sink's hooks behind an open breaker without
spending attempts ([A slow or dead sink](delivery.md#a-slow-or-dead-sink)).
Under those defaults, an outage this short is retried and never dead-letters.

### Replay is safe

A replay is a durable job. It drips dead rows back into the delivery queue at
`rate` items per second, and only while dispatch has spare capacity (the
oldest-due lag stays under `max_lag_ms`, 2 s by default), so it cannot flood a
recovering sink or starve live traffic. Each replayed delivery keeps the hook's
original id and carries the job's `replay_id`. Watch the job or stop it:

```sh
curl -s localhost:4002/v1/replays/<job-id>
# => {"state":"done","moved":1,"delivered":1,...}
curl -XPATCH localhost:4002/v1/replays/<job-id> -d '{"state":"cancelled"}'
```

Redelivery is still at-least-once, so the receiver stays idempotent. A hook that
reached the sink before it failed is delivered again with the same `id` and the
same `idempotency_key`; the worker dedupes on the latter, here with an
in-memory set and in a real worker with a unique key in its database. In this
drill the broker was down for every attempt, so replay delivers `evt_4` once
and never again.

## 6. Inspect the node

```sh
curl localhost:4002/health
# => {"status":"ok","instance":"default","version":"…","roles":["claim_check","dispatch","edge","storage"]}
curl localhost:4002/ready
# => {"status":"ready","instance":"default","store":"ok"}
curl -s localhost:4002/metrics | grep ankusa_ingest_requests_total
curl localhost:4002/v1/config      # the effective config, secrets redacted
curl localhost:4002/v1/quarantine  # hooks held after a failed verification
# fixed the secret? release the held hooks that now verify:
curl -XPOST localhost:4002/v1/replays -d '{"kind":"quarantine"}'
```

`/health` is liveness. `/ready` answers whether this node can take a hook right
now, which means the store took a synced write, and that is the probe a load
balancer should use.

Port 4002 is the operator API and should never be published publicly. The
compose file binds it to `127.0.0.1` on the host, and `ankusa.yml` sets
`admin.ip: 0.0.0.0` with `auth.allow_insecure: true` so that the published port
reaches the API inside the container. The result is a demo open to anything that
can reach it. A real deployment sets `admin.auth` to a token, your OIDC
provider, or client certificates; see
[Control-plane authentication](configuration.md#control-plane-authentication).

## 7. Point a real provider at it

Add the provider as a second source under `sources:` in `ankusa.yml`:

```yaml
  stripe:
    verify: {type: stripe, secret: "${STRIPE_WHSEC}", tolerance_seconds: 300}
    on_verify_failure: quarantine
    sinks:
      - type: kafka
        brokers: [redpanda:9092]
        topic: ankusa.events     # the same topic; keyed by tenant/source, so its own FIFO group
        inline_max_bytes: 8192
```

Pass the secret in as an environment variable by adding `environment:
[STRIPE_WHSEC]` to the `ankusa` service, then check the file before starting:

```sh
STRIPE_WHSEC=whsec_... docker compose run --rm ankusa check-config
# config OK: roles=[:edge, :dispatch, :storage, :claim_check] sources=demo,stripe ...
STRIPE_WHSEC=whsec_... docker compose up -d
```

A bad file exits `78` and names the field. Expose ingest with a tunnel, then set
the provider's endpoint to `<tunnel>/webhooks/stripe`:

```sh
ngrok http 4000     # or cloudflared / tailscale funnel
```

GitHub and Standard Webhooks sources are the same shape with a different
`verify.type`: [`configuration.md`](configuration.md#sources).

## Ingest responses

| Status | Meaning |
| --- | --- |
| `201` | Accepted, durably so. A repeat on a source with `dedupe:` answers `201` with the original `id` and `"duplicate": true`, and stores nothing new. A hook that no sink of its source selects through a sink [`filter`](configuration.md#sink-filters) answers `201` with `"filtered": true`: received, and not stored. |
| `202` | Quarantined after a failed verification (`on_verify_failure: quarantine`). It is held, not accepted for delivery. |
| `400` | The body could not be read, or a header holds a byte outside visible ASCII, space and tab (`invalid_header`; no sink could carry it, so retrying is pointless). |
| `401` | Verification failed under `on_verify_failure: reject`. |
| `404` | Unknown source, or a source owned by another tenant. |
| `409` | `dedupe_pending`: another node is committing the same event key (fleet-scoped dedupe). Retry. |
| `413` | Body over `max_body_bytes`. |
| `429` | Over a rate limit, nothing stored: the tenant's (`rate_limited`) or, for a hook that would be quarantined, its source's quarantine bucket (`quarantine_rate_limited`). Retry after `Retry-After`. |
| `503` | No durable destination right now: overload, the store could not take the commit (`store_unavailable`), the intake log could not confirm (`intake_unavailable`, `intake_pending`), or a sink refused under `wal.type: none`. Also a full quarantine pen (`quarantine_full`, `Retry-After: 60`). Retry later. |

`201 accepted` is the only committed response, and it arrives only after a
durable accept: the store's synced commit (`wal.type: disk`, the default), the
store plus the Kafka intake log (`wal.type: dual`), or every sink's confirm
(`wal.type: none`). There is no `200`. Without a source `dedupe:` setting, every
accepted POST is a new hook with a new `id`, and a provider retry after a lost
ack is stored and delivered again.

The catch URL is `/webhooks/:source_id` by default. A tenant-in-the-URL scheme
is one config line away; see [`multi-tenancy.md`](multi-tenancy.md).

## Clean up

```sh
docker compose down -v
```

## Where next

[`configuration.md`](configuration.md) has every YAML key. [`architecture.md`](architecture.md)
explains the guarantee and the failure modes behind the drill you just ran.
[`delivery.md`](delivery.md) covers sinks, retries, the dead-letter queue and
quarantine, and [`deployment.md`](deployment.md) covers roles, fleets and the
container. For a job framework, read [`integrations.md`](integrations.md); for
the library instead of the container, [`elixir.md`](elixir.md).

Runnable examples for every delivery transport are in
[examples/README.md](https://github.com/jamescarr/ankusa/blob/main/examples/README.md),
and the index of these pages is [`README.md`](README.md).
