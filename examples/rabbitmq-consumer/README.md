# Example: ingest fleet → RabbitMQ → claim-check gateway → worker

A complete deployed topology, proving the framework isn't just a library —
it's something you run, front real webhooks with, and consume from a queue,
with a Claim Check gateway (`docs/claim-check.md`) fronting the object store
for consumers that shouldn't hold storage credentials.

```mermaid
flowchart LR
    P[Provider / curl] -->|POST /hooks/demo| I[Ankusa ingest\nedge+dispatch+storage]
    I -->|WAL fsync, then ack| P
    I -->|"small body (base64)"| X((ankusa.events\nexchange))
    I -.fat body: Direct check-in.-> S[(S3 / floci)]
    I -->|"fat body → message carries a ticket"| X
    X -->|ankusa.# binding, owned by consumer| Q[worker's queue]
    Q --> W[TypeScript worker\nno S3 credentials]
    W -->|GET /v1/claims/... Bearer token| CC[claim-check\n:claim_check role]
    CC -.Direct.-> S
```

**What each piece is doing:**

- `ingest/` — a real `Ankusa.Instance` (`ANKUSA_ROLES=edge,dispatch,storage`,
  the default), configured entirely from environment variables.
  `Ankusa.Sink.RabbitMQ` publishes every delivered hook to the
  `ankusa.events` exchange; `Ankusa.BlobStore.S3` backs both segment
  compaction (the framework's own async archival, `seg/...` keys) and the
  sink's fat-payload claim check (`claims/...` keys) — same bucket, same
  credentials, no separate storage code.
- `claim-check` — the **same image**, `ANKUSA_ROLES=claim_check` is the only
  difference. Its own listener (`:4001`), its own bearer token. It's the
  only piece besides `ingest` that ever holds S3 credentials.
- `worker/` — a minimal TypeScript consumer with **no S3 credentials at
  all**. It declares and binds its own queue (`ankusa.#` against the
  exchange) — the ingest framework never touches a queue, only the
  exchange — and redeems fat-payload tickets through `claim-check`'s HTTP
  API instead of talking to the object store directly.
- `floci` — local S3-compatible emulator (see the root README's "Object
  store adapters" section); stands in for real S3/R2/MinIO.

## Small vs. fat payloads

`Ankusa.Sink.RabbitMQ` inlines a body under `INLINE_MAX_BYTES` (default 8 KiB,
base64-encoded in the message). Anything larger is checked in through
`Ankusa.ClaimCheck` and the message carries a ticket instead —
`{"claim": {"tenant_id": ..., "id": ..., "size": ..., "sha256": ...}}`.
RabbitMQ throughput and memory stay flat regardless of how large a webhook
payload is. The worker redeems the claim (a `GET` against `claim-check`,
verified end to end against the ticket's `sha256`) only when one is present;
otherwise it just decodes the inline body. Try both — the commands below
send one of each.

## Run it

```sh
docker compose up --build
```

Brings up: RabbitMQ (management UI at `http://localhost:15672`, guest/guest),
`floci` (S3 emulator, bucket `ankusa-example` auto-created), the ingest server
(`localhost:4000`), the claim-check gateway (`localhost:4001`), and the
worker (consuming and printing to its own logs).

Send a small hook:

```sh
curl -XPOST localhost:4000/hooks/demo -H 'content-type: application/json' \
  -d '{"id":"evt_1","event":"push"}'
```

Send a fat one (anything over 8 KiB checks in through the gateway instead):

```sh
python3 -c "import json; print(json.dumps({'id':'evt_2','items':[{'n':i} for i in range(2000)]}))" \
  | curl -XPOST localhost:4000/hooks/demo -H 'content-type: application/json' --data-binary @-
```

Watch the worker print both:

```sh
docker compose logs -f worker
```

You'll see `via=inline` for the first and `via=claim:<id>` for the second,
followed by the actual decoded payload in each case — proof the ticket
round-trips through the real gateway and the real object store, not just
that a message arrived. You can also redeem a ticket by hand:

```sh
curl -H 'authorization: Bearer dev-claim-check-token' \
  http://localhost:4001/v1/claims/default/<id>
```

Tear down:

```sh
docker compose down -v
```

## Scaling the ingest fleet

`docker compose up --build --scale ingest=3` runs three independent ingest
containers, each with its own local WAL, all publishing to the same
exchange, checking claims in against the same `claim-check` gateway, and
writing to the same bucket. Nothing about `Ankusa.Sink.RabbitMQ` or
`Ankusa.BlobStore.S3` changes — that's the "durable state, not RPC" rule
holding here exactly like it does between the edge/dispatch/storage roles
inside one instance. (You'd need a load balancer in front for the ingest
port at that point — a deployment concern, not something the framework
does for you.) For a *shared* WAL across those nodes instead of N
independent local ones, see `ankusa_postgres/` in the repo root.

## What's stubbed on purpose

`worker/src/worker.ts`'s `handleHook` just prints. That's the boundary the
prompt asked for: "queue bound to exchange with client code consuming (we
won't implement)" — replace it with your real processing; everything above
it (topology declaration, message decode, claim redemption and integrity
verification, ack/nack) is real, working code, not a stub.
