# Using Ankusa with a job framework, without coupling to one

Ankusa knows nothing about your job system. It hands each hook to an
`Ankusa.Sink` and guarantees at-least-once delivery; what happens next is your
receiver's business. This page describes that seam, then works through two
integrations, Oban and Celery, and the queue transports that skip HTTP
entirely.

## The seam

The handoff is an `Ankusa.Sink`:

```elixir
@callback deliver(Envelope.t(), ctx(), opts :: keyword()) :: :ok | {:error, term()}
```

Return `:ok` only once you are certain the hook was handled durably. A job
enqueued in a transaction that is about to commit counts. A job merely
constructed in memory does not. `{:error, reason}` triggers the source's
`Ankusa.RetryPolicy`: Ankusa retries, then dead-letters (see
[`delivery.md`](delivery.md)).

A sink never talks to a job framework's SDK directly unless it is your own
in-process sink; the Oban section has one. The shipped sinks are all
framework-neutral: `Sink.Log`, `Sink.Http`, `Sink.RabbitMQ`, `Sink.Kafka`,
`Sink.NATS`, `Sink.Redis`, `Sink.SQS` and `Sink.GooglePubSub`.

## HTTP handoff (any language)

The most common shape, and the one both worked examples below use: point
`Sink.Http` at your service's own endpoint.

```elixir
sinks: [{Ankusa.Sink.Http, url: "https://jobs.internal/deliveries", secret: System.fetch_env!("DELIVERIES_WHSEC"), timeout_ms: 5_000}]
```

### What the sink sends

The body is the raw bytes the provider sent, verbatim. Beside it come these
headers.

`x-ankusa-id`, `x-ankusa-source`, `x-ankusa-idempotency-key`,
`x-ankusa-verification` (`ok`, `failed` or `skipped`: how the hook was verified
at ingest) and, when the source has a tenant, `x-ankusa-tenant`. Every delivery
also carries `x-ankusa-received-at` (unix milliseconds the edge accepted the
hook, the queue message's `received_at`) and `x-ankusa-body-sha256` (the
lowercase hex SHA-256 of the body sent). The SDKs read both as advisory:
`received_at` and `body_sha256` are null when absent or malformed, never an
error, so a receiver can order by arrival time or check the bytes it read
without hashing them itself.

`x-ankusa-verification-flagged: true` marks a hook its source accepted despite
a failed check (`on_verify_failure: accept_flag`); treat that delivery as
unverified. `x-ankusa-dedupe-key` and `x-ankusa-replay-id` appear when the hook
carries them, `x-ankusa-event-type` when the source reads the provider's event
name, and `x-ankusa-ordering-key` on an `ordered:` sink, where one lane's
deliveries are sequential. `traceparent` and `tracestate` travel with a hook
that carries a W3C trace context; a provider's own `traceparent` is replaced
rather than forwarded beside it.

Provider request headers (`X-GitHub-Delivery`, `webhook-id`, …) are forwarded
per the source's `forward_headers` option, and carried in `Sink.Message`'s
`headers`. Authentication and framing headers are dropped, as is every
`x-ankusa-*` name. They arrive beside the headers above. When the source
extracts one as its dedupe key, it is also `x-ankusa-dedupe-key`. With a
signing `secret` (below), the sink's own `webhook-id`, `webhook-timestamp` and
`webhook-signature` replace any forwarded headers of those names.

### What your endpoint answers

Respond `2xx` only after the job is durably enqueued: a `202` sent after an
in-transaction insert commits, not before.

`408`, `409`, `425`, `429`, any other `4xx` not listed next, `5xx` and a
timeout are retried per `dispatch.retry`, and no sooner than a `Retry-After`
you send. `400`, `401`, `403`, `404`, `410`, `413`, `422` and `451` are
permanent and dead-letter at once: fix the cause and replay the DLQ (see
[`delivery.md`](delivery.md)). The sink reads the status, and at most
`max_response_bytes` (64 KiB) of your response body.

### Dedupe on the idempotency key

Consumers are idempotent receivers. Delivery is at-least-once, so your endpoint
can see the same key twice: a `2xx` lost in transit, or a dispatch retry after a
timeout that actually succeeded.

Ankusa computes the key once per hook: `tenant:source_id:dedupe_key` when the
source extracts the provider's event key, otherwise the hook's `x-ankusa-id`.
Read it; do not rebuild it. Your enqueue must be idempotent on it. Both examples
below show a way: a unique row keyed on it in the Oban example, an idempotent
task body in Celery.

Provider retries arrive as distinct hooks with distinct `x-ankusa-id`s unless
the source sets `dedupe`, which collapses them at ingest into one hook and so
one key. Without it, dedupe those on the provider's own event id in the body,
such as Stripe's `id`.

### Signed deliveries

Every delivery is signed. `Sink.Http` needs a `secret`, or an explicit
`allow_unsigned: true`; a sink with neither does not load. Each delivery then
carries a [Standard Webhooks](https://www.standardwebhooks.com/) signature:
`webhook-id` (the hook id), `webhook-timestamp` (unix seconds at send time) and
`webhook-signature` (`v1,<base64 HMAC-SHA256>` over
`<webhook-id>.<webhook-timestamp>.<raw body>`, one entry per secret while you
rotate). Without it, any host that can reach your consumer's URL could inject
hooks for any tenant.

```yaml
sinks:
  - type: http
    url: https://jobs.internal/deliveries
    secret: ${DELIVERIES_WHSEC}          # whsec_ + base64, or a list while rotating
```

Every SDK verifies it the same way: a `whsec_` secret decodes from base64, any
other string is its own bytes, the compare is constant time, and the timestamp
window is 300 seconds by default. A bad signature fails with a never-retryable
`InvalidSignatureError` (`code`: `invalid_secret`, `missing_header`,
`invalid_timestamp`, `timestamp_out_of_tolerance`, `no_matching_signature`).
Answer `401`: the delivery dead-letters at once, and a fixed secret plus a DLQ
replay redelivers it.

| SDK | Verify |
| --- | --- |
| TypeScript (`ankusa`) | `verifySignature({ headers, body, secrets })` |
| Python (`ankusa`) | `verify_signature(headers, body, secrets)` |
| Go | `ankusa.VerifySignature(headers, body, secrets, ankusa.VerifyOptions{})` |
| Rust | `verify_signature(&headers, body, &secrets, VerifyOptions::default())` |
| Ruby (`ankusa-sdk`) | `Ankusa.verify_signature(headers, body, secrets)` |
| PHP | `Ankusa\Webhook\Signature::verify($headers, $body, $secrets)` |
| Java | `Signature.verify(headers, body, secrets)` |
| Elixir (`ankusa_sdk`) | `Ankusa.SDK.Signature.verify/4`, or `Ankusa.SDK.Receiver`'s `:secret` option |
| Clojure (`ankusa-clj`) | `(ankusa.sdk.signature/verify headers body secrets)` |

Verify the raw bytes, before any JSON parser touches them. The shared vectors
are `conformance/cases/signature.json`.

Each SDK also has the receiver-side decision in one call, closed by default the
same way the sink is. Given the sink's secrets it verifies as above; given none
and no explicit opt-out it refuses every delivery (`invalid_secret`), so an
unconfigured receiver is never open. The signature is checked before the hook
id, so a request that is not a signed delivery is a `401` whatever else it
lacks. A signed delivery with no `x-ankusa-id` is a `400`.

| SDK | Receive (signed by default) |
| --- | --- |
| TypeScript (`ankusa`) | `receiveDelivery({ headers, body, secrets })`; `allowUnsigned: true` to opt out |
| Python (`ankusa`) | `receive_delivery(headers, body, secrets=[...])`; `allow_unsigned=True` to opt out |
| Go | `ankusa.ReceiveDelivery(headers, body, ankusa.ReceiveOptions{Secrets: ...})`; `AllowUnsigned: true` to opt out |
| Rust | `receive_delivery(&headers, body, &ReceiveOptions { secrets, ..Default::default() })`; `allow_unsigned: true` to opt out |
| Ruby (`ankusa-sdk`) | `Ankusa.receive_delivery(headers, body, secrets: [...])`; `allow_unsigned: true` to opt out |
| PHP | `Ankusa\Webhook\Receiver::receive($headers, $body, $secrets)`; `allowUnsigned: true` to opt out |
| Java | `Receiver.receive(headers, body, Receiver.Options.signed(secrets))`; `Options.unsigned()` to opt out |
| Elixir (`ankusa_sdk`) | the `Ankusa.SDK.Receiver` plug: `secret:` is required (it refuses to start without), `allow_unsigned: true` to opt out; `Ankusa.SDK.Webhook.receive_delivery/3` without Plug |
| Clojure (`ankusa-clj`) | `(ankusa.sdk.webhook/receive-delivery headers body {:secrets [...]})`; `:allow-unsigned true` to opt out |

An opt-out on the sink (`allow_unsigned: true`) and on the receiver go
together. A receiver that opts out accepts anything, so keep it off any address
you do not control. The shared vectors are the `webhook.require_signature`
cases in `conformance/cases/receive.json`.

Every parsed-headers, decoded-message and received-delivery result also carries
how the node verified the hook at ingest: `{status, flagged}`, or `null` from a
node older than the field. `flagged` is `true` only when the node accepted a
failed check (`on_verify_failure: accept_flag`), which means the delivery is
unverified. Each SDK answers that in one call and fails closed: the hook is
trusted only for `status: "ok"` with `flagged: false`. A flagged, `failed` or
`skipped` hook, a status the SDK does not know, and a hook from a node older
than the field are all untrusted.

| SDK | Verification accessor | Trust check |
| --- | --- | --- |
| TypeScript (`ankusa`) | `parsed.verification` / `message.verification` / `received.verification` | `isTrusted(hook)` |
| Python (`ankusa`) | `parsed.verification` / `message.verification` / `received.verification` | `is_trusted(hook)` |
| Go | `parsed.Verification` / `message.Verification` / `received.Verification` | `hook.Trusted()` |
| Rust | `parsed.verification` / `message.verification` / `received.verification` | `hook.is_trusted()` |
| Ruby (`ankusa-sdk`) | `parsed.verification` / `message.verification` / `received.verification` | `hook.trusted?` |
| PHP | `$parsed->verification` / `$message->verification` / `$received->verification` | `$hook->isTrusted()` |
| Java | `parsed.verification()` / `message.verification()` / `received.verification()` | `hook.trusted()` |
| Elixir (`ankusa_sdk`) | `hook.verification` / `message.verification` / `received.verification` | `Ankusa.SDK.Hook.trusted?(hook)` |
| Clojure (`ankusa-clj`) | `(:verification parsed)` / `(:verification message)` / `(:verification received)` | `(ankusa.sdk.verification/trusted? hook)` |

The shared vectors are `conformance/cases/verification.json`: the
`message.verification` and `webhook.verification` features, including the
`trusted` operation, where a `flagged: true` message is untrusted in every SDK.

Elixir consumers get the whole contract as a `Plug`: `Ankusa.SDK.Receiver`
(Hex package
[`ankusa_sdk`](https://github.com/jamescarr/ankusa/tree/main/packages/sdk-elixir))
reads the raw body before any parser, parses the `x-ankusa-*` headers, calls
your `Ankusa.SDK.Handler` implementation, and answers `202` once it returns
`:ok`. It answers `503` when the handler returns `{:error, _}`, and `500` if it
raises or returns anything else; Ankusa retries both, which is exactly the
retry signal above. Mount it above `Plug.Parsers`:

```elixir
plug Ankusa.SDK.Receiver,
  path: "/deliveries",
  handler: MyApp.Hooks,
  secret: System.fetch_env!("DELIVERIES_WHSEC")

plug Plug.Parsers, parsers: [:json], json_decoder: JSON
```

The queue side has an equivalent: `Ankusa.SDK.Message.decode/1` parses
`Sink.Message` JSON, and `to_hook/2` redeems a claim check before handing the
same `Ankusa.SDK.Hook` to the same handler.

## Oban

[`examples/oban-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/oban-consumer/)
is the full worked deployment: a Kubernetes (kind) cluster running three
self-contained all-role Ankusa nodes, each with its own RocksDB store on a
persistent volume, and a two-replica consumer service that receives the HTTP
handoff and enqueues Oban jobs. It is proven zero-loss under normal load, chaos
pod kills, and burst.

Ankusa and its `ingest_app` wrapper know nothing about Oban. The consumer is the
only place Oban is imported, and it talks to Ankusa over plain HTTP.

### The `/deliveries` route

The router keys on the `x-ankusa-idempotency-key` header the sink ships, falling
back to the ankusa id for a sender that predates it. That key is the primary key
of a `processed_webhooks` row, and the Oban job is inserted in the same
transaction as the row.

```elixir
defp handle_delivery(conn, body) do
  case header(conn, "x-ankusa-id") do
    nil -> send_resp(conn, 400, JSON.encode!(%{error: "missing x-ankusa-id"}))
    ankusa_id -> insert_job(conn, ankusa_id, body)
  end
end

defp insert_job(conn, ankusa_id, body) do
  # The key Ankusa computed: read it, never rebuild it.
  key = header(conn, "x-ankusa-idempotency-key") || ankusa_id

  result =
    Repo.transaction(fn ->
      # `xmax = 0` means this insert created the row; the conflict path means
      # the hook already arrived, and no second job is queued for it.
      %Postgrex.Result{rows: [[inserted]]} =
        Repo.query!(
          """
          INSERT INTO processed_webhooks
            (idempotency_key, ankusa_id, source_id, tenant_id, body, body_sha256, deliveries)
          VALUES ($1, $2, $3, $4, $5, $6, 1)
          ON CONFLICT (idempotency_key) DO UPDATE
            SET deliveries = processed_webhooks.deliveries + 1
          RETURNING (xmax = 0) AS inserted
          """,
          [
            key,
            ankusa_id,
            header(conn, "x-ankusa-source") || "",
            header(conn, "x-ankusa-tenant"),
            body,
            :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)
          ]
        )

      if inserted do
        case %{"idempotency_key" => key} |> WebhookWorker.new() |> Oban.insert() do
          {:ok, %Oban.Job{id: id}} -> id
          {:error, reason} -> Repo.rollback({:job_insert, reason})
        end
      else
        :duplicate
      end
    end)

  case result do
    {:ok, job_id} when is_integer(job_id) ->
      send_resp(conn, 202, JSON.encode!(%{job_id: job_id, duplicate: false}))

    {:ok, :duplicate} ->
      send_resp(conn, 202, JSON.encode!(%{job_id: nil, duplicate: true}))

    {:error, _reason} ->
      send_resp(conn, 503, "")
  end
end
```

### `WebhookWorker`

```elixir
defmodule WebhookWorker do
  use Oban.Worker, queue: :webhooks, max_attempts: 10

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"idempotency_key" => key}}) do
    result =
      Repo.transaction(fn ->
        %Postgrex.Result{rows: rows} =
          Repo.query!(
            "SELECT processed_at FROM processed_webhooks WHERE idempotency_key = $1 FOR UPDATE",
            [key]
          )

        # Not yet processed: run the business effect here, inside the
        # transaction that marks it done. A pruned or already-processed row is
        # nothing to do.
        if rows == [[nil]] do
          Repo.query!(
            "UPDATE processed_webhooks SET processed_at = now() WHERE idempotency_key = $1",
            [key]
          )
        end
      end)

    case result do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
```

The `idempotency_key` primary key is what makes the `POST` idempotent. A second
delivery of the same hook, whether from a lost `2xx` or a DLQ replay, bumps
`deliveries` and queues no second job. The row, not Oban's job uniqueness, is
the dedupe, so pruning completed jobs can never let a retry or a replay re-run
the effect.

`processed_at` is what proves "processed" against actual attempts rather than
against enqueue alone. The `deliveries` column is what the end-to-end results
table in [`testing.md`](testing.md) reports as `extra deliveries`. Full source:
[`examples/oban-consumer/consumer_app`](https://github.com/jamescarr/ankusa/tree/main/examples/oban-consumer/consumer_app).

### In-process: embedding Ankusa and Oban in the same app

An app that embeds Ankusa directly, rather than fronting it with a separate HTTP
service, can skip the HTTP hop and write a sink that calls `Oban.insert/1`
in-process:

```elixir
defmodule MyApp.ObanSink do
  @behaviour Ankusa.Sink

  @impl true
  def deliver(env, ctx, _opts) do
    args = %{
      "idempotency_key" => Ankusa.Envelope.idempotency_key(env),
      "ankusa_id" => env.id,
      "source_id" => env.source_id,
      "tenant_id" => ctx.tenant_id,
      "content_type" => env.content_type,
      "body_base64" => Base.encode64(env.body)
    }

    case args |> MyApp.WebhookWorker.new(unique: [period: :infinity, keys: [:idempotency_key]]) |> Oban.insert() do
      {:ok, _job} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
```

```elixir
sinks: [{MyApp.ObanSink, []}]
```

The key is the same one, `Ankusa.Envelope.idempotency_key/1`, which is the value
`Sink.Http` ships as `x-ankusa-idempotency-key`. Only the transport changes
between Ankusa's dispatch pipeline and the enqueue: a function call instead of a
socket. Remember that Oban's `unique:` only dedupes against jobs still in the
table. With a pruner, either keep the key in your own table as the HTTP example
does, or set the unique period past the prune age.

## Celery

A minimal Flask endpoint gets you the same contract for a Python stack:

```python
from flask import Flask, request, jsonify
from tasks import process_webhook  # a Celery task, acks_late=True

app = Flask(__name__)

@app.post("/deliveries")
def deliveries():
    ankusa_id = request.headers.get("x-ankusa-id")
    if not ankusa_id:
        return jsonify(error="missing x-ankusa-id"), 400

    key = request.headers.get("x-ankusa-idempotency-key") or ankusa_id

    source = request.headers.get("x-ankusa-source")
    tenant = request.headers.get("x-ankusa-tenant")
    content_type = request.headers.get("content-type")
    body_b64 = base64.b64encode(request.get_data()).decode()

    try:
        process_webhook.apply_async(
            args=[key, ankusa_id, source, tenant, content_type, body_b64],
            task_id=key,
        )
    except Exception:
        # broker down, etc. Ankusa retries per dispatch.retry, then DLQs.
        return "", 503

    return "", 202
```

```elixir
sinks: [{Ankusa.Sink.Http, url: "https://jobs.internal/deliveries", secret: System.fetch_env!("DELIVERIES_WHSEC"), timeout_ms: 5_000}]
```

An `apply_async` that raises, because the broker is unreachable, surfaces as a
503, which Ankusa retries per `dispatch.retry` exactly like any other sink
failure.

The task is declared `acks_late=True`, so a worker crash mid-task redelivers
from the broker rather than silently dropping the delivery. On RabbitMQ, pair
that with `broker_transport_options={"confirm_publish": True}`, so `apply_async`
itself cannot silently succeed against a broker that never durably queued the
message.

Celery does not dedupe on `task_id` the way Oban's `unique:` does. A `task_id`
collision with Celery and Redis, the common combination, can raise depending on
the backend, but that is not a documented guarantee across every
broker/backend pair. Treat `task_id=key` as a debugging and traceability aid,
not a dedup mechanism, and make `process_webhook`'s body itself idempotent on
the idempotency key with an upsert, the way the Oban example's
`processed_webhooks` row does. That is the same at-least-once delivery,
idempotent consumer rule that applies to every sink on this page.

## Queue handoff

Consumers already on RabbitMQ, Kafka, NATS JetStream or Amazon SQS do not need
an HTTP hop at all. `Ankusa.Sink.RabbitMQ`, `Ankusa.Sink.Kafka`, `Ankusa.Sink.NATS`
and `Ankusa.Sink.SQS` publish `Ankusa.Sink.Message`, the same wire format on
every transport, directly to a broker or queue your worker fleet already
consumes from.

Redis pub/sub (`Ankusa.Sink.Redis`) publishes the same message with one
caveat: it is fan-out, not a queue. A worker that is down misses whatever was
published while it was away, and a publish that no live subscriber receives is
an error rather than a stored job. Use it for consumers that may miss messages,
not for durable work.

[`delivery.md`](delivery.md) has the full message contract and the
reconnection and confirm semantics. For end-to-end deployments where the worker
is itself the consumer, rather than a job-framework broker in between, see
[`examples/rabbitmq-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/rabbitmq-consumer/),
[`examples/nats-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/nats-consumer/),
[`examples/kafka-sqs-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/kafka-sqs-consumer/)
and [`examples/jetstream-k8s/`](https://github.com/jamescarr/ankusa/tree/main/examples/jetstream-k8s/).
