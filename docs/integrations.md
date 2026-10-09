# Using Ankusa with a job framework, without coupling to one

Ankusa never knows your job system. This page is the seam every job
framework, Oban, Celery, Sidekiq, whatever, hangs off of, and two worked
integrations (Oban, Celery) to copy from.

## The seam

The handoff is an `Ankusa.Sink`, and delivery is at-least-once:

```elixir
@callback deliver(Envelope.t(), ctx(), opts :: keyword()) :: :ok | {:error, term()}
```

Return `:ok` only once you're certain the hook was actually handled durably:
a job enqueued in a transaction that's about to commit counts; a job merely
constructed in memory doesn't. `{:error, reason}` triggers the source's
`Ankusa.RetryPolicy`: Ankusa retries, then dead-letters (see
[`delivery.md`](delivery.md)). A sink never talks to a job framework's SDK
directly unless it's your own in-process sink (see the Oban section below);
the shipped sinks (`Sink.Log`, `Sink.Http`, `Sink.RabbitMQ`, `Sink.Kafka`,
`Sink.NATS`, `Sink.Redis`) are all framework-neutral.

## HTTP handoff (any language)

The most common shape, and the one every worked example on this page uses:
point `Sink.Http` at your service's own endpoint.

```elixir
sinks: [{Ankusa.Sink.Http, url: "https://jobs.internal/deliveries", timeout_ms: 5_000}]
```

**The contract:**

- The raw body, verbatim, exactly as the provider sent it.
- Headers: `x-ankusa-id`, `x-ankusa-source`, `x-ankusa-idempotency-key`,
  `x-ankusa-tenant` (when the source has a tenant), and `content-type`;
  `x-ankusa-dedupe-key` and `x-ankusa-replay-id` when the hook carries them.
- Respond `2xx` only after the job is durably enqueued: a `202` after an
  in-transaction insert commits, not before.
- `408`, `429`, any other `4xx` not listed next, `5xx` and a timeout are
  retried per `dispatch.retry` (no sooner than a `Retry-After` you send), then
  dead-lettered; `400`, `401`, `403`, `404`, `410`, `413` and `422` are
  permanent and dead-letter at once — fix the cause and replay the DLQ (see
  [`delivery.md`](delivery.md)). Only the status is read: the sink reads at
  most `max_response_bytes` (64 KiB) of your response body.
- Dedupe on `x-ankusa-idempotency-key`: consumers are idempotent receivers.
  Delivery is at-least-once, so your endpoint can see the same key twice (a
  `2xx` that was lost in transit, a dispatch retry after a timeout that
  actually succeeded). Ankusa computes the key once per hook:
  `tenant:source_id:dedupe_key` when the source extracts the provider's event
  key, else the hook's `x-ankusa-id`. Read it; don't rebuild it. Your enqueue
  MUST be idempotent on it: see the worked examples below for how (a unique
  row keyed on it in the Oban example, an idempotent task body in Celery).
- Provider retries arrive as **distinct hooks** with distinct `x-ankusa-id`s
  unless the source sets `dedupe`, which collapses them at ingest into one
  hook (and so one key). Without it, dedupe those on the provider's own event
  id in the body (e.g. Stripe's `id`).
- Provider request headers (`X-GitHub-Delivery`, `webhook-id`, …) are
  forwarded by `Sink.Http` per the source's `forward_headers` option (and
  carried in `Sink.Message`'s `headers`), minus authentication and framing
  headers and every `x-ankusa-*` name. They arrive beside the `x-ankusa-*`
  headers above; when the source extracts one as its dedupe key it is also
  `x-ankusa-dedupe-key`. With a signing `secret` (below) the sink's own
  `webhook-id`, `webhook-timestamp` and `webhook-signature` replace any
  forwarded headers of those names.

### Signed deliveries

Give the sink a `secret` and every delivery carries a
[Standard Webhooks](https://www.standardwebhooks.com/) signature:
`webhook-id` (the hook id), `webhook-timestamp` (unix seconds at send time)
and `webhook-signature` (`v1,<base64 HMAC-SHA256>` over
`<webhook-id>.<webhook-timestamp>.<raw body>`; one entry per secret while you
rotate).

```yaml
sinks:
  - type: http
    url: https://jobs.internal/deliveries
    secret: ${DELIVERIES_WHSEC}          # whsec_ + base64, or a list while rotating
```

Every SDK verifies it the same way — `whsec_` secrets decode from base64, any
other string is its own bytes; a constant-time compare; a 300-second timestamp
window by default — and fails with a never-retryable `InvalidSignatureError`
(`code`: `invalid_secret`, `missing_header`, `invalid_timestamp`,
`timestamp_out_of_tolerance`, `no_matching_signature`). Answer `401`: it
dead-letters at once, and a fixed secret plus a DLQ replay redelivers.

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

Verify the raw bytes, before any JSON parser touches them. The shared vectors
are `conformance/cases/signature.json`.

Elixir consumers get this contract as a `Plug`: `Ankusa.SDK.Receiver` (Hex
package
[`ankusa_sdk`](https://github.com/jamescarr/ankusa/tree/main/packages/sdk-elixir))
reads the raw body before any parser, parses the `x-ankusa-*` headers, calls
your `Ankusa.SDK.Handler` implementation, and answers `202` once it returns
`:ok` — or `503` when it returns `{:error, _}`, and `500` if it raises or
returns anything else; Ankusa retries both, exactly the retry signal above.
Mount it above `Plug.Parsers`:

```elixir
plug Ankusa.SDK.Receiver, path: "/deliveries", handler: MyApp.Hooks
plug Plug.Parsers, parsers: [:json], json_decoder: JSON
```

The queue side has an equivalent: `Ankusa.SDK.Message.decode/1` parses
`Sink.Message` JSON and `to_hook/2` redeems a claim check before handing the
same `Ankusa.SDK.Hook` to the same handler.

## Oban

[`examples/oban-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/oban-consumer/)
is the full worked deployment: a Kubernetes (kind) cluster running three
self-contained all-role Ankusa nodes (each with its own RocksDB store on a
persistent volume) and a two-replica consumer service that receives the HTTP
handoff and enqueues Oban jobs, proven zero-loss under normal load, chaos pod
kills, and burst. Ankusa and its `ingest_app` wrapper know nothing about Oban;
the consumer is the only place Oban is imported, and it talks to Ankusa over
plain HTTP.

### `/deliveries` route

The router keys on the `x-ankusa-idempotency-key` header the sink ships (the
ankusa id when a sender predates it). That key is the primary key of a
`processed_webhooks` row, and the Oban job is inserted in the same transaction
as the row.

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

The `idempotency_key` primary key is what makes the `POST` idempotent: a second
delivery of the same hook (a lost `2xx`, a DLQ replay) bumps `deliveries` and
queues no second job. The row, not Oban's job uniqueness, is the dedupe, so
pruning completed jobs can never let a retry or a replay re-run the effect.
`processed_at` is what proves "processed" against actual attempts rather than
just against enqueue, and the `deliveries` column is what `docs/testing.md`'s
results table measures.
Full source: [`examples/oban-consumer/consumer_app`](https://github.com/jamescarr/ankusa/tree/main/examples/oban-consumer/consumer_app).

### In-process: embedding Ankusa and Oban in the same app

An app that embeds Ankusa directly (rather than fronting it with a separate
HTTP service) can skip the HTTP hop and write a sink that calls `Oban.insert/1`
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

Same idempotency key (`Ankusa.Envelope.idempotency_key/1`, the value `Sink.Http`
ships as `x-ankusa-idempotency-key`), and the transport is the only thing that
changes between Ankusa's dispatch pipeline and the enqueue: a function call
instead of a socket. Oban's `unique:` only dedupes against jobs still in the
table, so with a pruner either keep the key in your own table as the HTTP
example does or set the unique period past the prune age.

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
sinks: [{Ankusa.Sink.Http, url: "https://jobs.internal/deliveries", timeout_ms: 5_000}]
```

`apply_async` raising (broker unreachable) surfaces as a 5xx, which Ankusa
retries per `dispatch.retry` exactly like any other sink failure. The task
itself is declared `acks_late=True`, so a worker crash mid-task redelivers
from the broker rather than silently dropping it; on RabbitMQ, pair that with
`broker_transport_options={"confirm_publish": True}` so `apply_async` itself
doesn't silently succeed against a broker that never durably queued the
message.

**Celery does not dedupe on `task_id`** the way Oban's `unique:` does: a
`task_id` collision with Celery+Redis (the common combination) can raise
depending on backend, but isn't a documented guarantee across all
broker/backend pairs. Treat `task_id=key` as a debugging/traceability
aid, not a dedup mechanism, and make `process_webhook`'s body itself
idempotent on the idempotency key (an upsert, same as the Oban example's
`processed_webhooks` row above), the same "at-least-once delivery, idempotent
consumer" rule that applies to every sink on this page.

## Queue handoff

Consumers already on RabbitMQ, Kafka, or NATS JetStream don't need an HTTP hop
at all: `Ankusa.Sink.RabbitMQ`, `Ankusa.Sink.Kafka`, and `Ankusa.Sink.NATS`
publish `Ankusa.Sink.Message` (the same wire format on all three transports)
directly to a broker your worker fleet already consumes from. Redis pub/sub
(`Ankusa.Sink.Redis`) publishes the same message, with a caveat: it is
fan-out, not a queue. A worker that is down misses whatever was published
while it was away, and a publish no live subscriber receives is an error
rather than a stored job — use it for consumers that may miss messages, not
for durable work. See
[`delivery.md`](delivery.md) for the full
message contract and reconnection/confirm semantics, and the three existing
worked examples,
[`examples/rabbitmq-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/rabbitmq-consumer/),
[`examples/nats-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/nats-consumer/)
and
[`examples/kafka-sqs-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/kafka-sqs-consumer/),
for end-to-end deployments where the worker is itself the consumer (rather
than a job-framework broker in between).
