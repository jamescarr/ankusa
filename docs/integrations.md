# Using Ankusa with a job framework, without coupling to one

Ankusa never knows your job system. This page is the seam every job
framework — Oban, Celery, Sidekiq, whatever — hangs off of, and two worked
integrations (Oban, Celery) to copy from.

## The seam

The handoff is an `Ankusa.Sink`, and delivery is at-least-once:

```elixir
@callback deliver(Envelope.t(), ctx(), opts :: keyword()) :: :ok | {:error, term()}
```

Return `:ok` only once you're certain the hook was actually handled durably —
a job enqueued in a transaction that's about to commit counts; a job merely
constructed in memory doesn't. `{:error, reason}` triggers the source's
`Ankusa.RetryPolicy`: Ankusa retries, then dead-letters (see
[`delivery.md`](delivery.md)). A sink never talks to a job framework's SDK
directly unless it's your own in-process sink (see the Oban section below);
the shipped sinks (`Sink.Log`, `Sink.Http`, `Sink.RabbitMQ`, `Sink.Kafka`,
`Sink.NATS`) are all framework-neutral.

## HTTP handoff (any language)

The most common shape, and the one every worked example on this page uses:
point `Sink.Http` at your service's own endpoint.

```elixir
sinks: [{Ankusa.Sink.Http, url: "https://jobs.internal/deliveries", timeout_ms: 5_000}]
```

**The contract:**

- The raw body, verbatim, exactly as the provider sent it.
- Headers: `x-ankusa-id`, `x-ankusa-source`, `x-ankusa-tenant` (when the
  source has a tenant), `x-ankusa-seq`, and `content-type`.
- Respond `2xx` only after the job is durably enqueued — a `202` after an
  in-transaction insert commits, not before.
- A non-`2xx` response or a timeout is retried per `dispatch.retry`, then
  dead-lettered (see [`delivery.md`](delivery.md)).
- Dedupe on `x-ankusa-id` — the receiver in front of dispatch already
  collapsed provider retries, but at-least-once delivery means your
  endpoint can still see the same `x-ankusa-id` twice (a `2xx` that was lost
  in transit, a dispatch retry after a timeout that actually succeeded).
  Your enqueue MUST be idempotent on this id — see the worked examples below
  for how (a unique job key in Oban, `task_id=` in Celery).

## Oban

[`examples/oban-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/oban-consumer/)
is the full worked deployment: a Kubernetes (kind) cluster running an Ankusa
edge fleet, a singleton dispatch/storage worker, and a two-replica consumer
service that receives the HTTP handoff and enqueues Oban jobs — proven
zero-loss under normal load, chaos pod kills, and burst. Ankusa and its
`ingest_app` wrapper know nothing about Oban; the consumer is the only place
Oban is imported, and it talks to Ankusa over plain HTTP.

### `/deliveries` route

```elixir
defp handle_delivery(conn, body) do
  case header(conn, "x-ankusa-id") do
    nil -> send_resp(conn, 400, JSON.encode!(%{error: "missing x-ankusa-id"}))
    ankusa_id -> insert_job(conn, ankusa_id, body)
  end
end

defp insert_job(conn, ankusa_id, body) do
  args = %{
    "ankusa_id" => ankusa_id,
    "source_id" => header(conn, "x-ankusa-source"),
    "tenant_id" => header(conn, "x-ankusa-tenant"),
    "content_type" => header(conn, "content-type"),
    "body_base64" => Base.encode64(body)
  }

  args
  |> WebhookWorker.new(unique: [period: :infinity, keys: [:ankusa_id]])
  |> Oban.insert()
  |> case do
    # Oban's `%Oban.Job{}` carries a `conflict?` boolean: when the `unique:`
    # key matches an already-inserted job, `Oban.insert/1` still returns
    # `{:ok, job}`, but `job` is the *existing* row and `conflict?` is
    # `true` — the documented way to tell "already queued" from "brand new"
    # without a second query.
    {:ok, %Oban.Job{id: id, conflict?: conflict?}} ->
      send_resp(conn, 202, JSON.encode!(%{job_id: id, duplicate: conflict?}))

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
  def perform(%Oban.Job{args: args}) do
    body = Base.decode64!(args["body_base64"])
    sha256 = :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)

    Repo.query!(
      "INSERT INTO processed_webhooks (ankusa_id, source_id, tenant_id, body_sha256, deliveries, processed_at) " <>
        "VALUES ($1,$2,$3,$4,1,now()) ON CONFLICT (ankusa_id) DO UPDATE SET deliveries = processed_webhooks.deliveries + 1",
      [args["ankusa_id"], args["source_id"], args["tenant_id"], sha256]
    )

    :ok
  end
end
```

`unique: [period: :infinity, keys: [:ankusa_id]]` is what makes the `POST`
idempotent on `x-ankusa-id`: a second enqueue with the same id never creates
a second job. The `ON CONFLICT ... DO UPDATE SET deliveries = deliveries + 1`
in `perform/1` is a second, independent idempotency layer — it's what proves
"processed" against actual attempts rather than just against enqueue, and is
what the `deliveries` column in `docs/testing.md`'s results table measures.
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
      "ankusa_id" => env.id,
      "source_id" => env.source_id,
      "tenant_id" => ctx.tenant_id,
      "content_type" => env.content_type,
      "body_base64" => Base.encode64(env.body)
    }

    case args |> MyApp.WebhookWorker.new(unique: [period: :infinity, keys: [:ankusa_id]]) |> Oban.insert() do
      {:ok, _job} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
```

```elixir
sinks: [{MyApp.ObanSink, []}]
```

Same idempotency key (`:ankusa_id`), same `Oban.Worker` shape as the HTTP
example — the only thing that changes is the transport between Ankusa's
dispatch pipeline and the enqueue: a function call instead of a socket.

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

    source = request.headers.get("x-ankusa-source")
    tenant = request.headers.get("x-ankusa-tenant")
    content_type = request.headers.get("content-type")
    body_b64 = base64.b64encode(request.get_data()).decode()

    try:
        process_webhook.apply_async(
            args=[ankusa_id, source, tenant, content_type, body_b64],
            task_id=ankusa_id,
        )
    except Exception:
        # broker down, etc. — Ankusa retries per dispatch.retry, then DLQs.
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

**Celery does not dedupe on `task_id`** the way Oban's `unique:` does — a
`task_id` collision with Celery+Redis (the common combination) can raise
depending on backend, but isn't a documented guarantee across all
broker/backend pairs. Treat `task_id=ankusa_id` as a debugging/traceability
aid, not a dedup mechanism, and make `process_webhook`'s body itself
idempotent on `ankusa_id` (an upsert, same as `WebhookWorker.perform/1`
above) — the same "at-least-once delivery, idempotent consumer" rule that
applies to every sink on this page.

## Queue handoff

Consumers already on RabbitMQ, Kafka, or NATS JetStream don't need an HTTP hop
at all — `Ankusa.Sink.RabbitMQ`, `Ankusa.Sink.Kafka`, and `Ankusa.Sink.NATS`
publish `Ankusa.Sink.Message` (the same wire format on all three transports)
directly to a broker your worker fleet already consumes from. See
[`delivery.md`](delivery.md) for the full
message contract and reconnection/confirm semantics, and the two existing
worked examples —
[`examples/rabbitmq-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/rabbitmq-consumer/)
and
[`examples/kafka-sqs-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/kafka-sqs-consumer/)
— for end-to-end deployments where the worker is itself the consumer (rather
than a job-framework broker in between).
