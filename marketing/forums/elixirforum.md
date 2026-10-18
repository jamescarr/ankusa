Title: Ankusa – a self-hosted webhook receiver (fsync before 2xx, retries, DLQ, replay; HTTP/RabbitMQ/Kafka/NATS/Redis sinks)
Category: Your Libraries & Projects › Libraries (https://elixirforum.com/c/your-libraries-os-mentoring/libraries/43)
Tags: webhooks, hex, otp
Status: ready

## What I built

I built Ankusa because I kept losing webhooks I had already received: a worker down for an hour, a database-backed inbox buckling under a burst. Ankusa answers `201 accepted` only after the hook is in a synced RocksDB batch on disk, then delivers it to your sinks (HTTP, RabbitMQ, Kafka, NATS JetStream, Redis) with retries, a dead-letter queue and durable replay jobs. It ships as a Docker image and as a Hex library you can embed in your own supervision tree. Everything is 0.x, so APIs may change.

## The Elixir angle

Add `{:ankusa, "~> 0.4"}` (use whatever version is on Hex when you read this). The RocksDB NIF builds from source, so you need cmake >= 3.12, a C++20 compiler, and zstd and OpenSSL development headers. The Docker image already has them.

A source is a config entry:

```elixir
config :ankusa,
  autostart: true,
  source_store:
    {Ankusa.SourceStore.Static,
     sources: %{
       "stripe" => [
         verifier: {Ankusa.Verifier.Hmac, scheme: :stripe, secret: System.get_env("STRIPE_WHSEC")},
         sinks: [{Ankusa.Sink.Http, url: "https://example.internal/stripe"}]
       ]
     }}
```

If you want to supervise it yourself, build a config with `Ankusa.Config.new/1` and call `Ankusa.Instance.start_link/1` from your own app. Which parts run is a runtime decision, via `ANKUSA_ROLES`:

```sh
ANKUSA_ROLES=edge,dispatch,storage mix run --no-halt   # the store-backed shape
ANKUSA_ROLES=claim_check mix run --no-halt             # claim-check gateway, any node
```

## Your own sink

`Ankusa.Sink` is a behaviour, and `deliver/3` is the only required callback. An app that embeds Ankusa can skip the HTTP hop and enqueue straight into Oban, keyed on the idempotency key so a redelivery inserts no second job:

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

## The OTP story

Ingest goes through a group-commit batcher GenServer per partition, and each caller blocks until the batch it landed in has committed. The instance supervisor is `:rest_for_one`: the core that acks hooks (store, source store, edge) is separate from dispatch, storage, metrics and the listeners, which each run under their own restart budget with backoff (`Ankusa.Instance.Isolated`). A sink that keeps raising stops dispatch, not the edge, and hooks wait in the store. Roles `edge`, `dispatch`, `storage` and `claim_check` decide which subtrees start.

## Limits I want you to know about

- Delivery is at-least-once. Receivers must be idempotent on the `x-ankusa-idempotency-key` header.
- Durability is to power loss on this host only. For a fleet, run N independent nodes behind a load balancer.
- The claim-check gateway, admin and route-admin ports (4001, 4002, 4003) are unauthenticated by design. They listen on 127.0.0.1 by default; front them with your proxy or network policy if you publish them.

## Links

- HexDocs: https://hexdocs.pm/ankusa
- Repo: https://github.com/jamescarr/ankusa
- Elixir guide: https://github.com/jamescarr/ankusa/blob/main/docs/elixir.md
- Architecture: https://github.com/jamescarr/ankusa/blob/main/docs/architecture.md
- A kind e2e with an Oban consumer: https://github.com/jamescarr/ankusa/tree/main/examples/oban-consumer

## What I want feedback on

- Are the shapes of the `Ankusa.Sink`, `Ankusa.Verifier` and `Ankusa.RouteResolver` behaviours right? Would you implement your own?
- Is `wal.type: none` direct mode useful if you run Broadway consumers? It publishes to the sinks in the request and answers `201` after every sink confirms, with no retry, DLQ or replay.

For the worker side there is `{:ankusa_sdk, "~> 0.3"}`, which includes an `Ankusa.SDK.Receiver` Plug.

Tell me what breaks: https://github.com/jamescarr/ankusa/issues
