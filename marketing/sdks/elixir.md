Subreddit: r/elixir
Title: Ankusa: self-hosted webhook receiver on OTP — fsync before 2xx, RabbitMQ/Kafka/NATS/Redis sinks, Hex library or Docker image
Flair: none
Status: ready

This is my own project, so read it as a launch post. I built Ankusa, a self-hosted webhook receiver on OTP that never answers `2xx` until the hook is durably accepted. After that it retries to your sinks (HTTP, RabbitMQ, Kafka, NATS, Redis), dead-letters on give-up, and replays on demand. Run the Docker image, or embed it: it is a Hex library, with `Ankusa.Sink`, `Ankusa.Verifier` and `Ankusa.RouteResolver` as behaviours. Building from source needs cmake, a C++20 compiler, and zstd and OpenSSL headers for the RocksDB NIF; the image already has them.

The OTP part: one group-commit batcher `GenServer` per partition, a synced RocksDB commit per batch, and a supervisor that restarts dispatch, storage and the listeners under their own budgets instead of taking the instance down.

For the worker side there is `ankusa_sdk`. `Ankusa.SDK.Receiver` is a `Plug` that hands each HTTP-sink delivery to your handler module, standalone under Bandit or mounted above the body parsers in a Phoenix endpoint. It does not verify provider signatures, because Ankusa does that at the edge, and it ships no broker client, so bring your own. It passes the same 131 shared conformance cases as the other seven SDKs.

Delivery is at-least-once, so dedupe on `x-ankusa-idempotency-key`. Everything is 0.x and APIs may change.

In a Kubernetes chaos run where I deleted two Ankusa pods and a consumer pod mid-load, every phase, including chaos, reported `missing: 0` (measured on an Apple M4 Pro laptop under kind/OrbStack, not a production-scale claim).

I want feedback on the shape of those three behaviours.

```
{:ankusa, "~> 0.4"}
{:ankusa_sdk, "~> 0.3"}

defmodule MyApp.Hooks do
  @behaviour Ankusa.SDK.Handler

  @impl Ankusa.SDK.Handler
  def handle_hook(%Ankusa.SDK.Hook{} = hook, _arg) do
    # Return :ok only once the hook is durably handled; delivery is
    # at-least-once, so dedupe on the idempotency key
    # (Ankusa.SDK.Idempotency.key/2), not on the arrival alone.
    case MyApp.Store.insert(hook.id, hook.body) do
      :inserted -> :ok
      :duplicate -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
```

SDK README: https://github.com/jamescarr/ankusa/blob/main/packages/sdk-elixir/README.md
HexDocs: https://hexdocs.pm/ankusa
Launch post: {{BLOG_URL}}/ankusa-launch
Repo: https://github.com/jamescarr/ankusa
