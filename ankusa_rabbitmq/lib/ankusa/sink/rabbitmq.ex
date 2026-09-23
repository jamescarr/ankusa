defmodule Ankusa.Sink.RabbitMQ do
  @moduledoc """
  Publishes delivered hooks to a RabbitMQ **exchange**. Never touches a queue
  — binding a queue to the exchange, and everything downstream of that, is
  the consumer's job, not this sink's. That mirrors real AMQP topology
  ownership: producers own exchanges, consumers own their own queues.

  Messages are small on purpose. A body under `:inline_max_bytes` (default
  8 KiB) rides along base64-encoded; anything larger is checked in through
  `Ankusa.ClaimCheck` (the instance's configured `claim_check.adapter` — see
  `docs/claim-check.md`) and the message carries a claim ticket instead. This
  is the same "small hot path, big payloads elsewhere" principle the
  WAL/segment design already applies, now extended to the queue: RabbitMQ
  throughput and memory stay flat regardless of how large a webhook payload
  is, and any consumer — BEAM or not — redeems the ticket without needing
  blob-store credentials of its own.

  ## Message shape (JSON)

      {
        "id": "01a0...", "source_id": "stripe", "tenant_id": "acme",
        "received_at": 1737500000000, "content_type": "application/json", "size": 245,
        "body_base64": "eyJpZCI6...."           // inline, when size <= threshold
      }

      {
        "id": "01a0...", "source_id": "stripe", "tenant_id": "acme",
        "received_at": 1737500000000, "content_type": "application/octet-stream", "size": 3145728,
        "claim": {"v": 1, "tenant_id": "acme", "id": "01a0...", "size": 3145728,
                  "sha256": "9f86d0...", "content_type": "application/octet-stream"}
      }

  ## opts

    * `:exchange`          — required
    * `:exchange_type`     — default `:topic`
    * `:url`                — AMQP URL, default `"amqp://guest:guest@localhost:5672"`
    * `:routing_key`       — a static string, or a 1-arity fun `(Envelope.t() -> String.t())`;
                              default `"ankusa.\#{source_id}"`
    * `:inline_max_bytes`  — default `8_192`
    * `:retry_ms`          — reconnect backoff, default `5_000`
    * `:confirm_timeout_ms` — publisher-confirm wait, default `5_000`
  """

  @behaviour Ankusa.Sink

  alias Ankusa.Envelope
  alias Ankusa.Sink.RabbitMQ.Connection

  @impl true
  def deliver(%Envelope{} = env, ctx, opts) do
    exchange = Keyword.fetch!(opts, :exchange)

    with {:ok, name} <- ensure_started(ctx.instance, exchange, opts),
         {:ok, payload} <- build_payload(env, ctx, opts) do
      Connection.publish(name, routing_key(env, opts), payload)
    end
  end

  # ── connection lifecycle ────────────────────────────────────────────────

  defp ensure_started(instance, exchange, opts) do
    name = Ankusa.via(instance, {:rabbitmq_conn, exchange})

    child = %{
      id: {Connection, instance, exchange},
      start: {Connection, :start_link, [Keyword.merge(opts, instance: instance, name: name)]}
    }

    case DynamicSupervisor.start_child(Ankusa.Sink.RabbitMQ.Supervisor, child) do
      {:ok, _pid} -> {:ok, name}
      {:error, {:already_started, _pid}} -> {:ok, name}
      {:error, reason} -> {:error, reason}
    end
  end

  # ── message building ────────────────────────────────────────────────────

  defp build_payload(env, ctx, opts) do
    threshold = Keyword.get(opts, :inline_max_bytes, 8_192)

    base = %{
      id: env.id,
      source_id: env.source_id,
      tenant_id: env.tenant_id,
      received_at: env.received_at,
      content_type: env.content_type,
      size: env.size
    }

    if env.size <= threshold do
      {:ok, JSON.encode!(Map.put(base, :body_base64, Base.encode64(env.body)))}
    else
      case check_in_claim(env, ctx) do
        {:ok, ticket} ->
          {:ok, JSON.encode!(Map.put(base, :claim, Ankusa.ClaimCheck.Ticket.to_map(ticket)))}

        {:error, reason} ->
          {:error, {:claim_check, reason}}
      end
    end
  end

  defp check_in_claim(env, ctx) do
    meta = %{tenant_id: env.tenant_id, id: env.id, content_type: env.content_type}
    Ankusa.ClaimCheck.check_in(ctx.instance, env.body, meta)
  end

  defp routing_key(env, opts) do
    case Keyword.get(opts, :routing_key, &default_routing_key/1) do
      key when is_binary(key) -> key
      fun when is_function(fun, 1) -> fun.(env)
    end
  end

  defp default_routing_key(env), do: "ankusa.#{env.source_id}"
end
