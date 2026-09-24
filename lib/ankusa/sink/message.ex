defmodule Ankusa.Sink.Message do
  @moduledoc """
  The wire format every queue-style sink publishes (`Sink.RabbitMQ`,
  `Sink.Kafka`, `Sink.NATS`), so a consumer parses one format regardless of
  transport.

  A body of at most `inline_max_bytes` rides inline, base64-encoded. Anything
  larger is checked in through `Ankusa.ClaimCheck` and the message carries the
  ticket instead (see `docs/claim-check.md`):

      {"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme",
       "received_at": 1737500000000, "content_type": "application/json", "size": 245,
       "body_base64": "eyJpZCI6..."}

      {"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme",
       "received_at": 1737500000000, "content_type": "application/octet-stream", "size": 3145728,
       "claim": {"v": 1, "tenant_id": "acme", "id": "01a0...", "size": 3145728,
                 "sha256": "9f86d0...", "content_type": "application/octet-stream"}}

  `v` changes only when an existing field changes meaning or disappears.
  Adding a field keeps `v: 1`; consumers must ignore keys they don't know.

  Base64 inflates the inline body by 4/3, so `inline_max_bytes * 4/3` plus
  ~1 KiB of envelope must stay under the smallest message limit on the path
  (Kafka `max.message.bytes`, 1 MiB by default; NATS `max_payload`, 1 MiB;
  SQS, 256 KiB). The 8 KiB default is far below all of them. Nothing here
  enforces it: an oversized message
  fails at the broker and goes through the source's retry policy like any
  other sink error.
  """

  alias Ankusa.{ClaimCheck, Envelope}

  @spec encode(Envelope.t(), Ankusa.Sink.ctx(), pos_integer()) ::
          {:ok, binary()} | {:error, {:claim_check, ClaimCheck.reason()}}
  def encode(%Envelope{} = env, ctx, inline_max_bytes)
      when is_integer(inline_max_bytes) and inline_max_bytes > 0 do
    base = %{
      v: 1,
      id: env.id,
      source_id: env.source_id,
      tenant_id: env.tenant_id,
      received_at: env.received_at,
      content_type: env.content_type,
      size: env.size
    }

    if env.size <= inline_max_bytes do
      {:ok, JSON.encode!(Map.put(base, :body_base64, Base.encode64(env.body)))}
    else
      meta = %{tenant_id: env.tenant_id, id: env.id, content_type: env.content_type}

      case ClaimCheck.check_in(ctx.instance, env.body, meta) do
        {:ok, ticket} ->
          {:ok, JSON.encode!(Map.put(base, :claim, ClaimCheck.Ticket.to_map(ticket)))}

        {:error, reason} ->
          {:error, {:claim_check, reason}}
      end
    end
  end
end
