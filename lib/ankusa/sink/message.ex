defmodule Ankusa.Sink.Message do
  @moduledoc """
  Canonical wire message format for queue-style sinks (RabbitMQ, Kafka, SQS).

  This is the byte-identical JSON message shape across every transport: the same
  payload rides AMQP, Kafka, and a bridge into SQS. Consumers parse one format,
  not three.

  ## Message shape

  Small payloads (≤ `inline_max_bytes`, default 8 KiB) ride along base64-encoded.
  Large ones are claim-checked into the blob store and the message carries a ticket.

      {
        "v": 1,
        "id": "01a0b1c2d3...",
        "source_id": "stripe",
        "tenant_id": "acme",
        "received_at": 1737500000000,
        "content_type": "application/json",
        "size": 245,
        "body_base64": "eyJpZCI6..."
      }

  or

      {
        "v": 1,
        "id": "01a0b1c2d3...",
        "source_id": "stripe",
        "tenant_id": "acme",
        "received_at": 1737500000000,
        "content_type": "application/json",
        "size": 524288,
        "claim": {
          "v": 1,
          "tenant_id": "acme",
          "id": "01a0b1c2d3...",
          "size": 524288,
          "sha256": "d4e5f6...",
          "content_type": "application/json"
        }
      }

  ## Version indicator

  `"v": 1` is **additive**. Existing consumers ignore unknown keys, so a `v2`
  message with new fields is still readable by a `v1` consumer. Breaking changes
  increment the version and require consumer updates. This module guards the
  contract.

  ## Size ceiling (documented, not enforced)

  Base64 inflates by 4/3, so `inline_max_bytes × 4/3 + ~1 KiB` overhead must stay
  under the smallest limit on the path: Kafka `max.message.bytes` (1 MiB default)
  and the SQS message size limit (256 KiB). The 8 KiB default leaves plenty of
  room. If a message exceeds the transport's limit, the sink fails with
  `{:error, reason}` and the retry policy takes over (then the DLQ if retries
  exhaust).
  """

  alias Ankusa.Envelope

  @doc """
  Encode an envelope into the canonical JSON wire format.

  - `env`: the envelope to encode
  - `ctx`: the sink context (instance name for claim check)
  - `inline_max_bytes`: threshold; larger payloads are claim-checked

  Returns `{:ok, json_binary}` or `{:error, {:claim_check, reason}}` if the
  claim check fails.
  """
  @spec encode(Envelope.t(), ctx :: map(), inline_max_bytes :: pos_integer()) ::
          {:ok, binary()} | {:error, {:claim_check, Ankusa.ClaimCheck.reason()}}
  def encode(%Envelope{} = env, ctx, inline_max_bytes) when inline_max_bytes > 0 do
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
      with {:ok, ticket} <- check_in_claim(env, ctx) do
        {:ok, JSON.encode!(Map.put(base, :claim, Ankusa.ClaimCheck.Ticket.to_map(ticket)))}
      end
    end
  end

  defp check_in_claim(env, ctx) do
    meta = %{tenant_id: env.tenant_id, id: env.id, content_type: env.content_type}
    Ankusa.ClaimCheck.check_in(ctx.instance, env.body, meta)
  end
end
