defmodule Ankusa.SDK.Message do
  @moduledoc """
  The wire format every queue-style sink publishes (`Ankusa.Sink.RabbitMQ`,
  `Ankusa.Sink.Kafka`, `Ankusa.Sink.NATS`, `Ankusa.Sink.Redis`), and the bridge
  from one of those messages to an `Ankusa.SDK.Hook`.

  The SDK ships **no** broker client: the app brings its own Broadway/AMQP/
  brod/gnat/Redix consumer, hands each message body to `decode/1`, and calls
  `to_hook/2` — which redeems a claim check when the message carries one — before
  running the same handler the HTTP receiver runs.

  A body of at most the sink's `inline_max_bytes` (default 64 KiB) rides inline,
  base64-encoded:

      {"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme",
       "received_at": 1737500000000, "content_type": "application/json", "size": 245,
       "body_base64": "eyJpZCI6..."}

  Anything larger lives in the claim check and the message carries the ref
  instead, plus the lowercase hex sha256 `to_hook/2` verifies the redeemed bytes
  against:

      {"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme",
       "received_at": 1737500000000, "content_type": "application/json", "size": 3145728,
       "claim": "urn:ankusa:claim:v1:acme:01M39VMD8RA3C5HR4RBV67Y002",
       "sha256": "3bea8a9a07c1e8dc..."}

  `v` changes only when an existing field changes meaning or disappears, and
  consumers must ignore keys they don't know — so `decode/1` accepts a message
  with extra keys and rejects one whose `v` it does not speak.

  Kafka/NATS headers and the RabbitMQ routing key are not read: the JSON body
  carries everything.
  """

  alias Ankusa.SDK.{ClaimCheck, Hook, InvalidMessageError}

  @type t :: %__MODULE__{
          v: pos_integer(),
          id: String.t(),
          source_id: String.t() | nil,
          tenant_id: String.t() | nil,
          received_at: integer() | nil,
          content_type: String.t() | nil,
          size: non_neg_integer(),
          body: binary() | nil,
          claim: String.t() | nil,
          sha256: String.t() | nil
        }

  defstruct [
    :v,
    :id,
    :source_id,
    :tenant_id,
    :received_at,
    :content_type,
    :size,
    :body,
    :claim,
    :sha256
  ]

  @doc """
  Decode one message body.

  Returns `{:error, %Ankusa.SDK.InvalidMessageError{}}` for anything that isn't
  a v1 message; `error.reason` names exactly what was wrong.
  """
  @spec decode(binary()) :: {:ok, t()} | {:error, InvalidMessageError.t()}
  def decode(data) when is_binary(data) do
    with {:ok, value} <- decode_json(data),
         {:ok, raw} <- object(value),
         :ok <- version(raw),
         {:ok, message} <- fields(raw),
         {:ok, message} <- body(message, raw) do
      {:ok, message}
    end
  end

  @doc """
  Turn a decoded message into the `Ankusa.SDK.Hook` a handler takes, redeeming
  the claim check when the message carries one.

  A `Ankusa.SDK.ClaimCheck` client is required even for an inline message, so a
  consumer cannot forget to supply one and then fail the day a payload exceeds
  the sink's threshold. A redemption failure returns the claim-check error
  as-is: its `retryable` field is the ack/requeue decision.

  Delivery is at-least-once — the same hook id can arrive twice — so a handler
  still dedupes on `hook.id`.
  """
  @spec to_hook(t(), ClaimCheck.t()) :: {:ok, Hook.t()} | {:error, Exception.t()}
  def to_hook(%__MODULE__{body: body} = message, %ClaimCheck{}) when is_binary(body) do
    {:ok, hook(message, body)}
  end

  def to_hook(%__MODULE__{claim: claim, sha256: sha256} = message, %ClaimCheck{} = claim_check) do
    case ClaimCheck.redeem(claim_check, claim, sha256) do
      {:ok, body} -> {:ok, hook(message, body)}
      {:error, error} -> {:error, error}
    end
  end

  defp hook(message, body) do
    %Hook{
      id: message.id,
      source_id: message.source_id,
      tenant_id: message.tenant_id,
      content_type: message.content_type,
      body: body,
      received_at: message.received_at,
      size: message.size
    }
  end

  defp decode_json(data) do
    case JSON.decode(data) do
      {:ok, value} -> {:ok, value}
      {:error, _reason} -> {:error, invalid(data, :invalid_json)}
    end
  end

  defp object(value) when is_map(value), do: {:ok, value}
  defp object(value), do: {:error, invalid(value, :not_an_object)}

  defp version(%{"v" => 1}), do: :ok
  defp version(%{"v" => other}), do: {:error, invalid(other, {:unsupported_version, other})}
  defp version(_raw), do: {:error, invalid(nil, {:unsupported_version, nil})}

  defp fields(raw) do
    with {:ok, id} <- id(raw),
         {:ok, source_id} <- source_id(raw),
         {:ok, tenant_id} <- tenant_id(raw),
         {:ok, received_at} <- received_at(raw),
         {:ok, content_type} <- content_type(raw),
         {:ok, size} <- size(raw) do
      {:ok,
       %__MODULE__{
         v: 1,
         id: id,
         source_id: source_id,
         tenant_id: tenant_id,
         received_at: received_at,
         content_type: content_type,
         size: size
       }}
    end
  end

  defp id(%{"id" => id}) when is_binary(id) and id != "", do: {:ok, id}
  defp id(_raw), do: {:error, invalid_field("id")}

  defp source_id(%{"source_id" => source_id}) when is_binary(source_id), do: {:ok, source_id}
  defp source_id(_raw), do: {:error, invalid_field("source_id")}

  defp tenant_id(raw) do
    case Map.fetch(raw, "tenant_id") do
      {:ok, tenant_id} when is_binary(tenant_id) or is_nil(tenant_id) -> {:ok, tenant_id}
      :error -> {:ok, nil}
      {:ok, other} -> {:error, invalid(other, {:invalid_field, "tenant_id"})}
    end
  end

  defp received_at(%{"received_at" => received_at}) when is_integer(received_at),
    do: {:ok, received_at}

  defp received_at(_raw), do: {:error, invalid_field("received_at")}

  defp content_type(raw) do
    case Map.fetch(raw, "content_type") do
      {:ok, content_type} when is_binary(content_type) or is_nil(content_type) ->
        {:ok, content_type}

      :error ->
        {:ok, nil}

      {:ok, other} ->
        {:error, invalid(other, {:invalid_field, "content_type"})}
    end
  end

  defp size(%{"size" => size}) when is_integer(size) and size >= 0, do: {:ok, size}
  defp size(_raw), do: {:error, invalid_field("size")}

  # Which body form a message carries is decided by which keys are present:
  # both is ambiguous (a producer bug the reader should not guess about),
  # neither is incomplete.
  defp body(%__MODULE__{} = message, raw) do
    case {Map.has_key?(raw, "body_base64"), Map.has_key?(raw, "claim")} do
      {true, true} -> {:error, invalid(raw, :ambiguous_body)}
      {true, false} -> inline_body(message, raw["body_base64"])
      {false, true} -> claim_body(message, raw)
      {false, false} -> {:error, invalid(raw, :missing_body)}
    end
  end

  defp inline_body(message, body_base64) when is_binary(body_base64) do
    case Base.decode64(body_base64) do
      {:ok, body} -> {:ok, %{message | body: body}}
      :error -> {:error, invalid(body_base64, :invalid_body_base64)}
    end
  end

  defp inline_body(_message, other), do: {:error, invalid(other, {:invalid_field, "body_base64"})}

  defp claim_body(message, raw) do
    with {:ok, claim} <- claim(raw),
         {:ok, sha256} <- sha256(raw) do
      {:ok, %{message | claim: claim, sha256: sha256}}
    end
  end

  defp claim(raw) do
    case raw["claim"] do
      claim when is_binary(claim) -> {:ok, claim}
      other -> {:error, invalid(other, {:invalid_field, "claim"})}
    end
  end

  defp sha256(raw) do
    case raw["sha256"] do
      sha256 when is_binary(sha256) -> {:ok, sha256}
      _other -> {:error, invalid_field("sha256")}
    end
  end

  defp invalid_field(key), do: invalid(key, {:invalid_field, key})

  defp invalid(_value, reason) do
    %InvalidMessageError{message: "invalid message: #{inspect(reason)}", reason: reason}
  end
end
