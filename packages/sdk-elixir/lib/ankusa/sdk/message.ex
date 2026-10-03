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
  base64-encoded, and a larger body travels as a claim ref:

      {"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme",
       "received_at": 1737500000000, "content_type": "application/json", "size": 245,
       "sha256": "2cf24dba5fb0a30e...", "dedupe_key": "evt_1", "replay_id": null,
       "headers": {"x-github-event": "push"}, "body_base64": "eyJpZCI6..."}

      {"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme",
       "received_at": 1737500000000, "content_type": "application/json", "size": 3145728,
       "sha256": "3bea8a9a07c1e8dc...", "dedupe_key": null, "replay_id": null,
       "headers": {}, "claim": "urn:ankusa:claim:v1:acme:01M39VMD8RA3C5HR4RBV67Y002"}

  `sha256` is the lowercase hex SHA-256 of the body; `dedupe_key` is the
  provider's event key when the source extracted one; `replay_id` names the
  replay job when this delivery is a replay of an older one; `headers` are the
  provider request headers the sink forwarded (lowercased, `""` joined).

  `v` changes only when an existing field changes meaning or disappears, and
  consumers must ignore keys they don't know — so `decode/1` accepts a message
  with extra keys and rejects one whose `v` it does not speak.

  ## Integrity

  `decode/1` checks what it can without the claim store: an inline body's
  length against `size` and its bytes against `sha256`, and a claim ref's tenant
  against `tenant_id`. A failure is an `Ankusa.SDK.InvalidMessageError` with a
  `code` (`invalid_json`, `not_an_object`, `unsupported_version`,
  `invalid_field`, `ambiguous_body`, `missing_body`, `invalid_body_base64`,
  `size_mismatch`, `integrity`, `tenant_mismatch`) and, for `invalid_field`, the
  offending `field`.

  Kafka/NATS headers and the RabbitMQ routing key are not read: the JSON body
  carries everything.
  """

  alias Ankusa.SDK.{ClaimCheck, ClaimRef, Hook, InvalidMessageError}

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
          sha256: String.t() | nil,
          dedupe_key: String.t() | nil,
          replay_id: String.t() | nil,
          headers: %{String.t() => String.t()}
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
    :sha256,
    :dedupe_key,
    :replay_id,
    headers: %{}
  ]

  @sha256_pattern ~r/\A[0-9a-f]{64}\z/

  @doc """
  Decode one message body.

  Returns `{:error, %Ankusa.SDK.InvalidMessageError{}}` for anything that isn't
  a valid v1 message; `error.code` and `error.field` name exactly what was
  wrong (`error.reason` carries the same as an Elixir term).
  """
  @spec decode(binary()) :: {:ok, t()} | {:error, InvalidMessageError.t()}
  def decode(data) when is_binary(data) do
    with {:ok, value} <- decode_json(data),
         {:ok, raw} <- object(value),
         :ok <- version(raw),
         {:ok, message} <- fields(raw),
         {:ok, message} <- body(message, raw) do
      integrity(message)
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
  still dedupes on the key in `Ankusa.SDK.Idempotency.key/2`.
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
      size: message.size,
      dedupe_key: message.dedupe_key,
      replay_id: message.replay_id,
      headers: message.headers
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

  # The contract fixes this order: the first bad field wins, and its name is the
  # one the error reports.
  defp fields(raw) do
    with {:ok, id} <- id(raw),
         {:ok, source_id} <- source_id(raw),
         {:ok, received_at} <- received_at(raw),
         {:ok, size} <- size(raw),
         {:ok, tenant_id} <- optional_string(raw, "tenant_id"),
         {:ok, content_type} <- optional_string(raw, "content_type"),
         {:ok, dedupe_key} <- optional_string(raw, "dedupe_key"),
         {:ok, replay_id} <- optional_string(raw, "replay_id"),
         {:ok, headers} <- headers(raw),
         {:ok, sha256} <- sha256(raw) do
      {:ok,
       %__MODULE__{
         v: 1,
         id: id,
         source_id: source_id,
         tenant_id: tenant_id,
         received_at: received_at,
         content_type: content_type,
         size: size,
         dedupe_key: dedupe_key,
         replay_id: replay_id,
         headers: headers,
         sha256: sha256
       }}
    end
  end

  defp id(%{"id" => id}) when is_binary(id) and id != "", do: {:ok, id}
  defp id(_raw), do: {:error, invalid_field("id")}

  defp source_id(%{"source_id" => source_id}) when is_binary(source_id), do: {:ok, source_id}
  defp source_id(_raw), do: {:error, invalid_field("source_id")}

  defp received_at(%{"received_at" => received_at}) when is_integer(received_at),
    do: {:ok, received_at}

  defp received_at(_raw), do: {:error, invalid_field("received_at")}

  defp size(%{"size" => size}) when is_integer(size) and size >= 0, do: {:ok, size}
  defp size(_raw), do: {:error, invalid_field("size")}

  defp optional_string(raw, key) do
    case Map.fetch(raw, key) do
      :error -> {:ok, nil}
      {:ok, value} when is_binary(value) or is_nil(value) -> {:ok, value}
      {:ok, _other} -> {:error, invalid_field(key)}
    end
  end

  defp headers(raw) do
    case Map.fetch(raw, "headers") do
      :error ->
        {:ok, %{}}

      {:ok, headers} when is_map(headers) ->
        if Enum.all?(headers, fn {_name, value} -> is_binary(value) end) do
          {:ok, headers}
        else
          {:error, invalid_field("headers")}
        end

      {:ok, _other} ->
        {:error, invalid_field("headers")}
    end
  end

  defp sha256(raw) do
    case Map.fetch(raw, "sha256") do
      :error ->
        {:ok, nil}

      {:ok, nil} ->
        {:ok, nil}

      {:ok, value} when is_binary(value) ->
        if Regex.match?(@sha256_pattern, value),
          do: {:ok, value},
          else: {:error, invalid_field("sha256")}

      {:ok, _other} ->
        {:error, invalid_field("sha256")}
    end
  end

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
  rescue
    ArgumentError -> {:error, invalid(body_base64, :invalid_body_base64)}
  end

  defp inline_body(_message, other), do: {:error, invalid(other, :invalid_body_base64)}

  defp claim_body(message, raw) do
    with {:ok, claim} <- claim(raw),
         {:ok, ref} <- parse_claim(claim),
         {:ok, sha256} <- claim_sha256(raw) do
      message = %{message | claim: claim, sha256: sha256}

      case message.tenant_id do
        nil -> {:ok, message}
        tenant when tenant == ref.tenant_id -> {:ok, message}
        _other -> {:error, invalid(nil, :tenant_mismatch)}
      end
    end
  end

  defp claim(raw) do
    case raw["claim"] do
      claim when is_binary(claim) -> {:ok, claim}
      other -> {:error, invalid(other, {:invalid_field, "claim"})}
    end
  end

  defp parse_claim(claim) do
    case ClaimRef.parse(claim) do
      {:ok, ref} -> {:ok, ref}
      {:error, _error} -> {:error, invalid(claim, {:invalid_field, "claim"})}
    end
  end

  defp claim_sha256(raw) do
    case raw["sha256"] do
      sha256 when is_binary(sha256) -> {:ok, sha256}
      _other -> {:error, invalid_field("sha256")}
    end
  end

  # Inline integrity: the decoded length is the size, and the body's digest is
  # the sha256 the message carries. A claim body is opaque here (its bytes are
  # the claim store's, checked when redeemed); only its tenant is known.
  defp integrity(%__MODULE__{body: body, size: size, sha256: sha256} = message)
       when is_binary(body) do
    cond do
      byte_size(body) != size -> {:error, invalid(nil, :size_mismatch)}
      is_binary(sha256) and sha256 != digest(body) -> {:error, invalid(nil, :integrity)}
      true -> {:ok, message}
    end
  end

  defp integrity(%__MODULE__{} = message), do: {:ok, message}

  defp digest(body), do: Base.encode16(:crypto.hash(:sha256, body), case: :lower)

  defp invalid_field(key), do: invalid(key, {:invalid_field, key})

  defp invalid(_value, reason) do
    {code, field} = classify(reason)

    %InvalidMessageError{
      message: "invalid message: #{inspect(reason)}",
      reason: reason,
      code: code,
      field: field
    }
  end

  defp classify(:invalid_json), do: {"invalid_json", nil}
  defp classify(:not_an_object), do: {"not_an_object", nil}
  defp classify({:unsupported_version, _v}), do: {"unsupported_version", nil}
  defp classify({:invalid_field, key}), do: {"invalid_field", key}
  defp classify(:ambiguous_body), do: {"ambiguous_body", nil}
  defp classify(:missing_body), do: {"missing_body", nil}
  defp classify(:invalid_body_base64), do: {"invalid_body_base64", nil}
  defp classify(:size_mismatch), do: {"size_mismatch", nil}
  defp classify(:integrity), do: {"integrity", nil}
  defp classify(:tenant_mismatch), do: {"tenant_mismatch", nil}
end
