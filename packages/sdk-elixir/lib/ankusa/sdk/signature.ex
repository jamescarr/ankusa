defmodule Ankusa.SDK.Signature do
  @moduledoc """
  Verify the [Standard Webhooks](https://www.standardwebhooks.com/) signature
  an `Ankusa.Sink.Http` sink with a `secret` adds to every delivery.

  `webhook-signature` holds space-separated `v1,<base64>` entries, each an
  HMAC-SHA256 over `<webhook-id>.<webhook-timestamp>.<body>`. A delivery
  passes when any `v1` entry matches any configured secret (several during a
  rotation), compared in constant time, and `webhook-timestamp` is within the
  tolerance of now.

  Secrets are `whsec_` + base64, or any other string used as its own bytes.
  `Ankusa.SDK.Receiver` calls this for you when given `:secret`.
  """

  alias Ankusa.SDK.InvalidSignatureError

  @default_tolerance_seconds 300

  @typedoc "A verified delivery's `webhook-id` and `webhook-timestamp`."
  @type verified :: %{id: String.t(), timestamp: non_neg_integer()}

  @doc """
  Verify one delivery. `headers` is a map or a list of `{name, value}` pairs
  (names case-insensitive; in a list the first occurrence wins); `body` must
  be the raw bytes received; `secrets` is one secret or a list.

  Options: `:tolerance_seconds` (default #{@default_tolerance_seconds}) and
  `:now` (unix seconds; default the system clock).

  The checks run in this order, the first failure deciding the error's
  `:code`: `"invalid_secret"`, `"missing_header"` (`webhook-id`,
  `webhook-timestamp`, `webhook-signature`), `"invalid_timestamp"`,
  `"timestamp_out_of_tolerance"`, `"no_matching_signature"`.
  """
  @spec verify(Enumerable.t(), binary(), String.t() | [String.t()], keyword()) ::
          {:ok, verified()} | {:error, InvalidSignatureError.t()}
  def verify(headers, body, secrets, opts \\ []) when is_binary(body) do
    tolerance = Keyword.get(opts, :tolerance_seconds, @default_tolerance_seconds)
    now = Keyword.get_lazy(opts, :now, fn -> System.system_time(:second) end)
    lowered = lower(headers)

    with {:ok, keys} <- keys(List.wrap(secrets)),
         {:ok, id} <- required(lowered, "webhook-id"),
         {:ok, raw_timestamp} <- required(lowered, "webhook-timestamp"),
         {:ok, signature} <- required(lowered, "webhook-signature"),
         {:ok, timestamp} <- timestamp(raw_timestamp),
         :ok <- within(timestamp, now, tolerance) do
      signed = [id, ?., raw_timestamp, ?., body]

      candidates =
        for "v1," <> encoded <- String.split(signature, " "),
            {:ok, decoded} <- [Base.decode64(encoded)],
            do: decoded

      matched? =
        Enum.any?(keys, fn key ->
          expected = :crypto.mac(:hmac, :sha256, key, signed)

          Enum.any?(candidates, fn candidate ->
            byte_size(candidate) == byte_size(expected) and
              :crypto.hash_equals(candidate, expected)
          end)
        end)

      if matched? do
        {:ok, %{id: id, timestamp: timestamp}}
      else
        error("no_matching_signature", "webhook-signature", "no webhook-signature entry matches")
      end
    end
  end

  defp lower(headers) when is_list(headers) do
    Enum.reduce(headers, %{}, fn
      {name, value}, acc when is_binary(name) -> Map.put_new(acc, String.downcase(name), value)
      _other, acc -> acc
    end)
  end

  defp lower(headers) when is_map(headers) do
    Map.new(headers, fn {name, value} -> {String.downcase(to_string(name)), value} end)
  end

  defp keys([]), do: error("invalid_secret", nil, "no secret configured")

  defp keys(secrets) do
    Enum.reduce_while(secrets, {:ok, []}, fn secret, {:ok, acc} ->
      case key(secret) do
        {:ok, key} -> {:cont, {:ok, [key | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp key("whsec_" <> encoded) do
    case Base.decode64(encoded) do
      {:ok, key} when key != "" -> {:ok, key}
      _ -> error("invalid_secret", nil, "a whsec_ secret is not valid base64")
    end
  end

  defp key(secret) when is_binary(secret) and secret != "", do: {:ok, secret}
  defp key(_secret), do: error("invalid_secret", nil, "an empty or non-string secret")

  defp required(headers, name) do
    case Map.get(headers, name) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> error("missing_header", name, "missing #{name} header")
    end
  end

  defp timestamp(raw) do
    if raw =~ ~r/\A[0-9]+\z/ do
      {:ok, String.to_integer(raw)}
    else
      error("invalid_timestamp", "webhook-timestamp", "webhook-timestamp is not a unix time")
    end
  end

  defp within(timestamp, now, tolerance) do
    if abs(now - timestamp) <= tolerance do
      :ok
    else
      error(
        "timestamp_out_of_tolerance",
        "webhook-timestamp",
        "webhook-timestamp is outside the tolerance window"
      )
    end
  end

  defp error(code, field, message) do
    {:error, %InvalidSignatureError{message: message, code: code, field: field}}
  end
end
