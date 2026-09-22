defmodule Ankusa.Verifier.StandardWebhooks do
  @moduledoc ~S"""
  Verifier for the [Standard Webhooks](https://www.standardwebhooks.com) spec.

  Requires the `webhook-id`, `webhook-timestamp`, and `webhook-signature`
  headers. The secret is `"whsec_<base64>"`; the `whsec_` prefix is stripped and
  the remainder Base64-decoded to the raw HMAC key.

  Signed content is `"#{id}.#{timestamp}.#{body}"`, HMAC-SHA256 over the key,
  Base64-encoded. `webhook-signature` is a space-separated list of `v1,<b64sig>`
  tokens; any matching token accepts (constant-time).
  """

  @behaviour Ankusa.Verifier

  alias Ankusa.Envelope
  alias Ankusa.Verifier

  @default_tolerance 300

  @impl true
  @spec verify(Envelope.t(), keyword()) :: :ok | {:error, term()}
  def verify(%Envelope{} = env, opts) do
    id = Envelope.header(env, "webhook-id")
    ts = Envelope.header(env, "webhook-timestamp")
    sig_header = Envelope.header(env, "webhook-signature")

    with true <- present?(id) and present?(ts) and present?(sig_header),
         :ok <- check_timestamp(ts, opts),
         {:ok, key} <- decode_secret(opts) do
      signed = "#{id}.#{ts}.#{env.body}"
      expected = Base.encode64(:crypto.mac(:hmac, :sha256, key, signed))

      if any_match?(sig_header, expected) do
        :ok
      else
        {:error, :no_match}
      end
    else
      false -> {:error, :missing_headers}
      {:error, _} = err -> err
    end
  end

  defp present?(nil), do: false
  defp present?(""), do: false
  defp present?(v) when is_binary(v), do: true

  defp check_timestamp(ts, opts) do
    tolerance = Keyword.get(opts, :tolerance, @default_tolerance)

    case Integer.parse(ts) do
      {ts_int, _} ->
        now = System.system_time(:second)

        if abs(now - ts_int) <= tolerance do
          :ok
        else
          {:error, :timestamp_out_of_tolerance}
        end

      :error ->
        {:error, :timestamp_out_of_tolerance}
    end
  end

  defp decode_secret(opts) do
    secret = Keyword.get(opts, :secret, "")
    raw = String.replace_prefix(secret, "whsec_", "")

    case Base.decode64(raw) do
      {:ok, key} -> {:ok, key}
      :error -> {:error, :bad_secret}
    end
  end

  defp any_match?(sig_header, expected) do
    sig_header
    |> String.split(" ", trim: true)
    |> Enum.any?(fn token ->
      case String.split(token, ",", parts: 2) do
        ["v1", sig] -> Verifier.secure_compare(sig, expected)
        _ -> false
      end
    end)
  end
end
