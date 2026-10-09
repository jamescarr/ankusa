defmodule Ankusa.Sink.Http.Signer do
  @moduledoc """
  [Standard Webhooks](https://www.standardwebhooks.com/) signatures for
  `Ankusa.Sink.Http`'s `:secret` option, so a worker can tell a delivery from
  this node apart from anything else that can reach its URL.

  Three headers: `webhook-id` (the hook id), `webhook-timestamp` (unix
  seconds at send time) and `webhook-signature`, a space-separated list with
  one `v1,<base64>` entry per secret — HMAC-SHA256 over
  `"<webhook-id>.<webhook-timestamp>.<body>"`. Several secrets sign with each,
  so a receiver can rotate keys without a gap.

  A secret is `whsec_` + the base64 key (the Standard Webhooks form, decoded
  with `Ankusa.Verifier.Hmac.raw_key/2`), or any other non-empty string, used
  as its own bytes — the same rule every SDK's `verify_signature` applies
  (`conformance/cases/signature.json`).
  """

  @doc """
  The signature headers for one delivery, or `:error` when a secret is empty
  or its `whsec_` base64 does not decode.
  """
  @spec headers(String.t(), iodata(), String.t() | [String.t()], integer()) ::
          {:ok, [{String.t(), String.t()}]} | :error
  def headers(id, body, secrets, now_s) when is_binary(id) and is_integer(now_s) do
    ts = Integer.to_string(now_s)
    signed = [id, ?., ts, ?., body]

    secrets
    |> List.wrap()
    |> Enum.reduce_while([], fn secret, acc ->
      case key(secret) do
        {:ok, key} ->
          mac = :crypto.mac(:hmac, :sha256, key, signed)
          {:cont, ["v1," <> Base.encode64(mac) | acc]}

        :error ->
          {:halt, :error}
      end
    end)
    |> case do
      :error ->
        :error

      [] ->
        :error

      signatures ->
        {:ok,
         [
           {"webhook-id", id},
           {"webhook-timestamp", ts},
           {"webhook-signature", signatures |> Enum.reverse() |> Enum.join(" ")}
         ]}
    end
  end

  defp key("whsec_" <> _ = secret) do
    case Ankusa.Verifier.Hmac.raw_key(secret, :whsec_base64) do
      {:ok, key} when key != "" -> {:ok, key}
      _ -> :error
    end
  end

  defp key(secret) when is_binary(secret) and secret != "", do: {:ok, secret}
  defp key(_secret), do: :error
end
