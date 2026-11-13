defmodule Ankusa.Gateway.Url do
  @moduledoc """
  The scheme rule of the gateway's outbound endpoints — an identity provider's
  introspection endpoint, an enricher's API: `https`, or `http` only to a
  loopback host.

  An origin is always literal configuration: a URL that may be templated is
  compiled with `Ankusa.Gateway.Template.compile_url!/3`, which refuses a
  variable in the scheme, host or port. So a caller never chooses where a
  credential or a request goes.
  """

  alias Ankusa.Gateway.Options

  @doc """
  `url` if it is `https`, or `http` to a loopback host; raises
  `ArgumentError` naming `key` otherwise.
  """
  @spec https!(term(), String.t()) :: String.t()
  def https!(url, key) when is_binary(url) do
    case URI.parse(url) do
      %URI{scheme: "https", host: host} when is_binary(host) and host != "" ->
        url

      %URI{scheme: "http", host: host} when is_binary(host) and host != "" ->
        if loopback?(host), do: url, else: raise(ArgumentError, message(url, key))

      _other ->
        raise ArgumentError, message(url, key)
    end
  end

  def https!(other, key), do: raise(ArgumentError, message(other, key))

  defp message(url, key),
    do: "#{key} must be an https URL (or http to a loopback host), got #{inspect(url)}"

  defp loopback?(host) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, address} -> Options.loopback?(address)
      {:error, _} -> host in ["localhost", "localhost."]
    end
  end
end
