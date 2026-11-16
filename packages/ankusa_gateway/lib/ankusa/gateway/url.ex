defmodule Ankusa.Gateway.Url do
  @moduledoc """
  The scheme rule of every outbound endpoint of the gateway — a policy
  decision point, an identity provider's introspection endpoint, an
  enricher's API, a permissions table: `https`, or `http` only to a loopback
  host (`localhost`, `*.localhost`, a loopback address), or `http` anywhere
  when the endpoint's `allow_insecure_http: true` says so.

  An origin is always literal configuration: a URL that may be templated is
  compiled with `Ankusa.Gateway.Template.compile_url!/3`, which refuses a
  variable in the scheme, host or port. So a caller never chooses where a
  credential or a request goes. `Ankusa.Gateway.Authz.Remote.url!/3` adds the
  rule that a decision point's URL is not templated at all.
  """

  alias Ankusa.Gateway.Options

  @doc """
  `url` if its scheme passes the rule; raises `ArgumentError` naming `key`
  (the URL's own dotted key) otherwise. `allow_insecure_http: true` accepts
  `http` to any host.
  """
  @spec https!(term(), String.t(), keyword()) :: String.t()
  def https!(url, key, opts \\ [])

  def https!(url, key, opts) when is_binary(url) do
    insecure? = Keyword.get(opts, :allow_insecure_http, false)

    case URI.parse(url) do
      %URI{scheme: "https", host: host} when is_binary(host) and host != "" ->
        url

      %URI{scheme: "http", host: host} when is_binary(host) and host != "" ->
        if insecure? or loopback?(host),
          do: url,
          else: raise(ArgumentError, message(url, key, opts))

      _other ->
        raise ArgumentError, message(url, key, opts)
    end
  end

  def https!(other, key, opts), do: raise(ArgumentError, message(other, key, opts))

  defp message(url, key, opts) do
    if Keyword.has_key?(opts, :allow_insecure_http) do
      "#{key} must be an https URL, or http to a loopback host (set allow_insecure_http: true " <>
        "beside it to accept http anywhere), got #{inspect(url)}"
    else
      "#{key} must be an https URL (or http to a loopback host), got #{inspect(url)}"
    end
  end

  @doc "Whether `host` (a URL's host) is this machine."
  @spec loopback?(String.t()) :: boolean()
  def loopback?(host) when is_binary(host) do
    cond do
      host in ["localhost", "localhost."] -> true
      String.ends_with?(host, ".localhost") -> true
      true -> loopback_address?(host)
    end
  end

  defp loopback_address?(host) do
    case :inet.parse_address(String.to_charlist(String.trim(host, "[]"))) do
      {:ok, address} -> Options.loopback?(address)
      {:error, _} -> false
    end
  end
end
