defmodule Ankusa.Listener do
  @moduledoc """
  What every HTTP listener the instance runs (the ingest edge, the admin API,
  the route-management API, the claim-check gateway, the metrics listener) has
  in common: the connection limits and timeouts of `Ankusa.Config`'s
  `listener` section, and the optional TLS block each listener carries.

  Bandit's own defaults are tuned for a browser-facing app (a 60 s header read
  timeout, unlimited keep-alive, 16 384 connections for each of 100 acceptors,
  HTTP/2 on), so the instance passes every limit explicitly: see
  `bandit_options/2`.

  ## TLS

  A `tls` block is `nil` (plain HTTP) or a map:

  | Key | Default | Meaning |
  | --- | --- | --- |
  | `certfile`, `keyfile` | required | PEM files of the server certificate and its key |
  | `cacertfile` | `nil` | PEM bundle of the CAs that sign client certificates; required by `verify: :verify_peer` |
  | `verify` | `:verify_none` | `:verify_peer` makes the listener demand a client certificate that chains to `cacertfile` (mTLS): a client without one is refused at the handshake, and the accepted certificate is available to the request as `Plug.Conn.get_peer_data/1`'s `:ssl_cert` |
  | `versions` | `[:"tlsv1.2", :"tlsv1.3"]` | protocol versions offered |
  | `ciphers` | `nil` | a list of OpenSSL-style names (`"ECDHE-RSA-AES256-GCM-SHA384"`) or TLS 1.3 names (`"TLS_AES_256_GCM_SHA384"`); `nil` leaves the Erlang/OTP defaults |
  """

  alias Ankusa.Config

  @tls_defaults %{
    certfile: nil,
    keyfile: nil,
    cacertfile: nil,
    verify: :verify_none,
    versions: [:"tlsv1.2", :"tlsv1.3"],
    ciphers: nil
  }

  @verify_modes [:verify_none, :verify_peer]
  @versions [:"tlsv1.2", :"tlsv1.3"]

  @typedoc "A normalised `tls` block."
  @type tls :: %{
          certfile: Path.t(),
          keyfile: Path.t(),
          cacertfile: Path.t() | nil,
          verify: :verify_none | :verify_peer,
          versions: [:"tlsv1.2" | :"tlsv1.3"],
          ciphers: [String.t()] | nil
        }

  @doc """
  Validate one `tls` block (`key` is its dotted config name, for messages) and
  return it as a map with every key present and every path absolute, or `nil`.
  Raises `ArgumentError` naming the key.
  """
  @spec normalize_tls!(term(), String.t()) :: tls() | nil
  def normalize_tls!(nil, _key), do: nil

  def normalize_tls!(tls, key) when is_map(tls) or is_list(tls) do
    given = tls_map!(tls, key)

    for k <- Map.keys(given), not Map.has_key?(@tls_defaults, k) do
      raise ArgumentError, "unknown Ankusa.Config key: #{key}.#{k}"
    end

    tls = Map.merge(@tls_defaults, given)

    %{
      tls
      | certfile: file!(tls.certfile, "#{key}.certfile", true),
        keyfile: file!(tls.keyfile, "#{key}.keyfile", true),
        cacertfile: file!(tls.cacertfile, "#{key}.cacertfile", false),
        verify: verify!(tls.verify, key),
        versions: versions!(tls.versions, key),
        ciphers: ciphers!(tls.ciphers, key)
    }
    |> peer_needs_ca!(key)
  end

  def normalize_tls!(other, key) do
    raise ArgumentError,
          "#{key} must be nil or a map or keyword list of TLS options, got #{inspect(other)}"
  end

  defp tls_map!(tls, key) do
    cond do
      is_map(tls) -> Map.new(tls)
      Keyword.keyword?(tls) -> Map.new(tls)
      true -> raise ArgumentError, "#{key} must be nil or a map or keyword list"
    end
  end

  defp file!(nil, key, true),
    do: raise(ArgumentError, "#{key} is required when #{tls_parent(key)} is set")

  defp file!(nil, _key, false), do: nil

  defp file!(path, key, _required) when is_binary(path) do
    expanded = Path.expand(path)

    if File.regular?(expanded),
      do: expanded,
      else: raise(ArgumentError, "#{key} must be a readable file, got #{inspect(path)}")
  end

  defp file!(other, key, _required),
    do: raise(ArgumentError, "#{key} must be a file path, got #{inspect(other)}")

  defp tls_parent(key), do: key |> String.split(".") |> Enum.drop(-1) |> Enum.join(".")

  defp verify!(verify, _key) when verify in @verify_modes, do: verify

  defp verify!(other, key) do
    raise ArgumentError,
          "#{key}.verify must be :verify_peer or :verify_none, got #{inspect(other)}"
  end

  defp versions!([_ | _] = versions, key) do
    if Enum.all?(versions, &(&1 in @versions)),
      do: Enum.uniq(versions),
      else: bad_versions!(versions, key)
  end

  defp versions!(other, key), do: bad_versions!(other, key)

  defp bad_versions!(other, key) do
    raise ArgumentError,
          "#{key}.versions must be a non-empty list drawn from #{inspect(@versions)}, got #{inspect(other)}"
  end

  defp ciphers!(nil, _key), do: nil

  defp ciphers!([_ | _] = ciphers, key) do
    for cipher <- ciphers do
      unless is_binary(cipher) and suite(cipher) != :error do
        raise ArgumentError,
              "#{key}.ciphers has an unknown cipher suite: #{inspect(cipher)}"
      end
    end

    ciphers
  end

  defp ciphers!(other, key) do
    raise ArgumentError,
          "#{key}.ciphers must be nil or a non-empty list of cipher suite names, got #{inspect(other)}"
  end

  defp suite(name) do
    :ssl.str_to_suite(String.to_charlist(name))
  rescue
    _ -> :error
  catch
    _, _ -> :error
  else
    {:error, _} -> :error
    suite -> suite
  end

  defp peer_needs_ca!(%{verify: :verify_peer, cacertfile: nil}, key) do
    raise ArgumentError,
          "#{key}.cacertfile is required with #{key}.verify: :verify_peer: " <>
            "it names the CAs that sign client certificates"
  end

  defp peer_needs_ca!(tls, _key), do: tls

  @doc """
  The Bandit options that apply `config.listener`'s limits and the listener's
  own `tls` block. The caller adds `:plug`, `:port` and `:ip`.

  `listener.max_connections` is spread over `listener.num_acceptors`
  (Thousand Island counts connections per acceptor).
  """
  @spec bandit_options(Config.t(), tls() | nil) :: keyword()
  def bandit_options(%Config{listener: listener}, tls) do
    acceptors = listener.num_acceptors || default_acceptors()

    thousand_island = [
      num_acceptors: acceptors,
      num_connections: max(1, ceil_div(listener.max_connections, acceptors)),
      read_timeout: listener.read_timeout_ms
    ]

    [
      http_1_options: [
        max_header_count: listener.max_header_count,
        max_header_length: listener.max_header_length,
        max_requests: listener.max_requests_per_connection
      ],
      http_2_options: [enabled: listener.http2]
    ]
    |> Keyword.merge(tls_options(tls, thousand_island))
  end

  @doc "The default number of acceptors: two per scheduler."
  @spec default_acceptors() :: pos_integer()
  def default_acceptors, do: System.schedulers_online() * 2

  defp ceil_div(a, b), do: div(a + b - 1, b)

  defp tls_options(nil, thousand_island),
    do: [scheme: :http, thousand_island_options: thousand_island]

  defp tls_options(tls, thousand_island) do
    transport =
      [
        versions: tls.versions,
        verify: tls.verify
      ] ++
        if(tls.cacertfile, do: [cacertfile: tls.cacertfile], else: []) ++
        if(tls.verify == :verify_peer, do: [fail_if_no_peer_cert: true], else: []) ++
        if(tls.ciphers, do: [ciphers: Enum.map(tls.ciphers, &suite!/1)], else: [])

    [
      scheme: :https,
      certfile: tls.certfile,
      keyfile: tls.keyfile,
      thousand_island_options: [transport_options: transport] ++ thousand_island
    ]
  end

  defp suite!(name) do
    case suite(name) do
      :error -> raise ArgumentError, "unknown cipher suite #{inspect(name)}"
      suite -> suite
    end
  end
end
