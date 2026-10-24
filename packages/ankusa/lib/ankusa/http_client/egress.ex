defmodule Ankusa.HttpClient.Egress do
  @moduledoc """
  The egress policy: which URLs Ankusa may send a hook to (CWE-918).

  An HTTP sink forwards the raw webhook body to a URL somebody configured — an
  operator in YAML, or a tenant through the admin API. Without a policy that URL
  may be `http://127.0.0.1:4003/...` (Ankusa's own admin listener), the cloud
  metadata service (`http://169.254.169.254/...`), or any service on the
  private network. `Ankusa.Sink.Http` therefore calls `validate_target/3` (via
  `pin/3`) on every attempt, so a name that resolves somewhere else on the
  next retry is checked again, and a deny is a permanent failure.

  ## The policy

  `Ankusa.Config`'s `egress` section (YAML `egress:`):

    * `:allow_insecure_http` — `false`: only `https` URLs. `true` also allows
      `http`, which carries the hook in the clear
    * `:deny_cidrs` — an address inside any of these is refused. Default:
      loopback, RFC 1918, carrier-grade NAT, link-local (the metadata service),
      unique-local and link-local IPv6, and the unspecified addresses
    * `:allow_cidrs` — a deny exception: an address inside one of these passes
      even when a deny CIDR also holds it (`["10.20.0.0/16"]` for the one
      internal network sinks live on)
    * `:allow_hosts` — hostnames exempt from the CIDR check, for sinks reached
      by service name (`["worker"]`). The name is still resolved and pinned
    * `:deny_hosts` — hostnames refused before any lookup; wins over
      `:allow_hosts`. Default: `["metadata.google.internal"]`

  A name is resolved for both address families with the OTP resolver
  (`:inet.getaddrs`), and the
  target is refused when **any** of its addresses is denied and not allowed: a
  name with one public and one private record is a rebinding setup, not a
  multi-homed service. IPv4-mapped IPv6 addresses (`::ffff:127.0.0.1`) are
  matched as the IPv4 address they stand for.

  ## Pinning

  `pin/3` returns what to send instead of the URL it was given: the URL names the
  checked IP address and a `host` header carries the name, so the connection
  cannot go anywhere the check did not look and nothing is left to rebind. For
  `https` the connection options also carry `hostname: name`, which Mint uses for
  SNI and for the certificate check, so the peer must still present a certificate
  valid for the *name* (peer verification cannot be switched off without
  `allow_insecure_tls`, see `Ankusa.HttpClient`).

  Every connection a pool opens to the pinned URL goes to that address:
  `Ankusa.HttpClient.Pools` keeps one pool per pinned address and `hostname`, and
  `pin/3` runs again, resolving the name afresh, before every request. A name that
  later resolves to a denied address is refused on that request, before any pool
  is touched; one that resolves to another public address gets a pool of its own,
  and the old one is swept once idle.

  Reasons in `{:error, {:egress_denied, reason}}` carry the host and the
  offending address, never the URL: it may hold credentials.
  """

  alias Ankusa.Net

  @default_policy %Ankusa.Config{}.egress

  @type policy :: %{
          optional(atom()) => term(),
          allow_insecure_http: boolean(),
          deny_cidrs: [String.t()],
          allow_cidrs: [String.t()],
          allow_hosts: [String.t()],
          deny_hosts: [String.t()]
        }

  @type reason ::
          {:url, String.t()}
          | {:scheme, String.t()}
          | {:host, String.t()}
          | {:address, String.t(), String.t()}
          | {:resolve, String.t(), term()}
          | {:policy, String.t()}

  @typedoc "What to send: `url`, extra request `headers`, and `connect_options` for the client."
  @type pinned :: %{
          url: String.t(),
          headers: [{String.t(), String.t()}],
          connect_options: keyword()
        }

  @type resolver :: (charlist(), :inet | :inet6 -> {:ok, [:inet.ip_address()]} | {:error, term()})

  @keys [:allow_insecure_http, :deny_cidrs, :allow_cidrs, :allow_hosts, :deny_hosts]

  @doc "The default policy: `https` only, every private, loopback and link-local range denied."
  @spec default_policy() :: policy()
  def default_policy, do: @default_policy

  @doc """
  Check `url` against `policy`: `:ok`, or `{:error, {:egress_denied, reason}}`.

  Options: `:resolver` (a `(charlist, family -> {:ok, [ip]} | {:error, reason}`
  function, default `:inet.getaddrs` with the timeout) and `:timeout` (the lookup deadline in
  milliseconds, default `5000`).
  """
  @spec validate_target(String.t(), policy(), keyword()) ::
          :ok | {:error, {:egress_denied, reason()}}
  def validate_target(url, policy, opts \\ []) do
    with {:ok, _pinned} <- pin(url, policy, opts), do: :ok
  end

  @doc """
  `validate_target/3`, returning what to send: see "Pinning".

  Takes `validate_target/3`'s options.
  """
  @spec pin(String.t(), policy(), keyword()) ::
          {:ok, pinned()} | {:error, {:egress_denied, reason()}}
  def pin(url, policy, opts \\ []) do
    case check(url, policy, opts) do
      {:ok, uri, addresses} -> {:ok, build_pin(uri, addresses)}
      {:error, reason} -> {:error, {:egress_denied, reason}}
    end
  end

  @doc """
  Raise `ArgumentError`, naming the key, unless `config.egress` is a well-formed
  policy (`Ankusa.Config` calls it, and so does the instance at boot), and unless
  every `Ankusa.Sink.Http` the config names — lifecycle sinks and the static
  sources — passes `Ankusa.Sink.Http.validate_opts!/1`.
  """
  @spec validate_config!(Ankusa.Config.t()) :: :ok
  def validate_config!(%{egress: egress} = config) do
    validate_policy!(egress)

    config.lifecycle.sinks
    |> Enum.concat(static_sinks(config))
    |> Enum.each(fn
      {Ankusa.Sink.Http, opts} -> Ankusa.Sink.Http.validate_opts!(opts)
      _other -> :ok
    end)
  end

  @doc "Raise `ArgumentError` unless `policy` is a well-formed policy map. See `validate_config!/1`."
  @spec validate_policy!(term()) :: :ok
  def validate_policy!(policy) do
    unless is_map(policy) and Enum.all?(@keys, &Map.has_key?(policy, &1)) do
      raise ArgumentError,
            "egress must be a map with the keys #{Enum.map_join(@keys, ", ", &inspect/1)}, " <>
              "got: #{inspect(policy)}"
    end

    unless is_boolean(policy.allow_insecure_http) do
      raise ArgumentError,
            "egress.allow_insecure_http must be true or false, got: " <>
              inspect(policy.allow_insecure_http)
    end

    for key <- [:deny_cidrs, :allow_cidrs], do: validate_cidrs!(key, Map.fetch!(policy, key))
    for key <- [:allow_hosts, :deny_hosts], do: validate_hosts!(key, Map.fetch!(policy, key))
    :ok
  end

  # ── the check ───────────────────────────────────────────────────────────────

  defp check(url, policy, opts) do
    with {:ok, uri} <- parse(url),
         :ok <- check_scheme(uri, policy),
         host = normalize_host(uri.host),
         :ok <- check_host(host, policy),
         {:ok, addresses} <- resolve(uri.host, opts),
         {:ok, rules} <- compile(policy),
         :ok <- check_addresses(host, addresses, rules) do
      {:ok, uri, addresses}
    end
  end

  # `URI.parse/1`, the parser Req reads the URL with: what is checked is the host
  # the request goes to.
  defp parse(url) when is_binary(url) do
    case URI.parse(url) do
      %URI{scheme: scheme, host: host} = uri when is_binary(scheme) and is_binary(host) ->
        if host == "", do: {:error, {:url, "has no host"}}, else: {:ok, uri}

      %URI{} ->
        {:error, {:url, "must be an absolute http(s) URL"}}
    end
  end

  defp parse(_other), do: {:error, {:url, "must be an absolute http(s) URL"}}

  defp check_scheme(%URI{scheme: scheme}, policy) do
    case String.downcase(scheme) do
      "https" -> :ok
      "http" when policy.allow_insecure_http -> :ok
      "http" -> {:error, {:scheme, "http"}}
      other -> {:error, {:scheme, other}}
    end
  end

  defp check_host(host, policy) do
    if host in normalize_hosts(policy.deny_hosts), do: {:error, {:host, host}}, else: :ok
  end

  defp resolve(host, opts) do
    resolver = Keyword.get_lazy(opts, :resolver, fn -> default_resolver(opts) end)
    name = :binary.bin_to_list(host)

    case {lookup(resolver, name, :inet), lookup(resolver, name, :inet6)} do
      {{:error, reason}, {:error, _other}} ->
        {:error, {:resolve, host, reason}}

      {v4, v6} ->
        case Enum.uniq(addresses(v4) ++ addresses(v6)) do
          [] -> {:error, {:resolve, host, :nxdomain}}
          addresses -> {:ok, addresses}
        end
    end
  end

  # A resolver that raises or exits is a lookup that failed: the target is
  # refused, never waved through.
  defp lookup(resolver, name, family) do
    resolver.(name, family)
  rescue
    error -> {:error, error}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp addresses({:ok, list}) when is_list(list), do: Enum.map(list, &Net.normalize/1)
  defp addresses(_error), do: []

  defp default_resolver(opts) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    fn name, family -> :inet.getaddrs(name, family, timeout) end
  end

  defp compile(policy) do
    with {:ok, deny} <- parse_cidrs(policy.deny_cidrs),
         {:ok, allow} <- parse_cidrs(policy.allow_cidrs) do
      {:ok, %{deny: deny, allow: allow, allow_hosts: normalize_hosts(policy.allow_hosts)}}
    end
  end

  defp parse_cidrs(list) do
    Enum.reduce_while(list, {:ok, []}, fn cidr, {:ok, acc} ->
      case Net.parse_cidr(cidr) do
        {:ok, parsed} -> {:cont, {:ok, [parsed | acc]}}
        {:error, _} -> {:halt, {:error, {:policy, "invalid CIDR #{inspect(cidr)}"}}}
      end
    end)
  end

  defp check_addresses(host, addresses, rules) do
    exempt_host? = host in rules.allow_hosts

    case Enum.find(addresses, &(not permitted?(&1, rules, exempt_host?))) do
      nil -> :ok
      address -> {:error, {:address, host, Net.to_string(address)}}
    end
  end

  defp permitted?(address, rules, exempt_host?) do
    exempt_host? or not contained?(rules.deny, address) or contained?(rules.allow, address)
  end

  defp contained?(cidrs, ip), do: Enum.any?(cidrs, &(ip >= &1.first and ip <= &1.last))

  # ── the pin ─────────────────────────────────────────────────────────────────

  # `check/3` has admitted only `http` and `https`: both are pinned.
  defp build_pin(%URI{} = uri, addresses) do
    address = Enum.find(addresses, &(tuple_size(&1) == 4)) || hd(addresses)
    host = Net.to_string(address)

    %{
      url: %{uri | host: host, authority: nil} |> URI.to_string(),
      headers: [{"host", host_header(uri)}],
      connect_options: connect_options(uri, address)
    }
  end

  # The name the URL carried, as the `host` header and (https) the name the peer
  # certificate is checked against.
  defp host_header(%URI{host: host, port: port, scheme: scheme}) do
    host = if String.contains?(host, ":"), do: "[" <> host <> "]", else: host

    if port in [nil, URI.default_port(String.downcase(scheme))],
      do: host,
      else: "#{host}:#{port}"
  end

  defp connect_options(%URI{scheme: scheme, host: host}, address) do
    hostname = if String.downcase(scheme) == "https", do: [hostname: host], else: []
    family = if tuple_size(address) == 8, do: [transport_opts: [inet6: true]], else: []
    hostname ++ family
  end

  # ── config validation ───────────────────────────────────────────────────────

  defp validate_cidrs!(key, list) do
    unless is_list(list) do
      raise ArgumentError, "egress.#{key} must be a list of CIDRs, got: #{inspect(list)}"
    end

    for cidr <- list do
      case Net.parse_cidr(cidr) do
        {:ok, _parsed} ->
          :ok

        {:error, :mapped_range} ->
          raise ArgumentError,
                "egress.#{key}: invalid CIDR #{inspect(cidr)}: #{Net.mapped_range_hint()}"

        {:error, message} ->
          raise ArgumentError, "egress.#{key}: #{message}"
      end
    end
  end

  defp validate_hosts!(key, list) do
    unless is_list(list) and Enum.all?(list, &(is_binary(&1) and String.trim(&1) != "")) do
      raise ArgumentError,
            "egress.#{key} must be a list of non-empty hostnames, got: #{inspect(list)}"
    end
  end

  defp normalize_hosts(list), do: Enum.map(list, &normalize_host/1)

  # Hostnames compare lowercased, without the root dot (`Metadata.Google.Internal.`).
  defp normalize_host(host),
    do: host |> String.trim() |> String.trim_trailing(".") |> String.downcase()

  defp static_sinks(config) do
    config |> Ankusa.Queue.static_sources() |> Enum.flat_map(fn {_id, source} -> source.sinks end)
  end
end
