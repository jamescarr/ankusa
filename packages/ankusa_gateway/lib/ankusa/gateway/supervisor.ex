defmodule Ankusa.Gateway.Supervisor do
  @moduledoc """
  The gateway's tree, started by `Ankusa.Gateway.child_spec/1` and registered
  as `Ankusa.via(instance, :gateway)`. `:rest_for_one`:

    1. `Ankusa.Instance.RegistryWatch` (significant: a restart of the
       registry stops the gateway, so its parent starts it again registered
       in the new one);
    2. the `:gateway` failure domain (`Ankusa.Instance.Isolated`), in order:
       the keys server of the chain's `jwt` authenticators, the three caches
       (`Ankusa.Gateway.Cache`: `:authn`, `:authz`, `:acl`), every
       authenticator's own children, one `Ankusa.Gateway.Caller` per
       `:remote` authorizer and every authorizer's own children, the claim
       read cache (`Ankusa.ClaimCheck.Cache`, when the claims API is on and
       `claim_check.read_cache_bytes > 0`), the reader
       (`Ankusa.ClaimCheck.Reader`, always: it owns the request gate),
       `Ankusa.Gateway.Gauges`, and the listener, last.

  The listener takes core's `listener.*` limits (`Ankusa.Listener.bandit_options/3`)
  with `gateway.tls`, `gateway.ip` and `gateway.port`.

  The instance must be running: `init/1` raises otherwise.
  """

  use Supervisor

  require Logger

  alias Ankusa.Gateway.{Authn, Authz, Cache, Config}

  # The ACL cache's bounds until `gateway.sharing` configures them.
  @acl_cache [max_entries: 100_000, max_bytes: 67_108_864, max_ttl_ms: 60_000]

  @spec start_link(atom()) :: Supervisor.on_start()
  def start_link(instance) when is_atom(instance),
    do: Supervisor.start_link(__MODULE__, instance, name: Ankusa.via(instance, :gateway))

  @impl true
  def init(instance) do
    with {:ok, config} <- Ankusa.fetch_config(instance),
         pid when is_pid(pid) <- Ankusa.whereis(instance, :instance) do
      gateway = Ankusa.Gateway.config(instance)
      tables!(instance)
      warn(config, gateway)

      children = [
        {Ankusa.Instance.RegistryWatch, instance},
        {Ankusa.Instance.Isolated,
         instance: instance, domain: :gateway, children: domain_children(config, gateway)}
      ]

      Supervisor.init(children, strategy: :rest_for_one, auto_shutdown: :any_significant)
    else
      _ -> raise ArgumentError, "Ankusa.Gateway: start Ankusa.Instance #{instance} first"
    end
  end

  # Tables that live as long as this supervisor: it owns them.
  defp tables!(instance) do
    :ets.new(Authz.log_table(instance), [:named_table, :public, :set])
  end

  defp domain_children(config, %Config{instance: instance} = gateway) do
    keys_children(gateway) ++
      cache_children(gateway) ++
      authenticator_children(gateway) ++
      Authz.caller_specs(gateway) ++
      authorizer_children(gateway) ++
      claim_children(config, gateway) ++
      [
        {Ankusa.ClaimCheck.Reader, instance: instance},
        {Ankusa.Gateway.Gauges, instance: instance},
        listener(config, gateway)
      ]
  end

  @doc false
  # One keys server for every `jwt` authenticator of the chain, a realm each,
  # keyed by the authenticator's index. Public for tests that drive the
  # authentication plug without the whole tree.
  @spec keys_children(Config.t()) :: [Supervisor.child_spec() | {module(), keyword()}]
  def keys_children(%Config{instance: instance, auth: %{authenticators: authenticators}}) do
    realms =
      for {%{module: Authn.JWT, config: jwt}, index} <- Enum.with_index(authenticators),
          into: %{},
          do: {index, Map.put(jwt, :label, "gateway.auth.authenticators[#{index}]")}

    if realms == %{},
      do: [],
      else: [
        {Ankusa.Admin.Auth.JWT.Keys,
         instance: instance, server: Authn.JWT.keys_server(), realms: realms}
      ]
  end

  defp cache_children(%Config{instance: instance, auth: auth, authz: authz}) do
    authz_max_ttl =
      Enum.max([authz.cache.allow_ttl_ms, authz.cache.deny_ttl_ms, 60_000]) +
        authz.cache.serve_stale_ms

    [
      Cache.child_spec_for(instance, :authn,
        max_entries: auth.cache.max_entries,
        max_bytes: auth.cache.max_bytes,
        max_ttl_ms: max(auth.cache.ttl_ms, auth.cache.negative_ttl_ms)
      ),
      Cache.child_spec_for(instance, :authz,
        max_entries: authz.cache.max_entries,
        max_bytes: authz.cache.max_bytes,
        max_ttl_ms: authz_max_ttl
      ),
      Cache.child_spec_for(instance, :acl, @acl_cache)
    ]
  end

  defp authenticator_children(%Config{
         instance: instance,
         auth: %{authenticators: authenticators}
       }) do
    for {%{module: module, name: name, config: config}, index} <- Enum.with_index(authenticators),
        function_exported?(module, :child_specs, 1),
        spec <-
          module.child_specs(%{instance: instance, index: index, name: name, config: config}),
        do: spec
  end

  defp authorizer_children(%Config{instance: instance, authz: %{authorizers: authorizers}}) do
    for {name, %{module: module, state: state}} <- Enum.sort(authorizers),
        function_exported?(module, :child_specs, 3),
        spec <- module.child_specs(instance, name, state),
        do: spec
  end

  defp claim_children(config, %Config{instance: instance, claims: %{enabled: true}}) do
    if config.claim_check.read_cache_bytes > 0,
      do: [{Ankusa.ClaimCheck.Cache, instance: instance}],
      else: []
  end

  defp claim_children(_config, _gateway), do: []

  defp listener(config, %Config{} = gateway) do
    Supervisor.child_spec(
      {Bandit,
       Ankusa.Listener.bandit_options(config, gateway.tls) ++
         [
           plug: {Ankusa.Gateway.Router, [instance: gateway.instance]},
           ip: Ankusa.Config.listen_ip!(gateway.ip, "gateway.ip"),
           port: gateway.port
         ]},
      id: Ankusa.Gateway.Router
    )
  end

  defp warn(config, gateway) do
    if gateway.auth.authenticators == [] and not match?(%{verify: :verify_peer}, gateway.tls) do
      Logger.warning(
        "[ankusa] gateway on #{address(gateway)} has no authentication " <>
          "(gateway.auth.authenticators is empty); add an authenticator before exposing it " <>
          "beyond this host"
      )
    end

    local? =
      Enum.any?(
        Ankusa.ClaimCheck.Stores.list(config),
        &match?({_, Ankusa.BlobStore.LocalFS, _}, &1)
      )

    if gateway.claims.enabled and config.roles == [] and local? do
      Logger.warning(
        "[ankusa] the claim gateway reads claims from local disk (LocalFS); other nodes' " <>
          "claims are not here unless this directory is shared with them. Point " <>
          "claim_check.blob_stores at the stores the dispatch nodes write to."
      )
    end
  end

  defp address(%Config{ip: ip, port: port}),
    do: "#{:inet.ntoa(Ankusa.Config.listen_ip!(ip, "gateway.ip"))}:#{port}"
end
