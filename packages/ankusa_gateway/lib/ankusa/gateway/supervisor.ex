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
       authenticator's own children then every enricher's, one
       `Ankusa.Gateway.Caller` per
       `:remote` authorizer and every authorizer's own children, the
       connection pools of the spaces' stores, the claim
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
    Ankusa.Gateway.Objects.Budget.setup(instance)
  end

  defp domain_children(config, %Config{instance: instance} = gateway) do
    keys_children(gateway) ++
      cache_children(gateway) ++
      authn_children(gateway) ++
      Authz.caller_specs(gateway) ++
      sharing_children(gateway) ++
      authorizer_children(gateway) ++
      space_pools(gateway) ++
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

  # The connection pools of the spaces' stores, those core has not started.
  defp space_pools(%Config{instance: instance, spaces: spaces}) do
    stores = for {_name, %{store: store}} <- Enum.sort(spaces), uniq: true, do: store
    Ankusa.BlobStore.Pool.child_specs_for(instance, stores)
  end

  # The `:acl` caller the `grants` authorizer and ownership read through.
  defp sharing_children(%Config{sharing: %{enabled: true}, instance: instance}),
    do: [Ankusa.Gateway.ACL.Store.caller_spec(instance)]

  defp sharing_children(_gateway), do: []

  defp cache_children(%Config{instance: instance, auth: auth, authz: authz} = gateway) do
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
      Cache.child_spec_for(instance, :acl,
        max_entries: gateway.sharing.acl_cache_max_entries,
        max_bytes: gateway.sharing.acl_cache_max_bytes,
        max_ttl_ms: gateway.sharing.acl_cache_ttl_ms
      )
    ]
  end

  @doc false
  # Every authenticator's children, then every enricher's, in chain order.
  # Public for tests that drive the authentication plug without the whole tree.
  @spec authn_children(Config.t()) :: [Supervisor.child_spec() | {module(), keyword()}]
  def authn_children(%Config{} = config),
    do: authenticator_children(config) ++ enricher_children(config)

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

  defp enricher_children(%Config{instance: instance, auth: %{enrichers: enrichers}}) do
    for {%{module: module, config: config}, index} <- Enum.with_index(enrichers),
        function_exported?(module, :child_specs, 3),
        spec <- module.child_specs(instance, index, config),
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
    # The share-link authenticator alone (sharing on) leaves every other
    # request anonymous, as an empty chain does.
    own = Enum.reject(gateway.auth.authenticators, &(&1.type == :link))

    if own == [] and not match?(%{verify: :verify_peer}, gateway.tls) do
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

    if gateway.sharing.enabled do
      for {name, %{store: {Ankusa.BlobStore.LocalFS, _}}} <- Enum.sort(gateway.spaces) do
        Logger.warning(
          "[ankusa] gateway.sharing on a LocalFS space (#{name}): grant updates are only safe " <>
            "from one gateway node"
        )
      end
    end

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
