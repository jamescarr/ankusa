# The Redis lifecycle harness: an instance on the Redis route store and the Redis
# source store that leaves exactly what a `kill -9` in the middle of a change
# leaves, for `test/ankusa/routes/store/redis_lifecycle_test.exs`.
#
# It runs as its own OS process, with no Mix and no `:ankusa` application, so the
# process that kills it gets no chance to flush anything:
#
#     ERL_LIBS=<build>/lib DATA_DIR=/data elixir lifecycle_harness.exs serve
#
# A route or a Redis-store source is a dual write: Ankusa's audit row first, as a
# *pending* row, then the change in Redis, then the row moves into the trail.
# `serve` stops that sequence at each place a kill can land, and prints `READY`:
#
#   * `r-diverged` is created for real (a finished change), then a pending
#     update of it is begun with hashes that match nothing, as if another node
#     had changed it meanwhile;
#   * `r-applied` and `acme.s-applied` are in Redis, with a pending row whose
#     `after_hash` is what is there (the kill came after the change, before the
#     row finished);
#   * `r-ghost` and `acme.s-ghost` have a pending row and nothing in Redis (the
#     kill came before the change).
#
# The only lifecycle sink never confirms, so nothing is delivered before the kill.
#
# Required by `test_helper.exs` for `Ankusa.RedisLifecycleHarness.config/2`: the
# file only runs a command when its argv is exactly `serve`.
defmodule Ankusa.RedisLifecycleHarness do
  @moduledoc false

  @instance :redis_lifecycle_harness
  @url System.get_env("REDIS_URL", "redis://localhost:6399")
  @routes_namespace "ankusa:routes:lifecycle_harness"
  @sources_namespace "ankusa:sources:lifecycle_harness"

  defmodule HoldSink do
    @moduledoc false
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: Process.sleep(:infinity)
  end

  @doc false
  def instance, do: @instance

  @doc false
  def url, do: @url

  @doc "Every Redis key the harness's namespaces use."
  def keys do
    for key <- ~w(routes ip_rules version), do: "#{@routes_namespace}:#{key}"
  end

  @doc false
  def source_keys, do: for(key <- ~w(sources version), do: "#{@sources_namespace}:#{key}")

  @doc false
  def decoder(_source_id, _spec), do: [sinks: [{Ankusa.Sink.Log, []}]]

  @doc "The instance config both the serving child and a reopening parent use."
  def config(data_dir, sinks) do
    Ankusa.Config.new(
      instance: @instance,
      data_dir: data_dir,
      port: 0,
      roles: [:edge],
      routes: [
        enabled: true,
        admin: [port: 0],
        store:
          {Ankusa.Routes.Store.Redis, url: @url, namespace: @routes_namespace, tick_ms: 60_000}
      ],
      source_store:
        {Ankusa.SourceStore.Redis,
         url: @url, namespace: @sources_namespace, tick_ms: 60_000, decoder: &__MODULE__.decoder/2},
      lifecycle: %{sinks: sinks}
    )
  end

  @doc "Boot the instance, leave the pending rows, print `READY`, wait to be killed."
  def serve do
    config = config(System.fetch_env!("DATA_DIR"), [{HoldSink, []}])

    # stdout carries only `READY`; log output goes to stderr.
    :ok = :logger.remove_handler(:default)
    :ok = :logger.add_handler(:default, :logger_std_h, %{config: %{type: :standard_error}})

    {:ok, _} = Application.ensure_all_started(:bandit)
    {:ok, _} = Application.ensure_all_started(:nebulex)
    {:ok, _} = Application.ensure_all_started(:redix)
    {:ok, _} = Registry.start_link(keys: :unique, name: Ankusa.Registry)
    Ankusa.put_config(config)
    {:ok, _} = Ankusa.Instance.start_link(config)

    # A finished change: its event is in the outbox, undelivered.
    {:ok, _route} = Ankusa.Routes.create(@instance, route_attrs("r-diverged"))

    pending_route_applied()
    pending_route("r-ghost", "route.create", nil, hash_of(route("r-ghost")))
    pending_route("r-diverged", "route.update", "before-elsewhere", "after-elsewhere")
    pending_source_applied()
    pending_source_ghost()

    IO.puts("READY")
    Process.sleep(:infinity)
  end

  defp route_attrs(id),
    do: %{"id" => id, "path" => "/hooks/#{id}", "methods" => ["POST"]}

  defp route(id) do
    {:ok, route} = Ankusa.Routes.Route.from_attrs(route_attrs(id))
    route
  end

  defp hash_of(route), do: Ankusa.Routes.definition_hash(@instance, route)

  defp spec, do: %{"sinks" => [%{"type" => "log"}]}

  defp begin!(attrs) do
    {:ok, _pending} = Ankusa.Audit.begin(@instance, Ankusa.Audit.Context.system(), attrs)
  end

  defp route_row(id, action, before_hash, after_hash, route) do
    %{
      action: action,
      resource: id,
      before_hash: before_hash,
      after_hash: after_hash,
      revision: route && route.revision,
      entity: route && Ankusa.Routes.Route.to_json(route)
    }
  end

  # In Redis, row pending: the kill came after the change.
  defp pending_route_applied do
    route = route("r-applied")
    meta = Ankusa.Routes.meta(@instance)
    :ok = Ankusa.Routes.Store.insert(@instance, route, meta.version)
    begin!(route_row("r-applied", "route.create", nil, hash_of(route), route))
  end

  # Row pending, not in Redis (`r-ghost`) or in a state it was not heading to
  # (`r-diverged`).
  defp pending_route(id, action, before_hash, after_hash) do
    begin!(route_row(id, action, before_hash, after_hash, nil))
  end

  defp pending_source_applied do
    {:ok, stored} =
      Ankusa.SourceStore.Redis.put(@instance, "acme", "s-applied", spec(), :create)

    begin!(source_row("source.create", "s-applied", Ankusa.Audit.digest(@instance, stored.spec)))
  end

  defp pending_source_ghost do
    begin!(source_row("source.create", "s-ghost", Ankusa.Audit.digest(@instance, spec())))
  end

  defp source_row(action, name, after_hash) do
    %{
      action: action,
      tenant: "acme",
      resource: "acme.#{name}",
      before_hash: nil,
      after_hash: after_hash
    }
  end
end

case System.argv() do
  ["serve"] -> Ankusa.RedisLifecycleHarness.serve()
  _ -> :ok
end
