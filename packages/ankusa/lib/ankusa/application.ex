defmodule Ankusa.Application do
  @moduledoc false

  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    # Node-wide credential caches: owned by the application's own process, so
    # they live as long as `:ankusa` does, not as long as whichever request or
    # task filled them first.
    :ok = Ankusa.AWS.Credentials.create_table()
    :ok = Ankusa.BlobStore.Azure.ManagedIdentity.create_table()
    :ok = Ankusa.BlobStore.Breaker.create_table()
    :ok = Ankusa.BlobStore.TokenCache.create_table()
    :ok = Ankusa.HttpClient.Pools.create_table()

    # The node's outbound HTTP pools (`Ankusa.HttpClient.Pools`): the Finch first,
    # then the process that opens, bounds and sweeps its pools. A restart of
    # either restarts everything after it.
    children =
      [
        Ankusa.HttpClient.Pools.finch_child_spec(),
        Ankusa.HttpClient.Pools,
        {Registry, keys: :unique, name: Ankusa.Registry}
      ] ++ default_instance()

    # `rest_for_one`: a Registry restart forgets every registered name, so
    # anything started after it (the default instance) must restart with it.
    opts = [strategy: :rest_for_one, name: Ankusa.Supervisor]

    with {:ok, pid} <- Supervisor.start_link(children, opts), do: {:ok, pid, []}
  end

  # The VM is stopping (SIGTERM): drain every instance in this tree before the
  # tree stops (`Ankusa.Instance.Drain`). An instance an embedding app or
  # `ankusa_server` supervises itself is drained by that app's own `prep_stop/1`.
  @impl true
  def prep_stop(state) do
    instances()
    |> Task.async_stream(&Ankusa.Instance.Drain.run/1, timeout: :infinity, ordered: false)
    |> Stream.run()

    state
  end

  defp instances do
    for {{Ankusa.Instance, instance}, pid, _type, _modules} <-
          Supervisor.which_children(Ankusa.Supervisor),
        is_pid(pid),
        do: instance
  catch
    :exit, _ -> []
  end

  # The default instance boots from application env only if `autostart` is
  # explicitly enabled (opt-in — a library must not bind a port just because
  # it's a dependency). The repo's own `config/config.exs` turns it on
  # outside `:test`.
  defp default_instance do
    if Application.get_env(:ankusa, :autostart, false) do
      config = build_config()

      Logger.info(
        "[ankusa] starting instance #{config.instance} roles=#{inspect(config.roles)} " <>
          "port=#{config.port} data_dir=#{config.data_dir}"
      )

      [{Ankusa.Instance, config}]
    else
      []
    end
  end

  defp build_config do
    Ankusa.Config.new(
      instance: :default,
      port: port(),
      data_dir: Application.get_env(:ankusa, :data_dir, "./data"),
      roles: roles(),
      source_store:
        {Ankusa.SourceStore.Static, sources: Application.get_env(:ankusa, :sources, %{})}
    )
  end

  defp port do
    case System.get_env("PORT") do
      nil -> Application.get_env(:ankusa, :port, 4000)
      value -> String.to_integer(value)
    end
  end

  defp roles do
    case System.get_env("ANKUSA_ROLES") do
      nil ->
        Application.get_env(:ankusa, :roles, [:edge, :dispatch, :storage])

      value ->
        Ankusa.Config.parse_roles!(value)
    end
  end
end
