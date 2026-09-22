defmodule Hook.Application do
  @moduledoc false

  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    children =
      [{Registry, keys: :unique, name: Hook.Registry}] ++ default_instance()

    opts = [strategy: :one_for_one, name: Hook.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # The default instance boots from application env unless `autostart` is false
  # (tests start their own isolated instances).
  defp default_instance do
    if Application.get_env(:hook, :autostart, true) do
      config = build_config()

      Logger.info(
        "[hook] starting instance #{config.instance} roles=#{inspect(config.roles)} " <>
          "port=#{config.port} data_dir=#{config.data_dir}"
      )

      [{Hook.Instance, config}]
    else
      []
    end
  end

  defp build_config do
    Hook.Config.new(
      instance: :default,
      port: port(),
      data_dir: Application.get_env(:hook, :data_dir, "./data"),
      roles: roles(),
      source_store: {Hook.SourceStore.Static, sources: Application.get_env(:hook, :sources, %{})}
    )
  end

  defp port do
    case System.get_env("PORT") do
      nil -> Application.get_env(:hook, :port, 4000)
      value -> String.to_integer(value)
    end
  end

  defp roles do
    case System.get_env("HOOK_ROLES") do
      nil ->
        Application.get_env(:hook, :roles, [:edge, :dispatch, :storage])

      value ->
        value |> String.split(",", trim: true) |> Enum.map(&String.to_atom(String.trim(&1)))
    end
  end
end
