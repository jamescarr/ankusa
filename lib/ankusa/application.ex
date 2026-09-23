defmodule Ankusa.Application do
  @moduledoc false

  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    children =
      [{Registry, keys: :unique, name: Ankusa.Registry}] ++ default_instance()

    opts = [strategy: :one_for_one, name: Ankusa.Supervisor]
    Supervisor.start_link(children, opts)
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
