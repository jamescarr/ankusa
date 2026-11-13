defmodule AnkusaGatewayServer.CLI do
  @moduledoc """
  The `bin/ankusa_gateway eval` entry points behind the gateway image's
  `check-config`, `print-config` and `version` commands. Each prints to stdout
  and exits `0`, or `78` (`EX_CONFIG`) on a config error, without starting the
  gateway.
  """

  alias AnkusaGatewayServer.Config

  @doc "Validate the config, print a one-line summary, exit 0. Exit 78 on error."
  @spec check_config() :: no_return()
  def check_config do
    IO.puts(check_config_line(loaded!()))
    halt(0)
  end

  @doc false
  # The line `check_config/0` prints: what the operator's smoke test greps.
  @spec check_config_line(Config.loaded()) :: String.t()
  def check_config_line(%{config: config, gateway: gateway}) do
    "config OK: gateway=#{gateway.port} " <>
      "claims=#{if gateway.claims.enabled, do: "on", else: "off"} " <>
      "storage=#{inspect(elem(config.storage.blob_store, 0))}"
  end

  @doc """
  Print the effective config as redacted JSON, exit 0: core's view
  (`Ankusa.Admin.Redact.config/1`) with the gateway's under `"gateway"`
  (`Ankusa.Gateway.Config.redact/1`). Exit 78 on error.
  """
  @spec print_config() :: no_return()
  def print_config do
    IO.puts(JSON.encode!(config_view(loaded!())))
    halt(0)
  end

  @doc false
  # The object `print_config/0` prints.
  @spec config_view(Config.loaded()) :: map()
  def config_view(%{config: config, gateway: gateway}) do
    config
    |> Ankusa.Admin.Redact.config()
    |> Map.put("gateway", Ankusa.Gateway.Config.redact(gateway))
  end

  @doc "Print the image's, the gateway library's and core's versions, exit 0."
  @spec version() :: no_return()
  def version do
    for app <- [:ankusa_gateway_server, :ankusa_gateway, :ankusa] do
      IO.puts("#{app} #{AnkusaGatewayServer.Application.version(app)}")
    end

    halt(0)
  end

  defp loaded! do
    # `YamlElixir` reads through `yamerl`, whose application must be up. Under
    # `eval` nothing else has started it.
    _ = Application.ensure_all_started(:yaml_elixir)
    Config.load_or_halt!()
  end

  @spec halt(non_neg_integer()) :: no_return()
  defp halt(status), do: System.halt(status)
end
