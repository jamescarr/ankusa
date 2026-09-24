defmodule AnkusaServer.CLI do
  @moduledoc """
  The `bin/ankusa eval` entry points behind the container's `check-config`,
  `print-config`, and `version` commands.

  These exist so an operator can answer "is my config valid?" and "what config
  is this container actually running?" from outside the container, without an
  Elixir shell and without starting the instance (so it works for a config that
  would fail to boot, and does not bind a port to look at a file).

  Each prints to stdout and exits: `0` on success, `78` (`EX_CONFIG`) on a
  config error.
  """

  # Not `alias AnkusaServer.Application`: that would shadow `Elixir.Application`
  # inside this module, and `Application.ensure_all_started/1` below would
  # silently become a call to the server's own application module.
  alias AnkusaServer.Config

  @doc "Validate the config, print a one-line summary, exit 0. Exit 78 on error."
  @spec check_config() :: no_return()
  def check_config do
    config = config!()

    IO.puts(
      "config OK: roles=#{inspect(config.roles)} sources=#{AnkusaServer.Application.sources(config)} " <>
        "wal=#{inspect(elem(config.wal, 0))} storage=#{inspect(elem(config.storage.blob_store, 0))}"
    )

    halt(0)
  end

  @doc """
  Print the effective config as redacted JSON, exit 0. Exit 78 on error.

  Redaction is core's (`Ankusa.Admin.Redact`), the same function behind
  `GET /v1/config`, so printing the config to a terminal and reading it over
  HTTP can never disagree about what is safe to show.
  """
  @spec print_config() :: no_return()
  def print_config do
    IO.puts(JSON.encode!(Ankusa.Admin.Redact.config(config!())))
    halt(0)
  end

  @doc "Print the server and core versions, exit 0."
  @spec version() :: no_return()
  def version do
    IO.puts("ankusa_server #{AnkusaServer.Application.version()}")
    IO.puts("ankusa #{AnkusaServer.Application.core_version()}")
    halt(0)
  end

  defp config! do
    ensure_yaml()
    Config.load_or_halt!().config
  end

  # `YamlElixir` reads through `yamerl`, whose application must be up. Under
  # `eval` nothing else has started it.
  defp ensure_yaml, do: Application.ensure_all_started(:yaml_elixir)

  defp halt(status), do: System.halt(status)
end
