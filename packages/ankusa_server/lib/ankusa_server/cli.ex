defmodule AnkusaServer.CLI do
  @moduledoc """
  The `bin/ankusa eval` entry points behind the container's `check-config`,
  `print-config`, `version` and `recover` commands.

  These exist so an operator can answer "is my config valid?" and "what config
  is this container actually running?" from outside the container, without an
  Elixir shell and without starting the instance (so it works for a config that
  would fail to boot, and does not bind a port to look at a file).

  Each prints to stdout and exits: `0` on success, `78` (`EX_CONFIG`) on a
  config error. `recover` also exits `1` when the recovery itself fails and `64`
  for a bad option.
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
        "wal=#{Ankusa.Queue.label(config.wal)} storage=#{inspect(elem(config.storage.blob_store, 0))}"
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

  @doc """
  Brings a **stopped** node's store forward from its intake log (`wal.type: dual`,
  ADR 0002), by the code a booting node runs: `docker run … recover [--node ID]
  [--from backup|earliest|POSITION] [--dry-run] [--allow-gap]`, with the same
  config file and volume as the node. Prints one line (`records=… applied=…
  present=… collided=… unroutable=… skipped=… republished=…`), exits `0` on
  success, and changes nothing the second time. `--allow-gap` accepts a log that
  no longer reaches back to the restored store, and writes an audit row saying so.
  """
  @spec recover([String.t()]) :: no_return()
  def recover(args) do
    case Ankusa.Intake.Recovery.parse_args(args) do
      {:ok, opts} ->
        run_recovery(config!(), Keyword.put(opts, :actor, "ankusa recover"))

      {:error, message} ->
        IO.puts(:stderr, "ankusa recover: " <> message)
        halt(64)
    end
  end

  @spec run_recovery(Ankusa.Config.t(), keyword()) :: no_return()
  defp run_recovery(config, opts) do
    case Ankusa.Intake.Recovery.run_offline(config, opts) do
      {:ok, summary} ->
        IO.puts(Ankusa.Intake.Recovery.format_summary(summary))
        halt(0)

      {:error, :not_dual} ->
        IO.puts(:stderr, "ankusa recover: wal.type is not dual: there is no intake log to replay")
        halt(1)

      {:error, reason} ->
        IO.puts(:stderr, "ankusa recover: recovery failed: #{inspect(reason)}")
        halt(1)
    end
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

  @spec halt(non_neg_integer()) :: no_return()
  defp halt(status), do: System.halt(status)
end
