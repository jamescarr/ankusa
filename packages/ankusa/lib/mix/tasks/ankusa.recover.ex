defmodule Mix.Tasks.Ankusa.Recover do
  @shortdoc "Replays this node's intake log into its (stopped) store: wal.type: dual recovery"

  @moduledoc """
  Brings a stopped node's store forward from the intake log (`wal: :dual`, ADR
  0002), by the same code a booting node runs.

      mix ankusa.recover --instance default --node ankusa-a
      mix ankusa.recover --from earliest --dry-run
      mix ankusa.recover --allow-gap

  The node's `Ankusa.Config` is `config :ankusa, :config, [...]` (a keyword list
  of `Ankusa.Config.new/1` options, `wal: :dual` and its `intake`, `backup` and
  `source_store` blocks included): the task needs the node's own configuration,
  never a copy of it. Start the replacement node with the lost node's
  `intake.node_id` on an empty volume: the store is restored from the backup
  first, then the log is replayed from its `m:intake_offset`. A running node does
  this at boot; run the task by hand with `intake.recover_on_boot: false`.

  Prints `records=… applied=… present=… collided=… unroutable=… skipped=…
  republished=…` and exits `0`; a second run changes nothing.

  Options:

    * `--instance` — default `default`
    * `--node` — asserts the node id (must equal `intake.node_id`)
    * `--from` — `backup` (the store's own offset, the default), `earliest`, or a position
    * `--dry-run` — read and count, write and publish nothing
    * `--allow-gap` — go on when the log no longer reaches back to the restored
      store's offset, saying which positions are lost
  """

  use Mix.Task

  alias Ankusa.Intake.Recovery

  @impl Mix.Task
  def run(args) do
    run_opts =
      case Recovery.parse_args(args) do
        {:ok, opts} -> opts
        {:error, message} -> Mix.raise(message)
      end

    Mix.Task.run("app.config")

    config = config(run_opts[:instance])

    case Recovery.run_offline(config, run_opts) do
      {:ok, summary} -> Mix.shell().info(Recovery.format_summary(summary))
      {:error, reason} -> Mix.raise("recovery failed: #{inspect(reason)}")
    end
  end

  # The instance name is an operator's command-line argument, never request input.
  # sobelow_skip ["DOS.StringToAtom"]
  defp config(instance) do
    options = Application.get_env(:ankusa, :config, [])

    case instance do
      nil -> Ankusa.Config.new(options)
      name -> Ankusa.Config.new(Keyword.put(options, :instance, String.to_atom(name)))
    end
  end
end
