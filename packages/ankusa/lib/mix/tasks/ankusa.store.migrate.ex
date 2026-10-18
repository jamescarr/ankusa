defmodule Mix.Tasks.Ankusa.Store.Migrate do
  @shortdoc "Shows or applies the store format migrations of a data directory"

  @moduledoc """
  Shows, or applies, the store format migrations a data directory needs before
  this release can serve from it.

      mix ankusa.store.migrate --dry-run
      mix ankusa.store.migrate --data-dir /var/lib/ankusa --instance default

  The store is `<data-dir>/<instance>/store`. A node applies the same
  migrations by itself when it boots; run this to see the plan first
  (`--dry-run`, read-only, safe beside a running node), or to migrate ahead of
  a rollout. Without `--dry-run` the task opens the store, so stop the node that
  owns it first.

  A store newer than this release is refused, as at boot, naming both versions.

  Options:

    * `--dry-run` — print the plan, change nothing
    * `--data-dir` — default `config :ankusa, :data_dir` (`./data`)
    * `--instance` — default `default`
  """

  use Mix.Task

  alias Ankusa.Store.Migrate

  @impl Mix.Task
  def run(args) do
    {opts, _argv, invalid} =
      OptionParser.parse(args,
        strict: [dry_run: :boolean, data_dir: :string, instance: :string]
      )

    if invalid != [], do: Mix.raise("invalid options: #{inspect(invalid)}")

    Mix.Task.run("app.config")

    data_dir = opts[:data_dir] || Application.get_env(:ankusa, :data_dir, "./data")
    instance = opts[:instance] || "default"
    store = Path.join([data_dir, instance, "store"])

    if opts[:dry_run] do
      dry_run(store)
    else
      migrate(data_dir, instance, store)
    end
  end

  defp dry_run(store) do
    Mix.shell().info("store: #{store}")

    case Migrate.plan(store) do
      {:ok, plan} -> report(plan)
      {:error, :no_store} -> Mix.shell().info("no store there yet: nothing to migrate")
      {:error, reason} -> Mix.raise("could not read #{store}: #{inspect(reason)}")
    end
  end

  defp report(%{found: found, current: current} = plan) do
    Mix.shell().info("format: #{found || "unversioned (format 1)"} (this release: #{current})")

    cond do
      is_integer(found) and found > current ->
        Mix.raise("refuses to open: store format #{found} is newer than #{current}")

      plan.pending == [] ->
        Mix.shell().info("nothing to do")

      true ->
        Enum.each(plan.pending, fn {from, to, module} ->
          Mix.shell().info("  #{from} -> #{to}: #{module.describe()}")
        end)

        with %{to: to, cursor: cursor} <- plan.resume do
          Mix.shell().info("resumes #{to} at cursor #{Base.encode16(cursor)}")
        end
    end
  end

  # The instance name is an operator's command-line argument, never request input.
  # sobelow_skip ["DOS.StringToAtom"]
  defp migrate(data_dir, instance, store) do
    # The default instance must not boot (and bind a port) because this task
    # started the application.
    Application.put_env(:ankusa, :autostart, false)
    {:ok, _} = Application.ensure_all_started(:ankusa)

    name = String.to_atom(instance)

    Ankusa.put_config(
      Ankusa.Config.new(instance: name, data_dir: data_dir, roles: [:edge], port: 0)
    )

    # A refused open stops the store with its reason; being linked, this task
    # must trap that exit to report it instead of dying with it.
    Process.flag(:trap_exit, true)

    case Ankusa.Store.start_link(instance: name) do
      {:ok, pid} ->
        GenServer.stop(pid)
        Mix.shell().info("store #{store} is at format #{Migrate.current_format()}")

      {:error, reason} ->
        Mix.raise("#{store} was not migrated: #{inspect(reason)}")
    end
  end
end
