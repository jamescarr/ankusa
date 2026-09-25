defmodule Mix.Tasks.Ankusa.Wal.Members do
  @shortdoc "Inspect or change an Ankusa Ra WAL cluster's membership"

  @moduledoc """
  Inspect and change the membership of an `Ankusa.WAL.Ra` cluster.

      mix ankusa.wal.members list     --instance default --node ankusa@wal-0
      mix ankusa.wal.members add      --instance default --seed ankusa@wal-0
      mix ankusa.wal.members remove   --instance default --node ankusa@wal-2
      mix ankusa.wal.members transfer --instance default --node ankusa@wal-1

  `add` is run **on the node being added** (`--seed` names an existing member to
  talk to), because it has to start that node's own Raft member first: a member
  that has never existed has to be told the cluster it belongs to before the
  cluster is told about it. `remove` and `transfer` run anywhere that can reach a
  member.

  Options:

    * `--instance` — the Ankusa instance whose cluster to talk to (required).
    * `--node` — the node to act on (defaults to the local node).
    * `--seed` — an existing member to reach the cluster through. Defaults to
      `--node` for `list`/`remove`/`transfer`, and is required for `add`.
    * `--data-dir` — where the new member keeps its log (`add` only). Defaults
      to `$ANKUSA_DATA_DIR` (or `./data`) plus `<instance>/ra`.
    * `--timeout` — milliseconds to wait for a Ra query (default `10000`).
  """

  use Mix.Task

  alias Ankusa.WAL.Ra

  @switches [
    instance: :string,
    node: :string,
    seed: :string,
    to: :string,
    data_dir: :string,
    timeout: :integer
  ]

  @impl true
  def run(args) do
    Mix.Task.run("app.start")
    {opts, argv, invalid} = OptionParser.parse(args, strict: @switches)

    if invalid != [] do
      Mix.raise("unrecognised option(s): #{inspect(invalid)}")
    end

    instance =
      opts[:instance] || Mix.raise("--instance is required; see mix help ankusa.wal.members")

    cluster = cluster(instance)
    timeout = opts[:timeout] || 10_000

    case argv do
      ["list" | _] ->
        list(cluster, opts, timeout)

      ["add" | _] ->
        add(instance, cluster, opts, timeout)

      ["remove" | _] ->
        remove(cluster, opts, timeout)

      ["transfer" | _] ->
        transfer(cluster, opts, timeout)

      [] ->
        Mix.raise("expected one of: list, add, remove, transfer")

      [other | _] ->
        Mix.raise("unknown action #{inspect(other)}; expected list, add, remove or transfer")
    end
  end

  defp list(cluster, opts, timeout) do
    seed = seed(cluster, opts)

    case :ra.members(seed, timeout) do
      {:ok, members, leader} ->
        Mix.shell().info("leader: #{format(leader)}")

        Enum.each(members, fn member ->
          marker = if member == leader, do: "*", else: " "
          Mix.shell().info("#{marker} #{format(member)}")
        end)

      other ->
        Mix.raise("could not read membership through #{format(seed)}: #{inspect(other)}")
    end
  end

  # Run on the node being added: start its member from the existing members'
  # list, then ask the cluster to accept it.
  defp add(instance, cluster, opts, timeout) do
    seed =
      opts[:seed] ||
        Mix.raise("add needs --seed naming an existing member to reach the cluster through")

    seed = server_id(cluster, seed)

    case :ra.members(seed, timeout) do
      {:ok, members, _leader} ->
        data_dir = opts[:data_dir] || default_data_dir(instance)
        system = system(instance)
        File.mkdir_p!(data_dir)

        start_system(system, data_dir)

        machine = %{}

        case :ra.start_server(
               system,
               cluster,
               {cluster, node()},
               {:module, Ra.Machine, machine},
               members
             ) do
          :ok ->
            :ok

          {:error, reason} ->
            Mix.raise("could not start the member on #{node()}: #{inspect(reason)}")
        end

        case :ra.add_member(seed, {cluster, node()}, timeout) do
          {:ok, _leader} -> Mix.shell().info("added #{node()} to #{cluster}")
          other -> Mix.raise("could not add #{node()}: #{inspect(other)}")
        end

      other ->
        Mix.raise("could not reach #{format(seed)}: #{inspect(other)}")
    end
  end

  defp remove(cluster, opts, timeout) do
    victim = server_id(cluster, opts[:node] || to_string(node()))
    seed = seed(cluster, opts)

    case :ra.remove_member(seed, victim, timeout) do
      {:ok, _leader} -> Mix.shell().info("removed #{format(victim)} from #{cluster}")
      other -> Mix.raise("could not remove #{format(victim)}: #{inspect(other)}")
    end
  end

  defp transfer(cluster, opts, timeout) do
    target = opts[:to] || opts[:node]

    target =
      target || Mix.raise("transfer needs --node (or --to) naming the new leader's node")

    seed = seed(cluster, opts)

    case :ra.transfer_leadership(seed, server_id(cluster, target), timeout) do
      :ok -> Mix.shell().info("transferred leadership to #{target}")
      :already_leader -> Mix.shell().info("#{target} is already the leader")
      other -> Mix.raise("could not transfer leadership to #{target}: #{inspect(other)}")
    end
  end

  defp seed(cluster, opts) do
    server_id(cluster, opts[:seed] || opts[:node] || to_string(node()))
  end

  defp server_id(cluster, name), do: {cluster, String.to_atom(name)}

  defp cluster(instance), do: :"ankusa_wal_#{instance}"
  defp system(instance), do: :"ankusa_ra_#{instance}"

  defp default_data_dir(instance) do
    Path.join([System.get_env("ANKUSA_DATA_DIR", "./data"), instance, "ra"])
  end

  defp start_system(system, data_dir) do
    {:ok, _} = Application.ensure_all_started(:ra)
    data_dir = String.to_charlist(data_dir)

    config =
      :ra_system.default_config()
      |> Map.put(:name, system)
      |> Map.put(:data_dir, data_dir)
      |> Map.put(:wal_data_dir, data_dir)
      |> Map.put(:names, :ra_system.derive_names(system))

    case :ra_system.start(config) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> Mix.raise("could not start the local Ra system: #{inspect(reason)}")
    end
  end

  defp format({cluster, node}), do: "#{cluster}@#{node}"
  defp format(leader), do: inspect(leader)
end
