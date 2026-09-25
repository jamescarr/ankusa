defmodule Ankusa.WAL.ClusterCase do
  @moduledoc """
  A real multi-node Ra cluster for the `ankusa_ra` suites: N `:peer` nodes, each
  hosting one Raft member, driven from the test node over Erlang distribution.

  Nothing here is mocked. The members are separate VMs with their own log
  directories, they elect a leader through real Raft, and they are killed and
  restarted with `:peer.stop/1` and `:peer.start_link/1` — which is the only way
  to test the things this adapter exists for: replication, leader change,
  snapshot install and lease failover.

  ## Shape

      setup_all do
        peers = Ankusa.WAL.ClusterCase.start_peers(3)
        on_exit(fn -> Ankusa.WAL.ClusterCase.stop_peers(peers) end)
        %{peers: peers}
      end

      setup %{peers: peers} do
        cluster = Ankusa.WAL.ClusterCase.start_cluster(peers)
        on_exit(fn -> Ankusa.WAL.ClusterCase.stop_cluster(cluster) end)
        %{cluster: cluster}
      end

  Peers are expensive to boot (a VM each) and cheap to reuse, so they are shared
  across a module; the WAL cluster on top of them is per-test, with its own
  instance name, its own data directories, and its own leader election.

  ## Requirements

  The test node must be distributed — Ra members talk to each other by
  `{Name, Node}` — which `test_helper.exs` arranges before any suite loads.
  """

  alias Ankusa.WAL.Ra

  @doc """
  Start `count` peer nodes and make them able to run Ankusa code.

  Returns `%{peers: %{node => pid}, configs: %{node => peer_start_options}}`.
  """
  @spec start_peers(pos_integer()) :: map()
  def start_peers(count \\ 3) do
    Enum.reduce(1..count, %{peers: %{}, configs: %{}}, fn n, acc ->
      add_peer(acc, :"ankusa_ra_node#{n}")
    end)
  end

  @doc """
  Start one more peer node under a name of its own — for the snapshot-install
  test, where a member joins a cluster that already exists.
  """
  @spec start_peer(atom()) :: map()
  def start_peer(name), do: add_peer(%{peers: %{}, configs: %{}}, name)

  @doc "Merge `extra` peers into `acc` (both are `start_peers/1` results)."
  @spec merge_peers(map(), map()) :: map()
  def merge_peers(acc, extra) do
    %{
      peers: Map.merge(acc.peers, extra.peers),
      configs: Map.merge(acc.configs, extra.configs)
    }
  end

  defp add_peer(acc, name) do
    paths = Enum.map(:code.get_path(), &to_charlist/1)
    config = %{name: name}
    {:ok, pid} = :peer.start_link(config)
    node = :peer.call(pid, :erlang, :node, [])

    # The peer runs the same code as this node: the build directory is on the
    # same filesystem, so handing over our code path is enough — but only the
    # modules under `lib/` and `test/support/`, which is why everything a peer
    # runs lives in `Ankusa.WAL.ClusterCase.Node` and not in a test module.
    :peer.call(pid, :code, :add_paths, [paths])
    :peer.call(pid, :application, :ensure_all_started, [:ankusa])

    %{
      peers: Map.put(acc.peers, node, pid),
      configs: Map.put(acc.configs, node, config)
    }
  end

  @doc "Stop every peer node."
  @spec stop_peers(map()) :: :ok
  def stop_peers(%{peers: peers}) do
    Enum.each(peers, &stop_peer/1)
    :ok
  end

  defp stop_peer({_node, pid}) do
    :peer.stop(pid)
  catch
    _, _ -> :ok
  end

  @doc """
  Start a fresh WAL cluster on `peers`: one member per peer node, a new instance
  name, a data directory per node, and a client on this node.

  Returns a map describing the cluster, including `:config` — the client config,
  already in `:persistent_term` — for tests that drive `Ankusa.WAL` directly.
  """
  @spec start_cluster(map(), keyword()) :: map()
  def start_cluster(peers, opts \\ []) do
    instance = Keyword.get(opts, :instance, :"ra#{System.unique_integer([:positive])}")
    cluster = :"ankusa_wal_#{instance}"

    dir =
      Path.join(System.tmp_dir!(), "ankusa_ra_#{instance}_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)

    member_nodes = peers.peers |> Map.keys() |> Enum.sort()
    members = Enum.map(member_nodes, &{cluster, &1})

    for {node, pid} <- peers.peers do
      # One data directory per node, under the shared root: the members must
      # not share a log directory any more than two VMs share a disk.
      node_dir = Path.join(dir, Atom.to_string(node))

      :ok =
        :peer.call(pid, Ankusa.WAL.ClusterCase.Node, :boot, [
          instance,
          cluster,
          node_dir,
          members
        ])
    end

    config =
      Ankusa.Config.new(
        instance: instance,
        data_dir: dir,
        roles: [:edge],
        wal: {Ra, members: members}
      )

    Ankusa.put_config(config)
    {:ok, _pid} = Ra.start_link(instance: instance, config: config)

    leader = wait_for_leader(members, Keyword.get(opts, :leader_timeout_ms, 20_000))

    %{
      instance: instance,
      cluster: cluster,
      dir: dir,
      members: members,
      member_nodes: member_nodes,
      peers: peers,
      config: config,
      leader: leader,
      ttl_ms: config.dispatch.lease_ttl_ms
    }
  end

  @doc "Stop the cluster: every member on every peer, then the client here."
  @spec stop_cluster(map()) :: :ok
  def stop_cluster(cluster) do
    Enum.each(cluster.peers.peers, &stop_peer_member(&1, cluster.instance))

    case Ankusa.whereis(cluster.instance, :wal) do
      nil -> :ok
      pid -> stop_local(pid)
    end

    _ = File.rm_rf(cluster.dir)
    :ok
  end

  defp stop_peer_member({_node, pid}, instance) do
    :peer.call(pid, Ankusa.WAL.ClusterCase.Node, :stop, [instance])
  catch
    _, _ -> :ok
  end

  defp stop_local(pid) do
    GenServer.stop(pid)
  catch
    :exit, _ -> :ok
  end

  @doc """
  Kill one member's VM outright — a crash, not a graceful stop. The peer is gone
  until `restart_member/2`.
  """
  @spec kill_member(map(), atom()) :: :ok
  def kill_member(cluster, node) do
    case Map.fetch(cluster.peers.peers, node) do
      {:ok, pid} ->
        :peer.stop(pid)
        :ok

      :error ->
        raise ArgumentError, "no peer for #{inspect(node)}"
    end
  end

  @doc """
  Bring a killed member back, recovering from its own directories — the same
  path a pod restart takes. Returns the cluster with the new peer pid.
  """
  @spec restart_member(map(), atom()) :: map()
  def restart_member(cluster, node) do
    config =
      Map.get_lazy(cluster.peers.configs, node, fn ->
        raise ArgumentError, "no peer config for #{inspect(node)}"
      end)

    {:ok, pid} = :peer.start_link(config)
    :peer.call(pid, :code, :add_paths, [Enum.map(:code.get_path(), &to_charlist/1)])
    :peer.call(pid, :application, :ensure_all_started, [:ankusa])

    node_dir = Path.join(cluster.dir, Atom.to_string(node))

    :ok =
      :peer.call(pid, Ankusa.WAL.ClusterCase.Node, :boot, [
        cluster.instance,
        cluster.cluster,
        node_dir,
        cluster.members
      ])

    put_in(cluster.peers.peers[node], pid)
  end

  @doc "The cluster's current leader, or `nil`."
  @spec leader(map()) :: tuple() | nil
  def leader(%{members: members}) do
    Enum.find_value(members, fn member ->
      case :ra.members(member, 2_000) do
        {:ok, _members, leader} when leader != nil -> leader
        _ -> nil
      end
    end)
  end

  @doc "Block until the cluster has a leader, or fail with a useful message."
  @spec wait_for_leader([tuple()], timeout()) :: tuple()
  def wait_for_leader(members, timeout \\ 20_000) do
    do_wait(members, System.monotonic_time(:millisecond) + timeout)
  end

  defp do_wait(members, deadline) do
    case Enum.find_value(members, fn member ->
           case :ra.members(member, 1_000) do
             {:ok, _members, leader} when leader != nil -> leader
             _ -> nil
           end
         end) do
      nil ->
        if System.monotonic_time(:millisecond) > deadline do
          raise "Ra cluster never elected a leader within the deadline"
        else
          Process.sleep(100)
          do_wait(members, deadline)
        end

      leader ->
        leader
    end
  end
end

defmodule Ankusa.WAL.ClusterCase.Node do
  @moduledoc """
  The peer-side half of `Ankusa.WAL.ClusterCase`: runs *on* a peer node, where
  the Ra system and the local member live.

  Everything here is called with `:peer.call/4`, so it returns plain terms and
  raises on failure — the origin sees the exception as an RPC error.
  """

  alias Ankusa.WAL.Ra

  @doc "Boot this node's Ra system and member, and register the WAL process."
  def boot(instance, _cluster, data_dir, members) do
    File.mkdir_p!(data_dir)

    config =
      Ankusa.Config.new(
        instance: instance,
        data_dir: data_dir,
        # `:wal` is what makes this node host a member rather than just talk to
        # the cluster — the same switch a production `wal` StatefulSet sets.
        roles: [:wal],
        wal: {Ra, members: members}
      )

    Ankusa.put_config(config)
    {:ok, pid} = Ra.start_link(instance: instance, config: config)
    # `start_link` ran in a temporary process on this node, which is about to
    # exit; the member must outlive it.
    Process.unlink(pid)
    :ok
  end

  @doc "Stop this node's WAL process."
  def stop(instance) do
    case Ankusa.whereis(instance, :wal) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end

    :ok
  catch
    :exit, _ -> :ok
  end

  @doc """
  Start a dispatch pipeline on this node — the peer-side half of the lease
  failover test, where two nodes race for the same `:dispatch` lease.
  """
  def start_pipeline(instance, config) do
    Ankusa.put_config(config)
    Ankusa.Dispatch.Pipeline.start_link(instance: instance, config: config)
  end

  @doc """
  Read through this node's own WAL process.

  On a member node that takes the local `read_entries` path (no distribution
  hop), which is a different branch from the client's leader read — this is how
  the suite exercises both.
  """
  def read(instance, after_seq, limit) do
    Ankusa.WAL.read(instance, after_seq, limit)
    |> Enum.map(&Map.take(&1, [:id, :seq, :body, :source_id, :tenant_id]))
  end

  @doc "Who currently holds `name`, read from the cluster's replicated state."
  def lease_holder(instance, name) do
    case Ankusa.WAL.Ra.remote_aux(
           Ankusa.config(instance).wal |> elem(1) |> Keyword.fetch!(:members),
           :overview
         ) do
      {:ok, overview} ->
        case overview |> Map.get(:leases, %{}) |> Map.get(name) do
          %{holder: holder} -> holder
          _ -> nil
        end

      _ ->
        nil
    end
  end

  @doc """
  Bytes in the largest snapshot file under `data_dir`, and the number of
  segment files — the two numbers that show a snapshot is bookkeeping, not
  payload, and that the log is being reclaimed.
  """
  def footprint(data_dir) do
    files = data_dir |> Path.join("**/*") |> Path.wildcard() |> Enum.filter(&File.regular?/1)

    snapshots =
      files
      |> Enum.filter(&String.contains?(&1, "snapshot"))
      |> Enum.map(&File.stat!(&1).size)

    segments = Enum.count(files, &String.contains?(&1, "segment"))

    {Enum.max([0 | snapshots]), segments}
  end
end
