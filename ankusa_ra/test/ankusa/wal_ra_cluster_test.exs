defmodule Ankusa.WAL.RaClusterTest do
  @moduledoc """
  Level 1: the Ra WAL as a real cluster — bootstrap, replication, reads from
  every vantage point, compaction and snapshot install, leadership change, and
  lease failover.

  Every member is a separate VM (`Ankusa.WAL.ClusterCase`). Sizes are tuned for
  a test run rather than for a benchmark — the assertions are
  scale-independent properties (a snapshot that does not track payload bytes,
  segments that are reclaimed, bodies that come back byte-exact), not counts.
  """

  use ExUnit.Case, async: false

  @moduletag :dist

  alias Ankusa.{Envelope, UUIDv7, WAL}
  alias Ankusa.WAL.ClusterCase
  alias Ankusa.WAL.ClusterCase.Node, as: Peer
  alias Ankusa.WAL.Ra

  # Past the machine's release interval (10 000) once half are truncated, which
  # is what makes Ra snapshot and drop the segments nothing live points into.
  @records 20_000
  @payload 1_024

  setup_all do
    peers = ClusterCase.start_peers(3)
    on_exit(fn -> ClusterCase.stop_peers(peers) end)
    %{peers: peers}
  end

  setup %{peers: peers} do
    cluster = ClusterCase.start_cluster(peers)
    on_exit(fn -> ClusterCase.stop_cluster(cluster) end)
    %{cluster: cluster}
  end

  test "bootstrap elects a leader, and restarting every member keeps the membership", %{
    cluster: cluster
  } do
    assert cluster.leader != nil
    assert {:ok, members, leader} = :ra.members(hd(cluster.members), 5_000)
    assert Enum.sort(members) == Enum.sort(cluster.members)
    assert leader == cluster.leader

    # Every member restarts, one at a time, the way a rolling restart does.
    cluster =
      Enum.reduce(cluster.member_nodes, cluster, fn node, acc ->
        :ok = ClusterCase.kill_member(acc, node)
        ClusterCase.restart_member(acc, node)
      end)

    leader = ClusterCase.wait_for_leader(cluster.members)

    assert {:ok, members, ^leader} = :ra.members(hd(cluster.members), 5_000)
    assert Enum.sort(members) == Enum.sort(cluster.members)
  end

  test "appends land from a non-member client, and a follower redirects", %{cluster: cluster} do
    # The client on this node is not a member: it only talks to the cluster.
    assert {:ok, [{:committed, first}]} = WAL.append(cluster.instance, [entry("from-client")])
    assert first.seq == 1

    # A follower accepts the command and redirects it to the leader rather than
    # rejecting it — the property that makes a client's cached leader harmless.
    follower = cluster.members |> Enum.reject(&(&1 == cluster.leader)) |> hd()

    assert {:ok, {:ok, [{:committed, 2}]}} =
             Ra.remote_command(follower, {:append, {node(), 1}, records("via-follower")},
               timeout: 10_000
             )

    assert [one, two] = WAL.read(cluster.instance, 0, 10)
    assert one.body == "from-client"
    assert two.body == "via-follower"
  end

  test "reads are byte-exact and served from a member's own copy too", %{cluster: cluster} do
    bodies = [
      <<>>,
      :crypto.strong_rand_bytes(@payload),
      :crypto.strong_rand_bytes(300 * @payload)
    ]

    for body <- bodies do
      assert {:ok, [{:committed, env}]} =
               WAL.append(cluster.instance, [%{envelope: envelope(body)}])

      assert env.seq
    end

    # Through the non-member client: leader-planned and leader-served.
    from_leader = WAL.read(cluster.instance, 0, 10)
    assert Enum.map(from_leader, & &1.body) == bodies

    # Through a member's own WAL process: the local `read_entries` path, no
    # distribution hop.
    from_member = read_from_cluster(cluster, 0, 10)
    assert Enum.map(from_member, & &1.body) == bodies
    assert Enum.map(from_member, & &1.seq) == Enum.map(from_leader, & &1.seq)
  end

  # The largest body the edge will ever hand the WAL. It must still fit one Raft
  # command (the adapter validates that at boot), so this is the size where the
  # "split into several commands" path must *not* engage, and byte-exactness is
  # checked again at that boundary.
  test "a body of exactly max_body_bytes survives the log byte-exact", %{cluster: cluster} do
    size = cluster.config.max_body_bytes
    body = :crypto.strong_rand_bytes(size)

    assert {:ok, [{:committed, env}]} =
             WAL.append(cluster.instance, [%{envelope: envelope(body)}])

    assert env.seq
    assert byte_size(body) == size
    seq = env.seq

    # Through the leader: planned and served remotely.
    assert [%{body: ^body, seq: ^seq}] = WAL.read(cluster.instance, seq - 1, 1)

    # And through a member's own copy, where the payload is fetched from the
    # Raft log rather than relayed.
    assert [%{body: ^body}] = read_from_cluster(cluster, seq - 1, 1)
  end

  test "compaction snapshots without the payloads and reclaims truncated segments", %{
    cluster: cluster
  } do
    payload = :crypto.strong_rand_bytes(@payload)
    committed = append_all(cluster.instance, Enum.map(1..@records, fn _ -> payload end))
    assert length(committed) == @records

    cutoff = Enum.at(committed, 11_500).seq
    storage = hold_lease(cluster.instance, :storage)
    assert :ok = WAL.truncate_through(cluster.instance, cutoff, storage.token)

    # (a) every untruncated body is still byte-exact
    live = WAL.read(cluster.instance, cutoff, 200)
    assert length(live) == 200
    assert Enum.all?(live, &(&1.body == payload))

    # (b) and Ra took a snapshot, whose size does not track payload bytes:
    #     20 MB of bodies went in, so a snapshot holding them could not be small.
    assert wait_until(fn -> elem(footprint(cluster), 0) > 0 end, 30_000)
    {snapshot_bytes, segments} = footprint(cluster)
    assert snapshot_bytes < 1_000_000

    # (c) and the log was reclaimed rather than kept forever
    assert segments < @records
  end

  test "a member added after compaction catches up through snapshot install", %{
    cluster: cluster
  } do
    payload = :crypto.strong_rand_bytes(@payload)
    committed = append_all(cluster.instance, Enum.map(1..12_000, fn _ -> payload end))

    cutoff = Enum.at(committed, 11_000).seq
    storage = hold_lease(cluster.instance, :storage)
    :ok = WAL.truncate_through(cluster.instance, cutoff, storage.token)
    assert wait_until(fn -> elem(footprint(cluster), 0) > 0 end, 30_000)

    # A fourth VM joins a cluster that already has a compacted log: it cannot
    # have the entries, so the only way it can catch up is a snapshot.
    extra = ClusterCase.start_peer(:"ankusa_ra_extra#{System.unique_integer([:positive])}")
    on_exit(fn -> ClusterCase.stop_peers(extra) end)
    newcomer = extra.peers |> Map.keys() |> hd()

    :ok =
      :peer.call(
        Map.fetch!(extra.peers, newcomer),
        Peer,
        :boot,
        [
          cluster.instance,
          cluster.cluster,
          Path.join(cluster.dir, Atom.to_string(newcomer)),
          cluster.members ++ [{cluster.cluster, newcomer}]
        ]
      )

    seed = hd(cluster.members)
    assert {:ok, _leader} = :ra.add_member(seed, {cluster.cluster, newcomer}, 30_000)

    assert wait_until(
             fn ->
               case :ra.members(seed, 5_000) do
                 {:ok, ms, _} -> length(ms) == 4
                 _ -> false
               end
             end,
             60_000
           )

    # And it answers reads: every live record must have arrived through the
    # snapshot, on the newcomer's own copy.
    newcomer_pid = Map.fetch!(extra.peers, newcomer)

    assert wait_until(
             fn ->
               case :peer.call(newcomer_pid, Peer, :read, [cluster.instance, cutoff, 200]) do
                 live when is_list(live) ->
                   length(live) == 200 and Enum.all?(live, &(&1.body == payload))

                 _ ->
                   false
               end
             end,
             60_000
           )
  end

  test "leadership transfers under load without losing an ack", %{cluster: cluster} do
    writer =
      Task.async(fn ->
        for i <- 1..200 do
          {:ok, [{:committed, env}]} = WAL.append(cluster.instance, [entry("w#{i}")])
          env.seq
        end
      end)

    for _ <- 1..3 do
      case ClusterCase.leader(cluster) do
        nil ->
          :ok

        leader ->
          target = cluster.members |> Enum.reject(&(&1 == leader)) |> hd()
          _ = :ra.transfer_leadership(leader, target, 5_000)
      end

      Process.sleep(50)
    end

    seqs = Task.await(writer, 60_000)
    assert length(Enum.uniq(seqs)) == 200

    stored = WAL.read(cluster.instance, 0, 1_000)
    stored_seqs = Enum.map(stored, & &1.seq)
    assert Enum.sort(seqs) == Enum.sort(stored_seqs)
    assert Enum.uniq(stored_seqs) == stored_seqs
  end

  test "a standby dispatcher takes over when the active one dies", %{cluster: cluster} do
    [active_node, standby_node] = Enum.take(cluster.member_nodes, 2)

    :ok = :erpc.cast(active_node, Peer, :start_pipeline, [cluster.instance, cluster.config])
    :ok = :erpc.cast(standby_node, Peer, :start_pipeline, [cluster.instance, cluster.config])

    holder =
      wait_until_value(fn -> Peer.lease_holder(cluster.instance, :dispatch) end, 15_000)

    assert holder != nil
    assert holder |> String.split("/") |> hd() |> String.to_atom() == active_node

    # Kill the active node outright: the lease can only be taken over by expiry,
    # which is exactly what the TTL is for.
    :ok = ClusterCase.kill_member(cluster, active_node)
    _ = :erlang.disconnect_node(active_node)

    timeout = cluster.ttl_ms + div(cluster.ttl_ms, 3) + 10_000

    assert wait_until(
             fn ->
               case Peer.lease_holder(cluster.instance, :dispatch) do
                 holder when is_binary(holder) ->
                   String.starts_with?(holder, "#{standby_node}/")

                 _ ->
                   false
               end
             end,
             timeout
           )
  end

  test "an append larger than one command is split and returns the same results", %{
    cluster: cluster
  } do
    wal_opts =
      cluster.config.wal |> elem(1) |> Keyword.put(:max_command_bytes, 64 * 1_024)

    Ankusa.put_config(%{cluster.config | wal: {Ra, wal_opts}})

    records =
      for i <- 1..64 do
        %{envelope: envelope(:crypto.strong_rand_bytes(4_096), "split-#{i}")}
      end

    assert {:ok, results} = WAL.append(cluster.instance, records)
    assert length(results) == 64
    assert Enum.all?(results, &match?({:committed, _}, &1))

    seqs = Enum.map(results, fn {:committed, env} -> env.seq end)
    assert Enum.uniq(seqs) == seqs
  end

  test "a config whose bodies cannot fit one command is refused at boot" do
    config =
      Ankusa.Config.new(
        instance: :"cfg#{System.unique_integer([:positive])}",
        max_body_bytes: 32 * 1024 * 1024,
        wal: {Ra, members: [{:ankusa_wal_x, node()}]}
      )

    assert_raise ArgumentError, ~r/must fit one Ra command/, fn ->
      Ra.validate_config!(config, elem(config.wal, 1))
    end
  end

  # ── helpers ───────────────────────────────────────────────────────────────

  defp append_all(instance, payloads) do
    payloads
    |> Enum.chunk_every(2_000)
    |> Enum.flat_map(fn chunk ->
      records = Enum.map(chunk, &%{envelope: envelope(&1)})
      {:ok, results} = WAL.append(instance, records)
      Enum.map(results, fn {:committed, env} -> env end)
    end)
  end

  defp read_from_cluster(cluster, after_seq, limit) do
    cluster.members
    |> Enum.map(fn {_cluster, node} ->
      :peer.call(Map.fetch!(cluster.peers.peers, node), Peer, :read, [
        cluster.instance,
        after_seq,
        limit
      ])
    end)
    |> Enum.max_by(&length/1)
  end

  defp footprint(cluster) do
    cluster.peers.peers
    |> Map.values()
    |> Enum.map(fn pid -> :peer.call(pid, Peer, :footprint, [cluster.dir <> "/"]) end)
    |> Enum.reduce({0, 0}, fn {snapshot, segments}, {s, g} ->
      {max(s, snapshot), max(g, segments)}
    end)
  end

  defp hold_lease(instance, name) do
    {:ok, lease} = Ankusa.WAL.LeaseHelpers.hold_lease(instance, name, ttl_ms: 60_000)
    lease
  end

  defp entry(body, id \\ nil), do: %{envelope: envelope(body, id)}

  defp records(body),
    do: [{UUIDv7.generate(), "t1", "src", nil, Envelope.to_binary(%{envelope(body) | seq: nil})}]

  defp envelope(body, id \\ nil) do
    %Envelope{
      id: id || UUIDv7.generate(),
      source_id: "src",
      tenant_id: "t1",
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/hooks/src",
      headers: [],
      body: body,
      size: byte_size(body)
    }
  end

  defp wait_until(fun, timeout), do: wait_until(fun, timeout, System.monotonic_time(:millisecond))

  defp wait_until(fun, timeout, started) do
    if fun.() do
      true
    else
      if System.monotonic_time(:millisecond) - started > timeout do
        false
      else
        Process.sleep(100)
        wait_until(fun, timeout, started)
      end
    end
  end

  defp wait_until_value(fun, timeout), do: wait_until_value(fun, timeout, nil)

  defp wait_until_value(fun, timeout, started) do
    case fun.() do
      nil ->
        started = started || System.monotonic_time(:millisecond)

        if System.monotonic_time(:millisecond) - started > timeout do
          nil
        else
          Process.sleep(100)
          wait_until_value(fun, timeout, started)
        end

      value ->
        value
    end
  end
end
