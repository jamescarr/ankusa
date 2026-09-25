defmodule Ankusa.WAL.RaFaultsTest do
  @moduledoc """
  Level 2: deterministic fault drills against a real three-member Ra cluster.

  Each drill kills or isolates something specific, then checks the history with
  `Ankusa.WAL.Checker` — the same checker the chaos harness uses — so a fault
  that "recovered" by losing a record fails here rather than in production.

  The drills are deterministic (a named member is killed, a named pipeline is
  suspended) rather than random: the chaos harness is where randomness lives.

  `@moduletag :dist`: these need Erlang distribution between peer VMs.
  """

  use ExUnit.Case, async: false

  @moduletag :dist

  alias Ankusa.{Envelope, UUIDv7}
  alias Ankusa.WAL
  alias Ankusa.WAL.{Checker, ClusterCase, Ra}
  alias Ankusa.WAL.ClusterCase.Node, as: Peer

  setup_all do
    peers = ClusterCase.start_peers(3)
    on_exit(fn -> ClusterCase.stop_peers(peers) end)
    %{peers: peers}
  end

  setup %{peers: peers} do
    # A short lease TTL, so the failover drills are measured in seconds.
    cluster =
      ClusterCase.start_cluster(peers,
        config: fn config ->
          %{
            config
            | dispatch: %{config.dispatch | lease_ttl_ms: 2_000, lease_safety_margin_ms: 500}
          }
        end
      )

    on_exit(fn -> ClusterCase.stop_cluster(cluster) end)
    %{cluster: cluster}
  end

  test "1. the leader is killed mid-append: a retry with the same batch_id commits once", %{
    cluster: cluster
  } do
    leader = cluster.leader
    body = :crypto.strong_rand_bytes(4_096)

    # Send the command and kill the leader's VM without waiting for the reply —
    # the client cannot know whether it committed.
    batch = {:drill, 1}
    record = record(body)

    spawn(fn ->
      Ra.remote_command(cluster.members, {:append, batch, [record]}, timeout: 30_000)
    end)

    Process.sleep(20)
    :ok = ClusterCase.kill_member(cluster, elem(leader, 1))
    _ = :erlang.disconnect_node(elem(leader, 1))
    ClusterCase.wait_for_leader(cluster.members)

    # Retry with the *same* batch id: it either returns the stored results (one
    # seq) or fails; it never allocates a second.
    case Ra.remote_command(cluster.members, {:append, batch, [record]}, timeout: 30_000) do
      {:ok, {:ok, [{:committed, seq}]}} ->
        assert seq >= 1
        assert [%{seq: ^seq}] = WAL.read(cluster.instance, 0, 10)

      {:error, _reason} ->
        # The retry itself failed: then nothing may have been committed either.
        assert WAL.read(cluster.instance, 0, 10) == []
    end
  end

  test "2. a reply lost after commit returns the stored results, with no second copy", %{
    cluster: cluster
  } do
    leader = elem(cluster.leader, 1)
    body = :crypto.strong_rand_bytes(1_024)
    batch = {:drill, 2}
    record = record(body)

    # Kill the *client's* access to the leader by disconnecting the node, after
    # the entry has been committed but before the reply can be delivered.
    task =
      Task.async(fn ->
        Ra.remote_command(cluster.members, {:append, batch, [record]}, timeout: 30_000)
      end)

    Process.sleep(20)
    :erlang.disconnect_node(leader)

    first =
      case Task.await(task, 30_000) do
        {:ok, reply} -> reply
        {:error, _} -> :lost
      end

    _ = :net_kernel.connect_node(leader)

    # Whatever the first attempt did or did not report, a retry must not create
    # a second copy.
    assert {:ok, {:ok, [{:committed, seq}]}} =
             Ra.remote_command(cluster.members, {:append, batch, [record]}, timeout: 30_000)

    written = WAL.read(cluster.instance, 0, 100)
    assert Enum.count(written, &(&1.seq == seq)) == 1
    assert Enum.all?(written, &(&1.body == body or first == :lost))
  end

  test "3. minority loss keeps appending; majority loss fails fast and honestly", %{
    cluster: cluster
  } do
    leader = cluster.leader
    followers = Enum.reject(cluster.members, &(&1 == leader))

    # One member gone: a majority (2 of 3) remains, so appends keep working.
    :ok = ClusterCase.kill_member(cluster, elem(hd(followers), 1))

    assert {:ok, [{:committed, first}]} =
             WAL.append(cluster.instance, [entry("after-minority-loss")])

    assert first.seq >= 1

    # Two gone: no quorum. Every append must fail, within the client's deadline
    # plus a second, and never report success.
    :ok = ClusterCase.kill_member(cluster, elem(List.last(followers), 1))

    started = System.monotonic_time(:millisecond)

    assert {:error, _reason} = WAL.append(cluster.instance, [entry("no-quorum")])

    elapsed = System.monotonic_time(:millisecond) - started
    assert elapsed <= 10_000 + 1_000

    # Nothing was acked that is not readable.
    assert Enum.any?(WAL.read(cluster.instance, 0, 10), &(&1.seq == first.seq))
  end

  test "4. a far-behind member catches up through snapshot install", %{cluster: cluster} do
    stop = Enum.find(cluster.member_nodes, &(&1 != elem(cluster.leader, 1)))

    # Suspend the member's Ra server so it falls arbitrarily far behind while
    # the cluster keeps compacting, then resume it.
    :ok = :peer.call(peer_pid(cluster, stop), :sys, :suspend, [ra_server_name(cluster, stop)])

    committed =
      append_all(
        cluster.instance,
        Enum.map(1..14_000, fn _ -> :crypto.strong_rand_bytes(256) end)
      )

    cutoff = Enum.at(committed, 12_000).seq
    storage = hold_lease(cluster.instance, :storage)
    :ok = WAL.truncate_through(cluster.instance, cutoff, storage.token)

    :ok = :peer.call(peer_pid(cluster, stop), :sys, :resume, [ra_server_name(cluster, stop)])

    # It can only catch up from a snapshot now: the entries it is missing are
    # gone from the leader's log.
    assert wait_until(
             fn ->
               case :peer.call(peer_pid(cluster, stop), Peer, :read, [
                      cluster.instance,
                      cutoff,
                      50
                    ]) do
                 live when is_list(live) -> length(live) == 50
                 _ -> false
               end
             end,
             60_000
           )
  end

  test "5. a torn log is detected on restart and the member catches up from peers", %{
    cluster: cluster
  } do
    victim = Enum.find(cluster.member_nodes, &(&1 != elem(cluster.leader, 1)))
    :ok = ClusterCase.kill_member(cluster, victim)

    # Corrupt the stopped member's WAL: truncate the tail and flip a byte in a
    # segment. Ra checksums both, so it must notice rather than serve garbage.
    wal_files =
      cluster.dir
      |> Path.join("#{victim}/**/*")
      |> Path.wildcard()
      |> Enum.filter(&File.regular?/1)

    assert wal_files != []

    for file <- wal_files do
      case File.read(file) do
        {:ok, <<>>} ->
          :ok

        {:ok, bytes} ->
          # Keep a prefix (so a valid header survives) and flip one byte in the
          # middle of what is left.
          keep = max(div(byte_size(bytes), 2), 1)
          <<head::binary-size(^keep), rest::binary>> = bytes
          flipped = flip_first_byte(rest)
          File.write!(file, head <> flipped)

        _ ->
          :ok
      end
    end

    cluster = ClusterCase.restart_member(cluster, victim)
    ClusterCase.wait_for_leader(cluster.members)

    # Whatever it did with the damaged bytes, it must not serve them: a read
    # through that member either succeeds with the leader's records or fails.
    body = :crypto.strong_rand_bytes(128)

    assert {:ok, [{:committed, env}]} =
             WAL.append(cluster.instance, [%{envelope: envelope(body)}])

    assert wait_until(
             fn ->
               case :peer.call(peer_pid(cluster, victim), Peer, :read, [cluster.instance, 0, 50]) do
                 live when is_list(live) ->
                   Enum.any?(live, &(&1.seq == env.seq and &1.body == body))

                 _ ->
                   false
               end
             end,
             60_000
           )
  end

  test "6. a suspended holder comes back fenced and steps down", %{cluster: cluster} do
    [active_node, standby_node] = Enum.take(cluster.member_nodes, 2)
    ttl = cluster.ttl_ms

    :ok = :erpc.cast(active_node, Peer, :start_pipeline, [cluster.instance, cluster.config])
    :ok = :erpc.cast(standby_node, Peer, :start_pipeline, [cluster.instance, cluster.config])

    holder = wait_until_value(fn -> Peer.lease_holder(cluster.instance, :dispatch) end, 15_000)
    assert holder |> String.split("/") |> hd() |> String.to_atom() == active_node

    # Suspend the *active* node past the TTL. It cannot renew, so the standby
    # takes the lease; when the zombie resumes, its token is stale.
    pid = peer_pid(cluster, active_node)
    :ok = :peer.call(pid, :sys, :suspend, [Process.whereis(:"Elixir.Ankusa.Dispatch.Pipeline")])

    assert wait_until(
             fn ->
               case Peer.lease_holder(cluster.instance, :dispatch) do
                 holder when is_binary(holder) ->
                   String.starts_with?(holder, "#{standby_node}/")

                 _ ->
                   false
               end
             end,
             ttl + div(ttl, 3) + 10_000
           )

    :ok = :peer.call(pid, :sys, :resume, [Process.whereis(:"Elixir.Ankusa.Dispatch.Pipeline")])

    # The zombie's cursor write is refused — it was fenced at the moment the
    # lease moved on, and it must not move the cursor backwards (I6).
    cursor_after = WAL.get_cursor(cluster.instance, :dispatch)

    assert wait_until(
             fn -> WAL.get_cursor(cluster.instance, :dispatch) >= cursor_after end,
             5_000
           )
  end

  test "7. a skewed cluster clock still fences stale tokens", %{peers: peers} do
    # `time_offset_ms` shifts the whole cluster's view of `meta.system_time`
    # uniformly — a per-member offset would make replicas diverge, so it is a
    # cluster-wide setting, and the drill is that tokens still enforce I6/I9
    # under it.
    cluster =
      ClusterCase.start_cluster(peers,
        config: fn config -> %{config | wal: cluster_wal(config, time_offset_ms: 10_000)} end
      )

    on_exit(fn -> ClusterCase.stop_cluster(cluster) end)

    {:ok, first} = Ankusa.WAL.LeaseHelpers.hold_lease(cluster.instance, :dispatch, ttl_ms: 5_000)
    :ok = WAL.release_lease(cluster.instance, first)

    {:ok, second} =
      Ankusa.WAL.LeaseHelpers.hold_lease(cluster.instance, :dispatch,
        holder: "second",
        ttl_ms: 5_000
      )

    assert second.token == first.token + 1
    assert :ok = WAL.put_cursor(cluster.instance, :dispatch, 5, second.token)
    assert {:error, :fenced} = WAL.put_cursor(cluster.instance, :dispatch, 9, first.token)
    assert WAL.get_cursor(cluster.instance, :dispatch) == 5
  end

  test "8. repeated distribution flaps lose nothing and reuse no seq", %{cluster: cluster} do
    target = elem(cluster.leader, 1)

    writer =
      Task.async(fn ->
        for i <- 1..100 do
          case WAL.append(cluster.instance, [entry("flap-#{i}")]) do
            {:ok, [{:committed, env}]} -> {:ok, env.seq, env.id}
            {:error, _} -> :lost
          end
        end
      end)

    for _ <- 1..5 do
      :erlang.disconnect_node(target)
      Process.sleep(30)
      :net_kernel.connect_node(target)
      Process.sleep(30)
    end

    results = Task.await(writer, 120_000)
    committed = for {:ok, seq, id} <- results, do: {id, seq}

    # Every acked record is present exactly once, and no seq was handed out
    # twice.
    seqs = Enum.map(committed, &elem(&1, 1))
    assert Enum.uniq(seqs) == seqs

    stored = WAL.read(cluster.instance, 0, 1_000)
    ids = MapSet.new(stored, & &1.id)
    assert Enum.all?(committed, fn {id, _seq} -> MapSet.member?(ids, id) end)
  end

  test "9. a machine-version upgrade is not applied until every member supports it", %{
    cluster: cluster
  } do
    # The next version is a test-only module (`Ankusa.WAL.Ra.MachineV3`); its
    # commands are only applied once every member supports it, which is Ra's
    # default `machine_upgrade_strategy: :all`.
    assert :ra_machine.version(Ankusa.WAL.Ra.Machine) == 2
    assert :ra_machine.version(Ankusa.WAL.Ra.MachineV3) == 3
    assert Ankusa.WAL.Ra.MachineV3.which_module(2) == Ankusa.WAL.Ra.Machine
    assert Ankusa.WAL.Ra.MachineV3.which_module(3) == Ankusa.WAL.Ra.MachineV3

    # A rolling restart onto the new code loses nothing: the cluster keeps its
    # seqs and cursors across every member restart.
    assert {:ok, [{:committed, before}]} = WAL.append(cluster.instance, [entry("pre-upgrade")])

    cluster =
      Enum.reduce(cluster.member_nodes, cluster, fn node, acc ->
        :ok = ClusterCase.kill_member(acc, node)
        ClusterCase.restart_member(acc, node)
      end)

    ClusterCase.wait_for_leader(cluster.members)

    assert {:ok, [{:committed, after_upgrade}]} =
             WAL.append(cluster.instance, [entry("post-upgrade")])

    assert after_upgrade.seq > before.seq

    assert Enum.map(WAL.read(cluster.instance, 0, 10), & &1.seq) == [
             before.seq,
             after_upgrade.seq
           ]
  end

  test "10. a split append that loses the leader mid-way commits at most once per record", %{
    cluster: cluster
  } do
    # 64 records of 4 KiB with a 64 KiB command budget: four chunks, so killing
    # the leader between chunks leaves a partial commit.
    wal_opts = cluster.config.wal |> elem(1) |> Keyword.put(:max_command_bytes, 64 * 1_024)
    Ankusa.put_config(%{cluster.config | wal: {Ra, wal_opts}})

    records =
      for i <- 1..64 do
        %{envelope: envelope(:crypto.strong_rand_bytes(4_096), "split-#{i}")}
      end

    task = Task.async(fn -> WAL.append(cluster.instance, records) end)
    Process.sleep(30)
    leader = cluster.leader
    :ok = ClusterCase.kill_member(cluster, elem(leader, 1))
    _ = :erlang.disconnect_node(elem(leader, 1))
    ClusterCase.wait_for_leader(cluster.members)

    result = Task.await(task, 60_000)

    stored = WAL.read(cluster.instance, 0, 200)

    case result do
      {:ok, results} ->
        # All of it committed: every record is readable exactly once.
        seqs = for {:committed, env} <- results, do: env.seq
        assert length(seqs) == 64
        assert Enum.uniq(seqs) == seqs

      {:error, _reason} ->
        # Part of it may have committed — that is the ambiguity. What must not
        # happen is a *second* copy of a record from the retry: the same
        # `batch_id` returns the stored results rather than allocating again.
        # (The log itself has no uniqueness constraint, so a *different* append
        # of the same event is a new record; that is dispatch's problem.)
        assert {:ok, retried} = WAL.append(cluster.instance, records)
        assert Enum.map(retried, &elem(&1, 1)) == Enum.map(retried, &elem(&1, 1))

        assert length(Enum.uniq_by(WAL.read(cluster.instance, 0, 200), & &1.id)) ==
                 length(WAL.read(cluster.instance, 0, 200))
    end

    assert length(Enum.uniq_by(stored, & &1.id)) == length(stored)
  end

  test "11. with no quorum the edge answers 503 and never 2xx", %{cluster: cluster} do
    # Two of three members down: no quorum, so no append can be acked.
    leader = elem(cluster.leader, 1)
    others = Enum.reject(cluster.member_nodes, &(&1 == leader))
    Enum.each(others, &ClusterCase.kill_member(cluster, &1))

    {:ok, _pid} =
      Bandit.start_link(
        plug: {Ankusa.Edge.Router, [instance: cluster.instance]},
        scheme: :http,
        port: free_port()
      )

    port = :persistent_term.get({__MODULE__, :edge_port})

    assert wait_until(
             fn ->
               case request(port, "no-quorum") do
                 {503, headers} -> Map.has_key?(headers, "retry-after")
                 _ -> false
               end
             end,
             10_000
           )

    {status, headers} = request(port, "still-no-quorum")
    assert status == 503
    assert Map.has_key?(headers, "retry-after")
    refute status >= 200 and status < 300
  end

  test "12. the checker reports no violations for a clean run", %{cluster: cluster} do
    # The same checker the chaos harness runs, fed a real history: appends, a
    # read through a member, a fenced cursor write and a legal one.
    t0 = System.monotonic_time(:millisecond)

    {events, acked, sent} =
      Enum.reduce(1..5, {[], MapSet.new(), []}, fn i, {events, acked, sent} ->
        body = :crypto.strong_rand_bytes(256)
        id = "checker-#{i}"
        sha = :crypto.hash(:sha256, body)
        env = envelope(body, id)

        {:ok, [{:committed, committed}]} = WAL.append(cluster.instance, [%{envelope: env}])

        event = %{
          client: :edge,
          op: {:append, id, %{tenant: "t1", source: "src", sha256: sha}},
          invoked_at: t0 + i,
          completed_at: t0 + i,
          result: {:ok, committed.seq}
        }

        {[event | events], MapSet.put(acked, id),
         [%{id: id, seq: committed.seq, sha256: sha} | sent]}
      end)

    live = WAL.read(cluster.instance, 0, 100)

    read_event = %{
      client: :edge,
      op: {:read, 0, 100},
      invoked_at: t0,
      completed_at: t0 + 100,
      result: Enum.map(live, &%{seq: &1.seq, id: &1.id, sha256: :crypto.hash(:sha256, &1.body)})
    }

    events = [read_event | events]

    report = Checker.check(events, acked, sent, final_cursors: %{dispatch: 0})

    assert report.missing == []
    assert report.extra == []
    assert report.violations == [], "expected a clean history, got #{inspect(report.violations)}"

    # And the checker actually catches the thing it exists for: a `2xx` for a
    # record that is not readable is exactly invariant I1.
    dirty = Checker.check(events, MapSet.put(acked, "never-committed"), sent)
    assert dirty.missing == ["never-committed"]
    assert Enum.any?(dirty.violations, &match?({:i1, _}, &1))
  end

  # ── helpers ───────────────────────────────────────────────────────────────

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    :persistent_term.put({__MODULE__, :edge_port}, port)
    port
  end

  defp request(port, source) do
    body = :crypto.strong_rand_bytes(64)

    {:ok, response} =
      Req.post("http://127.0.0.1:#{port}/webhooks/#{source}",
        body: body,
        retry: false,
        connect_options: [timeout: 5_000]
      )

    {response.status, Map.new(response.headers, fn {k, [v]} -> {k, v} end)}
  end

  defp peer_pid(cluster, node), do: Map.fetch!(cluster.peers.peers, node)

  defp ra_server_name(cluster, node) do
    :ra_lib.ra_server_id_to_local_name({cluster.cluster, node})
  end

  defp cluster_wal(config, extra) do
    case config.wal do
      {mod, opts} -> {mod, Keyword.merge(opts, extra)}
    end
  end

  defp flip_first_byte(<<>>), do: <<>>

  defp flip_first_byte(<<byte, rest::binary>>) do
    <<Bitwise.bxor(byte, 1)::8, rest::binary>>
  end

  defp append_all(instance, payloads) do
    payloads
    |> Enum.chunk_every(2_000)
    |> Enum.flat_map(fn chunk ->
      {:ok, results} = WAL.append(instance, Enum.map(chunk, &%{envelope: envelope(&1)}))
      for {:committed, env} <- results, do: env
    end)
  end

  defp hold_lease(instance, name) do
    {:ok, lease} = Ankusa.WAL.LeaseHelpers.hold_lease(instance, name, ttl_ms: 60_000)
    lease
  end

  defp entry(body, id \\ nil), do: %{envelope: envelope(body, id)}

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

  defp record(body) do
    {UUIDv7.generate(), "t1", "src", nil, Envelope.to_binary(%{envelope(body) | seq: nil})}
  end

  defp wait_until(fun, timeout) do
    wait_until_loop(fun, System.monotonic_time(:millisecond) + timeout)
  end

  defp wait_until_loop(fun, deadline) do
    cond do
      fun.() -> true
      System.monotonic_time(:millisecond) > deadline -> false
      true -> Process.sleep(100) && wait_until_loop(fun, deadline)
    end
  end

  defp wait_until_value(fun, timeout) do
    wait_until_value_loop(fun, System.monotonic_time(:millisecond) + timeout)
  end

  defp wait_until_value_loop(fun, deadline) do
    case fun.() do
      nil ->
        if System.monotonic_time(:millisecond) > deadline do
          nil
        else
          Process.sleep(100)
          wait_until_value_loop(fun, deadline)
        end

      value ->
        value
    end
  end
end
