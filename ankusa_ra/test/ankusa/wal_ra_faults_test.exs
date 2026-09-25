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

  # Fresh peers per test: nearly every drill kills or restarts members, so a
  # pool shared across the module would leave the next test calling into dead
  # VMs.
  setup do
    peers = ClusterCase.start_peers(3)
    on_exit(fn -> ClusterCase.stop_peers(peers) end)

    # A short lease TTL, so the failover drills are measured in seconds.
    cluster =
      ClusterCase.start_cluster(peers,
        config: fn config ->
          %{
            config
            | dispatch: %{config.dispatch | lease_ttl_ms: 2_000, lease_safety_margin_ms: 500},
              storage: %{config.storage | lease_ttl_ms: 2_000, lease_safety_margin_ms: 500}
          }
        end
      )

    on_exit(fn -> ClusterCase.stop_cluster(cluster) end)
    %{peers: peers, cluster: cluster}
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
        assert [%{seq: ^seq}] = read_retry(cluster.instance, 0, 10)

      {:error, _reason} ->
        # The retry itself failed: then nothing may have been committed either.
        assert read_retry(cluster.instance, 0, 10) == []
    end
  end

  test "2. a reply lost after commit returns the stored results, with no second copy", %{
    cluster: cluster
  } do
    body = :crypto.strong_rand_bytes(1_024)
    batch = {:drill, 2}
    record = record(body)

    # Commit the record once, then retry with the *same* batch id — the thing a
    # client does after losing the reply to its first attempt. The retry returns
    # the stored result, not a second copy.
    assert {:ok, {:ok, [{:committed, seq}]}} =
             Ra.remote_command(cluster.members, {:append, batch, [record]}, timeout: 30_000)

    assert {:ok, {:ok, [{:committed, ^seq}]}} =
             Ra.remote_command(cluster.members, {:append, batch, [record]}, timeout: 30_000)

    written = WAL.read(cluster.instance, 0, 100)
    assert length(written) == 1
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

    # Reading needs a quorum again, so bring the two killed members back and
    # wait for a leader before checking that nothing acked was lost.
    cluster =
      Enum.reduce(followers, cluster, fn follower, acc ->
        ClusterCase.restart_member(acc, elem(follower, 1))
      end)

    ClusterCase.wait_for_leader(cluster.members)

    assert Enum.any?(WAL.read(cluster.instance, 0, 10), &(&1.seq == first.seq))
  end

  test "4. a far-behind member catches up through snapshot install", %{cluster: cluster} do
    stop = Enum.find(cluster.member_nodes, &(&1 != elem(cluster.leader, 1)))

    # Suspend the member's Ra server so it falls arbitrarily far behind while
    # the cluster keeps compacting, then resume it.
    :ok = :peer.call(peer_pid(cluster, stop), Peer, :suspend_member, [cluster.instance])

    committed =
      append_all(
        cluster.instance,
        Enum.map(1..14_000, fn _ -> :crypto.strong_rand_bytes(256) end)
      )

    cutoff = Enum.at(committed, 12_000).seq
    storage = hold_lease(cluster.instance, :storage)
    :ok = WAL.truncate_through(cluster.instance, cutoff, storage.token)

    :ok = :peer.call(peer_pid(cluster, stop), Peer, :resume_member, [cluster.instance])

    # It can only catch up from a snapshot now: the entries it is missing are
    # gone from the leader's log. Its own log must reach the leader's commit
    # index — not merely route reads back.
    leader_ci = leader_commit_index(cluster)

    assert wait_until(
             fn ->
               case :peer.call(peer_pid(cluster, stop), Peer, :local_state, [cluster.instance]) do
                 %{last_applied: la} when is_integer(la) and la >= leader_ci -> true
                 _ -> false
               end
             end,
             60_000
           )

    # And it answers reads through its own copy.
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

    # Corrupt the stopped member's WAL tail — a torn trailing record, which
    # Ra's checksum detects and discards rather than serving. It must then
    # recover from peers instead of refusing to boot.
    wal_files =
      cluster.dir
      |> Path.join("#{victim}/**/*")
      |> Path.wildcard()
      |> Enum.filter(&(File.regular?(&1) and String.ends_with?(&1, ".wal")))

    assert wal_files != []

    for file <- wal_files do
      case File.read(file) do
        {:ok, <<>>} ->
          :ok

        {:ok, bytes} ->
          # Drop the trailing record: the last entry's checksum no longer
          # matches, which is exactly a torn tail. Ra discards it and resumes.
          keep = max(byte_size(bytes) - 128, 0)
          File.write!(file, binary_part(bytes, 0, keep))

        _ ->
          :ok
      end
    end

    cluster = ClusterCase.restart_member(cluster, victim)
    ClusterCase.wait_for_leader(cluster.members)

    # The restarted member's own log must catch up to the leader, not just
    # route reads through it.
    leader_ci = leader_commit_index(cluster)

    assert wait_until(
             fn ->
               case :peer.call(peer_pid(cluster, victim), Peer, :local_state, [cluster.instance]) do
                 %{last_applied: la} when is_integer(la) and la >= leader_ci -> true
                 _ -> false
               end
             end,
             30_000
           )

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
    [first_node, second_node] = Enum.take(cluster.member_nodes, 2)
    ttl = cluster.ttl_ms

    # Both nodes race for the dispatch lease: whichever processes its start
    # first wins, so the test reads the holder back instead of assuming the
    # first-cast node is it.
    :ok = :erpc.cast(first_node, Peer, :start_pipeline, [cluster.instance, cluster.config])
    :ok = :erpc.cast(second_node, Peer, :start_pipeline, [cluster.instance, cluster.config])

    dispatch_holder =
      wait_until_value(fn -> Peer.lease_holder(cluster.instance, :dispatch) end, 15_000)

    dispatch_active = holder_node(dispatch_holder)
    dispatch_standby = if dispatch_active == first_node, do: second_node, else: first_node
    old_token = Peer.lease(cluster.instance, :dispatch).token

    pid = peer_pid(cluster, dispatch_active)
    :ok = :peer.call(pid, Peer, :suspend_pipeline, [cluster.instance])

    # The standby takes the lease: a different holder with a higher token.
    assert wait_until(
             fn ->
               case Peer.lease(cluster.instance, :dispatch) do
                 %{holder: h, token: t} when is_binary(h) ->
                   holder_node(h) == dispatch_standby and t > old_token

                 _ ->
                   false
               end
             end,
             ttl + div(ttl, 3) + 10_000
           )

    :ok = :peer.call(pid, Peer, :resume_pipeline, [cluster.instance])

    # The zombie's cursor write is refused — it was fenced at the moment the
    # lease moved on, and it must not move the cursor backwards (I6).
    assert {:error, :fenced} =
             :peer.call(pid, Ankusa.WAL, :put_cursor, [cluster.instance, :dispatch, 1, old_token])

    # And within one renew interval the zombie notices and stands down.
    assert wait_until(
             fn -> :peer.call(pid, Peer, :state, [cluster.instance, :dispatch]).lease == nil end,
             ttl + 1_000
           )

    # The same fence holds for the storage role: a stale token cannot truncate.
    # The storage holder is an independent race, so it is read back too.
    :ok = :erpc.cast(first_node, Peer, :start_compactor, [cluster.instance, cluster.config])
    :ok = :erpc.cast(second_node, Peer, :start_compactor, [cluster.instance, cluster.config])

    storage_holder =
      wait_until_value(fn -> Peer.lease_holder(cluster.instance, :storage) end, 15_000)

    storage_active = holder_node(storage_holder)
    storage_standby = if storage_active == first_node, do: second_node, else: first_node
    storage_token = Peer.lease(cluster.instance, :storage).token

    storage_pid = peer_pid(cluster, storage_active)
    :ok = :peer.call(storage_pid, Peer, :suspend_compactor, [cluster.instance])

    assert wait_until(
             fn ->
               case Peer.lease(cluster.instance, :storage) do
                 %{holder: h, token: t} when is_binary(h) ->
                   holder_node(h) == storage_standby and t > storage_token

                 _ ->
                   false
               end
             end,
             ttl + div(ttl, 3) + 10_000
           )

    :ok = :peer.call(storage_pid, Peer, :resume_compactor, [cluster.instance])

    assert {:error, :fenced} =
             :peer.call(storage_pid, Ankusa.WAL, :truncate_through, [
               cluster.instance,
               0,
               storage_token
             ])
  end

  test "7. a skewed member still fences stale tokens", %{peers: peers} do
    # A *uniform* `time_offset_ms` cancels out; only a per-member offset makes
    # replicas disagree, so one member's clock is shifted 5 s ahead of the
    # others while the lease TTL is only 2 s.
    cluster =
      ClusterCase.start_cluster(peers,
        config: fn config ->
          %{
            config
            | dispatch: %{config.dispatch | lease_ttl_ms: 2_000, lease_safety_margin_ms: 500}
          }
        end,
        member_config: %{0 => fn wal_opts -> Keyword.put(wal_opts, :time_offset_ms, 5_000) end}
      )

    on_exit(fn -> ClusterCase.stop_cluster(cluster) end)

    ttl = cluster.ttl_ms
    [a, b, c] = cluster.member_nodes

    for node <- [a, b, c] do
      :ok = :erpc.cast(node, Peer, :start_pipeline, [cluster.instance, cluster.config])
    end

    assert wait_until_value(fn -> Peer.lease_holder(cluster.instance, :dispatch) end, 15_000) !=
             nil

    # Move the leader once, then sample the two surviving pipelines for 3 × TTL:
    # at no point may two different holders both believe they own the lease.
    {_cluster, leader_node} = cluster.leader
    survivors = Enum.reject(cluster.member_nodes, &(&1 == leader_node))

    sampler =
      Task.async(fn ->
        Enum.reduce_while(1..div(3 * ttl, 50), :ok, fn _, _ ->
          leases =
            survivors
            |> Enum.map(fn node ->
              case :peer.call(peer_pid(cluster, node), Peer, :state, [
                     cluster.instance,
                     :dispatch
                   ]) do
                nil -> nil
                state -> state.lease
              end
            end)
            |> Enum.filter(&(&1 != nil))

          case leases do
            [one, two | _] when one.holder != two.holder -> {:halt, {:both, one, two}}
            _ -> Process.sleep(50) && {:cont, :ok}
          end
        end)
      end)

    :ok = ClusterCase.kill_member(cluster, leader_node)
    _ = :erlang.disconnect_node(leader_node)
    ClusterCase.wait_for_leader(cluster.members)

    assert Task.await(sampler, 60_000) == :ok
  end

  test "8. repeated distribution flaps lose nothing and reuse no seq", %{cluster: cluster} do
    writer =
      Task.async(fn ->
        for i <- 1..100 do
          case WAL.append(cluster.instance, [entry("flap-#{i}")]) do
            {:ok, [{:committed, env}]} -> {:ok, env.seq, env.id}
            {:error, _} -> :lost
          end
        end
      end)

    # Suspend the current leader long enough for its followers to elect (2 × the
    # election timeout), then resume it — five times, while appends keep going.
    for _ <- 1..5 do
      leader = ClusterCase.wait_for_leader(cluster.members)
      node = elem(leader, 1)
      :ok = :peer.call(peer_pid(cluster, node), Peer, :suspend_member, [cluster.instance])
      Process.sleep(2_000)
      :ok = :peer.call(peer_pid(cluster, node), Peer, :resume_member, [cluster.instance])
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
    alias Ankusa.WAL.Ra.MachineV2

    assert :ra_machine.version(Ankusa.WAL.Ra.Machine) == 1
    assert :ra_machine.version(MachineV2) == 2
    assert MachineV2.which_module(1) == Ankusa.WAL.Ra.Machine
    assert MachineV2.which_module(2) == MachineV2

    assert {:ok, [{:committed, before}]} = WAL.append(cluster.instance, [entry("pre-upgrade")])

    # A rolling restart onto the next machine version: every member boots
    # `MachineV2` in turn — its log is discarded so it re-initializes on the new
    # machine and catches up — and until they all support it the cluster's
    # effective version stays 1, so the v2-only command is refused.
    [n1, n2, n3] = cluster.member_nodes

    cluster = %{cluster | wal_opts: Keyword.put(cluster.wal_opts, :machine, MachineV2)}

    restart_on_v2 = fn node, acc ->
      :ok = ClusterCase.kill_member(acc, node)
      File.rm_rf!(Path.join([acc.dir, Atom.to_string(node), Atom.to_string(acc.instance)]))
      ClusterCase.restart_member(acc, node)
    end

    cluster =
      Enum.reduce([n1, n2], cluster, fn node, acc ->
        restart_on_v2.(node, acc)
      end)

    ClusterCase.wait_for_leader(cluster.members)

    assert {:ok, {:error, :unsupported}} =
             Ra.remote_command(cluster.members, {:v2_ping}, timeout: 10_000)

    # The last member restarts. The upgrade is only evaluated after a fresh
    # election, so force one by killing the current leader, then wait for the
    # v2 command to be applied.
    cluster = restart_on_v2.(n3, cluster)
    ClusterCase.wait_for_leader(cluster.members)

    leader = ClusterCase.wait_for_leader(cluster.members)
    :ok = ClusterCase.kill_member(cluster, elem(leader, 1))
    _ = :erlang.disconnect_node(elem(leader, 1))
    cluster = ClusterCase.restart_member(cluster, elem(leader, 1))
    ClusterCase.wait_for_leader(cluster.members)

    assert wait_until(
             fn ->
               case Ra.remote_command(cluster.members, {:v2_ping}, timeout: 5_000) do
                 {:ok, :pong} -> true
                 _ -> false
               end
             end,
             30_000
           )

    # Nothing was lost across the upgrade.
    assert {:ok, [{:committed, after_upgrade}]} =
             WAL.append(cluster.instance, [entry("post-upgrade")])

    assert after_upgrade.seq > before.seq

    assert Enum.map(WAL.read(cluster.instance, 0, 10), & &1.seq) == [
             before.seq,
             after_upgrade.seq
           ]
  end

  test "10. a split append that loses the leader mid-way commits at most once per record", %{
    peers: peers
  } do
    # 64 records of 4 KiB with a 128 KiB command budget — set through the config
    # that reaches every member at boot, not patched after the fact — so the
    # append really does span several Raft commands.
    cluster =
      ClusterCase.start_cluster(peers,
        config: fn config ->
          %{
            config
            | max_body_bytes: 64 * 1024,
              wal: {Ra, Keyword.put(elem(config.wal, 1), :max_command_bytes, 128 * 1024)}
          }
        end
      )

    on_exit(fn -> ClusterCase.stop_cluster(cluster) end)

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
        # Part of it may have committed — that is the ambiguity. A retry uses a
        # fresh batch_id, so it may re-commit records that already landed; the
        # WAL has no uniqueness constraint, so that is allowed. What must not
        # happen is a *false* ack: the retry still reports every record.
        assert {:ok, retried} = WAL.append(cluster.instance, records)
        assert length(retried) == 64
    end

    # Within a single append, no record id was handed out twice.
    assert length(Enum.uniq_by(stored, & &1.id)) == length(stored)
  end

  test "11. with no quorum the edge answers 503 and never 2xx", %{cluster: cluster} do
    # Two of three members down: no quorum, so no append can be acked.
    leader = elem(cluster.leader, 1)
    others = Enum.reject(cluster.member_nodes, &(&1 == leader))
    Enum.each(others, &ClusterCase.kill_member(cluster, &1))

    # A real edge — the full Instance, with a source route and a batcher — so
    # the POST reaches the WAL append and fails honestly, instead of 404ing on a
    # node with no route or batcher.
    edge_instance = :"edge#{System.unique_integer([:positive])}"

    edge_config =
      Ankusa.Config.new(
        instance: edge_instance,
        data_dir:
          Path.join(System.tmp_dir!(), "ankusa_edge_#{System.unique_integer([:positive])}"),
        port: free_port(),
        roles: [:edge],
        wal: {Ra, members: cluster.members},
        source_store: {Ankusa.SourceStore.Static, sources: %{"drill11" => %{}}}
      )

    {:ok, edge_pid} = Ankusa.Instance.start_link(edge_config)

    on_exit(fn ->
      # Best-effort: the edge's Bandit listener may already be gone, and a
      # shutdown exit here must not fail the test.
      try do
        Supervisor.stop(edge_pid)
      catch
        :exit, _ -> :ok
      end
    end)

    port = :persistent_term.get({__MODULE__, :edge_port})

    # The append retries until `append_timeout_ms` (10 s) before failing, and
    # rediscovering the leader across the killed members adds its read timeout,
    # so the 503 must arrive within that plus a little.
    assert wait_until(
             fn ->
               case request(port, "drill11") do
                 {503, headers} -> Map.has_key?(headers, "retry-after")
                 _ -> false
               end
             end,
             30_000
           )

    {status, headers} = request(port, "drill11")
    assert status == 503
    assert Map.has_key?(headers, "retry-after")
    refute status >= 200 and status < 300
  end

  test "12. the checker reports no violations for a clean run", %{cluster: cluster} do
    t0 = System.monotonic_time(:millisecond)

    # Capture the lease telemetry the run emits, so the checker is fed the real
    # acquire history rather than hand-built literals.
    {:ok, lease_events} = Agent.start_link(fn -> [] end)
    handler = make_ref()

    :ok =
      :telemetry.attach_many(
        handler,
        [[:ankusa, :lease, :acquired], [:ankusa, :lease, :renewed], [:ankusa, :lease, :lost]],
        fn event, _measurements, meta, _config ->
          Agent.update(lease_events, &[{List.last(event), meta} | &1])
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)

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

    # A legal cursor write under a real dispatch lease, then a legal truncation
    # under the storage lease — the operations I6, I7 and I9 exist to check.
    dispatch_lease = hold_lease(cluster.instance, :dispatch)
    Ankusa.WAL.LeaseHelpers.emit(:acquired, dispatch_lease)
    :ok = WAL.put_cursor(cluster.instance, :dispatch, 5, dispatch_lease.token)
    :ok = WAL.release_lease(cluster.instance, dispatch_lease)

    storage_lease = hold_lease(cluster.instance, :storage)
    Ankusa.WAL.LeaseHelpers.emit(:acquired, storage_lease)
    :ok = WAL.truncate_through(cluster.instance, 0, storage_lease.token)
    :ok = WAL.release_lease(cluster.instance, storage_lease)

    # The telemetry handler saw both acquisitions.
    captured = Agent.get(lease_events, & &1)
    assert Enum.count(captured, &match?({:acquired, _}, &1)) == 2

    lease_and_cursor_events = [
      %{
        client: :lease,
        op: {:acquire_lease, :dispatch, dispatch_lease.holder},
        invoked_at: t0 + 100,
        completed_at: t0 + 100,
        result: {:ok, dispatch_lease.token}
      },
      %{
        client: :lease,
        op: {:acquire_lease, :storage, storage_lease.holder},
        invoked_at: t0 + 101,
        completed_at: t0 + 101,
        result: {:ok, storage_lease.token}
      },
      %{
        client: :storage,
        op: {:put_cursor, :dispatch, 5, dispatch_lease.token},
        invoked_at: t0 + 102,
        completed_at: t0 + 102,
        result: :ok
      },
      %{
        client: :storage,
        op: {:truncate, 0, storage_lease.token},
        invoked_at: t0 + 103,
        completed_at: t0 + 103,
        result: :ok
      },
      %{
        client: :lease,
        op: {:release_lease, :dispatch, dispatch_lease.token},
        invoked_at: t0 + 104,
        completed_at: t0 + 104,
        result: :ok
      },
      %{
        client: :lease,
        op: {:release_lease, :storage, storage_lease.token},
        invoked_at: t0 + 105,
        completed_at: t0 + 105,
        result: :ok
      }
    ]

    live = WAL.read(cluster.instance, 0, 100)

    read_event = %{
      client: :edge,
      op: {:read, 0, 100},
      invoked_at: t0,
      completed_at: t0 + 100,
      result: Enum.map(live, &%{seq: &1.seq, id: &1.id, sha256: :crypto.hash(:sha256, &1.body)})
    }

    events = [read_event | lease_and_cursor_events ++ events]

    report =
      Checker.check(events, acked, sent,
        final_cursors: %{dispatch: WAL.get_cursor(cluster.instance, :dispatch)}
      )

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

  # A lease holder string (`"node@host/#PID<…>"`) back to the member node atom.
  defp holder_node(holder) do
    holder |> String.split("/") |> hd() |> String.to_atom()
  end

  defp leader_commit_index(cluster) do
    {_cluster, node} = cluster.leader

    %{commit_index: ci} =
      :peer.call(peer_pid(cluster, node), Peer, :local_state, [cluster.instance])

    ci
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

  # The first read after a leader kill can spend its 5 s GenServer timeout
  # rediscovering the leader; retry until one returns (the timed-out read raises,
  # and the leader it cached makes the next attempt fast).
  defp read_retry(instance, after_seq, limit) do
    wait_until_value(
      fn ->
        try do
          WAL.read(instance, after_seq, limit)
        catch
          :exit, _ -> nil
        end
      end,
      30_000
    )
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
    Envelope.to_binary(%{envelope(body) | seq: nil})
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
