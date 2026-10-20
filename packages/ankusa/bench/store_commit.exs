# The store commit under load: what the queue writer's group commit and pipelined
# WAL sync buy, with the fsync made slow and the bodies made large (DUR-4).
#
#     MIX_ENV=test mix run bench/store_commit.exs
#
# Both cases drive `Ankusa.Edge.Batcher.commit/4` directly (no HTTP, no verifier)
# with 10 KB bodies over 4 partitions, and make the fsync cost what a disk's does
# by sleeping for `fsync` milliseconds on every WAL sync (a telemetry handler on
# `[:ankusa, :wal_sync, :start]` runs in `Ankusa.Queue.Syncer`, before its
# `sync_wal`, so the sleep is the sync's own wait and the writer is free to apply
# the next group meanwhile).
#
#   1. Throughput. 256 closed-loop clients for `DURATION` seconds, at a simulated
#      fsync of 1, 5 and 10 ms: records per second, groups applied per second,
#      fsyncs per second, mean group (records per group), groups per fsync, the
#      mean time the writer spends applying one group, and ack p50/p99. Each cell
#      runs grouped
#      (`batcher.group_commit_*` and `max_commits` at their defaults) and ungrouped
#      (`group_commit_max_records: 1, max_commits: 1, group_commit_delay_ms: 0`:
#      every partition's batch is a commit of its own, one at a time, which is what
#      the writer did before cross-partition group commit).
#
#   2. Burst. 1 000 req/s of 10 KB bodies arrive open loop; the writer stalls for
#      one second (the requests pile up behind it), then drains while arrivals
#      continue. The requests that arrived *after* the stall are the ones the
#      group cap protects: they queue behind the backlog, so they wait for as
#      many groups as the backlog makes.
#
# Passes only if, at the 5 ms cell, grouped throughput is at least 1.5 x ungrouped
# (ungrouped shares fsyncs across partitions too since the WAL sync is pipelined,
# so what is left of the gap is the size of the group: 2 x before DUR-4.b, when
# every ungrouped commit paid its own fsync), the writer applies more groups per
# second than the disk syncs (at least 1.2 x: a writer that waits for the fsync
# after every group applies one group per fsync, 1 / (apply + fsync): the fsync is
# off its critical path), and the burst's post-stall ack p99 is at most 50 ms
# (the SLO). Exits 1 otherwise.
#
# Env knobs: DURATION (3), CLIENTS (256), BODY_BYTES (10240), BURST_RATE (1000),
# BURST_FSYNC_MS (2), BURST_STALL_MS (1000), BURST_MS (3000).

Logger.configure(level: :warning)

defmodule Bench.StoreCommit do
  alias Ankusa.Edge.Batcher
  alias Ankusa.Envelope

  @partitions 4

  def run(opts) do
    seconds = int("DURATION", 3)
    clients = int("CLIENTS", 256)
    body = :crypto.strong_rand_bytes(int("BODY_BYTES", 10_240))

    if System.get_env("SWEEP") == "1",
      do: sweep(clients, seconds, body),
      else: measure(opts, clients, seconds, body)
  end

  # `SWEEP=1`: how the commit slots per partition shape the groups (not a check).
  defp sweep(clients, seconds, body) do
    IO.puts(
      "\nsweep: #{clients} clients, 5 ms fsync, #{@partitions} partitions\nmax_commits records/s  commits/s  mean group   p50 ms   p99 ms"
    )

    for k <- [1, 2, 4, 8, 16, 64] do
      r = throughput(%{max_commits: k}, 5, clients, seconds, body)

      IO.puts(
        "#{String.pad_trailing("#{k}", 11)}#{String.pad_leading(fmt(r.per_s, 0), 10)}  #{String.pad_leading(fmt(r.commits_per_s, 0), 9)}  #{String.pad_leading(fmt(r.mean_group), 10)}  #{String.pad_leading(fmt(r.p50_ms), 7)}  #{String.pad_leading(fmt(r.p99_ms), 7)}"
      )
    end
  end

  defp measure(opts, clients, seconds, body) do
    IO.puts("""

    throughput: #{clients} closed-loop clients, #{byte_size(body)} B bodies, #{@partitions} partitions, #{seconds} s per cell
    fsync   mode       records/s  groups/s  fsyncs/s  mean group  groups/fsync  apply ms   p50 ms   p99 ms
    """)

    rows =
      for fsync <- [1, 5, 10], mode <- [:ungrouped, :grouped], into: %{} do
        result = throughput(batcher_for(mode), fsync, clients, seconds, body)
        print(fsync, mode, result)
        {{fsync, mode}, result}
      end

    ratios =
      for fsync <- [1, 5, 10], into: %{} do
        {fsync, rows[{fsync, :grouped}].per_s / max(rows[{fsync, :ungrouped}].per_s, 1)}
      end

    IO.puts(
      "\ngrouped / ungrouped records per second: " <>
        Enum.map_join(ratios, ", ", fn {f, r} -> "#{f} ms: #{fmt(r)}x" end)
    )

    burst = burst(body)

    five = rows[{5, :grouped}]
    overlap = five.commits_per_s / max(five.syncs_per_s, 0.001)

    failures =
      [
        {:ratio_5ms, ratios[5] >= 1.5,
         "grouped throughput at 5 ms fsync is #{fmt(ratios[5])}x ungrouped (< 1.5x)"},
        {:pipelined, overlap >= 1.2,
         "at 5 ms fsync the writer applied #{fmt(five.commits_per_s, 0)} groups/s against " <>
           "#{fmt(five.syncs_per_s, 0)} fsyncs/s (#{fmt(overlap)} per fsync, < 1.2): " <>
           "the fsync is still on its critical path"},
        {:burst_p99, burst.p99_ms <= 50.0,
         "burst post-stall ack p99 #{fmt(burst.p99_ms)} ms (> 50 ms)"}
      ]
      |> Enum.reject(&elem(&1, 1))

    if opts[:verbose], do: IO.inspect(burst)

    case failures do
      [] ->
        IO.puts(
          "\nPASS: grouped >= 1.5x ungrouped at 5 ms fsync, #{fmt(overlap)} groups applied per " <>
            "fsync (>= 1.2), burst p99 <= 50 ms"
        )

      failures ->
        for {_name, _ok, message} <- failures, do: IO.puts("FAIL: #{message}")
        System.halt(1)
    end
  end

  # ── throughput ────────────────────────────────────────────────────────────

  defp batcher_for(:grouped), do: %{}

  defp batcher_for(:ungrouped),
    do: %{group_commit_max_records: 1, max_commits: 1, group_commit_delay_ms: 0}

  defp throughput(batcher, fsync, clients, seconds, body) do
    with_instance(batcher, fsync, fn inst, commits ->
      deadline = System.monotonic_time(:millisecond) + seconds * 1000
      started = System.monotonic_time(:microsecond)

      latencies =
        1..clients
        |> Task.async_stream(
          fn i -> client_loop(inst, rem(i, @partitions), body, deadline, []) end,
          max_concurrency: clients,
          timeout: :infinity
        )
        |> Enum.flat_map(fn {:ok, micros} -> Enum.map(micros, &(&1 / 1000)) end)

      elapsed = (System.monotonic_time(:microsecond) - started) / 1_000_000
      n = length(latencies)
      sorted = Enum.sort(latencies)
      c = :counters.get(commits, 1)
      s = :counters.get(commits, 2)
      apply_ms = :counters.get(commits, 3) / 1000 / max(c, 1)

      %{
        per_s: n / elapsed,
        commits_per_s: c / elapsed,
        syncs_per_s: s / elapsed,
        mean_group: n / max(c, 1),
        groups_per_sync: c / max(s, 1),
        apply_ms: apply_ms,
        p50_ms: pct(sorted, 0.50),
        p99_ms: pct(sorted, 0.99)
      }
    end)
  end

  defp client_loop(inst, partition, body, deadline, acc) do
    if System.monotonic_time(:millisecond) >= deadline do
      acc
    else
      t0 = System.monotonic_time(:microsecond)
      {:committed, _} = Batcher.commit(inst, partition, entry(body), :infinity)

      client_loop(inst, partition, body, deadline, [
        System.monotonic_time(:microsecond) - t0 | acc
      ])
    end
  end

  # ── burst ─────────────────────────────────────────────────────────────────

  defp burst(body) do
    rate = int("BURST_RATE", 1000)
    fsync = int("BURST_FSYNC_MS", 2)
    stall_ms = int("BURST_STALL_MS", 1000)
    total_ms = int("BURST_MS", 3000)

    with_instance(%{}, fsync, fn inst, _commits ->
      writer = Ankusa.whereis(inst, :queue_writer)
      parent = self()
      started = System.monotonic_time(:millisecond)

      :ok = :sys.suspend(writer)

      resumer =
        spawn_link(fn ->
          Process.sleep(stall_ms)
          :ok = :sys.resume(writer)
        end)

      # open loop: request i is due at i / rate, whatever became of the ones before
      count = div(total_ms * rate, 1000)

      for i <- 0..(count - 1) do
        due = started + div(i * 1000, rate)
        wait = due - System.monotonic_time(:millisecond)
        if wait > 0, do: Process.sleep(wait)

        spawn_link(fn ->
          t0 = System.monotonic_time(:millisecond)
          {:committed, _} = Batcher.commit(inst, rem(i, @partitions), entry(body), :infinity)
          send(parent, {:acked, t0 - started, System.monotonic_time(:millisecond) - t0})
        end)
      end

      _ = resumer
      acks = collect(count, [])
      {during, after_stall} = Enum.split_with(acks, fn {arrived, _ms} -> arrived < stall_ms end)
      lat = fn rows -> rows |> Enum.map(&elem(&1, 1)) |> Enum.sort() end

      IO.puts("""

      burst: #{rate} req/s open loop, writer stalled #{stall_ms} ms, #{fsync} ms fsync, #{byte_size(body)} B bodies
        arrived during the stall (#{length(during)}):  p99 #{fmt(pct(lat.(during), 0.99))} ms (waited out the stall)
        arrived after it        (#{length(after_stall)}): p50 #{fmt(pct(lat.(after_stall), 0.50))} ms  p99 #{fmt(pct(lat.(after_stall), 0.99))} ms  max #{fmt(pct(lat.(after_stall), 1.0))} ms
      """)

      %{p99_ms: pct(lat.(after_stall), 0.99)}
    end)
  end

  defp collect(0, acc), do: acc

  defp collect(n, acc) do
    receive do
      {:acked, arrived, ms} -> collect(n - 1, [{arrived, ms} | acc])
    after
      60_000 -> raise "burst: #{n} requests never acked"
    end
  end

  # ── plumbing ──────────────────────────────────────────────────────────────

  # A fresh instance (its own store) with every WAL sync slowed by `fsync` ms, and
  # the groups applied, the syncs and the time spent applying groups counted
  # (`:counters` 1, 2, 3: groups, syncs, microseconds of apply).
  defp with_instance(batcher, fsync, fun) do
    name = :"store_commit_#{System.unique_integer([:positive])}"

    dir =
      Path.join(
        System.tmp_dir!(),
        "ankusa_store_commit_#{System.system_time(:microsecond)}_#{:erlang.phash2(name)}"
      )

    File.mkdir_p!(dir)

    config =
      Ankusa.Config.new(
        instance: name,
        data_dir: dir,
        port: 0,
        roles: [:edge],
        batcher: Map.merge(%{partitions: @partitions, max_queue: 100_000}, batcher),
        source_store:
          {Ankusa.SourceStore.Static, sources: %{"bench" => [sinks: [{Ankusa.Sink.Log, []}]]}}
      )

    {:ok, sup} = Ankusa.Instance.start_link(config)
    commits = :counters.new(3, [])
    handler = "store-commit-#{name}"

    :telemetry.attach_many(
      handler,
      [[:ankusa, :wal_sync, :start], [:ankusa, :commit, :stop]],
      fn
        [:ankusa, :wal_sync, :start], _measurements, %{instance: ^name}, _cfg ->
          :counters.add(commits, 2, 1)
          if fsync > 0, do: Process.sleep(fsync)

        [:ankusa, :commit, :stop], %{duration: duration}, %{instance: ^name}, _cfg ->
          :counters.add(commits, 1, 1)
          :counters.add(commits, 3, System.convert_time_unit(duration, :native, :microsecond))

        _event, _measurements, _metadata, _cfg ->
          :ok
      end,
      nil
    )

    try do
      fun.(name, commits)
    after
      :telemetry.detach(handler)
      Supervisor.stop(sup)
      File.rm_rf(dir)
    end
  end

  defp entry(body) do
    env = %Envelope{
      id: Ankusa.UUIDv7.generate(),
      source_id: "bench",
      tenant_id: "default",
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/webhooks/bench",
      headers: [],
      body: body,
      size: byte_size(body)
    }

    %{envelope: env, sinks: [{Ankusa.Sink.Log, []}]}
  end

  defp print(fsync, mode, r) do
    IO.puts(
      "#{String.pad_trailing("#{fsync} ms", 7)} #{String.pad_trailing("#{mode}", 10)} " <>
        "#{String.pad_leading(fmt(r.per_s, 0), 9)}  #{String.pad_leading(fmt(r.commits_per_s, 0), 8)}  " <>
        "#{String.pad_leading(fmt(r.syncs_per_s, 0), 8)}  #{String.pad_leading(fmt(r.mean_group), 10)}  " <>
        "#{String.pad_leading(fmt(r.groups_per_sync), 12)}  #{String.pad_leading(fmt(r.apply_ms), 8)}  " <>
        "#{String.pad_leading(fmt(r.p50_ms), 7)}  #{String.pad_leading(fmt(r.p99_ms), 7)}"
    )
  end

  # The `p`-quantile (0..1) of `sorted`, in whatever unit it holds (ms here).
  defp pct([], _p), do: 0.0

  defp pct(sorted, p) do
    rank = trunc(Float.ceil(p * length(sorted))) - 1
    Enum.at(sorted, rank |> min(length(sorted) - 1) |> max(0))
  end

  defp fmt(x, digits \\ 2), do: :erlang.float_to_binary(x / 1, decimals: digits)
  defp int(name, default), do: String.to_integer(System.get_env(name, "#{default}"))
end

Bench.StoreCommit.run(verbose: System.get_env("VERBOSE") == "1")
