# In-process end-to-end bench for Ankusa core: N ingests through the edge, then
# wait for dispatch to hand every acked hook to a sink that sleeps a fixed
# amount. Run it with:
#
#     MIX_ENV=test mix run bench/core_bench.exs
#
# MIX_ENV=test because `config/config.exs` autostarts the default instance
# (port 4000) outside `:test`.
#
# Env knobs: N (20000), CONCURRENCY (256), SINK_LATENCY_MS (5), BODY_BYTES (512),
# ORDERED (0; 1 makes the sink an ordered lane sink over 64 keys),
# STUB_INGEST (0; 1 answers every request at once instead of calling
# `Ankusa.Edge.Ingest.ingest/2`: what the harness itself can drive, not a
# measurement of Ankusa; the drain is skipped).
#
# Harness: every request (body included) is built once, before the clock
# starts, and CONCURRENCY long-lived client processes claim the next one off a
# shared counter. The timed section is the `ingest/2` call and an atomic
# counter bump, so at 10 KB the harness does not cap what the code under test
# can show (a body build and a `Task` per item cost ~60 µs of CPU each).
#
# Prints one JSON line (`ingest_per_s`, `p50_ms`, `p99_ms`, `end_to_end_per_s`,
# ... — `p50_ms`/`p99_ms` are the ingest latency percentiles; `mise run
# bench:gate` reads them) and a table. Exits 1 if any acked hook never reached
# the sink, or if draining times out.

defmodule Bench.Sink do
  @behaviour Ankusa.Sink

  @impl true
  def deliver(env, _ctx, opts) do
    Process.sleep(Keyword.fetch!(opts, :latency_ms))
    :ets.insert(:bench_delivered, {env.id})
    :ok
  end
end

defmodule Bench.Client do
  @moduledoc false
  # One long-lived client of the closed loop: waits for `:go`, then claims
  # request indexes off `counter` until `n` is spent and calls ingest with each,
  # timing only that call. Requests are the prebuilt tuple in `:persistent_term`
  # (a read is O(1) and copies nothing).

  def start(parent, counter, n, stub?) do
    spawn_link(fn ->
      requests = :persistent_term.get({__MODULE__, :requests})

      receive do
        :go -> :ok
      end

      {latencies, acked, ids} = loop(requests, counter, n, stub?, [], 0, [])
      send(parent, {:done, self(), System.monotonic_time(:microsecond), latencies, acked, ids})
    end)
  end

  defp loop(requests, counter, n, stub?, latencies, acked, ids) do
    case :atomics.add_get(counter, 1, 1) do
      i when i > n ->
        {latencies, acked, ids}

      i ->
        request = elem(requests, i - 1)
        t0 = System.monotonic_time(:microsecond)

        result =
          if stub?, do: {:ok, request}, else: Ankusa.Edge.Ingest.ingest(:bench, request)

        latency_us = System.monotonic_time(:microsecond) - t0

        case result do
          {:ok, _env} when stub? ->
            loop(requests, counter, n, stub?, [latency_us | latencies], acked + 1, ids)

          {:ok, env} ->
            loop(requests, counter, n, stub?, [latency_us | latencies], acked + 1, [env.id | ids])

          _other ->
            loop(requests, counter, n, stub?, [latency_us | latencies], acked, ids)
        end
    end
  end
end

Logger.configure(level: :warning)

n = String.to_integer(System.get_env("N", "20000"))
concurrency = String.to_integer(System.get_env("CONCURRENCY", "256"))
latency_ms = String.to_integer(System.get_env("SINK_LATENCY_MS", "5"))
body_bytes = String.to_integer(System.get_env("BODY_BYTES", "512"))
# ORDERED=1: the source's sink is `ordered: true` (ADR 0004), keyed by an
# `x-entity` header that cycles through 64 keys, so 64 lanes run concurrently.
ordered? = System.get_env("ORDERED") in ["1", "true"]
lane_keys = 64
stub? = System.get_env("STUB_INGEST") in ["1", "true"]

:ets.new(:bench_delivered, [:set, :public, :named_table])

# `:erlang.unique_integer/1` is only unique *within* a VM — two `mix run`s
# start from the same sequence and can pick the same name, which would make
# this bench replay (and redeliver) another run's WAL. Time plus a counter
# plus a random suffix, and clean up after.
dir =
  Path.join(
    System.tmp_dir!(),
    "ankusa_bench_#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}_" <>
      Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
  )

File.mkdir_p!(dir)
System.at_exit(fn _ -> File.rm_rf(dir) end)

config =
  Ankusa.Config.new(
    instance: :bench,
    data_dir: dir,
    port: 0,
    roles: [:edge, :dispatch, :storage],
    source_store:
      {Ankusa.SourceStore.Static,
       sources: %{
         "bench" => [
           verifier: {Ankusa.Verifier.None, []},
           on_verify_failure: :accept_flag,
           sinks: [
             {Bench.Sink,
              [latency_ms: latency_ms] ++
                if(ordered?, do: [ordered: true, ordering_key: {:header, "x-entity"}], else: [])}
           ]
         ]
       }}
  )

{:ok, instance_sup} = Ankusa.Instance.start_link(config)

# ── requests, built before the clock ────────────────────────────────────────

pad = String.duplicate("x", body_bytes)

requests =
  for i <- 1..n do
    %{
      source_id: "bench",
      method: "POST",
      path: "/webhooks/bench",
      headers: if(ordered?, do: [{"x-entity", "k#{rem(i, lane_keys)}"}], else: []),
      body:
        Jason.encode!(%{
          "id" => "bench-#{i}-#{:erlang.unique_integer([:positive])}",
          "pad" => pad
        })
    }
  end

:persistent_term.put({Bench.Client, :requests}, List.to_tuple(requests))
:erlang.garbage_collect()

# ── ingest ──────────────────────────────────────────────────────────────────

counter = :atomics.new(1, signed: false)
clients = for _ <- 1..concurrency, do: Bench.Client.start(self(), counter, n, stub?)

ingest_start_us = System.monotonic_time(:microsecond)
Enum.each(clients, &send(&1, :go))

done =
  Enum.map(clients, fn pid ->
    receive do
      {:done, ^pid, end_us, latencies, acked, ids} -> {end_us, latencies, acked, ids}
    end
  end)

ingest_end_us = done |> Enum.map(&elem(&1, 0)) |> Enum.max()
latencies_us = Enum.flat_map(done, &elem(&1, 1))
acked = done |> Enum.map(&elem(&1, 2)) |> Enum.sum()
acked_ids = Enum.flat_map(done, &elem(&1, 3))
ingest_s = (ingest_end_us - ingest_start_us) / 1_000_000

# ── drain ───────────────────────────────────────────────────────────────────

deadline_us = System.monotonic_time(:microsecond) + 600_000_000

wait_for_drain = fn wait_for_drain ->
  now_us = System.monotonic_time(:microsecond)

  cond do
    :ets.info(:bench_delivered, :size) >= acked -> {:ok, now_us}
    now_us > deadline_us -> :timeout
    true -> Process.sleep(10) && wait_for_drain.(wait_for_drain)
  end
end

# A stubbed run delivered nothing, so there is nothing to wait for.
drain_result = if stub?, do: {:ok, ingest_end_us}, else: wait_for_drain.(wait_for_drain)

drain_end_us =
  if drain_result == :timeout,
    do: System.monotonic_time(:microsecond),
    else: elem(drain_result, 1)

sorted = Enum.sort(latencies_us)

percentile = fn p ->
  count = length(sorted)
  idx = max(0, min(count - 1, ceil(p * count) - 1))
  Enum.at(sorted, idx) / 1000
end

missing = Enum.count(acked_ids, fn id -> not :ets.member(:bench_delivered, id) end)

report = %{
  acked: acked,
  ingest_per_s: if(ingest_s > 0, do: Float.round(acked / ingest_s, 2), else: 0.0),
  p50_ms: percentile.(0.50),
  p99_ms: percentile.(0.99),
  ingest_p50_ms: percentile.(0.50),
  ingest_p95_ms: percentile.(0.95),
  ingest_p99_ms: percentile.(0.99),
  ingest_max_ms: percentile.(1.00),
  drain_s: Float.round((drain_end_us - ingest_end_us) / 1_000_000, 3),
  end_to_end_per_s:
    Float.round(acked / max((drain_end_us - ingest_start_us) / 1_000_000, 0.001), 2),
  missing: missing
}

IO.puts(Jason.encode!(report))

IO.puts("""

core bench
----------
acked             #{report.acked}
ingest_per_s      #{report.ingest_per_s}
ingest p50 ms     #{report.ingest_p50_ms}
ingest p95 ms     #{report.ingest_p95_ms}
ingest p99 ms     #{report.ingest_p99_ms}
ingest max ms     #{report.ingest_max_ms}
drain_s           #{report.drain_s}
end_to_end_per_s  #{report.end_to_end_per_s}
missing           #{report.missing}
""")

# Close the store before the VM goes: one that exits (or halts) with a database
# open can die inside the native teardown, and that crash would replace this
# script's own exit status.
Supervisor.stop(instance_sup)

if drain_result == :timeout do
  IO.write(:stderr, "core_bench: timed out waiting for delivery (#{acked} acked)\n")
  System.halt(1)
end

if missing > 0 do
  IO.write(:stderr, "core_bench: #{missing} acked hook(s) never reached the sink\n")
  System.halt(1)
end
