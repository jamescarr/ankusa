# In-process end-to-end bench for Ankusa core: N ingests through the edge, then
# wait for dispatch to hand every acked hook to a sink that sleeps a fixed
# amount. Run it with:
#
#     MIX_ENV=test mix run bench/core_bench.exs
#
# MIX_ENV=test because `config/config.exs` autostarts the default instance
# (port 4000) outside `:test`.
#
# Env knobs: N (20000), CONCURRENCY (256), SINK_LATENCY_MS (5), BODY_BYTES (512).
#
# Exits 1 if any acked hook never reached the sink, or if draining times out.

defmodule Bench.Sink do
  @behaviour Ankusa.Sink

  @impl true
  def deliver(env, _ctx, opts) do
    Process.sleep(Keyword.fetch!(opts, :latency_ms))
    :ets.insert(:bench_delivered, {env.id})
    :ok
  end

  # Deliberately no `@impl`: baseline core has no `ordering_key/2` callback, so
  # its absence-of-warning is what lets this one script run on both trees.
  def ordering_key(_env, _opts), do: nil
end

Logger.configure(level: :warning)

n = String.to_integer(System.get_env("N", "20000"))
concurrency = String.to_integer(System.get_env("CONCURRENCY", "256"))
latency_ms = String.to_integer(System.get_env("SINK_LATENCY_MS", "5"))
body_bytes = String.to_integer(System.get_env("BODY_BYTES", "512"))

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
           dedup_key: {Ankusa.DedupKey.Rules, json: ["id"]},
           on_verify_failure: :accept_flag,
           sinks: [{Bench.Sink, latency_ms: latency_ms}]
         ]
       }}
  )

{:ok, _instance} = Ankusa.Instance.start_link(config)

# ── ingest ──────────────────────────────────────────────────────────────────

ingest_start_us = System.monotonic_time(:microsecond)

results =
  1..n
  |> Task.async_stream(
    fn i ->
      body =
        Jason.encode!(%{
          "id" => "bench-#{i}-#{:erlang.unique_integer([:positive])}",
          "pad" => String.duplicate("x", body_bytes)
        })

      t0 = System.monotonic_time(:microsecond)

      result =
        Ankusa.Edge.Ingest.ingest(:bench, %{
          source_id: "bench",
          method: "POST",
          path: "/webhooks/bench",
          headers: [],
          body: body
        })

      {System.monotonic_time(:microsecond) - t0, result}
    end,
    max_concurrency: concurrency,
    ordered: false,
    timeout: :infinity
  )
  |> Enum.map(fn {:ok, value} -> value end)

ingest_end_us = System.monotonic_time(:microsecond)

{latencies_us, acked_ids} =
  Enum.reduce(results, {[], []}, fn {latency_us, result}, {lats, ids} ->
    case result do
      {:ok, env} -> {[latency_us | lats], [env.id | ids]}
      _other -> {[latency_us | lats], ids}
    end
  end)

acked = length(acked_ids)
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

drain_result = wait_for_drain.(wait_for_drain)

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

if drain_result == :timeout do
  IO.write(:stderr, "core_bench: timed out waiting for delivery (#{acked} acked)\n")
  System.halt(1)
end

if missing > 0 do
  IO.write(:stderr, "core_bench: #{missing} acked hook(s) never reached the sink\n")
  System.halt(1)
end
