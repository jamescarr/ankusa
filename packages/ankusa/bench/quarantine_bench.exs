# The quarantine pen under a forged flood (DUR-9, ground rule 4's check):
#
#     MIX_ENV=test mix run bench/quarantine_bench.exs
#
# One edge node with the default `quarantine` config (per-source bucket 100 /
# 20 per s, 1 GiB cap), 500 sources whose signature check always fails and whose
# policy is `:quarantine`, and one clean source with no verifier. Two phases,
# each paced open-loop (hooks start on a 10 ms clock, whatever the node answers,
# so latency is not hidden by a closed loop slowing itself down):
#
#   1. the clean source alone at `CLEAN_RATE` hooks/s for `SECONDS`: its p99 is
#      the baseline;
#   2. `RATE` forged hooks/s over the 500 sources for `SECONDS`, while the clean
#      source keeps the same rate.
#
# Prints one JSON line and a table, and exits 1 unless
#
#   * every forged hook was quarantined (`202`-worthy) at >= 95 % of `RATE`,
#   * the forged hooks' p99 ack latency is under 50 ms (DUR-9's *Done when*),
#   * the clean source's p99 during the flood is within 2x of its solo p99 (or
#     under 25 ms: at that scale a scheduler hiccup is the whole difference), and
#   * the pen counted exactly the bytes its entries hold.
#
# Env knobs: RATE (5000; a multiple of 100), SECONDS (10), CLEAN_RATE (500; a
# multiple of 100), SOURCES (500), BODY_BYTES (1024).

Logger.configure(level: :warning)

defmodule Bench.Quarantine do
  @tick_ms 10

  def env_int(name, default), do: String.to_integer(System.get_env(name, "#{default}"))

  @doc """
  Starts `rate` calls of `fun.(n)` a second for `seconds`, on a 10 ms clock, in
  a process of its own. Returns a task whose result is the `{latency_us, result}`
  of every call, once all have answered.
  """
  def paced(rate, seconds, fun) do
    Task.async(fn ->
      per_tick = div(rate, div(1000, @tick_ms))
      ticks = seconds * div(1000, @tick_ms)
      table = :ets.new(:latencies, [:duplicate_bag, :public, write_concurrency: true])
      t0 = System.monotonic_time(:millisecond)

      for tick <- 0..(ticks - 1), i <- 1..per_tick do
        if i == 1 do
          wait = t0 + tick * @tick_ms - System.monotonic_time(:millisecond)
          if wait > 0, do: Process.sleep(wait)
        end

        n = tick * per_tick + i

        spawn(fn ->
          started = System.monotonic_time(:microsecond)
          result = fun.(n)
          :ets.insert(table, {System.monotonic_time(:microsecond) - started, result})
        end)
      end

      await(table, ticks * per_tick, System.monotonic_time(:second) + 120)
      results = :ets.tab2list(table)
      :ets.delete(table)
      {System.monotonic_time(:millisecond) - t0, results}
    end)
  end

  defp await(table, expected, deadline) do
    cond do
      :ets.info(table, :size) >= expected -> :ok
      System.monotonic_time(:second) > deadline -> raise "calls never answered"
      true -> Process.sleep(20) && await(table, expected, deadline)
    end
  end

  def percentile_ms([], _p), do: 0.0

  def percentile_ms(latencies_us, p) do
    sorted = latencies_us |> Enum.sort() |> List.to_tuple()
    idx = max(0, min(tuple_size(sorted) - 1, ceil(p * tuple_size(sorted)) - 1))
    Float.round(elem(sorted, idx) / 1000, 3)
  end
end

alias Bench.Quarantine, as: B

rate = B.env_int("RATE", 5_000)
seconds = B.env_int("SECONDS", 10)
clean_rate = B.env_int("CLEAN_RATE", 500)
sources = B.env_int("SOURCES", 500)
body_bytes = B.env_int("BODY_BYTES", 1024)

secret = "whsec_" <> Base.encode64("supersecret-key")

quarantining =
  Map.new(1..sources, fn i ->
    {"q-#{i}",
     [
       verifier: {Ankusa.Verifier.Hmac, scheme: :standard_webhooks, secret: secret},
       on_verify_failure: :quarantine,
       sinks: [{Ankusa.Sink.Log, []}]
     ]}
  end)

dir =
  Path.join(
    System.tmp_dir!(),
    "ankusa_qbench_#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}_" <>
      Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
  )

File.mkdir_p!(dir)
System.at_exit(fn _ -> File.rm_rf(dir) end)

config =
  Ankusa.Config.new(
    instance: :qbench,
    data_dir: dir,
    port: 0,
    roles: [:edge],
    source_store:
      {Ankusa.SourceStore.Static,
       sources:
         Map.put(quarantining, "clean",
           verifier: {Ankusa.Verifier.None, []},
           sinks: [{Ankusa.Sink.Log, []}]
         )}
  )

{:ok, instance_sup} = Ankusa.Instance.start_link(config)

pad = String.duplicate("x", body_bytes)

forged = fn n ->
  source = "q-#{rem(n, sources) + 1}"

  Ankusa.Edge.Ingest.ingest(:qbench, %{
    source_id: source,
    method: "POST",
    path: "/webhooks/#{source}",
    headers: [{"webhook-signature", "v1,nope"}],
    body: ~s({"n":#{n},"pad":"#{pad}"})
  })
end

clean = fn n ->
  Ankusa.Edge.Ingest.ingest(:qbench, %{
    source_id: "clean",
    method: "POST",
    path: "/webhooks/clean",
    headers: [],
    body: ~s({"id":"clean-#{n}","pad":"#{pad}"})
  })
end

# Warm the code paths and the store before anything is measured.
for n <- 1..200, do: forged.(n)
for n <- 1..200, do: clean.(n)

# ── phase 1: the clean source alone ─────────────────────────────────────────

{_ms, solo} = B.paced(clean_rate, max(div(seconds, 2), 2), clean) |> Task.await(180_000)
solo_p99 = solo |> Enum.map(&elem(&1, 0)) |> B.percentile_ms(0.99)

# ── phase 2: the flood, with the clean source still running ─────────────────

clean_task = B.paced(clean_rate, seconds, clean)
flood_task = B.paced(rate, seconds, forged)
{flood_ms, flood} = Task.await(flood_task, 180_000)
{_ms, clean_under_flood} = Task.await(clean_task, 180_000)

flood_lat = Enum.map(flood, &elem(&1, 0))
quarantined = Enum.count(flood, &match?({_, {:quarantined, _}}, &1))
refused = length(flood) - quarantined
clean_ok = Enum.count(clean_under_flood, &match?({_, {:ok, _}}, &1))
clean_p99 = clean_under_flood |> Enum.map(&elem(&1, 0)) |> B.percentile_ms(0.99)

# The pen's byte count is the bytes its entries hold, to the byte.
{:ok, %{bytes: held_bytes, entries: entries, pending: pending}} =
  Ankusa.Edge.Quarantine.held(:qbench)

%{lo: lo, hi: hi} = Ankusa.Store.Keys.family(:quarantine)

{:ok, summed} =
  Ankusa.Store.fold(:qbench, :quarantine, {lo, hi}, 0, fn _key, value, acc ->
    {:cont, acc + :erlang.binary_to_term(value).size}
  end)

report = %{
  forged: length(flood),
  quarantined: quarantined,
  refused: refused,
  quarantined_per_s: Float.round(quarantined / (flood_ms / 1000), 1),
  p50_ms: B.percentile_ms(flood_lat, 0.50),
  p99_ms: B.percentile_ms(flood_lat, 0.99),
  max_ms: B.percentile_ms(flood_lat, 1.0),
  clean_solo_p99_ms: solo_p99,
  clean_flood_p99_ms: clean_p99,
  clean_flood_ok: clean_ok,
  pen_entries: entries,
  pen_bytes: held_bytes,
  pen_bytes_summed: summed,
  pen_pending: pending
}

IO.puts(Jason.encode!(report))

IO.puts("""

quarantine bench (#{rate}/s over #{sources} sources, #{seconds} s)
-----------------------------------------------------------
quarantined          #{report.quarantined} of #{report.forged} (#{report.refused} refused)
quarantined_per_s    #{report.quarantined_per_s}
forged p50 ms        #{report.p50_ms}
forged p99 ms        #{report.p99_ms}
forged max ms        #{report.max_ms}
clean p99 ms solo    #{report.clean_solo_p99_ms}
clean p99 ms flood   #{report.clean_flood_p99_ms}
pen entries / bytes  #{report.pen_entries} / #{report.pen_bytes} (summed #{report.pen_bytes_summed})
""")

failures =
  Enum.reject(
    [
      {quarantined == length(flood), "#{refused} forged hook(s) were not quarantined"},
      {report.quarantined_per_s >= rate * 0.95,
       "only #{report.quarantined_per_s}/s quarantined, wanted #{rate}/s"},
      {report.p99_ms < 50.0, "forged p99 #{report.p99_ms} ms is not under 50 ms"},
      {clean_p99 <= max(2 * solo_p99, 25.0),
       "clean p99 #{clean_p99} ms under the flood is over 2x its solo #{solo_p99} ms"},
      {clean_ok == length(clean_under_flood),
       "#{length(clean_under_flood) - clean_ok} clean hook(s) were refused during the flood"},
      {held_bytes == summed, "the pen counts #{held_bytes} bytes, its entries hold #{summed}"},
      {pending == 0, "#{pending} write(s) still pending after the flood"}
    ],
    &elem(&1, 0)
  )

Supervisor.stop(instance_sup)

if failures != [] do
  for {_, message} <- failures, do: IO.write(:stderr, "quarantine_bench: #{message}\n")
  System.halt(1)
end
