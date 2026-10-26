Mix.ensure_application!(:tools)
Mix.ensure_application!(:runtime_tools)
# Throwaway: process-level CPU profile of the 10 KB cell (not committed).
defmodule Bench.Sink do
  @behaviour Ankusa.Sink
  @impl true
  def deliver(env, _ctx, opts) do
    Process.sleep(Keyword.fetch!(opts, :latency_ms))
    :ets.insert(:bench_delivered, {env.id})
    :ok
  end
end

defmodule Prof do
  def label(pid) do
    case Process.info(pid, [:registered_name, :dictionary, :initial_call]) do
      nil ->
        "dead"

      info ->
        if Keyword.get(info[:dictionary] || [], :bench_client), do: throw(:client)
        name = info[:registered_name]
        dict = info[:dictionary] || []
        ic = Keyword.get(dict, :"$initial_call") || info[:initial_call]
        lbl = Keyword.get(dict, :"$process_label")

        cond do
          lbl != nil ->
            inspect(lbl)

          name != nil and name != [] ->
            inspect(name)

          true ->
            case Registry.keys(Ankusa.Registry, pid) do
              [k | _] -> inspect(k)
              [] -> inspect(ic)
            end
        end
    end
  end

  def snapshot do
    for pid <- Process.list(), into: %{} do
      case Process.info(pid, :reductions) do
        {:reductions, r} -> {pid, r}
        nil -> {pid, 0}
      end
    end
  end

  def report(before, after_) do
    Enum.reduce(after_, %{}, fn {pid, r}, acc ->
      d = r - Map.get(before, pid, 0)

      if d > 0,
        do:
          Map.update(
            acc,
            try do
              label(pid)
            catch
              :client -> "clients"
            end,
            d,
            &(&1 + d)
          ),
        else: acc
    end)
    |> Enum.sort_by(fn {_k, v} -> -v end)
    |> Enum.take(30)
  end
end

Logger.configure(level: :warning)
n = String.to_integer(System.get_env("N", "20000"))
concurrency = String.to_integer(System.get_env("CONCURRENCY", "256"))
latency_ms = 5
body_bytes = String.to_integer(System.get_env("BODY_BYTES", "10240"))
eprof? = System.get_env("EPROF") in ["1", "true"]

:ets.new(:bench_delivered, [:set, :public, :named_table])
dir = Path.join(System.tmp_dir!(), "ankusa_prof_#{System.unique_integer([:positive])}")
File.mkdir_p!(dir)
System.at_exit(fn _ -> File.rm_rf(dir) end)

config =
  Ankusa.Config.new(
    instance: :bench,
    data_dir: dir,
    port: 0,
    roles: [:edge, :dispatch, :storage],
    batcher: %{group_commit_delay_ms: String.to_integer(System.get_env("GC_DELAY", "1"))},
    source_store:
      {Ankusa.SourceStore.Static,
       sources: %{
         "bench" => [
           verifier: {Ankusa.Verifier.None, []},
           on_verify_failure: :accept_flag,
           sinks: [{Bench.Sink, [latency_ms: latency_ms]}]
         ]
       }}
  )

{:ok, instance_sup} = Ankusa.Instance.start_link(config)
:ets.new(:prof_counts, [:set, :public, :named_table])

:telemetry.attach_many(
  "prof",
  [[:ankusa, :commit, :stop], [:ankusa, :wal_sync, :stop]],
  fn ev, meas, _meta, _ ->
    case ev do
      [:ankusa, :commit, :stop] ->
        :ets.update_counter(:prof_counts, :commits, {2, 1}, {:commits, 0})

        :ets.update_counter(
          :prof_counts,
          :commit_hooks,
          {2, meas[:batch_size] || 0},
          {:commit_hooks, 0}
        )

      [:ankusa, :wal_sync, :stop] ->
        :ets.update_counter(:prof_counts, :syncs, {2, 1}, {:syncs, 0})
    end
  end,
  nil
)

pad = String.duplicate("x", body_bytes)

requests =
  for i <- 1..n,
      do: %{
        source_id: "bench",
        method: "POST",
        path: "/webhooks/bench",
        headers: [],
        body: Jason.encode!(%{"id" => "bench-#{i}", "pad" => pad})
      }

:persistent_term.put(:reqs, List.to_tuple(requests))
counter = :atomics.new(1, signed: false)
parent = self()

clients =
  for _ <- 1..concurrency do
    spawn_link(fn ->
      Process.put(:bench_client, true)
      reqs = :persistent_term.get(:reqs)
      receive do: (:go -> :ok)

      loop = fn loop, acked ->
        case :atomics.add_get(counter, 1, 1) do
          i when i > n ->
            acked

          i ->
            case Ankusa.Edge.Ingest.ingest(:bench, elem(reqs, i - 1)) do
              {:ok, _} -> loop.(loop, acked + 1)
              _ -> loop.(loop, acked)
            end
        end
      end

      acked = loop.(loop, 0)
      send(parent, {:done, self(), System.monotonic_time(:microsecond), acked})
      receive do: (:stop -> :ok)
    end)
  end

writer = Ankusa.whereis(:bench, :queue_writer)

eprof_pids =
  case System.get_env("EPROF_TARGET", "writer") do
    "all" ->
      Process.list() -- [Process.whereis(:eprof)]

    "writer" ->
      [writer]

    "syncer" ->
      [
        Ankusa.whereis(:bench, :queue_writer)
        |> Process.info(:links)
        |> elem(1)
        |> Enum.filter(&is_pid/1)
        |> Enum.find(fn p ->
          match?({:dictionary, d} when is_list(d), Process.info(p, :dictionary)) and
            Keyword.get(elem(Process.info(p, :dictionary), 1), :"$initial_call") ==
              {Ankusa.Queue.Syncer, :init, 1}
        end)
      ]

    "client" ->
      clients

    "dispatch" ->
      [Ankusa.whereis(:bench, :dispatch)]

    "compactor" ->
      [Ankusa.whereis(:bench, :compactor)]
  end
  |> Enum.reject(&is_nil/1)

if eprof?, do: IO.inspect(:eprof.start_profiling(eprof_pids), label: "eprof")
if eprof?, do: :eprof.start()

:erlang.system_flag(:scheduler_wall_time, true)
os_pid = System.pid()

ps_cpu = fn ->
  case File.read("/proc/self/stat") do
    {:ok, stat} ->
      # fields 14 and 15 (utime, stime) in clock ticks, after the ")" closing the command name
      [_, rest] = String.split(stat, ") ", parts: 2)
      fields = String.split(rest)
      (String.to_integer(Enum.at(fields, 11)) + String.to_integer(Enum.at(fields, 12))) / 100

    _ ->
      {out, 0} = System.cmd("ps", ["-o", "cputime=", "-p", os_pid])

      [m, sec] =
        out
        |> String.trim()
        |> String.split(":")
        |> Enum.map(&String.to_float(&1 <> if(String.contains?(&1, "."), do: "", else: ".0")))

      m * 60 + sec
  end
end

ps0 = ps_cpu.()
swt0 = :erlang.statistics(:scheduler_wall_time_all)
before = Prof.snapshot()
{_, rt0} = :erlang.statistics(:runtime)
msacc? = Code.ensure_loaded?(:msacc)
if msacc?, do: :msacc.start()
t0 = System.monotonic_time(:microsecond)
Enum.each(clients, &send(&1, :go))
done = Enum.map(clients, fn pid -> receive do: ({:done, ^pid, t, a} -> {t, a}) end)
if msacc?, do: :msacc.stop()
{_, rt1} = :erlang.statistics(:runtime)
after_ = Prof.snapshot()
swt1 = :erlang.statistics(:scheduler_wall_time_all)
ps1 = ps_cpu.()
busy = Enum.zip(swt0, swt1) |> Enum.map(fn {{_, a0, _}, {_, a1, _}} -> a1 - a0 end) |> Enum.sum()
IO.puts("scheduler_busy_ms=#{div(busy, 1_000_000)} ps_cpu_s=#{Float.round(ps1 - ps0, 2)}")
Enum.each(clients, &send(&1, :stop))

if eprof?,
  do:
    (
      :eprof.stop_profiling()
      :eprof.analyze(:total, sort: :time)
    )

t1 = done |> Enum.map(&elem(&1, 0)) |> Enum.max()
acked = done |> Enum.map(&elem(&1, 1)) |> Enum.sum()
secs = (t1 - t0) / 1_000_000

IO.puts(
  "acked=#{acked} secs=#{Float.round(secs, 2)} ingest/s=#{round(acked / secs)} sched_busy_us_per_hook=#{div(div(busy, 1000), acked)} ps_cpu_us_per_hook=#{round((ps1 - ps0) * 1_000_000 / acked)}"
)

IO.puts(
  "commits=#{:ets.lookup_element(:prof_counts, :commits, 2)} hooks_per_commit=#{div(:ets.lookup_element(:prof_counts, :commit_hooks, 2), max(:ets.lookup_element(:prof_counts, :commits, 2), 1))} syncs=#{:ets.lookup_element(:prof_counts, :syncs, 2)}"
)

IO.puts("\n-- reductions by process (delta, top 30) --")

total =
  after_
  |> Enum.map(fn {pid, r} -> r - Map.get(before, pid, 0) end)
  |> Enum.filter(&(&1 > 0))
  |> Enum.sum()

for {k, v} <- Prof.report(before, after_),
    do:
      IO.puts(
        String.pad_leading(Integer.to_string(v), 12) <>
          "  " <> String.pad_leading("#{Float.round(100 * v / total, 1)}%", 7) <> "  " <> k
      )

IO.puts("total=#{total} per_hook=#{div(total, max(acked, 1))}")
IO.puts("\n-- msacc --")
if msacc?, do: :msacc.print()

if System.get_env("COMPACT") == "1" do
  comp = Ankusa.whereis(:bench, :compactor)

  if System.get_env("EPROF_COMPACT") == "1",
    do:
      (
        :eprof.start()
        :eprof.start_profiling([comp])
      )

  {:reductions, r0} = Process.info(comp, :reductions)
  c0 = ps_cpu.()
  t0 = System.monotonic_time(:microsecond)

  segs =
    Stream.repeatedly(fn ->
      {:ok, n} = Ankusa.Storage.Compactor.tick(:bench)
      n
    end)
    |> Enum.take_while(&(&1 > 0))
    |> Enum.sum()

  t1 = System.monotonic_time(:microsecond)
  {:reductions, r1} = Process.info(comp, :reductions)

  if System.get_env("EPROF_COMPACT") == "1",
    do:
      (
        :eprof.stop_profiling()
        :eprof.analyze(:total, sort: :time)
      )

  IO.puts(
    "compactor: segments=#{segs} wall_ms=#{div(t1 - t0, 1000)} cpu_us_per_hook=#{round((ps_cpu.() - c0) * 1_000_000 / acked)} reductions_per_hook=#{div(r1 - r0, acked)}"
  )
end

deadline = System.monotonic_time(:millisecond) + 600_000

wait = fn wait ->
  if :ets.info(:bench_delivered, :size) >= acked or System.monotonic_time(:millisecond) > deadline,
    do: :ok,
    else:
      (
        Process.sleep(20)
        wait.(wait)
      )
end

wait.(wait)
Supervisor.stop(instance_sup)
