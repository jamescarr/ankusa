# A sink outage at the default 24 h retry horizon: do the queue gauges and the
# dispatch scan stay flat with the backlog, and what does a wave of
# horizon-expired rows do to `archive_pending`? (DEL-2, Evidence.)
#
#     MIX_ENV=test mix run bench/outage_bench.exs
#     BACKLOG=1000000 DEAD=100000 MIX_ENV=test mix run bench/outage_bench.exs
#
# Two runs against a real `Ankusa.Instance` whose only sink always fails, with
# hooks stored through `Ankusa.Queue.enqueue/2` (the ack path's own write):
#
#   steady   BACKLOG hooks, the default policy (24 h horizon, breakers on). The
#            store is filled with no dispatch role so the backlog is exact, then
#            dispatch starts and fails every row once (the first wave: every row
#            is rewritten as a retry or parked behind the breaker). Then the
#            gauge tick (`Ankusa.Metrics.Gauges.measure/1`) and the dispatch
#            scan (`Pipeline.tick/1` with nothing due) are timed with the
#            whole backlog sitting in the store, and the gauge tick was also
#            timed at 1 %, 10 %, 50 % and 100 % of the fill.
#   expiry   DEAD hooks, `max_elapsed_ms: 2000` (the shape of the 24 h
#            horizon passing for a whole backlog at once, compressed: every row
#            is dead-lettered by its first failure past the horizon), a
#            storage role so the compactor archives what ended. Samples the
#            gauge tick, `archive_pending`, `dead` and `scheduled` once a second
#            through the wave, then drives `Compactor.tick/1` until the archive
#            is drained.
#
# Prints one JSON line per measurement. Environment:
#
#   BACKLOG    hooks of the `steady` run (default 100_000)
#   DEAD       hooks of the `expiry` run (default 30_000)
#   BODY       body bytes (default 256)
#   BENCH_DIR  where the stores live (default the system temp dir)

Logger.configure(level: :warning)

defmodule Bench.Outage do
  alias Ankusa.{Config, Envelope}
  alias Ankusa.Dispatch.Pipeline
  alias Ankusa.Metrics.Gauges
  alias Ankusa.Storage.Compactor

  defmodule Down do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: {:error, :sink_down}
  end

  def env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> value |> String.replace("_", "") |> String.to_integer()
    end
  end

  def emit(map), do: IO.puts(JSON.encode!(map))

  defp start(inst, dir, roles, dispatch) do
    config =
      Config.new(
        instance: inst,
        data_dir: dir,
        port: 0,
        roles: roles,
        dispatch: dispatch,
        storage: %{interval_ms: 0, roll_ms: 0},
        source_store: {Ankusa.SourceStore.Static, sources: %{"outage" => [sinks: [{Down, []}]]}}
      )

    {:ok, sup} = Supervisor.start_link([{Ankusa.Instance, config}], strategy: :one_for_one)
    sup
  end

  # Timed `Gauges.measure/1`, and the `[:ankusa, :queue, :state]` it emitted.
  defp gauge(inst) do
    {us, :ok} = :timer.tc(fn -> Gauges.measure(inst) end)
    {us, Process.get(:queue_state, %{})}
  end

  defp attach(inst) do
    id = "outage-bench-#{inst}"

    :telemetry.attach(
      id,
      [:ankusa, :queue, :state],
      fn
        _event, measurements, %{instance: i}, inst when i == inst ->
          Process.put(:queue_state, measurements)

        _event, _measurements, _meta, _inst ->
          :ok
      end,
      inst
    )
  end

  defp median(values), do: values |> Enum.sort() |> Enum.at(div(length(values), 2))

  defp envelope(i, body) do
    %Envelope{
      id: "evt_#{i}",
      source_id: "outage",
      tenant_id: "t#{rem(i, 5)}",
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/hooks",
      headers: [],
      content_type: "application/json",
      body: body,
      size: byte_size(body)
    }
  end

  defp fill(inst, n, body, marks) do
    sinks = [{Down, []}]
    started = System.monotonic_time(:millisecond)

    {samples, _} =
      1..n
      |> Enum.chunk_every(500)
      |> Enum.reduce({[], 0}, fn chunk, {samples, done} ->
        entries = for i <- chunk, do: %{envelope: envelope(i, body), sinks: sinks}
        {:ok, _} = Ankusa.Queue.enqueue(inst, entries)
        done = done + length(chunk)

        if Enum.any?(marks, &(done >= &1 and done - length(chunk) < &1)) do
          {us, state} = gauge(inst)
          {[%{rows: done, gauge_us: us, pending: state[:pending]} | samples], done}
        else
          {samples, done}
        end
      end)

    {Enum.reverse(samples), System.monotonic_time(:millisecond) - started}
  end

  # `Pipeline.tick/1` answers within 30 s; a wave over a big backlog takes
  # longer, so ask again until nothing is claimed, running or due.
  defp drain(inst, tries \\ 600) do
    Pipeline.tick(inst)
  catch
    :exit, _ when tries > 0 -> drain(inst, tries - 1)
  end

  def steady(n, body, root) do
    inst = :"outage_steady_#{System.unique_integer([:positive])}"
    dir = Path.join(root, "ankusa_outage_steady_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    attach(inst)

    sup = start(inst, dir, [:edge, :storage], %{})
    marks = for pct <- [1, 10, 50, 100], do: div(n * pct, 100)
    {fill_samples, fill_ms} = fill(inst, n, body, marks)
    Supervisor.stop(sup)

    emit(%{
      run: "steady",
      phase: "fill",
      backlog: n,
      fill_ms: fill_ms,
      gauge_by_backlog: fill_samples
    })

    sup = start(inst, dir, [:edge, :dispatch, :storage], %{})
    {wave_us, {:ok, settled}} = :timer.tc(fn -> drain(inst) end)

    {_, state} = gauge(inst)
    gauges = for _ <- 1..9, do: elem(gauge(inst), 0)
    ticks = for _ <- 1..9, do: elem(:timer.tc(fn -> Pipeline.tick(inst) end), 0)

    emit(%{
      run: "steady",
      phase: "outage",
      backlog: n,
      first_wave_ms: div(wave_us, 1000),
      settled_in_wave: settled,
      queue_state: state,
      gauge_tick_us_median: median(gauges),
      gauge_tick_us_max: Enum.max(gauges),
      scan_tick_us_median: median(ticks),
      scan_tick_us_max: Enum.max(ticks)
    })

    Supervisor.stop(sup)
    File.rm_rf(dir)
  end

  def expiry(n, body, root) do
    inst = :"outage_expiry_#{System.unique_integer([:positive])}"
    dir = Path.join(root, "ankusa_outage_expiry_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    attach(inst)

    sup = start(inst, dir, [:edge, :storage], %{})
    {_samples, fill_ms} = fill(inst, n, body, [])
    Supervisor.stop(sup)

    policy =
      {Ankusa.RetryPolicy.Exponential,
       base_ms: 500, max_ms: 500, jitter: false, max_elapsed_ms: 2_000}

    sup = start(inst, dir, [:edge, :dispatch, :storage], %{breaker_failures: 0, retry: policy})
    started = System.monotonic_time(:millisecond)

    samples =
      Stream.repeatedly(fn ->
        Process.sleep(1_000)
        {us, state} = gauge(inst)

        %{
          t_s: div(System.monotonic_time(:millisecond) - started, 1000),
          gauge_us: us,
          dead: state[:dead],
          scheduled: state[:scheduled],
          pending: state[:pending],
          archive_pending: state[:archive_pending]
        }
      end)
      |> Enum.reduce_while([], fn sample, acc ->
        acc = [sample | acc]

        if sample.dead >= n or sample.t_s > 900,
          do: {:halt, Enum.reverse(acc)},
          else: {:cont, acc}
      end)

    wave_s = List.last(samples).t_s

    compaction =
      Stream.repeatedly(fn ->
        {us, {:ok, count}} = :timer.tc(fn -> Compactor.tick(inst) end)
        {_, state} = gauge(inst)
        %{tick_ms: div(us, 1000), archived: count, archive_pending: state[:archive_pending]}
      end)
      |> Enum.reduce_while([], fn tick, acc ->
        acc = [tick | acc]

        if tick.archive_pending == 0 or length(acc) >= 200,
          do: {:halt, Enum.reverse(acc)},
          else: {:cont, acc}
      end)

    emit(%{
      run: "expiry",
      backlog: n,
      fill_ms: fill_ms,
      horizon_wave_s: wave_s,
      samples: samples,
      max_archive_pending: samples |> Enum.map(&(&1.archive_pending || 0)) |> Enum.max(),
      gauge_us_max: samples |> Enum.map(& &1.gauge_us) |> Enum.max(),
      compaction_ticks: length(compaction),
      compaction_ms: compaction |> Enum.map(& &1.tick_ms) |> Enum.sum(),
      archive_pending_after: List.last(compaction).archive_pending
    })

    Supervisor.stop(sup)
    File.rm_rf(dir)
  end
end

root = System.get_env("BENCH_DIR") || System.tmp_dir!()
body = String.duplicate("x", Bench.Outage.env_int("BODY", 256))

Bench.Outage.steady(Bench.Outage.env_int("BACKLOG", 100_000), body, root)
Bench.Outage.expiry(Bench.Outage.env_int("DEAD", 30_000), body, root)
