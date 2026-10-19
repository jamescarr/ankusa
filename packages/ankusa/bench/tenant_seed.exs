# Shared by `bench/gauges_bench.exs` and `bench/dispatch_tick_bench.exs`: a store
# of the tenant key layout (ADR 0001) filled with due rows, their markers and
# counters, without going through ingest, plus the merge load of a busy node
# and the flatness rule both benches enforce.

defmodule Bench.TenantSeed do
  alias Ankusa.Queue.Deliveries
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Queue.Tenants
  alias Ankusa.Store

  @chunk 20_000
  # How many batches of counter merges a second the load process commits.
  @merge_batches_per_s 3_000

  @doc "An empty store under a fresh directory: `{instance, dir}`."
  def open(label) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ankusa_tenant_bench_#{label}_#{System.unique_integer([:positive])}_" <>
          Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
      )

    File.mkdir_p!(dir)
    instance = :"bench_#{label}_#{System.unique_integer([:positive])}"

    config =
      Ankusa.Config.new(instance: instance, data_dir: dir, port: 0, roles: [:dispatch])

    Ankusa.put_config(config)
    {:ok, _pid} = Store.start_link(instance: instance)
    {instance, dir}
  end

  def close(instance, dir) do
    GenServer.stop(Ankusa.via(instance, :store), :normal, 30_000)
    File.rm_rf(dir)
  end

  @doc "Hands out ordinals to `names` (in the store, as the Writer would): the ordinals."
  def tenants(instance, names) do
    {:ok, next} = Tenants.next_ord(instance)

    {ords, _next} =
      names
      |> Enum.chunk_every(@chunk)
      |> Enum.reduce({[], next}, fn chunk, {acc, next} ->
        {new, ops, next, []} = Tenants.allocate(next, chunk, nil)
        :ok = Store.write(instance, ops)
        {acc ++ Enum.map(chunk, &Map.fetch!(new, &1)), next}
      end)

    ords
  end

  @doc """
  `count` due rows of tenant `ord` at `at`, `kind` 0 (first attempt) or 1
  (retry), from `seq`: the index keys, markers and counter deltas a commit
  writes. Returns the next free seq.
  """
  def due_rows(instance, ord, count, at, kind, seq) do
    field = if kind == 0, do: :pending, else: :scheduled

    seq..(seq + count - 1)//1
    |> Enum.chunk_every(@chunk)
    |> Enum.each(fn seqs ->
      ops =
        Enum.flat_map(seqs, &Deliveries.due_ops(ord, at, &1, 0, 100, kind)) ++
          Counters.ops([{ord, field, length(seqs)}, {ord, :bytes, 100 * length(seqs)}])

      :ok = Store.write(instance, Deliveries.coalesce(ops))
    end)

    seq + count
  end

  @doc """
  Starts a process committing batches of counter merges (what ingest and
  dispatch do to the counters) at `#{@merge_batches_per_s}` a second;
  `stop_load/1` ends it.
  """
  def start_load(instance) do
    parent = self()

    spawn_link(fn ->
      receive do
        :go -> load_loop(instance, parent, System.monotonic_time(:millisecond), 0)
      end
    end)
    |> tap(&send(&1, :go))
  end

  def stop_load(pid) do
    ref = Process.monitor(pid)
    send(pid, :stop)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    after
      5_000 -> Process.exit(pid, :kill)
    end
  end

  defp load_loop(instance, parent, started, written) do
    receive do
      :stop -> :ok
    after
      0 ->
        due = div((System.monotonic_time(:millisecond) - started) * @merge_batches_per_s, 1000)

        if written < due do
          ops = Counters.ops([{1, :pending, 1}, {1, :bytes, 100}, {1, :inflight, 1}])
          :ok = Store.write(instance, ops)
          load_loop(instance, parent, started, written + 1)
        else
          Process.sleep(1)
          load_loop(instance, parent, started, written)
        end
    end
  end

  @doc """
  The lowest of `rounds` medians of `runs` microsecond timings of `fun`, in
  milliseconds (after 5 warm-ups). A tick costs a fraction of a millisecond, so
  one median is moved by whatever else the machine did in those 20 ms; noise only
  ever adds time, so the quietest round is the cell's cost, and a real growth with
  the queue raises every round.
  """
  def median_ms(runs, fun, rounds \\ 5) do
    for _ <- 1..5, do: fun.()

    1..rounds
    |> Enum.map(fn _ ->
      times = for _ <- 1..runs, do: elem(:timer.tc(fun), 0)
      Enum.at(Enum.sort(times), div(runs, 2)) / 1000
    end)
    |> Enum.min()
  end

  @doc """
  Prints the cells and fails the script unless the slowest is within 1.5x of
  the fastest and under 50 ms (ADR 0001, Proof).
  """
  def verdict(title, cells) do
    IO.puts("\n#{title}")
    for {label, ms} <- cells, do: IO.puts("  #{String.pad_trailing(label, 32)} #{Float.round(ms, 3)} ms")

    {_, fastest} = Enum.min_by(cells, &elem(&1, 1))
    {_, slowest} = Enum.max_by(cells, &elem(&1, 1))
    ratio = slowest / fastest
    IO.puts("  slowest/fastest = #{Float.round(ratio, 2)}x (limit 1.5x), slowest #{Float.round(slowest, 3)} ms (limit 50 ms)")

    if ratio > 1.5 or slowest >= 50 do
      IO.puts("FAIL #{title}")
      System.halt(1)
    else
      IO.puts("PASS #{title}")
    end
  end

  @doc "Runs `measure.(instance, ords)` against each cell and returns `[{label, ms}]`."
  def cells(rows_axis, tenants_axis, measure) do
    now = System.system_time(:millisecond)

    rows =
      for rows <- rows_axis do
        {instance, dir} = open("rows")
        ords = tenants(instance, for(i <- 1..10, do: "t#{i}"))
        seed_active(instance, ords, div(rows, 10), now, 1)
        result = measured(instance, fn -> measure.(instance, ords) end)
        close(instance, dir)
        {"#{rows} rows, 10 tenants", result}
      end

    tenants =
      for count <- tenants_axis do
        {instance, dir} = open("tenants")
        ords = tenants(instance, for(i <- 1..count, do: "t#{i}"))
        {active, idle} = Enum.split(ords, min(8, count))

        # Eight tenants have due rows; the others' rows are scheduled a day ahead.
        seq = seed_active(instance, active, 1_000, now, 1)

        idle_rows(instance, idle, now + 86_400_000, seq)

        result = measured(instance, fn -> measure.(instance, active) end)
        close(instance, dir)
        {"#{count} tenants, 8 active", result}
      end

    rows ++ tenants
  end

  defp seed_active(instance, ords, per_tenant, now, seq) do
    Enum.reduce(ords, seq, fn ord, seq ->
      due_rows(instance, ord, per_tenant, now - 1_000, 0, seq)
    end)
  end

  # One retry row each, in chunks of 5 000 tenants.
  defp idle_rows(instance, ords, at, seq) do
    ords
    |> Enum.chunk_every(5_000)
    |> Enum.reduce(seq, fn chunk, seq ->
      {ops, next} =
        Enum.map_reduce(chunk, seq, fn ord, seq ->
          {Deliveries.due_ops(ord, at, seq, 0, 100, 1) ++
             Counters.ops([{ord, :scheduled, 1}, {ord, :bytes, 100}]), seq + 1}
        end)

      :ok = Store.write(instance, Deliveries.coalesce(List.flatten(ops)))
      next
    end)
  end

  defp measured(instance, fun) do
    load = start_load(instance)
    result = median_ms(200, fun)
    stop_load(load)
    result
  end
end
