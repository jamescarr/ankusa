# The perf regression gate (`mise run bench:gate`).
#
# Runs `bench/core_bench.exs` for every cell of the matrix below, `BENCH_RUNS`
# (3) times each, takes the median of every metric per cell, writes
# `bench/last.json`, and compares it with the checked-in `bench/baseline.json`:
#
#   FAIL p99_ms <cell>:        p99_ms > 1.2 x baseline
#   FAIL ingest_per_s <cell>:  ingest_per_s < 0.8 x baseline
#
# `p50_ms` and `end_to_end_per_s` are recorded and shown, not gated: they are
# the numbers to look at when a gated one moves.
#
# The cells are `512B` and `10240B` (body size, N=20000, CONCURRENCY=256: the
# saturated case, where a CPU cost per hook shows up as lost `ingest_per_s`)
# and `512B-c32` (CONCURRENCY=32: the unsaturated case). At 256 workers the
# pipeline is throughput-bound and latency is 256 / throughput, so a fixed
# delay in the request path (a `Process.sleep(2)` in `Ingest.ingest/2`) leaves
# both gated numbers where they were; at 32 workers latency is the service
# time and the same delay doubles p99.
#
# Both files are `{"<cell>": {"ingest_per_s", "p50_ms", "p99_ms",
# "end_to_end_per_s"}, ..., "runner": "<class>"}`. A baseline is only
# meaningful on the machine that recorded it: CI runners of one class differ
# by a factor of two in speed from job to job, so the CI job records the
# merge-base on its own runner first (BENCH_REBASELINE=1 with BENCH_BASELINE
# pointing at a temp file) and compares against that. The checked-in
# `bench/baseline.json` is the reference for local runs on the same class of
# machine; a runner-class mismatch is printed as a warning and the comparison
# still runs.
#
# The checked-in baseline also carries an `f1_code` block (`{"commit",
# "<cell>": {...}}`): the F1 baseline's code (before DUR-4), measured with this
# bench harness on the machine, and in the session, that recorded the cells
# around it. The gate ignores it; it is the like-with-like reference of
# DUR-4.b.b.b's "at least 2x the F1 baseline". A rebaseline run carries the
# block over from the file it overwrites; re-measure it (docs/testing.md
# "Perf gate") when the harness, `bench/core_bench.exs`, changes.
#
# Every run of a cell gets a fresh data directory of its own (the cell's store
# lives in `TMPDIR`, and the bench removes it on exit), so no cell and no leg
# can inherit another's store or its page cache. By default the directory is
# under `/dev/shm` when that is a writable tmpfs with room: an ingest commit
# ends in an fsync, and on the runner's disk one slow fsync is the whole p99
# tail (a stalled commit holds up to `group_commit_max_records` hooks).
# `BENCH_DATA_ROOT` overrides the root; anywhere without a tmpfs the runner's
# disk is used and the fsync is part of the measurement (docs/testing.md
# "Perf gate").
#
# Before any measured run, every cell gets one discarded warm-up run of its
# own. The first run of a cell pays costs that do not belong to the code: the
# `mix run` boot, module loading, allocator growth, a cold page cache and the
# CPU governor ramping. They show up as a trend across rounds — the failing
# base leg of run 41586954095 had `512B-c32` round medians of 6.12, 5.77 and
# 4.78 ms with the first round always slowest — and no number of extra rounds
# settles a trend. The warm-up sample feeds neither the medians nor the
# spread; it is measured on this runner, for this leg, once per cell.
#
# A p99 over 20k samples is noisy on a shared machine, and noise only ever
# adds latency and takes throughput away. So a cell over a limit after its
# `BENCH_RUNS` runs is run `BENCH_RUNS` more times and judged on its best run
# of all (lowest p99, highest ingest/s): a regression raises every run, noise
# rarely does.
#
# The same noise decides what the limit *can* say: the limits are ratios of
# 1.2x and 0.8x, so the cell's own measurements have to agree to better than
# that. A cell whose runs in a round span more than 1.2x, or whose rounds — the
# p99 medians the gate compares — do, is measured again: at most two more
# rounds of `BENCH_RUNS` runs each (nine runs at most, never more). What is
# still wider than the limit after that is INCONCLUSIVE: the measurement could
# not decide, which is neither a pass nor a regression. Such a cell prints its
# `INCONCLUSIVE p99_ms <cell>: samples span Nx across rounds (...)` line, gets
# a `::warning::` annotation, appends its samples and the fact that it was not
# judged to the GitHub step summary (`GITHUB_STEP_SUMMARY`, a no-op off CI),
# shows `inconclusive` in the table's verdict column, and is left out of the
# failures: the job does not fail on it. A cell whose failure the measurement
# *can* resolve still fails, and a cell that is over its limit and also
# unresolvable says how far over: `inconclusive (over limit by 6.2%)`.
# `BENCH_REBASELINE=1` is the base leg: it records the cells it measured and
# leaves a cell it could not settle out of the baseline it writes, printing the
# same `INCONCLUSIVE p99_ms <cell>: …` line, `::warning::` annotation and step
# summary as a comparison run, then exits 0. It never records a baseline from
# samples that wide — a baseline measured that shakily would make every later
# comparison meaningless — and it never fails the job over a measurement it
# cannot make: no diff was involved, and the branch leg still has to run. A
# cell absent from that baseline is **not judged** by the comparison leg, never
# passed: it prints the INCONCLUSIVE line for it, marks it `inconclusive` and
# does not fail on it, while every cell the baseline does carry keeps its
# judgement — a cell over its limit still fails.
#
# Env knobs:
#
#   BENCH_RUNS=3            runs per cell (the baseline is recorded with 3)
#   BENCH_CONFIRM=1         0 skips the second round for cells over a limit
#   BENCH_RUNNER=<class>    override the detected runner class
#   BENCH_BASELINE=<path>   the file to compare with, or with BENCH_REBASELINE
#                           the file to write (default bench/baseline.json)
#   BENCH_REBASELINE=1      write the medians to the baseline file instead of
#                           comparing (the checked-in one changes only in a
#                           `perf: rebaseline <reason>` PR; see docs/testing.md)
#   BENCH_DATA_ROOT=<dir>   where the per-run data directories go (default: a
#                           tmpfs if there is one, else the system temp dir)
#   BENCH_TRACE=1           sample each run's data directory and print its peak
#                           size (diagnostics; off, the gate does not touch the
#                           store's directory during a run)
#   BENCH_SELF_TEST_INCONCLUSIVE=<cell>
#                           SELF-TEST ONLY, CI never sets it: multiplies that
#                           cell's `p99_ms` per round and run so its spread
#                           cannot settle, which exercises the inconclusive
#                           path (a PR's evidence for the policy) without
#                           waiting for a noisy runner
#
# Run it from `packages/ankusa` (the mise task does):
#
#     elixir bench/gate.exs

defmodule Bench.Gate do
  @n 20_000
  @sink_latency_ms 5
  # {cell, BODY_BYTES, CONCURRENCY}; `512B-ordered` is `512B` with the source's sink
  # `ordered: true` over 64 keys (`ORDERED=1`, ADR 0004).
  @matrix [
    {"512B", 512, 256},
    {"10240B", 10_240, 256},
    {"512B-c32", 512, 32},
    {"512B-ordered", 512, 256}
  ]
  @metrics ~w(ingest_per_s p50_ms p99_ms end_to_end_per_s)

  # {metric, :min | :max, factor}: `:max` fails above factor x baseline, `:min`
  # below it.
  @gates [{"p99_ms", :max, 1.2}, {"ingest_per_s", :min, 0.8}]

  # Extra rounds of `BENCH_RUNS` runs a cell that cannot settle is given
  # (header). Bounded: two more rounds, then the cell is inconclusive.
  @spread_retries 2

  # Self-test only (header, "BENCH_SELF_TEST_INCONCLUSIVE"): that cell's
  # `p99_ms` is multiplied by [round][run], so the first round is wider than
  # the limit, the rounds' medians disagree, and the cell runs out of retries
  # as inconclusive — deterministically, on any machine. CI never sets it.
  @self_test_switch "BENCH_SELF_TEST_INCONCLUSIVE"
  @self_test_p99_factors [[1.0, 2.0, 1.0], [3.0, 3.0, 3.0], [1.0, 1.0, 1.0]]

  @bench_dir Path.dirname(__ENV__.file)
  @last Path.join(@bench_dir, "last.json")
  @default_baseline Path.join(@bench_dir, "baseline.json")

  # A per-run store for the 10 KB cell is a few hundred MB; below this much
  # free space a tmpfs is left alone and the runner's disk is used.
  @tmpfs_min_free 2 * 1024 * 1024 * 1024
  @tmpfs_candidates ["/dev/shm"]

  def main do
    runs = env_int("BENCH_RUNS", 3)
    cells = Enum.map(@matrix, &elem(&1, 0))
    runner = runner_class()
    rebaseline? = System.get_env("BENCH_REBASELINE") in ["1", "true"]
    # Read before the minutes of benching, so a missing baseline fails at once.
    baseline = if rebaseline?, do: nil, else: load_baseline(runner)
    root = data_root()
    trace? = System.get_env("BENCH_TRACE") in ["1", "true"]

    IO.puts("==> bench data root: #{root} (#{describe_root(root)}), a fresh directory per run")

    warmup(@matrix, root, trace?)
    samples = collect(@matrix, runs, root, trace?, %{})
    {samples, inconclusive} = settle_spread(@matrix, runs, root, trace?, samples, cells)
    medians = medians(samples, cells)
    rows = if baseline, do: evaluate(baseline, medians, cells), else: []
    # A cell the base leg could not settle has no entry in the baseline it
    # wrote: it is not judged (header), never compared with a missing number.
    unjudged = if baseline, do: Enum.filter(cells, &is_nil(baseline[&1])), else: []
    failing = rows |> Enum.filter(& &1.failed) |> Enum.map(& &1.cell) |> Enum.uniq()

    # One more round of runs for just the cells over a limit, judged on their
    # best run (see the header).
    {samples, medians, rows} =
      if failing != [] and env_int("BENCH_CONFIRM", 1) == 1 do
        IO.puts(
          "==> #{Enum.join(failing, ", ")} over a limit: running them again to rule out noise"
        )

        retry = Enum.filter(@matrix, fn {cell, _, _} -> cell in failing end)
        samples = collect(retry, runs, root, trace?, samples)
        medians = medians(samples, cells, failing)
        {samples, medians, evaluate(baseline, medians, cells)}
      else
        {samples, medians, rows}
      end

    write_json(@last, cells, medians, runner, [
      {"recorded_at", DateTime.utc_now() |> DateTime.to_iso8601()},
      {"runs", samples |> Map.values() |> Enum.map(&length/1) |> Enum.max()}
    ])

    IO.puts("wrote #{Path.relative_to_cwd(@last)}")

    not_judged = not_judged_entries(inconclusive, unjudged)

    cond do
      rebaseline? and inconclusive != [] ->
        # The cells this leg can measure are recorded; the ones it cannot are
        # left out (a baseline measured from samples that wide would make every
        # later comparison meaningless), reported exactly as a comparison run
        # reports them, and the job goes on to the branch leg (header).
        cells_inconclusive = Enum.map(inconclusive, &elem(&1, 0))
        settled = Enum.reject(cells, &(&1 in cells_inconclusive))

        print_table(cells, medians, empty_baseline(cells), rows, cells_inconclusive)
        print_inconclusive(not_judged)
        annotate_inconclusive(not_judged)
        step_summary(not_judged, rows)

        write_json(
          baseline_path(),
          settled,
          medians,
          runner,
          [],
          carried_f1_code(baseline_path())
        )

        IO.puts(
          "rebaselined #{baseline_path()} (runner #{runner}) without " <>
            "#{Enum.join(cells_inconclusive, ", ")}: not judged, see the step summary"
        )

        IO.puts(inconclusive_summary(not_judged, rows))

      rebaseline? ->
        write_json(baseline_path(), cells, medians, runner, [], carried_f1_code(baseline_path()))
        IO.puts("rebaselined #{baseline_path()} (runner #{runner})")

      true ->
        report(cells, medians, baseline, rows, not_judged)
    end
  end

  # A cell's p99 can only support a 1.2x decision if its measurements agree to
  # better than that. A round whose runs are wider than the limit's headroom,
  # or whose rounds (the p99 medians the gate compares) are, is measured
  # again: at most `@spread_retries` more rounds of `runs` runs each, then the
  # budget is gone and whatever is still that wide is inconclusive (header). A
  # cell that settles in its first round is decided on that round, like before;
  # a cell that never needed a retry never gets one.
  defp settle_spread(matrix, runs, root, trace?, samples, cells),
    do: settle(matrix, runs, root, trace?, samples, cells, 0)

  defp settle(matrix, runs, root, trace?, samples, cells, extra_rounds) do
    case unsettled(samples, cells, runs) do
      [] ->
        {samples, []}

      unsettled when extra_rounds == @spread_retries ->
        # Out of rounds: the cells still wider than the limit's headroom at the
        # level the gate compares cannot be decided on this runner.
        {samples, unstable(samples, cells, runs)}

      unsettled ->
        for {cell, spread, _values} <- unsettled do
          IO.puts(
            "==> p99_ms #{cell} spans #{ratio(spread)} (#{spread_samples(samples, cell, runs)})" <>
              ": re-measuring it (extra round #{extra_rounds + 1} of #{@spread_retries})"
          )
        end

        retry_cells = Enum.map(unsettled, &elem(&1, 0))
        retry = Enum.filter(matrix, fn {cell, _, _} -> cell in retry_cells end)
        retried = collect(retry, runs, root, trace?, %{}, extra_rounds + 1)
        samples = Map.merge(samples, retried, fn _cell, first, more -> first ++ more end)
        settle(matrix, runs, root, trace?, samples, cells, extra_rounds + 1)
    end
  end

  # The cells that have to be measured again: a round's runs spread wider than
  # the limit, or the rounds' medians do. The rounds' medians are the stronger
  # statement, so a cell in both appears once, with the rounds' samples.
  defp unsettled(samples, cells, runs) do
    from_rounds = unstable(samples, cells, runs)
    decided = Enum.map(from_rounds, &elem(&1, 0))

    from_rounds ++
      Enum.reject(wide(samples, cells), fn {cell, _, _} -> cell in decided end)
  end

  # What a re-measurement message shows: the round in hand, or the rounds'
  # medians once there is more than one.
  defp spread_samples(samples, cell, runs) do
    values = Enum.map(samples[cell], & &1["p99_ms"])

    if length(values) > runs,
      do: "rounds #{rounds_joined(values, runs)}",
      else: "#{runs} runs: #{join_samples(values)}"
  end

  # Cells whose own p99 samples span more than the p99 limit's factor.
  defp wide(samples, cells) do
    for cell <- cells,
        values = Enum.map(samples[cell], & &1["p99_ms"]),
        length(values) > 1,
        min = Enum.min(values),
        min > 0,
        spread = Enum.max(values) / min,
        spread > spread_limit() do
      {cell, spread, values}
    end
  end

  # The cells whose rounds, i.e. the p99 measurements the gate compares,
  # disagree by more than the limit can resolve.
  defp unstable(samples, cells, runs) do
    for cell <- cells,
        values = Enum.map(samples[cell], & &1["p99_ms"]),
        length(values) > runs,
        rounds = values |> Enum.chunk_every(runs) |> Enum.map(&median/1),
        min = Enum.min(rounds),
        min > 0,
        spread = Enum.max(rounds) / min,
        spread > spread_limit() do
      {cell, spread, rounds_joined(values, runs)}
    end
  end

  defp rounds_joined(values, runs) do
    values
    |> Enum.chunk_every(runs)
    |> Enum.map_join(" | ", fn round -> join_samples(round) end)
  end

  defp spread_limit do
    {_metric, _kind, factor} = Enum.find(@gates, fn {metric, _, _} -> metric == "p99_ms" end)
    factor
  end

  defp ratio(spread), do: :erlang.float_to_binary(spread * 1.0, decimals: 2) <> "\u00D7"

  defp join_samples(values),
    do: Enum.map_join(values, ", ", fn v -> :erlang.float_to_binary(v * 1.0, decimals: 2) end)

  # The cells the gate cannot judge, and why: the measurement could not settle
  # the cell's p99 (`inconclusive`), or the base leg could not settle it, so
  # the baseline it wrote has no entry for the cell (`unjudged`). One entry per
  # cell, in matrix order — a cell with both problems is reported once, with
  # its samples and the missing baseline entry — and neither is ever a pass
  # (header).
  defp not_judged_entries(inconclusive, unjudged) do
    details =
      Map.new(inconclusive, fn {cell, spread, samples} ->
        {cell,
         "samples span #{ratio(spread)} across rounds (#{samples}) — wider than the " <>
           "#{ratio(spread_limit())} the limit can resolve"}
      end)

    for {cell, _, _} <- @matrix,
        details[cell] || cell in unjudged do
      %{cell: cell, detail: not_judged_detail(details[cell], cell in unjudged)}
    end
  end

  defp not_judged_detail(detail, false), do: detail

  defp not_judged_detail(nil, true),
    do:
      "absent from the baseline (the base leg could not settle it on this runner), " <>
        "so there is nothing to compare it with"

  defp not_judged_detail(detail, true),
    do: detail <> " — and the baseline has no entry for it, so it cannot be judged"

  defp print_inconclusive(not_judged) do
    for %{cell: cell, detail: detail} <- not_judged do
      IO.puts("INCONCLUSIVE p99_ms #{cell}: #{detail}")
    end
  end

  defp empty_baseline(cells), do: Map.new(cells, fn cell -> {cell, %{}} end)

  # One discarded run per cell before the measured rounds (header): its sample
  # feeds neither the medians nor the spread, only the runner state the
  # measured rounds inherit.
  defp warmup(matrix, root, trace?) do
    IO.puts("==> warm-up: one discarded run per cell (feeds neither the medians nor the spread)")

    for {cell, bytes, concurrency} <- matrix do
      IO.puts("==> bench #{cell} warm-up (discarded)")

      run_bench(
        root,
        trace?,
        cell,
        bytes,
        concurrency,
        String.ends_with?(cell, "-ordered"),
        "warmup"
      )
    end
  end

  # Round-robin over the cells so a noisy minute is spread over all of them.
  # `round` is the measurement round within the cell's budget (0 for the
  # first, then 1..@spread_retries for the re-measurements); only the
  # self-test switch reads it.
  defp collect(matrix, runs, root, trace?, samples, round \\ 0) do
    for run <- 1..runs, {cell, bytes, concurrency} <- matrix, reduce: samples do
      acc ->
        IO.puts("==> bench #{cell} run #{run}/#{runs}")

        sample =
          run_bench(
            root,
            trace?,
            cell,
            bytes,
            concurrency,
            String.ends_with?(cell, "-ordered"),
            run
          )

        sample = self_test_p99(sample, cell, round, run)
        Map.update(acc, cell, [sample], &(&1 ++ [sample]))
    end
  end

  # `BENCH_SELF_TEST_INCONCLUSIVE=<cell>` (header): a switch no CI job sets. It
  # multiplies the cell's `p99_ms` by round and run, so the first round is
  # wider than the limit, the second's median is three times the first's, and
  # the gate has to take the inconclusive path on any machine.
  defp self_test_p99(sample, cell, round, run) do
    if System.get_env(@self_test_switch) == cell do
      factor =
        @self_test_p99_factors
        |> Enum.at(min(round, length(@self_test_p99_factors) - 1))
        |> Enum.at(run - 1) || 1.0

      IO.puts(
        "    SELF-TEST #{@self_test_switch}=#{cell}: p99_ms x#{factor} (round #{round + 1})"
      )

      Map.update!(sample, "p99_ms", &(&1 * factor))
    else
      sample
    end
  end

  # The median of every metric per cell; for the cells in `best_of`, the best
  # run instead (lowest latencies, highest throughputs).
  defp medians(samples, cells, best_of \\ []) do
    Map.new(cells, fn cell ->
      {cell,
       Map.new(@metrics, fn m ->
         values = Enum.map(samples[cell], & &1[m])
         {m, if(cell in best_of, do: best(m, values), else: median(values))}
       end)}
    end)
  end

  defp best(metric, values) when metric in ["p50_ms", "p99_ms"], do: Enum.min(values)
  defp best(_metric, values), do: Enum.max(values)

  defp baseline_path, do: System.get_env("BENCH_BASELINE", @default_baseline)

  defp run_bench(root, trace?, cell, body_bytes, concurrency, ordered?, run) do
    # The bench puts its store in `TMPDIR` and removes it on exit. A directory
    # of its own per run is what keeps one cell (or one leg) from inheriting
    # another's store, its size or its page cache.
    dir = Path.join(root, "ankusa_bench_gate_#{cell}_#{run}_#{unique_suffix()}")
    File.mkdir_p!(dir)
    before = dir_bytes(dir)
    sampler = if trace?, do: start_sampler(dir)

    env = [
      {"MIX_ENV", "test"},
      {"TMPDIR", dir},
      {"N", Integer.to_string(@n)},
      {"CONCURRENCY", Integer.to_string(concurrency)},
      {"SINK_LATENCY_MS", Integer.to_string(@sink_latency_ms)},
      {"BODY_BYTES", Integer.to_string(body_bytes)},
      {"ORDERED", if(ordered?, do: "1", else: "0")}
    ]

    {out, status} =
      System.cmd("mix", ["run", "bench/core_bench.exs"], env: env, stderr_to_stdout: true)

    peak = if sampler, do: stop_sampler(sampler)
    after_bytes = dir_bytes(dir)
    File.rm_rf(dir)

    IO.puts(
      "    store #{dir}: #{bytes(before)} before, #{bytes(after_bytes)} after" <>
        if(peak, do: ", #{bytes(peak)} peak (traced)", else: "")
    )

    if status != 0 do
      IO.write(out)
      abort("core_bench.exs exited #{status} (BODY_BYTES=#{body_bytes})")
    end

    json =
      out
      |> String.split("\n")
      |> Enum.find(&(String.starts_with?(&1, "{") and String.contains?(&1, ~s("acked":))))

    if json == nil do
      IO.write(out)
      abort("core_bench.exs printed no JSON report line")
    end

    report = JSON.decode!(json)
    IO.puts("    #{inspect(Map.take(report, @metrics))}")
    Map.new(@metrics, fn m -> {m, to_float(Map.fetch!(report, m))} end)
  end

  # Where the per-run data directories go. `/dev/shm` is the runner's tmpfs: an
  # ingest commit ends in an fsync, and a tmpfs makes that the "costs nothing"
  # regime (docs/testing.md "Perf gate") instead of letting one slow fsync
  # become the whole p99 tail. Anywhere without room for a run's store, the
  # system temp dir is used and the log says so.
  defp data_root do
    case System.get_env("BENCH_DATA_ROOT") do
      root when is_binary(root) and root != "" ->
        File.mkdir_p!(root)
        root

      _ ->
        Enum.find(@tmpfs_candidates, &tmpfs_usable?/1) || System.tmp_dir!()
    end
  end

  defp tmpfs_usable?(path) do
    File.dir?(path) and match?({:ok, _}, File.stat(path)) and
      writable?(path) and enough_free?(path)
  end

  defp writable?(path) do
    probe = Path.join(path, ".ankusa_bench_probe_#{unique_suffix()}")

    case File.write(probe, "") do
      :ok ->
        File.rm(probe)
        true

      _ ->
        false
    end
  end

  defp enough_free?(path) do
    case free_bytes(path) do
      nil -> false
      bytes -> bytes >= @tmpfs_min_free
    end
  end

  defp describe_root(root) do
    case {fs_type(root), free_bytes(root)} do
      {"tmpfs", bytes} -> "tmpfs, #{bytes(bytes)} free"
      {type, bytes} when is_binary(type) -> "#{type}, #{bytes(bytes)} free"
      {nil, bytes} -> "system temp dir, #{bytes(bytes)} free"
    end
  end

  defp fs_type(path) do
    with {:ok, mounts} <- File.read("/proc/mounts") do
      mounts
      |> String.split("\n", trim: true)
      |> Enum.find_value(fn line ->
        case String.split(line) do
          [_device, mount, type | _] when mount == path -> type
          _ -> nil
        end
      end)
    else
      _ -> nil
    end
  end

  # `df -Pk`: the POSIX form, so it means the same on the runner and a laptop.
  defp free_bytes(path) do
    with {out, 0} <- System.cmd("df", ["-Pk", path], stderr_to_stdout: true),
         line <- out |> String.split("\n", trim: true) |> List.last(),
         available <- line |> String.split() |> Enum.at(3),
         {kb, ""} <- Integer.parse(available) do
      kb * 1024
    else
      _ -> nil
    end
  end

  defp dir_bytes(path) do
    case File.stat(path) do
      {:ok, %{type: :regular, size: size}} ->
        size

      {:ok, %{type: :directory}} ->
        case File.ls(path) do
          {:ok, entries} -> entries |> Enum.map(&dir_bytes(Path.join(path, &1))) |> Enum.sum()
          _ -> 0
        end

      _ ->
        0
    end
  end

  defp bytes(nil), do: "n/a"
  defp bytes(n) when n < 1024, do: "#{n} B"
  defp bytes(n) when n < 1024 * 1024, do: "#{Float.round(n / 1024, 1)} KiB"
  defp bytes(n) when n < 1024 * 1024 * 1024, do: "#{Float.round(n / 1024 / 1024, 1)} MiB"
  defp bytes(n), do: "#{Float.round(n / 1024 / 1024 / 1024, 2)} GiB"

  # `BENCH_TRACE=1`: the peak size of the run's data directory, sampled once a
  # second in the background. Off by default: the gate must not walk the
  # store's directory while the store is what is being measured.
  defp start_sampler(dir) do
    parent = self()
    ref = make_ref()
    pid = spawn_link(fn -> sample_loop(dir, 0, parent, ref) end)
    {pid, ref}
  end

  defp sample_loop(dir, peak, parent, ref) do
    receive do
      {:stop, ^ref} -> send(parent, {:peak, ref, max(peak, dir_bytes(dir))})
    after
      1_000 -> sample_loop(dir, max(peak, dir_bytes(dir)), parent, ref)
    end
  end

  defp stop_sampler({pid, ref}) do
    send(pid, {:stop, ref})

    receive do
      {:peak, ^ref, peak} -> peak
    after
      5_000 -> 0
    end
  end

  defp unique_suffix do
    "#{System.system_time(:microsecond)}_#{Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)}"
  end

  defp load_baseline(runner) do
    path = baseline_path()

    baseline =
      case File.read(path) do
        {:ok, body} ->
          JSON.decode!(body)

        {:error, reason} ->
          abort(
            "no baseline at #{path} (#{:file.format_error(reason)}); " <>
              "record one with BENCH_REBASELINE=1 mise run bench:gate"
          )
      end

    if baseline["runner"] != runner do
      IO.puts(
        "WARN runner class #{runner} != baseline #{baseline["runner"]}: " <>
          "the numbers are not comparable across machines"
      )
    end

    baseline
  end

  # A cell the base leg could not settle is absent from the baseline it wrote:
  # it is not judged — never passed, never failed — and the caller reports it
  # inconclusive (header). A cell the baseline does have is judged as usual.
  defp evaluate(baseline, medians, cells) do
    for cell <- cells,
        not is_nil(baseline[cell]),
        {metric, kind, factor} <- @gates,
        base = get_in(baseline, [cell, metric]) || abort("baseline has no #{cell}.#{metric}"),
        current = medians[cell][metric] do
      failed = if kind == :max, do: current > factor * base, else: current < factor * base

      %{
        cell: cell,
        metric: metric,
        kind: kind,
        factor: factor,
        base: base,
        current: current,
        failed: failed
      }
    end
  end

  defp report(cells, medians, baseline, rows, not_judged) do
    cells_inconclusive = Enum.map(not_judged, & &1.cell)
    print_table(cells, medians, baseline, rows, cells_inconclusive)
    print_inconclusive(not_judged)

    # A cell the measurement cannot decide is neither a pass nor a regression:
    # it is reported as INCONCLUSIVE, not as a FAIL, and it does not fail the
    # run (header). A cell whose failure is resolvable still does.
    failures =
      Enum.filter(rows, &(&1.failed and &1.cell not in cells_inconclusive))

    for f <- failures do
      cmp = if f.kind == :max, do: ">", else: "<"

      IO.puts(
        "FAIL #{f.metric} #{f.cell}: #{fmt(f.current)} #{cmp} #{f.factor}\u00D7#{fmt(f.base)} " <>
          "(limit #{fmt(f.factor * f.base)})"
      )
    end

    if not_judged != [] do
      annotate_inconclusive(not_judged)
      step_summary(not_judged, rows)
      IO.puts(inconclusive_summary(not_judged, rows))
    end

    cond do
      failures != [] ->
        IO.puts("FAIL")
        System.halt(1)

      not_judged != [] ->
        # Not judged and not failed: the summary line above says so, and the
        # exit code is 0 for it. Never `PASS`: the measurement did not pass.
        :ok

      true ->
        IO.puts("PASS")
    end
  end

  # One warning annotation per cell the gate could not judge, where the job's
  # log and the pull request show it.
  defp annotate_inconclusive(not_judged) do
    for %{cell: cell} <- not_judged do
      IO.puts("::warning::INCONCLUSIVE p99_ms #{cell} — not judged")
    end
  end

  # The step summary line for each such cell: which cell, why, and that it was
  # not judged. `GITHUB_STEP_SUMMARY` is unset off CI, where this does nothing.
  defp step_summary(not_judged, rows) do
    case System.get_env("GITHUB_STEP_SUMMARY") do
      path when is_binary(path) and path != "" ->
        lines =
          for %{cell: cell, detail: detail} <- not_judged do
            "- **INCONCLUSIVE** `p99_ms #{cell}`#{over_limit_note(cell, rows)}: **not judged** — " <>
              "#{detail}.\n"
          end

        File.write!(path, lines, [:append])

      _ ->
        :ok
    end
  end

  # The closing line for a run with cells that were not judged, e.g.
  # `1 cell inconclusive (512B-c32): not judged, see the step summary`; a cell
  # that is also over its limit carries that note, e.g.
  # `512B-c32 (p99_ms over limit by 6.2%)`. Deliberately not `PASS`: nothing
  # was decided either way.
  defp inconclusive_summary(not_judged, rows) do
    cells =
      Enum.map_join(not_judged, ", ", fn %{cell: cell} ->
        cell <> over_limit_note(cell, rows)
      end)

    count = length(not_judged)
    noun = if count == 1, do: "cell", else: "cells"

    "#{count} #{noun} inconclusive (#{cells}): not judged, see the step summary"
  end

  # A cell that is over a limit and also unresolvable keeps saying how far over
  # it is: the indecision is about the measurement, not about the regression.
  defp over_limit_note(cell, rows) do
    case Enum.find(rows, &(&1.cell == cell and &1.failed)) do
      nil -> ""
      row -> " (#{row.metric} over limit by #{percent_over(row)}%)"
    end
  end

  defp percent_over(row) do
    limit = row.factor * row.base
    pct = abs(row.current - limit) / limit * 100
    :erlang.float_to_binary(pct * 1.0, decimals: 1)
  end

  defp print_table(cells, medians, baseline, rows, inconclusive) do
    gated = Map.new(rows, fn r -> {{r.cell, r.metric}, r} end)

    IO.puts("")
    IO.puts(pad(["cell", "metric", "baseline", "current", "delta", "limit", ""]))

    for cell <- cells, metric <- @metrics do
      base = get_in(baseline, [cell, metric])
      current = medians[cell][metric]

      {limit, status} =
        case gated[{cell, metric}] do
          nil ->
            # No gate row: the baseline has no entry for the cell (the base leg
            # could not settle it), so it is not judged.
            {"-", if(cell in inconclusive, do: "inconclusive", else: "")}

          r ->
            status =
              cond do
                cell in inconclusive -> "inconclusive"
                r.failed -> "FAIL"
                true -> "ok"
              end

            {limit_label(r), status}
        end

      IO.puts(pad([cell, metric, fmt(base), fmt(current), delta(base, current), limit, status]))
    end

    IO.puts("")
  end

  defp limit_label(%{kind: :max, factor: f, base: b}), do: "<= #{fmt(f * b)}"
  defp limit_label(%{kind: :min, factor: f, base: b}), do: ">= #{fmt(f * b)}"

  defp pad(cols) do
    widths = [9, 18, 12, 12, 9, 12, 12]

    cols
    |> Enum.zip(widths)
    |> Enum.map_join(" ", fn {c, w} -> String.pad_trailing(to_string(c), w) end)
    |> String.trim_trailing()
  end

  defp delta(nil, _current), do: "n/a"
  defp delta(base, _current) when base == 0, do: "n/a"

  defp delta(base, current) do
    pct = (current - base) / base * 100
    "#{if pct >= 0, do: "+", else: ""}#{:erlang.float_to_binary(pct * 1.0, decimals: 1)}%"
  end

  defp fmt(nil), do: "n/a"
  defp fmt(v), do: :erlang.float_to_binary(v * 1.0, decimals: 2)

  defp median(values) do
    sorted = Enum.sort(values)
    count = length(sorted)
    mid = div(count, 2)

    if rem(count, 2) == 1 do
      Enum.at(sorted, mid)
    else
      (Enum.at(sorted, mid - 1) + Enum.at(sorted, mid)) / 2
    end
  end

  # Two-level JSON written by hand so the checked-in baseline is stable and
  # reviewable: cells in matrix order, metrics in `@metrics` order, then the
  # `f1_code` block (when there is one), runner last.
  defp write_json(path, cells, medians, runner, extra, f1_code \\ nil) do
    cell_lines =
      for cell <- cells do
        fields =
          Enum.map_join(@metrics, ",\n", fn m ->
            ~s(    "#{m}": #{:erlang.float_to_binary(medians[cell][m] * 1.0, decimals: 3)})
          end)

        ~s(  "#{cell}": {\n#{fields}\n  })
      end

    f1_lines = if f1_code, do: [f1_code_block(f1_code)], else: []

    tail =
      Enum.map([{"runner", runner} | extra], fn {k, v} -> ~s(  "#{k}": #{JSON.encode!(v)}) end)

    File.write!(path, "{\n" <> Enum.join(cell_lines ++ f1_lines ++ tail, ",\n") <> "\n}\n")
  end

  # Every cell the carried block has, in matrix order — not just the cells this
  # run writes: a baseline that leaves an unsettled cell out still carries the
  # whole `f1_code` block (header).
  defp f1_code_block(f1_code) do
    cell_lines =
      for {cell, _, _} <- @matrix, f1_code[cell] do
        fields =
          Enum.map_join(@metrics, ", ", fn m ->
            ~s("#{m}": #{:erlang.float_to_binary(f1_code[cell][m] * 1.0, decimals: 3)})
          end)

        ~s(    "#{cell}": {#{fields}})
      end

    lines = [~s(    "commit": #{JSON.encode!(f1_code["commit"])}) | cell_lines]
    ~s(  "f1_code": {\n#{Enum.join(lines, ",\n")}\n  })
  end

  # The `f1_code` block of the file a rebaseline is about to overwrite, if any.
  defp carried_f1_code(path) do
    with {:ok, body} <- File.read(path),
         {:ok, %{"f1_code" => f1_code}} <- JSON.decode(body) do
      f1_code
    else
      _ -> nil
    end
  end

  # The class of machine the numbers belong to. CI sets RUNNER_OS/RUNNER_ARCH
  # ("Linux"/"X64"); anywhere else it is the local OS and CPU.
  defp runner_class do
    case System.get_env("BENCH_RUNNER") do
      nil -> detect_runner()
      "" -> detect_runner()
      class -> class
    end
  end

  defp detect_runner do
    if System.get_env("GITHUB_ACTIONS") == "true" do
      os = System.get_env("RUNNER_OS", "unknown") |> String.downcase()
      arch = System.get_env("RUNNER_ARCH", "unknown") |> String.downcase()
      "github-#{os}-#{arch}"
    else
      {_family, os} = :os.type()

      arch =
        :erlang.system_info(:system_architecture) |> List.to_string() |> String.split("-") |> hd()

      "local-#{os}-#{arch}"
    end
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end

  defp to_float(v), do: v * 1.0

  defp abort(message) do
    IO.puts(:stderr, "bench gate: #{message}")
    System.halt(1)
  end
end

Bench.Gate.main()
