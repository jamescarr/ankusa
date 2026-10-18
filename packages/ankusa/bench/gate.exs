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
# meaningful on the runner class that recorded it (the CI runner, not a
# laptop); a mismatch is printed as a warning and the comparison still runs.
#
# A p99 over 20k samples is noisy on a shared machine, so a cell over a limit
# is run `BENCH_RUNS` more times and judged on the median of all of its runs;
# a regression fails both rounds, noise rarely does.
#
# Env knobs:
#
#   BENCH_RUNS=3            runs per cell (the baseline is recorded with 3)
#   BENCH_CONFIRM=1         0 skips the second round for cells over a limit
#   BENCH_RUNNER=<class>    override the detected runner class
#   BENCH_BASELINE=<path>   compare with another file
#   BENCH_REBASELINE=1      write the medians to bench/baseline.json instead of
#                           comparing (a `perf: rebaseline <reason>` PR only;
#                           see docs/testing.md)
#
# Run it from `packages/ankusa` (the mise task does):
#
#     elixir bench/gate.exs

defmodule Bench.Gate do
  @n 20_000
  @sink_latency_ms 5
  # {cell, BODY_BYTES, CONCURRENCY}
  @matrix [{"512B", 512, 256}, {"10240B", 10_240, 256}, {"512B-c32", 512, 32}]
  @metrics ~w(ingest_per_s p50_ms p99_ms end_to_end_per_s)

  # {metric, :min | :max, factor}: `:max` fails above factor x baseline, `:min`
  # below it.
  @gates [{"p99_ms", :max, 1.2}, {"ingest_per_s", :min, 0.8}]

  @bench_dir Path.dirname(__ENV__.file)
  @last Path.join(@bench_dir, "last.json")
  @default_baseline Path.join(@bench_dir, "baseline.json")

  def main do
    runs = env_int("BENCH_RUNS", 3)
    cells = Enum.map(@matrix, &elem(&1, 0))
    runner = runner_class()
    rebaseline? = System.get_env("BENCH_REBASELINE") in ["1", "true"]
    # Read before the minutes of benching, so a missing baseline fails at once.
    baseline = if rebaseline?, do: nil, else: load_baseline(runner)

    samples = collect(@matrix, runs, %{})
    medians = medians(samples, cells)
    rows = if baseline, do: evaluate(baseline, medians, cells), else: []
    failing = rows |> Enum.filter(& &1.failed) |> Enum.map(& &1.cell) |> Enum.uniq()

    # A p99 over 20k samples on a shared machine is noisy: one more round of
    # runs for just the failing cells (their median is then over twice the
    # runs) separates noise from a regression, which fails both rounds.
    {samples, medians, rows} =
      if failing != [] and env_int("BENCH_CONFIRM", 1) == 1 do
        IO.puts(
          "==> #{Enum.join(failing, ", ")} over a limit: running them again to rule out noise"
        )

        retry = Enum.filter(@matrix, fn {cell, _, _} -> cell in failing end)
        samples = collect(retry, runs, samples)
        medians = medians(samples, cells)
        {samples, medians, evaluate(baseline, medians, cells)}
      else
        {samples, medians, rows}
      end

    write_json(@last, cells, medians, runner, [
      {"recorded_at", DateTime.utc_now() |> DateTime.to_iso8601()},
      {"runs", samples |> Map.values() |> Enum.map(&length/1) |> Enum.max()}
    ])

    IO.puts("wrote #{Path.relative_to_cwd(@last)}")

    if rebaseline? do
      write_json(@default_baseline, cells, medians, runner, [])
      IO.puts("rebaselined #{Path.relative_to_cwd(@default_baseline)} (runner #{runner})")
    else
      report(cells, medians, baseline, rows)
    end
  end

  # Round-robin over the cells so a noisy minute is spread over all of them.
  defp collect(matrix, runs, samples) do
    for run <- 1..runs, {cell, bytes, concurrency} <- matrix, reduce: samples do
      acc ->
        IO.puts("==> bench #{cell} run #{run}/#{runs}")
        sample = run_bench(bytes, concurrency)
        Map.update(acc, cell, [sample], &(&1 ++ [sample]))
    end
  end

  defp medians(samples, cells) do
    Map.new(cells, fn cell ->
      {cell, Map.new(@metrics, fn m -> {m, median(Enum.map(samples[cell], & &1[m]))} end)}
    end)
  end

  defp run_bench(body_bytes, concurrency) do
    env = [
      {"MIX_ENV", "test"},
      {"N", Integer.to_string(@n)},
      {"CONCURRENCY", Integer.to_string(concurrency)},
      {"SINK_LATENCY_MS", Integer.to_string(@sink_latency_ms)},
      {"BODY_BYTES", Integer.to_string(body_bytes)}
    ]

    {out, status} =
      System.cmd("mix", ["run", "bench/core_bench.exs"], env: env, stderr_to_stdout: true)

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

  defp load_baseline(runner) do
    path = System.get_env("BENCH_BASELINE", @default_baseline)

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

  defp evaluate(baseline, medians, cells) do
    for cell <- cells,
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

  defp report(cells, medians, baseline, rows) do
    print_table(cells, medians, baseline, rows)

    failures = Enum.filter(rows, & &1.failed)

    for f <- failures do
      cmp = if f.kind == :max, do: ">", else: "<"

      IO.puts(
        "FAIL #{f.metric} #{f.cell}: #{fmt(f.current)} #{cmp} #{f.factor}\u00D7#{fmt(f.base)} " <>
          "(limit #{fmt(f.factor * f.base)})"
      )
    end

    if failures == [] do
      IO.puts("PASS")
    else
      IO.puts("FAIL")
      System.halt(1)
    end
  end

  defp print_table(cells, medians, baseline, rows) do
    gated = Map.new(rows, fn r -> {{r.cell, r.metric}, r} end)

    IO.puts("")
    IO.puts(pad(["cell", "metric", "baseline", "current", "delta", "limit", ""]))

    for cell <- cells, metric <- @metrics do
      base = get_in(baseline, [cell, metric])
      current = medians[cell][metric]

      {limit, status} =
        case gated[{cell, metric}] do
          nil -> {"-", ""}
          r -> {limit_label(r), if(r.failed, do: "FAIL", else: "ok")}
        end

      IO.puts(pad([cell, metric, fmt(base), fmt(current), delta(base, current), limit, status]))
    end

    IO.puts("")
  end

  defp limit_label(%{kind: :max, factor: f, base: b}), do: "<= #{fmt(f * b)}"
  defp limit_label(%{kind: :min, factor: f, base: b}), do: ">= #{fmt(f * b)}"

  defp pad(cols) do
    widths = [9, 18, 12, 12, 9, 12, 5]

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
  # reviewable: cells in matrix order, metrics in `@metrics` order, runner last.
  defp write_json(path, cells, medians, runner, extra) do
    cell_lines =
      for cell <- cells do
        fields =
          Enum.map_join(@metrics, ",\n", fn m ->
            ~s(    "#{m}": #{:erlang.float_to_binary(medians[cell][m] * 1.0, decimals: 3)})
          end)

        ~s(  "#{cell}": {\n#{fields}\n  })
      end

    tail =
      Enum.map([{"runner", runner} | extra], fn {k, v} -> ~s(  "#{k}": #{JSON.encode!(v)}) end)

    File.write!(path, "{\n" <> Enum.join(cell_lines ++ tail, ",\n") <> "\n}\n")
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
