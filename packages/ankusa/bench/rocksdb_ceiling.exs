# The raw ceiling under the ingest path: how many 10 KB hook rows this node's
# RocksDB can take per second, with the store exactly as `Ankusa.Store` opens it.
#
#     MIX_ENV=test mix run bench/rocksdb_ceiling.exs
#
# It starts a real instance (`Ankusa.Instance`, `roles: [:edge]`, nothing else
# running) so the store is opened with the real options: `queue.rocksdb`'s
# tunables, the nine column families and the `hooks` family's zstd + blob-file
# settings. It then writes 10 KB hook rows the way `Ankusa.Queue.Writer` does
# — `Ankusa.Store.write/3` of `{:put, :hooks, Store.Keys.hook(seq), value}` ops
# — from `PARTITIONS` (4) concurrent writer processes, one per batcher
# partition, in two modes:
#
#   * `sync_false`: every batch applied with `sync: false` and nothing ever
#     fsyncs. The CPU/apply ceiling: what the store layer can do when the disk
#     costs nothing (a RAM disk, or a cell with a free fsync).
#   * `syncer`: the same writes plus one process that does what
#     `Ankusa.Queue.Syncer` does — one `Ankusa.Store.sync_wal/1` in flight at a
#     time, taking everything applied since the last one and syncing again as
#     soon as something new is applied. The cadence the ingest path gives the
#     WAL, at its most aggressive (the syncer coalesces as much as it can).
#
# Neither mode runs the batcher, the dedupe/index rows, dispatch or the
# compactor. The number is therefore an upper bound on what `bench:gate`'s
# 10240B cell can reach with the same options on the same disk, not a
# prediction of it: it says whether 2x the F1 baseline's ingest rate is below
# what the store alone can absorb here.
#
# Env knobs: N (100000 hook rows per run), RUNS (3, best run reported),
# BATCHES ("1,16,64,256" puts per write, one cell each), PARTITIONS (4),
# BODY_BYTES (10240), WARMUP (0.1x N, written and discarded before the clock).
#
# Prints one JSON line per cell and a table; exit status is 0 unless a write or
# a sync failed.

Logger.configure(level: :error)

defmodule Bench.RocksCeiling do
  alias Ankusa.Store

  @values 64

  def run do
    n = int("N", 100_000)
    runs = int("RUNS", 3)
    partitions = int("PARTITIONS", 4)
    body_bytes = int("BODY_BYTES", 10_240)
    batches = batches(System.get_env("BATCHES", "1,16,64,256"))
    warmup = div(n * int("WARMUP_PCT", 10), 100)
    values = values(body_bytes)

    IO.puts("""

    rocksdb ceiling: #{n} x #{body_bytes} B hook rows per run, #{partitions} partitions, \
    #{runs} run(s) per cell, best run reported
    runner #{runner()}  schedulers #{System.schedulers_online()}  tmpdir #{System.tmp_dir!()}
    """)

    IO.puts(
      "#{String.pad_trailing("mode", 12)}#{String.pad_leading("batch", 6)}" <>
        "#{String.pad_leading("puts/s", 12)}#{String.pad_leading("MB/s", 10)}" <>
        "#{String.pad_leading("fsyncs/s", 11)}#{String.pad_leading("fsync ms", 10)}"
    )

    cells =
      for batch <- batches, mode <- [:sync_false, :syncer] do
        measured =
          for _ <- 1..runs do
            cell(mode, n, batch, partitions, values, warmup)
          end

        best = Enum.max_by(measured, & &1.puts_per_s)

        report =
          Map.merge(best, %{
            runs: runs,
            run_puts_per_s: Enum.map(measured, &Float.round(&1.puts_per_s, 1))
          })

        IO.puts(
          "#{String.pad_trailing("#{mode}", 12)}#{String.pad_leading("#{batch}", 6)}" <>
            "#{String.pad_leading(fmt(report.puts_per_s, 0), 12)}" <>
            "#{String.pad_leading(fmt(report.mb_per_s, 1), 10)}" <>
            "#{String.pad_leading(fmt(report.syncs_per_s, 0), 11)}" <>
            String.pad_leading(fmt(report.fsync_p50_ms, 2), 10)
        )

        report
      end

    for cell <- cells, do: IO.puts(Jason.encode!(Map.put(cell, :bench, "rocksdb_ceiling")))

    for mode <- [:sync_false, :syncer] do
      best = cells |> Enum.filter(&(&1.mode == mode)) |> Enum.max_by(& &1.puts_per_s)

      IO.puts(
        Jason.encode!(%{
          bench: "rocksdb_ceiling_best",
          mode: mode,
          batch: best.batch,
          puts_per_s: best.puts_per_s,
          mb_per_s: best.mb_per_s,
          syncs_per_s: best.syncs_per_s,
          fsync_p50_ms: best.fsync_p50_ms,
          runner: best.runner
        })
      )
    end
  end

  # ── one cell: a fresh store, a warmup, then a timed run ───────────────────

  defp cell(mode, n, batch, partitions, values, warmup) do
    name = :"ceiling_#{System.unique_integer([:positive])}"

    dir =
      Path.join(
        System.tmp_dir!(),
        "ankusa_ceiling_#{System.system_time(:microsecond)}_#{:erlang.phash2(name)}"
      )

    File.mkdir_p!(dir)

    config =
      Ankusa.Config.new(
        instance: name,
        data_dir: dir,
        port: 0,
        roles: [:edge],
        batcher: %{partitions: partitions, max_queue: 100_000},
        source_store:
          {Ankusa.SourceStore.Static, sources: %{"bench" => [sinks: [{Ankusa.Sink.Log, []}]]}}
      )

    {:ok, sup} = Ankusa.Instance.start_link(config)

    try do
      # Keys never repeat inside one cell, and the warmup's are a lower range.
      offsets = %{warmup: 0, timed: 1_000_000_000_000}
      if warmup > 0, do: _ = timed_run(:sync_false, name, warmup, batch, partitions, values, 0)
      result = timed_run(mode, name, n, batch, partitions, values, offsets.timed)

      %{
        mode: mode,
        batch: batch,
        partitions: partitions,
        body_bytes: byte_size(elem(values, 0)),
        puts: result.puts,
        puts_per_s: result.puts_per_s,
        mb_per_s: result.mb_per_s,
        syncs_per_s: result.syncs_per_s,
        fsync_p50_ms: result.fsync_p50_ms,
        runner: runner()
      }
    after
      Supervisor.stop(sup)
      File.rm_rf(dir)
    end
  end

  defp timed_run(mode, name, n, batch, partitions, values, key_base) do
    applied = :atomics.new(1, signed: false)
    parent = self()
    per = div(n + partitions - 1, partitions)

    writers =
      for p <- 1..partitions do
        spawn_link(fn ->
          receive do
            :go -> put_loop(name, per, batch, key_base + p * 100_000_000, values, applied)
          end

          send(parent, {:written, self(), System.monotonic_time(:microsecond)})
        end)
      end

    syncer =
      if mode == :syncer do
        spawn_link(fn -> sync_loop(name, applied, parent, 0, 0, []) end)
      else
        nil
      end

    :erlang.garbage_collect()
    started = System.monotonic_time(:microsecond)
    Enum.each(writers, &send(&1, :go))
    if syncer, do: send(syncer, :go)

    ended =
      for _ <- writers do
        receive do
          {:written, _pid, at} -> at
        end
      end
      |> Enum.max()

    {syncs, durations} =
      if syncer do
        send(syncer, :stop)

        receive do
          {:synced, syncs, durations} -> {syncs, durations}
        after
          120_000 -> raise "syncer did not stop"
        end
      else
        {0, []}
      end

    elapsed_s = (ended - started) / 1_000_000
    puts = per * partitions

    %{
      puts: puts,
      puts_per_s: puts / elapsed_s,
      mb_per_s: puts * byte_size(elem(values, 0)) / 1_000_000 / elapsed_s,
      syncs_per_s: if(syncs > 0, do: syncs / elapsed_s, else: 0.0),
      fsync_p50_ms: pct(Enum.sort(durations), 0.50) / 1000
    }
  end

  # One batcher partition: `total` hook rows in batches of `batch`, applied with
  # `sync: false` (the writer's own mode since DUR-4.b), keyed by a dense seq.
  defp put_loop(_name, 0, _batch, _key, _values, _applied), do: :ok

  defp put_loop(name, total, batch, key, values, applied) do
    ops =
      for i <- 0..(min(batch, total) - 1) do
        {:put, :hooks, Store.Keys.hook(key + i), elem(values, rem(i, tuple_size(values)))}
      end

    :ok = Store.write(name, ops, sync: false)
    :atomics.add(applied, 1, 1)
    put_loop(name, total - length(ops), batch, key + length(ops), values, applied)
  end

  # `Ankusa.Queue.Syncer`'s cadence: one `sync_wal` at a time, everything applied
  # since the last one, and the next one as soon as that returns if there is
  # anything new. Idles while the writers are between batches.
  defp sync_loop(name, applied, parent, last, syncs, durations) do
    receive do
      :go -> sync_loop(name, applied, parent, last, syncs, durations)
      :stop -> send(parent, {:synced, syncs, durations})
    after
      0 ->
        case :atomics.get(applied, 1) do
          ^last ->
            Process.sleep(1)
            sync_loop(name, applied, parent, last, syncs, durations)

          n ->
            started = System.monotonic_time(:microsecond)
            :ok = Store.sync_wal(name)
            took = System.monotonic_time(:microsecond) - started
            sync_loop(name, applied, parent, n, syncs + 1, [took | durations])
        end
    end
  end

  # ── plumbing ──────────────────────────────────────────────────────────────

  # 10 KB values with the shape `bench/core_bench.exs` sends: JSON with a pad of
  # one repeated byte, so the `hooks` family's zstd + blob settings pay what
  # they pay for a real 10 KB body instead of for random bytes.
  defp values(body_bytes) do
    pad = String.duplicate("x", body_bytes)

    for i <- 1..@values do
      Jason.encode!(%{"id" => "ceiling-#{i}", "pad" => pad})
    end
    |> List.to_tuple()
  end

  defp batches(spec), do: spec |> String.split(",", trim: true) |> Enum.map(&String.to_integer/1)

  defp pct([], _p), do: 0.0

  defp pct(sorted, p) do
    rank = trunc(Float.ceil(p * length(sorted))) - 1
    Enum.at(sorted, rank |> min(length(sorted) - 1) |> max(0))
  end

  defp runner do
    {_, os} = :os.type()
    arch = :erlang.system_info(:system_architecture) |> to_string() |> String.split("-") |> hd()
    "#{os}-#{arch}"
  end

  defp fmt(x, digits), do: :erlang.float_to_binary(x / 1, decimals: digits)
  defp int(name, default), do: String.to_integer(System.get_env(name, "#{default}"))
end

Bench.RocksCeiling.run()
