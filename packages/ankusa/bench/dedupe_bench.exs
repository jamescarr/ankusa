# What the index family's bloom filter buys the ingest dedupe lookup (DUR-3, Proof).
#
#     MIX_ENV=test mix run bench/dedupe_bench.exs
#     KEYS=100000000 CACHE_BYTES=1073741824 MIX_ENV=test mix run bench/dedupe_bench.exs
#
# The ack path asks the `index` family for a batch of dedupe keys it has never
# seen (`Store.multi_get/3` in `Queue.Writer.dedupe_check/5`) and then commits
# them, so the cost of a *miss* in a family holding KEYS dedupe keys is the
# commit's. Four runs against stores filled with KEYS keys (each a `?u` key and
# its `?e` expiry key, random like a provider's event ids), every sample a batch
# of fresh keys looked up and then committed with `sync: true`:
#
#   before        the settings before DUR-3: no bloom filter, no limit on open files
#   before_cache  the same store reopened with `CACHE_BYTES` of block cache
#   after         the new defaults on a store whose files carry the filters
#   after_cache   the same store reopened with `CACHE_BYTES` of block cache: the
#                 index and filter blocks live in the cache
#                 (`cache_index_and_filter_blocks`), so a store this size wants a
#                 cache that can hold them
#
# A filter exists only in files written after it was turned on, so `before` and
# `after` each fill their own store (the first is deleted before the second is
# filled). Prints one JSON line per run and a summary comparing each pair at the
# same block cache. Environment:
#
#   KEYS          dedupe keys in the store (default 1_000_000; the proof run is 100_000_000)
#   SAMPLES       measured batches per run (default 2_000)
#   BATCH         keys per batch (default 256, the batcher's `max_batch`)
#   CACHE_BYTES   block cache of `before_cache` and `after_cache` (default 1 GiB)
#   BENCH_DIR     where the stores live (default the system temp dir)
#
# The files stay in the OS page cache on a machine with the RAM for them: the
# numbers are the CPU and block-decoding cost of a miss, which is what a filter
# saves, and a lower bound on what it saves when the blocks are on disk.

Logger.configure(level: :warning)

defmodule Bench.Dedupe do
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @tenant "acme"
  @source "src_bench"
  @ttl_ms 72 * 3_600_000
  @chunk 25_000
  @workers 4

  def env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> value |> String.replace("_", "") |> String.to_integer()
    end
  end

  @doc "A fresh, empty store: `{instance, dir}`."
  def open(label, rocksdb, root) do
    dir = Path.join(root, "ankusa_dedupe_bench_#{label}_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    instance = :"dedupe_bench_#{label}_#{System.unique_integer([:positive])}"
    start(instance, dir, rocksdb)
    {instance, dir}
  end

  @doc """
  Starts the store of `instance` in `dir` with `rocksdb` settings. A reopen
  uses the instance it was filled under: the store's path is under its name.
  """
  def start(instance, dir, rocksdb) do
    config =
      Ankusa.Config.new(
        instance: instance,
        data_dir: dir,
        port: 0,
        roles: [:dispatch],
        queue: %{rocksdb: rocksdb}
      )

    Ankusa.put_config(config)
    {:ok, _pid} = Store.start_link(instance: instance)
    instance
  end

  def close(instance), do: GenServer.stop(Ankusa.via(instance, :store), :normal, 120_000)

  defp pair_ops(now) do
    key = :rand.bytes(16) |> Base.encode16(case: :lower)
    at = now + @ttl_ms - :rand.uniform(@ttl_ms)

    [
      {:put, :index, Keys.dedupe(@tenant, @source, key), <<at::64, :rand.bytes(16)::binary>>},
      {:put, :index, Keys.dedupe_expiry(at, @tenant, @source, key), <<>>}
    ]
  end

  @doc "Fills the index family with `keys` dedupe keys and waits for compaction to settle."
  def fill(instance, keys) do
    now = System.system_time(:millisecond)
    chunks = div(keys + @chunk - 1, @chunk)
    started = System.monotonic_time(:millisecond)

    1..chunks
    |> Task.async_stream(
      fn n ->
        count = min(@chunk, keys - (n - 1) * @chunk)
        ops = Enum.flat_map(1..count, fn _ -> pair_ops(now) end)
        :ok = Store.write(instance, ops, sync: false)
        n
      end,
      max_concurrency: @workers,
      ordered: false,
      timeout: :infinity
    )
    |> Enum.each(fn {:ok, n} ->
      if rem(n, 400) == 0 do
        elapsed = div(System.monotonic_time(:millisecond) - started, 1_000)
        IO.puts(:stderr, "  chunk #{n} of #{chunks} written (#{elapsed}s)")
        guard_disk!()
      end
    end)

    flush(instance)
    settle(instance)
  end

  # A store of 10^8 keys is about 20 GB: stop before the disk is full for everyone.
  @min_free_kb 4 * 1024 * 1024

  defp guard_disk! do
    root = System.get_env("BENCH_DIR") || System.tmp_dir!()
    {out, 0} = System.cmd("df", ["-Pk", root])
    [_header, row] = out |> String.split("\n", trim: true) |> Enum.take(2)
    free_kb = row |> String.split() |> Enum.at(3) |> String.to_integer()

    if free_kb < @min_free_kb do
      raise "only #{div(free_kb, 1024)} MiB free under #{root}: stopping the fill (set BENCH_DIR to a bigger volume or lower KEYS)"
    end
  end

  defp flush(instance) do
    [{:handles, handles}] = :ets.lookup(:"ankusa_store_#{instance}", :handles)
    :ok = :rocksdb.flush(handles.db, Map.fetch!(handles.cfs, :index), [])
  end

  # Background compaction is part of a store this size: measure once it has caught up.
  defp settle(instance, quiet \\ 0) do
    {:ok, pending} = Store.property(instance, :index, "rocksdb.compaction-pending")
    {:ok, running} = Store.property(instance, :index, "rocksdb.num-running-compactions")
    {:ok, flushing} = Store.property(instance, :index, "rocksdb.mem-table-flush-pending")

    cond do
      pending + running + flushing > 0 ->
        Process.sleep(2_000)
        settle(instance)

      quiet < 3 ->
        Process.sleep(2_000)
        settle(instance, quiet + 1)

      true ->
        :ok
    end
  end

  @doc "`samples` batches: a multi_get of fresh keys, then their sync commit."
  def measure(instance, samples, batch) do
    run = fn ->
      now = System.system_time(:millisecond)
      ops = Enum.flat_map(1..batch, fn _ -> pair_ops(now) end)
      ukeys = for {:put, :index, <<?u, _::binary>> = key, _} <- ops, do: key

      t0 = System.monotonic_time(:microsecond)
      {:ok, found} = Store.multi_get(instance, :index, ukeys)
      t1 = System.monotonic_time(:microsecond)
      :ok = Store.write(instance, ops, sync: true)
      t2 = System.monotonic_time(:microsecond)

      true = Enum.all?(found, &(&1 == :not_found))
      {t1 - t0, t2 - t0}
    end

    Enum.each(1..200, fn _ -> run.() end)
    timings = Enum.map(1..samples, fn _ -> run.() end)

    %{
      multi_get: timings |> Enum.map(&elem(&1, 0)) |> percentiles(),
      commit: timings |> Enum.map(&elem(&1, 1)) |> percentiles()
    }
  end

  defp percentiles(values) do
    sorted = Enum.sort(values)
    n = length(sorted)
    at = fn p -> Enum.at(sorted, min(n - 1, floor(n * p))) / 1000 end
    %{p50_ms: Float.round(at.(0.5), 3), p99_ms: Float.round(at.(0.99), 3)}
  end

  def line(label, keys, batch, result, bytes) do
    IO.puts(
      JSON.encode!(%{
        run: label,
        keys: keys,
        batch: batch,
        multi_get_p50_ms: result.multi_get.p50_ms,
        multi_get_p99_ms: result.multi_get.p99_ms,
        commit_p50_ms: result.commit.p50_ms,
        commit_p99_ms: result.commit.p99_ms,
        index_sst_bytes: bytes
      })
    )
  end

  def sst_bytes(instance) do
    {:ok, bytes} = Store.property(instance, :index, "rocksdb.total-sst-files-size")
    bytes
  end
end

alias Bench.Dedupe

keys = Dedupe.env_int("KEYS", 1_000_000)
samples = Dedupe.env_int("SAMPLES", 2_000)
batch = Dedupe.env_int("BATCH", 256)
cache_bytes = Dedupe.env_int("CACHE_BYTES", 1_073_741_824)
root = System.get_env("BENCH_DIR") || System.tmp_dir!()

defaults = Ankusa.Config.new().queue.rocksdb
cache = %{block_cache_bytes: cache_bytes}

IO.puts(:stderr, "before: filling #{keys} keys without a bloom filter")
no_filter = %{bloom_bits_per_key: 0, max_open_files: -1}
{instance, dir} = Dedupe.open("before", no_filter, root)
Dedupe.fill(instance, keys)
before = Dedupe.measure(instance, samples, batch)
Dedupe.line("before", keys, batch, before, Dedupe.sst_bytes(instance))
Dedupe.close(instance)

Dedupe.start(instance, dir, Map.merge(no_filter, cache))
before_cache = Dedupe.measure(instance, samples, batch)
Dedupe.line("before_cache", keys, batch, before_cache, Dedupe.sst_bytes(instance))
Dedupe.close(instance)
File.rm_rf!(dir)

IO.puts(
  :stderr,
  "after: filling #{keys} keys with the defaults (bloom #{defaults.bloom_bits_per_key} bits)"
)

{instance, dir} = Dedupe.open("after", %{}, root)
Dedupe.fill(instance, keys)
after_default = Dedupe.measure(instance, samples, batch)
Dedupe.line("after", keys, batch, after_default, Dedupe.sst_bytes(instance))
Dedupe.close(instance)

Dedupe.start(instance, dir, cache)
after_cache = Dedupe.measure(instance, samples, batch)
Dedupe.line("after_cache", keys, batch, after_cache, Dedupe.sst_bytes(instance))
Dedupe.close(instance)
File.rm_rf!(dir)

change = fn new, old -> Float.round((new - old) / old * 100, 1) end

IO.puts(
  JSON.encode!(%{
    summary: "p99 change vs. before at the same block cache (negative = faster)",
    keys: keys,
    commit_default_cache_pct: change.(after_default.commit.p99_ms, before.commit.p99_ms),
    commit_sized_cache_pct: change.(after_cache.commit.p99_ms, before_cache.commit.p99_ms),
    multi_get_default_cache_pct: change.(after_default.multi_get.p99_ms, before.multi_get.p99_ms),
    multi_get_sized_cache_pct:
      change.(after_cache.multi_get.p99_ms, before_cache.multi_get.p99_ms)
  })
)
