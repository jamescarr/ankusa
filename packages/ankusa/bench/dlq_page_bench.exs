# One `GET /v1/dlq` page against the size of the DLQ (DEL-6, Proof).
#
#     MIX_ENV=test mix run bench/dlq_page_bench.exs
#
# A page must cost the same whatever the DLQ holds: `Ankusa.Queue.Deliveries.dead/2`
# scans the dead index newest first and stops at `limit + 1` matches (or at its
# scan budget), so it never folds the dead family — before DEL-6 the listing
# did, and a page was O(size of the DLQ). This bench seeds `ROWS` dead-letter
# rows over `TENANTS` tenants and `SINKS` sinks, then calls the real path — a
# `Plug.Test` conn through `Ankusa.Admin.Router.call/2`, auth plug, query
# parsing and JSON body included — `CALLS` times per case:
#
#   no filter                the first page, and a page resumed from a cursor
#                            in the middle of the DLQ
#   ?tenant=<a tenant>       the same two through the by-tenant index
#   ?sink=<n>                the sink predicate
#   ?error_class=transient   the deferred predicate (a delivery row read per
#                            candidate)
#
# Rows are written straight to the store, in batches: the dead index keys (the
# global and the by-tenant one), the delivery row and the hook a commit would
# write, plus the counters. Ingesting `ROWS` hooks through the edge would take
# hours. Every hook carries a 2-byte body, so this store is far smaller than a
# production DLQ of the same row count; a page only reads the rows and hooks it
# returns, so the bodies of the rest change nothing.
#
# Prints one JSON line and a table, and exits 1 unless every case's p99 is
# under `LIMIT_MS` (100, DEL-6's *Done when*).
#
# Env knobs: ROWS (10_000_000), TENANTS (4), SINKS (3), CALLS (30), WARMUP (3),
#            PAGE_LIMIT (100, the router's default), LIMIT_MS (100),
#            SETTLE_MS (5_000, the pause after seeding before measuring).

Logger.configure(level: :warning)
Code.require_file("tenant_seed.exs", __DIR__)

defmodule Bench.DlqPage do
  alias Ankusa.{Config, Envelope, Store}
  alias Ankusa.Queue.Deliveries
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Store.Keys

  @chunk 5_000
  # Stop seeding when the store has grown to this share of the free space: a
  # full disk on the machine running the bench helps nobody, and a partial
  # store is not a measurement.
  @disk_share 3 / 4

  def env_int(name, default), do: String.to_integer(System.get_env(name, "#{default}"))

  @doc "An empty store under a fresh directory with the admin API enabled: `{instance, dir}`."
  def open do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ankusa_dlq_bench_#{System.unique_integer([:positive])}_" <>
          Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
      )

    File.mkdir_p!(dir)
    instance = :"bench_dlq_#{System.unique_integer([:positive])}"

    config =
      Config.new(
        instance: instance,
        data_dir: dir,
        port: 0,
        roles: [:dispatch],
        admin: %{enabled: true, port: 0}
      )

    Ankusa.put_config(config)
    {:ok, _pid} = Store.start_link(instance: instance)
    {instance, dir}
  end

  def close(instance, dir) do
    GenServer.stop(Ankusa.via(instance, :store), :normal, 60_000)
    File.rm_rf(dir)
  end

  @doc """
  Writes `rows` dead-letter rows to the store. Row `seq` (1..rows) belongs to
  tenant `rem(seq - 1, tenants)` and sink `rem(seq - 1, sinks)`, and was
  dead-lettered at `base_at + seq`, so the newest is at the end of the range.

  Returns `{:ok, info}` with the tenant ordinals, the seed time and the store's
  size, or `{:error, {:disk, free_kb, projected_kb}}` when finishing `rows`
  would use more than `@disk_share` of the free space.
  """
  def seed(instance, dir, %{rows: rows, tenants: tenants, sinks: sinks, base_at: base_at}) do
    names = Enum.map(1..tenants, &"t-#{&1}")
    ords = Bench.TenantSeed.tenants(instance, names)
    ord_tuple = List.to_tuple(ords)

    started = System.monotonic_time(:millisecond)

    chunks = Enum.chunk_every(1..rows, @chunk)

    seeded =
      Enum.reduce(chunks, 0, fn chunk, acc ->
        ops = Enum.flat_map(chunk, &row_ops(ord_tuple, tenants, sinks, base_at, &1))
        :ok = Store.write(instance, ops)
        acc = acc + length(chunk)

        if rem(div(acc, @chunk), 100) == 0 do
          check_space!(dir, rows, acc)
        end

        acc
      end)

    seed_ms = System.monotonic_time(:millisecond) - started

    # One counter batch, as `dead_ops/8` would have written: `total` needs the
    # node aggregate (ordinal 0), a tenant-filtered page the per-tenant rows.
    deltas =
      names
      |> Enum.with_index()
      |> Enum.flat_map(fn {_name, i} ->
        count = div(rows, tenants) + if(i < rem(rows, tenants), do: 1, else: 0)
        ord = elem(ord_tuple, i)
        [{ord, :dead, count}, {ord, :dead_bytes, count * 2}, {ord, :bytes, count * 2}]
      end)

    :ok = Store.write(instance, Counters.ops(deltas))

    {:ok,
     %{
       names: names,
       ords: ords,
       base_at: base_at,
       rows: seeded,
       seed_ms: seed_ms,
       store_kb: dir_kb(dir),
       free_kb: free_kb(dir)
     }}
  end

  defp row_ops(ords, tenants, sinks, base_at, seq) do
    i = seq - 1
    tenant = rem(i, tenants)
    ord = elem(ords, tenant)
    sink = rem(i, sinks)
    at = base_at + seq
    source_id = "src-#{tenant + 1}"
    id = "hook-#{seq}"
    class = if rem(i, 2) == 0, do: :transient, else: :permanent

    hook =
      Envelope.to_binary(%Envelope{
        id: id,
        source_id: source_id,
        received_at: at,
        method: "POST",
        path: "/hooks/#{source_id}",
        headers: [{"content-type", "application/json"}],
        content_type: "application/json",
        body: "{}",
        size: 2,
        seq: seq
      })

    row =
      Deliveries.encode_row(%{
        module: Ankusa.Sink.Log,
        state: :dead,
        at: at,
        error: "sink returned an error",
        error_class: class,
        attempts: 3,
        size: 2,
        source_id: source_id,
        id: id
      })

    [
      {:put, :hooks, Keys.hook(seq), hook},
      {:put, :deliveries, Keys.delivery(seq, sink), row},
      {:put, :index, Keys.dead(at, seq, sink), :erlang.term_to_binary({source_id, id, ord})},
      {:put, :index, Keys.dead_by_tenant(ord, at, seq, sink), <<>>}
    ]
  end

  defp check_space!(dir, rows, seeded) do
    db_kb = dir_kb(dir)
    free_kb = free_kb(dir)
    projected_kb = div(db_kb * rows, max(seeded, 1))

    if projected_kb > trunc(free_kb * @disk_share) do
      raise "stopping the seed: #{rows} rows project to #{mb(projected_kb)} MB and " <>
              "#{mb(free_kb)} MB is free (already #{mb(db_kb)} MB at #{seeded} rows). " <>
              "Rerun with a smaller ROWS."
    end
  end

  @doc """
  One page through the router: `{status, entry_count, total, body_bytes}`.
  """
  def request(instance, path) do
    conn =
      Plug.Test.conn(:get, path)
      |> Ankusa.Admin.Router.call(Ankusa.Admin.Router.init(instance: instance))

    body = Jason.decode!(conn.resp_body)

    %{
      status: conn.status,
      entries: if(is_list(body["entries"]), do: length(body["entries"]), else: 0),
      total: body["total"],
      bytes: byte_size(conn.resp_body)
    }
  end

  @doc "`calls` timings of `path` after `warmup` unmeasured calls: `%{p50, p99, max, ...}` in ms."
  def measure(instance, path, calls, warmup) do
    for _ <- 1..warmup, do: request(instance, path)

    results =
      for _ <- 1..calls do
        started = System.monotonic_time(:microsecond)
        result = request(instance, path)
        {System.monotonic_time(:microsecond) - started, result}
      end

    latencies = Enum.map(results, &elem(&1, 0))
    last = results |> List.last() |> elem(1)

    %{
      p50: percentile_ms(latencies, 0.50),
      p99: percentile_ms(latencies, 0.99),
      max: percentile_ms(latencies, 1.0),
      status: last.status,
      entries: last.entries,
      total: last.total,
      body_bytes: last.bytes,
      ok?: Enum.all?(results, fn {_us, r} -> r.status == 200 end)
    }
  end

  def percentile_ms([], _p), do: 0.0

  def percentile_ms(latencies_us, p) do
    sorted = latencies_us |> Enum.sort() |> List.to_tuple()
    idx = max(0, min(tuple_size(sorted) - 1, ceil(p * tuple_size(sorted)) - 1))
    Float.round(elem(sorted, idx) / 1000, 3)
  end

  @doc "A cursor in the middle of the DLQ, for a row of tenant `tenant` (an index, or nil)."
  def mid_cursor(info, rows, tenants, sinks, tenant_index) do
    mid = div(rows, 2)

    seq =
      case tenant_index do
        nil -> mid
        index -> index + 1 + div(mid - index - 1, tenants) * tenants
      end

    <<info.base_at + seq::64, seq::64, rem(seq - 1, sinks)::16>>
  end

  def cursor_param(cursor), do: Base.url_encode64(cursor, padding: false)

  def dir_kb(dir) do
    case File.ls(dir) do
      {:ok, _} -> du_kb(dir)
      _ -> 0
    end
  end

  @doc "Free space on `dir`'s filesystem, in KiB."
  def free_kb(dir) do
    {out, 0} = System.cmd("df", ["-k", dir])

    out
    |> String.split("\n", trim: true)
    |> List.last()
    |> String.split()
    |> Enum.at(3)
    |> String.to_integer()
  end

  defp du_kb(dir) do
    case System.cmd("du", ["-sk", dir], stderr_to_stdout: true) do
      {out, 0} ->
        out |> String.split() |> hd() |> String.to_integer()

      _ ->
        0
    end
  end

  def mb(kb), do: Float.round(kb / 1024, 1)
end

alias Bench.DlqPage, as: B

rows = B.env_int("ROWS", 10_000_000)
tenants = B.env_int("TENANTS", 4)
sinks = B.env_int("SINKS", 3)
calls = B.env_int("CALLS", 30)
warmup = B.env_int("WARMUP", 3)
page_limit = B.env_int("PAGE_LIMIT", 100)
limit_ms = B.env_int("LIMIT_MS", 100)
# The seed writes the whole DLQ in one burst, so RocksDB is still compacting it
# when the first calls arrive; a page measured then carries the compaction's
# block-cache misses, not its own cost. Let the store settle first.
settle_ms = B.env_int("SETTLE_MS", 5_000)

# Dead-letter times run up to now, one ms apart: the newest row is the newest
# key, which is where the first page starts.
base_at = System.system_time(:millisecond) - rows

{instance, dir} = B.open()
System.at_exit(fn _ -> File.rm_rf(dir) end)

{:ok, info} =
  try do
    B.seed(instance, dir, %{rows: rows, tenants: tenants, sinks: sinks, base_at: base_at})
  rescue
    error in RuntimeError ->
      IO.puts(:stderr, Exception.message(error))
      System.halt(2)
  end

tenant = Enum.at(info.names, 1)
any_cursor = info |> B.mid_cursor(rows, tenants, sinks, nil) |> B.cursor_param()
tenant_cursor = info |> B.mid_cursor(rows, tenants, sinks, 1) |> B.cursor_param()

Process.sleep(settle_ms)

cases = [
  {"no filter, first page", "/v1/dlq?limit=#{page_limit}"},
  {"no filter, mid cursor", "/v1/dlq?limit=#{page_limit}&cursor=#{any_cursor}"},
  {"tenant, first page", "/v1/dlq?limit=#{page_limit}&tenant=#{tenant}"},
  {"tenant, mid cursor", "/v1/dlq?limit=#{page_limit}&tenant=#{tenant}&cursor=#{tenant_cursor}"},
  {"sink filter", "/v1/dlq?limit=#{page_limit}&sink=1"},
  {"error_class filter", "/v1/dlq?limit=#{page_limit}&error_class=transient"}
]

results =
  for {label, path} <- cases do
    result = B.measure(instance, path, calls, warmup)
    Map.merge(result, %{case: label, path: path, pass?: result.ok? and result.p99 < limit_ms})
  end

report = %{
  rows: info.rows,
  tenants: tenants,
  sinks: sinks,
  calls: calls,
  page_limit: page_limit,
  seed_ms: info.seed_ms,
  settle_ms: settle_ms,
  rows_per_s: round(info.rows / max(info.seed_ms, 1) * 1000),
  store_mb: B.mb(info.store_kb),
  free_mb_after: B.mb(info.free_kb),
  limit_ms: limit_ms,
  cases: Enum.map(results, &Map.drop(&1, [:path]))
}

IO.puts(Jason.encode!(report))

IO.puts("""

#{info.rows} dead rows over #{tenants} tenants and #{sinks} sinks, seeded in #{info.seed_ms} ms \
(#{report.rows_per_s} rows/s, #{report.store_mb} MB on disk); #{calls} calls per case, \
p99 against #{limit_ms} ms:

  case                        entries  total      p50 ms  p99 ms  max ms  verdict
  --------------------------  -------  ---------  ------  ------  ------  -------
""")

for result <- results do
  IO.puts(
    "  #{String.pad_trailing(result.case, 26)}  " <>
      "#{String.pad_leading("#{result.entries}", 7)}  " <>
      "#{String.pad_leading("#{result.total}", 9)}  " <>
      "#{String.pad_leading("#{result.p50}", 6)}  " <>
      "#{String.pad_leading("#{result.p99}", 6)}  " <>
      "#{String.pad_leading("#{result.max}", 6)}  " <>
      "#{if result.pass?, do: "PASS", else: "FAIL"}"
  )
end

B.close(instance, dir)

failures = Enum.reject(results, & &1.pass?)

if failures != [] do
  IO.puts(:stderr, "\nFAILED: #{Enum.map_join(failures, ", ", & &1.case)}")
  System.halt(1)
end
