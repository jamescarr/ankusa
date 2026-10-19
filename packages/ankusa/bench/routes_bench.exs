# The route table at 10^6 routes (TEN-9, Proof).
#
#     MIX_ENV=test mix run bench/routes_bench.exs
#
#     ROUTES_BENCH_N       routes (default 1_000_000)
#     ROUTES_BENCH_PATHS   distinct first segments (default N / 10: ten routes each)
#
# `N` routes over `PATHS` distinct paths are published as one snapshot (what a
# boot, a Redis load or a reload does after its rows are decoded), then:
#
#   boot / reload   the first publish, and a second one over the same routes
#                   (budget: under 60 s each at 10^6; scaled linearly below that)
#   miss p99        a request no route matches, decided with the decision cache
#                   out of the picture (`Ankusa.Routes.dry_run/2`: a full
#                   normalize + IP rules + scan), 100 000 distinct paths
#                   (budget: under 1 ms)
#   hit p99         the same for paths that do match
#   create / list   the median time of one API create and of one list page at
#                   1 000 routes and at N: no more than 3x apart plus 5 ms (a
#                   create is a synced audit write, so a few ms is the floor)
#   bytes / route   the snapshot table plus the store's copy of the definitions,
#                   against `Ankusa.Routes.Snapshot.estimated_bytes/1` (what
#                   `routes.memory_budget_bytes` is checked with at boot)
#
# Exits 1 if any budget is missed. `mise run bench:gate` runs it at 10^5 routes.

Logger.configure(level: :warning)

alias Ankusa.Routes
alias Ankusa.Routes.{Route, Snapshot}

n = String.to_integer(System.get_env("ROUTES_BENCH_N", "1000000"))

paths =
  String.to_integer(System.get_env("ROUTES_BENCH_PATHS", Integer.to_string(max(div(n, 10), 1))))

boot_budget_ms = max(round(60_000 * n / 1_000_000), 5_000)

defmodule Bench.Routes do
  def start(n, instance) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ankusa_routes_bench_#{instance}_#{System.unique_integer([:positive])}"
      )

    config =
      Ankusa.Config.new(
        instance: instance,
        data_dir: dir,
        port: 0,
        roles: [:edge],
        routes: [
          enabled: true,
          max_routes: max(n * 2, 10_000),
          memory_budget_bytes: 64 * 1_073_741_824,
          admin: [port: 0]
        ]
      )

    {:ok, sup} = Supervisor.start_link([{Ankusa.Instance, config}], strategy: :one_for_one)
    {config, sup, dir}
  end

  # Route structs, built by updating one validated template: the cost of
  # decoding a stored route is the stores' to measure, not this bench's.
  def routes(n, paths) do
    {:ok, template} = Route.from_attrs(%{"id" => "t", "path" => "/t/t"})

    base =
      Enum.map(1..n, fn i ->
        id = "r#{i}"
        route = %{template | id: id, path: "/p#{rem(i, paths)}/r#{div(i, paths)}"}
        {id, route}
      end)

    # A few generic patterns, so the `[]` bucket takes part in every merge.
    generic =
      for k <- 1..50 do
        id = "g#{k}"
        {id, %{template | id: id, path: "/:a/g#{k}/:c"}}
      end

    Map.new(base ++ generic)
  end

  def publish(instance, routes) do
    store = Ankusa.whereis(instance, :routes_store)

    {micros, :ok} =
      :timer.tc(fn ->
        :sys.replace_state(
          store,
          fn state ->
            :ok = Snapshot.publish(%{state | routes: routes, version: state.version + 1})
            %{state | routes: routes, version: state.version + 1}
          end,
          :infinity
        )

        :ok
      end)

    micros / 1_000
  end

  def percentile(sorted, p),
    do: Enum.at(sorted, min(round(length(sorted) * p) - 1, length(sorted) - 1))

  def micros(fun) do
    {micros, _} = :timer.tc(fun)
    micros
  end

  def median(list), do: list |> Enum.sort() |> Enum.at(div(length(list), 2))

  def mb(bytes), do: Float.round(bytes / 1_048_576, 1)
end

IO.puts("routes bench: #{n} routes over #{paths} distinct paths")

{big, sup, big_dir} = Bench.Routes.start(n, :routes_bench_big)
instance = big.instance
{build_micros, routes} = :timer.tc(fn -> Bench.Routes.routes(n, paths) end)
IO.puts("  built #{map_size(routes)} definitions: #{div(build_micros, 1_000)} ms")

# ── boot and reload ─────────────────────────────────────────────────────────
boot_ms = Bench.Routes.publish(instance, routes)
reload_ms = Bench.Routes.publish(instance, routes)
IO.puts("  boot (first publish)   #{Float.round(boot_ms, 1)} ms   (budget #{boot_budget_ms})")
IO.puts("  reload (second)        #{Float.round(reload_ms, 1)} ms   (budget #{boot_budget_ms})")

# ── memory ──────────────────────────────────────────────────────────────────
Process.sleep(1_500)
:erlang.garbage_collect(Ankusa.whereis(instance, :routes_store))
table_bytes = :ets.info(Snapshot.table(instance), :memory) * :erlang.system_info(:wordsize)
{:memory, state_bytes} = Process.info(Ankusa.whereis(instance, :routes_store), :memory)
total = map_size(routes)
bytes_per_route = (table_bytes + state_bytes) / total
estimate = Snapshot.estimated_bytes(1) |> max(1)

IO.puts(
  "  memory                 table #{Bench.Routes.mb(table_bytes)} MiB + store state " <>
    "#{Bench.Routes.mb(state_bytes)} MiB = #{round(bytes_per_route)} B/route (estimate #{estimate})"
)

# ── miss and hit latency, cache out of the picture ──────────────────────────
request = fn path -> %{"method" => "POST", "path" => path, "ip" => "203.0.113.9"} end

Enum.each(1..2_000, fn i -> Routes.dry_run(instance, request.("/warm#{i}/x")) end)

misses =
  for i <- 1..100_000 do
    path =
      case rem(i, 3) do
        0 -> "/p#{rem(i, paths)}/nope#{i}"
        1 -> "/unknown#{i}/r1"
        2 -> "/zz#{i}/h#{rem(i, 50) + 1}/c"
      end

    Bench.Routes.micros(fn -> Routes.dry_run(instance, request.(path)) end)
  end

hits =
  for i <- 1..20_000 do
    id = rem(i * 7919, n) + 1
    path = "/p#{rem(id, paths)}/r#{div(id, paths)}"

    Bench.Routes.micros(fn ->
      {:ok, %{decision: :allow}} = Routes.dry_run(instance, request.(path))
    end)
  end

miss_sorted = Enum.sort(misses)
hit_sorted = Enum.sort(hits)
miss_p99 = Bench.Routes.percentile(miss_sorted, 0.99)
hit_p99 = Bench.Routes.percentile(hit_sorted, 0.99)

IO.puts(
  "  miss  p50 #{Bench.Routes.percentile(miss_sorted, 0.5)} µs  p99 #{miss_p99} µs  (budget 1000)"
)

IO.puts("  hit   p50 #{Bench.Routes.percentile(hit_sorted, 0.5)} µs  p99 #{hit_p99} µs")

# ── create and list against table size ──────────────────────────────────────
measure = fn inst, label ->
  creates =
    for i <- 1..60 do
      Bench.Routes.micros(fn ->
        {:ok, _} =
          Routes.create(inst, %{"id" => "bench#{label}#{i}", "path" => "/bench/#{label}/#{i}"})
      end)
    end

  middle = "r#{div(n, 2)}"

  lists =
    for _ <- 1..60 do
      Bench.Routes.micros(fn -> {:ok, _} = Routes.list(inst, cursor: middle, limit: 100) end)
    end

  {Bench.Routes.median(creates) / 1_000, Bench.Routes.median(lists) / 1_000}
end

{small, small_sup, small_dir} = Bench.Routes.start(1_000, :routes_bench_small)
{:ok, template} = Route.from_attrs(%{"id" => "t", "path" => "/t/t"})

small_routes =
  Map.new(1..1_000, fn i ->
    id = "r#{i}"
    {id, %{template | id: id, path: "/p#{i}/r0"}}
  end)

Bench.Routes.publish(small.instance, small_routes)

{create_small, list_small} = measure.(small.instance, "s")
{create_big, list_big} = measure.(instance, "b")

IO.puts(
  "  create median  #{Float.round(create_small, 2)} ms at 1000 routes, #{Float.round(create_big, 2)} ms at #{n}"
)

IO.puts(
  "  list   median  #{Float.round(list_small, 3)} ms at 1000 routes, #{Float.round(list_big, 3)} ms at #{n}"
)

# ── verdict ─────────────────────────────────────────────────────────────────
failures =
  [
    {boot_ms <= boot_budget_ms, "boot #{round(boot_ms)} ms > #{boot_budget_ms} ms"},
    {reload_ms <= boot_budget_ms, "reload #{round(reload_ms)} ms > #{boot_budget_ms} ms"},
    {miss_p99 < 1_000, "miss p99 #{miss_p99} µs >= 1000 µs"},
    {create_big <= create_small * 3 + 5, "create grew: #{create_small} -> #{create_big} ms"},
    {list_big <= list_small * 3 + 5, "list grew: #{list_small} -> #{list_big} ms"},
    {bytes_per_route <= estimate,
     "#{round(bytes_per_route)} B/route exceeds the #{estimate} B the memory budget assumes"}
  ]
  |> Enum.reject(&elem(&1, 0))
  |> Enum.map(&elem(&1, 1))

Supervisor.stop(small_sup)
Supervisor.stop(sup)
File.rm_rf(big_dir)
File.rm_rf(small_dir)

case failures do
  [] ->
    IO.puts("PASS")

  failures ->
    Enum.each(failures, &IO.puts("FAIL #{&1}"))
    System.halt(1)
end
