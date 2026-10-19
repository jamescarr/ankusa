# The Redis route store loading 10^6 routes (TEN-9, Proof).
#
#     docker compose up -d --wait          # the package's Redis, :6399
#     MIX_ENV=test mix run bench/routes_load_bench.exs
#
#     ROUTES_BENCH_N   routes (default 1_000_000), over N / 10 distinct first segments
#     REDIS_URL        default redis://localhost:6399
#
# Writes N route definitions into a scratch namespace the way a fleet that had
# created them would have left them, then boots one edge node against it and
# reloads it (a peer's write moves the version), timing both. Each is a paged
# `HSCAN` load through the default 5 s command timeout and no more than the
# 60 s the task allows at 10^6. Exits 1 on a missed budget or a failed load.
# The scratch namespace is deleted afterwards.

Logger.configure(level: :warning)

alias Ankusa.Routes
alias Ankusa.Routes.Route

n = String.to_integer(System.get_env("ROUTES_BENCH_N", "1000000"))
paths = max(div(n, 10), 1)
url = System.get_env("REDIS_URL", "redis://localhost:6399")
namespace = "ankusa:routes:bench:#{System.unique_integer([:positive])}"
budget_ms = max(round(60_000 * n / 1_000_000), 5_000)

{:ok, conn} = Redix.start_link(url)
{:ok, template} = Route.from_attrs(%{"id" => "t", "path" => "/t/t"})

IO.puts("routes load bench: #{n} routes over #{paths} distinct first segments, #{url}")

{seed_micros, _} =
  :timer.tc(fn ->
    1..n
    |> Stream.map(fn i ->
      route = %{template | id: "r#{i}", path: "/p#{rem(i, paths)}/r#{div(i, paths)}"}
      ["HSET", "#{namespace}:routes", route.id, JSON.encode!(Route.to_stored_json(route))]
    end)
    |> Stream.chunk_every(2_000)
    |> Enum.each(&Redix.pipeline!(conn, &1, timeout: 60_000))

    Redix.command!(conn, ["SET", "#{namespace}:version", "2"])
  end)

IO.puts("  seeded Redis: #{div(seed_micros, 1_000)} ms")

config =
  Ankusa.Config.new(
    instance: :routes_load_bench,
    roles: [:edge],
    data_dir: Path.join(System.tmp_dir!(), "ankusa_routes_load_bench_#{System.unique_integer([:positive])}"),
    port: 0,
    routes: [
      enabled: true,
      max_routes: n * 2,
      memory_budget_bytes: 64 * 1_073_741_824,
      admin: [port: 0],
      store:
        {Ankusa.Routes.Store.Redis,
         url: url, namespace: namespace, tick_ms: 3_600_000, load_timeout_ms: 600_000}
    ]
  )

{boot_micros, {:ok, sup}} =
  :timer.tc(fn -> Supervisor.start_link([{Ankusa.Instance, config}], strategy: :one_for_one) end)

instance = config.instance
boot_ms = boot_micros / 1_000
meta = Routes.meta(instance)
IO.puts("  boot    #{Float.round(boot_ms, 1)} ms for #{meta.count} routes (budget #{budget_ms})")

# A peer's write: bump the version and nudge, so this node does a full reload.
{reload_micros, reloaded} =
  :timer.tc(fn ->
    Redix.command!(conn, ["INCR", "#{namespace}:version"])
    Redix.command!(conn, ["PUBLISH", namespace, "3"])

    Stream.repeatedly(fn -> Process.sleep(10) && Routes.meta(instance) end)
    |> Enum.find(&(&1.version == 3))
  end)

reload_ms = reload_micros / 1_000
IO.puts("  reload  #{Float.round(reload_ms, 1)} ms, count #{reloaded.count} (budget #{budget_ms})")

{:ok, ip} = Ankusa.Net.parse("203.0.113.9")
probe = fn id -> "/p#{rem(id, paths)}/r#{div(id, paths)}" end

decided =
  for id <- [1, div(n, 2), n] do
    path = probe.(id)

    {{:ok, "r" <> _}, _cached} =
      Routes.authorize_path(instance, "POST", String.split(path, "/", trim: true), path, ip)
  end

Supervisor.stop(sup)
keys = Enum.map(~w(routes ip_rules version), &"#{namespace}:#{&1}")
Redix.command!(conn, ["DEL" | keys])

failures =
  [
    {meta.count == n, "booted #{meta.count} routes, wanted #{n}"},
    {reloaded.count == n, "reloaded #{reloaded.count} routes, wanted #{n}"},
    {length(decided) == 3, "probe routes did not resolve"},
    {boot_ms <= budget_ms, "boot #{round(boot_ms)} ms > #{budget_ms} ms"},
    {reload_ms <= budget_ms, "reload #{round(reload_ms)} ms > #{budget_ms} ms"}
  ]
  |> Enum.reject(&elem(&1, 0))
  |> Enum.map(&elem(&1, 1))

case failures do
  [] ->
    IO.puts("PASS")

  failures ->
    Enum.each(failures, &IO.puts("FAIL #{&1}"))
    System.halt(1)
end
