# Dispatch fairness between tenants (TEN-3, ground rule 4's check):
#
#     MIX_ENV=test mix run bench/tenant_fairness_bench.exs
#
# Two instances with the same default config. On the first, a light tenant
# commits `LIGHT` hooks (default 200), 5 ms apart, to a fast sink: its p99
# delivery lag is the baseline. On the second, a heavy tenant first commits
# 99 times as many hooks to a sink that takes `SLOW_MS` (default 100) per
# delivery, then the light tenant does the same as before. Prints both
# tenants' p99 delivery lag (committed to sink call) and the heavy tenant's
# peak claimed rows against its window, and exits 1 unless
#
#   * the light tenant's p99 is within 2x of its solo p99 (or under 25 ms: at
#     that scale a scheduler hiccup is the whole difference), and
#   * the heavy tenant never held more claimed rows than its window
#     (`dispatch.tenants.default.max_inflight`) or ran more attempts than its
#     concurrency.
#
# Env knobs: LIGHT (200), SLOW_MS (100).

Logger.configure(level: :warning)

defmodule Bench.Fairness do
  defmodule Fast do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      :ets.insert(Keyword.fetch!(opts, :table), {env.id, System.monotonic_time(:microsecond)})
      :ok
    end
  end

  # Holds its slot for `ms`; `atomics` counts the attempts running at once (1)
  # and the most that ever did (2), and stamps what it delivered.
  defmodule Slow do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      ref = Keyword.fetch!(opts, :atomics)
      bump_max(ref, :atomics.add_get(ref, 1, 1))
      Process.sleep(Keyword.fetch!(opts, :ms))
      :atomics.sub(ref, 1, 1)
      :ets.insert(Keyword.fetch!(opts, :table), {env.id, System.monotonic_time(:microsecond)})
      :ok
    end

    defp bump_max(ref, now) do
      seen = :atomics.get(ref, 2)

      cond do
        now <= seen -> :ok
        :atomics.compare_exchange(ref, 2, seen, now) == :ok -> :ok
        true -> bump_max(ref, now)
      end
    end
  end

  def start(name, table, atomics, slow_ms) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ankusa_fairness_#{name}_#{System.unique_integer([:positive])}_" <>
          Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
      )

    File.mkdir_p!(dir)
    System.at_exit(fn _ -> File.rm_rf(dir) end)

    config =
      Ankusa.Config.new(
        instance: name,
        data_dir: dir,
        port: 0,
        roles: [:edge, :dispatch, :storage],
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{
             "light" => [
               verifier: {Ankusa.Verifier.None, []},
               on_verify_failure: :accept_flag,
               sinks: [{Fast, table: table}]
             ],
             "heavy" => [
               verifier: {Ankusa.Verifier.None, []},
               on_verify_failure: :accept_flag,
               sinks: [{Slow, table: table, atomics: atomics, ms: slow_ms}]
             ]
           }}
      )

    {:ok, _sup} = Ankusa.Instance.start_link(config)
    config
  end

  def ingest(instance, source, tenant) do
    {:ok, env} =
      Ankusa.Edge.Ingest.ingest(instance, %{
        source_id: source,
        method: "POST",
        path: "/webhooks/#{source}",
        headers: [],
        body: ~s({"id":"#{System.unique_integer([:positive])}"}),
        tenant_id: tenant
      })

    env.id
  end

  # `n` hooks of the light tenant, 5 ms apart: `%{id => committed_at_us}`.
  def light_load(instance, n) do
    for _ <- 1..n, into: %{} do
      t0 = System.monotonic_time(:microsecond)
      id = ingest(instance, "light", "light-tenant")
      Process.sleep(5)
      {id, t0}
    end
  end

  def lags(starts, table, timeout_ms \\ 30_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    wait(starts, table, deadline)

    for {id, t0} <- starts do
      [{^id, t1}] = :ets.lookup(table, id)
      (t1 - t0) / 1000
    end
  end

  defp wait(starts, table, deadline) do
    delivered = Enum.count(starts, fn {id, _t0} -> :ets.member(table, id) end)

    cond do
      delivered == map_size(starts) -> :ok
      System.monotonic_time(:millisecond) > deadline -> raise "light hooks never all arrived"
      true -> Process.sleep(10) && wait(starts, table, deadline)
    end
  end

  def p99(values) do
    sorted = Enum.sort(values)
    Enum.at(sorted, max(0, ceil(0.99 * length(sorted)) - 1))
  end

  def claimed(instance, ord) do
    case :sys.get_state(Ankusa.whereis(instance, :dispatch)).tenant_claimed do
      %{^ord => {rows, _bytes, _tenant}} -> rows
      _ -> 0
    end
  end
end

alias Bench.Fairness

light = String.to_integer(System.get_env("LIGHT", "200"))
slow_ms = String.to_integer(System.get_env("SLOW_MS", "100"))
heavy = 99 * light

solo_table = :ets.new(:solo, [:public, :set])
mixed_table = :ets.new(:mixed, [:public, :set])
atomics = :atomics.new(2, [])

solo_config = Fairness.start(:fairness_solo, solo_table, :atomics.new(2, []), slow_ms)
mixed_config = Fairness.start(:fairness_mixed, mixed_table, atomics, slow_ms)

window = mixed_config.dispatch.tenants.default.max_inflight
concurrency = mixed_config.dispatch.tenants.default.concurrency

solo_p99 =
  solo_config.instance
  |> Fairness.light_load(light)
  |> Fairness.lags(solo_table)
  |> Fairness.p99()

# The heavy tenant's backlog first, concurrently, as a flood arrives.
heavy_started = System.monotonic_time(:microsecond)

heavy_ids =
  1..heavy
  |> Task.async_stream(
    fn _ -> Fairness.ingest(mixed_config.instance, "heavy", "heavy-tenant") end,
    max_concurrency: 64,
    ordered: false,
    timeout: :infinity
  )
  |> Enum.map(fn {:ok, id} -> id end)

{:ok, ord} = Ankusa.Queue.Tenants.ord(mixed_config.instance, "heavy-tenant")

peak = :counters.new(1, [])

track = fn ->
  rows = Fairness.claimed(mixed_config.instance, ord)
  if rows > :counters.get(peak, 1), do: :counters.put(peak, 1, rows)
end

# the light load, sampling the heavy tenant's window as it goes
starts =
  for _ <- 1..light, into: %{} do
    t0 = System.monotonic_time(:microsecond)
    id = Fairness.ingest(mixed_config.instance, "light", "light-tenant")
    track.()
    Process.sleep(5)
    {id, t0}
  end

mixed_p99 = starts |> Fairness.lags(mixed_table) |> Fairness.p99()

# the heavy tenant's own lag, over what was delivered while this ran
finished = System.monotonic_time(:microsecond)

heavy_lags =
  for id <- heavy_ids, [{^id, t1}] <- [:ets.lookup(mixed_table, id)], t1 <= finished do
    (t1 - heavy_started) / 1000
  end

heavy_p99 = if heavy_lags == [], do: 0.0, else: Fairness.p99(heavy_lags)
peak_claimed = :counters.get(peak, 1)
peak_running = :atomics.get(atomics, 2)
ratio = mixed_p99 / max(solo_p99, 0.001)

f = &:erlang.float_to_binary(&1 / 1, decimals: 2)

IO.puts("""
tenant fairness (light #{light} hooks, heavy #{heavy} hooks to a #{slow_ms} ms sink)
  light tenant p99, alone:           #{f.(solo_p99)} ms
  light tenant p99, with the heavy:  #{f.(mixed_p99)} ms  (#{f.(ratio)}x)
  heavy tenant p99, #{length(heavy_lags)} delivered:  #{f.(heavy_p99)} ms
  heavy tenant peak claimed rows:    #{peak_claimed} (window #{window})
  heavy tenant peak running:         #{peak_running} (concurrency #{concurrency})
""")

failures =
  [
    {mixed_p99 > max(2 * solo_p99, 25), "light tenant p99 is more than 2x its solo p99"},
    {peak_claimed > window, "heavy tenant claimed more rows than its window"},
    {peak_running > concurrency, "heavy tenant ran more attempts than its concurrency"}
  ]
  |> Enum.filter(&elem(&1, 0))
  |> Enum.map(&elem(&1, 1))

case failures do
  [] ->
    IO.puts("PASS")

  failures ->
    for failure <- failures, do: IO.puts("FAIL: #{failure}")
    System.halt(1)
end
