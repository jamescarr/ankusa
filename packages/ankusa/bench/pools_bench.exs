# The owned outbound connection pools against many origins (DEL-3, Proof).
#
#     MIX_ENV=test mix run bench/pools_bench.exs
#
# Tunables (environment): ORIGINS (100000), MAX_POOLS (1024), CONCURRENCY (32, the
# dispatch default), REQUESTS (3200, per instant-sink round), ROUNDS (5).
#
# Part 1 sends one request to each of ORIGINS distinct origins through
# `Ankusa.HttpClient`, CONCURRENCY at a time, every origin a distinct tenant and
# every request to a new origin while the earlier ones are still within their idle
# time (nothing is allowed to go idle: the pool ceiling, not the idle sweep, is
# what bounds them). One loopback listener cannot have 10^5 addresses, so an origin
# is told apart the way the pool tells origins apart, by its connection profile
# (`connect_options: [hostname: ...]`): a distinct profile is a distinct Finch pool
# with its own connections, exactly as a distinct host and port is. It prints the
# peak number of live pools, of open client sockets, of file descriptors, and the
# RSS before, at the peak and after; then lets everything go idle, sweeps, and
# prints them again. Passes only if the peak pools never pass MAX_POOLS, nothing was
# refused, and after the sweep no pool or client socket is left.
#
# Part 2 is "throughput at one origin is unchanged": one origin, CONCURRENCY at once,
# against a sink that answers instantly and against one that takes 100 ms (the
# case a pool smaller than the concurrency would silently cap at
# `pool_size / 0.1 s`), through the owned pool and through Req's default pool, which
# is what the same request used before; the median of ROUNDS alternating rounds each.
# Passes only if the owned pool keeps at least 90 % of the default pool's requests
# per second at the slow sink. The instant-sink ratio is reported, not gated: it
# measures the per-request cost of the pool bookkeeping alone, and is noisy.
#
# Exits 1 when a check fails. Not part of `mise run bench:gate`: 10^5 origins take
# minutes.

Logger.configure(level: :warning)

defmodule Bench.Pong do
  @moduledoc false
  @behaviour Plug
  import Plug.Conn

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, opts) do
    if opts[:delay] > 0, do: Process.sleep(opts[:delay])
    send_resp(conn, 200, "ok")
  end
end

defmodule Bench.Pools do
  @moduledoc false

  alias Ankusa.HttpClient
  alias Ankusa.HttpClient.Pools

  def env(name, default), do: String.to_integer(System.get_env(name, "#{default}"))

  def listener(delay) do
    port = Ankusa.TestHelpers.free_port()

    {:ok, _pid} =
      Bandit.start_link(
        plug: {Bench.Pong, delay: delay},
        scheme: :http,
        ip: {127, 0, 0, 1},
        port: port,
        startup_log: false,
        thousand_island_options: [num_acceptors: 32, num_connections: 100_000]
      )

    port
  end

  def fds, do: length(File.ls!("/dev/fd"))

  def rss_mb do
    {out, 0} = System.cmd("ps", ["-o", "rss=", "-p", System.pid()])
    String.to_integer(String.trim(out)) / 1024
  end

  # Sockets this VM holds open *to* `port`: the pools' connections, not the
  # listener's side of them.
  def clients(port) do
    Enum.count(:erlang.ports(), fn p ->
      Port.info(p, :name) == {:name, ~c"tcp_inet"} and
        match?({:ok, {_ip, ^port}}, :inet.peername(p))
    end)
  end

  def snapshot(instance, port) do
    %{pools: Pools.count(instance), clients: clients(port), fds: fds(), rss_mb: rss_mb()}
  end

  def line(label, s) do
    IO.puts(
      "  #{String.pad_trailing(label, 12)} pools #{String.pad_leading("#{s.pools}", 6)}  " <>
        "client sockets #{String.pad_leading("#{s.clients}", 6)}  " <>
        "fds #{String.pad_leading("#{s.fds}", 6)}  rss #{Float.round(s.rss_mb, 1)} MB"
    )
  end

  def instance(pools) do
    instance = Ankusa.TestHelpers.unique_instance()
    Ankusa.put_config(Ankusa.Config.new(instance: instance, egress: %{pools: pools}))
    instance
  end

  def peak_sampler(instance, port) do
    parent = self()

    spawn_link(fn ->
      sample = fn sample, peak ->
        receive do
          :stop -> send(parent, {:peak, peak})
        after
          100 ->
            s = snapshot(instance, port)

            sample.(sample, %{
              pools: max(peak.pools, s.pools),
              clients: max(peak.clients, s.clients),
              fds: max(peak.fds, s.fds),
              rss_mb: max(peak.rss_mb, s.rss_mb)
            })
        end
      end

      sample.(sample, %{pools: 0, clients: 0, fds: 0, rss_mb: 0})
    end)
  end

  def rate(requests, concurrency, fun) do
    started = System.monotonic_time(:millisecond)

    1..requests
    |> Task.async_stream(fn _ -> fun.() end, max_concurrency: concurrency, timeout: :infinity)
    |> Stream.run()

    requests / ((System.monotonic_time(:millisecond) - started) / 1000)
  end

  def owned(url, instance),
    do: fn ->
      {:ok, 200, _} = HttpClient.request(:get, url, [], nil, 5_000, instance: instance)
    end

  def refused(_event, %{count: n}, _meta, counter), do: :counters.add(counter, 1, n)

  def default_req(url) do
    fn ->
      {:ok, %Req.Response{status: 200}} =
        Req.request(method: :get, url: url, retry: false, decode_body: false)
    end
  end
end

alias Ankusa.HttpClient
alias Bench.Pools, as: B

origins = B.env("ORIGINS", 100_000)
max_pools = B.env("MAX_POOLS", 1_024)
concurrency = B.env("CONCURRENCY", 32)
requests = B.env("REQUESTS", 3_200)

# ── Part 1: many origins ──────────────────────────────────────────────────────

IO.puts("Part 1: #{origins} origins, #{concurrency} at a time, max_pools #{max_pools}")

port = B.listener(0)
url = "http://127.0.0.1:#{port}/"

instance =
  B.instance(%{
    max_pools: max_pools,
    max_pools_per_tenant: 65_536,
    pool_size: 32,
    pool_max_idle_ms: 1_000
  })

before_run = B.snapshot(instance, port)
B.line("before", before_run)

sampler = B.peak_sampler(instance, port)

refused_counter = :counters.new(1, [])

:telemetry.attach(
  "pools-bench-refused",
  [:ankusa, :http_pool, :refused],
  &Bench.Pools.refused/4,
  refused_counter
)

started = System.monotonic_time(:millisecond)

results =
  1..origins
  |> Task.async_stream(
    fn i ->
      # `linger: {true, 0}`: closing a connection resets it instead of leaving the
      # client port in TIME_WAIT. This run closes ~10^5 connections to loopback in
      # seconds; without it the host's ~16 000 ephemeral ports run out
      # (`:eaddrnotavail`), which says nothing about the pools.
      HttpClient.request(:get, url, [], nil, 5_000,
        instance: instance,
        tenant: "tenant-#{i}",
        connect_options: [hostname: "origin-#{i}.bench", transport_opts: [linger: {true, 0}]]
      )
    end,
    max_concurrency: concurrency,
    timeout: :infinity
  )
  |> Enum.reduce(%{ok: 0, error: 0, first_error: nil}, fn
    {:ok, {:ok, 200, _}}, acc -> %{acc | ok: acc.ok + 1}
    other, acc -> %{acc | error: acc.error + 1, first_error: acc.first_error || other}
  end)

elapsed_s = (System.monotonic_time(:millisecond) - started) / 1000
send(sampler, :stop)

peak =
  receive do
    {:peak, peak} -> peak
  end

at_end = B.snapshot(instance, port)
refused = :counters.get(refused_counter, 1)

B.line("peak", peak)
B.line("end of run", at_end)

IO.puts(
  "  #{results.ok} ok, #{results.error} failed, #{refused} refused, " <>
    "#{Float.round(origins / elapsed_s, 1)} origins/s over #{Float.round(elapsed_s, 1)} s"
)

if results.first_error, do: IO.puts("  first failure: #{inspect(results.first_error)}")

# Everything goes idle: the sweep closes the pools, and their connections with them.
Process.sleep(1_200)
:ok = Ankusa.HttpClient.Pools.sweep_now()
Process.sleep(500)
after_sweep = B.snapshot(instance, port)
B.line("after sweep", after_sweep)

part1 = [
  {"every request answered", results.ok == origins},
  {"peak pools #{peak.pools} within max_pools #{max_pools}", peak.pools <= max_pools},
  {"nothing refused", refused == 0},
  {"no pool left after the sweep", after_sweep.pools == 0},
  {"no client socket left after the sweep", after_sweep.clients == 0}
]

# ── Part 2: one origin, unchanged throughput ──────────────────────────────────

rounds = B.env("ROUNDS", 5)

IO.puts("\nPart 2: one origin, #{concurrency} at a time, median of #{rounds} alternating rounds")

one = B.instance(%{})

median = fn values -> values |> Enum.sort() |> Enum.at(div(length(values), 2)) end

cells =
  for {label, delay, per_round} <- [
        {"instant sink", 0, requests},
        # 100 ms a request: `concurrency * 10` requests is a second per round.
        {"100 ms sink", 100, concurrency * 10}
      ] do
    sink_port = B.listener(delay)
    sink_url = "http://127.0.0.1:#{sink_port}/"

    # One warm-up round each, so neither pays for opening its connections.
    B.rate(concurrency * 2, concurrency, B.owned(sink_url, one))
    B.rate(concurrency * 2, concurrency, B.default_req(sink_url))

    samples =
      for _ <- 1..rounds do
        owned = B.rate(per_round, concurrency, B.owned(sink_url, one))
        default = B.rate(per_round, concurrency, B.default_req(sink_url))
        {owned, default}
      end

    owned = median.(Enum.map(samples, &elem(&1, 0)))
    default = median.(Enum.map(samples, &elem(&1, 1)))

    IO.puts(
      "  #{String.pad_trailing(label, 13)} owned pool #{Float.round(owned, 1)}/s   " <>
        "Req default #{Float.round(default, 1)}/s   ratio #{Float.round(owned / default, 2)}"
    )

    {label, owned, default}
  end

{_label, slow_owned, slow_default} = List.last(cells)

part2 = [
  {"100 ms sink: owned pool keeps 90 % of the default's requests per second",
   slow_owned >= 0.9 * slow_default}
]

# ── Verdict ───────────────────────────────────────────────────────────────────

IO.puts("")

failed =
  for {name, ok?} <- part1 ++ part2 do
    IO.puts("  #{if ok?, do: "PASS", else: "FAIL"}  #{name}")
    ok?
  end
  |> Enum.count(&(not &1))

if failed > 0, do: System.halt(1)
