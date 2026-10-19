# The crash harness: one definition of "an instance under load that a drill may
# kill", shared by `test/ankusa/loss_test.exs` (kill -9 of a child BEAM) and the
# container drills in `scripts/drills/` (ENOSPC, EIO, power-loss cut).
#
# It runs as its own OS process, with no Mix and no `:ankusa` application, so
# the process that kills it gets no chance to flush anything:
#
#     ERL_LIBS=<build>/lib DATA_DIR=/data elixir crash_harness.exs serve
#     ERL_LIBS=<build>/lib DATA_DIR=/data ACKS_FILE=acks.txt elixir crash_harness.exs verify
#
# `serve` starts one `:edge` instance on `DATA_DIR` (the edge listens on
# `HTTP_PORT`, default an ephemeral port) and ingests concurrently, printing
# `ACK <id>` after each `201` (the synced commit has returned). `RATE` is the
# target hooks per second across all ingesters (default 0: unpaced),
# `CONCURRENCY` the number of ingesters (default 32) and `BODY_BYTES` the size
# of each hook's body (default 0: a few bytes). `TENANTS` (default 1) is the
# number of tenants: with more than one, the ingesters round-robin over
# sources `load-<i>` owned by tenants `t<i>`, each with a sink, so every ack
# leaves a due row under its tenant's ordinal (with one, a single sink-less
# `load` source of tenant `default`).
#
# `verify` opens `Ankusa.Store` alone over `DATA_DIR`, reads back every id in
# `ACKS_FILE` (stdout of a `serve` run) and prints
# `acked=<n> recovered=<n> missing=<n>`; it exits 1 when any acked hook is gone.
#
# Required by `test_helper.exs` for `Ankusa.CrashHarness.config/2`: the file
# only runs a command when its argv is exactly `serve` or `verify`.
defmodule Ankusa.CrashHarness do
  @moduledoc false

  @instance :crash_harness
  @source_id "load"

  @doc "The `{source_id, source opts}` pairs of an instance with `tenants` tenants."
  def sources(1), do: [{@source_id, [verifier: {Ankusa.Verifier.None, []}]}]

  def sources(tenants) do
    for i <- 0..(tenants - 1) do
      {"#{@source_id}-#{i}",
       [
         verifier: {Ankusa.Verifier.None, []},
         tenant_id: "t#{i}",
         sinks: [{Ankusa.Sink.Log, []}]
       ]}
    end
  end

  @doc "The instance config both the serving child and a reopening parent use."
  def config(data_dir, opts \\ []) do
    Ankusa.Config.new(
      instance: @instance,
      data_dir: data_dir,
      port: Keyword.get(opts, :port, 0),
      roles: [:edge],
      source_store:
        {Ankusa.SourceStore.Static, sources: Map.new(sources(Keyword.get(opts, :tenants, 1)))},
      batcher: %{partitions: 4, max_batch: 64, max_delay_ms: 5, max_queue: 100_000}
    )
  end

  @doc "Boot the instance and ingest until killed."
  def serve do
    tenants = env_int("TENANTS", 1)

    config =
      config(System.fetch_env!("DATA_DIR"), port: env_int("HTTP_PORT", 0), tenants: tenants)

    source_ids = tenants |> sources() |> Enum.map(&elem(&1, 0))
    pace_ms = pace_ms(env_int("RATE", 0), env_int("CONCURRENCY", 32))
    body = body(env_int("BODY_BYTES", 0))

    # stdout carries only `ACK <id>` lines; log output goes to stderr. The
    # handler's output type cannot be changed in place, so it is replaced.
    :ok = :logger.remove_handler(:default)
    :ok = :logger.add_handler(:default, :logger_std_h, %{config: %{type: :standard_error}})

    {:ok, _} = Application.ensure_all_started(:bandit)
    {:ok, _} = Registry.start_link(keys: :unique, name: Ankusa.Registry)
    Ankusa.put_config(config)
    {:ok, _} = Ankusa.Instance.start_link(config)

    for n <- 1..env_int("CONCURRENCY", 32) do
      spawn(fn -> loop(config.instance, pace_ms, body, source_ids, n) end)
    end

    Process.sleep(:infinity)
  end

  @doc "Reopen the store alone and check every acked id reads back."
  def verify do
    config = config(System.fetch_env!("DATA_DIR"))
    acked = read_acks(System.fetch_env!("ACKS_FILE"))

    {:ok, _} = Registry.start_link(keys: :unique, name: Ankusa.Registry)
    Ankusa.put_config(config)
    {:ok, store} = Ankusa.Store.start_link(instance: config.instance)
    {:ok, hooks} = Ankusa.Queue.hooks(config.instance, 0, 10_000_000)
    GenServer.stop(store)

    missing = MapSet.difference(acked, MapSet.new(hooks, & &1.id))

    IO.puts(
      "acked=#{MapSet.size(acked)} recovered=#{MapSet.size(acked) - MapSet.size(missing)} " <>
        "missing=#{MapSet.size(missing)}"
    )

    if MapSet.size(missing) > 0 do
      IO.puts("lost: #{inspect(Enum.take(missing, 5))}")
      System.halt(1)
    end
  end

  # `ACK <id>` lines. A SIGKILL can cut the last line mid-write, and a partial
  # id was never acked, so a tail without its newline is not counted.
  defp read_acks(path) do
    lines = path |> File.read!() |> String.split("\n")

    lines
    |> Enum.drop(-1)
    |> Enum.flat_map(fn
      "ACK " <> id -> [id]
      _ -> []
    end)
    |> MapSet.new()
  end

  defp loop(instance, pace_ms, body, source_ids, n) do
    source_id = Enum.at(source_ids, rem(n, length(source_ids)))

    req = %{
      source_id: source_id,
      method: "POST",
      path: "/webhooks/#{source_id}",
      headers: [],
      body: body
    }

    try do
      case Ankusa.Edge.Ingest.ingest(instance, req) do
        {:ok, env} ->
          IO.puts("ACK #{env.id}")
          Process.sleep(pace_ms)

        _refused ->
          Process.sleep(max(pace_ms, 20))
      end
    rescue
      _ -> Process.sleep(max(pace_ms, 20))
    catch
      :exit, _ -> Process.sleep(max(pace_ms, 20))
    end

    loop(instance, pace_ms, body, source_ids, n + 1)
  end

  # A JSON body of `bytes` (a few bytes when 0). Constant padding: the store
  # compresses hook bodies, and the WAL, which is what a full disk hits, does not.
  defp body(0), do: ~s({"n":1})
  defp body(bytes), do: ~s({"pad":") <> String.duplicate("x", max(bytes - 10, 0)) <> ~s("})

  defp pace_ms(0, _concurrency), do: 0
  defp pace_ms(rate, concurrency), do: max(1, div(1000 * concurrency, rate))

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end
end

case System.argv() do
  ["serve"] -> Ankusa.CrashHarness.serve()
  ["verify"] -> Ankusa.CrashHarness.verify()
  _ -> :ok
end
