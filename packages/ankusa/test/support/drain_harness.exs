# The drain harness: one instance in its own OS process, under the real `:ankusa`
# application, so a SIGTERM reaches `Ankusa.Application.prep_stop/1` exactly as
# it does in the container. `test/ankusa/drain_test.exs` starts it, holds
# requests in flight, sends the signal and checks what the clients saw.
#
#     ERL_LIBS=<build>/lib DATA_DIR=/data HTTP_PORT=4000 elixir drain_harness.exs serve
#
# `serve` starts the `:ankusa` application (no instance: `autostart` is off), then
# one `:edge` + `:dispatch` instance in its supervision tree with a source `drain`
# whose sink is slow, and prints `READY` once the listener is up. `LOG_LEVEL`
# (default `none`) sends log lines to stderr. It then reads commands from stdin:
#
#     STALL <ms>   suspend the queue writer, print `STALLED`; after <ms> resume it
#                  and print `RESUMED`. A POST sent meanwhile blocks in the
#                  batcher, which is how a drill holds requests in flight.
#
# Required by `test_helper.exs` for `Ankusa.DrainHarness.config/2`: the file only
# runs when its argv is exactly `serve`.
defmodule Ankusa.DrainHarness do
  @moduledoc false

  @instance :drain_harness

  defmodule SlowSink do
    @moduledoc false
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, opts) do
      Process.sleep(Keyword.fetch!(opts, :delay_ms))
      :ok
    end
  end

  @doc "The instance config the serving child and a reopening parent both use."
  def config(data_dir, opts \\ []) do
    Ankusa.Config.new(
      instance: @instance,
      data_dir: data_dir,
      port: Keyword.get(opts, :port, 0),
      roles: [:edge, :dispatch],
      listener: %{drain_delay_ms: 1_000},
      batcher: %{partitions: 4},
      source_store:
        {Ankusa.SourceStore.Static,
         sources: %{
           "drain" => [
             verifier: {Ankusa.Verifier.None, []},
             sinks: [{SlowSink, [delay_ms: 500]}]
           ]
         }}
    )
  end

  @doc "Boot the instance and serve until stopped."
  def serve do
    config =
      config(System.fetch_env!("DATA_DIR"),
        port: String.to_integer(System.get_env("HTTP_PORT", "0"))
      )

    # stdout carries only the protocol lines; log output goes to stderr.
    :ok = :logger.remove_handler(:default)

    case System.get_env("LOG_LEVEL", "none") do
      "none" ->
        :ok

      level ->
        :ok =
          :logger.add_handler(:default, :logger_std_h, %{
            level: String.to_existing_atom(level),
            filter_default: :log,
            filters: [],
            config: %{type: :standard_error}
          })
    end

    {:ok, _} = Application.ensure_all_started(:ankusa)
    {:ok, _} = Supervisor.start_child(Ankusa.Supervisor, {Ankusa.Instance, config})

    spawn(&commands/0)
    IO.puts("READY")
    Process.sleep(:infinity)
  end

  defp commands do
    case IO.gets("") do
      "STALL " <> ms ->
        stall(ms |> String.trim() |> String.to_integer())
        commands()

      :eof ->
        :ok

      _ ->
        commands()
    end
  end

  defp stall(ms) do
    writer = Ankusa.whereis(@instance, :queue_writer)
    :ok = :sys.suspend(writer)
    IO.puts("STALLED")
    Process.sleep(ms)
    :ok = :sys.resume(writer)
    IO.puts("RESUMED")
  end
end

case System.argv() do
  ["serve"] -> Ankusa.DrainHarness.serve()
  _ -> :ok
end
