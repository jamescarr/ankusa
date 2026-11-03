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
# It also prints the two timestamps the drain promise is measured on, in this
# process's own `System.monotonic_time(:millisecond)`:
#
#     SIGTERM <ms>    the VM received SIGTERM (`:erl_signal_server`'s default
#                     handler still stops the node through `prep_stop/1`)
#     DRAINING <ms>   `Ankusa.Health.drain/1` had run: `/ready` now answers
#                     `503 draining` from this moment on
#
# The distance between them is what the test asserts on. Measuring from this
# process' receipt of the signal leaves out the test's `kill` fork/exec and its
# polled `/ready` round trip, which are the harness's costs, not the drainer's.
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

  # `:erl_signal_server`'s second handler: the stop's own starting timestamp.
  # SIGTERM is handled by the VM by default, and `erl_signal_handler` (already
  # installed) is what stops it; this one only reports when the signal arrived.
  defmodule Signal do
    @moduledoc false
    @behaviour :gen_event

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_event(:sigterm, state) do
      IO.puts("SIGTERM #{System.monotonic_time(:millisecond)}")
      {:ok, state}
    end

    def handle_event(_event, state), do: {:ok, state}

    @impl true
    def handle_call(_request, state), do: {:ok, :ok, state}

    @impl true
    def handle_info(_message, state), do: {:ok, state}

    @impl true
    def terminate(_reason, _state), do: :ok

    @impl true
    def code_change(_old_vsn, state, _extra), do: {:ok, state}
  end

  @doc "The instance config the serving child and a reopening parent both use."
  def config(data_dir, opts \\ []) do
    Ankusa.Config.new(
      instance: @instance,
      data_dir: data_dir,
      port: Keyword.get(opts, :port, 0),
      roles: [:edge, :dispatch],
      listener: %{drain_delay_ms: 1_000},
      batcher: %{partitions: 4, max_batch: 64},
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

    # After the instance is up, so `draining?` only ever turns true for the stop.
    :ok = :gen_event.add_handler(:erl_signal_server, Signal, nil)
    spawn(&announce_draining/0)
    spawn(&commands/0)
    IO.puts("READY")
    Process.sleep(:infinity)
  end

  # `Ankusa.Health.drain/1` is the flip: the next `/ready` answers `503 draining`.
  # A 1 ms poll sees it, no HTTP round trip on the listener that is holding the
  # drill's requests, and prints it on this process' own clock.
  defp announce_draining do
    if Ankusa.Health.draining?(@instance) do
      IO.puts("DRAINING #{System.monotonic_time(:millisecond)}")
    else
      Process.sleep(1)
      announce_draining()
    end
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
