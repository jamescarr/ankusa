# The lifecycle harness: an instance that makes one source change and waits to be
# killed, for `test/ankusa/lifecycle_test.exs` ("survives kill").
#
# It runs as its own OS process, with no Mix and no `:ankusa` application, so the
# process that kills it gets no chance to flush anything:
#
#     ERL_LIBS=<build>/lib DATA_DIR=/data elixir lifecycle_harness.exs serve
#
# `serve` boots one `:edge` instance with a writable source store on `DATA_DIR`
# and a lifecycle sink that never confirms (`HoldSink`), so the event cannot have
# been delivered by the time the parent kills it. It creates one source, prints
# `PUT_DONE` the moment `SourceStore.put/6` returns, then sleeps. The parent
# SIGKILLs it on that line, reopens the store with a sink that records, and
# expects the event.
#
# Required by `test_helper.exs` for `Ankusa.LifecycleHarness.config/2`: the file
# only runs a command when its argv is exactly `serve`.
defmodule Ankusa.LifecycleHarness do
  @moduledoc false

  @instance :lifecycle_harness

  defmodule HoldSink do
    @moduledoc false
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: Process.sleep(:infinity)
  end

  @doc false
  def decoder(_source_id, _spec),
    do: [verifier: {Ankusa.Verifier.None, []}, sinks: [{Ankusa.Sink.Log, []}]]

  @doc "The instance config both the serving child and a reopening parent use."
  def config(data_dir, sinks) do
    Ankusa.Config.new(
      instance: @instance,
      data_dir: data_dir,
      port: 0,
      roles: [:edge],
      source_store: {Ankusa.SourceStore.Persistent, decoder: &__MODULE__.decoder/2},
      lifecycle: %{sinks: sinks}
    )
  end

  @doc "The source the child creates."
  def spec,
    do: %{"verify" => %{"type" => "none"}, "sinks" => [%{"type" => "log"}]}

  @doc "Boot the instance, make the change, print `PUT_DONE`, wait to be killed."
  def serve do
    config = config(System.fetch_env!("DATA_DIR"), [{HoldSink, []}])

    # stdout carries only `PUT_DONE`; log output goes to stderr.
    :ok = :logger.remove_handler(:default)
    :ok = :logger.add_handler(:default, :logger_std_h, %{config: %{type: :standard_error}})

    {:ok, _} = Application.ensure_all_started(:bandit)
    {:ok, _} = Registry.start_link(keys: :unique, name: Ankusa.Registry)
    Ankusa.put_config(config)
    {:ok, _} = Ankusa.Instance.start_link(config)

    {:ok, _stored} = Ankusa.SourceStore.put(@instance, "acme", "billing", spec(), :create)
    IO.puts("PUT_DONE")
    Process.sleep(:infinity)
  end
end

case System.argv() do
  ["serve"] -> Ankusa.LifecycleHarness.serve()
  _ -> :ok
end
