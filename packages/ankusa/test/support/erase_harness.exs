# The erase harness: an instance that starts erasing a tenant and waits to be
# killed mid-erase, for `test/ankusa/tenant_test.exs` ("resumes after kill -9").
#
# It runs as its own OS process, with no Mix and no `:ankusa` application, so the
# process that kills it gets no chance to flush anything:
#
#     ERL_LIBS=<build>/lib DATA_DIR=/data elixir erase_harness.exs serve
#
# `serve` boots one `:edge` and `:dispatch` instance with a writable source store
# on `DATA_DIR`, creates a source for each of the tenants `acme` and `globex`,
# ingests three hooks into each and waits until dispatch has claimed all of them:
# every attempt is held by `HoldSink`, which never returns, so acme's claimed rows
# cannot settle. Then it starts erasing acme and prints `STAGE <stage>` whenever
# the job moves. The erase reaches its `queue` stage, waits there for the running
# attempts it can never get an answer from, and the parent SIGKILLs it on
# `STAGE queue`: a kill with claimed rows in the store and the erase half done.
#
# Required by `test_helper.exs` for `Ankusa.EraseHarness.config/2`: the file only
# runs a command when its argv is exactly `serve`.
defmodule Ankusa.EraseHarness do
  @moduledoc false

  @instance :erase_harness
  @tenants ["acme", "globex"]
  @hooks 3

  defmodule HoldSink do
    @moduledoc false
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: Process.sleep(:infinity)
  end

  @doc false
  def decoder(_source_id, _spec),
    do: [verifier: {Ankusa.Verifier.None, []}, sinks: [{HoldSink, []}]]

  @doc "The instance config both the serving child and a reopening parent use."
  def config(data_dir, extra \\ []) do
    [
      instance: @instance,
      data_dir: data_dir,
      port: 0,
      roles: [:edge, :dispatch],
      source_store: {Ankusa.SourceStore.Persistent, decoder: &__MODULE__.decoder/2},
      dispatch: %{
        retry:
          {Ankusa.RetryPolicy.Exponential, base_ms: 3_600_000, max_attempts: 10, jitter: false},
        breaker_failures: 0
      }
    ]
    |> Keyword.merge(extra)
    |> Ankusa.Config.new()
  end

  @doc "The hooks each tenant is given, and how many."
  def hooks, do: @hooks

  @doc "Boot the instance, make acme's rows inflight, start the erase, report its stages."
  def serve do
    config = config(System.fetch_env!("DATA_DIR"))

    # stdout carries only `CLAIMED` and `STAGE` lines; log output goes to stderr.
    :ok = :logger.remove_handler(:default)
    :ok = :logger.add_handler(:default, :logger_std_h, %{config: %{type: :standard_error}})

    {:ok, _} = Application.ensure_all_started(:bandit)
    {:ok, _} = Registry.start_link(keys: :unique, name: Ankusa.Registry)
    Ankusa.put_config(config)
    {:ok, _} = Ankusa.Instance.start_link(config)

    for tenant <- @tenants do
      {:ok, _} =
        Ankusa.SourceStore.put(@instance, tenant, "billing", %{"sink" => "hold"}, :create)
    end

    for tenant <- @tenants, n <- 1..@hooks do
      source_id = "#{tenant}.billing"

      {:ok, _env} =
        Ankusa.Edge.Ingest.ingest(@instance, %{
          source_id: source_id,
          method: "POST",
          path: "/webhooks/#{source_id}",
          headers: [],
          body: "hook #{n}"
        })
    end

    await_claimed()
    IO.puts("CLAIMED")

    {:ok, :started} = Ankusa.Tenant.delete(@instance, "acme", confirm: "acme")
    watch(nil)
  end

  defp await_claimed do
    case Ankusa.Queue.pending_for_tenant(@instance, "acme") do
      {:ok, %{inflight: @hooks}} ->
        :ok

      _not_yet ->
        Process.sleep(20)
        await_claimed()
    end
  end

  defp watch(last) do
    stage =
      case Ankusa.Tenant.status(@instance, "acme") do
        {:ok, %{state: :erasing, stage: stage}} -> stage
        other -> other
      end

    if stage != last, do: IO.puts("STAGE #{inspect(stage)}")
    Process.sleep(20)
    watch(stage)
  end
end

case System.argv() do
  ["serve"] -> Ankusa.EraseHarness.serve()
  _ -> :ok
end
