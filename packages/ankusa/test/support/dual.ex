defmodule Ankusa.Test.Dual do
  @moduledoc """
  Fixtures for `wal: :dual` tests: an instance over `Ankusa.Intake.Memory`, a
  source that dedupes on `x-event-id` and delivers to the test process, a fleet
  store whose claims a test can read, and the waits the asynchronous halves need.
  """

  import Ankusa.TestHelpers

  alias Ankusa.Dispatch.Pipeline
  alias Ankusa.Edge.Ingest
  alias Ankusa.Intake.Pending
  alias Ankusa.Queue

  defmodule CollectSink do
    @moduledoc false
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      send(Keyword.fetch!(opts, :pid), {:delivered, env.id})
      :ok
    end
  end

  defmodule FleetStub do
    @moduledoc false
    # A fleet store whose claims and settles a test can read: `{:promote, ...}` and
    # `{:release, ...}` ops are kept in the table in the order they were settled.
    @behaviour Ankusa.RateLimiter.Store
    @behaviour Ankusa.Dedupe.Store

    def child_spec(opts), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}

    @impl Ankusa.RateLimiter.Store
    def start_link(_opts), do: :ignore

    defp table(instance), do: Keyword.fetch!(elem(Ankusa.config(instance).fleet.store, 1), :table)

    @impl Ankusa.RateLimiter.Store
    def hit(_instance, _charges, nil, _timeout_ms), do: :ok

    def hit(instance, _charges, claim, _timeout_ms) do
      t = table(instance)
      key = {claim.tenant, claim.source_id, claim.key}

      if :ets.insert_new(t, {key, {:pending, claim.hook_id}}) do
        :ok
      else
        case :ets.lookup(t, key) do
          [{_key, {:committed, id}}] -> {:duplicate, id}
          _pending -> {:pending, 1_000}
        end
      end
    end

    @impl Ankusa.RateLimiter.Store
    def reset(_instance, _tenant, _timeout_ms), do: :ok

    @impl Ankusa.Dedupe.Store
    def settle(instance, ops, _timeout_ms) do
      t = table(instance)

      for op <- ops do
        :ets.insert(t, {{:op, System.unique_integer([:monotonic])}, op})

        case op do
          {:promote, claim, id} ->
            :ets.insert(t, {{claim.tenant, claim.source_id, claim.key}, {:committed, id}})

          {:release, claim} ->
            :ets.delete(t, {claim.tenant, claim.source_id, claim.key})
        end
      end

      :ok
    end
  end

  def dual_config(overrides) do
    test_config(
      [
        roles: [:edge],
        wal: :dual,
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{
             "load" => [
               verifier: {Ankusa.Verifier.None, []},
               sinks: [{CollectSink, [pid: self()]}],
               dedupe: %{header: "x-event-id"}
             ]
           }},
        intake: %{
          type: :memory,
          node_id: "node-a",
          probe_ms: 100,
          publish_timeout_ms: 3_000,
          reconcile_deadline_ms: 5_000
        },
        backup: %{enabled: true, interval_ms: 3_600_000, max_age_ms: 3_600_000}
      ] ++ overrides
    )
  end

  def start_dual(overrides \\ []) do
    config = dual_config(overrides)
    ExUnit.Callbacks.start_supervised!({Ankusa.Instance, config})
    config
  end

  def ingest(config, key, extra_headers \\ []) do
    headers = [{"x-event-id", key} | extra_headers]
    Ingest.ingest(config.instance, request("load", ~s({"k":"#{key}"}), headers))
  end

  def hook_ids(config) do
    {:ok, hooks} = Queue.hooks(config.instance, 0, 1_000)
    Enum.map(hooks, & &1.id)
  end

  def wait_until(fun, deadline_ms \\ 5_000) do
    cond do
      fun.() -> true
      deadline_ms <= 0 -> false
      true -> Process.sleep(10) && wait_until(fun, deadline_ms - 10)
    end
  end

  def pending(config), do: Pending.meta(Pending.table(config.instance), :records, 0)

  def drained(config), do: wait_until(fn -> pending(config) == 0 end)

  def deliver_all(config), do: Pipeline.tick(config.instance)

  def ops(table) do
    for {{:op, _}, op} <- :ets.tab2list(table) |> Enum.sort(), do: op
  end

  @doc """
  One hook straight into `Queue.enqueue/2`, past the batcher (whose partition runs
  one commit at a time: a second request on the partition of a held one would
  wait behind it). Returns the writer's per-item results.
  """
  def enqueue_direct(config) do
    env = %Ankusa.Envelope{
      id: Ankusa.UUIDv7.generate(),
      source_id: "load",
      tenant_id: "default",
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/webhooks/load",
      headers: [],
      body: "{}",
      size: 2
    }

    Queue.enqueue(config.instance, [%{envelope: env, sinks: [{CollectSink, [pid: self()]}]}])
  end
end
