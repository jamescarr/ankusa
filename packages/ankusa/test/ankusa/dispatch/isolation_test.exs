defmodule Ankusa.Dispatch.IsolationTest do
  @moduledoc """
  The pipeline's per-sink scheduling, circuit breakers, sink error classes and
  an unavailable source store, end to end against `Ankusa.Instance`.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Dispatch.Pipeline
  alias Ankusa.Envelope
  alias Ankusa.Queue.Deliveries

  @moduletag capture_log: true

  defmodule ReplySink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, opts), do: Keyword.fetch!(opts, :reply)
  end

  defmodule TagSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, ctx, opts) do
      send(
        Keyword.fetch!(opts, :pid),
        {:delivered, Keyword.fetch!(opts, :tag), env.id, ctx.attempt}
      )

      :ok
    end
  end

  defmodule SleepSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, opts) do
      Process.sleep(Keyword.fetch!(opts, :ms))
      :ok
    end
  end

  # Fails while its agent holds `:fail`; delivers once the test flips it.
  defmodule SwitchSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      case Agent.get(Keyword.fetch!(opts, :agent), & &1) do
        :ok ->
          send(Keyword.fetch!(opts, :pid), {:delivered, env.id})
          :ok

        :fail ->
          {:error, :down}

        :gone ->
          {:error, {:permanent, :gone}}
      end
    end
  end

  # A source store backed by something that can be down.
  defmodule OutageStore do
    @behaviour Ankusa.SourceStore

    def set(instance, state), do: :persistent_term.put({__MODULE__, instance}, state)

    @impl true
    def fetch(instance, source_id) do
      %Ankusa.Config{source_store: {_mod, opts}} = Ankusa.config(instance)

      case :persistent_term.get({__MODULE__, instance}, :up) do
        :down ->
          {:error, :unavailable}

        :up ->
          case Map.fetch(Keyword.fetch!(opts, :sources), source_id) do
            {:ok, spec} -> {:ok, Ankusa.Source.new(source_id, spec)}
            :error -> :error
          end
      end
    end

    @impl true
    def list(instance) do
      %Ankusa.Config{source_store: {_mod, opts}} = Ankusa.config(instance)
      Map.keys(Keyword.fetch!(opts, :sources))
    end
  end

  defp retry(overrides \\ []) do
    {Ankusa.RetryPolicy.Exponential,
     Keyword.merge([base_ms: 0, max_attempts: 10, jitter: false], overrides)}
  end

  defp build_env(source_id) do
    %Envelope{
      id: "evt_" <> Integer.to_string(System.unique_integer([:positive])),
      source_id: source_id,
      tenant_id: "acme",
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/hooks",
      headers: [],
      content_type: "application/json",
      body: "{}",
      size: 2
    }
  end

  defp start(sources, dispatch, opts \\ []) do
    store = Keyword.get(opts, :store, Ankusa.SourceStore.Static)

    config =
      test_config(
        roles: [:edge, :dispatch],
        source_store: {store, sources: sources},
        dispatch: Map.merge(%{retry: retry()}, dispatch)
      )

    start_supervised!({Ankusa.Instance, config})
    config.instance
  end

  defp rows(inst, seqs) do
    Enum.map(seqs, fn seq ->
      {:ok, row} = Deliveries.row(inst, seq, 0)
      row
    end)
  end

  describe "sink error classes" do
    test "{:permanent, _} is dead-lettered after one attempt, whatever the policy has left" do
      inst =
        start(
          %{"s" => %{sinks: [{ReplySink, reply: {:error, {:permanent, :gone}}}]}},
          %{retry: retry(max_attempts: 5)}
        )

      %{seq: seq} = enqueue!(inst, build_env("s"))
      assert {:ok, _} = Pipeline.tick(inst)

      assert [%{state: :dead, attempts: 1, error: error}] = rows(inst, [seq])
      assert error =~ ":permanent"
    end

    test "{:retry_after, ms, _} is retried no sooner than ms" do
      inst =
        start(
          %{"s" => %{sinks: [{ReplySink, reply: {:error, {:retry_after, 60_000, :slow}}}]}},
          %{}
        )

      before = System.system_time(:millisecond)
      %{seq: seq} = enqueue!(inst, build_env("s"))
      assert {:ok, 0} = Pipeline.tick(inst)

      assert [%{state: :pending, attempts: 1}] = rows(inst, [seq])
      assert {:ok, at} = Deliveries.next_due_at(inst, 0)
      assert at >= before + 60_000
    end
  end

  describe "per-sink isolation" do
    test "a source whose sink hangs leaves slots for another source's sink" do
      inst =
        start(
          %{
            "slow" => %{sinks: [{SleepSink, ms: 10_000}]},
            "fast" => %{sinks: [{TagSink, tag: :fast, pid: self()}]}
          },
          %{concurrency: 4, sink_concurrency: 2, attempt_timeout_ms: 2_000}
        )

      for _ <- 1..20, do: enqueue!(inst, build_env("slow"))
      %{id: id} = enqueue!(inst, build_env("fast"))

      assert_receive {:delivered, :fast, ^id, 1}, 1_000
    end
  end

  describe "circuit breaker" do
    setup do
      {:ok, agent} = Agent.start_link(fn -> :fail end)
      %{agent: agent}
    end

    test "opens after breaker_failures, parks rows without spending attempts, probes, closes",
         %{agent: agent} do
      inst =
        start(
          %{"s" => %{sinks: [{SwitchSink, agent: agent, pid: self()}]}},
          %{concurrency: 1, breaker_failures: 3, breaker_open_ms: 500, breaker_max_open_ms: 1_000}
        )

      seqs = for _ <- 1..10, do: enqueue!(inst, build_env("s")).seq
      opened_at = System.system_time(:millisecond)
      assert {:ok, 0} = Pipeline.tick(inst)

      # Exactly three attempts ran; everything else waits for the breaker.
      first = rows(inst, seqs)
      assert Enum.sum_by(first, & &1.attempts) == 3
      assert Enum.count(first, &(&1.attempts == 1)) == 3
      assert Enum.count(first, &(&1.attempts == 0)) == 7
      assert Enum.all?(first, &(&1.state == :pending and &1.at >= opened_at + 400))

      # Half-open: one probe, which fails and opens it again.
      Process.sleep(600)
      assert {:ok, 0} = Pipeline.tick(inst)
      assert Enum.sum_by(rows(inst, seqs), & &1.attempts) == 4

      # The sink recovers: the next probe closes the breaker, and every row is
      # delivered once its parking is over.
      Agent.update(agent, fn _ -> :ok end)
      Process.sleep(1_100)
      assert {:ok, _} = Pipeline.tick(inst)
      Process.sleep(600)
      assert {:ok, _} = Pipeline.tick(inst)

      for _ <- seqs, do: assert_receive({:delivered, _id}, 1_000)
      refute_receive {:delivered, _id}
      assert stored_ids(inst) == []
    end

    test "breaker_failures: 0 never parks: every row is attempted until it dead-letters",
         %{agent: agent} do
      inst =
        start(
          %{"s" => %{sinks: [{SwitchSink, agent: agent, pid: self()}]}},
          %{concurrency: 1, breaker_failures: 0, retry: retry(max_attempts: 3)}
        )

      seqs = for _ <- 1..6, do: enqueue!(inst, build_env("s")).seq
      assert {:ok, _} = Pipeline.tick(inst)
      assert Enum.all?(rows(inst, seqs), &(&1.state == :dead and &1.attempts == 3))
    end

    test "a {:permanent, _} failure never opens the breaker" do
      inst =
        start(
          %{"s" => %{sinks: [{ReplySink, reply: {:error, {:permanent, :gone}}}]}},
          %{concurrency: 1, breaker_failures: 2}
        )

      seqs = for _ <- 1..5, do: enqueue!(inst, build_env("s")).seq
      assert {:ok, _} = Pipeline.tick(inst)
      assert Enum.all?(rows(inst, seqs), &(&1.state == :dead and &1.attempts == 1))
    end

    test "a {:permanent, _} answer to the probe closes the breaker", %{agent: agent} do
      inst =
        start(
          %{"s" => %{sinks: [{SwitchSink, agent: agent, pid: self()}]}},
          %{concurrency: 1, breaker_failures: 2, breaker_open_ms: 300, breaker_max_open_ms: 300}
        )

      seqs = for _ <- 1..5, do: enqueue!(inst, build_env("s")).seq
      assert {:ok, 0} = Pipeline.tick(inst)
      assert Enum.count(rows(inst, seqs), &(&1.attempts == 0)) == 3

      # The destination is back, and refuses these hooks for good: the probe's
      # answer closes the breaker, so every parked row runs and dead-letters
      # instead of being parked again behind a probe that already finished.
      Agent.update(agent, fn _ -> :gone end)
      Process.sleep(350)
      assert {:ok, _} = Pipeline.tick(inst)
      Process.sleep(350)
      assert {:ok, _} = Pipeline.tick(inst)

      assert Enum.all?(rows(inst, seqs), &(&1.state == :dead))
    end
  end

  describe "an unavailable source store" do
    test "reschedules the row without spending an attempt, and delivers once it answers" do
      sources = %{"s" => %{sinks: [{TagSink, tag: :s, pid: self()}]}}
      inst = start(sources, %{}, store: OutageStore)
      on_exit(fn -> :persistent_term.erase({OutageStore, inst}) end)

      OutageStore.set(inst, :down)
      env = build_env("s")

      {:ok, [{:committed, %{seq: seq}}]} =
        Ankusa.Queue.enqueue(inst, [%{envelope: env, sinks: [{TagSink, tag: :s, pid: self()}]}])

      assert {:ok, 0} = Pipeline.tick(inst)
      refute_received {:delivered, :s, _, _}
      assert [%{state: :pending, attempts: 0}] = rows(inst, [seq])

      OutageStore.set(inst, :up)
      Process.sleep(1_100)
      assert {:ok, _} = Pipeline.tick(inst)

      id = env.id
      assert_receive {:delivered, :s, ^id, 1}
    end
  end
end
