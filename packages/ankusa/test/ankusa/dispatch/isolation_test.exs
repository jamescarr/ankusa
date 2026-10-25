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

  # Holds its slot for `ms` and counts, in `atomics` (1: running now, 2: the
  # most that ever ran at once), how many attempts overlapped.
  defmodule CountingSleepSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, opts) do
      ref = Keyword.fetch!(opts, :atomics)
      bump_max(ref, :atomics.add_get(ref, 1, 1))
      Process.sleep(Keyword.fetch!(opts, :ms))
      :atomics.sub(ref, 1, 1)
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

  # Tells the test when a hook arrived, on the monotonic clock.
  defmodule StampSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      send(Keyword.fetch!(opts, :pid), {:stamp, env.id, System.monotonic_time(:microsecond)})
      :ok
    end
  end

  # Holds every attempt until the agent says `:open`.
  defmodule HoldSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      send(Keyword.fetch!(opts, :pid), {:held, env.id})
      wait(Keyword.fetch!(opts, :agent))
    end

    defp wait(agent) do
      if Agent.get(agent, & &1) == :open do
        :ok
      else
        Process.sleep(5)
        wait(agent)
      end
    end
  end

  # A batching sink that holds each attempt until the test says go, and tells it
  # whether dispatch said it may batch.
  defmodule BatchedHoldSink do
    @behaviour Ankusa.Sink

    @impl true
    def batched?(_opts), do: true

    @impl true
    def deliver(env, ctx, opts) do
      send(Keyword.fetch!(opts, :pid), {:held, env.id, ctx[:batch], self()})
      id = env.id

      receive do
        {:go, ^id} -> :ok
      after
        10_000 -> {:error, :gate_timeout}
      end
    end
  end

  # Fails for the tenant `bad`, delivers for everyone else, as a batching sink.
  defmodule BatchedTenantSink do
    @behaviour Ankusa.Sink

    @impl true
    def batched?(_opts), do: true

    @impl true
    def deliver(%{tenant_id: "bad"}, _ctx, _opts), do: {:error, :down}

    def deliver(env, _ctx, opts) do
      send(Keyword.fetch!(opts, :pid), {:delivered, env.tenant_id, env.id})
      :ok
    end
  end

  # Reports the ctx it was given.
  defmodule CtxSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, ctx, opts) do
      send(Keyword.fetch!(opts, :pid), {:ctx, ctx})
      :ok
    end
  end

  # Fails for the tenant `bad`, delivers for everyone else.
  defmodule TenantSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(%{tenant_id: "bad"}, _ctx, _opts), do: {:error, :down}

    def deliver(%{tenant_id: "slow"}, _ctx, opts) do
      Process.sleep(Keyword.fetch!(opts, :ms))
      :ok
    end

    def deliver(env, _ctx, opts) do
      send(Keyword.fetch!(opts, :pid), {:delivered, env.tenant_id, env.id})
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

  defp build_env(source_id, tenant \\ "acme") do
    %Envelope{
      id: "evt_" <> Integer.to_string(System.unique_integer([:positive])),
      source_id: source_id,
      tenant_id: tenant,
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
      assert {:ok, at} = Deliveries.next_due_at(inst)
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

  # What dispatch does for a sink that batches (`c:Ankusa.Sink.batched?/1`): its
  # attempts wait for a batch's response, so they are not counted against the
  # concurrency limits, and each still settles its own row.
  describe "batching sinks" do
    defp dispatch_state(inst), do: :sys.get_state(Ankusa.whereis(inst, :dispatch))

    test "attempts are not capped by sink_concurrency, a tenant's concurrency or concurrency" do
      inst =
        start(
          %{"b" => %{sinks: [{BatchedHoldSink, pid: self()}]}},
          %{concurrency: 2, sink_concurrency: 1, tenants: %{default: %{concurrency: 1}}}
        )

      ids = for _ <- 1..6, do: enqueue!(inst, build_env("b")).id

      # all six are in flight at once with every limit at 1 or 2, and each
      # was told it may wait in a batcher
      held =
        for _ <- 1..6 do
          assert_receive {:held, id, true, task}, 5_000
          {id, task}
        end

      assert Enum.sort(Enum.map(held, &elem(&1, 0))) == Enum.sort(ids)
      assert map_size(dispatch_state(inst).running) == 6

      for {id, task} <- held, do: send(task, {:go, id})
      assert {:ok, _} = Pipeline.tick(inst)
      assert map_size(dispatch_state(inst).running) == 0
    end

    test "an attempt that waits too long times out alone, and frees only its own row" do
      inst =
        start(
          %{"b" => %{sinks: [{BatchedHoldSink, pid: self()}]}},
          %{concurrency: 1, attempt_timeout_ms: 100, retry: retry(max_attempts: 1)}
        )

      %{seq: seq} = enqueue!(inst, build_env("b"))
      assert_receive {:held, _id, true, _task}, 5_000

      assert {:ok, 1} = Pipeline.tick(inst)
      assert [%{state: :dead, attempts: 1}] = rows(inst, [seq])
      assert map_size(dispatch_state(inst).running) == 0
    end

    test "a plain sink beside it is still capped, and gets no batch flag" do
      inst =
        start(
          %{"p" => %{sinks: [{CtxSink, pid: self()}]}},
          %{concurrency: 4, sink_concurrency: 1}
        )

      enqueue!(inst, build_env("p"))
      assert_receive {:ctx, ctx}, 5_000
      refute Map.has_key?(ctx, :batch)
    end

    test "one tenant's open breaker leaves the other tenant's entries alone" do
      inst =
        start(
          %{"b" => %{sinks: [{BatchedTenantSink, pid: self()}]}},
          %{breaker_failures: 2, breaker_open_ms: 60_000, retry: retry(max_attempts: 10)}
        )

      for _ <- 1..3, do: enqueue!(inst, build_env("b", "bad"))
      Pipeline.tick(inst)

      assert map_size(dispatch_state(inst).breakers) == 1

      %{id: id} = enqueue!(inst, build_env("b", "good"))
      Pipeline.tick(inst)
      assert_receive {:delivered, "good", ^id}, 5_000
    end
  end

  describe "per-tenant scheduling" do
    # Commits `envs` as a few batches: a backlog in one call, not one fsync each.
    defp enqueue_many!(inst, envs) do
      for chunk <- Enum.chunk_every(envs, 256) do
        items =
          Enum.map(chunk, fn env ->
            {:ok, source} = Ankusa.SourceStore.fetch(inst, env.source_id)
            %{envelope: env, sinks: source.sinks}
          end)

        {:ok, _} = Ankusa.Queue.enqueue(inst, items)
      end

      :ok
    end

    defp claimed_rows(inst, ord) do
      case :sys.get_state(Ankusa.whereis(inst, :dispatch)).tenant_claimed do
        %{^ord => {rows, _bytes, _tenant}} -> rows
        _ -> 0
      end
    end

    # The p99 of `n` hooks of `tenant` committed 15 ms apart: milliseconds from
    # just before the commit to the sink call. `sample` runs once per hook.
    defp light_p99(inst, tenant, n, sample) do
      starts =
        for _ <- 1..n do
          env = build_env("light", tenant)
          t0 = System.monotonic_time(:microsecond)
          enqueue!(inst, env)
          sample.()
          Process.sleep(15)
          {env.id, t0}
        end
        |> Map.new()

      lags =
        for _ <- 1..n do
          assert_receive {:stamp, id, t1}, 10_000
          (t1 - Map.fetch!(starts, id)) / 1000
        end

      lags |> Enum.sort() |> Enum.at(ceil(0.99 * n) - 1)
    end

    # Ground rule 4's check, on dispatch: a tenant with 99 times another's load,
    # whose sink is slow, must not move the light tenant's p99 by more than 2x
    # (its solo p99 is a few milliseconds, so the bound has a floor of 25 ms: at
    # that scale a scheduler hiccup is the whole difference), and it must never
    # hold more than its own window.
    test "tenant fairness" do
      window = 16
      atomics = :atomics.new(2, [])

      sources = %{
        "heavy" => %{sinks: [{CountingSleepSink, ms: 150, atomics: atomics}]},
        "light" => %{sinks: [{StampSink, pid: self()}]}
      }

      dispatch = %{
        concurrency: 8,
        sink_concurrency: 4,
        max_inflight: 64,
        batch: 16,
        tenants: %{default: %{max_inflight: window, concurrency: 4}}
      }

      solo = start(sources, dispatch)
      solo_p99 = light_p99(solo, "light-tenant", 24, fn -> :ok end)

      inst = start(sources, dispatch)
      light = 24
      heavy = 99 * light
      enqueue_many!(inst, for(_ <- 1..heavy, do: build_env("heavy", "heavy-tenant")))
      {:ok, ord} = Ankusa.Queue.Tenants.ord(inst, "heavy-tenant")

      # the heavy tenant has filled its window and is queueing behind its slots
      await(fn -> claimed_rows(inst, ord) == window end)

      samples = :ets.new(:window_samples, [:public])
      sample = fn -> :ets.insert(samples, {System.unique_integer(), claimed_rows(inst, ord)}) end

      mixed_p99 = light_p99(inst, "light-tenant", light, sample)

      widest = samples |> :ets.tab2list() |> Enum.map(&elem(&1, 1)) |> Enum.max()
      assert widest == window, "the heavy tenant never filled its window (#{widest})"

      # A never exceeds its window, nor its concurrency (4 per sink and per tenant)
      assert :atomics.get(atomics, 2) in 2..4

      assert mixed_p99 <= max(2 * solo_p99, 25),
             "light tenant p99 #{mixed_p99} ms against #{solo_p99} ms alone"
    end

    defp await(fun, tries \\ 400) do
      cond do
        fun.() -> :ok
        tries == 0 -> flunk("condition never became true")
        true -> Process.sleep(10) && await(fun, tries - 1)
      end
    end

    test "a page is shared across the active tenants, not taken by the oldest" do
      inst =
        start(
          %{"s" => %{sinks: [{TagSink, tag: :s, pid: self()}]}},
          %{concurrency: 1, max_inflight: 8, batch: 8}
        )

      # Both tenants are visible to the first scan: the pipeline waits.
      pipeline = Ankusa.whereis(inst, :dispatch)
      :ok = :sys.suspend(pipeline)
      enqueue_many!(inst, for(_ <- 1..40, do: build_env("s", "x")))
      %{id: y} = enqueue!(inst, build_env("s", "y"))
      :ok = :sys.resume(pipeline)

      # A window of 8 and a page of 8: oldest-first would claim eight of x's and
      # leave y for after them. One at a time, y is the second to run.
      first =
        for _ <- 1..3 do
          assert_receive {:delivered, :s, id, 1}, 5_000
          id
        end

      assert y in first
    end

    test "a breaker is per tenant: one tenant's failing sink does not park another's" do
      inst =
        start(
          %{"s" => %{sinks: [{TenantSink, pid: self()}]}},
          %{breaker_failures: 2, breaker_open_ms: 60_000, breaker_max_open_ms: 60_000}
        )

      handler = "breaker-#{System.unique_integer([:positive])}"
      parent = self()

      :ok =
        :telemetry.attach(
          handler,
          [:ankusa, :dispatch, :breaker],
          fn _event, _measure, meta, _config -> send(parent, {:breaker, meta.state}) end,
          nil
        )

      on_exit(fn -> :telemetry.detach(handler) end)

      for _ <- 1..4, do: enqueue!(inst, build_env("s", "bad"))
      assert_receive {:breaker, :open}, 5_000

      %{id: id} = enqueue!(inst, build_env("s", "good"))
      assert_receive {:delivered, "good", ^id}, 2_000
    end

    test "sink_concurrency is per tenant: a tenant's backlog on a slow sink does not hold another's" do
      inst =
        start(
          %{"s" => %{sinks: [{TenantSink, pid: self(), ms: 10_000}]}},
          %{concurrency: 4, sink_concurrency: 2, attempt_timeout_ms: 20_000}
        )

      # "slow" occupies its two slots and queues eighteen more behind them
      for _ <- 1..20, do: enqueue!(inst, build_env("s", "slow"))
      %{id: id} = enqueue!(inst, build_env("s", "fast"))

      assert_receive {:delivered, "fast", ^id}, 1_000
    end

    test "a tenant's window caps its claimed rows, and the gauge reports it" do
      {:ok, agent} = Agent.start_link(fn -> :closed end)

      inst =
        start(
          %{"s" => %{sinks: [{HoldSink, pid: self(), agent: agent}]}},
          %{
            concurrency: 8,
            max_inflight: 64,
            tenants: %{:default => %{max_inflight: 8}, "capped" => %{max_inflight: 3}}
          }
        )

      handler = "tenant-inflight-#{System.unique_integer([:positive])}"
      parent = self()

      :ok =
        :telemetry.attach(
          handler,
          [:ankusa, :dispatch, :tenant_state],
          fn _event, measure, meta, _config ->
            send(parent, {:tenant_inflight, meta.tenant, measure.inflight})
          end,
          nil
        )

      on_exit(fn -> :telemetry.detach(handler) end)

      enqueue_many!(inst, for(_ <- 1..12, do: build_env("s", "capped")))
      enqueue_many!(inst, for(_ <- 1..12, do: build_env("s", "open")))

      # eight attempts run at once (three of "capped", five of "open": its own
      # window is eight but the global concurrency is shared), and no more rows
      # are claimed for "capped" than its override allows
      for _ <- 1..8, do: assert_receive({:held, _}, 5_000)
      {:ok, ord} = Ankusa.Queue.Tenants.ord(inst, "capped")
      assert claimed_rows(inst, ord) == 3
      assert_receive {:tenant_inflight, "capped", 3}, 3_000

      # once it drains, the tenant is reported once as 0
      Agent.update(agent, fn _ -> :open end)
      assert {:ok, _} = Pipeline.tick(inst)
      assert_receive {:tenant_inflight, "capped", 0}, 3_000
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
