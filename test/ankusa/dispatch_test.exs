defmodule Ankusa.DispatchTest do
  use ExUnit.Case, async: false

  alias Ankusa.{Config, Envelope, WAL}
  alias Ankusa.Dispatch.{DLQ, Pipeline}

  # ── test sinks ─────────────────────────────────────────────────────────────

  defmodule CapturingSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, ctx, opts) do
      send(Keyword.fetch!(opts, :pid), {:delivered, env.id, ctx.attempt})
      :ok
    end
  end

  defmodule FlakySink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, ctx, opts) do
      agent = Keyword.fetch!(opts, :agent)
      succeed_at = Keyword.fetch!(opts, :succeed_at)
      n = Agent.get_and_update(agent, fn c -> {c + 1, c + 1} end)

      if n >= succeed_at do
        send(Keyword.fetch!(opts, :pid), {:delivered, env.id, ctx.attempt})
        :ok
      else
        {:error, {:transient, n}}
      end
    end
  end

  defmodule AlwaysFail do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: {:error, :always}
  end

  # ── helpers ────────────────────────────────────────────────────────────────

  defp build_env(source_id) do
    id = "evt_" <> Integer.to_string(System.unique_integer([:positive]))

    %Envelope{
      id: id,
      source_id: source_id,
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/hooks",
      headers: [],
      content_type: "application/json",
      body: "{}"
    }
  end

  defp start(sinks, opts) do
    inst = :"t#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{inst}")
    on_exit(fn -> File.rm_rf(dir) end)

    base = [
      instance: inst,
      data_dir: dir,
      roles: [:edge, :dispatch, :storage],
      source_store: {Ankusa.SourceStore.Static, sources: %{"src1" => %{sinks: sinks}}}
    ]

    base =
      case Keyword.get(opts, :dispatch) do
        nil -> base
        dispatch -> base ++ [dispatch: dispatch]
      end

    config = Config.new(base)
    Ankusa.put_config(config)
    start_supervised!({Ankusa.WAL.DiskLog, instance: inst, config: config})

    pipe_opts = [instance: inst, config: config] ++ Keyword.take(opts, [:max_sleep_ms])
    start_supervised!({Pipeline, pipe_opts})

    %{inst: inst, config: config}
  end

  # ── tests ──────────────────────────────────────────────────────────────────

  test "delivers committed envelopes in order and advances the cursor" do
    %{inst: inst} = start([{CapturingSink, pid: self()}], max_sleep_ms: 5)

    env = build_env("src1")
    {:ok, [{:committed, committed}]} = WAL.append(inst, [%{envelope: env}])

    assert {:ok, 1} = Pipeline.tick(inst)
    assert_receive {:delivered, id, 1}
    assert id == committed.id
    assert WAL.get_cursor(inst, :dispatch) == committed.seq
  end

  test "retries a failing sink until it succeeds" do
    {:ok, agent} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> if Process.alive?(agent), do: Agent.stop(agent) end)

    sinks = [{FlakySink, pid: self(), agent: agent, succeed_at: 3}]
    %{inst: inst} = start(sinks, max_sleep_ms: 5)

    env = build_env("src1")
    {:ok, [{:committed, committed}]} = WAL.append(inst, [%{envelope: env}])

    assert {:ok, 1} = Pipeline.tick(inst)
    # third attempt is the one that succeeds
    assert_receive {:delivered, _id, 3}
    assert WAL.get_cursor(inst, :dispatch) == committed.seq
  end

  test "dead-letters after exhausting the retry policy" do
    %{inst: inst, config: config} =
      start([{AlwaysFail, []}],
        max_sleep_ms: 5,
        dispatch: %{retry: {Ankusa.RetryPolicy.Exponential, max_attempts: 2}}
      )

    env = build_env("src1")
    {:ok, [{:committed, committed}]} = WAL.append(inst, [%{envelope: env}])

    assert {:ok, 1} = Pipeline.tick(inst)

    entries = DLQ.entries(config)
    assert [%{envelope: dead, reason: {:sink, AlwaysFail, :always}}] = entries
    assert dead.id == committed.id
    # still at-least-once: a fully handled (dead-lettered) envelope advances the cursor
    assert WAL.get_cursor(inst, :dispatch) == committed.seq
  end

  test "replay re-delivers dead-lettered hooks through the source sinks" do
    %{inst: inst, config: config} =
      start([{AlwaysFail, []}],
        max_sleep_ms: 5,
        dispatch: %{retry: {Ankusa.RetryPolicy.Exponential, max_attempts: 2}}
      )

    env = build_env("src1")
    {:ok, _} = WAL.append(inst, [%{envelope: env}])
    assert {:ok, 1} = Pipeline.tick(inst)
    assert [_] = DLQ.entries(config)

    # swap the source to a capturing sink so replay is observable
    replay_config =
      Config.new(
        instance: inst,
        data_dir: config.data_dir,
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{"src1" => %{sinks: [{CapturingSink, pid: self()}]}}}
      )

    Ankusa.put_config(replay_config)

    assert Ankusa.Dispatch.replay(inst, source_id: "src1") == 1
    assert_receive {:delivered, id, 1}
    assert id == env.id
  end
end
