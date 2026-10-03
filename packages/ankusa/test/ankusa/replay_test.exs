defmodule Ankusa.ReplayTest do
  @moduledoc """
  The replay engine end to end: pacing, lag guard, idempotent create, restart
  resume, auto-pause, the created_at cutoff, and archive redrive. Runs against
  real instances, following the `dispatch_test.exs` inline-sink style.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.{Envelope, Replay, Store, UUIDv7}
  alias Ankusa.Dispatch.Pipeline
  alias Ankusa.Queue.Deliveries
  alias Ankusa.Storage.Compactor
  alias Ankusa.Store.Keys

  defmodule RecordSink do
    @moduledoc "Records `{id, replay_id}` to the test pid and confirms."

    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, ctx, opts) do
      send(Keyword.fetch!(opts, :pid), {:replayed, env.id, ctx[:replay_id]})
      :ok
    end
  end

  defmodule AlwaysFail do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: {:error, :always}
  end

  # Holds a live delivery (sleeping in the task) so the pipeline's oldest-due
  # lag can be forced past a job's `max_lag_ms`.
  defmodule GateSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, ctx, opts) do
      send(Keyword.fetch!(opts, :pid), {:gated, env.id})
      Process.sleep(Keyword.get(opts, :sleep_ms, 0))
      :ok
    end
  end

  defp start(sources, overrides \\ []) do
    config =
      test_config(
        Keyword.merge(
          [
            roles: [:edge, :dispatch],
            source_store: {Ankusa.SourceStore.Static, sources: sources},
            dispatch: %{
              retry: {Ankusa.RetryPolicy.Exponential, base_ms: 0, max_attempts: 1, jitter: false}
            }
          ],
          overrides
        )
      )

    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp build_env(source_id) do
    %Envelope{
      id: UUIDv7.generate(),
      source_id: source_id,
      tenant_id: "default",
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/hooks/#{source_id}",
      headers: [],
      content_type: nil,
      body: "body",
      size: 4
    }
  end

  # A dead row straight into the store, as a commit at `at` would leave it.
  defp write_dead!(inst, seq, env, module, at) do
    row = %{module: module, state: :dead, attempts: 2, at: at, error: "x", size: env.size}

    :ok =
      Store.write(
        inst,
        [
          {:put, :hooks, Keys.hook(seq), Envelope.to_binary(env)},
          {:put, :deliveries, Keys.delivery(seq, 0), Deliveries.encode_row(row)},
          {:put, :index, Keys.dead(at, seq, 0), :erlang.term_to_binary({env.source_id, env.id})}
        ],
        sync: true
      )
  end

  defp dead_rows(inst, count, module \\ RecordSink) do
    at = System.system_time(:millisecond) - 60_000

    # Seqs far above the writer's own `next_seq`, so no later enqueue can
    # collide with these fabricated rows.
    for i <- 1..count do
      env = build_env("src")
      write_dead!(inst, 1_000_000 + i, env, module, at)
      env
    end
  end

  defp poll(fun, deadline_ms \\ 10_000) do
    poll_until(fun, System.monotonic_time(:millisecond) + deadline_ms)
  end

  defp poll_until(fun, deadline) do
    case fun.() do
      {:ok, value} ->
        value

      :retry ->
        if System.monotonic_time(:millisecond) > deadline do
          flunk("poll timed out")
        else
          Process.sleep(50)
          poll_until(fun, deadline)
        end
    end
  end

  test "pacing: a job moves dead rows at its rate and delivers each once with replay_id" do
    config = start(%{"src" => [sinks: [{RecordSink, pid: self()}]]})
    inst = config.instance
    envs = dead_rows(inst, 500)

    assert {:ok, :created, job} = Replay.start(inst, %{kind: :dlq, rate: 100})
    assert job.state == :running

    Process.sleep(1_000)

    {:ok, midway} = Replay.get(inst, job.id)
    assert midway.moved in 60..160

    # One more second: the burst cap keeps any window at the rate plus one
    # tick's worth (100 + 20), never a full second of burst on top.
    Process.sleep(1_000)
    {:ok, later} = Replay.get(inst, job.id)
    assert (later.moved - midway.moved) in 60..140

    # Eventually everything is delivered exactly once, each with the job's id.
    final =
      poll(
        fn ->
          case Replay.get(inst, job.id) do
            {:ok, %{state: :done, moved: 500}} -> {:ok, :done}
            _ -> :retry
          end
        end,
        30_000
      )

    assert final == :done

    ids = MapSet.new(envs, & &1.id)

    received =
      for _ <- 1..500 do
        assert_receive {:replayed, id, replay_id}, 2_000
        assert MapSet.member?(ids, id)
        assert replay_id == job.id
        id
      end

    assert MapSet.new(received) == ids
    refute_received {:replayed, _, _}

    job =
      poll_job_state(inst, job.id, fn job ->
        job.state == :done and job.delivered == 500
      end)

    assert job.moved == 500
  end

  defp poll_job_state(inst, id, fun, deadline_ms \\ 30_000) do
    poll_job_until(inst, id, fun, System.monotonic_time(:millisecond) + deadline_ms)
  end

  defp poll_job_until(inst, id, fun, deadline) do
    job =
      case Replay.get(inst, id) do
        {:ok, job} -> job
        _other -> nil
      end

    if job != nil and fun.(job) do
      job
    else
      if System.monotonic_time(:millisecond) > deadline do
        flunk("replay job #{id} never reached the expected state")
      else
        Process.sleep(50)
        poll_job_until(inst, id, fun, deadline)
      end
    end
  end

  test "rate 1 still moves rows (one item per tick)" do
    config = start(%{"src" => [sinks: [{RecordSink, pid: self()}]]})
    inst = config.instance
    envs = dead_rows(inst, 5)

    assert {:ok, :created, job} = Replay.start(inst, %{kind: :dlq, rate: 1})

    job =
      poll_job_state(inst, job.id, fn job -> job.state == :done and job.moved == 5 end, 20_000)

    assert job.state == :done

    ids = MapSet.new(envs, & &1.id)

    for _ <- 1..5 do
      assert_receive {:replayed, id, _rid}, 2_000
      assert MapSet.member?(ids, id)
    end
  end

  test "lag guard: a job throttles while live deliveries lag past its max_lag_ms" do
    # Live traffic: one sink whose delivery blocks the only slot, so live rows
    # pile up due behind it and the oldest-due lag grows without bound.
    config =
      start(%{"live" => [sinks: [{GateSink, pid: self(), sleep_ms: 10_000}]]},
        dispatch: %{
          # One task at a time and a window of two: the pipeline claims two
          # live rows (one running, one queued) and the rest stay due, so the
          # oldest-due lag grows past `max_lag_ms`.
          concurrency: 1,
          max_inflight: 2,
          retry: {Ankusa.RetryPolicy.Exponential, base_ms: 0, max_attempts: 1, jitter: false}
        }
      )

    inst = config.instance
    dead_rows(inst, 200, GateSink)

    for _ <- 1..50 do
      {:ok, _} =
        Ankusa.Queue.enqueue(inst, [
          %{envelope: build_env("live"), sinks: [{GateSink, pid: self(), sleep_ms: 10_000}]}
        ])
    end

    handler = "replay-throttle-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler,
        [:ankusa, :replay, :throttled],
        fn _event, _measurements, meta, pid -> send(pid, {:throttled, meta}) end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler) end)

    assert {:ok, :created, job} = Replay.start(inst, %{kind: :dlq, rate: 1_000, max_lag_ms: 100})

    # The live rows are due and their sink blocks: lag exceeds 100 ms quickly,
    # so the job stops moving and reports :lag.
    assert_receive {:throttled, %{replay_id: id, reason: :lag}}, 5_000
    assert id == job.id

    {:ok, frozen} = Replay.get(inst, job.id)
    moved_once = frozen.moved
    Process.sleep(600)
    {:ok, still} = Replay.get(inst, job.id)
    assert still.moved == moved_once

    # Clean up the stuck live rows so the test can finish: kill the instance.
    Process.exit(Ankusa.whereis(inst, :instance), :kill)
  end

  test "idempotent create: the same spec twice is :created then :existing" do
    config = start(%{"src" => [sinks: [{RecordSink, pid: self()}]]})
    inst = config.instance
    dead_rows(inst, 5)

    spec = %{kind: :dlq, source_id: "src", rate: 500}

    assert {:ok, :created, job1} = Replay.start(inst, spec)
    assert {:ok, :existing, job2} = Replay.start(inst, spec)
    assert job1.id == job2.id

    # A different filter is a different job.
    assert {:ok, :created, job3} = Replay.start(inst, %{kind: :dlq, source_id: "other"})
    assert job3.id != job1.id
  end

  test "restart resume: a stopped instance resumes from the job's cursor" do
    config = start(%{"src" => [sinks: [{RecordSink, pid: self()}]]})
    inst = config.instance
    envs = dead_rows(inst, 300)

    assert {:ok, :created, job} = Replay.start(inst, %{kind: :dlq, rate: 100})

    # Stop the whole instance around halfway.
    poll(fn ->
      case Replay.get(inst, job.id) do
        {:ok, %{moved: moved}} when moved >= 150 -> {:ok, moved}
        _ -> :retry
      end
    end)

    :ok = Supervisor.stop(Ankusa.whereis(inst, :instance))

    # The test supervisor restarts the instance (permanent child); wait for the
    # replayer to be loaded again.
    poll(fn ->
      case Replay.list(inst) do
        {:ok, [%{id: id} | _]} when id == job.id -> {:ok, :loaded}
        _ -> :retry
      end
    end)

    # Everything still gets delivered exactly once and the job finishes.
    poll(
      fn ->
        case Replay.get(inst, job.id) do
          {:ok, %{state: :done, moved: 300}} -> {:ok, :done}
          _ -> :retry
        end
      end,
      30_000
    )

    ids = MapSet.new(envs, & &1.id)

    received =
      for _ <- 1..300 do
        assert_receive {:replayed, id, replay_id}, 2_000
        assert MapSet.member?(ids, id)
        assert replay_id == job.id
        id
      end

    assert MapSet.new(received) == ids
    assert {:ok, %{total: 0}} = Ankusa.Queue.dead(inst, limit: 10)
  end

  test "auto-pause: a job whose deliveries all dead-letter again pauses itself" do
    config = start(%{"src" => [sinks: [{AlwaysFail, []}]]})
    inst = config.instance

    # 500 rows at rate 100 keeps the job running for ~5 s, so the auto-pause
    # (which needs >= 100 dead outcomes while the job is still running) cannot
    # race the job's completion.
    dead_rows(inst, 500, AlwaysFail)

    assert {:ok, :created, job} = Replay.start(inst, %{kind: :dlq, rate: 100})

    paused =
      poll_job_state(inst, job.id, fn job -> job.state == :paused end, 20_000)

    assert paused.error =~ "auto-paused"
    assert paused.error =~ "replayed deliveries dead-lettered again"
    assert paused.dead >= 100
    assert paused.moved < 500
  end

  test "rows that die again after the job's created_at are never picked up by that job" do
    config = start(%{"src" => [sinks: [{RecordSink, pid: self()}]]})
    inst = config.instance
    dead_rows(inst, 10)

    assert {:ok, :created, job} = Replay.start(inst, %{kind: :dlq, rate: 10_000})

    # Rows that die at or after the job's creation time are beyond its `upto`
    # bound, so the same job never touches them.
    for seq <- 100..104 do
      env = build_env("src")
      write_dead!(inst, seq, env, AlwaysFail, job.created_at + 1)
    end

    poll_job_state(inst, job.id, fn job -> job.state == :done end)

    {:ok, job} = Replay.get(inst, job.id)
    assert job.moved == 10

    # The five late rows are still dead.
    assert {:ok, %{total: 5}} = Ankusa.Queue.dead(inst, limit: 10)
  end

  test "a dead row that does not decode stays in the DLQ and does not stall the job" do
    config = start(%{"src" => [sinks: [{RecordSink, pid: self()}]]})
    inst = config.instance
    dead_rows(inst, 3)

    at = System.system_time(:millisecond) - 60_000
    bad = build_env("src")
    bad_key = Keys.dead(at, 999, 0)

    :ok =
      Store.write(
        inst,
        [
          {:put, :hooks, Keys.hook(999), Envelope.to_binary(bad)},
          {:put, :deliveries, Keys.delivery(999, 0), <<0, 1, 2>>},
          {:put, :index, bad_key, :erlang.term_to_binary({bad.source_id, bad.id})}
        ],
        sync: true
      )

    assert {:ok, :created, job} = Replay.start(inst, %{kind: :dlq, rate: 10_000})
    poll_job_state(inst, job.id, fn job -> job.state == :done end)

    # The three good rows moved; the corrupt one was passed over, not deleted.
    {:ok, job} = Replay.get(inst, job.id)
    assert job.moved == 3
    assert {:ok, _} = Store.get(inst, :index, bad_key)
  end

  test "archive redrive: compacted hooks re-deliver through the chosen sinks, no new obligations" do
    config =
      start(
        %{"arc" => [sinks: [{RecordSink, pid: self()}]]},
        roles: [:edge, :dispatch, :storage],
        storage: %{roll_ms: 0, interval_ms: 0}
      )

    inst = config.instance

    # Ingest 50 hooks; they deliver normally and are compacted away. `rate: 25`
    # means one tick moves at most 5 entries, so the redrive spans several
    # pages of one segment — every entry must still arrive exactly once.
    from = System.system_time(:millisecond)

    envs =
      for _ <- 1..50 do
        {:ok, [{:committed, env}]} =
          Ankusa.Queue.enqueue(inst, [
            %{envelope: build_env("arc"), sinks: [{RecordSink, pid: self()}]}
          ])

        env
      end

    to = System.system_time(:millisecond)

    for _ <- 1..50, do: assert_receive({:replayed, _id, nil}, 2_000)

    assert {:ok, _} = Pipeline.tick(inst)
    assert {:ok, _} = Compactor.tick(inst)

    # Everything is archived: no pending obligations remain.
    assert {:ok, [], false} = Ankusa.Queue.Archive.pending(inst, 0, 1_000_000)

    assert {:ok, :created, job} =
             Replay.start(inst, %{
               kind: :archive,
               from: from - 1,
               to: to,
               sinks: [0],
               rate: 25
             })

    ids = MapSet.new(envs, & &1.id)

    received =
      for _ <- 1..50 do
        assert_receive {:replayed, id, replay_id}, 5_000
        assert MapSet.member?(ids, id)
        assert replay_id == job.id
        id
      end
      |> MapSet.new()

    assert received == ids
    refute_received {:replayed, _, _}

    # A couple of replayer ticks pass while the job finishes: the same segment
    # must not be re-driven.
    Process.sleep(1_000)
    refute_received {:replayed, _, _}

    poll_job_state(inst, job.id, fn job -> job.state == :done and job.moved == 50 end, 30_000)

    # No new archive obligations from the replayed hooks.
    assert {:ok, [], false} = Ankusa.Queue.Archive.pending(inst, 0, 1_000_000)
  end

  test "validation: a too-recent archive `to` and an unknown kind are rejected" do
    config = start(%{"src" => [sinks: [{RecordSink, pid: self()}]]})
    inst = config.instance

    now = System.system_time(:millisecond)

    assert {:error, {:invalid, "to"}} =
             Replay.start(inst, %{kind: :archive, from: 0, to: now})

    assert {:error, {:invalid, "kind"}} = Replay.start(inst, %{kind: :nope})
    assert {:error, {:invalid, "from"}} = Replay.start(inst, %{kind: :archive, to: 1_000})

    assert {:error, {:invalid, "sinks"}} =
             Replay.start(inst, %{kind: :archive, from: 0, to: 1_000, sinks: []})

    assert {:error, {:invalid, "rate"}} = Replay.start(inst, %{kind: :dlq, rate: 0})

    assert {:error, {:invalid, "until"}} =
             Replay.start(inst, %{kind: :dlq, since: 5, until: 4})

    # A key belonging to the other kind is refused, not silently dropped:
    # `{"kind":"dlq","from":…,"to":…}` must never become an unbounded DLQ
    # replay.
    assert {:error, {:invalid, "from"}} =
             Replay.start(inst, %{kind: :dlq, from: 0, to: 1_000})

    assert {:error, {:invalid, "since"}} =
             Replay.start(inst, %{kind: :archive, from: 0, to: 1_000, since: 5})
  end
end
