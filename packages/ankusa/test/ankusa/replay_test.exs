defmodule Ankusa.ReplayTest do
  @moduledoc """
  The replay engine end to end: pacing, lag guard, idempotent create, restart
  resume, auto-pause, the created_at cutoff, and archive redrive. Runs against
  real instances, following the `dispatch_test.exs` inline-sink style.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Dispatch.Pipeline
  alias Ankusa.{Envelope, Replay, Store, UUIDv7}
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
    def deliver(env, _ctx, opts) do
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
              retry: {Ankusa.RetryPolicy.Exponential, base_ms: 0, max_attempts: 1, jitter: false},
              # Replays re-drive rows that fail again: a breaker would park them.
              breaker_failures: 0
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
          {:put, :deliveries, Keys.delivery(seq, 0), Deliveries.encode_row(row)}
        ] ++
          dead_index_ops(
            inst,
            seq,
            0,
            at,
            env.source_id,
            env.id,
            env.tenant_id || "default",
            env.size
          ),
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

  # Receives `{:replayed, id, replay_id}` until every id in `ids` has arrived at
  # least once; a repeat is fine (dispatch is at-least-once), a stranger is not.
  defp collect_replayed(seen, ids, replay_id) do
    if MapSet.equal?(seen, ids) do
      seen
    else
      receive do
        {:replayed, id, rid} ->
          assert rid == replay_id
          assert MapSet.member?(ids, id)
          collect_replayed(MapSet.put(seen, id), ids, replay_id)
      after
        5_000 -> flunk("#{MapSet.size(ids) - MapSet.size(seen)} ids were never delivered")
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
        flunk("replay job #{id} never reached the expected state: #{inspect(job)}")
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

    # Every row is delivered and the job finishes.
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

    # The replayer moves each dead row exactly once (its cursor is committed
    # with the rows: `moved: 300` above). Dispatch redelivers a row whose
    # delivery was in flight when the instance stopped, which at-least-once
    # allows, so an id may arrive twice; every id must arrive at least once.
    assert collect_replayed(MapSet.new(), ids, job.id) == ids
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

  test "auto-pause: a replay into a destination whose breaker opens pauses itself" do
    config =
      start(%{"src" => [sinks: [{AlwaysFail, []}]]},
        dispatch: %{
          retry: {Ankusa.RetryPolicy.Exponential, base_ms: 0, max_attempts: 1, jitter: false},
          breaker_failures: 5,
          breaker_open_ms: 60_000,
          breaker_max_open_ms: 60_000
        }
      )

    inst = config.instance
    dead_rows(inst, 500, AlwaysFail)

    assert {:ok, :created, job} = Replay.start(inst, %{kind: :dlq, rate: 100})

    # Five attempts open the breaker; every row after them is parked, never
    # dead-lettered, so only the parked count can pause the job.
    paused = poll_job_state(inst, job.id, fn job -> job.state == :paused end, 20_000)

    assert paused.error =~ "parked behind an open circuit breaker"
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
          {:put, :deliveries, Keys.delivery(999, 0), <<0, 1, 2>>}
        ] ++ dead_index_ops(inst, 999, 0, at, bad.source_id, bad.id),
        sync: true
      )

    assert {:ok, :created, job} = Replay.start(inst, %{kind: :dlq, rate: 10_000})
    poll_job_state(inst, job.id, fn job -> job.state == :done end)

    # The three good rows moved; the corrupt one was passed over, not deleted.
    {:ok, job} = Replay.get(inst, job.id)
    assert job.moved == 3
    assert {:ok, _} = Store.get(inst, :index, bad_key)

    # It stays listable: one corrupt row must not take `GET /v1/dlq` down.
    assert {:ok, %{total: 1, entries: [%{reason: "undecodable delivery row"}]}} =
             Ankusa.Queue.dead(inst, limit: 10)
  end

  # Dead index entries only: the scan reads the key and its `{source, id}`
  # value, so rows a filter skips never need a hook or a delivery row.
  defp dead_keys!(inst, source_id, first_seq, count, at) do
    ops =
      for seq <- first_seq..(first_seq + count - 1) do
        dead_index_ops(inst, seq, 0, at, source_id, "id-#{seq}")
      end
      |> List.flatten()

    :ok = Store.write(inst, ops, sync: true)
  end

  test "dead_page halts at its scan budget before the filter, and resumes from its cursor" do
    config = start(%{"src" => [sinks: [{RecordSink, pid: self()}]]})
    inst = config.instance
    at = System.system_time(:millisecond) - 120_000
    dead_keys!(inst, "a", 2_000_001, 25, at)
    write_dead!(inst, 3_000_000, build_env("b"), RecordSink, at + 1_000)

    range = {<<?x, 0::64>>, <<?x, at + 2_000::64>>}

    # Ten keys examined, none of them "b": the page ends there, reports the
    # cursor it reached, and does not claim the range is exhausted.
    assert {:ok, [], last, false, 10} =
             Deliveries.dead_page(inst, range, %{source_id: "b"}, 100, 10)

    assert last == Keys.dead(at, 2_000_010, 0)

    # The next page, from that cursor, reaches the "b" row and finishes.
    assert {:ok, [{_key, 3_000_000, 0, _ord}], _last, true, 16} =
             Deliveries.dead_page(
               inst,
               {last <> <<0>>, elem(range, 1)},
               %{source_id: "b"},
               100,
               100
             )
  end

  test "a job whose first matching row is past one tick's scan budget still moves it" do
    config =
      start(%{
        "a" => [sinks: [{RecordSink, pid: self()}]],
        "b" => [sinks: [{RecordSink, pid: self()}]]
      })

    inst = config.instance
    at = System.system_time(:millisecond) - 120_000

    # More "a" rows than the replayer examines in one tick (20 000), all
    # dead-lettered before the single "b" row.
    dead_keys!(inst, "a", 2_000_001, 20_050, at)
    write_dead!(inst, 3_000_000, build_env("b"), RecordSink, at + 1_000)

    assert {:ok, :created, job} = Replay.start(inst, %{kind: :dlq, source_id: "b", rate: 10_000})
    poll_job_state(inst, job.id, fn job -> job.state == :done end)

    {:ok, job} = Replay.get(inst, job.id)
    assert job.moved == 1
    assert job.scanned == 20_051
    assert_receive {:replayed, _id, replay_id}, 5_000
    assert replay_id == job.id
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

  # ── quarantine release ─────────────────────────────────────────────────────

  defp swh_secret, do: "whsec_" <> Base.encode64(:crypto.strong_rand_bytes(24))

  # A writable source store whose spec carries the secret, so a test can
  # rotate it with `SourceStore.put/5`.
  defp start_quarantining do
    pid = self()

    decoder = fn _source_id, spec ->
      [
        verifier: {Ankusa.Verifier.Hmac, scheme: :standard_webhooks, secret: spec["secret"]},
        on_verify_failure: :quarantine,
        sinks: [{RecordSink, pid: pid}]
      ]
    end

    start(%{}, source_store: {Ankusa.SourceStore.Persistent, decoder: decoder})
  end

  test "quarantine release: re-verified against the current secret, original id, replay_id" do
    config = start_quarantining()
    inst = config.instance
    {old, new} = {swh_secret(), swh_secret()}

    assert {:ok, _} = Ankusa.SourceStore.put(inst, "acme", "billing", %{"secret" => old}, :create)

    # The provider already signs with the new secret; the source still has the old one.
    body = ~s({"type":"invoice.paid"})
    headers = standard_webhooks_headers("msg_1", body, new)

    assert {:quarantined, :no_match} =
             Ankusa.Edge.Ingest.ingest(inst, request("acme.billing", body, headers))

    assert {:ok, [held]} = Ankusa.Edge.Quarantine.recent(inst, 10)
    assert held.tenant_id == "acme"

    # Still the old secret: the job re-checks, finds nothing that passes, and
    # leaves the hook in the pen.
    assert {:ok, :created, first} = Replay.start(inst, kind: :quarantine)
    first = poll_job_state(inst, first.id, &(&1.state == :done))
    assert {first.moved, first.skipped, first.scanned} == {0, 1, 1}
    assert {:ok, [^held]} = Ankusa.Edge.Quarantine.recent(inst, 10)

    assert {:ok, _} = Ankusa.SourceStore.put(inst, "acme", "billing", %{"secret" => new}, :update)

    assert {:ok, :created, second} = Replay.start(inst, kind: :quarantine)
    second = poll_job_state(inst, second.id, &(&1.state == :done))
    assert {second.moved, second.skipped} == {1, 0}

    held_id = held.id
    second_id = second.id
    assert_receive {:replayed, ^held_id, ^second_id}, 5_000
    assert {:ok, []} = Ankusa.Edge.Quarantine.recent(inst, 10)
    assert {:ok, %{total: 0}} = Ankusa.Queue.dead(inst, limit: 10)

    # Nothing is left to release, and nothing is delivered twice.
    assert {:ok, :created, third} = Replay.start(inst, kind: :quarantine)
    third = poll_job_state(inst, third.id, &(&1.state == :done))
    assert {third.moved, third.scanned} == {0, 0}
    refute_receive {:replayed, _, _}, 200
  end

  test "quarantine release judges the signature's timestamp at the hook's receive time" do
    config = start_quarantining()
    inst = config.instance
    secret = swh_secret()

    assert {:ok, _} =
             Ankusa.SourceStore.put(inst, "acme", "billing", %{"secret" => secret}, :create)

    # Both arrived an hour ago, signed with the source's secret. The first was
    # on time when it arrived; the second was already an hour stale then — a
    # replayed old request, which no release may let through.
    received_at = System.system_time(:millisecond) - 3_600_000
    arrived = div(received_at, 1000)
    on_time = held(received_at, standard_webhooks_headers("msg_1", "{}", secret, arrived))
    stale = held(received_at, standard_webhooks_headers("msg_2", "{}", secret, arrived - 3_600))

    for env <- [on_time, stale], do: :ok = Ankusa.Edge.Quarantine.put(inst, env, :no_match)

    assert {:ok, :created, job} = Replay.start(inst, kind: :quarantine)
    job = poll_job_state(inst, job.id, &(&1.state == :done))
    assert {job.moved, job.skipped} == {1, 1}

    {on_time_id, job_id} = {on_time.id, job.id}
    assert_receive {:replayed, ^on_time_id, ^job_id}, 5_000
    assert {:ok, [%{id: still_held}]} = Ankusa.Edge.Quarantine.recent(inst, 10)
    assert still_held == stale.id
  end

  defp held(received_at, headers) do
    %Envelope{
      id: UUIDv7.generate(),
      source_id: "acme.billing",
      tenant_id: "acme",
      received_at: received_at,
      method: "POST",
      path: "/webhooks/acme.billing",
      headers: headers,
      content_type: nil,
      body: "{}",
      size: 2
    }
  end

  test "quarantine release needs the queue writer: an edge-less node refuses it" do
    config = start(%{"src" => [sinks: [{RecordSink, pid: self()}]]}, roles: [:dispatch])

    assert {:error, {:role_not_enabled, :edge}} =
             Replay.start(config.instance, kind: :quarantine)
  end

  defmodule LifecycleSink do
    @moduledoc "Reports every lifecycle delivery as `{:lifecycle_seen, id, replay_id, event}`."

    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, ctx, opts) do
      send(
        Keyword.fetch!(opts, :pid),
        {:lifecycle_seen, env.id, ctx[:replay_id], JSON.decode!(env.body)}
      )

      :ok
    end
  end

  @doc false
  def lifecycle_decoder(_source_id, _spec), do: [sinks: [{Ankusa.Sink.Log, []}]]

  defp start_lifecycle(sinks) do
    start(%{},
      source_store: {Ankusa.SourceStore.Persistent, decoder: &__MODULE__.lifecycle_decoder/2},
      lifecycle: %{sinks: sinks}
    )
  end

  defp put_source(inst, tenant, name) do
    {:ok, _stored} =
      Ankusa.SourceStore.put(inst, tenant, name, %{"sinks" => [%{"type" => "log"}]}, :create)
  end

  defp live_events(count) do
    for _ <- 1..count do
      assert_receive {:lifecycle_seen, id, nil, event}, 5_000
      {id, event}
    end
  end

  describe "lifecycle replay" do
    test "lifecycle replay re-emits a window of the trail under the job's id, keeping each event id" do
      config = start_lifecycle([{LifecycleSink, pid: self()}])
      inst = config.instance

      put_source(inst, "acme", "a")
      put_source(inst, "acme", "b")
      put_source(inst, "globex", "c")
      live = Map.new(live_events(3))
      assert map_size(live) == 3

      assert {:ok, :created, job} = Replay.start(inst, kind: :lifecycle, tenants: ["acme"])
      job = poll_job_state(inst, job.id, &(&1.state == :done))
      job_id = job.id

      # Only acme's two events, each with the id it was first emitted with.
      seen =
        for _ <- 1..2 do
          assert_receive {:lifecycle_seen, id, ^job_id, event}, 5_000
          {id, event}
        end

      refute_receive {:lifecycle_seen, _, _, _}, 200

      for {id, event} <- seen do
        assert event["replayid"] == job_id
        assert event["data"]["tenant"] == "acme"
        assert live[id]["subject"] == event["subject"]
        assert live[id]["sequence"] == event["sequence"]
      end

      assert {job.moved, job.delivered, job.dead} == {2, 2, 0}
    end

    test "lifecycle replay honours since and until on the change's own time" do
      config = start_lifecycle([{LifecycleSink, pid: self()}])
      inst = config.instance

      put_source(inst, "acme", "early")
      Process.sleep(20)
      middle = System.system_time(:millisecond)
      Process.sleep(20)
      put_source(inst, "acme", "late")
      live_events(2)

      assert {:ok, :created, job} = Replay.start(inst, kind: :lifecycle, since: middle)
      poll_job_state(inst, job.id, &(&1.state == :done))
      assert_receive {:lifecycle_seen, _, _, %{"subject" => "acme.late"}}, 5_000
      refute_receive {:lifecycle_seen, _, _, _}, 200

      assert {:ok, :created, job} = Replay.start(inst, kind: :lifecycle, until: middle)
      poll_job_state(inst, job.id, &(&1.state == :done))
      assert_receive {:lifecycle_seen, _, _, %{"subject" => "acme.early"}}, 5_000
      refute_receive {:lifecycle_seen, _, _, _}, 200
    end

    test "lifecycle replay of an empty window finishes with nothing moved" do
      config = start_lifecycle([{LifecycleSink, pid: self()}])

      assert {:ok, :created, job} = Replay.start(config.instance, kind: :lifecycle)
      job = poll_job_state(config.instance, job.id, &(&1.state == :done))
      assert {job.moved, job.delivered} == {0, 0}
    end

    test "lifecycle replay needs a lifecycle sink on the node" do
      config = start_lifecycle([])

      assert {:error, {:role_not_enabled, :lifecycle}} =
               Replay.start(config.instance, kind: :lifecycle)
    end

    test "lifecycle replay takes since, until and tenants and nothing else" do
      config = start_lifecycle([{LifecycleSink, pid: self()}])

      assert {:error, {:invalid, "source_id"}} =
               Replay.start(config.instance, kind: :lifecycle, source_id: "x")

      assert {:error, {:invalid, "tenants"}} =
               Replay.start(config.instance, kind: :lifecycle, tenants: [7])

      assert {:error, {:invalid, "until"}} =
               Replay.start(config.instance, kind: :lifecycle, since: 10, until: 5)
    end
  end

  describe "DEL-6 filters" do
    defp write_dead_row!(inst, seq, env, sink, at, extra \\ %{}) do
      row =
        Map.merge(
          %{module: RecordSink, state: :dead, attempts: 2, at: at, error: "x", size: env.size},
          extra
        )

      :ok =
        Store.write(
          inst,
          [
            {:put, :hooks, Keys.hook(seq), Envelope.to_binary(env)},
            {:put, :deliveries, Keys.delivery(seq, sink), Deliveries.encode_row(row)}
          ] ++
            dead_index_ops(
              inst,
              seq,
              sink,
              at,
              env.source_id,
              env.id,
              env.tenant_id || "default",
              env.size
            ),
          sync: true
        )

      env
    end

    defp tenant_env(tenant), do: %{build_env("src") | tenant_id: tenant}

    defp two_sinks,
      do: %{"src" => [sinks: [{RecordSink, pid: self()}, {RecordSink, pid: self()}]]}

    defp await_done(inst, id) do
      poll(fn ->
        case Replay.get(inst, id) do
          {:ok, %{state: :done} = job} -> {:ok, job}
          _ -> :retry
        end
      end)
    end

    defp dead_ids(inst, opts \\ []) do
      {:ok, %{entries: entries}} = Ankusa.Queue.dead(inst, [limit: 100] ++ opts)
      entries |> Enum.map(& &1.envelope.id) |> MapSet.new()
    end

    test "tenant filter: a job scoped to one tenant revives only that tenant's rows" do
      config = start(%{"src" => [sinks: [{RecordSink, pid: self()}]]})
      inst = config.instance
      at = System.system_time(:millisecond) - 60_000

      acme =
        for i <- 1..4, do: write_dead_row!(inst, 2_000_000 + i, tenant_env("acme"), 0, at)

      globex =
        for i <- 1..3, do: write_dead_row!(inst, 2_100_000 + i, tenant_env("globex"), 0, at)

      assert {:ok, :created, job} =
               Replay.start(inst, %{kind: :dlq, tenants: ["acme"], rate: 500})

      assert job.filter.tenants == ["acme"]

      acme_ids = MapSet.new(acme, & &1.id)
      assert collect_replayed(MapSet.new(), acme_ids, job.id) == acme_ids

      done = await_done(inst, job.id)
      assert done.moved == 4

      # Only globex's rows are still dead, and none of them was delivered.
      assert dead_ids(inst) == MapSet.new(globex, & &1.id)
      refute_receive {:replayed, _id, _replay_id}, 200

      assert {:ok, %{dead: 0}} = Deliveries.dead_for_tenant(inst, "acme")
      assert {:ok, %{dead: 3}} = Deliveries.dead_for_tenant(inst, "globex")
    end

    test "sink filter: a job for one sink index revives only that sink's rows" do
      config = start(two_sinks())
      inst = config.instance
      at = System.system_time(:millisecond) - 60_000
      env = build_env("src")
      second = build_env("src")

      write_dead_row!(inst, 3_000_001, env, 0, at)
      write_dead_row!(inst, 3_000_001, env, 1, at + 1)
      write_dead_row!(inst, 3_000_002, second, 1, at + 2)

      assert {:ok, :created, job} = Replay.start(inst, %{kind: :dlq, sink: 1, rate: 500})
      assert collect_replayed(MapSet.new(), MapSet.new([env.id, second.id]), job.id)

      assert %{moved: 2} = await_done(inst, job.id)
      assert {:ok, %{total: 1, entries: [%{sink: 0}]}} = Ankusa.Queue.dead(inst, limit: 10)

      assert {:error, {:invalid, "sink"}} = Replay.start(inst, %{kind: :dlq, sink: -1})
    end

    test "error_class filter: only rows dead-lettered as that class are revived" do
      config = start(%{"src" => [sinks: [{RecordSink, pid: self()}]]})
      inst = config.instance
      at = System.system_time(:millisecond) - 60_000

      transient =
        write_dead_row!(inst, 4_000_001, build_env("src"), 0, at, %{error_class: :transient})

      permanent =
        write_dead_row!(inst, 4_000_002, build_env("src"), 0, at, %{error_class: :permanent})

      unclassified = write_dead_row!(inst, 4_000_003, build_env("src"), 0, at)

      assert {:ok, :created, job} =
               Replay.start(inst, %{kind: :dlq, error_class: :transient, rate: 500})

      assert collect_replayed(MapSet.new(), MapSet.new([transient.id]), job.id)
      assert %{moved: 1, scanned: 3} = await_done(inst, job.id)

      # What the job left is still listed, and still filterable by its class.
      assert dead_ids(inst) == MapSet.new([permanent.id, unclassified.id])
      assert dead_ids(inst, error_class: :permanent) == MapSet.new([permanent.id])
      assert dead_ids(inst, error_class: :transient) == MapSet.new()

      assert {:error, {:invalid, "error_class"}} =
               Replay.start(inst, %{kind: :dlq, error_class: :bogus})

      # Quarantine jobs have no sink or class: a typo, not a dropped filter.
      assert {:error, {:invalid, "error_class"}} =
               Replay.start(inst, %{kind: :quarantine, error_class: :transient})
    end

    test "job_id: the same id answers the same job in any state, a different spec is refused" do
      config = start(%{"src" => [sinks: [{RecordSink, pid: self()}]]})
      inst = config.instance
      at = System.system_time(:millisecond) - 60_000
      env = write_dead_row!(inst, 5_000_001, tenant_env("acme"), 0, at)
      job_id = UUIDv7.generate()
      spec = %{kind: :dlq, tenants: ["acme"], job_id: job_id, rate: 500}

      assert {:ok, :created, job} = Replay.start(inst, spec)
      assert job.id == job_id
      assert collect_replayed(MapSet.new(), MapSet.new([env.id]), job_id)
      assert %{moved: 1} = await_done(inst, job_id)

      # Finished, and still the answer: a fleet command started again gets it.
      assert {:ok, :existing, again} = Replay.start(inst, spec)
      assert %{id: ^job_id, state: :done, moved: 1} = again

      # The id belongs to that job: a different filter on it is not a second job.
      assert {:error, {:invalid, "job_id"}} =
               Replay.start(inst, %{spec | tenants: ["globex"]})

      assert {:error, {:invalid, "job_id"}} = Replay.start(inst, %{kind: :dlq, job_id: "nope"})
    end

    test "job_id survives a restart: the resumed node answers the job it had" do
      config = start(%{"src" => [sinks: [{RecordSink, pid: self()}]]})
      inst = config.instance
      dead_rows(inst, 3)
      job_id = UUIDv7.generate()
      spec = %{kind: :dlq, job_id: job_id, rate: 500}

      assert {:ok, :created, _job} = Replay.start(inst, spec)
      assert %{moved: 3} = await_done(inst, job_id)

      # The test supervisor restarts the instance; the replayer loads its jobs again.
      :ok = Supervisor.stop(Ankusa.whereis(inst, :instance))

      poll(fn ->
        case Replay.list(inst) do
          {:ok, [%{id: ^job_id} | _]} -> {:ok, :loaded}
          _ -> :retry
        end
      end)

      assert {:ok, :existing, %{id: ^job_id, state: :done}} = Replay.start(inst, spec)
    end
  end
end
