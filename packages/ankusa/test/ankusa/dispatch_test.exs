defmodule Ankusa.DispatchTest do
  @moduledoc """
  Dispatch end to end, against `Ankusa.Instance`: hooks committed through the
  store, delivery rows claimed by `Ankusa.Dispatch.Pipeline`, and the DLQ,
  replay and reclaim behaviour.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Dispatch.Pipeline
  alias Ankusa.Envelope
  alias Ankusa.Queue.Deliveries
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @moduletag capture_log: true

  # ── test sinks ─────────────────────────────────────────────────────────────

  defmodule TagSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      send(Keyword.fetch!(opts, :pid), {:delivered, Keyword.fetch!(opts, :tag), env.id})
      :ok
    end
  end

  defmodule FlakySink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, ctx, opts) do
      n = Agent.get_and_update(Keyword.fetch!(opts, :agent), fn c -> {c + 1, c + 1} end)

      if n >= Keyword.fetch!(opts, :succeed_at) do
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

  defmodule RaisingSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: raise("boom")
  end

  # Returns a bare `:error`, not `{:error, _}`: dispatch must normalize it.
  defmodule BadReturnSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: :error
  end

  # `:block` holds the delivery task open until the test says go; `:ok` answers
  # immediately. Same module, so a restarted source binds the row by index.
  defmodule GateSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      id = env.id

      case Keyword.fetch!(opts, :mode) do
        :block ->
          send(Keyword.fetch!(opts, :pid), {:gate_started, id, self()})

          receive do
            {:go, ^id} -> :ok
          after
            5_000 -> {:error, :gate_timeout}
          end

        :ok ->
          send(Keyword.fetch!(opts, :pid), {:gate_ok, id})
          :ok
      end
    end
  end

  # Fails while its agent holds `:fail`, succeeds once the test flips it.
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
      end
    end
  end

  # ── helpers ────────────────────────────────────────────────────────────────

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

  defp start(sources, opts \\ []) do
    dispatch = Map.merge(%{retry: retry()}, Keyword.get(opts, :dispatch, %{}))

    config =
      test_config(
        roles: [:edge, :dispatch],
        source_store: {Ankusa.SourceStore.Static, sources: sources},
        dispatch: dispatch
      )

    start_supervised!({Ankusa.Instance, config})
    config.instance
  end

  # A hook, its pending row for sink 0 and the due key, as a commit stamped at
  # `at` leaves them. Written straight to the store, so a test can make a commit
  # become visible whenever it likes.
  defp write_pending!(inst, seq, env, module, at) do
    bin = Envelope.to_binary(env)

    row =
      Deliveries.encode_row(%{
        module: module,
        state: :pending,
        attempts: 0,
        at: at,
        error: nil,
        size: byte_size(bin)
      })

    :ok =
      Store.write(
        inst,
        [
          {:put, :hooks, Keys.hook(seq), bin},
          {:put, :deliveries, Keys.delivery(seq, 0), row},
          {:put, :index, Keys.due(at, seq, 0), <<byte_size(bin)::32>>}
        ],
        sync: true
      )
  end

  # ── delivery ───────────────────────────────────────────────────────────────

  test "a hook reaches every sink of its source exactly once" do
    inst =
      start(%{
        "src1" => %{sinks: [{TagSink, tag: :a, pid: self()}, {TagSink, tag: :b, pid: self()}]}
      })

    id = enqueue!(inst, build_env("src1")).id
    assert {:ok, _} = Pipeline.tick(inst)

    assert_receive {:delivered, :a, ^id}
    assert_receive {:delivered, :b, ^id}
    refute_receive {:delivered, :a, ^id}
    refute_receive {:delivered, :b, ^id}

    # No storage role: once both rows are delivered the hook has no obligation.
    assert stored_ids(inst) == []
  end

  test "retries a failing sink until it succeeds" do
    {:ok, agent} = Agent.start_link(fn -> 0 end)

    inst = start(%{"src1" => %{sinks: [{FlakySink, pid: self(), agent: agent, succeed_at: 3}]}})

    env = enqueue!(inst, build_env("src1"))
    assert {:ok, _} = Pipeline.tick(inst)

    assert_receive {:delivered, id, 3}
    assert id == env.id
    assert stored_ids(inst) == []
  end

  # Dispatch scans from just under the last due time it claimed, not from the
  # start of time. A commit is stamped before its fsync and visible after it, so
  # a stalled commit can land beneath that floor: its wake carries the stamp so
  # the scan goes back for it.
  test "a row that became visible below the scan floor is delivered when its wake arrives" do
    inst = start(%{"src1" => %{sinks: [{TagSink, tag: :a, pid: self()}]}})

    # Deliver one hook, so the scan floor climbs to just under its due time.
    first = enqueue!(inst, build_env("src1")).id
    assert {:ok, _} = Pipeline.tick(inst)
    assert_receive {:delivered, :a, ^first}

    stamp = System.system_time(:millisecond) - 30_000
    late = build_env("src1")
    write_pending!(inst, 1_000_000, late, TagSink, stamp)

    # Without the wake, a scan from the floor does not see the row: it sits 30 s
    # below it. (This is what makes the next assertion mean something.)
    assert {:ok, 0} = Pipeline.tick(inst)
    refute_received {:delivered, :a, _}

    send(Ankusa.whereis(inst, :dispatch), {:wake, stamp})
    late_id = late.id
    assert {:ok, _} = Pipeline.tick(inst)
    assert_receive {:delivered, :a, ^late_id}
  end

  # Outcomes are written in batches, and the capacity they free is claimed when
  # the batch is flushed. Housekeeping flushes too: if it lands between a
  # window draining and that flush, it cancels the flush timer, and nothing
  # would claim the rest of the backlog until some other event woke dispatch.
  test "a drained window is refilled even when housekeeping flushes first" do
    inst =
      start(
        %{"src1" => %{sinks: [{GateSink, mode: :block, pid: self()}]}},
        dispatch: %{concurrency: 1, max_inflight: 1}
      )

    ids = for _ <- 1..3, do: enqueue!(inst, build_env("src1")).id

    # A window of one: the first hook is in flight, its sink holding it.
    assert_receive {:gate_started, first, task}, 2_000
    pipeline = Ankusa.whereis(inst, :dispatch)

    # Finish it while the Pipeline is suspended, so its outcome and a
    # housekeeping tick are both waiting in the mailbox, in that order, when it
    # resumes: the tick flushes the outcome before the flush timer can.
    :ok = :sys.suspend(pipeline)
    ref = Process.monitor(task)
    send(task, {:go, first})
    assert_receive {:DOWN, ^ref, :process, ^task, _}, 2_000
    send(pipeline, :housekeeping)
    :ok = :sys.resume(pipeline)

    # The other two still get dispatched, one at a time.
    assert_receive {:gate_started, second, task2}, 2_000
    send(task2, {:go, second})
    assert_receive {:gate_started, third, task3}, 2_000
    send(task3, {:go, third})

    assert Enum.sort([first, second, third]) == Enum.sort(ids)
  end

  # The drain barrier too. `tick/1` has to return once a backlog bigger than the
  # window has gone through, even when a housekeeping tick lands right after the
  # first window drains. (With a caller waiting, `maybe_reply_waiters` flushes
  # and reschedules before housekeeping runs, so this one pins the waiter side
  # and the stale window flag; the test above pins housekeeping's own refill.)
  test "tick returns after a backlog larger than the window drains, housekeeping included" do
    inst =
      start(
        %{"src1" => %{sinks: [{GateSink, mode: :block, pid: self()}]}},
        dispatch: %{concurrency: 2, max_inflight: 2}
      )

    ids = for _ <- 1..5, do: enqueue!(inst, build_env("src1")).id

    # The first window of two is in flight, both sinks holding it.
    assert_receive {:gate_started, a, task_a}, 2_000
    assert_receive {:gate_started, b, task_b}, 2_000

    pipeline = Ankusa.whereis(inst, :dispatch)
    waiter = Task.async(fn -> Pipeline.tick(inst) end)
    await_waiter(pipeline)

    # Drain that window with the Pipeline suspended, so both outcomes and a
    # housekeeping tick are waiting in its mailbox when it resumes.
    :ok = :sys.suspend(pipeline)

    refs =
      for {task, id} <- [{task_a, a}, {task_b, b}] do
        ref = Process.monitor(task)
        send(task, {:go, id})
        ref
      end

    for ref <- refs, do: assert_receive({:DOWN, ^ref, :process, _, _}, 2_000)
    send(pipeline, :housekeeping)
    :ok = :sys.resume(pipeline)

    # The rest of the backlog follows, a window at a time.
    rest =
      for _ <- 1..3 do
        assert_receive {:gate_started, id, task}, 2_000
        send(task, {:go, id})
        id
      end

    assert Enum.sort([a, b | rest]) == Enum.sort(ids)
    assert {:ok, _} = Task.await(waiter, 5_000)
  end

  defp await_waiter(pipeline, tries \\ 100) do
    cond do
      :sys.get_state(pipeline).waiters != [] ->
        :ok

      tries == 0 ->
        flunk("the tick call never reached the pipeline")

      true ->
        Process.sleep(10)
        await_waiter(pipeline, tries - 1)
    end
  end

  test "D1: a failing sink's retries do not hold the slots a healthy source needs" do
    inst =
      start(
        %{
          "flaky" => %{sinks: [{AlwaysFail, []}]},
          "healthy" => %{sinks: [{TagSink, tag: :healthy, pid: self()}]}
        },
        dispatch: %{
          concurrency: 1,
          max_inflight: 2,
          retry: retry(base_ms: 20, max_attempts: 1_000)
        }
      )

    for _ <- 1..20, do: enqueue!(inst, build_env("flaky"))

    healthy = for _ <- 1..5, do: enqueue!(inst, build_env("healthy")).id

    for id <- healthy do
      assert_receive {:delivered, :healthy, ^id}, 2_000
    end
  end

  # ── the DLQ ────────────────────────────────────────────────────────────────

  test "one dead row per exhausted sink, and the hook stays until it clears" do
    inst =
      start(
        %{
          "src1" => %{
            sinks: [{AlwaysFail, []}, {TagSink, tag: :captured, pid: self()}]
          }
        },
        dispatch: %{retry: retry(max_attempts: 2)}
      )

    id = enqueue!(inst, build_env("src1")).id
    assert {:ok, _} = Pipeline.tick(inst)

    assert_receive {:delivered, :captured, ^id}
    refute_receive {:delivered, :captured, ^id}

    assert {:ok, %{total: 1, entries: [entry]}} = Ankusa.Queue.dead(inst, limit: 10)
    assert entry.envelope.id == id
    assert entry.reason == inspect({:sink, AlwaysFail, :always})

    # The dead row is still an obligation, so the hook is still stored.
    assert stored_ids(inst) == [id]
  end

  test "D5: replay re-delivers a dead hook once and then reclaims it" do
    {:ok, agent} = Agent.start_link(fn -> :fail end)

    inst =
      start(%{"src1" => %{sinks: [{SwitchSink, pid: self(), agent: agent}]}},
        dispatch: %{retry: retry(max_attempts: 1)}
      )

    env = enqueue!(inst, build_env("src1"))
    assert {:ok, _} = Pipeline.tick(inst)
    assert {:ok, %{total: 1}} = Ankusa.Queue.dead(inst, limit: 10)

    Agent.update(agent, fn _ -> :ok end)

    assert Ankusa.Dispatch.replay(inst, %{}) == {:ok, 1}
    assert {:ok, _} = Pipeline.tick(inst)
    assert_receive {:delivered, id}
    assert id == env.id

    assert Ankusa.Dispatch.replay(inst, %{}) == {:ok, 0}
    assert stored_ids(inst) == []
  end

  test "a raising sink and a bad-returning sink are dead-lettered, not fatal to dispatch" do
    inst =
      start(%{"src1" => %{sinks: [{RaisingSink, []}, {BadReturnSink, []}]}},
        dispatch: %{retry: retry(max_attempts: 1)}
      )

    pid = Ankusa.whereis(inst, :dispatch)

    env = enqueue!(inst, build_env("src1"))
    assert {:ok, _} = Pipeline.tick(inst)

    assert Process.alive?(pid)
    assert Ankusa.whereis(inst, :dispatch) == pid

    assert {:ok, %{total: 2, entries: entries}} = Ankusa.Queue.dead(inst, limit: 10)

    reasons = entries |> Enum.map(& &1.reason) |> MapSet.new()

    assert reasons ==
             MapSet.new([
               inspect({:sink, RaisingSink, {:raised, %RuntimeError{message: "boom"}}},
                 limit: 50,
                 printable_limit: 4096
               ),
               inspect({:sink, BadReturnSink, {:bad_return, :error}},
                 limit: 50,
                 printable_limit: 4096
               )
             ])

    assert stored_ids(inst) == [env.id]
  end

  # ── source gone ────────────────────────────────────────────────────────────

  test "D2: hooks whose source was deleted are dead-lettered after a restart" do
    inst = unique_instance()
    dir = unique_data_dir(inst)
    on_exit(fn -> File.rm_rf(dir) end)

    cfg1 = persistent_config(inst, dir, [:edge])
    start_supervised!({Ankusa.Instance, cfg1})

    spec = %{"sinks" => [%{"type" => "log"}]}
    assert {:ok, _} = Ankusa.SourceStore.put(inst, "acme", "billing", spec, :create)

    committed = [
      enqueue!(inst, build_env("acme.billing")),
      enqueue!(inst, build_env("acme.billing"))
    ]

    assert Ankusa.SourceStore.delete(inst, "acme", "billing") == :ok
    stop_supervised!({Ankusa.Instance, cfg1.instance})

    cfg2 = persistent_config(inst, dir, [:edge, :dispatch])
    start_supervised!({Ankusa.Instance, cfg2})

    assert {:ok, _} = Pipeline.tick(inst)

    assert {:ok, %{total: 2, entries: entries}} = Ankusa.Queue.dead(inst, limit: 10)

    reason = inspect({:source_gone, "acme.billing"})
    assert Enum.map(entries, & &1.reason) == [reason, reason]

    assert entries |> Enum.map(& &1.envelope.id) |> Enum.sort() ==
             committed |> Enum.map(& &1.id) |> Enum.sort()
  end

  # ── crash recovery ─────────────────────────────────────────────────────────

  test "D7: a claimed row is retried after a restart; a delivered row is not" do
    inst = unique_instance()
    dir = unique_data_dir(inst)
    on_exit(fn -> File.rm_rf(dir) end)

    dispatch = %{concurrency: 1, retry: retry()}

    cfg1 =
      restart_config(
        inst,
        dir,
        [
          {TagSink, tag: :cap, pid: self()},
          {GateSink, pid: self(), mode: :block}
        ],
        dispatch
      )

    start_supervised!({Ankusa.Instance, cfg1})

    env = enqueue!(inst, build_env("src1"))

    assert_receive {:delivered, :cap, cap_id}
    assert cap_id == env.id

    # The gate task is holding the hook's second row claimed, but the first
    # row's outcome is recorded before the gate even starts (concurrency 1).
    assert_receive {:gate_started, gate_id, _task}
    assert gate_id == env.id

    stop_supervised!({Ankusa.Instance, cfg1.instance})

    cfg2 =
      restart_config(
        inst,
        dir,
        [
          {TagSink, tag: :cap, pid: self()},
          {GateSink, pid: self(), mode: :ok}
        ],
        dispatch
      )

    start_supervised!({Ankusa.Instance, cfg2})

    assert {:ok, _} = Pipeline.tick(inst)
    assert_receive {:gate_ok, ^gate_id}
    refute_receive {:delivered, :cap, ^cap_id}
    assert stored_ids(inst) == []
  end

  # ── config helpers ─────────────────────────────────────────────────────────

  defp persistent_config(inst, dir, roles) do
    Ankusa.Config.new(
      instance: inst,
      data_dir: dir,
      port: 0,
      roles: roles,
      source_store: {Ankusa.SourceStore.Persistent, [decoder: &decode_source/2]}
    )
  end

  defp restart_config(inst, dir, sinks, dispatch) do
    Ankusa.Config.new(
      instance: inst,
      data_dir: dir,
      port: 0,
      roles: [:edge, :dispatch],
      dispatch: dispatch,
      source_store: {Ankusa.SourceStore.Static, sources: %{"src1" => %{sinks: sinks}}}
    )
  end

  defp decode_source(_source_id, %{"sinks" => [_ | _]}), do: [sinks: [{Ankusa.Sink.Log, []}]]

  defp decode_source(source_id, spec) do
    raise ArgumentError, "unexpected spec for #{source_id}: #{inspect(spec)}"
  end
end
