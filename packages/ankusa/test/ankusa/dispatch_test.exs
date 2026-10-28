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

  # The threshold callback is user code and raises; `deliver/3` itself is fine.
  defmodule RaisingThresholdSink do
    @behaviour Ankusa.Sink

    @impl true
    def inline_max_bytes(_opts), do: raise("threshold")

    @impl true
    def deliver(_env, _ctx, _opts), do: :ok
  end

  defmodule ReportingSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      send(Keyword.fetch!(opts, :pid), {:delivered, env.id})
      :ok
    end
  end

  # What a throttled destination answers: a pause far longer than any policy.
  defmodule ThrottleSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, opts),
      do: {:error, {:retry_after, Keyword.fetch!(opts, :ms), :throttled}}
  end

  # A credential the destination refuses: the operator's to fix.
  defmodule ConfigErrorSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: {:error, {:permanent, {:config, :access_denied}}}
  end

  # A hook the destination will never take: not a config error.
  defmodule PermanentSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: {:error, {:permanent, :bad_request}}
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
          {:put, :deliveries, Keys.delivery(seq, 0), row}
        ] ++ due_index_ops(inst, seq, 0, at, byte_size(bin), env.tenant_id || "default"),
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

  @doc false
  # Runs in the queue's syncer, before its fsync: tells the test and waits.
  def hold_sync(_event, _measurements, %{instance: inst}, {inst, test_pid}) do
    send(test_pid, {:syncing, self()})

    receive do
      :release -> :ok
    after
      10_000 -> :ok
    end
  end

  def hold_sync(_event, _measurements, _metadata, _config), do: :ok

  # DUR-4.b, ground rules 1 and 5: the writer applies a group before its fsync, and
  # a row that is in the store is visible to every reader, but dispatch must not
  # deliver a hook the provider has not been told `201` for.
  test "a group applied but not yet fsynced is not delivered until the fsync returns" do
    inst = start(%{"src1" => %{sinks: [{TagSink, tag: :a, pid: self()}]}})
    handler = "dispatch-hold-sync-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:ankusa, :wal_sync, :start],
      &__MODULE__.hold_sync/4,
      {inst, self()}
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    task = Task.async(fn -> enqueue!(inst, build_env("src1")) end)
    assert_receive {:syncing, syncer}, 5_000

    # the due row is in the store and long past due, and dispatch has been woken
    assert Deliveries.next_due_at(inst) |> elem(1) != nil
    send(Ankusa.whereis(inst, :dispatch), :wake)
    refute_receive {:delivered, :a, _}, 400

    send(syncer, :release)
    id = Task.await(task).id
    assert_receive {:delivered, :a, ^id}, 5_000
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

  # Dispatch scans a tenant from just under the last due time it claimed for it,
  # not from the start of time. A commit is stamped before its fsync and visible
  # after it, so a stalled commit can land beneath that cursor: its wake carries
  # the stamp so the scan goes back for it. (A tenant that drains loses its
  # cursor, so the tenant here keeps a row due in an hour.)
  test "a row that became visible below the scan floor is delivered when its wake arrives" do
    inst = start(%{"src1" => %{sinks: [{TagSink, tag: :a, pid: self()}]}})

    write_pending!(
      inst,
      999_999,
      build_env("src1"),
      TagSink,
      System.system_time(:millisecond) + 3_600_000
    )

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

  # ADR 0001 §5: a tick walks the active-tenant index, not every tenant. 1 000
  # tenants have a row, 997 of them not due for an hour: only the 3 with work
  # due are visited, and `[:dispatch, :state]` says so. The 3 are written last
  # and come due together, so no scan can see them one at a time.
  test "a tick visits only tenants with work" do
    inst = start(%{"src1" => %{sinks: [{TagSink, tag: :a, pid: self()}]}})
    far = System.system_time(:millisecond) + 3_600_000

    for n <- 4..1_000 do
      env = %{build_env("src1") | tenant_id: "tenant-#{n}"}
      write_pending!(inst, 10_000 + n, env, TagSink, far)
    end

    due_at = System.system_time(:millisecond) + 300

    ids =
      for n <- 1..3 do
        env = %{build_env("src1") | tenant_id: "tenant-#{n}"}
        write_pending!(inst, 10_000 + n, env, TagSink, due_at)
        env.id
      end

    handler = "visits-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler,
        [:ankusa, :dispatch, :state],
        &__MODULE__.forward_state/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler) end)

    Process.sleep(max(due_at - System.system_time(:millisecond), 0) + 50)
    assert {:ok, _settled} = Pipeline.tick(inst)
    for id <- ids, do: assert_receive({:delivered, :a, ^id}, 5_000)

    await_active_tenants(3)
  end

  @doc false
  def forward_state(_event, measurements, _meta, pid),
    do: send(pid, {:dispatch_state, measurements})

  # Waits for a housekeeping report of exactly `expected` visited tenants. The
  # report is once a second and resets the count, so one that lands while the
  # scans are still in progress says fewer; those are skipped. More than
  # `expected` means a tenant with nothing due was visited, which is the failure
  # this test exists to catch.
  defp await_active_tenants(expected, deadline_ms \\ 5_000) do
    started = System.monotonic_time(:millisecond)

    receive do
      {:dispatch_state, %{active_tenants: ^expected}} ->
        :ok

      {:dispatch_state, %{active_tenants: n}} when n < expected ->
        left = deadline_ms - (System.monotonic_time(:millisecond) - started)

        if left > 0,
          do: await_active_tenants(expected, left),
          else: flunk("dispatch reported #{n} visited tenants, expected #{expected}")

      {:dispatch_state, %{active_tenants: n}} ->
        flunk("dispatch visited #{n} tenants, expected #{expected}")
    after
      deadline_ms ->
        flunk("dispatch reported no scan of #{expected} tenants within #{deadline_ms} ms")
    end
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

  test "D6: an attempt that outlasts dispatch.attempt_timeout_ms is killed, frees its slot, and counts as failed" do
    inst =
      start(
        %{
          "hang" => %{sinks: [{GateSink, mode: :block, pid: self()}]},
          "healthy" => %{sinks: [{TagSink, tag: :healthy, pid: self()}]}
        },
        dispatch: %{concurrency: 1, attempt_timeout_ms: 100, retry: retry(max_attempts: 2)}
      )

    hang_id = enqueue!(inst, build_env("hang")).id
    healthy_id = enqueue!(inst, build_env("healthy")).id

    assert_receive {:gate_started, ^hang_id, task_pid}, 1_000
    ref = Process.monitor(task_pid)

    # `:noproc` only if the kill beat the monitor. The gate's own timeout is
    # 5 s, so nothing but the deadline ends the task inside this second.
    assert_receive {:DOWN, ^ref, :process, ^task_pid, reason}, 1_000
    assert reason in [:killed, :noproc]

    # concurrency: 1, so this only runs because the hung attempt's slot was freed
    assert_receive {:delivered, :healthy, ^healthy_id}, 1_000

    # the retry (due at once) hangs and is killed too, and then the policy gives up
    assert {:ok, _} = Pipeline.tick(inst)
    assert {:ok, %{total: 1, entries: [entry]}} = Ankusa.Queue.dead(inst, limit: 10)
    assert entry.envelope.id == hang_id

    assert entry.reason ==
             inspect({:sink, GateSink, {:attempt_timeout, 100}}, limit: 50, printable_limit: 4096)
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

  test "D5: a replay job re-delivers a dead hook once and then reclaims it" do
    {:ok, agent} = Agent.start_link(fn -> :fail end)

    inst =
      start(%{"src1" => %{sinks: [{SwitchSink, pid: self(), agent: agent}]}},
        dispatch: %{retry: retry(max_attempts: 1)}
      )

    env = enqueue!(inst, build_env("src1"))
    assert {:ok, _} = Pipeline.tick(inst)
    assert {:ok, %{total: 1}} = Ankusa.Queue.dead(inst, limit: 10)

    Agent.update(agent, fn _ -> :ok end)

    assert {:ok, :created, job} = Ankusa.Replay.start(inst, %{kind: :dlq, rate: 1_000})
    assert job.state == :running

    assert_receive {:delivered, id}, 5_000
    assert id == env.id

    # The job finishes and the hook is reclaimed.
    deadline = System.monotonic_time(:millisecond) + 5_000

    wait = fn wait ->
      case Ankusa.Replay.get(inst, job.id) do
        {:ok, %{state: :done, moved: 1, delivered: 1}} ->
          :ok

        _ ->
          if System.monotonic_time(:millisecond) > deadline,
            do: flunk("job did not finish"),
            else:
              (
                Process.sleep(50)
                wait.(wait)
              )
      end
    end

    wait.(wait)
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

  test "a sink whose inline_max_bytes/1 raises is dead-lettered, not fatal to dispatch (D3)" do
    inst =
      start(
        %{"src1" => %{sinks: [{RaisingThresholdSink, []}, {ReportingSink, pid: self()}]}},
        dispatch: %{retry: retry(max_attempts: 1)}
      )

    pid = Ankusa.whereis(inst, :dispatch)

    env = enqueue!(inst, build_env("src1"))
    assert {:ok, _} = Pipeline.tick(inst)

    assert Process.alive?(pid)
    assert Ankusa.whereis(inst, :dispatch) == pid

    id = env.id
    assert_receive {:delivered, ^id}

    assert {:ok, %{total: 1, entries: [entry]}} = Ankusa.Queue.dead(inst, limit: 10)
    assert entry.envelope.id == id
    assert entry.reason =~ "inline_max_bytes"
    assert entry.reason =~ "threshold"
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

  test "D2: a row rebinds to its sink's module after a reorder, and is dead-lettered as sink_gone once the module is gone" do
    inst = unique_instance()
    dir = unique_data_dir(inst)
    on_exit(fn -> File.rm_rf(dir) end)

    dispatch = %{retry: retry()}
    original = [{TagSink, tag: :a, pid: self()}, {ReportingSink, pid: self()}]

    # Enqueue on an edge-only node, so the rows wait for a later dispatcher.
    cfg = restart_config(inst, dir, original, dispatch, [:edge])
    start_supervised!({Ankusa.Instance, cfg})
    %{id: id} = enqueue!(inst, build_env("src1"))
    stop_supervised!({Ankusa.Instance, inst})

    # Same modules, reordered, with new opts: row 0 (TagSink) now binds to
    # index 1, and delivers with the current opts.
    reordered = [{ReportingSink, pid: self()}, {TagSink, tag: :b, pid: self()}]
    start_supervised!({Ankusa.Instance, restart_config(inst, dir, reordered, dispatch)})
    assert {:ok, _} = Pipeline.tick(inst)
    assert_receive {:delivered, :b, ^id}, 1_000
    assert_receive {:delivered, ^id}, 1_000
    refute_receive {:delivered, :a, _}
    stop_supervised!({Ankusa.Instance, inst})

    start_supervised!({Ankusa.Instance, restart_config(inst, dir, original, dispatch, [:edge])})
    %{id: id2} = enqueue!(inst, build_env("src1"))
    stop_supervised!({Ankusa.Instance, inst})

    # TagSink is gone: its row is dead-lettered, the surviving sink delivers.
    remaining = [{ReportingSink, pid: self()}]
    start_supervised!({Ankusa.Instance, restart_config(inst, dir, remaining, dispatch)})
    assert {:ok, _} = Pipeline.tick(inst)
    assert_receive {:delivered, ^id2}, 1_000

    assert {:ok, %{total: 1, entries: [entry]}} = Ankusa.Queue.dead(inst, limit: 10)
    assert entry.envelope.id == id2
    assert entry.reason == inspect({:sink_gone, 0, TagSink}, limit: 50, printable_limit: 4096)
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

    # The gate task is holding the hook's second row claimed. The first row's
    # outcome may still be in the Pipeline's outcome buffer (written every
    # 10 ms); the graceful stop below flushes it in `terminate/2`. So this pins a
    # graceful restart. After a crash inside that 10 ms window the first row is
    # redelivered too, which at-least-once allows.
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

  # ── encodings this node cannot read (F7) ───────────────────────────────────

  defp pending_row(at, size) do
    %{
      module: TagSink,
      state: :pending,
      attempts: 0,
      at: at,
      error: nil,
      size: size,
      source_id: "src1"
    }
  end

  # A hook and its pending row as given: the bytes are the test's.
  defp write_raw!(inst, seq, hook_bin, row_bin, at) do
    :ok =
      Store.write(
        inst,
        [
          {:put, :hooks, Keys.hook(seq), hook_bin},
          {:put, :deliveries, Keys.delivery(seq, 0), row_bin}
        ] ++ due_index_ops(inst, seq, 0, at, byte_size(hook_bin)),
        sync: true
      )
  end

  test "F7: a delivery row in a version this node does not know is dead-lettered, hook intact" do
    inst = start(%{"src1" => %{sinks: [{TagSink, tag: :a, pid: self()}]}})
    at = System.system_time(:millisecond) - 1_000
    env = build_env("src1")

    hook = Envelope.to_binary(env, version: 1)

    <<"AD", 1, term::binary>> =
      Deliveries.encode_row(pending_row(at, byte_size(hook)), version: 1)

    write_raw!(inst, 7, hook, <<"AD", 9, term::binary>>, at)

    assert {:ok, _} = Pipeline.tick(inst)
    refute_receive {:delivered, :a, _}

    assert {:ok, %{state: :dead, error: ":unsupported_delivery_row_version"}} =
             Deliveries.row(inst, 7, 0)

    assert {:ok, %{total: 1, entries: [entry]}} = Ankusa.Queue.dead(inst, limit: 10)
    assert entry.reason == ":unsupported_delivery_row_version"
    assert entry.envelope.id == env.id
    assert entry.envelope.source_id == "src1"
  end

  test "F7: a hook and a row written by a newer node (v1, added fields) are delivered" do
    inst = start(%{"src1" => %{sinks: [{TagSink, tag: :a, pid: self()}]}})
    at = System.system_time(:millisecond) - 1_000
    env = build_env("src1")

    {_field, hook} = envelope_from_the_future(env, 1)
    row = pending_row(at, byte_size(hook)) |> Map.put(:first_attempt_at, at)
    write_raw!(inst, 7, hook, Deliveries.encode_row(row, version: 1), at)

    assert {:ok, _} = Pipeline.tick(inst)
    id = env.id
    assert_receive {:delivered, :a, ^id}
  end

  test "F7: v0 rows, as every release up to 0.6 wrote them, are delivered" do
    inst = start(%{"src1" => %{sinks: [{TagSink, tag: :a, pid: self()}]}})
    at = System.system_time(:millisecond) - 1_000
    env = build_env("src1")

    hook = :erlang.term_to_binary(Map.from_struct(env), [:deterministic])
    write_raw!(inst, 7, hook, :erlang.term_to_binary(pending_row(at, byte_size(hook))), at)

    assert {:ok, _} = Pipeline.tick(inst)
    id = env.id
    assert_receive {:delivered, :a, ^id}
  end

  # ── DEL-2: retry horizon, per-source policy, error classes ─────────────────

  @hour 3_600_000

  # A hook and its delivery row (sink 0) as given: `extra` is laid over a fresh
  # pending row, so a test can say how old the delivery is.
  defp write_row!(inst, seq, env, module, at, extra) do
    bin = Envelope.to_binary(env)

    row =
      Map.merge(
        %{
          module: module,
          state: :pending,
          attempts: 0,
          at: at,
          error: nil,
          size: byte_size(bin),
          source_id: env.source_id
        },
        extra
      )

    :ok =
      Store.write(
        inst,
        [
          {:put, :hooks, Keys.hook(seq), bin},
          {:put, :deliveries, Keys.delivery(seq, 0), Deliveries.encode_row(row)}
        ] ++ due_index_ops(inst, seq, 0, at, byte_size(bin), env.tenant_id || "default"),
        sync: true
      )
  end

  defp row!(inst, seq) do
    assert {:ok, row} = Deliveries.row(inst, seq, 0)
    row
  end

  defp failing(dispatch),
    do: start(%{"src1" => %{sinks: [{AlwaysFail, []}]}}, dispatch: dispatch)

  test "DEL-2: a row far past the old 84-attempt limit keeps retrying inside the 24 h horizon" do
    inst = failing(%{retry: retry(base_ms: 60_000, max_attempts: nil)})
    now = System.system_time(:millisecond)
    first = now - 7 * @hour

    write_row!(inst, 7, build_env("src1"), AlwaysFail, now, %{
      attempts: 200,
      first_attempt_at: first
    })

    assert {:ok, _} = Pipeline.tick(inst)

    assert %{state: :pending, attempts: 201, first_attempt_at: ^first, at: at} = row!(inst, 7)
    assert at > now
    assert {:ok, %{total: 0}} = Ankusa.Queue.dead(inst, limit: 10)
  end

  test "DEL-2: a row past its horizon is dead-lettered by its next failure" do
    inst = failing(%{retry: retry(base_ms: 60_000, max_attempts: nil)})
    now = System.system_time(:millisecond)
    env = build_env("src1")
    first = now - 24 * @hour - 1_000
    write_row!(inst, 7, env, AlwaysFail, now, %{attempts: 3, first_attempt_at: first})

    assert {:ok, _} = Pipeline.tick(inst)

    assert {:ok, %{total: 1, entries: [entry]}} = Ankusa.Queue.dead(inst, limit: 10)
    assert entry.envelope.id == env.id
    assert entry.reason == inspect({:sink, AlwaysFail, :always})
    assert %{state: :dead, attempts: 4} = row!(inst, 7)
  end

  test "DEL-2: a retry is never due after the horizon" do
    inst = failing(%{retry: retry(base_ms: 60_000, max_attempts: nil)})
    now = System.system_time(:millisecond)
    first = now - 24 * @hour + 5_000

    write_row!(inst, 7, build_env("src1"), AlwaysFail, now, %{
      attempts: 3,
      first_attempt_at: first
    })

    assert {:ok, _} = Pipeline.tick(inst)

    # the policy's backoff is 300 s; the horizon is 5 s away
    assert %{state: :pending, attempts: 4, at: at} = row!(inst, 7)
    assert at == first + 24 * @hour
  end

  test "DEL-2: an outage longer than the horizon dead-letters at the horizon, not at an attempt count" do
    horizon = 1_500

    inst =
      failing(%{
        breaker_failures: 0,
        retry: retry(base_ms: 50, max_ms: 50, max_attempts: nil, max_elapsed_ms: horizon)
      })

    write_row!(inst, 7, build_env("src1"), AlwaysFail, System.system_time(:millisecond), %{})

    assert await_dead(inst, 10_000) == 1

    # at 100-200 ms a retry, far more attempts than the old default of 10 here
    assert %{state: :dead, attempts: attempts, first_attempt_at: first, at: dead_at} =
             row!(inst, 7)

    assert attempts > 10
    assert dead_at - first >= horizon
    assert dead_at - first < horizon + 1_000
  end

  defp await_dead(inst, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    Stream.repeatedly(fn ->
      Process.sleep(50)
      Pipeline.tick(inst)
      {:ok, %{total: total}} = Ankusa.Queue.dead(inst, limit: 10)
      total
    end)
    |> Enum.find(&(&1 > 0 or System.monotonic_time(:millisecond) > deadline))
  end

  test "DEL-2: a Retry-After is capped by dispatch.max_retry_after_ms, and never past the horizon" do
    sources = %{"src1" => %{sinks: [{ThrottleSink, ms: 10 * @hour}]}}

    capped =
      start(sources, dispatch: %{max_retry_after_ms: 5_000, retry: retry(max_attempts: nil)})

    before = System.system_time(:millisecond)
    write_row!(capped, 7, build_env("src1"), ThrottleSink, before, %{})
    assert {:ok, _} = Pipeline.tick(capped)
    assert %{state: :pending, at: at} = row!(capped, 7)
    assert at >= before + 5_000
    assert at < System.system_time(:millisecond) + 5_001

    # the cap is 1 h and the horizon 2 s away: the horizon wins
    short =
      start(sources,
        dispatch: %{retry: retry(max_attempts: nil, max_elapsed_ms: 2_000)}
      )

    now = System.system_time(:millisecond)
    write_row!(short, 7, build_env("src1"), ThrottleSink, now, %{first_attempt_at: now})
    assert {:ok, _} = Pipeline.tick(short)
    assert %{state: :pending, at: at} = row!(short, 7)
    assert at == now + 2_000
  end

  test "DEL-2: a per-source retry override is honoured, and survives a restart" do
    inst = unique_instance()
    dir = unique_data_dir(inst)
    on_exit(fn -> File.rm_rf(dir) end)

    dispatch = %{retry: retry(base_ms: 60_000, max_attempts: nil, max_elapsed_ms: 1_000_000)}
    spec = %{"fail" => true, "sinks" => [%{"type" => "log"}]}

    cfg1 = persistent_config(inst, dir, [:edge], dispatch)
    start_supervised!({Ankusa.Instance, cfg1})

    assert {:ok, _} =
             Ankusa.SourceStore.put(
               inst,
               "acme",
               "short",
               Map.put(spec, "retry", %{"max_elapsed_ms" => 3_000}),
               :create
             )

    assert {:ok, _} = Ankusa.SourceStore.put(inst, "acme", "plain", spec, :create)

    now = System.system_time(:millisecond)
    write_row!(inst, 7, build_env("acme.short"), AlwaysFail, now, %{})
    write_row!(inst, 8, build_env("acme.plain"), AlwaysFail, now, %{})
    stop_supervised!({Ankusa.Instance, inst})

    # The source store reloads the override from disk; dispatch applies it.
    start_supervised!(
      {Ankusa.Instance, persistent_config(inst, dir, [:edge, :dispatch], dispatch)}
    )

    assert {:ok, _} = Pipeline.tick(inst)

    assert %{first_attempt_at: short_first, at: short_at} = short = row!(inst, 7)
    assert short.state == :pending
    # the override's 3 s horizon clamps the 60 s backoff...
    assert short_at == short_first + 3_000

    assert %{first_attempt_at: plain_first, at: plain_at} = plain = row!(inst, 8)
    assert plain.state == :pending
    # ...while the other source keeps the instance's policy: a full 60 s
    assert plain_at - plain_first >= 60_000
    assert plain_at - plain_first < 61_000
    stop_supervised!({Ankusa.Instance, inst})

    # The horizon's start is in the row: it is the same after another restart
    start_supervised!({Ankusa.Instance, persistent_config(inst, dir, [:edge], dispatch)})
    assert %{first_attempt_at: ^short_first, at: ^short_at} = row!(inst, 7)
  end

  test "DEL-2: a row parked behind an open breaker is due by its horizon, and dead-lettered past it" do
    inst =
      failing(%{
        breaker_failures: 1,
        breaker_open_ms: 60_000,
        breaker_max_open_ms: 60_000,
        retry: retry(base_ms: 60_000, max_attempts: nil)
      })

    now = System.system_time(:millisecond)
    write_row!(inst, 7, build_env("src1"), AlwaysFail, now, %{})
    assert {:ok, _} = Pipeline.tick(inst)
    # one failure opened the breaker
    assert %{state: :pending, attempts: 1} = row!(inst, 7)

    old = build_env("src1")
    stale = now - 25 * @hour
    write_row!(inst, 8, old, AlwaysFail, now, %{attempts: 4, first_attempt_at: stale})
    near = now - 24 * @hour + 3_000

    write_row!(inst, 9, build_env("src1"), AlwaysFail, now, %{attempts: 2, first_attempt_at: near})

    assert {:ok, _} = Pipeline.tick(inst)

    # past the horizon: dead, with no attempt made
    assert {:ok, %{total: 1, entries: [entry]}} = Ankusa.Queue.dead(inst, limit: 10)
    assert entry.envelope.id == old.id
    assert entry.reason == inspect({:retry_horizon, AlwaysFail})
    assert entry.error_class == :transient
    assert %{state: :dead, attempts: 4} = row!(inst, 8)

    # inside it: parked, attempts unchanged, but due no later than the horizon
    assert %{state: :pending, attempts: 2, at: at} = row!(inst, 9)
    assert at == near + 24 * @hour
  end

  test "a row parked behind a breaker a throttling sink opened is dead-lettered as throttled at its horizon" do
    inst =
      start(%{"src1" => %{sinks: [{ThrottleSink, ms: 60_000}]}},
        dispatch: %{
          breaker_failures: 1,
          breaker_open_ms: 60_000,
          breaker_max_open_ms: 60_000,
          retry: retry(base_ms: 60_000, max_attempts: nil)
        }
      )

    now = System.system_time(:millisecond)
    write_row!(inst, 7, build_env("src1"), ThrottleSink, now, %{})
    assert {:ok, _} = Pipeline.tick(inst)
    # one throttled answer opened the breaker
    assert %{state: :pending, attempts: 1} = row!(inst, 7)

    old = build_env("src1")

    write_row!(inst, 8, old, ThrottleSink, now, %{
      attempts: 4,
      first_attempt_at: now - 25 * @hour
    })

    assert {:ok, _} = Pipeline.tick(inst)

    assert {:ok, %{entries: [entry]}} = Ankusa.Queue.dead(inst, limit: 10)
    assert entry.envelope.id == old.id
    assert entry.reason == inspect({:retry_horizon, ThrottleSink})
    assert entry.error_class == :throttled
  end

  test "DEL-2: auth and config errors dead-letter at once and are counted per sink" do
    inst =
      start(%{
        "src1" => %{sinks: [{ConfigErrorSink, []}, {PermanentSink, []}, {AlwaysFail, []}]}
      })

    handler = "config-errors-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler,
        [:ankusa, :dispatch, :config_error],
        &__MODULE__.forward_config_error/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler) end)

    enqueue!(inst, build_env("src1"))
    assert {:ok, _} = Pipeline.tick(inst)

    assert_receive {:config_error, %{sink: ConfigErrorSink, source_id: "src1", tenant_id: "acme"}}
    refute_receive {:config_error, _}, 100

    # dead after one attempt each: both permanent ones; the transient one retries
    assert {:ok, %{total: 2, entries: entries}} = Ankusa.Queue.dead(inst, limit: 10)

    assert entries |> Enum.map(& &1.reason) |> Enum.sort() ==
             Enum.sort([
               inspect({:sink, ConfigErrorSink, {:permanent, {:config, :access_denied}}}),
               inspect({:sink, PermanentSink, {:permanent, :bad_request}})
             ])
  end

  @doc false
  def forward_config_error(_event, _measurements, meta, pid),
    do: send(pid, {:config_error, meta})

  # ── config helpers ─────────────────────────────────────────────────────────

  defp persistent_config(inst, dir, roles, dispatch \\ %{}) do
    Ankusa.Config.new(
      instance: inst,
      data_dir: dir,
      port: 0,
      roles: roles,
      dispatch: dispatch,
      source_store: {Ankusa.SourceStore.Persistent, [decoder: &decode_source/2]}
    )
  end

  defp restart_config(inst, dir, sinks, dispatch, roles \\ [:edge, :dispatch]) do
    Ankusa.Config.new(
      instance: inst,
      data_dir: dir,
      port: 0,
      roles: roles,
      dispatch: dispatch,
      source_store: {Ankusa.SourceStore.Static, sources: %{"src1" => %{sinks: sinks}}}
    )
  end

  # A source the store knows as failing, with an optional `retry` override.
  defp decode_source(_source_id, %{"fail" => true} = spec) do
    retry = for {key, value} <- Map.get(spec, "retry", %{}), do: {String.to_atom(key), value}
    [sinks: [{AlwaysFail, []}]] ++ if(retry == [], do: [], else: [retry: retry])
  end

  defp decode_source(_source_id, %{"sinks" => [_ | _]}), do: [sinks: [{Ankusa.Sink.Log, []}]]

  defp decode_source(source_id, spec) do
    raise ArgumentError, "unexpected spec for #{source_id}: #{inspect(spec)}"
  end
end
