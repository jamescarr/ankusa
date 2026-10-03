defmodule Ankusa.InstanceTest do
  @moduledoc """
  The instance's failure domains: an optional subtree that keeps crashing is
  restarted with backoff and never takes the edge listener down, and crash
  reports do not print configuration.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Edge.{Batcher, Ingest}
  alias Ankusa.Instance.Isolated

  @canary "s3cr3t-canary"

  defmodule CaptureSink do
    @moduledoc false
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      send(Keyword.fetch!(opts, :to), {:delivered, env.id})
      :ok
    end
  end

  # A subtree child that refuses to start while the `Agent` flag says `:fail`.
  defmodule FlakyChild do
    @moduledoc false

    def child_spec(flag),
      do: %{id: __MODULE__, start: {__MODULE__, :start_link, [flag]}}

    def start_link(flag) do
      case Agent.get(flag, & &1) do
        :fail -> {:error, :nope}
        :ok -> Agent.start_link(fn -> :up end)
      end
    end
  end

  @doc false
  def handle_event(event, measurements, metadata, test_pid),
    do: send(test_pid, {:telemetry, event, measurements, metadata})

  defp attach_subtree_events(inst) do
    id = {__MODULE__, inst}

    :telemetry.attach_many(
      id,
      [[:ankusa, :instance, :subtree_down], [:ankusa, :instance, :subtree_up]],
      &__MODULE__.handle_event/4,
      self()
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  defp eventually(fun, tries \\ 300) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        flunk("condition never became true")

      true ->
        Process.sleep(10)
        eventually(fun, tries - 1)
    end
  end

  # Kill `pid_fun.()` over and over, waiting for the supervisor to bring it
  # back each time, until the subtree gives up (its budget is 3 restarts in
  # 5 s, so four kills).
  defp kill_until_down(inst, domain, pid_fun, kills \\ 10) do
    cond do
      Isolated.subtree(inst, domain) == nil ->
        :ok

      kills == 0 ->
        flunk("#{domain} subtree never exhausted its restart budget")

      true ->
        pid = pid_fun.()
        Process.exit(pid, :kill)
        eventually(fn -> pid_fun.() != pid end)
        kill_until_down(inst, domain, pid_fun, kills - 1)
    end
  end

  @tag :capture_log
  test "a dispatch subtree that exhausts its restart budget leaves the edge up and comes back" do
    config =
      test_config(
        roles: [:edge, :dispatch],
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{"demo" => [sinks: [{CaptureSink, [to: self()]}]]}}
      )

    inst = config.instance
    attach_subtree_events(inst)
    start_supervised!({Ankusa.Instance, config})

    pids = fn ->
      Enum.map([:edge, {:batcher, 0}, :store, :instance], &Ankusa.whereis(inst, &1))
    end

    before = pids.()
    assert Enum.all?(before, &is_pid/1)

    kill_until_down(inst, :dispatch, fn -> Ankusa.whereis(inst, :dispatch) end)

    assert_receive {:telemetry, [:ankusa, :instance, :subtree_down], %{delay_ms: 1000},
                    %{instance: ^inst, domain: :dispatch}}

    # Dispatch is gone for now; nothing the edge needs went with it.
    assert pids.() == before

    assert {:ok, env} = Ingest.ingest(inst, request("demo", "x"))
    id = env.id

    # ...and the hook is delivered once the subtree is restarted.
    assert_receive {:telemetry, [:ankusa, :instance, :subtree_up], _, %{domain: :dispatch}}, 5_000
    assert_receive {:delivered, ^id}, 5_000
    assert pids.() == before
  end

  @tag :capture_log
  test "a restart that fails is retried with a longer delay, and the manager stays up" do
    inst = unique_instance()
    attach_subtree_events(inst)
    flag = start_supervised!({Agent, fn -> :ok end})

    manager =
      start_supervised!(
        {Isolated,
         instance: inst,
         domain: :flaky,
         children: [{FlakyChild, flag}],
         base_backoff_ms: 50,
         max_backoff_ms: 200}
      )

    # While the flag says `:fail` every restart attempt is refused, so one kill
    # spends the subtree's budget, and every retry after it fails too.
    Agent.update(flag, fn _ -> :fail end)
    sup = Isolated.subtree(inst, :flaky)
    [{_id, child, _type, _modules}] = Supervisor.which_children(sup)
    Process.exit(child, :kill)

    assert_receive {:telemetry, [:ankusa, :instance, :subtree_down], %{delay_ms: 50},
                    %{domain: :flaky}},
                   2_000

    assert_receive {:telemetry, [:ankusa, :instance, :subtree_down], %{delay_ms: 100},
                    %{domain: :flaky}},
                   2_000

    assert Isolated.subtree(inst, :flaky) == nil
    assert Ankusa.whereis(inst, {:isolated, :flaky}) == manager

    Agent.update(flag, fn _ -> :ok end)
    assert_receive {:telemetry, [:ankusa, :instance, :subtree_up], _, %{domain: :flaky}}, 2_000

    assert is_pid(Isolated.subtree(inst, :flaky))
    assert Ankusa.whereis(inst, {:isolated, :flaky}) == manager
  end

  @tag :capture_log
  test "a Registry partition crash rebuilds the instance with every process registered again" do
    config =
      test_config(
        roles: [:edge],
        source_store:
          {Ankusa.SourceStore.Static, sources: %{"demo" => [sinks: [{Ankusa.Sink.Log, []}]]}}
      )

    inst = config.instance
    start_supervised!({Ankusa.Instance, config})

    keys = [:instance, :store, :edge, :batcher_sup, {:batcher, 0}, :queue_writer]
    snapshot = fn -> Map.new(keys, &{&1, Ankusa.whereis(inst, &1)}) end
    before = snapshot.()
    assert Enum.all?(before, fn {_key, pid} -> is_pid(pid) end)

    # The Registry's partition owns every registration. The store and the
    # supervisors trap exits, so they outlive it unregistered unless the
    # instance is rebuilt.
    [{_id, partition, _type, _modules} | _] = Supervisor.which_children(Ankusa.Registry)
    Process.exit(partition, :kill)

    eventually(fn ->
      # `lookup/2` raises while the partition's tables are being recreated.
      try do
        now = snapshot.()
        Enum.all?(now, fn {_key, pid} -> is_pid(pid) end) and now.store != before.store
      rescue
        ArgumentError -> false
      end
    end)

    assert {:ok, _env} = Ingest.ingest(inst, request("demo", "x"))
  end

  @tag :capture_log
  test "a crash while handling a hook or a source write does not log the sinks it carried" do
    sink = {Ankusa.Sink.Log, [token: @canary]}

    config =
      test_config(
        roles: [:edge],
        batcher: %{partitions: 1},
        source_store: {Ankusa.SourceStore.Persistent, sources: %{}}
      )

    inst = config.instance
    start_supervised!({Ankusa.Instance, config})

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        # Break each process's state so the next call it handles raises; the
        # crash report then prints that call as its "Last message".
        batcher = Ankusa.whereis(inst, {:batcher, 0})
        :sys.replace_state(batcher, &%{&1 | count: :broken})
        catch_exit(Batcher.commit(inst, 0, %{envelope: nil, sinks: [sink]}))

        store = Ankusa.whereis(inst, :source_store)
        :sys.replace_state(store, &%{&1 | table: :no_such_table})
        spec = %{"sinks" => [%{"type" => "log", "token" => @canary}]}
        catch_exit(Ankusa.SourceStore.put(inst, "acme", "billing", spec, :create))

        # Both reports are logged before the processes exit; give the handler
        # a moment to write them.
        Process.sleep(200)
      end)

    assert log =~ "Ankusa.Edge.Batcher"
    assert log =~ "Ankusa.SourceStore.Persistent"
    refute log =~ @canary
  end

  test "no process the instance runs prints configuration in its status" do
    sink = {Ankusa.Sink.Log, [token: @canary]}

    config =
      test_config(
        roles: [:edge, :dispatch, :storage],
        claim_check: %{retention_days: 7},
        lifecycle: %{sinks: [sink]},
        source_store: {Ankusa.SourceStore.Persistent, sources: %{"demo" => [sinks: [sink]]}}
      )

    inst = config.instance
    start_supervised!({Ankusa.Instance, config})

    printed = fn key ->
      inst
      |> Ankusa.whereis(key)
      |> :sys.get_status()
      |> inspect(limit: :infinity, printable_limit: :infinity)
    end

    # The canary is in the state to begin with: the redaction is what hides it.
    raw = inst |> Ankusa.whereis(:dispatch) |> :sys.get_state() |> inspect(limit: :infinity)
    assert raw =~ @canary

    for key <- [
          :dispatch,
          :compactor,
          :claim_check_sweeper,
          :rate_limiter,
          :lifecycle,
          :source_store,
          {:isolated, :dispatch}
        ] do
      refute printed.(key) =~ @canary, "#{inspect(key)} printed the config in its status"
    end
  end
end
