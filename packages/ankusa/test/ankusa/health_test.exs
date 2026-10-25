defmodule Ankusa.HealthTest do
  @moduledoc """
  `Ankusa.Health.ready/1` and `GET /ready`: every check that applies to a node
  lists its state, and the ones that gate make it `503`. Covers the store, the
  `wal: :dual` intake log, a direct-mode (`wal: :none`) node whose sink fails, a
  data dir under its free-space floor, and a backlog over its threshold.
  """
  use ExUnit.Case, async: false

  @moduletag capture_log: true

  # Core does not depend on `:os_mon`; the disk tests start it themselves.
  @compile {:no_warn_undefined, :disksup}

  import Ankusa.TestHelpers, only: [test_config: 1]
  import Ankusa.Test.Dual

  alias Ankusa.Edge.Router
  alias Ankusa.Health
  alias Ankusa.Intake.{Gate, Memory, Pending}

  defmodule FlakySink do
    @moduledoc "A sink that answers what its agent holds: `:ok`, `{:error, reason}`, or `:raise`."
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, opts) do
      case Agent.get(Keyword.fetch!(opts, :agent), & &1) do
        :raise -> raise "sink exploded"
        answer -> answer
      end
    end
  end

  defmodule DrainingCheck do
    @moduledoc false
    @behaviour Ankusa.Health.Check

    @impl true
    def name, do: :drain
    @impl true
    def applies?(_config), do: true
    @impl true
    def gates?(_config), do: true
    @impl true
    def run(_config), do: {:draining, :shutting_down}
  end

  defmodule RaisingCheck do
    @moduledoc false
    @behaviour Ankusa.Health.Check

    @impl true
    def name, do: :boom
    @impl true
    def applies?(_config), do: true
    @impl true
    def gates?(_config), do: false
    @impl true
    def run(_config), do: raise("the check broke")
  end

  defmodule FailingGateCheck do
    @moduledoc false
    @behaviour Ankusa.Health.Check

    @impl true
    def name, do: :boom
    @impl true
    def applies?(_config), do: true
    @impl true
    def gates?(_config), do: true
    @impl true
    def run(_config), do: {:failing, :broken}
  end

  defp get_ready(config) do
    Router.call(Plug.Test.conn(:get, "/ready"), Router.init(instance: config.instance))
  end

  defp post(config) do
    Router.call(
      Plug.Test.conn(:post, "/webhooks/demo", ~s({"hello":"world"})),
      Router.init(instance: config.instance)
    )
  end

  defp start_direct(agent, listener \\ %{}) do
    config =
      test_config(
        roles: [:edge],
        wal: :none,
        listener: listener,
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{"demo" => [sinks: [{FlakySink, [agent: agent]}]]}}
      )

    start_supervised!({Ankusa.Instance, config})
    config
  end

  # The free bytes of the file system `dir` is on, from `:disksup`.
  defp free_bytes(dir) do
    [{_id, _total_kib, available_kib, _capacity} | _] =
      :disksup.get_disk_info(String.to_charlist(Path.expand(dir)))

    available_kib * 1024
  end

  # `:os_mon` runs for the test's length only: core does not start it, and the
  # gauges would otherwise report disk state in every test after this one. Mix
  # prunes the code path to the applications core declares, so `:os_mon` (and
  # the `:sasl` it needs) are put back on it first.
  defp with_os_mon do
    for app <- ~w(os_mon sasl),
        ebin <- Path.wildcard(Path.join([to_string(:code.root_dir()), "lib", "#{app}-*", "ebin"])) do
      Code.prepend_path(ebin)
    end

    {:ok, started} = Application.ensure_all_started(:os_mon)
    if :os_mon in started, do: on_exit(fn -> Application.stop(:os_mon) end)
  end

  defp without_os_mon do
    Application.stop(:os_mon)
    :ok
  end

  test "ready reports intake ok, failing (down) and failing (recovering)" do
    config = start_dual(intake: %{breaker_failures: 1})
    inst = config.instance
    name = to_string(inst)
    t = Pending.table(inst)

    assert {:ok,
            %{status: "ready", instance: ^name, store: "ok", intake: "ok", backup: "ok"} = ok} =
             Health.ready(inst)

    refute Map.has_key?(ok, :reasons)

    # boot replay running
    Pending.put_meta(t, :state, :recovering)

    assert {:error, %{status: "unavailable", intake: "failing", reasons: %{intake: "recovering"}}} =
             Health.ready(inst)

    Pending.put_meta(t, :state, :up)

    # the breaker is open and the log is not answering: drained
    Memory.set_ready(inst, false)
    Memory.fail(inst, :unavailable)
    assert {:error, _} = ingest(config, "evt-1")
    assert wait_until(fn -> Gate.state(t) == :open end)

    assert {:error,
            %{
              status: "unavailable",
              intake: "failing",
              store: "ok",
              reasons: %{intake: "down"}
            }} = Health.ready(inst)

    # open but the log answers again: ready for the one batch that probes it
    Memory.set_ready(inst, true)
    assert {:ok, %{intake: "ok"}} = Health.ready(inst)
  end

  test "a node lists only the checks that apply to it" do
    config = Ankusa.Test.Dual.dual_config(wal: :disk)
    ExUnit.Callbacks.start_supervised!({Ankusa.Instance, config})
    assert {:ok, body} = Health.ready(config.instance)

    # `wal: :disk` with the defaults: the store, and the backup the fixture turns on.
    assert Map.keys(body) |> Enum.sort() == [:backup, :instance, :status, :store]

    # A node that runs no store (only the claim check gateway) has no `store`
    # check, and the sinks one is for `wal: :none` alone.
    refute Health.Check.Store.applies?(Ankusa.Config.new(roles: [:claim_check]))
    refute Health.Check.Sinks.applies?(config)

    # `wal: :none` still runs the store (quarantine, audit): both checks apply.
    agent = start_supervised!({Agent, fn -> :ok end})
    direct = start_direct(agent)
    assert {:ok, body} = Health.ready(direct.instance)
    assert Map.keys(body) |> Enum.sort() == [:instance, :sinks, :status, :store]
  end

  test "an unready store names why, from a fixed set" do
    config = Ankusa.Test.Dual.dual_config(wal: :disk)
    inst = config.instance
    name = to_string(inst)
    ExUnit.Callbacks.start_supervised!({Ankusa.Instance, config})
    assert {:ok, %{store: "ok"} = ok} = Health.ready(inst)
    refute Map.has_key?(ok, :reasons)

    # A refused ingest batch (a full disk) the node was told about.
    Ankusa.Store.report_write_failure(inst, ~c"IO error: No space left on device: While append")

    assert {:error,
            %{
              status: "unavailable",
              instance: ^name,
              store: "failing",
              reasons: %{store: "no_space"}
            }} = Health.ready(inst)

    # A store that could not reopen is closed, and says why.
    pid = GenServer.whereis(Ankusa.via(inst, :store))
    good = :sys.get_state(pid).path
    blocker = Path.join(config.data_dir, "blocker")
    File.write!(blocker, "a file where a directory has to be")
    :sys.replace_state(pid, fn state -> %{state | path: Path.join(blocker, "store")} end)
    assert {:error, _reason} = Ankusa.Store.reopen(inst)

    assert {:error, %{status: "unavailable", store: "failing", reasons: %{store: reason}}} =
             Health.ready(inst)

    assert reason in ~w(no_space io_error read_only corruption busy other)

    :sys.replace_state(pid, fn state -> %{state | path: good} end)
    assert :ok = Ankusa.Store.reopen(inst)
  end

  describe "sinks, under wal: :none" do
    test "a sink whose publish failed makes the node unready until it publishes again" do
      agent = start_supervised!({Agent, fn -> {:error, :down} end})
      config = start_direct(agent)
      inst = config.instance
      name = to_string(inst)

      # Nothing has been published yet: ready.
      assert {:ok, %{status: "ready", sinks: "ok"}} = Health.ready(inst)
      assert get_ready(config).status == 200

      # The broker refuses: the hook is a 503, and so is the node's readiness.
      assert post(config).status == 503

      assert {:error,
              %{
                status: "unavailable",
                instance: ^name,
                sinks: "failing",
                reasons: %{sinks: "publish_failed"}
              }} = Health.ready(inst)

      ready = get_ready(config)
      assert ready.status == 503
      assert Plug.Conn.get_resp_header(ready, "retry-after") == ["1"]

      assert JSON.decode!(ready.resp_body) == %{
               "status" => "unavailable",
               "instance" => name,
               "store" => "ok",
               "sinks" => "failing",
               "reasons" => %{"sinks" => "publish_failed"}
             }

      # The broker is back: the first publish that confirms clears it, without
      # waiting out the window.
      Agent.update(agent, fn _ -> :ok end)
      assert post(config).status == 201
      assert {:ok, %{sinks: "ok"}} = Health.ready(inst)
      assert get_ready(config).status == 200
    end

    test "a failure ages out after health.sink_error_window_ms" do
      agent = start_supervised!({Agent, fn -> {:error, :down} end})
      config = start_direct(agent, %{ready_sink_error_window_ms: 50})

      assert post(config).status == 503
      assert {:error, %{sinks: "failing"}} = Health.ready(config.instance)

      assert wait_until(fn -> match?({:ok, %{sinks: "ok"}}, Health.ready(config.instance)) end)
    end

    test "a sink that raises counts as failing too" do
      agent = start_supervised!({Agent, fn -> :ok end})
      config = start_direct(agent)

      Agent.update(agent, fn _ -> :raise end)
      assert post(config).status == 503
      assert {:error, %{sinks: "failing"}} = Health.ready(config.instance)
    end
  end

  describe "disk, with health.min_free_bytes" do
    test "a data dir with less free space than the floor is unready" do
      with_os_mon()
      floor = free_bytes(System.tmp_dir!()) + 1_099_511_627_776

      config = test_config(roles: [:edge], wal: :disk, listener: %{ready_min_free_bytes: floor})
      start_supervised!({Ankusa.Instance, config})

      assert {:error,
              %{
                status: "unavailable",
                store: "ok",
                disk: "failing",
                reasons: %{disk: "below_floor"}
              }} = Health.ready(config.instance)

      ready = get_ready(config)
      assert ready.status == 503
      assert %{"disk" => "failing"} = JSON.decode!(ready.resp_body)
    end

    test "a floor the volume meets is ready" do
      with_os_mon()

      config = test_config(roles: [:edge], wal: :disk, listener: %{ready_min_free_bytes: 1})
      start_supervised!({Ankusa.Instance, config})

      assert {:ok, %{status: "ready", store: "ok", disk: "ok"}} = Health.ready(config.instance)
      assert get_ready(config).status == 200
    end

    test "a floor that cannot be measured fails instead of being skipped" do
      without_os_mon()

      config = test_config(roles: [:edge], wal: :disk, listener: %{ready_min_free_bytes: 1})
      start_supervised!({Ankusa.Instance, config})

      assert {:error, %{disk: "failing", reasons: %{disk: "unmeasured"}}} =
               Health.ready(config.instance)
    end
  end

  describe "backlog, with queue.max_oldest_due_ms" do
    test "a backlog over the threshold is a label: failing, and still 200" do
      config = dual_config(wal: :disk, queue: %{max_oldest_due_ms: 1})
      start_supervised!({Ankusa.Instance, config})
      inst = config.instance

      # Nothing is waiting.
      assert {:ok, %{backlog: "ok"}} = Health.ready(inst)

      # Only the edge role runs, so nothing claims the hook: its row stays due.
      assert {:ok, _hook} = ingest(config, "evt-backlog")

      assert wait_until(fn -> match?({:ok, %{backlog: "failing"}}, Health.ready(inst)) end)

      assert {:ok,
              %{
                status: "ready",
                store: "ok",
                backlog: "failing",
                reasons: %{backlog: "over_threshold"}
              }} = Health.ready(inst)

      ready = get_ready(config)
      assert ready.status == 200
      assert %{"backlog" => "failing"} = JSON.decode!(ready.resp_body)
    end

    test "a backlog under the threshold is ok" do
      config = dual_config(wal: :disk, queue: %{max_oldest_due_ms: 3_600_000})
      start_supervised!({Ankusa.Instance, config})

      assert {:ok, _hook} = ingest(config, "evt-fresh")
      assert {:ok, %{backlog: "ok"}} = Health.ready(config.instance)
    end
  end

  describe "the registry" do
    setup do
      config = start_dual()
      {:ok, instance: config.instance}
    end

    test "a check that reports draining makes the node unready, and the status says so", %{
      instance: inst
    } do
      assert {:error,
              %{status: "draining", drain: "draining", reasons: %{drain: "shutting_down"}}} =
               Health.evaluate(inst, [DrainingCheck])
    end

    test "a check that raises is failing with reason error, and never crashes the probe", %{
      instance: inst
    } do
      assert {:ok, %{status: "ready", boom: "failing", reasons: %{boom: "error"}}} =
               Health.evaluate(inst, [RaisingCheck])
    end

    test "every registered check has a distinct body key" do
      names = Enum.map(Ankusa.Health.Check.all(), & &1.name())
      assert names == Enum.uniq(names)
      assert names == [:drain, :store, :intake, :sinks, :disk, :backlog, :backup]
    end

    test "Health.drain/1 adds the drain check: /ready is 503 draining until the instance restarts",
         %{instance: inst} do
      config = Ankusa.config(inst)
      on_exit(fn -> Health.undrain(inst) end)

      refute Health.draining?(inst)
      assert {:ok, ready} = Health.ready(inst)
      refute Map.has_key?(ready, :drain)

      assert :ok = Health.drain(inst)
      assert Health.draining?(inst)
      conn = get_ready(config)
      assert conn.status == 503
      assert Plug.Conn.get_resp_header(conn, "retry-after") == ["1"]

      assert %{
               "status" => "draining",
               "drain" => "draining",
               "store" => "ok",
               "reasons" => %{"drain" => "shutting_down"}
             } = JSON.decode!(conn.resp_body)

      assert :ok = Health.undrain(inst)
      assert {:ok, %{status: "ready"}} = Health.ready(inst)
    end

    test "a draining node that also has a failing check is still draining", %{instance: inst} do
      on_exit(fn -> Health.undrain(inst) end)
      Health.drain(inst)

      assert {:error, %{status: "draining", boom: "failing", drain: "draining"}} =
               Health.evaluate(inst, [Ankusa.Health.Check.Drain, FailingGateCheck])
    end
  end
end
