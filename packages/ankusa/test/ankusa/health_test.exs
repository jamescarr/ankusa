defmodule Ankusa.HealthTest do
  @moduledoc """
  Readiness under `wal: :dual`: `intake` is `up`, `down` or `recovering`.
  """
  use ExUnit.Case, async: false

  @moduletag capture_log: true

  import Ankusa.Test.Dual

  alias Ankusa.Health
  alias Ankusa.Intake.{Gate, Memory, Pending}

  test "ready reports intake up, down and recovering" do
    config = start_dual(intake: %{breaker_failures: 1})
    inst = config.instance
    name = to_string(inst)
    t = Pending.table(inst)

    assert {:ok, %{status: "ready", instance: ^name, store: "ok", intake: "up", backup: _}} =
             Health.ready(inst)

    # boot replay running
    Pending.put_meta(t, :state, :recovering)

    assert {:error, %{status: "unavailable", intake: "recovering"}} = Health.ready(inst)
    Pending.put_meta(t, :state, :up)

    # the breaker is open and the log is not answering: drained
    Memory.set_ready(inst, false)
    Memory.fail(inst, :unavailable)
    assert {:error, _} = ingest(config, "evt-1")
    assert wait_until(fn -> Gate.state(t) == :open end)

    assert {:error, %{status: "unavailable", intake: "down", store: "ok"}} = Health.ready(inst)

    # open but the log answers again: ready for the one batch that probes it
    Memory.set_ready(inst, true)
    assert {:ok, %{intake: "up"}} = Health.ready(inst)
  end

  test "other modes carry no intake key" do
    config = Ankusa.Test.Dual.dual_config(wal: :disk)
    ExUnit.Callbacks.start_supervised!({Ankusa.Instance, config})
    assert {:ok, body} = Health.ready(config.instance)
    refute Map.has_key?(body, :intake)
  end

  test "an unready store names why, from a fixed set" do
    config = Ankusa.Test.Dual.dual_config(wal: :disk)
    inst = config.instance
    name = to_string(inst)
    ExUnit.Callbacks.start_supervised!({Ankusa.Instance, config})
    assert {:ok, %{store: "ok"} = ok} = Health.ready(inst)
    refute Map.has_key?(ok, :reason)

    # A refused ingest batch (a full disk) the node was told about.
    Ankusa.Store.report_write_failure(inst, ~c"IO error: No space left on device: While append")

    assert {:error,
            %{status: "unavailable", instance: ^name, store: "write_failed", reason: "no_space"}} =
             Health.ready(inst)

    # A store that could not reopen is closed, and says why.
    pid = GenServer.whereis(Ankusa.via(inst, :store))
    good = :sys.get_state(pid).path
    blocker = Path.join(config.data_dir, "blocker")
    File.write!(blocker, "a file where a directory has to be")
    :sys.replace_state(pid, fn state -> %{state | path: Path.join(blocker, "store")} end)
    assert {:error, _reason} = Ankusa.Store.reopen(inst)

    assert {:error, %{status: "unavailable", store: "store_unavailable", reason: reason}} =
             Health.ready(inst)

    assert reason in ~w(no_space io_error read_only corruption busy other)

    :sys.replace_state(pid, fn state -> %{state | path: good} end)
    assert :ok = Ankusa.Store.reopen(inst)
  end
end
