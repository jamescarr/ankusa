defmodule Ankusa.Sink.ReaperTest do
  use ExUnit.Case, async: true

  alias Ankusa.Sink.Reaper

  setup do
    n = System.unique_integer([:positive])
    sup = :"reaper_sup_#{n}"
    name = :"reaper_#{n}"

    start_supervised!({DynamicSupervisor, name: sup, strategy: :one_for_one})
    start_supervised!({Reaper, name: name, supervisor: sup, tick_ms: 3_600_000})

    %{sup: sup, name: name}
  end

  # A `:permanent` child, as the adapters' connections are.
  defp start_conn(sup) do
    child = %{id: make_ref(), start: {Agent, :start_link, [fn -> nil end]}, restart: :permanent}
    {:ok, pid} = DynamicSupervisor.start_child(sup, child)
    pid
  end

  # One sweep, run to completion before returning.
  defp sweep(name) do
    send(name, :tick)
    :sys.get_state(name)
  end

  test "a connection left idle past its idle_ms is stopped and not restarted", %{
    sup: sup,
    name: name
  } do
    pid = start_conn(sup)
    ref = Process.monitor(pid)
    :ok = Reaper.touch(name, :conn, pid, 50)

    sweep(name)
    assert Process.alive?(pid)

    Process.sleep(60)
    sweep(name)

    assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}
    assert DynamicSupervisor.which_children(sup) == []
    assert :ets.lookup(name, :conn) == []
  end

  test "a touch restarts the idle clock", %{sup: sup, name: name} do
    pid = start_conn(sup)
    :ok = Reaper.touch(name, :conn, pid, 80)
    Process.sleep(50)
    :ok = Reaper.touch(name, :conn, pid, 80)
    Process.sleep(50)

    sweep(name)
    assert Process.alive?(pid)
  end

  test "idle_ms 0 is never stopped", %{sup: sup, name: name} do
    pid = start_conn(sup)
    :ok = Reaper.touch(name, :conn, pid, 50)
    :ok = Reaper.touch(name, :conn, pid, 0)
    Process.sleep(60)

    sweep(name)
    assert Process.alive?(pid)
    assert :ets.lookup(name, :conn) == []
  end

  test "a row whose connection already died is dropped", %{sup: sup, name: name} do
    pid = start_conn(sup)
    :ok = Reaper.touch(name, :conn, pid, 3_600_000)
    :ok = DynamicSupervisor.terminate_child(sup, pid)

    sweep(name)
    assert :ets.lookup(name, :conn) == []
  end

  test "only idle connections go; the others stay", %{sup: sup, name: name} do
    idle = start_conn(sup)
    busy = start_conn(sup)
    :ok = Reaper.touch(name, {:conn, 1}, idle, 30)
    :ok = Reaper.touch(name, {:conn, 2}, busy, 3_600_000)
    Process.sleep(40)

    sweep(name)
    refute Process.alive?(idle)
    assert Process.alive?(busy)
  end

  describe "idle_ms/2" do
    test "defaults to 10 minutes, keeps 0, and never goes under the floor" do
      assert Reaper.idle_ms([], 6_000) == 600_000
      assert Reaper.idle_ms([idle_timeout_ms: 0], 6_000) == 0
      assert Reaper.idle_ms([idle_timeout_ms: 1_000], 6_000) == 6_000
      assert Reaper.idle_ms([idle_timeout_ms: 60_000], 6_000) == 60_000
    end

    test "rejects anything but a non-negative integer" do
      for bad <- [-1, 1.5, "60000"] do
        assert_raise ArgumentError, ~r/:idle_timeout_ms/, fn ->
          Reaper.idle_ms([idle_timeout_ms: bad], 6_000)
        end
      end
    end
  end
end
