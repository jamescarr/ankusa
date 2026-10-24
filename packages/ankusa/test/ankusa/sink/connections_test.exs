defmodule Ankusa.Sink.ConnectionsTest do
  # The tree's names are this module's alone, and tests in one module never run
  # at the same time.
  use ExUnit.Case, async: true

  alias Ankusa.Sink.Connections
  alias Ankusa.Sink.ConnectionsTest.Fake

  setup do
    start_supervised!(%{id: Fake, start: {Connections, :start_link, [Fake]}, type: :supervisor})
    :ok
  end

  def failing_start, do: {:error, :boom}

  defp agent(id, opts \\ []),
    do: %{id: id, start: {Agent, :start_link, [fn -> :s end, opts]}, restart: :temporary}

  defp row(key), do: :ets.lookup(Fake.Reaper, key)

  test "start_link/1 starts the sink's supervisor and reaper under its own names" do
    assert is_pid(Process.whereis(Fake.TopSup))
    assert is_pid(Process.whereis(Fake.Supervisor))
    assert is_pid(Process.whereis(Fake.Reaper))
    assert :ets.whereis(Fake.Reaper) != :undefined
  end

  test "nothing running: the child starts under the sink's supervisor and is touched" do
    assert {:ok, pid} = Connections.ensure(Fake, :k, fn -> nil end, fn -> agent(:c) end, 1_000)

    assert [{:undefined, ^pid, :worker, _}] = DynamicSupervisor.which_children(Fake.Supervisor)
    assert [{:k, ^pid, _last, 1_000}] = row(:k)
  end

  test "a running connection is used as-is, and its idle clock restarts" do
    {:ok, pid} = Connections.ensure(Fake, :k, fn -> nil end, fn -> agent(:c) end, 1_000)
    [{:k, ^pid, first, _}] = row(:k)

    assert {:ok, ^pid} =
             Connections.ensure(Fake, :k, fn -> pid end, fn -> flunk("started twice") end, 1_000)

    assert [{:k, ^pid, last, 1_000}] = row(:k)
    assert last >= first
  end

  test "a start that loses the race is the winner's pid" do
    child = fn -> agent(:raced, name: Fake.Raced) end

    assert {:ok, pid} = Connections.ensure(Fake, :k, fn -> nil end, child, 1_000)
    assert {:ok, ^pid} = Connections.ensure(Fake, :k, fn -> nil end, child, 1_000)
    assert [_one] = DynamicSupervisor.which_children(Fake.Supervisor)
  end

  test "idle_ms 0 is never reaped: no row" do
    assert {:ok, _pid} = Connections.ensure(Fake, :k, fn -> nil end, fn -> agent(:c) end, 0)
    assert row(:k) == []
  end

  test "a start that fails is the error, and nothing is touched" do
    child = fn -> %{id: :f, start: {__MODULE__, :failing_start, []}} end

    assert Connections.ensure(Fake, :k, fn -> nil end, child, 1_000) == {:error, :boom}
    assert row(:k) == []
  end
end
