defmodule Ankusa.WAL.Ra.MachineTest do
  @moduledoc """
  Focused unit tests for `Ankusa.WAL.Ra.Machine.apply/3`, where the property
  suite's generated scripts do not reach: the append-retry contract, in
  particular, that a batch id reused with a *different* record count is refused
  rather than answered with results that do not match what was asked.
  """

  use ExUnit.Case, async: true

  alias Ankusa.WAL.Ra.Machine

  defp meta(index), do: %{index: index, term: 1, system_time: index * 1_000, machine_version: 1}

  test "a retry with the same batch_id and count returns the stored results" do
    batch = :crypto.strong_rand_bytes(16)
    records = [<<1>>, <<2>>]

    {state, reply, []} = Machine.apply(meta(1), {:append, batch, records}, Machine.init(%{}))

    assert reply == {:ok, [{:committed, 1}, {:committed, 2}]}

    # Same batch_id, same count: the stored results, and nothing new allocated.
    {state2, reply2, []} = Machine.apply(meta(2), {:append, batch, records}, state)
    assert reply2 == reply
    assert state2.next_seq == state.next_seq
  end

  test "a batch_id reused with a different record count is refused" do
    batch = :crypto.strong_rand_bytes(16)

    {state, _reply, []} =
      Machine.apply(meta(1), {:append, batch, [<<1>>, <<2>>]}, Machine.init(%{}))

    {_state2, reply2, []} = Machine.apply(meta(2), {:append, batch, [<<1>>]}, state)
    assert reply2 == {:error, :batch_id_conflict}
  end

  test "import bootstraps an empty log" do
    {state, reply, []} =
      Machine.apply(meta(1), {:import, 5, %{dispatch: 3}}, Machine.init(%{}))

    assert reply == :ok
    assert state.next_seq == 6
    assert state.cursors == %{dispatch: 3}
  end

  test "import is refused on a non-empty log" do
    {state, _reply, []} =
      Machine.apply(meta(1), {:append, :crypto.strong_rand_bytes(16), [<<1>>]}, Machine.init(%{}))

    {_state2, reply2, []} = Machine.apply(meta(2), {:import, 5, %{}}, state)
    assert reply2 == {:error, :not_empty}
  end
end
