defmodule Ankusa.WAL.PostgresTest do
  @moduledoc """
  Requires a live Postgres: `docker compose up -d --wait` in this directory.
  """

  use ExUnit.Case, async: false

  alias Ankusa.{Envelope, UUIDv7, WAL}
  alias Ankusa.WAL.Postgres

  @pg_opts [
    hostname: "localhost",
    port: 5433,
    username: "ankusa",
    password: "ankusa",
    database: "ankusa_dev",
    pool_size: 4
  ]

  defp boot(instance) do
    config = Ankusa.Config.new(instance: instance, wal: {Postgres, @pg_opts})
    Ankusa.put_config(config)
    start_supervised!({Postgres, [instance: instance, config: config]}, id: instance)
    instance
  end

  setup do
    %{instance: boot(:"pg_#{System.unique_integer([:positive])}")}
  end

  defp envelope(overrides \\ %{}) do
    base = %Envelope{
      id: UUIDv7.generate(),
      source_id: "src",
      tenant_id: "t1",
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/hooks/src",
      headers: [],
      body: "payload",
      size: 7
    }

    struct(base, overrides)
  end

  defp entry(overrides \\ %{}), do: %{envelope: envelope(overrides)}

  test "put/get round-trips via append + read", %{instance: inst} do
    env = envelope()
    assert {:ok, [{:committed, committed}]} = WAL.append(inst, [%{envelope: env}])
    assert committed.seq
    assert committed.id == env.id

    [read_back] = WAL.read(inst, -1, 10)
    assert read_back.id == env.id
    assert read_back.body == "payload"
    assert read_back.seq == committed.seq
  end

  test "a nil tenant_id is coalesced at the storage boundary, not rejected", %{instance: inst} do
    assert {:ok, [{:committed, committed}]} = WAL.append(inst, [%{envelope: envelope(%{tenant_id: nil})}])
    assert committed.seq
  end

  test "intra-batch duplicates collapse to one commit", %{instance: inst} do
    id = "evt-#{System.unique_integer([:positive])}"
    a = entry(%{dedup_key: id})
    b = entry(%{dedup_key: id})

    assert {:ok, [{:committed, committed}, {:duplicate, dup_seq}]} = WAL.append(inst, [a, b])
    assert dup_seq == committed.seq
    assert length(WAL.read(inst, -1, 10)) == 1
  end

  test "cross-batch duplicate returns the original seq without writing again", %{instance: inst} do
    id = "evt-#{System.unique_integer([:positive])}"
    {:ok, [{:committed, first}]} = WAL.append(inst, [entry(%{dedup_key: id})])
    assert {:ok, [{:duplicate, dup_seq}]} = WAL.append(inst, [entry(%{dedup_key: id})])
    assert dup_seq == first.seq
    assert length(WAL.read(inst, -1, 10)) == 1
  end

  test "nil dedup_key never collides, even across otherwise-identical envelopes", %{
    instance: inst
  } do
    assert {:ok, [{:committed, a}]} = WAL.append(inst, [entry()])
    assert {:ok, [{:committed, b}]} = WAL.append(inst, [entry()])
    assert a.seq != b.seq
    assert length(WAL.read(inst, -1, 10)) == 2
  end

  test "concurrent writers racing the same dedup key: exactly one wins", %{instance: inst} do
    id = "evt-#{System.unique_integer([:positive])}"

    results =
      1..8
      |> Enum.map(fn _ -> Task.async(fn -> WAL.append(inst, [entry(%{dedup_key: id})]) end) end)
      |> Enum.map(&Task.await(&1, 5_000))
      |> Enum.map(fn {:ok, [result]} -> result end)

    committed = Enum.filter(results, &match?({:committed, _}, &1))
    duplicates = Enum.filter(results, &match?({:duplicate, _}, &1))

    assert length(committed) == 1
    assert length(duplicates) == 7
    [{:committed, winner}] = committed
    assert Enum.all?(duplicates, fn {:duplicate, seq} -> seq == winner.seq end)
    assert length(WAL.read(inst, -1, 10)) == 1
  end

  test "truncate_through deletes WAL rows but dedup keys still block a re-send", %{
    instance: inst
  } do
    id = "evt-#{System.unique_integer([:positive])}"
    {:ok, [{:committed, first}]} = WAL.append(inst, [entry(%{dedup_key: id})])

    :ok = WAL.truncate_through(inst, first.seq)
    assert WAL.read(inst, -1, 10) == []

    # the row is physically gone, but the dedup ledger is permanent
    assert {:ok, [{:duplicate, dup_seq}]} = WAL.append(inst, [entry(%{dedup_key: id})])
    assert dup_seq == first.seq
  end

  test "cursors default to 0 and persist", %{instance: inst} do
    assert WAL.get_cursor(inst, :dispatch) == 0
    assert :ok = WAL.put_cursor(inst, :dispatch, 42)
    assert WAL.get_cursor(inst, :dispatch) == 42
  end

  test "stats reflect committed records and cursors", %{instance: inst} do
    {:ok, _} = WAL.append(inst, [entry(), entry()])
    :ok = WAL.put_cursor(inst, :dispatch, 1)

    stats = WAL.stats(inst)
    assert stats.records == 2
    assert stats.next_seq == stats.max_seq + 1
    assert stats.cursors["dispatch"] == 1
  end

  test "two instances sharing one database never see each other's rows or dedup keys" do
    inst_a = boot(:"pg_iso_a_#{System.unique_integer([:positive])}")
    inst_b = boot(:"pg_iso_b_#{System.unique_integer([:positive])}")
    id = "evt-#{System.unique_integer([:positive])}"
    assert {:ok, [{:committed, _}]} = WAL.append(inst_a, [entry(%{dedup_key: id})])
    # same dedup key, different instance — must commit independently, not dedupe
    assert {:ok, [{:committed, _}]} = WAL.append(inst_b, [entry(%{dedup_key: id})])

    assert length(WAL.read(inst_a, -1, 10)) == 1
    assert length(WAL.read(inst_b, -1, 10)) == 1
  end
end
