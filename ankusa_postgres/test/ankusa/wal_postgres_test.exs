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
    # Not just `unique_integer/1`: that is unique within a VM, not across
    # restarts, and this adapter keeps its cursors and leases in a database that
    # outlives the test run.
    suffix = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
    %{instance: boot(:"pg_#{System.unique_integer([:positive])}#{suffix}")}
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
    assert {:ok, [{:committed, committed}]} =
             WAL.append(inst, [%{envelope: envelope(%{tenant_id: nil})}])

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

    Ankusa.WAL.LeaseHelpers.with_lease(inst, :storage, fn lease ->
      :ok = WAL.truncate_through(inst, first.seq, lease.token)
    end)

    assert WAL.read(inst, -1, 10) == []

    # the row is physically gone, but the dedup ledger is permanent
    assert {:ok, [{:duplicate, dup_seq}]} = WAL.append(inst, [entry(%{dedup_key: id})])
    assert dup_seq == first.seq
  end

  test "cursors default to 0 and persist", %{instance: inst} do
    assert WAL.get_cursor(inst, :dispatch) == 0

    Ankusa.WAL.LeaseHelpers.with_lease(inst, :dispatch, fn lease ->
      assert :ok = WAL.put_cursor(inst, :dispatch, 42, lease.token)
    end)

    assert WAL.get_cursor(inst, :dispatch) == 42
  end

  test "stats reflect committed records and cursors", %{instance: inst} do
    {:ok, _} = WAL.append(inst, [entry(), entry()])

    Ankusa.WAL.LeaseHelpers.with_lease(inst, :dispatch, fn lease ->
      :ok = WAL.put_cursor(inst, :dispatch, 1, lease.token)
    end)

    stats = WAL.stats(inst)
    assert stats.records == 2
    # the sequence is shared by every instance in the database, so it only ever
    # runs ahead of this instance's own rows
    assert stats.next_seq > stats.max_seq
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

  test "a cursor-following reader never skips a commit that lands behind it", %{instance: inst} do
    conn = Ankusa.via(inst, :wal)

    # Widen the allocation → COMMIT window: `seq` is allocated by the INSERT,
    # but the statement trigger sleeps *after* it, inside the still-open
    # transaction. That is what lets writer B allocate a higher seq and commit
    # before writer A's lower one lands — the exact race a cursor reader
    # (`seq > cursor`) must not lose to.
    try do
      Postgrex.query!(
        conn,
        """
        CREATE OR REPLACE FUNCTION ankusa_test_slow_commit() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          PERFORM pg_sleep(random() * 0.02);
          RETURN NULL;
        END
        $$
        """,
        []
      )

      Postgrex.query!(
        conn,
        """
        CREATE TRIGGER ankusa_test_slow_commit AFTER INSERT ON ankusa_wal
        FOR EACH STATEMENT EXECUTE FUNCTION ankusa_test_slow_commit()
        """,
        []
      )

      advance = fn cursor, seen, envelopes ->
        Enum.reduce(envelopes, {cursor, seen}, fn env, {c, s} ->
          {max(c, env.seq), MapSet.put(s, env.seq)}
        end)
      end

      reader =
        Task.async(fn ->
          drain = fn drain, cursor, seen ->
            case WAL.read(inst, cursor, 1000) do
              [] ->
                seen

              envelopes ->
                {cursor, seen} = advance.(cursor, seen, envelopes)
                drain.(drain, cursor, seen)
            end
          end

          poll = fn poll, cursor, seen ->
            receive do
              :stop ->
                drain.(drain, cursor, seen)
            after
              0 ->
                {cursor, seen} = advance.(cursor, seen, WAL.read(inst, cursor, 1000))
                Process.sleep(1)
                poll.(poll, cursor, seen)
            end
          end

          poll.(poll, 0, MapSet.new())
        end)

      writers =
        for _ <- 1..8 do
          Task.async(fn ->
            for _ <- 1..25 do
              {:ok, [{:committed, env}]} = WAL.append(inst, [entry()])
              env.seq
            end
          end)
        end

      committed = writers |> Task.await_many(30_000) |> List.flatten() |> MapSet.new()

      send(reader.pid, :stop)
      seen = Task.await(reader, 30_000)

      assert MapSet.difference(committed, seen) == MapSet.new()
    after
      Postgrex.query!(conn, "DROP TRIGGER IF EXISTS ankusa_test_slow_commit ON ankusa_wal", [])
      Postgrex.query!(conn, "DROP FUNCTION IF EXISTS ankusa_test_slow_commit()", [])
    end
  end
end
