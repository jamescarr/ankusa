defmodule Ankusa.WAL.PostgresTest do
  @moduledoc """
  Requires a live Postgres: `docker compose up -d --wait` in this directory.
  """

  use ExUnit.Case, async: false

  alias Ankusa.{Envelope, UUIDv7, WAL}
  alias Ankusa.WAL.Postgres

  # 12, not 4: the concurrency tests open one transaction per writer and eight
  # of them race, so a pool smaller than the writer count turns "which commit
  # lands first" into "which writer got a connection", and the losers fail with
  # a `queue_timeout` instead of an assertion.
  @pg_opts [
    hostname: "localhost",
    port: 5433,
    username: "ankusa",
    password: "ankusa",
    database: "ankusa_dev",
    pool_size: 12
  ]

  # Not just `unique_integer/1`: that is unique within a VM, not across
  # restarts, and this adapter keeps its rows, cursors and leases in a database
  # that outlives the test run. A run that reuses a name reads the previous
  # run's rows.
  defp uniq(prefix) do
    suffix = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
    :"#{prefix}_#{System.unique_integer([:positive])}#{suffix}"
  end

  defp boot(instance) do
    config = Ankusa.Config.new(instance: instance, wal: {Postgres, @pg_opts})
    Ankusa.put_config(config)
    start_supervised!({Postgres, [instance: instance, config: config]}, id: instance)
    instance
  end

  setup do
    %{instance: boot(uniq("pg"))}
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

  # The log has no uniqueness constraint: a provider's retry is appended again,
  # and dispatch decides whether it is a duplicate (see `Ankusa.DedupStoreTest`).
  test "the same event appended again is a new row with its own seq", %{instance: inst} do
    {:ok, [{:committed, first}]} = WAL.append(inst, [entry()])
    {:ok, [{:committed, second}]} = WAL.append(inst, [entry()])

    assert second.seq > first.seq
    assert length(WAL.read(inst, -1, 10)) == 2

    # Two copies in one batch are two rows as well.
    assert {:ok, [{:committed, third}, {:committed, fourth}]} =
             WAL.append(inst, [entry(), entry()])

    assert third.seq == second.seq + 1
    assert fourth.seq == third.seq + 1
  end

  test "concurrent writers appending the same event all commit, with distinct seqs", %{
    instance: inst
  } do
    results =
      1..8
      |> Enum.map(fn _ -> Task.async(fn -> WAL.append(inst, [entry()]) end) end)
      |> Enum.map(&Task.await(&1, 5_000))
      |> Enum.map(fn {:ok, [{:committed, env}]} -> env.seq end)

    assert length(Enum.uniq(results)) == 8
    assert length(WAL.read(inst, -1, 10)) == 8
  end

  test "truncate_through removes rows, and a later copy is still appended", %{instance: inst} do
    {:ok, [{:committed, first}]} = WAL.append(inst, [entry()])

    Ankusa.WAL.LeaseHelpers.with_lease(inst, :storage, fn lease ->
      :ok = WAL.truncate_through(inst, first.seq, lease.token)
    end)

    assert WAL.read(inst, -1, 10) == []

    {:ok, [{:committed, again}]} = WAL.append(inst, [entry()])
    assert again.seq > first.seq
    assert [%{seq: seq}] = WAL.read(inst, -1, 10)
    assert seq == again.seq
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

  test "two instances sharing one database never see each other's rows" do
    inst_a = boot(uniq("pg_iso_a"))
    inst_b = boot(uniq("pg_iso_b"))
    assert {:ok, [{:committed, _}]} = WAL.append(inst_a, [entry()])
    assert {:ok, [{:committed, _}]} = WAL.append(inst_b, [entry()])

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

      # Two calls: Postgrex prepares statements, so one call can carry only one
      # command. Dropping first keeps a run that died mid-test from poisoning
      # every later run.
      Postgrex.query!(conn, "DROP TRIGGER IF EXISTS ankusa_test_slow_commit ON ankusa_wal", [])

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
