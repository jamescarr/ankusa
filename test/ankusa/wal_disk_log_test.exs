defmodule Ankusa.WAL.DiskLogTest do
  use ExUnit.Case, async: false

  import Ankusa.TestHelpers
  alias Ankusa.{Envelope, WAL}

  setup do
    config = test_config()
    put_config(config)
    pid = start_supervised!({Ankusa.WAL.DiskLog, instance: config.instance, config: config})
    %{config: config, inst: config.instance, wal: pid}
  end

  defp env(source, body) do
    %Envelope{
      id: Ankusa.UUIDv7.generate(),
      source_id: source,
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/hooks/#{source}",
      headers: [],
      body: body,
      size: byte_size(body)
    }
  end

  test "append commits records, assigns dense monotonic seqs, and reads them back", %{inst: inst} do
    {:ok, results} =
      WAL.append(inst, [
        %{envelope: env("s", "a")},
        %{envelope: env("s", "b")},
        %{envelope: env("s", "c")}
      ])

    seqs = for {:committed, e} <- results, do: e.seq
    assert seqs == [1, 2, 3]

    read = WAL.read(inst, -1, 100)
    assert Enum.map(read, & &1.body) == ["a", "b", "c"]
    assert Enum.map(read, & &1.seq) == [1, 2, 3]
  end

  test "read is a cursor: only records after `after_seq`", %{inst: inst} do
    WAL.append(inst, for(n <- 1..5, do: %{envelope: env("s", "#{n}")}))
    assert WAL.read(inst, 2, 100) |> Enum.map(& &1.seq) == [3, 4, 5]
    assert WAL.read(inst, 2, 1) |> Enum.map(& &1.seq) == [3]
  end

  test "the same event appended twice is two records, with two seqs", %{inst: inst} do
    # No uniqueness constraint: a provider's retry is appended again, and
    # dispatch decides whether it is a duplicate (see `Ankusa.DedupStoreTest`).
    {:ok, [{:committed, e1}]} = WAL.append(inst, [%{envelope: env("s", "one")}])
    {:ok, [{:committed, e2}]} = WAL.append(inst, [%{envelope: env("s", "one")}])

    assert e2.seq == e1.seq + 1
    assert WAL.stats(inst).records == 2
  end

  test "recovers committed records and drops a torn trailing frame across restart", %{
    config: config,
    inst: inst
  } do
    WAL.append(inst, [%{envelope: env("s", "durable-1")}, %{envelope: env("s", "durable-2")}])
    :ok = stop_supervised({Ankusa.WAL.DiskLog, inst})

    # simulate a crash mid-write: append a partial (never-fsync'd) frame
    path = Path.join([config.data_dir, to_string(inst), "wal", "ankusa.wal"])
    File.write!(path, <<0x48, 0x4B, 1, 0, 0, 0, 0, 0>>, [:append])

    start_supervised!({Ankusa.WAL.DiskLog, instance: inst, config: config})

    read = WAL.read(inst, -1, 100)
    assert Enum.map(read, & &1.body) == ["durable-1", "durable-2"]
    # next append continues cleanly after the dropped torn tail
    {:ok, [{:committed, e}]} = WAL.append(inst, [%{envelope: env("s", "durable-3")}])
    assert e.seq == 3
  end

  test "truncate_through drops a prefix and leaves the rest readable", %{inst: inst} do
    WAL.append(inst, [
      %{envelope: env("s", "a")},
      %{envelope: env("s", "b")},
      %{envelope: env("s", "c")}
    ])

    Ankusa.WAL.LeaseHelpers.with_lease(inst, :storage, fn lease ->
      :ok = WAL.truncate_through(inst, 1, lease.token)
    end)

    assert WAL.read(inst, -1, 100) |> Enum.map(& &1.seq) == [2, 3]

    # A copy appended after truncation is a new record: the log keeps no ledger
    # of what it has already seen.
    {:ok, [{:committed, again}]} = WAL.append(inst, [%{envelope: env("s", "a-again")}])
    assert again.seq > 3
    assert WAL.stats(inst).records == 3
  end

  @doc false
  def handle_commit(_event, measurements, meta, test_pid),
    do: send(test_pid, {:commit, measurements, meta})

  test "a commit reports its size on the measurement side of [:commit, :stop]", %{inst: inst} do
    handler = {__MODULE__, System.unique_integer()}

    :telemetry.attach(
      handler,
      [:ankusa, :commit, :stop],
      &__MODULE__.handle_commit/4,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    {:ok, _} = WAL.append(inst, [%{envelope: env("s", "a")}, %{envelope: env("s", "bb")}])

    assert_receive {:commit, measurements, meta}

    # Measurements, not metadata: `:batch_size` and `:bytes` are what a
    # `Telemetry.Metrics.sum/2` is wired to, and a metric reads measurements.
    assert measurements.batch_size == 2
    assert measurements.bytes > 0
    assert is_integer(measurements.duration)
    assert meta.instance == inst
  end

  test "seq keeps increasing after a full truncation and restart" do
    # `rewrite_min_bytes: 0` forces the physical rewrite, so this covers the
    # worst case: the file is emptied while the seq floor lives on.
    config = test_config(wal: {Ankusa.WAL.DiskLog, rewrite_min_bytes: 0})
    put_config(config)
    inst = config.instance

    start_supervised!({Ankusa.WAL.DiskLog, instance: inst, config: config})

    {:ok, results} = WAL.append(inst, for(n <- 1..3, do: %{envelope: env("s", "#{n}")}))
    assert [1, 2, 3] == for({:committed, e} <- results, do: e.seq)

    Ankusa.WAL.LeaseHelpers.with_lease(inst, :storage, fn lease ->
      :ok = WAL.truncate_through(inst, 3, lease.token)
    end)

    assert WAL.stats(inst).records == 0

    :ok = stop_supervised({Ankusa.WAL.DiskLog, inst})
    start_supervised!({Ankusa.WAL.DiskLog, instance: inst, config: config})

    # Restarting a caught-up node must not restart its seqs at 1: dispatch's
    # cursor is already 3, so a seq 1 would be skipped and then deleted.
    {:ok, [{:committed, next}]} = WAL.append(inst, [%{envelope: env("s", "four")}])
    assert next.seq == 4
    assert [read] = WAL.read(inst, 3, 10)
    assert read.seq == 4
    assert read.body == "four"
  end
end
