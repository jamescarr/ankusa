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

  defp wal_path(config), do: Path.join(Ankusa.Config.path(config, "wal"), "ankusa.wal")

  defp restart_wal(config) do
    :ok = stop_supervised({Ankusa.WAL.DiskLog, config.instance})
    start_supervised!({Ankusa.WAL.DiskLog, instance: config.instance, config: config})
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

  test "recovers committed records and drops a torn trailing frame across restart", %{
    config: config,
    inst: inst
  } do
    WAL.append(inst, [%{envelope: env("s", "durable-1")}, %{envelope: env("s", "durable-2")}])
    :ok = stop_supervised({Ankusa.WAL.DiskLog, inst})

    # simulate a crash mid-write: append a partial (never-fsync'd) frame
    path = wal_path(config)
    File.write!(path, <<0x48, 0x4B, 1, 0, 0, 0, 0, 0>>, [:append])

    start_supervised!({Ankusa.WAL.DiskLog, instance: inst, config: config})

    read = WAL.read(inst, -1, 100)
    assert Enum.map(read, & &1.body) == ["durable-1", "durable-2"]
    # next append continues cleanly after the dropped torn tail
    {:ok, [{:committed, e}]} = WAL.append(inst, [%{envelope: env("s", "durable-3")}])
    assert e.seq == 3
  end

  test "truncate_through drops a prefix and seqs continue", %{inst: inst} do
    WAL.append(inst, [
      %{envelope: env("s", "a")},
      %{envelope: env("s", "b")},
      %{envelope: env("s", "c")}
    ])

    :ok = WAL.truncate_through(inst, 1)
    assert WAL.read(inst, -1, 100) |> Enum.map(& &1.seq) == [2, 3]

    {:ok, [{:committed, next}]} = WAL.append(inst, [%{envelope: env("s", "d")}])
    assert next.seq == 4
  end

  test "a failed append is reported, acks nothing, and consumes no seq", %{
    config: config,
    inst: inst,
    wal: wal
  } do
    {:ok, _} = WAL.append(inst, [%{envelope: env("s", "a")}, %{envelope: env("s", "b")}])
    pos = WAL.stats(inst).bytes
    {:ok, [{:committed, %{seq: 3}}]} = WAL.append(inst, [%{envelope: env("s", "ghost")}])

    # Rewind past "ghost", leaving its complete frame on disk beyond `write_pos`
    # — what a batch whose write landed but whose fsync failed leaves behind —
    # and swap in a read-only descriptor so the next `pwrite` fails. Raw fds
    # are owner-bound, so this has to run inside the WAL process.
    :sys.replace_state(wal, fn state ->
      :file.close(state.fd)
      {:ok, ro} = :file.open(state.path, [:read, :raw, :binary])
      :ets.delete(state.index, 3)
      %{state | fd: ro, write_pos: pos, next_seq: 3}
    end)

    assert {:error, _} = WAL.append(inst, [%{envelope: env("s", "c")}])
    assert Process.alive?(wal)
    assert WAL.stats(inst).next_seq == 3
    assert WAL.read(inst, -1, 100) |> Enum.map(& &1.body) == ["a", "b"]
    # The never-acked tail is gone from disk, not just from the index: a crash
    # now must not replay "ghost".
    assert File.stat!(wal_path(config)).size == pos

    # The descriptor `discard_tail` reopened is writable, and the failed
    # batch's seq is reused.
    {:ok, [{:committed, e}]} = WAL.append(inst, [%{envelope: env("s", "d")}])
    assert e.seq == 3

    restart_wal(config)

    read = WAL.read(inst, -1, 100)
    assert Enum.map(read, & &1.body) == ["a", "b", "d"]
    assert Enum.map(read, & &1.seq) == [1, 2, 3]
  end

  test "a cursor that can't be persisted is reported and left unchanged", %{
    config: config,
    inst: inst,
    wal: wal
  } do
    :ok = WAL.put_cursor(inst, :dispatch, 1)

    # A directory where `persist_term/2` wants its temp file makes every write
    # to it fail (`:eisdir`), the way a full disk would (`:enospc`).
    path = wal_path(config)
    File.mkdir_p!(path <> ".cursors.tmp")

    assert {:error, _} = WAL.put_cursor(inst, :dispatch, 2)
    assert WAL.get_cursor(inst, :dispatch) == 1
    assert Process.alive?(wal)

    File.rmdir!(path <> ".cursors.tmp")
    :ok = WAL.put_cursor(inst, :dispatch, 2)

    restart_wal(config)

    assert WAL.get_cursor(inst, :dispatch) == 2
  end

  test "a truncation floor that can't be persisted drops nothing", %{config: config, inst: inst} do
    {:ok, _} = WAL.append(inst, for(n <- 1..3, do: %{envelope: env("s", "#{n}")}))

    path = wal_path(config)
    File.mkdir_p!(path <> ".truncated.tmp")

    assert {:error, _} = WAL.truncate_through(inst, 2)
    assert WAL.read(inst, -1, 100) |> Enum.map(& &1.seq) == [1, 2, 3]

    File.rmdir!(path <> ".truncated.tmp")
    :ok = WAL.truncate_through(inst, 2)
    assert WAL.read(inst, -1, 100) |> Enum.map(& &1.seq) == [3]
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

    :ok = WAL.truncate_through(inst, 3)
    assert WAL.stats(inst).records == 0

    restart_wal(config)

    # Restarting a caught-up node must not restart its seqs at 1: dispatch's
    # cursor is already 3, so a seq 1 would be skipped and then deleted.
    {:ok, [{:committed, next}]} = WAL.append(inst, [%{envelope: env("s", "four")}])
    assert next.seq == 4
    assert [read] = WAL.read(inst, 3, 10)
    assert read.seq == 4
    assert read.body == "four"
  end
end
