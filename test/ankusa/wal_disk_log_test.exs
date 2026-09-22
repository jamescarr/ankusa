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

  defp env(source, body, dedup_key \\ nil) do
    %Envelope{
      id: Ankusa.UUIDv7.generate(),
      source_id: source,
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/hooks/#{source}",
      headers: [],
      body: body,
      dedup_key: dedup_key,
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

  test "dedup: a repeated (source, dedup_key) is not written and returns :duplicate", %{
    inst: inst
  } do
    {:ok, [{:committed, e1}]} = WAL.append(inst, [%{envelope: env("s", "one", "k1")}])
    {:ok, [dup]} = WAL.append(inst, [%{envelope: env("s", "two", "k1")}])
    assert dup == {:duplicate, e1.seq}
    # different source, same key -> not a duplicate
    {:ok, [{:committed, _}]} = WAL.append(inst, [%{envelope: env("other", "three", "k1")}])
    assert WAL.stats(inst).records == 2
  end

  test "dedup collisions within a single batch are absorbed", %{inst: inst} do
    {:ok, results} =
      WAL.append(inst, [
        %{envelope: env("s", "a", "dupe")},
        %{envelope: env("s", "b", "dupe")}
      ])

    assert [{:committed, e}, {:duplicate, seq}] = results
    assert seq == e.seq
    assert WAL.stats(inst).records == 1
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

  test "truncate_through drops a prefix but keeps dedup coverage", %{inst: inst} do
    WAL.append(inst, [
      %{envelope: env("s", "a", "ka")},
      %{envelope: env("s", "b", "kb")},
      %{envelope: env("s", "c", "kc")}
    ])

    :ok = WAL.truncate_through(inst, 1)
    assert WAL.read(inst, -1, 100) |> Enum.map(& &1.seq) == [2, 3]

    # a duplicate of a truncated record is still rejected (dedup survived)
    {:ok, [dup]} = WAL.append(inst, [%{envelope: env("s", "a-again", "ka")}])
    assert {:duplicate, 1} = dup
  end
end
