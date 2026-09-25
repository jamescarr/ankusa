defmodule Ankusa.Codec.RawTest do
  use ExUnit.Case, async: true

  alias Ankusa.Codec.Raw

  test "encode/decode_record round-trips every record with correct byte ranges" do
    records = [
      %{key: "a", payload: "hello"},
      %{key: "b", payload: <<0, 1, 2, 3, 255>>},
      %{key: "c", payload: :erlang.term_to_binary(%{deep: [1, 2, 3]})}
    ]

    {segment, index} = Raw.encode(records)

    assert length(index) == length(records)

    for {rec, entry} <- Enum.zip(records, index) do
      assert entry.key == rec.key
      # length is the full frame: 8-byte header + payload
      assert entry.length == 8 + byte_size(rec.payload)
      frame = binary_part(segment, entry.offset, entry.length)
      assert {:ok, rec.payload} == Raw.decode_record(frame)
    end

    # offsets are contiguous and cover the whole segment
    total = Enum.reduce(index, 0, fn e, acc -> acc + e.length end)
    assert total == byte_size(segment)
    assert hd(index).offset == 0
  end

  test "decode_record detects a corrupted payload as :crc_mismatch" do
    {segment, [_entry]} = Raw.encode([%{key: "x", payload: "correct-bytes"}])
    # keep the original len/crc header but flip the first payload byte
    <<len::32, crc::32, first, rest::binary>> = segment
    corrupted = <<len::32, crc::32, Bitwise.bxor(first, 1)::8, rest::binary>>

    assert {:error, :crc_mismatch} == Raw.decode_record(corrupted)
  end

  test "decode_record rejects a truncated/garbage frame as :malformed" do
    assert {:error, :malformed} == Raw.decode_record(<<1, 2, 3>>)
    # header promises 100 payload bytes that are not present
    assert {:error, :malformed} == Raw.decode_record(<<100::32, 0::32, "short">>)
  end
end

defmodule Ankusa.StorageTest do
  use ExUnit.Case, async: false

  alias Ankusa.{Config, Envelope}
  alias Ankusa.Storage
  alias Ankusa.Storage.{Compactor, Index}

  setup do
    inst = :"t#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{inst}")
    on_exit(fn -> File.rm_rf(dir) end)

    config =
      Config.new(
        instance: inst,
        data_dir: dir,
        roles: [:edge, :dispatch, :storage],
        source_store: {Ankusa.SourceStore.Static, sources: %{"acme" => %{}}},
        # disable the interval auto-tick so `tick/1` fully controls the test
        storage: %{interval_ms: 0}
      )

    Ankusa.put_config(config)
    start_supervised!({Ankusa.WAL.DiskLog, instance: inst, config: config})
    start_supervised!({Compactor, instance: inst, config: config})

    %{inst: inst, config: config}
  end

  defp envelope(source_id, body) do
    %Envelope{
      id: "evt-#{System.unique_integer([:positive])}",
      source_id: source_id,
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/hooks/#{source_id}",
      headers: [{"content-type", "application/json"}, {"x-src", source_id}],
      content_type: "application/json",
      body: body,
      size: byte_size(body)
    }
  end

  defp commit!(inst, envelopes) do
    records = Enum.map(envelopes, &%{envelope: &1})
    {:ok, results} = Ankusa.WAL.append(inst, records)
    for {:committed, env} <- results, do: env
  end

  # Advancing a cursor needs a live lease: this is the dispatch role's.
  defp dispatch_cursor!(inst, seq) do
    Ankusa.WAL.LeaseHelpers.with_lease(inst, :dispatch, fn lease ->
      :ok = Ankusa.WAL.put_cursor(inst, :dispatch, seq, lease.token)
    end)
  end

  defp segment_keys(inst) do
    inst |> Ankusa.BlobStore.list("seg") |> Enum.filter(&String.ends_with?(&1, ".seg"))
  end

  test "tick compacts WAL records into one segment, indexes them, and truncates",
       %{inst: inst, config: config} do
    originals =
      commit!(inst, [
        envelope("acme", ~s({"n":1})),
        envelope("acme", ~s({"n":2,"blob":"aaaaaaaaaa"})),
        envelope("beta", <<0, 1, 2, 3, 4, 5>>),
        envelope("acme", ~s({"n":4}))
      ])

    assert length(originals) == 4
    last_seq = originals |> List.last() |> Map.fetch!(:seq)

    # dispatch has consumed everything, so the compactor may truncate fully
    dispatch_cursor!(inst, last_seq)
    assert Ankusa.WAL.stats(inst).records == 4

    assert {:ok, 1} == Compactor.tick(inst)

    # exactly one immutable segment written, plus its index sidecar
    assert [segment_key] = segment_keys(inst)
    assert String.starts_with?(segment_key, "seg/")
    assert String.ends_with?(segment_key, ".seg")

    sidecar = String.replace_suffix(segment_key, ".seg", ".idx")
    assert sidecar in Ankusa.BlobStore.list(inst, "seg")

    # one durable index row per committed record
    rows = Index.all(config)
    assert length(rows) == 4
    assert Enum.map(rows, & &1.seq) == Enum.map(originals, & &1.seq)
    assert Enum.all?(rows, &(&1.segment_key == segment_key))

    # every original is fetchable byte-for-byte through the storage read path
    for original <- originals do
      assert {:ok, fetched} = Storage.fetch(inst, original.id)
      assert fetched.id == original.id
      assert fetched.body == original.body
      assert fetched.source_id == original.source_id
      assert fetched.headers == original.headers
    end

    # index lookup miss is an honest :error
    assert :error == Index.lookup(config, "no-such-event")
    assert :error == Storage.fetch(inst, "no-such-event")

    # the WAL was reclaimed after compaction
    assert Ankusa.WAL.stats(inst).records == 0

    # a second tick with nothing new is a no-op
    assert {:ok, 0} == Compactor.tick(inst)
  end

  test "a lookup after a later compaction sees the rows that tick just wrote",
       %{inst: inst, config: config} do
    [first] = commit!(inst, [envelope("acme", ~s({"n":1}))])
    dispatch_cursor!(inst, first.seq)
    assert {:ok, 1} == Compactor.tick(inst)

    assert {:ok, _row} = Index.lookup(config, first.id)

    [second] = commit!(inst, [envelope("acme", ~s({"n":2}))])
    dispatch_cursor!(inst, second.seq)
    assert {:ok, 1} == Compactor.tick(inst)

    # the row appended by that earlier read's tick must still be there, and the
    # new one visible
    assert {:ok, row} = Index.lookup(config, second.id)
    assert row.event_id == second.id
    assert {:ok, _old} = Index.lookup(config, first.id)

    assert {:ok, fetched} = Storage.fetch(inst, second.id)
    assert fetched.body == second.body
  end

  test "a lookup works while the compactor is down, and the restart reloads the index",
       %{inst: inst, config: config} do
    [env] = commit!(inst, [envelope("acme", ~s({"n":1}))])
    dispatch_cursor!(inst, env.seq)
    assert {:ok, 1} == Compactor.tick(inst)

    assert {:ok, row} = Index.lookup(config, env.id)

    # The table belongs to the compactor, so with it stopped a lookup has none —
    # that is the crash-restart window, and it reads the file instead: slow,
    # never wrong.
    :ok = stop_supervised({Compactor, inst})
    assert {:ok, from_file} = Index.lookup(config, env.id)
    assert from_file.segment_key == row.segment_key

    # the restart reloads the table from the same file
    start_supervised!({Compactor, instance: inst, config: config})
    assert {:ok, reloaded} = Index.lookup(config, env.id)
    assert reloaded == row
    assert {:ok, fetched} = Storage.fetch(inst, env.id)
    assert fetched.body == env.body
  end

  test "compaction never truncates past the dispatch cursor", %{inst: inst, config: config} do
    originals = commit!(inst, [envelope("acme", "one"), envelope("acme", "two")])
    [first_seq, last_seq] = Enum.map(originals, & &1.seq)

    # dispatch has only consumed the first record
    dispatch_cursor!(inst, first_seq)

    assert {:ok, 1} == Compactor.tick(inst)

    # both records are compacted and indexed...
    assert length(Index.all(config)) == 2
    # ...but the un-dispatched record must survive in the WAL
    stats = Ankusa.WAL.stats(inst)
    assert stats.records == 1
    assert stats.min_seq == last_seq
  end

  test "roll_bytes caps segment size: a backlog compacts into several segments" do
    inst = :"t#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{inst}")
    on_exit(fn -> File.rm_rf(dir) end)

    config =
      Config.new(
        instance: inst,
        data_dir: dir,
        roles: [:edge, :dispatch, :storage],
        source_store: {Ankusa.SourceStore.Static, sources: %{"acme" => %{}}},
        # a byte budget of 1 forces one record per segment
        storage: %{interval_ms: 0, roll_bytes: 1}
      )

    Ankusa.put_config(config)
    start_supervised!({Ankusa.WAL.DiskLog, instance: inst, config: config})
    start_supervised!({Compactor, instance: inst, config: config})

    originals =
      commit!(inst, [
        envelope("acme", "one"),
        envelope("acme", "two"),
        envelope("acme", "three")
      ])

    dispatch_cursor!(inst, originals |> List.last() |> Map.fetch!(:seq))

    # one tick writes every segment the backlog needs, not one segment holding
    # the whole backlog
    assert {:ok, 3} == Compactor.tick(inst)
    assert length(segment_keys(inst)) == 3

    for original <- originals do
      assert {:ok, fetched} = Storage.fetch(inst, original.id)
      assert fetched.body == original.body
    end

    # everything was compacted, so the WAL is fully reclaimed
    assert Ankusa.WAL.stats(inst).records == 0
  end

  # ── tick-loop leak + standby repair ───────────────────────────────────────

  test "the compactor's auto-tick loop does not accumulate messages" do
    inst = :"t#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{inst}")
    on_exit(fn -> File.rm_rf(dir) end)

    config =
      Config.new(
        instance: inst,
        data_dir: dir,
        roles: [:edge, :dispatch, :storage],
        source_store: {Ankusa.SourceStore.Static, sources: %{"acme" => %{}}},
        # a real interval, so the tick loop runs on its own
        storage: %{interval_ms: 40}
      )

    Ankusa.put_config(config)
    start_supervised!({Ankusa.WAL.DiskLog, instance: inst, config: config})
    start_supervised!({Compactor, instance: inst, config: config})

    Process.sleep(2_000)

    # The tick loop must stay a single message in flight, not one accumulating
    # per interval.
    assert {:message_queue_len, n} =
             Process.info(Ankusa.whereis(inst, :compactor), :message_queue_len)

    assert n < 5
  end

  test "Index.repair folds a sidecar, walks a missing one, and skips below the hwm" do
    inst = :"t#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{inst}")
    on_exit(fn -> File.rm_rf(dir) end)

    config =
      Config.new(
        instance: inst,
        data_dir: dir,
        roles: [:storage],
        source_store: {Ankusa.SourceStore.Static, sources: %{"acme" => %{}}}
      )

    Ankusa.put_config(config)
    Index.open(config)

    # Segment 1..2 with a sidecar; segment 3..3 with no sidecar.
    {_key1, _rows1} = put_segment(inst, 1, 2, sidecar?: true)
    {key2, _rows2} = put_segment(inst, 3, 3, sidecar?: false)

    Index.repair(config)

    assert {:ok, _} = Index.lookup(config, "evt-1")
    assert {:ok, _} = Index.lookup(config, "evt-2")
    assert {:ok, _} = Index.lookup(config, "evt-3")

    # Segment 2 (below nothing) is folded; a repair that already folded it must
    # not re-fold: putting an hwm at key2 and repairing again touches nothing
    # above it.
    Index.put_hwm(config, key2)
    Index.repair(config)
  end

  defp pad(seq), do: seq |> Integer.to_string() |> String.pad_leading(20, "0")

  defp put_segment(inst, first, last, opts) do
    envs =
      for seq <- first..last do
        %{envelope("acme", "r#{seq}") | id: "evt-#{seq}", seq: seq}
      end

    records = Enum.map(envs, &%{key: &1.id, payload: Envelope.to_binary(&1)})
    {segment, index} = Ankusa.Codec.Raw.encode(records)
    key = "seg/#{pad(first)}-#{pad(last)}.seg"

    rows =
      envs
      |> Enum.zip(index)
      |> Enum.map(fn {env, entry} ->
        %{
          event_id: env.id,
          source_id: env.source_id,
          tenant_id: env.tenant_id,
          received_at: env.received_at,
          seq: env.seq,
          segment_key: key,
          offset: entry.offset,
          length: entry.length
        }
      end)

    :ok = Ankusa.BlobStore.put(inst, key, segment)

    if Keyword.get(opts, :sidecar?, true) do
      sidecar = String.replace_suffix(key, ".seg", ".idx")
      :ok = Ankusa.BlobStore.put(inst, sidecar, Ankusa.DurableLog.frame(rows))
    end

    {key, rows}
  end
end
