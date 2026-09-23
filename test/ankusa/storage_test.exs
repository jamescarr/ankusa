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
    :ok = Ankusa.WAL.put_cursor(inst, :dispatch, last_seq)
    assert Ankusa.WAL.stats(inst).records == 4

    assert {:ok, 1} == Compactor.tick(inst)

    # exactly one immutable segment written
    assert [segment_key] = Ankusa.BlobStore.list(inst, "seg")
    assert String.starts_with?(segment_key, "seg/")
    assert String.ends_with?(segment_key, ".seg")

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

  test "a lookup after a later compaction never serves the cached earlier index",
       %{inst: inst, config: config} do
    [first] = commit!(inst, [envelope("acme", ~s({"n":1}))])
    :ok = Ankusa.WAL.put_cursor(inst, :dispatch, first.seq)
    assert {:ok, 1} == Compactor.tick(inst)

    # populates the cached map with exactly this one row
    assert {:ok, _row} = Index.lookup(config, first.id)

    [second] = commit!(inst, [envelope("acme", ~s({"n":2}))])
    :ok = Ankusa.WAL.put_cursor(inst, :dispatch, second.seq)
    assert {:ok, 1} == Compactor.tick(inst)

    # the row appended after the cache was populated must be visible
    assert {:ok, row} = Index.lookup(config, second.id)
    assert row.event_id == second.id
    assert {:ok, _old} = Index.lookup(config, first.id)

    assert {:ok, fetched} = Storage.fetch(inst, second.id)
    assert fetched.body == second.body
  end

  test "compaction never truncates past the dispatch cursor", %{inst: inst, config: config} do
    originals = commit!(inst, [envelope("acme", "one"), envelope("acme", "two")])
    [first_seq, last_seq] = Enum.map(originals, & &1.seq)

    # dispatch has only consumed the first record
    :ok = Ankusa.WAL.put_cursor(inst, :dispatch, first_seq)

    assert {:ok, 1} == Compactor.tick(inst)

    # both records are compacted and indexed...
    assert length(Index.all(config)) == 2
    # ...but the un-dispatched record must survive in the WAL
    stats = Ankusa.WAL.stats(inst)
    assert stats.records == 1
    assert stats.min_seq == last_seq
  end
end
