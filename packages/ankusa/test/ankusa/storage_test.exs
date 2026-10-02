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
  @moduledoc """
  The archive: hooks committed under the `:storage` role carry an archive
  obligation, a tick packs them into `seg/<first>-<last>.seg` plus a `.idx`
  object and a catalogue row, and `Ankusa.Storage.fetch/2` reads any of them
  back byte-for-byte.

  A hook is only deleted once *both* of its obligations clear — the archive's
  and every delivery row's — so these tests pin the archive-then-deliver and
  deliver-then-archive orders, the W5 case with no `:storage` role at all, and
  that a failing blob store defers a tick instead of crashing the compactor.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.{Envelope, Store}
  alias Ankusa.Dispatch.Pipeline
  alias Ankusa.Storage
  alias Ankusa.Storage.Compactor
  alias Ankusa.Store.Keys

  # ── test sinks ─────────────────────────────────────────────────────────────

  defmodule CapturingSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, ctx, opts) do
      send(Keyword.fetch!(opts, :pid), {:delivered, env.id, ctx.attempt})
      :ok
    end
  end

  # Holds the delivery open until the test releases it, so the test controls
  # whether the archive or the delivery clears first.
  defmodule GateSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      send(Keyword.fetch!(opts, :pid), {:started, env.id, self()})
      id = env.id

      receive do
        {:go, ^id} ->
          send(Keyword.fetch!(opts, :pid), {:delivered, id, 1})
          :ok
      after
        5_000 -> {:error, :gate_timeout}
      end
    end
  end

  # ── a failing blob store ───────────────────────────────────────────────────

  # Fails `put/4` for the first N calls (counted in an `Agent`), then delegates
  # to `LocalFS`, the way a full bucket or a 5xx from S3 behaves.
  defmodule FailingBlobStore do
    @behaviour Ankusa.BlobStore

    alias Ankusa.BlobStore.LocalFS

    @impl true
    def put(instance, key, data, opts) do
      case Agent.get_and_update(Keyword.fetch!(opts, :agent), fn
             n when n > 0 -> {:fail, n - 1}
             n -> {:ok, n}
           end) do
        :fail -> {:error, :boom}
        :ok -> LocalFS.put(instance, key, data, [])
      end
    end

    @impl true
    def get(instance, key, _opts), do: LocalFS.get(instance, key, [])

    @impl true
    def get_range(instance, key, offset, length, _opts),
      do: LocalFS.get_range(instance, key, offset, length, [])

    @impl true
    def delete(instance, key, _opts), do: LocalFS.delete(instance, key, [])

    @impl true
    def list(instance, prefix, _opts), do: LocalFS.list(instance, prefix, [])
  end

  # ── helpers ────────────────────────────────────────────────────────────────

  # Boot a full `Ankusa.Instance` with one source `"acme"` whose sinks are the
  # test's. `storage.interval_ms: 0` disables the compactor's timer so `tick/1`
  # alone drives the archive.
  defp start(opts) do
    sinks = Keyword.get(opts, :sinks, [{CapturingSink, [pid: self()]}])
    roles = Keyword.get(opts, :roles, [:edge, :dispatch, :storage])
    storage = Map.merge(%{interval_ms: 0}, Map.new(Keyword.get(opts, :storage, %{})))
    extra = Keyword.drop(opts, [:sinks, :roles, :storage])

    config =
      test_config(
        Keyword.merge(
          [
            roles: roles,
            source_store: {Ankusa.SourceStore.Static, sources: %{"acme" => %{sinks: sinks}}},
            storage: storage
          ],
          extra
        )
      )

    put_config(config)
    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp envelope(body) do
    %Envelope{
      id: "evt_" <> Integer.to_string(System.unique_integer([:positive])),
      source_id: "acme",
      tenant_id: "default",
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/hooks/acme",
      headers: [{"content-type", "application/json"}, {"x-test", "1"}],
      content_type: "application/json",
      body: body,
      size: byte_size(body)
    }
  end

  defp commit!(inst, envelopes), do: Enum.map(envelopes, &enqueue!(inst, &1))

  # ── tests ──────────────────────────────────────────────────────────────────

  test "a tick writes one segment and one index object, and every hook reads back byte-for-byte" do
    config = start(roles: [:edge, :storage])
    inst = config.instance

    originals =
      commit!(inst, [
        envelope(~s({"n":1})),
        envelope(~s({"n":2,"pad":"aaaaaaaaaa"})),
        envelope(<<0, 1, 2, 3, 4, 5>>),
        envelope(~s({"n":4}))
      ])

    assert length(originals) == 4
    assert Enum.all?(originals, &is_integer(&1.seq))

    assert {:ok, 1} == Compactor.tick(inst)

    keys = Ankusa.BlobStore.list(inst, "seg/")
    assert [seg_key] = Enum.filter(keys, &String.ends_with?(&1, ".seg"))
    assert String.starts_with?(seg_key, "seg/")
    assert [_idx_key] = Enum.filter(keys, &String.ends_with?(&1, ".idx"))
    assert length(keys) == 2

    # every original is fetchable byte-for-byte through the storage read path,
    # with the seq stamped from the segment index
    for original <- originals do
      assert {:ok, fetched} = Storage.fetch(inst, original.id)
      assert fetched.id == original.id
      assert fetched.body == original.body
      assert fetched.source_id == original.source_id
      assert fetched.headers == original.headers
      assert fetched.seq == original.seq
    end

    # the archive cleared, but every hook is still stored...
    assert Enum.sort(stored_ids(inst)) == Enum.sort(Enum.map(originals, & &1.id))

    # ...because this node runs no dispatch: its delivery rows are still pending
    for original <- originals do
      assert {:ok, row} = Store.get(inst, :deliveries, Keys.delivery(original.seq, 0))

      assert %{state: :pending, attempts: 0, module: CapturingSink} =
               :erlang.binary_to_term(row)
    end

    # a second tick with nothing new is a no-op
    assert {:ok, 0} == Compactor.tick(inst)
  end

  test "a hook survives one clear and is reclaimed once both clear (archive, then delivery)" do
    config = start(roles: [:edge, :dispatch, :storage], sinks: [{GateSink, [pid: self()]}])
    inst = config.instance

    [env] = commit!(inst, [envelope("gated")])
    assert_receive {:started, id, gate}, 2_000
    assert id == env.id

    # the archive clears first; the delivery is still held open, so the hook stays
    assert {:ok, 1} == Compactor.tick(inst)
    assert env.id in stored_ids(inst)

    # release the sink; now the last obligation clears and the hook is deleted
    send(gate, {:go, id})
    assert_receive {:delivered, ^id, _attempt}, 2_000
    assert {:ok, _} = Pipeline.tick(inst)
    refute env.id in stored_ids(inst)
    assert stored_ids(inst) == []
  end

  test "a hook survives one clear and is reclaimed once both clear (delivery, then archive)" do
    config = start(roles: [:edge, :dispatch, :storage], sinks: [{GateSink, [pid: self()]}])
    inst = config.instance

    [env] = commit!(inst, [envelope("gated")])
    assert_receive {:started, id, gate}, 2_000
    assert id == env.id

    # the delivery clears first; the archive obligation still holds the hook
    send(gate, {:go, id})
    assert_receive {:delivered, ^id, _attempt}, 2_000
    assert {:ok, _} = Pipeline.tick(inst)
    assert env.id in stored_ids(inst)

    # the archive clears; now the hook is gone
    assert {:ok, 1} == Compactor.tick(inst)
    refute env.id in stored_ids(inst)
    assert stored_ids(inst) == []
  end

  test "without the :storage role a delivered hook is reclaimed straight away (W5)" do
    config = start(roles: [:edge, :dispatch])
    inst = config.instance

    [env] = commit!(inst, [envelope("fast")])
    assert_receive {:delivered, id, 1}, 2_000
    assert id == env.id

    assert {:ok, _} = Pipeline.tick(inst)
    refute env.id in stored_ids(inst)
    assert stored_ids(inst) == []
  end

  test "roll_bytes caps segment size: three hooks compact into three segments" do
    config = start(roles: [:edge, :storage], storage: %{roll_bytes: 1})
    inst = config.instance

    originals = commit!(inst, [envelope("one"), envelope("two"), envelope("three")])

    # one tick writes every segment the backlog needs, not one holding it all
    assert {:ok, 3} == Compactor.tick(inst)
    assert length(Ankusa.BlobStore.list(inst, "seg/")) == 6

    for original <- originals do
      assert {:ok, fetched} = Storage.fetch(inst, original.id)
      assert fetched.body == original.body
      assert fetched.seq == original.seq
    end
  end

  test "a failing blob store fails the tick without crashing it, and the next tick retries" do
    agent = start_supervised!({Agent, fn -> 1 end})

    config =
      start(roles: [:edge, :storage], storage: %{blob_store: {FailingBlobStore, [agent: agent]}})

    inst = config.instance
    [env] = commit!(inst, [envelope("retry-me")])

    assert {:ok, 0} == Compactor.tick(inst)
    compactor = Ankusa.whereis(inst, :compactor)
    assert is_pid(compactor)
    assert Process.alive?(compactor)
    assert Ankusa.BlobStore.list(inst, "seg/") == []
    assert :error == Storage.fetch(inst, env.id)

    # the same hooks are written again under the same keys next tick
    assert {:ok, 1} == Compactor.tick(inst)
    assert {:ok, fetched} = Storage.fetch(inst, env.id)
    assert fetched.body == env.body
    assert fetched.seq == env.seq
  end

  test "the catalogue is in the store, so fetch survives an instance restart" do
    config = start(roles: [:edge, :storage])
    inst = config.instance

    originals = commit!(inst, [envelope("one"), envelope("two")])
    assert {:ok, 1} == Compactor.tick(inst)

    :ok = stop_supervised!({Ankusa.Instance, inst})
    start_supervised!({Ankusa.Instance, config})

    for original <- originals do
      assert {:ok, fetched} = Storage.fetch(inst, original.id)
      assert fetched.body == original.body
      assert fetched.seq == original.seq
    end
  end

  test "fetch of an unknown id is :error" do
    config = start(roles: [:edge, :storage])
    assert :error == Storage.fetch(config.instance, "no-such-event")
  end
end
