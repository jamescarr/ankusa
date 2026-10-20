defmodule Ankusa.StorageTest do
  @moduledoc """
  The archive: hooks committed under the `:storage` role carry an archive
  obligation, a tick packs them into `archive/v1/dt=<day>/<node>/seg-<first>-<last>.seg` plus a `.idx`
  object and a catalogue row, and `Ankusa.Storage.fetch/2` reads any of them
  back byte-for-byte.

  A hook is only deleted once *both* of its obligations clear — the archive's
  and every delivery row's — so these tests pin the archive-then-deliver and
  deliver-then-archive orders, the W5 case with no `:storage` role at all, and
  that a failing blob store defers a tick instead of crashing the compactor.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Dispatch.Pipeline
  alias Ankusa.{Envelope, Store}
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
    def put_if_absent(instance, key, data, opts), do: put(instance, key, data, opts)
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

  # A blob store whose write exits, the way a `GenServer.call` into a dead
  # token provider does.
  defmodule ExitingBlobStore do
    @behaviour Ankusa.BlobStore

    alias Ankusa.BlobStore.LocalFS

    @impl true
    def put(_instance, _key, _data, _opts), do: exit(:boom)

    @impl true
    def put_if_absent(_instance, _key, _data, _opts), do: exit(:boom)

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

  # ── a once-raising codec ───────────────────────────────────────────────────

  # Raises on its first `encode/1` and delegates to `Raw` afterwards. The
  # compactor process survives the tick, so its process dictionary remembers.
  defmodule RaisingOnceCodec do
    @behaviour Ankusa.Codec

    @impl true
    def encode(items) do
      if Process.get(:raised_once) do
        Ankusa.Codec.Raw.encode(items)
      else
        Process.put(:raised_once, true)
        raise "codec boom"
      end
    end

    @impl true
    def decode_record(bin), do: Ankusa.Codec.Raw.decode_record(bin)
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

  # Poll `fun` every 10 ms until it is truthy or `timeout` ms pass.
  defp eventually(fun, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(10)
        do_eventually(fun, deadline)
    end
  end

  defp segments_root(config), do: Ankusa.Config.path(config, "segments")

  defp archive_files(config, ext),
    do: Path.wildcard(Path.join(segments_root(config), "archive/v1/dt=*/*/*" <> ext))

  defp uuid_envelope(body), do: %{envelope(body) | id: Ankusa.UUIDv7.generate()}

  # One object-store root shared by several nodes, which differ by `intake.node_id`.
  defp shared_root do
    root = Path.join(System.tmp_dir!(), "ankusa_shared_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(root) end)
    root
  end

  defp shared_node(root, name) do
    start(
      roles: [:edge, :storage],
      intake: %{node_id: name},
      storage: %{blob_store: {Ankusa.BlobStore.LocalFS, root: root}}
    )
  end

  # What a release before the header wrote: bare frames, a bare `term_to_binary`
  # index, a catalogue row with no `format`.
  defp write_legacy_segment(inst, env) do
    payload = Envelope.to_binary(env)
    {frames, [entry]} = Ankusa.Codec.Raw.encode([%{key: env.id, payload: payload}])
    name = "seg/#{String.pad_leading("1", 20, "0")}-#{String.pad_leading("1", 20, "0")}"
    idx = %{env.id => {entry.offset, entry.length, env.seq}}

    :ok = Ankusa.BlobStore.put(inst, :segments, name <> ".seg", frames)
    :ok = Ankusa.BlobStore.put(inst, :segments, name <> ".idx", :erlang.term_to_binary(idx))

    %{
      key: name <> ".seg",
      idx_key: name <> ".idx",
      first_seq: 1,
      last_seq: 1,
      min_id: env.id,
      max_id: env.id,
      count: 1,
      bytes: byte_size(frames)
    }
  end

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

    {:ok, keys} = Ankusa.BlobStore.list(inst, :segments, "archive/v1/")
    assert [seg_key] = Enum.filter(keys, &String.ends_with?(&1, ".seg"))
    assert String.starts_with?(seg_key, "archive/v1/dt=")
    assert [_idx_key] = Enum.filter(keys, &String.ends_with?(&1, ".idx"))
    assert [_manifest] = Enum.filter(keys, &String.ends_with?(&1, "/_manifest.json"))
    assert length(keys) == 3

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

  test "segment has no authorization header, and neither has a claim pack made from the archived hook" do
    config = start(roles: [:edge, :storage])
    inst = config.instance
    body = :binary.copy("b", 200)

    headers = [
      {"authorization", "Bearer sk_live_topsecret"},
      {"Cookie", "sid=topsecret-session"},
      {"x-keep", "visible"}
    ]

    assert {:ok, %Envelope{} = env} =
             Ankusa.Edge.Ingest.ingest(inst, request("acme", body, headers))

    assert {:ok, 1} == Compactor.tick(inst)

    {:ok, keys} = Ankusa.BlobStore.list(inst, :segments, "archive/v1/")
    assert [_ | _] = keys

    for key <- keys do
      assert {:ok, object} = Ankusa.BlobStore.get(inst, :segments, key)
      refute object =~ "topsecret"
      refute String.downcase(object) =~ "authorization"
      refute String.downcase(object) =~ "cookie"
    end

    # the archived hook reads back with the headers the policy left
    assert {:ok, fetched} = Storage.fetch(inst, env.id)
    assert fetched.headers == [{"x-keep", "visible"}]
    assert fetched.body == body

    # a body over the inline threshold is checked in as a claim pack
    ctx = %{instance: inst, source_id: "acme", tenant_id: "default", attempt: 1}
    assert {:ok, json} = Ankusa.Sink.Message.encode(fetched, ctx, 100)
    refute json =~ "topsecret"
    assert "urn:ankusa:claim:v1:default:" <> _ = JSON.decode!(json)["claim"]

    [{_index, mod, opts}] = Ankusa.ClaimCheck.Stores.list(Ankusa.config(inst))
    assert {:ok, [_ | _] = claim_keys} = mod.list(inst, "claims/", opts)

    for key <- claim_keys do
      assert {:ok, pack} = mod.get(inst, key, opts)
      refute pack =~ "topsecret"
      refute String.downcase(pack) =~ "authorization"
    end
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
    assert {:ok, keys} = Ankusa.BlobStore.list(inst, :segments, "archive/v1/")
    # three segments, their three indexes, and the day's manifest
    assert length(keys) == 7

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
    assert Ankusa.BlobStore.list(inst, :segments, "archive/v1/") == {:ok, []}
    assert :error == Storage.fetch(inst, env.id)

    # the same hooks are written again under the same keys next tick
    assert {:ok, 1} == Compactor.tick(inst)
    assert {:ok, fetched} = Storage.fetch(inst, env.id)
    assert fetched.body == env.body
    assert fetched.seq == env.seq
  end

  test "a failed tick counts one archive failure, and the oldest unarchived hook's age is reported" do
    agent = start_supervised!({Agent, fn -> 1 end})

    config =
      start(roles: [:edge, :storage], storage: %{blob_store: {FailingBlobStore, [agent: agent]}})

    inst = config.instance
    test = self()
    handler = "archive-telemetry-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler,
      [[:ankusa, :archive, :failure], [:ankusa, :archive, :state]],
      fn event, measurements, meta, _ ->
        if meta.instance == inst, do: send(test, {:archive, event, measurements})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    commit!(inst, [envelope("old")])
    Process.sleep(30)

    # The write fails: one failure, and the age is reported all the same.
    assert {:ok, 0} == Compactor.tick(inst)
    assert_received {:archive, [:ankusa, :archive, :state], %{oldest_unarchived_age_ms: age}}
    assert age >= 20
    assert_received {:archive, [:ankusa, :archive, :failure], _}
    refute_received {:archive, [:ankusa, :archive, :failure], _}

    assert {:ok, 1} == Compactor.tick(inst)
    assert_received {:archive, [:ankusa, :archive, :state], %{oldest_unarchived_age_ms: older}}
    assert older >= age
    refute_received {:archive, [:ankusa, :archive, :failure], _}

    # Nothing is owed any more.
    assert {:ok, 0} == Compactor.tick(inst)
    assert_received {:archive, [:ankusa, :archive, :state], %{oldest_unarchived_age_ms: 0}}
  end

  test "consecutive failures back the timer off instead of retrying every interval" do
    agent = start_supervised!({Agent, fn -> 5 end})

    config =
      start(
        roles: [:edge, :storage],
        storage: %{interval_ms: 10, roll_ms: 0, blob_store: {FailingBlobStore, [agent: agent]}}
      )

    inst = config.instance
    [env] = commit!(inst, [envelope("backoff")])

    # Retried every 10 ms, the sixth write (the first to succeed) lands within
    # ~60 ms. Backed off (interval·2^n, jittered to at least half), it cannot
    # come before ~310 ms.
    Process.sleep(250)
    assert :error == Storage.fetch(inst, env.id)

    assert eventually(fn -> match?({:ok, _}, Storage.fetch(inst, env.id)) end, 3_000)
    assert {:ok, fetched} = Storage.fetch(inst, env.id)
    assert fetched.body == "backoff"
  end

  test "a blob store that exits fails the tick without crashing the compactor" do
    config =
      start(roles: [:edge, :storage], storage: %{blob_store: {ExitingBlobStore, []}})

    inst = config.instance
    [env] = commit!(inst, [envelope("exit-me")])
    compactor = Ankusa.whereis(inst, :compactor)

    assert {:ok, 0} == Compactor.tick(inst)
    assert Ankusa.whereis(inst, :compactor) == compactor
    assert Process.alive?(compactor)
    assert :error == Storage.fetch(inst, env.id)
  end

  test "a stored hook that does not decode is skipped; the rest are archived and the compactor lives" do
    config = start(roles: [:edge, :storage])
    inst = config.instance

    [good, poison] = commit!(inst, [envelope("good"), envelope("poison")])

    :ok =
      Ankusa.Store.write(inst, [{:put, :hooks, Ankusa.Store.Keys.hook(poison.seq), "garbage"}],
        sync: true
      )

    assert {:ok, 1} == Compactor.tick(inst)
    assert Process.alive?(Ankusa.whereis(inst, :compactor))

    assert {:ok, fetched} = Storage.fetch(inst, good.id)
    assert fetched.body == "good"
    assert :error == Storage.fetch(inst, poison.id)

    # Its obligation is gone, so the next tick has nothing left to do.
    assert {:ok, 0} == Compactor.tick(inst)
  end

  test "a raising codec fails the tick without crashing it, and the next tick retries" do
    config = start(roles: [:edge, :storage], storage: %{codec: {RaisingOnceCodec, []}})
    inst = config.instance
    [env] = commit!(inst, [envelope("codec-retry")])

    assert {:ok, 0} == Compactor.tick(inst)
    assert Process.alive?(Ankusa.whereis(inst, :compactor))

    assert {:ok, 1} == Compactor.tick(inst)
    assert {:ok, fetched} = Storage.fetch(inst, env.id)
    assert fetched.body == "codec-retry"
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

  describe "storage.key_prefix and a shared claim store (one bucket, several nodes)" do
    test "segments go under the node's prefix, list lists the node's own, and fetch finds them" do
      config = start(roles: [:edge, :storage], storage: %{key_prefix: "node-a/"})
      inst = config.instance
      [original] = commit!(inst, [envelope(~s({"n":1}))])

      assert {:ok, 1} == Compactor.tick(inst)

      root = Ankusa.Config.path(config, "segments")
      assert [_seg] = Path.wildcard(Path.join(root, "node-a/archive/v1/dt=*/*/seg-*.seg"))
      assert Path.wildcard(Path.join(root, "archive")) == []

      assert {:ok, [_, _, _] = keys} = Ankusa.BlobStore.list(inst, :segments, "archive/v1/")
      assert Enum.all?(keys, &String.starts_with?(&1, "node-a/archive/v1/dt="))
      assert {:ok, fetched} = Storage.fetch(inst, original.id)
      assert fetched.body == original.body
    end

    test "changing the prefix moves new segments only; archived ones still fetch" do
      config = start(roles: [:edge, :storage])
      [before] = commit!(config.instance, [envelope(~s({"n":1}))])
      assert {:ok, 1} == Compactor.tick(config.instance)
      stop_supervised!({Ankusa.Instance, config.instance})

      # The same node and data directory, restarted with a prefix.
      config = %{config | storage: %{config.storage | key_prefix: "node-a/"}}
      put_config(config)
      start_supervised!({Ankusa.Instance, config})

      [later] = commit!(config.instance, [envelope(~s({"n":2}))])
      assert {:ok, 1} == Compactor.tick(config.instance)

      assert {:ok, %{body: ~s({"n":1})}} = Storage.fetch(config.instance, before.id)
      assert {:ok, %{body: ~s({"n":2})}} = Storage.fetch(config.instance, later.id)
      root = Ankusa.Config.path(config, "segments")
      assert [_] = Path.wildcard(Path.join(root, "archive/v1/dt=*/*/seg-*.seg"))
      assert [_] = Path.wildcard(Path.join(root, "node-a/archive/v1/dt=*/*/seg-*.seg"))
    end

    test "two nodes sharing one root keep their segments apart and read each other's claims" do
      shared = Path.join(System.tmp_dir!(), "ankusa_shared_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf(shared) end)

      node = fn prefix ->
        start(
          roles: [:edge, :storage],
          storage: %{key_prefix: prefix, blob_store: {Ankusa.BlobStore.LocalFS, root: shared}},
          claim_check: %{blob_stores: [{Ankusa.BlobStore.LocalFS, root: shared}]}
        )
      end

      a = node.("a/")
      b = node.("b/")

      # The same seqs on both nodes, so the same segment names.
      [env_a] = commit!(a.instance, [envelope(~s({"node":"a"}))])
      [env_b] = commit!(b.instance, [envelope(~s({"node":"b"}))])
      assert {:ok, 1} == Compactor.tick(a.instance)
      assert {:ok, 1} == Compactor.tick(b.instance)

      assert {:ok, [_, _, _] = keys_a} =
               Ankusa.BlobStore.list(a.instance, :segments, "archive/v1/")

      assert {:ok, [_, _, _] = keys_b} =
               Ankusa.BlobStore.list(b.instance, :segments, "archive/v1/")

      assert Enum.all?(keys_a, &String.starts_with?(&1, "a/archive/v1/"))

      assert Enum.map(keys_a, &String.replace_prefix(&1, "a/", "")) ==
               Enum.map(keys_b, &String.replace_prefix(&1, "b/", ""))

      assert {:ok, %{body: ~s({"node":"a"})}} = Storage.fetch(a.instance, env_a.id)
      assert {:ok, %{body: ~s({"node":"b"})}} = Storage.fetch(b.instance, env_b.id)

      # A claim one node checks in, the other node's gateway reads.
      item = %{id: "evt_claim", body: "claimed bytes"}

      {:ok, %{"evt_claim" => %{ref: ref}}} =
        Ankusa.ClaimCheck.check_in(a.instance, "acme", [item])

      assert {:ok, "claimed bytes"} = Ankusa.ClaimCheck.read(b.instance, "acme", ref.claim_id)
    end
  end

  # ── the dated layout, the segment format, time-based roll ────────────────────

  describe "dated layout" do
    test "a segment, its index and the day manifest sit under archive/v1/dt=<day>/<node>/" do
      config = start(roles: [:edge, :storage], intake: %{node_id: "node-a"})
      inst = config.instance
      [original] = commit!(inst, [uuid_envelope(~s({"n":1}))])
      assert {:ok, 1} == Compactor.tick(inst)

      day = Ankusa.Storage.Layout.day(original.id)
      dir = Path.join(segments_root(config), "archive/v1/dt=#{day}/node-a")

      assert [seg] = Path.wildcard(Path.join(dir, "seg-*.seg"))
      assert Regex.match?(~r/seg-\d{20}-\d{20}\.seg\z/, seg)
      assert [_idx] = Path.wildcard(Path.join(dir, "seg-*.idx"))

      manifest = dir |> Path.join("_manifest.json") |> File.read!() |> JSON.decode!()
      assert %{"format" => 1, "node" => "node-a", "segments" => 1, "dt" => ^day} = manifest
      assert manifest["min_id"] == original.id and manifest["max_id"] == original.id

      assert {:ok, [row]} = Ankusa.Queue.Archive.segments_containing(inst, original.id)
      assert %{format: 1, node: "node-a", dt: ^day, codec: Ankusa.Codec.Raw} = row
      assert String.ends_with?(row.key, Path.basename(seg))
    end

    test "the manifest counts each segment once, across ticks" do
      config =
        start(roles: [:edge, :storage], intake: %{node_id: "node-a"}, storage: %{roll_bytes: 1})

      inst = config.instance
      commit!(inst, [uuid_envelope("one"), uuid_envelope("two"), uuid_envelope("three")])
      assert {:ok, 3} == Compactor.tick(inst)

      assert [path] = archive_files(config, "_manifest.json")
      assert %{"segments" => 3} = path |> File.read!() |> JSON.decode!()
    end
  end

  describe "versioned header: what is on disk" do
    test "versioned header: the object starts with ANKSEG, version, codec, count and ends in its sha256" do
      config = start(roles: [:edge, :storage])
      inst = config.instance
      [first, _second] = commit!(inst, [envelope("one"), envelope("two")])
      assert {:ok, 1} == Compactor.tick(inst)

      assert [path] = archive_files(config, ".seg")
      object = File.read!(path)
      assert <<"ANKSEG", 1, 1, 2::32, _::binary>> = object
      assert {:ok, %{format: 1, codec_id: 1, count: 2}} = Ankusa.Storage.Segment.verify(object, 1)

      assert {:ok, [row]} = Ankusa.Queue.Archive.segments_containing(inst, first.id)
      assert row.bytes == byte_size(object)

      assert row.sha256 ==
               object |> binary_part(byte_size(object) - 32, 32) |> Base.encode16(case: :lower)
    end

    test "versioned header: a segment altered after the write is not served" do
      config = start(roles: [:edge, :storage])
      inst = config.instance
      [original] = commit!(inst, [envelope(~s({"n":1,"pad":"xxxxxxxxxxxxxxxx"}))])
      assert {:ok, 1} == Compactor.tick(inst)
      assert {:ok, %{body: _}} = Storage.fetch(inst, original.id)

      [path] = archive_files(config, ".seg")
      object = File.read!(path)
      # one payload byte flipped: the frame's CRC no longer matches
      <<head::binary-size(40), byte, rest::binary>> = object
      File.write!(path, <<head::binary, Bitwise.bxor(byte, 1), rest::binary>>)
      assert :error == Storage.fetch(inst, original.id)

      # and the whole-object check (what a replay runs) refuses it too
      assert {:error, :sha_mismatch} = Ankusa.Storage.Segment.verify(File.read!(path), 1)

      File.write!(path, binary_part(object, 0, byte_size(object) - 5))
      assert {:error, :sha_mismatch} = Ankusa.Storage.Segment.verify(File.read!(path), 1)
    end
  end

  describe "safe idx: what is read back from the object store is not trusted" do
    test "safe idx: an index altered after the write is {:error, _} and the hook is not served" do
      config = start(roles: [:edge, :storage])
      inst = config.instance
      [original] = commit!(inst, [envelope(~s({"n":1}))])
      assert {:ok, 1} == Compactor.tick(inst)

      [path] = archive_files(config, ".idx")
      object = File.read!(path)
      assert {:ok, _} = Ankusa.Storage.Index.decode(object)

      <<head::binary-size(20), byte, rest::binary>> = object
      File.write!(path, <<head::binary, Bitwise.bxor(byte, 1), rest::binary>>)
      assert {:error, :sha_mismatch} = Ankusa.Storage.Index.decode(File.read!(path))
      assert :error == Storage.fetch(inst, original.id)
    end

    test "safe idx: an index that names an atom this node never saw is refused, not interned" do
      config = start(roles: [:edge, :storage])
      inst = config.instance
      [original] = commit!(inst, [envelope(~s({"n":1}))])
      assert {:ok, 1} == Compactor.tick(inst)

      name = "never_interned_dur8_" <> Integer.to_string(System.unique_integer([:positive]))

      # a legacy (headerless) index whose one value is an atom nobody created
      hostile = <<131, 116, 1::32, 109, 1::32, "a", 119, byte_size(name), name::binary>>
      [path] = archive_files(config, ".idx")
      File.write!(path, hostile)

      assert {:error, :bad_index} = Ankusa.Storage.Index.decode(hostile)
      assert :error == Storage.fetch(inst, original.id)

      assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
    end
  end

  describe "time roll" do
    test "time roll: a partial segment waits storage.roll_ms, then is written" do
      config = start(roles: [:edge, :storage], storage: %{interval_ms: 20, roll_ms: 400})
      inst = config.instance
      [original] = commit!(inst, [envelope(~s({"n":1}))])

      Process.sleep(150)
      assert archive_files(config, ".seg") == []

      # the segment's object lands before its catalogue row: wait for the hook itself
      assert eventually(fn -> match?({:ok, _}, Storage.fetch(inst, original.id)) end, 3_000)
      assert [_] = archive_files(config, ".seg")
    end

    test "time roll: a backlog of roll_bytes rolls at once, only the remainder waits" do
      config =
        start(
          roles: [:edge, :storage],
          storage: %{interval_ms: 20, roll_ms: 60_000, roll_bytes: 1}
        )

      inst = config.instance
      commit!(inst, [envelope("one"), envelope("two"), envelope("three")])

      assert eventually(fn -> length(archive_files(config, ".seg")) == 2 end, 3_000)
      Process.sleep(150)
      assert length(archive_files(config, ".seg")) == 2

      # an asked-for tick does not wait for the roll
      assert {:ok, 1} == Compactor.tick(inst)
      assert length(archive_files(config, ".seg")) == 3
    end

    test "time roll: roll_ms: 0 writes every tick" do
      config = start(roles: [:edge, :storage], storage: %{interval_ms: 20, roll_ms: 0})
      [original] = commit!(config.instance, [envelope(~s({"n":1}))])

      assert eventually(
               fn -> match?({:ok, _}, Storage.fetch(config.instance, original.id)) end,
               3_000
             )
    end
  end

  describe "storage.max_segments_per_tick" do
    test "one tick writes at most that many segments; the next tick takes the rest" do
      config =
        start(roles: [:edge, :storage], storage: %{roll_bytes: 1, max_segments_per_tick: 2})

      inst = config.instance
      commit!(inst, Enum.map(1..5, &envelope("hook #{&1}")))

      assert {:ok, 2} == Compactor.tick(inst)
      assert {:ok, 2} == Compactor.tick(inst)
      assert {:ok, 1} == Compactor.tick(inst)
      assert {:ok, 0} == Compactor.tick(inst)
    end
  end

  describe "reconcile other node" do
    test "reconcile other node: a second node serves the first's segments by day, then reconciles them" do
      root = shared_root()
      a = shared_node(root, "node-a")
      [original] = commit!(a.instance, [uuid_envelope(~s({"from":"a"}))])
      assert {:ok, 1} == Compactor.tick(a.instance)
      stop_supervised!({Ankusa.Instance, a.instance})

      b = shared_node(root, "node-b")
      inst = b.instance

      # nothing in b's catalogue, yet the day prefix leads to the segment
      assert {:ok, []} = Ankusa.Queue.Archive.segments_containing(inst, original.id)
      assert {:ok, %{body: ~s({"from":"a"})}} = Storage.fetch(inst, original.id)

      assert {:ok, %{catalogued: 1, present: 0, conflicts: 0, skipped: 0}} =
               Ankusa.Queue.Archive.reconcile(inst)

      assert {:ok, [row]} = Ankusa.Queue.Archive.segments_containing(inst, original.id)
      assert %{node: "node-a", format: 1} = row
      assert {:ok, %{body: ~s({"from":"a"}), seq: seq}} = Storage.fetch(inst, original.id)
      assert seq == original.seq

      # idempotent
      assert {:ok, %{catalogued: 0, present: 1}} = Ankusa.Queue.Archive.reconcile(inst)
    end

    test "reconcile other node: another node's segment never settles this node's own obligations" do
      root = shared_root()
      a = shared_node(root, "node-a")
      [from_a] = commit!(a.instance, [uuid_envelope(~s({"from":"a"}))])
      assert {:ok, 1} == Compactor.tick(a.instance)
      stop_supervised!({Ankusa.Instance, a.instance})

      b = shared_node(root, "node-b")
      inst = b.instance
      # the same seq as a's hook, a different hook
      [from_b] = commit!(inst, [uuid_envelope(~s({"from":"b"}))])
      assert from_b.seq == from_a.seq

      assert {:ok, %{catalogued: 1}} = Ankusa.Queue.Archive.reconcile(inst)
      assert {:ok, _} = Store.get(inst, :index, Keys.archive_pending(from_b.seq))

      assert {:ok, 1} == Compactor.tick(inst)
      assert {:ok, %{body: ~s({"from":"b"})}} = Storage.fetch(inst, from_b.id)
      # a's row was overwritten by b's at the same first seq; the day scan still finds it
      assert {:ok, %{body: ~s({"from":"a"})}} = Storage.fetch(inst, from_a.id)
    end

    test "reconcile other node: node: :own leaves the others alone" do
      root = shared_root()
      a = shared_node(root, "node-a")
      commit!(a.instance, [uuid_envelope(~s({"from":"a"}))])
      assert {:ok, 1} == Compactor.tick(a.instance)
      stop_supervised!({Ankusa.Instance, a.instance})

      b = shared_node(root, "node-b")
      assert {:ok, %{catalogued: 0}} = Ankusa.Queue.Archive.reconcile(b.instance, node: :own)
      assert {:ok, %{catalogued: 1}} = Ankusa.Queue.Archive.reconcile(b.instance, node: "node-a")
    end

    test "reconcile other node: an id that carries no time is found only through the catalogue" do
      root = shared_root()
      a = shared_node(root, "node-a")
      [original] = commit!(a.instance, [envelope(~s({"from":"a"}))])
      assert {:ok, 1} == Compactor.tick(a.instance)
      stop_supervised!({Ankusa.Instance, a.instance})

      b = shared_node(root, "node-b")
      assert :error == Storage.fetch(b.instance, original.id)
      assert {:ok, %{catalogued: 1}} = Ankusa.Queue.Archive.reconcile(b.instance)
      assert {:ok, %{body: ~s({"from":"a"})}} = Storage.fetch(b.instance, original.id)
    end
  end

  describe "legacy seg/ layout" do
    test "a legacy segment and its row (no format field) stay readable" do
      config = start(roles: [:edge, :storage])
      inst = config.instance
      env = %{envelope(~s({"legacy":true})) | seq: 1}
      row = write_legacy_segment(inst, env)
      assert :ok = Ankusa.Queue.Archive.archived(inst, row, [])

      assert {:ok, %{body: ~s({"legacy":true}), seq: 1}} = Storage.fetch(inst, env.id)
    end

    test "reconcile catalogues a legacy segment as format 0" do
      config = start(roles: [:edge, :storage])
      inst = config.instance
      env = %{envelope(~s({"legacy":true})) | seq: 1}
      write_legacy_segment(inst, env)
      assert :error == Storage.fetch(inst, env.id)

      assert {:ok, %{catalogued: 1}} = Ankusa.Queue.Archive.reconcile(inst, node: :own)
      assert {:ok, [row]} = Ankusa.Queue.Archive.segments_containing(inst, env.id)
      assert %{format: 0, codec: Ankusa.Codec.Raw, node: nil, dt: nil} = row
      assert {:ok, %{body: ~s({"legacy":true})}} = Storage.fetch(inst, env.id)
    end
  end
end
