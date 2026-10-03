defmodule Ankusa.StoreMigrateTest do
  @moduledoc """
  A 0.3 data dir is imported once, on first boot of the new store. The legacy
  bytes are written by `Ankusa.Test.LegacyDataDir`, which pins the 0.3 formats
  independently of any code that still exists.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.{Config, Envelope, Queue, SourceStore, Storage}
  alias Ankusa.Dispatch.Pipeline
  alias Ankusa.Edge.{Ingest, Quarantine, RateLimiter}
  alias Ankusa.Storage.Compactor
  alias Ankusa.Test.LegacyDataDir, as: Legacy

  @moduletag capture_log: true

  defmodule CaptureSink do
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, ctx, opts) do
      send(Keyword.fetch!(opts, :to), {:delivered, env.id, ctx.attempt})
      :ok
    end
  end

  defp demo_source(extra \\ []) do
    [verifier: {Ankusa.Verifier.None, []}, sinks: [{CaptureSink, to: self()}]] ++ extra
  end

  defp config(overrides \\ []) do
    test_config(
      Keyword.merge(
        [
          roles: [:edge, :dispatch, :storage],
          storage: %{interval_ms: 0},
          source_store: {Ankusa.SourceStore.Static, sources: %{"demo" => demo_source()}}
        ],
        overrides
      )
    )
  end

  defp envelope(seq, attrs \\ []) do
    body = "body-#{seq}"

    struct!(
      Envelope,
      Keyword.merge(
        [
          id: Ankusa.UUIDv7.generate(),
          source_id: "demo",
          tenant_id: nil,
          received_at: System.system_time(:millisecond),
          method: "POST",
          path: "/webhooks/demo",
          headers: [{"content-type", "text/plain"}],
          content_type: "text/plain",
          body: body,
          size: byte_size(body),
          seq: seq
        ],
        attrs
      )
    )
  end

  defp boot(config) do
    put_config(config)
    start_supervised!({Ankusa.Instance, config})
    config.instance
  end

  defp wal_dir(config), do: Config.path(config, "wal")
  defp migrated(config, name), do: Path.wildcard(Config.path(config, name) <> ".migrated-*")

  describe "the queue" do
    test "imports only what 0.3 had not finished: undelivered hooks deliver, unarchived hooks archive" do
      config = config()
      envs = for seq <- 1..6, do: envelope(seq)
      ids = Map.new(envs, &{&1.seq, &1.id})

      # Seq 1 is below the truncation floor, 2 is delivered and archived, 3 and 4
      # are delivered but not archived, 5 and 6 were never dispatched.
      Legacy.wal!(config, envs, cursors: %{dispatch: 4, compactor: 2}, floor: 1)

      inst = boot(config)
      {:ok, _} = Pipeline.tick(inst)

      assert_received {:delivered, id5, 1}
      assert_received {:delivered, id6, 1}
      assert Enum.sort([id5, id6]) == Enum.sort([ids[5], ids[6]])
      refute_received {:delivered, _, _}

      # 3..6 owe the archive an object; 1 and 2 owe nothing and are not imported.
      assert {:ok, 1} = Compactor.tick(inst)
      for seq <- 3..6, do: assert({:ok, %{id: _}} = Storage.fetch(inst, ids[seq]))
      for seq <- 1..2, do: assert(Storage.fetch(inst, ids[seq]) == :error)

      # The artifact was moved aside, not deleted, and seqs continue past it.
      refute File.exists?(wal_dir(config))
      assert [_] = migrated(config, "wal")
      assert {:ok, %Envelope{seq: 7}} = Ingest.ingest(inst, request("demo", "new"))
    end

    test "an imported hook is delivered to the source's sinks as they are now" do
      # No `:storage` role, so the hook owes the archive nothing.
      config = config(roles: [:edge, :dispatch])
      Legacy.wal!(config, [envelope(1)], cursors: %{dispatch: 0, compactor: 0})
      inst = boot(config)

      {:ok, 1} = Pipeline.tick(inst)

      assert_received {:delivered, _id, 1}
      # Delivered and (no archive obligation without :storage) reclaimed.
      assert stored_ids(inst) == []
    end

    test "dead letters carry over, with today's reason text, and replay to the current sinks" do
      config = config()
      dead = envelope(1)

      Legacy.log!(Path.join(Config.path(config, "dlq"), "dlq.log"), [
        Legacy.dlq_entry(dead, {:sink, SomeSink, :boom}, 1_700_000_000_000)
      ])

      inst = boot(config)

      assert {:ok, %{total: 1, entries: [entry]}} = Queue.dead(inst, limit: 10)
      assert entry.envelope.id == dead.id
      assert entry.reason == inspect({:sink, SomeSink, :boom})
      assert entry.at == 1_700_000_000_000

      assert {:ok, :created, _job} = Ankusa.Replay.start(inst, %{kind: :dlq})
      {:ok, _} = Pipeline.tick(inst)
      assert_receive {:delivered, id, 1}, 5_000
      assert id == dead.id
      assert {:ok, %{total: 0}} = Queue.dead(inst, limit: 10)
    end

    test "the next seq is past everything imported, including a dead letter" do
      config = config(roles: [:edge, :dispatch])

      Legacy.log!(Path.join(Config.path(config, "dlq"), "dlq.log"), [
        Legacy.dlq_entry(envelope(41), {:sink, SomeSink, :boom}, 1)
      ])

      inst = boot(config)

      assert {:ok, %Envelope{seq: 42}} = Ingest.ingest(inst, request("demo", "x"))
    end

    test "a hook dead-lettered repeatedly in 0.3 is one dead row, with the last reason" do
      config = config()
      dead = envelope(1)

      # 1,001 records cross the 1,000-record import batch boundary, so the
      # dedupe must survive a flush. 0.3 left the old DLQ entry in place on
      # every replay, so a real dlq.log can look like this.
      records =
        Enum.map(1..1_001, fn i ->
          Legacy.dlq_entry(dead, {:sink, SomeSink, i}, 1_700_000_000_000 + i)
        end)

      Legacy.log!(Path.join(Config.path(config, "dlq"), "dlq.log"), records)

      inst = boot(config)

      assert {:ok, %{total: 1, entries: [entry]}} = Queue.dead(inst, limit: 10)
      assert entry.reason == inspect({:sink, SomeSink, 1_001})
      assert entry.at == 1_700_000_001_001

      assert {:ok, :created, _job} = Ankusa.Replay.start(inst, %{kind: :dlq})
    end
  end

  describe "everything else 0.3 kept" do
    test "the quarantine pen, API sources, rate-limit overrides and the segment index carry over" do
      parent = self()

      decoder = fn _source_id, _spec ->
        [verifier: {Ankusa.Verifier.None, []}, sinks: [{CaptureSink, to: parent}]]
      end

      config =
        config(
          source_store:
            {Ankusa.SourceStore.Persistent, decoder: decoder, sources: %{"demo" => demo_source()}}
        )

      held = envelope(nil, id: "q-1", received_at: 1_700_000_000_000)

      Legacy.log!(Path.join(Config.path(config, "quarantine"), "quarantine.log"), [
        %{
          id: held.id,
          source_id: "demo",
          received_at: held.received_at,
          reason: {:verification_failed, :bad_signature},
          headers: [{"x-sig", "nope"}],
          body: "forged"
        }
      ])

      Legacy.sources_json!(config, [{"acme", "billing", %{"sinks" => [%{"type" => "log"}]}}])
      Legacy.rate_limits_json!(config, [{"acme", 2, 4}])

      archived = [envelope(1, id: "01-archived"), envelope(2, id: "02-archived")]

      rows =
        Legacy.segment!(config, "seg/00000000000000000001-00000000000000000002.seg", archived)

      Legacy.log!(Config.path(config, "segments/index.log"), rows)

      inst = boot(config)

      assert {:ok, [entry]} = Quarantine.recent(inst, 10)
      assert entry.id == "q-1"
      assert entry.source_id == "demo"
      assert entry.reason == {:verification_failed, :bad_signature}

      assert {:ok, %{tenant: "acme", name: "billing"}} = SourceStore.get(inst, "acme", "billing")
      assert RateLimiter.effective(inst, "acme") == {%{rate: 2, burst: 4}, :override}

      for env <- archived do
        assert {:ok, fetched} = Storage.fetch(inst, env.id)
        assert fetched.body == env.body
        assert fetched.seq == env.seq
      end

      for name <- ["sources.json", "rate_limits.json", "quarantine", "segments/index.log"] do
        assert [_] = migrated(config, name), "#{name} was not moved aside"
      end
    end
  end

  describe "an artifact that cannot be trusted stops the boot" do
    test "damage in the middle of the WAL, with acked frames after it, refuses to start" do
      config = config(roles: [:edge])
      path = Legacy.wal!(config, for(seq <- 1..10, do: envelope(seq)))
      third = path |> Legacy.frame_offsets() |> Enum.at(2)
      Legacy.flip_byte!(path, third + 30)

      Process.flag(:trap_exit, true)
      put_config(config)
      assert {:error, error} = start_supervised({Ankusa.Instance, config})

      assert {:damaged_legacy_wal, ^path, bad, later} = find_damage(error)
      assert bad == third
      assert later > bad

      # Untouched: the operator decides what to do with it.
      assert File.exists?(wal_dir(config))
      assert migrated(config, "wal") == []
    end

    test "a torn final frame is an unacked write: every complete frame imports" do
      config = config(roles: [:edge])
      envs = for seq <- 1..5, do: envelope(seq)
      path = Legacy.wal!(config, envs)

      half = IO.iodata_to_binary(Legacy.wal_frame(envelope(6)))
      File.write!(path, binary_part(half, 0, 30), [:append])

      inst = boot(config)

      assert {:ok, hooks} = Queue.hooks(inst, 0, 100)
      assert Enum.map(hooks, & &1.id) == Enum.map(envs, & &1.id)
    end

    test "a cursor file that is not what 0.3 wrote refuses to start rather than guess" do
      config = config(roles: [:edge])
      Legacy.wal!(config, [envelope(1)])
      cursors = Path.join(wal_dir(config), "ankusa.wal.cursors")
      File.write!(cursors, "garbage")

      Process.flag(:trap_exit, true)
      put_config(config)
      assert {:error, error} = start_supervised({Ankusa.Instance, config})

      assert {:corrupt_legacy_sidecar, ^cursors} = find_corrupt_sidecar(error)
      assert File.exists?(wal_dir(config))
    end

    test "a frame with a valid checksum around a term that will not decode refuses to start" do
      config = config(roles: [:edge])
      path = Legacy.wal!(config, [envelope(1)])

      payload = "not an external term"
      crc = :erlang.crc32(payload)
      frame = <<0x484B::16, 1::8, 0::8, 2::64, crc::32, byte_size(payload)::32, payload::binary>>
      File.write!(path, frame, [:append])

      Process.flag(:trap_exit, true)
      put_config(config)
      assert {:error, error} = start_supervised({Ankusa.Instance, config})

      assert {:legacy_import_failed, _} = find(error, &match?({:legacy_import_failed, _}, &1))
      assert File.exists?(wal_dir(config))
    end

    test "a sources.json that cannot be read refuses to start; it is not treated as empty" do
      config = config(roles: [:edge])

      path =
        Legacy.sources_json!(config, [{"acme", "billing", %{"sinks" => [%{"type" => "log"}]}}])

      File.chmod!(path, 0o000)
      on_exit(fn -> File.chmod(path, 0o644) end)

      # Root reads anything; then there is nothing to test.
      if match?({:error, :eacces}, File.read(path)) do
        Process.flag(:trap_exit, true)
        put_config(config)
        assert {:error, error} = start_supervised({Ankusa.Instance, config})

        assert {:legacy_read_failed, ^path, :eacces} =
                 find(error, &match?({:legacy_read_failed, _, _}, &1))

        assert File.exists?(path)
      end
    end

    test "a rate_limits.json that cannot be read refuses to start; it is not treated as empty" do
      config = config(roles: [:edge])
      path = Legacy.rate_limits_json!(config, [{"acme", 5, 10}])
      File.chmod!(path, 0o000)
      on_exit(fn -> File.chmod(path, 0o644) end)

      # Root reads anything; then there is nothing to test.
      if match?({:error, :eacces}, File.read(path)) do
        Process.flag(:trap_exit, true)
        put_config(config)
        assert {:error, error} = start_supervised({Ankusa.Instance, config})

        assert {:legacy_read_failed, ^path, :eacces} =
                 find(error, &match?({:legacy_read_failed, _, _}, &1))

        assert File.exists?(path)
      end
    end

    test "a length prefix no 0.3 record had stops that file's import, with the byte count" do
      config = config(roles: [:edge])
      dlq = Path.join(Config.path(config, "dlq"), "dlq.log")
      File.mkdir_p!(Path.dirname(dlq))
      File.write!(dlq, <<0x7FFFFFFF::32>>)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          inst = boot(config)
          assert {:ok, %{total: 0}} = Queue.dead(inst, limit: 10)
        end)

      assert log =~ "claims 2147483647 bytes"
      assert Path.wildcard(Config.path(config, "dlq") <> ".migrated-*") != []
    end
  end

  describe "an import happens once" do
    test "a 0.3 artifact that shows up after the first boot is never imported over live data" do
      config = config()
      inst = boot(config)
      assert {:ok, %Envelope{seq: 1}} = Ingest.ingest(inst, request("demo", "live"))
      {:ok, _} = Pipeline.tick(inst)
      assert_received {:delivered, _live, 1}

      stop_supervised!({Ankusa.Instance, config.instance})
      Legacy.wal!(config, [envelope(1), envelope(2)])
      inst = boot(config)

      {:ok, _} = Pipeline.tick(inst)
      refute_received {:delivered, _, _}
      assert File.exists?(wal_dir(config))
      assert migrated(config, "wal") == []
    end

    test "restarting after an import does not import again" do
      config = config()
      Legacy.wal!(config, [envelope(1)])
      inst = boot(config)
      {:ok, 1} = Pipeline.tick(inst)
      assert_received {:delivered, _, 1}

      stop_supervised!({Ankusa.Instance, config.instance})
      boot(config)
      {:ok, 0} = Pipeline.tick(inst)
      refute_received {:delivered, _, _}
    end

    test "under wal: :none the queue artifacts are left for a node that has a queue" do
      config =
        config(
          roles: [:edge],
          wal: :none,
          source_store:
            {Ankusa.SourceStore.Static,
             sources: %{
               "demo" => [
                 verifier: {Ankusa.Verifier.None, []},
                 sinks: [{CaptureSink, to: self()}]
               ]
             }}
        )

      Legacy.wal!(config, [envelope(1)])
      Legacy.rate_limits_json!(config, [{"acme", 2, 4}])

      inst = boot(config)

      # What a direct node can use is imported...
      assert RateLimiter.effective(inst, "acme") == {%{rate: 2, burst: 4}, :override}
      assert [_] = migrated(config, "rate_limits.json")
      # ...and the queue is not touched.
      assert File.exists?(wal_dir(config))
      assert migrated(config, "wal") == []
    end
  end

  # The instance supervisor wraps a failed child's reason; find the one we want.
  defp find_damage(term), do: find(term, &match?({:damaged_legacy_wal, _, _, _}, &1))
  defp find_corrupt_sidecar(term), do: find(term, &match?({:corrupt_legacy_sidecar, _}, &1))

  defp find(term, pred) do
    cond do
      pred.(term) -> term
      is_tuple(term) -> term |> Tuple.to_list() |> Enum.find_value(&find(&1, pred))
      is_list(term) -> Enum.find_value(term, &find(&1, pred))
      true -> nil
    end
  end
end
