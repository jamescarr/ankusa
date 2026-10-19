defmodule Ankusa.Store.BackupTest do
  @moduledoc """
  Host loss: a backup taken while the node runs, the whole `data_dir` gone,
  and a restart on the empty directory must read back every hook the backup
  held. Unreachable or damaged backups refuse the boot instead of starting
  empty, retention never deletes what a kept backup needs, and segments
  archived after the backup are found again without their seqs being reused.

  The object store is a `LocalFS` root outside `data_dir`, standing in for a
  bucket that survives the host.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.BlobStore.LocalFS
  alias Ankusa.{Envelope, Storage, Store}
  alias Ankusa.Storage.Compactor
  alias Ankusa.Store.Backup
  alias Ankusa.Test.CountingBlobStore

  defp blob_root do
    root = unique_data_dir(:bucket)
    on_exit(fn -> File.rm_rf(root) end)
    root
  end

  # An instance whose segments and backups go to `root`. Every hook has a sink
  # and no node dispatches it, so it stays in the store.
  defp start(root, opts \\ []) do
    config = config(root, opts)
    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp config(root, opts) do
    blob_store = Keyword.get(opts, :blob_store, {LocalFS, root: root})

    test_config(
      roles: Keyword.get(opts, :roles, [:edge]),
      source_store:
        {Ankusa.SourceStore.Static, sources: %{"acme" => %{sinks: [{Ankusa.Sink.Log, []}]}}},
      storage: %{interval_ms: 0, blob_store: blob_store},
      backup:
        Map.merge(
          %{enabled: true, interval_ms: 3_600_000},
          Map.new(Keyword.get(opts, :backup, %{}))
        )
    )
  end

  # The host is gone: nothing of `data_dir` survives, the bucket does.
  defp lose_host_and_restart(config) do
    stop_supervised!({Ankusa.Instance, config.instance})
    File.rm_rf!(config.data_dir)
    start_supervised!({Ankusa.Instance, config})
  end

  defp lose_host_and_boot(config) do
    stop_supervised!({Ankusa.Instance, config.instance})
    File.rm_rf!(config.data_dir)
    Process.flag(:trap_exit, true)
    Ankusa.Instance.start_link(config)
  end

  defp envelope(body \\ "{}") do
    %Envelope{
      id: "evt_" <> Integer.to_string(System.unique_integer([:positive])),
      source_id: "acme",
      tenant_id: "default",
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/hooks/acme",
      headers: [{"content-type", "application/json"}],
      content_type: "application/json",
      body: body,
      size: byte_size(body)
    }
  end

  defp commit!(inst, n, body \\ "{}"), do: for(_ <- 1..n, do: enqueue!(inst, envelope(body)))

  defp drain_puts(acc \\ []) do
    receive do
      {:blob_put, key} -> drain_puts([key | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp restore_reason({:error, {:shutdown, {:failed_to_start_child, {Store, _}, reason}}}),
    do: reason

  test "a backup, a wiped data dir and a restart bring back every acked hook" do
    root = blob_root()
    config = start(root)
    inst = config.instance

    committed = commit!(inst, 200)
    assert {:ok, %{uploaded: uploaded}} = Backup.run(inst)
    assert uploaded > 0

    lose_host_and_restart(config)

    assert stored_ids(inst) == Enum.map(committed, & &1.id)

    # The restored store hands out seqs above every one it already holds.
    next = enqueue!(inst, envelope())
    assert next.seq > committed |> Enum.map(& &1.seq) |> Enum.max()
  end

  test "a second backup uploads no shared file when nothing changed, and new ones after writes" do
    root = blob_root()
    config = start(root, blob_store: {CountingBlobStore, root: root, pid: self()})
    inst = config.instance

    commit!(inst, 20)
    assert {:ok, _} = Backup.run(inst)
    first = drain_puts()
    assert Enum.any?(first, &String.starts_with?(&1, "backup/sst/"))

    assert {:ok, %{id: id}} = Backup.run(inst)
    second = drain_puts()
    refute Enum.any?(second, &String.starts_with?(&1, "backup/sst/"))
    assert Enum.any?(second, &String.starts_with?(&1, "backup/#{id}/"))

    commit!(inst, 50)
    assert {:ok, _} = Backup.run(inst)
    assert Enum.any?(drain_puts(), &String.starts_with?(&1, "backup/sst/"))
  end

  test "with no backup in the store yet, an empty data dir starts empty" do
    config = start(blob_root())
    assert stored_ids(config.instance) == []
  end

  test "an unreachable backup store refuses to boot instead of starting empty" do
    root = blob_root()
    config = config(root, blob_store: {CountingBlobStore, root: root, get_error: :econnrefused})
    File.rm_rf!(config.data_dir)
    Process.flag(:trap_exit, true)

    assert {:store_restore_failed, _path, {:backup_unreachable, :econnrefused}} =
             restore_reason(Ankusa.Instance.start_link(config))

    refute File.exists?(Path.join(Ankusa.Config.path(config, "store"), "CURRENT"))
  end

  test "a file that fails its checksum refuses to boot" do
    root = blob_root()
    config = start(root)
    inst = config.instance

    commit!(inst, 50)
    assert {:ok, _} = Backup.run(inst)

    [sst | _] = root |> Path.join("backup/sst/*.sst") |> Path.wildcard() |> Enum.sort()
    <<first, rest::binary>> = File.read!(sst)
    File.write!(sst, <<Bitwise.bxor(first, 1), rest::binary>>)

    name = Path.basename(sst)

    assert {:store_restore_failed, _path, {:checksum_mismatch, ^name}} =
             restore_reason(lose_host_and_boot(config))
  end

  test "a restore interrupted part-way is started over by the next boot" do
    root = blob_root()
    config = start(root)
    inst = config.instance
    committed = commit!(inst, 50)
    assert {:ok, _} = Backup.run(inst)

    # What a crash mid-restore leaves: the marker, some files, no CURRENT.
    stop_supervised!({Ankusa.Instance, inst})
    File.rm_rf!(config.data_dir)
    store_dir = Ankusa.Config.path(config, "store")
    File.mkdir_p!(store_dir)
    File.write!(Path.join(store_dir, "RESTORE-IN-PROGRESS"), "x")
    File.write!(Path.join(store_dir, "000004.sst"), "half a file")

    start_supervised!({Ankusa.Instance, config})
    assert stored_ids(inst) == Enum.map(committed, & &1.id)
    refute File.exists?(Path.join(store_dir, "RESTORE-IN-PROGRESS"))
  end

  test "a restore whose archive reconcile failed is reconciled by the next boot" do
    root = blob_root()
    config = start(root, roles: [:edge, :storage])
    inst = config.instance

    before = commit!(inst, 5)
    assert {:ok, _} = Backup.run(inst)
    later = commit!(inst, 5)
    assert {:ok, 1} = Compactor.tick(inst)

    # The restore succeeds, then the segment listing the reconcile needs fails:
    # the database is whole (CURRENT is there) but behind the bucket.
    unreachable = %{
      config
      | storage: %{
          config.storage
          | blob_store: {CountingBlobStore, root: root, list_error: :econnrefused}
        }
    }

    stop_supervised!({Ankusa.Instance, inst})
    File.rm_rf!(config.data_dir)
    Process.flag(:trap_exit, true)

    assert {:store_restore_failed, _path, {:archive_reconcile_failed, _}} =
             restore_reason(Ankusa.Instance.start_link(unreachable))

    store_dir = Ankusa.Config.path(config, "store")
    assert File.exists?(Path.join(store_dir, "CURRENT"))
    assert File.exists?(Path.join(store_dir, "RESTORE-IN-PROGRESS"))

    # The next boot must not take that database as an existing, reconciled one.
    log =
      ExUnit.CaptureLog.capture_log(fn -> start_supervised!({Ankusa.Instance, config}) end)

    assert log =~ "reconciling now"
    refute File.exists?(Path.join(store_dir, "RESTORE-IN-PROGRESS"))

    for env <- before ++ later do
      assert {:ok, fetched} = Storage.fetch(inst, env.id)
      assert fetched.seq == env.seq
    end

    next = enqueue!(inst, envelope())
    assert next.seq > Enum.max(Enum.map(later, & &1.seq))
  end

  test "database files without CURRENT that no restore left are refused, not deleted" do
    root = blob_root()
    config = start(root)
    assert {:ok, _} = Backup.run(config.instance)

    stop_supervised!({Ankusa.Instance, config.instance})
    store_dir = Ankusa.Config.path(config, "store")
    File.rm!(Path.join(store_dir, "CURRENT"))
    before = File.ls!(store_dir)

    Process.flag(:trap_exit, true)

    assert {:store_restore_failed, _path, {:unrecognized_store_files, [_ | _] = names}} =
             restore_reason(Ankusa.Instance.start_link(config))

    assert Enum.all?(names, &(&1 in before))
    # Nothing was deleted to make room for the backup.
    assert before -- File.ls!(store_dir) == []
  end

  test "retention keeps `keep` backups and the shared files they reference" do
    root = blob_root()
    config = start(root, backup: %{keep: 2})
    inst = config.instance

    ids =
      for _ <- 1..3 do
        commit!(inst, 50)
        assert {:ok, %{id: id}} = Backup.run(inst)
        # ids sort by their millisecond; keep two runs out of one.
        Process.sleep(5)
        id
      end

    [first, second, third] = ids
    backup_dir = Path.join(root, "backup")

    manifests = backup_dir |> Path.join("*/manifest.json") |> Path.wildcard()

    assert Enum.sort(Enum.map(manifests, &(&1 |> Path.dirname() |> Path.basename()))) ==
             Enum.sort([second, third])

    # Every object of the first is gone (LocalFS leaves the empty directory).
    assert Path.wildcard(Path.join([backup_dir, first, "*"])) == []
    assert File.read!(Path.join(backup_dir, "LATEST")) == third

    referenced =
      for id <- [second, third],
          file <-
            Path.join([backup_dir, id, "manifest.json"])
            |> File.read!()
            |> JSON.decode!()
            |> Map.fetch!("files"),
          file["kind"] == "shared",
          into: MapSet.new(),
          do: file["name"]

    stored = backup_dir |> Path.join("sst") |> File.ls!() |> MapSet.new()
    assert stored == referenced
  end

  test "segments archived after the backup are re-catalogued and their seqs are not reused" do
    root = blob_root()
    {:ok, failures} = Agent.start_link(fn -> 0 end)

    config =
      start(root,
        roles: [:edge, :storage],
        blob_store: {CountingBlobStore, root: root, failures: failures}
      )

    inst = config.instance

    before = commit!(inst, 5)
    assert {:ok, _} = Backup.run(inst)
    later = commit!(inst, 5)
    assert {:ok, 1} = Compactor.tick(inst)

    # The host is lost with the backup as its last one: a clean stop would take
    # a final backup holding `later`, so the object store is down for it.
    Agent.update(failures, fn _ -> 1_000 end)
    ExUnit.CaptureLog.capture_log(fn -> stop_supervised!({Ankusa.Instance, inst}) end)
    Agent.update(failures, fn _ -> 0 end)
    File.rm_rf!(config.data_dir)
    start_supervised!({Ankusa.Instance, config})

    # The restored store holds only what the backup did...
    assert stored_ids(inst) == Enum.map(before, & &1.id)

    # ...and the archive copies of the rest are readable again.
    for env <- before ++ later do
      assert {:ok, fetched} = Storage.fetch(inst, env.id)
      assert fetched.body == env.body
      assert fetched.seq == env.seq
    end

    # The segment settled the restored hooks' archive obligations: the
    # compactor writes nothing over it.
    assert {:ok, 0} = Compactor.tick(inst)

    next = enqueue!(inst, envelope())
    assert next.seq > Enum.max(Enum.map(later, & &1.seq))
    assert {:ok, 1} = Compactor.tick(inst)

    for env <- later do
      assert {:ok, _} = Storage.fetch(inst, env.id)
    end
  end

  test "a node refuses to back up over another store's backups; a restored store carries on" do
    root = blob_root()

    # Two nodes booted on one prefix before either backed up: two stores.
    a = start(root)
    b = start(root)
    commit!(a.instance, 20)
    commit!(b.instance, 5)
    assert {:ok, %{id: first}} = Backup.run(a.instance)

    # The second must neither upload over A's backups nor purge them.
    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:error, {:foreign_backup, _a_id, _b_id}} = Backup.run(b.instance)
      end)

    assert log =~ "backup refused"
    assert File.read!(Path.join([root, "backup", "LATEST"])) == first
    assert File.exists?(Path.join([root, "backup", first, "manifest.json"]))

    assert {:ok, %{id: second}} = Backup.run(a.instance)
    stop_supervised!({Ankusa.Instance, b.instance})

    # A's host is lost: the restored store inherits A's id and backs up again.
    lose_host_and_restart(a)
    assert {:ok, %{id: third}} = Backup.run(a.instance)
    assert third != second
  end

  test "two live nodes with the same store id: the one whose LATEST was moved stops" do
    root = blob_root()
    a = start(root)
    commit!(a.instance, 10)
    assert {:ok, _} = Backup.run(a.instance)

    # B boots on an empty volume with A's prefix while A runs: it restores A's
    # backup and inherits A's store id, so the id alone cannot tell them apart.
    b = start(root)
    assert {:ok, %{id: from_b}} = Backup.run(b.instance)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:error, {:prefix_shared, ^from_b}} = Backup.run(a.instance)
      end)

    assert log =~ "did not write"
    assert File.read!(Path.join([root, "backup", "LATEST"])) == from_b
    assert File.exists?(Path.join([root, "backup", from_b, "manifest.json"]))

    # B wrote LATEST itself, so it carries on.
    assert {:ok, _} = Backup.run(b.instance)
  end

  test "a LATEST put of our own that landed but failed never locks this node out" do
    root = blob_root()
    {:ok, lost} = Agent.start_link(fn -> 0 end)
    {:ok, failures} = Agent.start_link(fn -> 0 end)
    store = {CountingBlobStore, root: root, lost_acks: lost, failures: failures}
    config = start(root, blob_store: store)
    inst = config.instance

    commit!(inst, 5)
    assert {:ok, _} = Backup.run(inst)

    # Our LATEST lands but the answer is lost, then a run of failed attempts.
    Agent.update(lost, fn _ -> 1 end)
    commit!(inst, 5)

    ExUnit.CaptureLog.capture_log(fn ->
      assert {:error, :timeout} = Backup.run(inst)

      for _ <- 1..10 do
        Agent.update(failures, fn _ -> 1 end)
        commit!(inst, 1)
        assert {:error, _} = Backup.run(inst)
      end
    end)

    # LATEST still names a backup this process wrote: the next run goes ahead.
    assert {:ok, _} = Backup.run(inst)
  end

  test "a checkpoint in flight does not block readiness" do
    config = start(blob_root())
    inst = config.instance
    store = Ankusa.whereis(inst, :store)

    commit!(inst, 400, String.duplicate("x", 10_240))

    dir = Path.join(config.data_dir, "checkpoint-under-test")
    task = Task.async(fn -> Store.checkpoint(inst, dir) end)

    # Hold the checkpoint process once its NIF returns, before it reports.
    pid = in_flight_checkpoint(store, task)
    assert is_pid(pid), "the checkpoint finished before it could be observed in flight"
    :erlang.suspend_process(pid)

    {micros, ready} = :timer.tc(fn -> Store.ready(inst) end)
    assert ready == :ok
    assert micros < 1_000_000
    assert Task.yield(task, 0) == nil

    :erlang.resume_process(pid)
    assert Task.await(task) == :ok
    assert File.exists?(Path.join(dir, "CURRENT"))
  end

  # ── DUR-6 ─────────────────────────────────────────────────────────────────

  @key_a String.duplicate("a", 40)
  @key_b String.duplicate("b", 40)

  @doc false
  def report(_event, measurements, meta, pid), do: send(pid, {:verify, measurements, meta})

  defp backup_files(root, pattern),
    do: root |> Path.join(pattern) |> Path.wildcard() |> Enum.sort()

  defp flip_first_byte(file) do
    <<first, rest::binary>> = File.read!(file)
    File.write!(file, <<Bitwise.bxor(first, 1), rest::binary>>)
  end

  defp latest(root), do: File.read!(Path.join([root, "backup", "LATEST"]))

  defp manifest_file(root, id), do: Path.join([root, "backup", id, "manifest.json"])

  # A boot on an empty volume that is allowed to fail.
  defp boot_empty(config) do
    File.rm_rf!(config.data_dir)
    Process.flag(:trap_exit, true)
    Ankusa.Instance.start_link(config)
  end

  defp with_keys(config, keys), do: %{config | backup: %{config.backup | signing_key: keys}}

  defp wait_until(fun, deadline_ms \\ 5_000) do
    cond do
      fun.() -> true
      deadline_ms <= 0 -> false
      true -> Process.sleep(20) && wait_until(fun, deadline_ms - 20)
    end
  end

  test "verify detects corruption: a flipped byte, a short file, a missing file, a bad manifest; it never writes" do
    root = blob_root()
    config = start(root, blob_store: {CountingBlobStore, root: root, pid: self()})
    inst = config.instance

    assert Backup.verify(inst) == :none

    commit!(inst, 50)
    assert {:ok, %{id: id}} = Backup.run(inst)
    drain_puts()

    assert {:ok, %{id: ^id, files: files, bytes: bytes}} = Backup.verify(inst)
    assert files > 0 and bytes > 0
    assert drain_puts() == []

    [sst | _] = backup_files(root, "backup/sst/*.sst")
    name = Path.basename(sst)
    original = File.read!(sst)

    flip_first_byte(sst)
    assert {:error, {:checksum_mismatch, ^name}} = Backup.verify(inst)

    File.write!(sst, binary_part(original, 0, byte_size(original) - 1))
    assert {:error, {:size_mismatch, ^name}} = Backup.verify(inst)

    File.rm!(sst)
    assert {:error, {:download_failed, key, :not_found}} = Backup.verify(inst)
    assert String.ends_with?(key, "backup/sst/" <> name)

    File.write!(sst, original)
    assert {:ok, %{id: ^id}} = Backup.verify(inst)

    manifest = manifest_file(root, id)
    good = File.read!(manifest)
    File.write!(manifest, "{")
    assert {:error, {:manifest_invalid, ^id, {:json, _}}} = Backup.verify(inst)
    File.write!(manifest, good)

    assert Backup.verify(inst, target: :secondary) == {:error, :no_such_target}
    assert drain_puts() == []
  end

  test "the verify job reports every verified target, and a corrupted backup is an error result" do
    handler = "backup-verify-#{System.unique_integer([:positive])}"
    :telemetry.attach(handler, [:ankusa, :backup, :verify], &__MODULE__.report/4, self())
    on_exit(fn -> :telemetry.detach(handler) end)

    root = blob_root()
    config = start(root, backup: %{verify_interval_ms: 50})
    inst = config.instance
    commit!(inst, 50)
    assert {:ok, _} = Backup.run(inst)

    assert_receive {:verify, %{files: files}, %{instance: ^inst, target: :primary, result: :ok}},
                   2_000

    assert files > 0

    [sst | _] = backup_files(root, "backup/sst/*.sst")
    name = Path.basename(sst)
    flip_first_byte(sst)

    ExUnit.CaptureLog.capture_log(fn ->
      assert_receive {:verify, _,
                      %{target: :primary, result: :error, reason: {:checksum_mismatch, ^name}}},
                     2_000
    end)
  end

  test "manifest signature: unsigned, mis-signed and tampered manifests are refused at restore; rotation reads old backups" do
    root = blob_root()
    config = start(root, backup: %{signing_key: [@key_a]})
    inst = config.instance
    committed = commit!(inst, 30)
    assert {:ok, _} = Backup.run(inst)
    stop_supervised!({Ankusa.Instance, inst})

    # The stop took one more backup: that is the one `LATEST` names.
    id = latest(root)
    file = manifest_file(root, id)
    original = File.read!(file)
    manifest = JSON.decode!(original)
    assert manifest["version"] == 2
    assert String.match?(manifest["signature"], ~r/\A[0-9a-f]{64}\z/)

    assert {:store_restore_failed, _, {:manifest_invalid, ^id, :bad_signature}} =
             restore_reason(boot_empty(with_keys(config, [@key_b])))

    tampered = Map.update!(manifest, "created_at_ms", &(&1 + 1))
    File.write!(file, JSON.encode!(tampered))

    assert {:store_restore_failed, _, {:manifest_invalid, ^id, :bad_signature}} =
             restore_reason(boot_empty(config))

    [entry | rest] = manifest["files"]
    resized = %{manifest | "files" => [%{entry | "size" => entry["size"] + 1} | rest]}
    File.write!(file, JSON.encode!(resized))

    assert {:store_restore_failed, _, {:manifest_invalid, ^id, :bad_signature}} =
             restore_reason(boot_empty(config))

    File.write!(file, manifest |> Map.delete("signature") |> JSON.encode!())

    assert {:store_restore_failed, _, {:manifest_invalid, ^id, :unsigned}} =
             restore_reason(boot_empty(config))

    File.write!(file, manifest |> Map.put("signature", 1) |> JSON.encode!())

    assert {:store_restore_failed, _, {:manifest_invalid, ^id, :bad_signature}} =
             restore_reason(boot_empty(config))

    refute File.exists?(Path.join(Ankusa.Config.path(config, "store"), "CURRENT"))

    # Rotation: the old key is still listed, so the old backup restores.
    File.write!(file, original)
    File.rm_rf!(config.data_dir)
    start_supervised!({Ankusa.Instance, with_keys(config, [@key_b, @key_a])})
    assert stored_ids(inst) == Enum.map(committed, & &1.id)
    stop_supervised!({Ankusa.Instance, inst})

    # Signing is opt-in at restore: no key configured, nothing is checked, and
    # the operator is told.
    File.rm_rf!(config.data_dir)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        start_supervised!({Ankusa.Instance, with_keys(config, [])})
      end)

    assert log =~ "no backup.signing_key is configured"
    assert stored_ids(inst) == Enum.map(committed, & &1.id)
  end

  test "a backup of a newer store format than this release writes is refused at restore" do
    root = blob_root()
    config = start(root)
    commit!(config.instance, 10)
    assert {:ok, _} = Backup.run(config.instance)
    stop_supervised!({Ankusa.Instance, config.instance})

    id = latest(root)
    file = manifest_file(root, id)
    manifest = file |> File.read!() |> JSON.decode!()
    assert manifest["format_version"] == Ankusa.Store.Migrate.current_format()
    File.write!(file, manifest |> Map.put("format_version", 99) |> JSON.encode!())

    supported = Ankusa.Store.Migrate.current_format()

    assert {:store_restore_failed, _, {:store_format_newer, 99, ^supported}} =
             restore_reason(boot_empty(config))

    refute File.exists?(Path.join(Ankusa.Config.path(config, "store"), "CURRENT"))
  end

  test "the manifest records the intake offset, and a restore carries it" do
    root = blob_root()
    config = start(root)
    inst = config.instance
    committed = commit!(inst, 10)

    offset = Ankusa.Store.Keys.meta("intake_offset")
    assert :ok = Store.write(inst, [{:put, :default, offset, <<42::64>>}], sync: true)
    assert {:ok, %{id: id}} = Backup.run(inst)
    assert (root |> manifest_file(id) |> File.read!() |> JSON.decode!())["intake_offset"] == 42

    lose_host_and_restart(config)
    assert Store.get(inst, :default, offset) == {:ok, <<42::64>>}
    assert stored_ids(inst) == Enum.map(committed, & &1.id)
  end

  test "final checkpoint on stop: a clean stop leaves LATEST newer than the last periodic backup, holding every acked hook" do
    root = blob_root()
    config = start(root)
    inst = config.instance

    first = commit!(inst, 20)
    assert {:ok, %{id: periodic}} = Backup.run(inst)
    second = commit!(inst, 30)
    assert latest(root) == periodic

    stop_supervised!({Ankusa.Instance, inst})

    final = latest(root)
    assert final > periodic
    assert (root |> manifest_file(final) |> File.read!() |> JSON.decode!())["id"] == final

    File.rm_rf!(config.data_dir)
    start_supervised!({Ankusa.Instance, config})
    assert stored_ids(inst) == Enum.map(first ++ second, & &1.id)
  end

  test "a final checkpoint that outlasts backup.final_timeout_ms is abandoned and does not hold the stop" do
    root = blob_root()

    config =
      start(root,
        blob_store: {CountingBlobStore, root: root, put_delay_ms: 1_500},
        backup: %{final_timeout_ms: 200}
      )

    commit!(config.instance, 20)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        {micros, :ok} =
          :timer.tc(fn -> stop_supervised!({Ankusa.Instance, config.instance}) end)

        assert micros < 5_000_000
      end)

    assert log =~ "did not finish within 200 ms"
    refute File.exists?(Path.join([root, "backup", "LATEST"]))
  end

  test "secondary restore: the primary bucket is lost and every acked id comes back from the secondary" do
    primary = blob_root()
    secondary = blob_root()
    config = start(primary, backup: %{secondary: {LocalFS, root: secondary}})
    inst = config.instance

    committed = commit!(inst, 100)
    assert {:ok, %{id: id}} = Backup.run(inst)
    assert latest(primary) == id
    assert latest(secondary) == id
    assert {:ok, %{id: ^id}} = Backup.verify(inst, target: :secondary)

    stop_supervised!({Ankusa.Instance, inst})
    File.rm_rf!(primary)
    File.rm_rf!(config.data_dir)

    log = ExUnit.CaptureLog.capture_log(fn -> start_supervised!({Ankusa.Instance, config}) end)
    assert log =~ "SECONDARY"
    assert stored_ids(inst) == Enum.map(committed, & &1.id)
  end

  test "a primary that fails its checks falls back to the secondary; both failing refuses with the primary's reason" do
    primary = blob_root()
    secondary = blob_root()
    config = start(primary, backup: %{secondary: {LocalFS, root: secondary}})
    inst = config.instance
    committed = commit!(inst, 60)
    assert {:ok, _} = Backup.run(inst)
    stop_supervised!({Ankusa.Instance, inst})

    [sst | _] = backup_files(primary, "backup/sst/*.sst")
    name = Path.basename(sst)
    flip_first_byte(sst)
    File.rm_rf!(config.data_dir)

    ExUnit.CaptureLog.capture_log(fn -> start_supervised!({Ankusa.Instance, config}) end)
    assert stored_ids(inst) == Enum.map(committed, & &1.id)
    stop_supervised!({Ankusa.Instance, inst})

    flip_first_byte(Path.join([secondary, "backup", "sst", name]))

    assert {:store_restore_failed, _, {:checksum_mismatch, ^name}} =
             ExUnit.CaptureLog.with_log(fn -> restore_reason(boot_empty(config)) end) |> elem(0)
  end

  test "a failing secondary fails the run, keeps the primary's backup, and does not re-upload the primary" do
    primary = blob_root()
    secondary = blob_root()
    {:ok, failures} = Agent.start_link(fn -> 1_000 end)

    config =
      start(primary,
        blob_store: {CountingBlobStore, root: primary, pid: self()},
        backup: %{secondary: {CountingBlobStore, root: secondary, failures: failures}}
      )

    inst = config.instance
    commit!(inst, 20)

    ExUnit.CaptureLog.capture_log(fn ->
      assert {:error, {:secondary, :injected_failure}} = Backup.run(inst)
    end)

    first = drain_puts()
    assert Enum.any?(first, &String.starts_with?(&1, "backup/sst/"))
    assert File.exists?(Path.join([primary, "backup", "LATEST"]))
    refute File.exists?(Path.join([secondary, "backup", "LATEST"]))

    Agent.update(failures, fn _ -> 0 end)
    assert {:ok, %{id: id}} = Backup.run(inst)
    refute Enum.any?(drain_puts(), &String.starts_with?(&1, "backup/sst/"))
    assert latest(primary) == id
    assert latest(secondary) == id
  end

  test "readiness: backup is fresh within max_age_ms, stale past it (503 only with require_fresh), unlabelled without one" do
    plain = start(blob_root())
    assert {:ok, body} = Ankusa.Health.ready(plain.instance)
    refute Map.has_key?(body, :backup)

    fresh = start(blob_root(), backup: %{max_age_ms: 3_600_000})
    assert {:ok, %{backup: "fresh", status: "ready"}} = Ankusa.Health.ready(fresh.instance)

    # Puts that always fail: no backup ever succeeds, so the age only grows.
    {:ok, failures} = Agent.start_link(fn -> 1_000_000 end)
    lax_root = blob_root()
    strict_root = blob_root()

    ExUnit.CaptureLog.capture_log(fn ->
      lax =
        start(lax_root,
          blob_store: {CountingBlobStore, root: lax_root, failures: failures},
          backup: %{interval_ms: 50, max_age_ms: 100}
        )

      assert wait_until(fn ->
               match?(
                 {:ok, %{backup: "stale", status: "ready"}},
                 Ankusa.Health.ready(lax.instance)
               )
             end)

      strict =
        start(strict_root,
          blob_store: {CountingBlobStore, root: strict_root, failures: failures},
          backup: %{interval_ms: 50, max_age_ms: 100, require_fresh: true}
        )

      assert wait_until(fn ->
               match?(
                 {:error, %{backup: "stale", status: "unavailable", store: "ok"}},
                 Ankusa.Health.ready(strict.instance)
               )
             end)

      # One good backup and it is fresh again.
      Agent.update(failures, fn _ -> 0 end)
      assert {:ok, _} = Backup.run(strict.instance)
      assert {:ok, %{backup: "fresh"}} = Ankusa.Health.ready(strict.instance)
    end)
  end

  defp in_flight_checkpoint(store, task) do
    case :sys.get_state(store) do
      %{checkpoint: %{pid: pid}} ->
        pid

      _ ->
        if Process.alive?(task.pid), do: in_flight_checkpoint(store, task), else: nil
    end
  end
end
