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

  alias Ankusa.{Envelope, Storage, Store}
  alias Ankusa.BlobStore.LocalFS
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
    config = start(root, roles: [:edge, :storage])
    inst = config.instance

    before = commit!(inst, 5)
    assert {:ok, _} = Backup.run(inst)
    later = commit!(inst, 5)
    assert {:ok, 1} = Compactor.tick(inst)

    lose_host_and_restart(config)

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

  defp in_flight_checkpoint(store, task) do
    case :sys.get_state(store) do
      %{checkpoint: %{pid: pid}} ->
        pid

      _ ->
        if Process.alive?(task.pid), do: in_flight_checkpoint(store, task), else: nil
    end
  end
end
