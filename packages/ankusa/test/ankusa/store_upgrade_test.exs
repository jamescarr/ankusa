defmodule Ankusa.StoreUpgradeTest do
  @moduledoc """
  The store's format version: an unversioned store is stamped, a store newer
  than this release refuses to open, the registry's migrations run in bounded
  synced batches and resume where a kill left them, and current code boots
  every checked-in fixture (`test/fixtures/store-v<n>.tar.zst`, written by
  `mix ankusa.store.fixture` at the release that introduced format `n`).
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.{Audit, Config, Envelope, Queue, SourceStore, Storage, Store}
  alias Ankusa.Edge.{Ingest, Quarantine}
  alias Ankusa.Store.{Keys, Migrate}
  alias Mix.Tasks.Ankusa.Store.Fixture
  alias Mix.Tasks.Ankusa.Store.Migrate, as: MigrateTask

  @moduletag capture_log: true

  @fixtures Path.expand("../fixtures", __DIR__)

  # Rewrites every hook value to `value <> "!"`, 100 rows a batch, so a batch
  # applied twice shows as `"!!"`. `crash_after` raises on that step call.
  defmodule Rewrite do
    @moduledoc false
    @behaviour Ankusa.Store.Migration

    alias Ankusa.Store
    alias Ankusa.Store.Keys

    @batch 100

    @impl true
    def describe, do: "append ! to every hook"

    @impl true
    def step(instance, cursor) do
      calls = :persistent_term.get({__MODULE__, :calls}, 0)

      if :persistent_term.get({__MODULE__, :crash_after}, :never) == calls do
        raise "killed mid-migration"
      end

      :persistent_term.put({__MODULE__, :calls}, calls + 1)

      after_seq =
        case cursor do
          nil -> 0
          <<seq::64>> -> seq
        end

      {:ok, {count, rows}} =
        Store.fold(
          instance,
          :hooks,
          {Keys.hook(after_seq + 1), Keys.family(:hooks).hi},
          {0, []},
          fn key, value, {n, acc} ->
            acc = [{key, value} | acc]
            if n + 1 >= @batch, do: {:halt, {n + 1, acc}}, else: {:cont, {n + 1, acc}}
          end
        )

      rows = Enum.reverse(rows)
      ops = for {key, value} <- rows, do: {:put, :hooks, key, value <> "!"}

      if count < @batch do
        {:ok, ops, :done}
      else
        {last, _value} = List.last(rows)
        {:ok, ops, {:more, last}}
      end
    end
  end

  setup do
    Mix.shell(Mix.Shell.Process)

    on_exit(fn ->
      Mix.shell(Mix.Shell.IO)
      :persistent_term.erase({Rewrite, :calls})
      :persistent_term.erase({Rewrite, :crash_after})
    end)
  end

  describe "a store written by an earlier release" do
    test "there is a fixture to boot" do
      assert Path.wildcard(Path.join(@fixtures, "store-v*.tar.zst")) != []
    end

    for path <- Path.wildcard(Path.join(@fixtures, "store-v*.tar.zst")) do
      test "boots #{Path.basename(path)} with everything it held" do
        {config, manifest} = unpack_fixture(unquote(path))
        [_, version] = Regex.run(~r/store-v(\d+)\./, unquote(path))
        assert manifest["format_version"] == String.to_integer(version)

        start_supervised!({Ankusa.Instance, config})
        assert_fixture(config.instance, manifest)
      end
    end

    test "the fixture task writes one that boots" do
      path = Path.join(System.tmp_dir!(), "ankusa_fixture_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf(path) end)

      written = Fixture.build(path <> ".tar.zst")
      on_exit(fn -> File.rm(path <> ".tar.zst") end)
      assert written["format_version"] == Migrate.current_format()
      assert written["segments"] >= 1

      {config, manifest} = unpack_fixture(path <> ".tar.zst")
      assert manifest == written

      start_supervised!({Ankusa.Instance, config})
      assert_fixture(config.instance, manifest)
    end
  end

  describe "the format version" do
    test "an unversioned store is stamped format 1 on open, then migrated up" do
      inst = open_store()
      assert Store.get(inst, :default, Keys.format_version()) == {:ok, "1"}

      # A store written before the marker existed has no key.
      :ok = Store.write(inst, [{:delete, :default, Keys.format_version()}], sync: true)
      assert Store.get(inst, :default, Keys.format_version()) == :not_found
      stop_supervised!({Store, inst})

      start_supervised!({Store, instance: inst, migrations: []})
      assert Store.get(inst, :default, Keys.format_version()) == {:ok, "1"}
      stop_supervised!({Store, inst})

      # This release's registry takes it to format 3.
      start_supervised!({Store, instance: inst})
      assert Store.get(inst, :default, Keys.format_version()) == {:ok, "3"}
    end

    test "format 1 sources become revision 1, and a replayed step keeps a revision it set" do
      inst = open_store()
      sources = for name <- ["a", "b", "c"], do: Keys.source("acme", name)
      ops = for key <- sources, do: {:put, :default, key, ~s({"sinks":[]})}

      :ok =
        Store.write(inst, ops ++ [{:put, :default, Keys.source_revision("acme", "b"), "7"}],
          sync: true
        )

      stop_supervised!({Store, inst})

      start_supervised!({Store, instance: inst})
      assert Store.get(inst, :default, Keys.source_revision("acme", "a")) == {:ok, "1"}
      assert Store.get(inst, :default, Keys.source_revision("acme", "b")) == {:ok, "7"}
      assert Store.get(inst, :default, Keys.source_revision("acme", "c")) == {:ok, "1"}
    end

    test "refuses newer format, naming both versions" do
      inst = open_store()
      config = Ankusa.config(inst)
      :ok = Store.write(inst, [{:put, :default, Keys.format_version(), "4"}], sync: true)
      stop_supervised!({Store, inst})

      assert {:error, error} = start_supervised({Store, instance: inst})
      assert {:store_format_newer, 4, 3} = find(error, &match?({:store_format_newer, _, _}, &1))

      # The same refusal from the tasks an operator runs.
      args = ["--data-dir", config.data_dir, "--instance", Atom.to_string(inst)]

      assert_raise Mix.Error, ~r/format 4 is newer than 3/, fn ->
        MigrateTask.run(["--dry-run" | args])
      end

      assert_raise Mix.Error, ~r/was not migrated.*store_format_newer/s, fn ->
        MigrateTask.run(args)
      end

      # A release whose registry reaches format 4 opens it as it is.
      migrations = [{1, 2, Rewrite}, {2, 3, Rewrite}, {3, 4, Rewrite}]
      start_supervised!({Store, instance: inst, migrations: migrations})
      assert Store.get(inst, :default, Keys.format_version()) == {:ok, "4"}
    end

    test "a format version that is not a number refuses to open" do
      inst = open_store()
      :ok = Store.write(inst, [{:put, :default, Keys.format_version(), "two"}], sync: true)
      stop_supervised!({Store, inst})

      assert {:error, error} = start_supervised({Store, instance: inst})
      assert {:bad_format_version, "two"} = find(error, &match?({:bad_format_version, _}, &1))
    end

    test "resumes interrupted migration at the cursor of the last committed batch" do
      inst = open_store()
      config = Ankusa.config(inst)
      migrations = [{1, 2, Rewrite}]

      ops = for seq <- 1..1_000, do: {:put, :hooks, Keys.hook(seq), "hook-#{seq}"}
      :ok = Store.write(inst, ops, sync: true)
      stop_supervised!({Store, inst})

      # Four batches commit, the fifth step is killed.
      :persistent_term.put({Rewrite, :crash_after}, 4)
      assert {:error, error} = start_supervised({Store, instance: inst, migrations: migrations})

      assert {:store_migration_failed, 2, _} =
               find(error, &match?({:store_migration_failed, _, _}, &1))

      # At rest the store says where it stopped.
      assert {:ok, plan} = Migrate.plan(Config.path(config, "store"), migrations)

      assert plan == %{
               found: 1,
               current: 2,
               pending: migrations,
               resume: %{to: 2, cursor: <<400::64>>}
             }

      # The next boot picks up at hook 401: six full batches and the empty one.
      :persistent_term.erase({Rewrite, :crash_after})
      :persistent_term.put({Rewrite, :calls}, 0)
      start_supervised!({Store, instance: inst, migrations: migrations})

      assert :persistent_term.get({Rewrite, :calls}) == 7
      assert Store.get(inst, :default, Keys.format_version()) == {:ok, "2"}
      assert Store.get(inst, :default, Keys.migrating(2)) == :not_found

      {lo, hi} = {Keys.hook(1), Keys.family(:hooks).hi}

      assert {:ok, values} =
               Store.fold(inst, :hooks, {lo, hi}, [], fn _k, v, acc -> {:cont, [v | acc]} end)

      assert length(values) == 1_000
      assert Enum.all?(values, &(String.ends_with?(&1, "!") and not String.ends_with?(&1, "!!")))
    end

    test "a registry with a gap refuses rather than skip a format" do
      inst = open_store()

      assert {:error, {:missing_migration, 1, 3}} =
               Migrate.upgrade_format(inst, [{2, 3, Rewrite}])

      assert Store.get(inst, :default, Keys.format_version()) == {:ok, "1"}
    end

    test "a dry run reports a store at rest, and migrate opens it" do
      inst = open_store()
      config = Ankusa.config(inst)
      stop_supervised!({Store, inst})

      assert Migrate.plan(Config.path(config, "store")) ==
               {:ok,
                %{
                  found: 1,
                  current: 3,
                  pending: [
                    {1, 2, Ankusa.Store.Migrations.AuditTrail},
                    {2, 3, Ankusa.Store.Migrations.TenantKeys}
                  ],
                  resume: nil
                }}

      args = ["--data-dir", config.data_dir, "--instance", Atom.to_string(inst)]
      MigrateTask.run(["--dry-run" | args])
      assert_received {:mix_shell, :info, ["store: " <> _]}
      assert_received {:mix_shell, :info, ["format: 1 (this release: 3)"]}
      assert_received {:mix_shell, :info, ["  1 -> 2: " <> _]}
      assert_received {:mix_shell, :info, ["  2 -> 3: re-key due/inflight/dead by tenant ordinal" <> _]}

      MigrateTask.run(args)
      assert_received {:mix_shell, :info, [line]}
      assert line =~ "is at format 3"

      empty = Path.join(config.data_dir, "nowhere")
      assert Migrate.plan(empty) == {:error, :no_store}
      MigrateTask.run(["--dry-run", "--data-dir", empty, "--instance", "x"])
      assert_received {:mix_shell, :info, ["no store there yet: nothing to migrate"]}
    end
  end

  alias Ankusa.Queue.{Deliveries, Tenants}
  alias Ankusa.Store.Migrations.AuditTrail

  # Fails on its `crash_after`-th step call, otherwise is `TenantKeys`.
  defmodule CrashingTenantKeys do
    @moduledoc false
    @behaviour Ankusa.Store.Migration

    @impl true
    def describe, do: "TenantKeys, killable"

    @impl true
    def step(instance, cursor) do
      calls = :persistent_term.get({__MODULE__, :calls}, 0)

      if :persistent_term.get({__MODULE__, :crash_after}, :never) == calls,
        do: raise("killed mid-migration")

      :persistent_term.put({__MODULE__, :calls}, calls + 1)
      Ankusa.Store.Migrations.TenantKeys.step(instance, cursor)
    end
  end

  # Store format 2 -> 3: tenant ordinals, re-keyed due/inflight rows, the
  # by-tenant dead index and the counters seeded from what the rows say.
  describe "the tenant keys migration (format 2 -> 3)" do
    setup do
      on_exit(fn ->
        :persistent_term.erase({CrashingTenantKeys, :calls})
        :persistent_term.erase({CrashingTenantKeys, :crash_after})
      end)
    end

    test "v2 fixture upgrades to v3 with counters equal to a fold" do
      {config, manifest} = unpack_fixture(Path.join(@fixtures, "store-v2.tar.zst"))
      inst = config.instance
      assert manifest["format_version"] == 2

      start_supervised!({Store, instance: inst})
      assert Store.get(inst, :default, Keys.format_version()) == {:ok, "3"}

      # The fixture holds the default tenant only: its retry rows and its dead
      # rows, whichever side of the migration's clock the retries fell on.
      assert {:ok, 1} = Tenants.ord(inst, "default")
      assert {:ok, 1} = Tenants.count(inst)

      assert {:ok, %{pending: pending, scheduled: scheduled}} =
               Queue.pending_for_tenant(inst, "default")

      assert pending + scheduled == length(manifest["pending_ids"])
      assert {:ok, %{dead: dead}} = Queue.dead_for_tenant(inst, "default")
      assert dead == length(manifest["dead_ids"])

      assert stored_counters(inst) == folded_counters(inst)
      assert Map.keys(stored_counters(inst)) == [0, 1]
    end

    test "tenant migration allocates each ordinal once" do
      inst = seeded_v2_store()
      start_supervised!({Store, instance: inst})

      assert {:ok, 4} = Tenants.count(inst)
      assert {:ok, 5} = Tenants.next_ord(inst)
      names = for ord <- 1..4, do: elem(Tenants.name(inst, ord), 1)
      assert Enum.sort(names) == ["acme", "deadonly", "default", "globex"]

      for {name, ord} <- Enum.with_index(names, 1), do: assert({:ok, ^ord} = Tenants.ord(inst, name))
    end

    test "a hook with no tenant or no envelope files under default" do
      inst = seeded_v2_store()
      start_supervised!({Store, instance: inst})

      # Seq 6 has no tenant, 7 holds bytes that are no envelope, 8 has no hook.
      assert {:ok, default} = Tenants.ord(inst, "default")

      assert for({{ord, _at, seq, _sink}, _size, _kind} <- fold_rows(inst, :due), seq in [6, 7, 8], do: ord) ==
               [default, default, default]

      assert {:ok, %{pending: 3}} = Queue.pending_for_tenant(inst, "default")
      assert stored_counters(inst) == folded_counters(inst)
    end

    test "resumes interrupted tenant migration without double counting" do
      migrations = [{1, 2, AuditTrail}, {2, 3, CrashingTenantKeys}]

      # An uninterrupted run is the reference, and counts its own steps.
      reference = seeded_v2_store(1_200)
      start_supervised!({Store, instance: reference, migrations: migrations}, id: :reference)
      steps = :persistent_term.get({CrashingTenantKeys, :calls})
      assert steps >= 7
      expected = snapshot(reference)
      assert stored_counters(reference) == folded_counters(reference)
      stop_supervised!(:reference)

      # Killed before step k, for every k: the next boot finishes the job and
      # lands on the same rows and the same counts.
      for k <- 0..(steps - 1) do
        inst = seeded_v2_store(1_200)
        :persistent_term.put({CrashingTenantKeys, :calls}, 0)
        :persistent_term.put({CrashingTenantKeys, :crash_after}, k)

        assert {:error, error} = start_supervised({Store, instance: inst, migrations: migrations})

        assert {:store_migration_failed, 3, _} =
                 find(error, &match?({:store_migration_failed, _, _}, &1))

        :persistent_term.erase({CrashingTenantKeys, :crash_after})
        start_supervised!({Store, instance: inst, migrations: migrations}, id: {:resumed, k})

        assert Store.get(inst, :default, Keys.format_version()) == {:ok, "3"}
        assert snapshot(inst) == expected, "killed before step #{k}"
        stop_supervised!({:resumed, k})
      end
    end

    test "a format 2 release refuses a format 3 store" do
      inst = seeded_v2_store()
      start_supervised!({Store, instance: inst}, id: :v3)
      stop_supervised!(:v3)

      # What a release that writes format 2 has for a registry.
      assert {:error, error} =
               start_supervised({Store, instance: inst, migrations: [{1, 2, AuditTrail}]})

      assert {:store_format_newer, 3, 2} = find(error, &match?({:store_format_newer, _, _}, &1))
    end

    test "a restored format 2 backup migrates on boot" do
      root = unique_data_dir(:bucket)
      on_exit(fn -> File.rm_rf(root) end)

      config =
        test_config(
          roles: [:edge],
          source_store: {Ankusa.SourceStore.Static, sources: %{}},
          storage: %{interval_ms: 0, blob_store: {Ankusa.BlobStore.LocalFS, root: root}},
          backup: %{enabled: true, interval_ms: 3_600_000}
        )

      put_config(config)
      inst = config.instance

      # A release that writes format 2 takes the backup.
      start_supervised!({Store, instance: inst, migrations: [{1, 2, AuditTrail}]}, id: :v2)
      seed_v2!(inst, 0)
      assert Store.get(inst, :default, Keys.format_version()) == {:ok, "2"}
      assert {:ok, %{uploaded: uploaded}} = Ankusa.Store.Backup.run(inst)
      assert uploaded > 0

      # The host is gone; the next boot restores the backup and migrates it.
      stop_supervised!(:v2)
      File.rm_rf!(config.data_dir)
      start_supervised!({Ankusa.Instance, config})

      assert Store.get(inst, :default, Keys.format_version()) == {:ok, "3"}
      assert {:ok, 4} = Tenants.count(inst)
      assert stored_counters(inst) == folded_counters(inst)
      assert {:ok, %{dead: 2}} = Queue.dead_for_tenant(inst, "deadonly")
    end

    test "the v3 fixture task writes a store that boots" do
      path =
        Path.join(System.tmp_dir!(), "ankusa_fixture_v3_#{System.unique_integer([:positive])}.tar.zst")

      on_exit(fn -> File.rm(path) end)

      written = Fixture.build(path)
      assert written["format_version"] == 3
      assert written["tenants"] == ["default", "acme", "globex", "deadonly"]

      # Boot it with the store alone, so nothing moves under the count.
      {config, manifest} = unpack_fixture(path)
      inst = config.instance
      start_supervised!({Store, instance: inst})

      for tenant <- manifest["tenants"], do: assert({:ok, _} = Tenants.ord(inst, tenant))
      assert {:ok, %{inflight: inflight}} = Queue.pending_for_tenant(inst, "acme")
      assert inflight >= 1
      assert {:ok, %{dead: 2}} = Queue.dead_for_tenant(inst, "deadonly")

      assert {:ok, %{pending: 0, scheduled: 0, inflight: 0}} =
               Queue.pending_for_tenant(inst, "deadonly")

      assert stored_counters(inst) == folded_counters(inst)

      # The checked-in v3 fixture is what this task wrote at the release that
      # introduced format 3.
      assert File.exists?(Path.join(@fixtures, "store-v3.tar.zst"))
    end
  end

  # ── a store of format 2, written by hand ───────────────────────────────

  defp seeded_v2_store(extra \\ 0) do
    inst = open_store()
    seed_v2!(inst, extra)
    stop_supervised!({Store, inst})
    inst
  end

  defp hook(tenant, n) do
    Envelope.to_binary(%Envelope{
      id: "evt-#{tenant}-#{n}",
      source_id: "load",
      tenant_id: tenant,
      received_at: 1,
      method: "POST",
      path: "/",
      headers: [],
      body: "b",
      size: 1
    })
  end

  defp delivery_row do
    Deliveries.encode_row(%{
      module: Ankusa.Sink.Log,
      state: :pending,
      attempts: 0,
      at: 1,
      error: nil,
      size: 100,
      source_id: "load"
    })
  end

  # Hooks, delivery rows and the format 2 index of: acme seqs 1-3 (1 and 2 due,
  # 3 a retry) and 11 (an archive obligation), globex 4 (a retry, and a dead
  # second sink) and 5 (claimed), no tenant 6, garbage 7, no hook 8, `deadonly`
  # 9 and 10 (dead) and no tenant 12 (an archive obligation). `extra` more due
  # rows of seq 1, to take the migration through several steps.
  defp seed_v2!(inst, extra) do
    past = 1_000
    future = System.system_time(:millisecond) + 86_400_000

    hooks = [
      {1, hook("acme", 1)},
      {2, hook("acme", 2)},
      {3, hook("acme", 3)},
      {4, hook("globex", 4)},
      {5, hook("globex", 5)},
      {6, hook(nil, 6)},
      {7, "not an envelope"},
      {9, hook("deadonly", 9)},
      {10, hook("deadonly", 10)},
      {11, hook("acme", 11)},
      {12, hook(nil, 12)}
    ]

    due =
      for {seq, at} <- [{1, past}, {2, past}, {3, future}, {4, future}, {6, past}, {7, past}, {8, past}],
          do: {:put, :index, Keys.due_v2(at, seq, 0), <<100::32>>}

    more = for sink <- 1..extra//1, do: {:put, :index, Keys.due_v2(past, 1, sink), <<100::32>>}
    dead = fn at, seq, sink, owner ->
      {:put, :index, Keys.dead(at, seq, sink), :erlang.term_to_binary(owner)}
    end

    :ok =
      Store.write(
        inst,
        for({seq, bin} <- hooks, do: {:put, :hooks, Keys.hook(seq), bin}) ++
          for(seq <- 1..10, do: {:put, :deliveries, Keys.delivery(seq, 0), delivery_row()}) ++
          [{:put, :deliveries, Keys.delivery(4, 1), delivery_row()}] ++
          due ++
          more ++
          [
            {:put, :index, Keys.inflight_v2(5, 0), <<100::32>>},
            dead.(500, 4, 1, {"load", "evt-globex-4b"}),
            dead.(501, 9, 0, {"load", "evt-deadonly-9"}),
            dead.(502, 10, 0, {"load", "evt-deadonly-10"}),
            {:put, :index, Keys.archive_pending(11), <<100::32>>},
            {:put, :index, Keys.archive_pending(12), <<100::32>>},
            {:put, :default, Keys.format_version(), "2"}
          ],
        sync: true
      )
  end

  # Every row the migration writes, and the counters.
  defp snapshot(inst) do
    families = [:due, :inflight, :dead, :dead_by_tenant, :active, :archive_pending, :tenants, :tenant_ords]
    {Map.new(families, fn family -> {family, fold_rows(inst, family)} end), stored_counters(inst)}
  end

  defp fold_rows(inst, :due) do
    for {key, <<size::32, kind::8>>} <- fold_raw(inst, :due),
        do: {Keys.decode_due(key), size, kind}
  end

  defp fold_rows(inst, family), do: fold_raw(inst, family)

  defp fold_raw(inst, family) do
    %{lo: lo, hi: hi} = Keys.family(family)
    {:ok, rows} = Store.fold(inst, family, {lo, hi}, [], fn k, v, acc -> {:cont, [{k, v} | acc]} end)
    Enum.reverse(rows)
  end

  defp open_store do
    config = test_config(roles: [:edge])
    put_config(config)
    start_supervised!({Store, instance: config.instance, migrations: []})
    config.instance
  end

  defp unpack_fixture(path) do
    config = test_config(Fixture.config_opts())
    root = Config.path(config, "")
    File.mkdir_p!(root)

    tar = path |> File.read!() |> :zstd.decompress() |> IO.iodata_to_binary()
    :ok = :erl_tar.extract({:binary, tar}, cwd: String.to_charlist(root))

    manifest = root |> Path.join("manifest.json") |> File.read!() |> JSON.decode!()
    put_config(config)
    {config, manifest}
  end

  defp assert_fixture(inst, manifest) do
    current = Integer.to_string(Migrate.current_format())
    assert Store.get(inst, :default, Keys.format_version()) == {:ok, current}

    # Hooks still held: the pending ones and whatever the dead rows keep.
    assert stored_ids(inst) == manifest["hook_ids"]

    assert {:ok, %{total: total, entries: dead}} = Queue.dead(inst, limit: 100)
    assert total == length(manifest["dead_ids"])
    assert dead |> Enum.map(& &1.envelope.id) |> Enum.sort() == Enum.sort(manifest["dead_ids"])

    assert {:ok, held} = Quarantine.recent(inst, 10)
    assert held |> Enum.map(& &1.id) |> Enum.sort() == Enum.sort(manifest["quarantine_ids"])

    # Every source has a revision: a format 1 store's were given revision 1 by
    # the migration, a format 2 store's are as the audited writes left them.
    for [tenant, name] <- manifest["sources"] do
      assert {:ok, %{revision: revision}} = SourceStore.get(inst, tenant, name)
      assert revision == Map.get(manifest["revisions"] || %{}, "#{tenant}.#{name}", 1)
    end

    # The audit trail the fixture's release wrote is read back whole, in order.
    if manifest["audit"] do
      assert {:ok, %{entries: entries}} = Audit.list(inst, limit: 1000)
      assert entries |> Enum.reverse() |> Enum.map(&[&1.action, &1.resource]) == manifest["audit"]
    end

    # The archive: every hook the fixture archived is read back by id, whether
    # or not its row is still in the store.
    for id <- manifest["archived_ids"] do
      assert {:ok, %Envelope{id: ^id}} = Storage.fetch(inst, id)
    end

    # The ingest dedupe window survived: a provider retry is a duplicate of
    # the hook the old release committed.
    for [source, key, id] <- manifest["dedupe"] do
      assert {:duplicate, %Envelope{id: ^id}} =
               Ingest.ingest(inst, request(source, "again", [{"x-event-id", key}]))
    end
  end

  defp find(term, fun) do
    cond do
      fun.(term) -> term
      is_tuple(term) -> term |> Tuple.to_list() |> find_in(fun)
      is_list(term) -> find_in(term, fun)
      true -> nil
    end
  end

  defp find_in(list, fun), do: Enum.find_value(list, &find(&1, fun))
end
