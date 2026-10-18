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

  alias Ankusa.{Config, Envelope, Queue, SourceStore, Storage, Store}
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
    test "an unversioned store is stamped format 1 on open" do
      inst = open_store()
      assert Store.get(inst, :default, Keys.format_version()) == {:ok, "1"}

      # A store written before the marker existed has no key.
      :ok = Store.write(inst, [{:delete, :default, Keys.format_version()}], sync: true)
      assert Store.get(inst, :default, Keys.format_version()) == :not_found
      stop_supervised!({Store, inst})

      start_supervised!({Store, instance: inst})
      assert Store.get(inst, :default, Keys.format_version()) == {:ok, "1"}
    end

    test "refuses newer format, naming both versions" do
      inst = open_store()
      config = Ankusa.config(inst)
      :ok = Store.write(inst, [{:put, :default, Keys.format_version(), "2"}], sync: true)
      stop_supervised!({Store, inst})

      assert {:error, error} = start_supervised({Store, instance: inst})
      assert {:store_format_newer, 2, 1} = find(error, &match?({:store_format_newer, _, _}, &1))

      # The same refusal from the tasks an operator runs.
      args = ["--data-dir", config.data_dir, "--instance", Atom.to_string(inst)]

      assert_raise Mix.Error, ~r/format 2 is newer than 1/, fn ->
        MigrateTask.run(["--dry-run" | args])
      end

      assert_raise Mix.Error, ~r/was not migrated.*store_format_newer/s, fn ->
        MigrateTask.run(args)
      end

      # A release whose registry reaches format 2 opens it as it is.
      start_supervised!({Store, instance: inst, migrations: [{1, 2, Rewrite}]})
      assert Store.get(inst, :default, Keys.format_version()) == {:ok, "2"}
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
               {:ok, %{found: 1, current: 1, pending: [], resume: nil}}

      args = ["--data-dir", config.data_dir, "--instance", Atom.to_string(inst)]
      MigrateTask.run(["--dry-run" | args])
      assert_received {:mix_shell, :info, ["store: " <> _]}
      assert_received {:mix_shell, :info, ["format: 1 (this release: 1)"]}
      assert_received {:mix_shell, :info, ["nothing to do"]}

      MigrateTask.run(args)
      assert_received {:mix_shell, :info, [line]}
      assert line =~ "is at format 1"

      empty = Path.join(config.data_dir, "nowhere")
      assert Migrate.plan(empty) == {:error, :no_store}
      MigrateTask.run(["--dry-run", "--data-dir", empty, "--instance", "x"])
      assert_received {:mix_shell, :info, ["no store there yet: nothing to migrate"]}
    end
  end

  defp open_store do
    config = test_config(roles: [:edge])
    put_config(config)
    start_supervised!({Store, instance: config.instance})
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

    for [tenant, name] <- manifest["sources"] do
      assert {:ok, _source} = SourceStore.get(inst, tenant, name)
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
