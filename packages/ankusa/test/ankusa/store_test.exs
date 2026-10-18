defmodule Ankusa.StoreTest do
  use ExUnit.Case, async: true

  import Ankusa.TestHelpers

  alias Ankusa.Config
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @moduletag capture_log: true

  setup do
    config = test_config(roles: [:edge])
    put_config(config)
    start_supervised!({Store, instance: config.instance})
    %{config: config, instance: config.instance}
  end

  describe "writes and point reads" do
    test "a batch applies across column families, deletes and range deletes", %{instance: inst} do
      :ok =
        Store.write(
          inst,
          [
            {:put, :hooks, Keys.hook(1), "one"},
            {:put, :hooks, Keys.hook(2), "two"},
            {:put, :hooks, Keys.hook(3), "three"},
            {:put, :deliveries, Keys.delivery(1, 0), "row"}
          ],
          sync: true
        )

      assert Store.get(inst, :hooks, Keys.hook(1)) == {:ok, "one"}
      assert Store.get(inst, :deliveries, Keys.delivery(1, 0)) == {:ok, "row"}
      assert Store.get(inst, :hooks, Keys.hook(9)) == :not_found

      assert {:ok, [{:ok, "one"}, :not_found, {:ok, "two"}]} =
               Store.multi_get(inst, :hooks, [Keys.hook(1), Keys.hook(9), Keys.hook(2)])

      :ok =
        Store.write(inst, [
          {:delete, :hooks, Keys.hook(1)},
          {:delete_range, :hooks, Keys.hook(2), Keys.hook(3)}
        ])

      assert Store.get(inst, :hooks, Keys.hook(1)) == :not_found
      assert Store.get(inst, :hooks, Keys.hook(2)) == :not_found
      assert Store.get(inst, :hooks, Keys.hook(3)) == {:ok, "three"}
    end

    test "a malformed key raises; it is never reported as an unavailable store", %{instance: inst} do
      assert_raise FunctionClauseError, fn ->
        Store.write(inst, [{:put, :hooks, ["iolist"], "v"}], [])
      end

      assert_raise FunctionClauseError, fn -> apply(Store, :get, [inst, :hooks, ["iolist"]]) end
      assert_raise ArgumentError, fn -> Store.multi_get(inst, :hooks, [["iolist"]]) end
    end

    test "every key builder yields a binary the store accepts", %{instance: inst} do
      keys = [
        {:default, Keys.source("acme", "billing")},
        {:default, Keys.rate_limit("acme")},
        {:archive, Keys.legacy_location("evt-1")},
        {:quarantine, Keys.quarantine_summary(1_700_000_000_000, "evt-1")},
        {:quarantine, Keys.quarantine_body(1_700_000_000_000, "evt-1")}
      ]

      for {cf, key} <- keys do
        assert is_binary(key)
        assert :ok = Store.write(inst, [{:put, cf, key, "v"}])
        assert Store.get(inst, cf, key) == {:ok, "v"}
      end
    end

    test "a store that is not running answers :store_unavailable" do
      assert Store.get(:no_such_instance, :hooks, Keys.hook(1)) == {:error, :store_unavailable}

      assert Store.write(:no_such_instance, [{:delete, :hooks, Keys.hook(1)}], []) ==
               {:error, :store_unavailable}
    end
  end

  describe "fold/6" do
    setup %{instance: inst} do
      ops = for seq <- 1..10, do: {:put, :hooks, Keys.hook(seq), "v#{seq}"}
      :ok = Store.write(inst, ops, [])
      %{hi: Keys.family(:hooks).hi}
    end

    defp seqs(inst, range, opts \\ []) do
      {:ok, acc} =
        Store.fold(
          inst,
          :hooks,
          range,
          [],
          fn <<seq::64>>, _v, acc -> {:cont, [seq | acc]} end,
          opts
        )

      acc
    end

    test "forward is half-open and ordered, and never yields a sentinel", %{
      instance: inst,
      hi: hi
    } do
      assert inst |> seqs({Keys.hook(3), Keys.hook(7)}) |> Enum.reverse() == [3, 4, 5, 6]
      assert inst |> seqs({Keys.hook(1), hi}) |> Enum.reverse() == Enum.to_list(1..10)
    end

    test "reverse walks newest first through the whole range", %{instance: inst, hi: hi} do
      assert inst |> seqs({Keys.hook(3), Keys.hook(8)}, reverse: true) |> Enum.reverse() ==
               [7, 6, 5, 4, 3]

      assert inst |> seqs({Keys.hook(1), hi}, reverse: true) |> Enum.reverse() ==
               Enum.to_list(10..1//-1)
    end

    test "halt stops early; empty ranges are empty, not errors", %{instance: inst, hi: hi} do
      assert {:ok, 2} =
               Store.fold(inst, :hooks, {Keys.hook(1), hi}, 0, fn _k, _v, n ->
                 if n + 1 == 2, do: {:halt, n + 1}, else: {:cont, n + 1}
               end)

      assert seqs(inst, {Keys.hook(100), hi}) == []
      assert seqs(inst, {Keys.hook(100), hi}, reverse: true) == []
      assert seqs(inst, {Keys.hook(5), Keys.hook(5)}) == []
    end

    test "a family only sees its own keys", %{instance: inst} do
      :ok =
        Store.write(inst, [
          {:put, :index, Keys.due(20, 2, 0), <<1::32>>},
          {:put, :index, Keys.due(10, 1, 0), <<1::32>>},
          {:put, :index, Keys.dead(5, 9, 0), "d"},
          {:put, :index, Keys.inflight(3, 0), <<>>}
        ])

      {lo, hi} = Keys.range(<<?d>>)

      assert {:ok, due} =
               Store.fold(inst, :due, {lo, hi}, [], fn k, _v, acc ->
                 {:cont, [Keys.decode_due(k) | acc]}
               end)

      assert Enum.reverse(due) == [{10, 1, 0}, {20, 2, 0}]
    end
  end

  describe "corruption is reported, never a shorter scan" do
    test "a corrupt blob fails the scan and the point read", %{config: config, instance: inst} do
      body = :binary.copy("x", 8192)
      ops = for seq <- 1..200, do: {:put, :hooks, Keys.hook(seq), body <> <<seq::32>>}
      :ok = Store.write(inst, ops, sync: true)
      flush!(inst, :hooks)
      stop_supervised!({Store, inst})

      flip_mid(largest(config, ".blob"))
      start_supervised!({Store, instance: inst})

      hi = Keys.family(:hooks).hi

      assert {:error, {:corruption, _}} =
               Store.fold(inst, :hooks, {Keys.hook(1), hi}, 0, fn _k, _v, n -> {:cont, n + 1} end)

      assert Enum.any?(1..200, fn seq ->
               match?({:error, {:corruption, _}}, Store.get(inst, :hooks, Keys.hook(seq)))
             end)
    end

    test "a corrupt sst data block fails the scan", %{config: config, instance: inst} do
      ops = for i <- 1..4000, do: {:put, :index, Keys.due(i, i, 0), <<512::32>>}
      :ok = Store.write(inst, ops, sync: true)
      flush!(inst, :index)
      stop_supervised!({Store, inst})

      flip_mid(largest(config, ".sst"))
      start_supervised!({Store, instance: inst})

      {lo, hi} = Keys.range(<<?d>>)

      assert {:error, {:corruption, _}} =
               Store.fold(inst, :due, {lo, hi}, 0, fn _k, _v, n -> {:cont, n + 1} end)
    end

    test "a damaged WAL record refuses to open; a torn tail drops cleanly", %{
      config: config,
      instance: inst
    } do
      for seq <- 1..50 do
        :ok =
          Store.write(inst, [{:put, :hooks, Keys.hook(seq), :binary.copy("w", 8192)}], sync: true)
      end

      stop_supervised!({Store, inst})

      log = newest(config, ".log")
      original = File.read!(log)

      flip_mid(log)
      assert {:error, error} = start_supervised({Store, instance: inst})
      assert inspect(error) =~ "store_open_failed"

      File.write!(log, binary_part(original, 0, byte_size(original) - 100))
      start_supervised!({Store, instance: inst})

      # Only the final, partly written record may be lost.
      for seq <- 1..49 do
        assert {:ok, _} = Store.get(inst, :hooks, Keys.hook(seq))
      end
    end
  end

  describe "reopen/1" do
    test "republishes working handles and keeps every committed key", %{instance: inst} do
      :ok = Store.write(inst, [{:put, :hooks, Keys.hook(1), "kept"}], sync: true)

      assert :ok = Store.reopen(inst)
      assert Store.get(inst, :hooks, Keys.hook(1)) == {:ok, "kept"}

      assert :ok = Store.write(inst, [{:put, :hooks, Keys.hook(2), "after"}], sync: true)
      assert Store.get(inst, :hooks, Keys.hook(2)) == {:ok, "after"}
    end
  end

  describe "ready/1" do
    test "writes the probe key synced, and reuses the answer for a second", %{instance: inst} do
      assert :ok = Store.ready(inst)
      assert {:ok, <<first::64>>} = Store.get(inst, :default, Keys.meta("ready_probe"))

      # Inside the cache window: no second write, the same timestamp.
      Process.sleep(5)
      assert :ok = Store.ready(inst)
      assert {:ok, <<^first::64>>} = Store.get(inst, :default, Keys.meta("ready_probe"))

      Process.sleep(1_050)
      assert :ok = Store.ready(inst)
      assert {:ok, <<second::64>>} = Store.get(inst, :default, Keys.meta("ready_probe"))
      assert second > first
    end

    test "a store that is not running answers :store_unavailable" do
      assert Store.ready(:no_such_instance) == {:error, :store_unavailable}
    end

    test "a write another process saw fail turns it unready even while the probe fits",
         %{instance: inst} do
      assert :ok = Store.ready(inst)

      # What a refused ingest batch reports; the cached `:ok` must not hide it.
      Store.report_write_failure(inst, :enospc)
      assert Store.ready(inst) == {:error, :write_failed}
    end
  end

  describe "a store that failed to reopen" do
    test "opens again by itself once whatever blocked it is gone" do
      cfg = test_config(roles: [:edge])
      inst = cfg.instance
      put_config(cfg)
      pid = start_supervised!({Store, instance: inst, retry_open_ms: 20})
      :ok = Store.write(inst, [{:put, :hooks, Keys.hook(1), "kept"}], sync: true)

      good = :sys.get_state(pid).path
      blocker = Path.join(cfg.data_dir, "blocker")
      File.write!(blocker, "a file where a directory has to be")

      # A path RocksDB cannot open, for any user on any OS: its parent is a file.
      :sys.replace_state(pid, fn state -> %{state | path: Path.join(blocker, "store")} end)

      assert {:error, _reason} = Store.reopen(inst)
      assert Store.get(inst, :hooks, Keys.hook(1)) == {:error, :store_unavailable}

      # Nobody asks again; the store does, and gets in once the path is good.
      :sys.replace_state(pid, fn state -> %{state | path: good} end)
      assert eventually(fn -> Store.get(inst, :hooks, Keys.hook(1)) == {:ok, "kept"} end)
    end
  end

  describe "report_write_failure/2" do
    test "reopens the store, at most once per interval, whichever process reports" do
      cfg = test_config(roles: [:edge])
      inst = cfg.instance

      put_config(cfg)
      opts = [instance: inst, retry_open_ms: 20, reopen_interval_ms: 60_000]
      pid = start_supervised!({Store, opts})

      :ok = Store.write(inst, [{:put, :hooks, Keys.hook(1), "kept"}], sync: true)

      good = :sys.get_state(pid).path
      blocker = Path.join(cfg.data_dir, "blocker")
      File.write!(blocker, "a file where a directory has to be")
      bad = Path.join(blocker, "store")

      # The reopen the report asks for fails on a path RocksDB cannot open (its
      # parent is a file), which leaves the store closed: reads prove it ran.
      :sys.replace_state(pid, fn state -> %{state | path: bad} end)
      assert :ok = Store.report_write_failure(inst, :enospc)

      assert eventually(fn ->
               Store.get(inst, :hooks, Keys.hook(1)) == {:error, :store_unavailable}
             end)

      # The store's own retry gets in once the path is good again.
      :sys.replace_state(pid, fn state -> %{state | path: good} end)
      assert eventually(fn -> Store.get(inst, :hooks, Keys.hook(1)) == {:ok, "kept"} end)

      # A second report inside the interval is dropped: the store is not closed.
      :sys.replace_state(pid, fn state -> %{state | path: bad} end)
      assert :ok = Store.report_write_failure(inst, :enospc)
      _ = :sys.get_state(pid)
      assert Store.get(inst, :hooks, Keys.hook(1)) == {:ok, "kept"}
    end
  end

  describe "effective settings" do
    # RocksDB ignores an option value it cannot parse and keeps the default, and
    # for `wal_recovery_mode` the default is the mode that silently drops every
    # acked write after the first bad WAL record. So read back what the database
    # actually opened with: RocksDB writes a fresh OPTIONS file on every open.
    test "the database opened with the recovery, blob and buffer settings we asked for", %{
      config: config
    } do
      sections = options_sections(config)
      db = sections["DBOptions"]

      assert db["wal_recovery_mode"] == "kTolerateCorruptedTailRecords"
      assert db["paranoid_checks"] == "true"
      assert db["max_total_wal_size"] == "536870912"

      hooks = sections[~s(CFOptions "hooks")]
      assert hooks["enable_blob_files"] == "true"
      assert hooks["min_blob_size"] == "4096"
      assert hooks["compression"] == "kZSTD"
      assert hooks["write_buffer_size"] == "67108864"

      # Same blob and compression settings, but not hooks' 64 MiB memtable.
      quarantine = sections[~s(CFOptions "quarantine")]
      assert quarantine["enable_blob_files"] == "true"
      assert quarantine["compression"] == "kZSTD"
      assert quarantine["write_buffer_size"] == "4194304"
    end
  end

  # ── helpers ───────────────────────────────────────────────────────────────

  defp eventually(fun, tries \\ 100) do
    cond do
      fun.() ->
        true

      tries == 0 ->
        false

      true ->
        Process.sleep(20)
        eventually(fun, tries - 1)
    end
  end

  # `%{"DBOptions" => %{"key" => "value"}, ~s(CFOptions "hooks") => ...}` from the
  # newest OPTIONS file in the store directory.
  defp options_sections(config) do
    config
    |> store_dir()
    |> Path.join("OPTIONS-*")
    |> Path.wildcard()
    |> Enum.max_by(&options_number/1)
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.reduce({nil, %{}}, fn line, {section, acc} ->
      line = String.trim(line)

      cond do
        String.starts_with?(line, "[") and String.ends_with?(line, "]") ->
          name = line |> String.trim_leading("[") |> String.trim_trailing("]")
          {name, Map.put_new(acc, name, %{})}

        section != nil and not String.starts_with?(line, "#") and String.contains?(line, "=") ->
          [key, value] = String.split(line, "=", parts: 2)
          {section, put_in(acc, [section, String.trim(key)], String.trim(value))}

        true ->
          {section, acc}
      end
    end)
    |> elem(1)
  end

  defp options_number(path) do
    path |> Path.basename() |> String.replace_prefix("OPTIONS-", "") |> String.to_integer()
  end

  # Flush a column family so its data is in sst/blob files, not only the WAL.
  defp flush!(inst, cf) do
    [{:handles, handles}] = :ets.lookup(:"ankusa_store_#{inst}", :handles)
    :ok = :rocksdb.flush(handles.db, Map.fetch!(handles.cfs, cf), [])
  end

  defp store_dir(config), do: Config.path(config, "store")

  defp largest(config, ext) do
    config |> store_dir() |> Path.join("*#{ext}") |> Path.wildcard() |> Enum.max_by(&size/1)
  end

  defp newest(config, ext) do
    config
    |> store_dir()
    |> Path.join("*#{ext}")
    |> Path.wildcard()
    |> Enum.max_by(&File.stat!(&1, time: :posix).mtime)
  end

  defp size(path), do: File.stat!(path).size

  defp flip_mid(path) do
    bin = File.read!(path)
    mid = div(byte_size(bin), 2)
    <<head::binary-size(^mid), byte, tail::binary>> = bin
    File.write!(path, <<head::binary, Bitwise.bxor(byte, 1), tail::binary>>)
  end
end
