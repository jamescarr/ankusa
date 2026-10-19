defmodule Ankusa.Intake.RecoveryTest do
  @moduledoc """
  Boot recovery (ADR 0002, section 8): the node's own log replayed into its
  store through the one seq-and-id rule, and the same code offline
  (`mix ankusa.recover`). The log is a file-backed `Ankusa.Intake.Memory`, so a
  replacement node started on an empty data directory finds it.
  """
  use ExUnit.Case, async: false

  @moduletag capture_log: true

  import Ankusa.Test.Dual
  import Ankusa.TestHelpers, only: [unique_instance: 0]

  alias Ankusa.Envelope
  alias Ankusa.Intake.{Memory, Pending, Recovery}
  alias Ankusa.Queue
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  defp log_path do
    path = Path.join(System.tmp_dir!(), "intake-#{System.unique_integer([:positive])}.log")
    on_exit(fn -> File.rm(path) end)
    path
  end

  defp log_config(path, overrides \\ []),
    do: dual_config([intake: %{memory: [path: path], offset_flush_ms: 100}] ++ overrides)

  defp start(config) do
    ExUnit.Callbacks.start_supervised!({Ankusa.Instance, config})
    config
  end

  defp stop(config), do: stop_supervised!({Ankusa.Instance, config.instance})

  # Acks `n` hooks, then loses the host: the instance stops and its data dir goes.
  defp ack_and_lose_host(config, n) do
    start(config)
    envs = for i <- 1..n, do: elem(ingest(config, "evt-#{i}"), 1)
    assert drained(config)
    stop(config)
    File.rm_rf!(config.data_dir)
    envs
  end

  defp hooks(config) do
    {:ok, hooks} = Queue.hooks(config.instance, 0, 1_000)
    hooks
  end

  defp u64(config, name) do
    case Store.get(config.instance, :default, Keys.meta(name)) do
      {:ok, <<n::64>>} -> n
      _ -> nil
    end
  end

  test "replays the tail after m:intake_offset on every boot, with the original seqs" do
    config = log_config(log_path())
    acked = ack_and_lose_host(config, 3)

    start(config)

    recovered = hooks(config)
    assert Enum.map(recovered, & &1.id) == Enum.map(acked, & &1.id)
    assert Enum.map(recovered, & &1.seq) == Enum.map(acked, & &1.seq)

    # the offset is the log head: the 3 acked records and the 3 the boot republished
    assert u64(config, "intake_offset") == length(Memory.records(config.instance)) - 1

    # next_seq was raised past every replayed seq
    assert {:ok, next} = ingest(config, "after")
    assert next.seq > Enum.max(Enum.map(acked, & &1.seq))

    # a second boot replays nothing new
    stop(config)
    start(config)
    assert length(hooks(config)) == 4
  end

  test "applies a record once: duplicate and reordered records change nothing" do
    config = log_config(log_path())
    acked = ack_and_lose_host(config, 2)

    # the log also holds the first record again, behind newer ones
    config = start(config)
    [first | _] = Memory.records(config.instance)
    Memory.put_raw(config.instance, Map.drop(first, [:position]))

    # the log holds the two acked records, the extra copy, and the two copies the
    # recovery boot republished (the replayed hooks sit above the fresh store's
    # watermark): every one of them is present, none is applied twice
    assert {:ok, %{records: 5, present: 5, applied: 0, collided: 0}} =
             Recovery.run(config.instance, from: :earliest)

    assert Enum.map(hooks(config), & &1.id) == Enum.map(acked, & &1.id)
  end

  test "a seq collision with a different id is re-sequenced, never overwritten" do
    config = log_config(log_path())
    [first, _] = ack_and_lose_host(config, 2)
    start(config)

    # another hook claims the seq of the first, as a second live node sharing the id would
    intruder = %{first | id: "intruder-id"}

    Memory.put_raw(config.instance, %{
      node: "node-a",
      seq: first.seq,
      id: "intruder-id",
      kind: "hook",
      version: "1",
      bin: Envelope.to_binary(%{intruder | seq: nil}),
      tenant: "default",
      source: "load",
      incarnation: "0"
    })

    assert {:ok, %{collided: 1}} = Recovery.run(config.instance, from: :earliest)

    ids = Enum.map(hooks(config), & &1.id)
    assert first.id in ids and "intruder-id" in ids
    assert length(ids) == 3
  end

  test "republishes local hooks above the watermark (disk to dual)" do
    path = log_path()
    disk = log_config(path, wal: :disk)
    start(disk)
    acked = for i <- 1..2, do: elem(ingest(disk, "evt-#{i}"), 1)
    stop(disk)

    # the same data dir, now dual: the first dual boot republishes the whole local tail
    dual = %{disk | wal: :dual}
    start(dual)

    assert Enum.map(Memory.records(dual.instance), & &1.id) == Enum.map(acked, & &1.id)
    assert u64(dual, "intake_seq") == 2
  end

  test "unknown version and kind are skipped and counted; an unknown source is unroutable" do
    config = log_config(log_path())
    [env] = ack_and_lose_host(config, 1)
    start(config)

    test = self()
    handler = "replay-#{inspect(config.instance)}"

    :telemetry.attach(
      handler,
      [:ankusa, :intake, :replay],
      fn _e, m, meta, _ -> send(test, {:replay, meta.result, m.count}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    raw = fn attrs ->
      Map.merge(
        %{
          node: "node-a",
          seq: 99,
          id: "x",
          kind: "hook",
          version: "1",
          bin: Envelope.to_binary(%{env | id: "ghost-hook", source_id: "ghost", seq: nil}),
          tenant: "default",
          source: "ghost",
          incarnation: "0"
        },
        attrs
      )
    end

    Memory.put_raw(config.instance, raw.(%{version: "9"}))
    Memory.put_raw(config.instance, raw.(%{kind: "tombstone"}))
    Memory.put_raw(config.instance, raw.(%{seq: 7, id: "ghost-hook"}))

    assert {:ok, %{unroutable: 1, applied: 0, skipped: 0}} =
             Recovery.run(config.instance, from: :earliest)

    assert_receive {:replay, :skipped_unknown, 1}
    assert_receive {:replay, :unroutable, 1}
  end

  test "a truncated log refuses a restored store and warns on an existing one; --allow-gap goes on and writes an audit row" do
    config = log_config(log_path())
    start(config)
    for i <- 1..3, do: ingest(config, "evt-#{i}")
    assert drained(config)
    assert wait_until(fn -> u64(config, "intake_offset") == 2 end)

    Memory.set_earliest(config.instance, 10)

    assert {:error, {:intake_truncated, 3, 10}} = Recovery.run(config.instance, origin: :restored)
    assert {:ok, _} = Recovery.run(config.instance, origin: :existing)

    # refusing and warning write nothing
    assert {:ok, %{entries: []}} =
             Ankusa.Audit.list(config.instance, action: "intake.recover.allow_gap")

    # a dry run says so and writes nothing either
    assert {:ok, _} =
             Recovery.run(config.instance, origin: :restored, allow_gap: true, dry_run: true)

    assert {:ok, %{entries: []}} =
             Ankusa.Audit.list(config.instance, action: "intake.recover.allow_gap")

    assert {:ok, _} =
             Recovery.run(config.instance,
               origin: :restored,
               allow_gap: true,
               actor: "ankusa recover"
             )

    assert {:ok, %{entries: [row]}} =
             Ankusa.Audit.list(config.instance, action: "intake.recover.allow_gap")

    assert row.actor == "ankusa recover"
    assert row.resource == "node-a"
    assert row.result == "ok"

    assert row.detail == %{
             "node" => "node-a",
             "wanted" => 3,
             "earliest" => 10,
             "lost_positions" => 7
           }
  end

  describe "parse_args (mix ankusa.recover and the image's recover command)" do
    test "reads the flags and defaults to the store's own offset" do
      assert {:ok, [from: :backup]} = Recovery.parse_args([])

      assert {:ok, opts} =
               Recovery.parse_args(
                 ~w(--node a --from earliest --dry-run --allow-gap --instance i)
               )

      assert Enum.sort(opts) ==
               [allow_gap: true, dry_run: true, from: :earliest, instance: "i", node: "a"]

      assert {:ok, [from: 42]} = Recovery.parse_args(~w(--from 42))
      assert {:ok, [from: :backup]} = Recovery.parse_args(~w(--from backup))
    end

    test "refuses what it cannot read" do
      assert {:error, "--from must be backup, earliest or a position, got \"soon\""} =
               Recovery.parse_args(~w(--from soon))

      assert {:error, "--from must be backup, earliest or a position, got \"-1\""} =
               Recovery.parse_args(~w(--from -1))

      assert {:error, "invalid options: " <> _} = Recovery.parse_args(~w(--bogus))
      assert {:error, "unexpected arguments: " <> _} = Recovery.parse_args(~w(stray))
    end

    test "format_summary is the one line both commands print" do
      summary = %{
        records: 5,
        applied: 3,
        present: 1,
        collided: 0,
        unroutable: 1,
        skipped: 2,
        republished: 4
      }

      assert Recovery.format_summary(summary) ==
               "records=5 applied=3 present=1 collided=0 unroutable=1 skipped=2 republished=4"
    end
  end

  test "node id mismatch refuses boot" do
    path = log_path()
    a = log_config(path)
    start(a)
    stop(a)

    b = %{a | intake: %{a.intake | node_id: "node-b"}}
    Ankusa.put_config(b)

    assert {:error, reason} = start_supervised({Ankusa.Instance, b})
    assert inspect(reason) =~ "intake_node_mismatch"
  end

  test "mix ankusa.recover is the same path as boot, a second run changes nothing, --dry-run writes nothing" do
    config = log_config(log_path(), intake: %{recover_on_boot: false})
    start(config)
    acked = for i <- 1..3, do: elem(ingest(config, "evt-#{i}"), 1)
    assert drained(config)
    stop(config)
    File.rm_rf!(config.data_dir)

    assert {:ok, %{records: 3, applied: 3}} = Recovery.run_offline(config, dry_run: true)
    assert {:ok, %{applied: 3, present: 0}} = Recovery.run_offline(config)
    # nothing is left above the store's offset: a second run changes nothing
    assert {:ok, %{records: 0, applied: 0, present: 0}} = Recovery.run_offline(config)

    start(config)
    assert Enum.map(hooks(config), & &1.id) == Enum.map(acked, & &1.id)
  end

  test "a clean stop leaves an offset at the log head" do
    config = log_config(log_path())
    start(config)
    for i <- 1..3, do: ingest(config, "evt-#{i}")
    assert drained(config)
    stop(config)

    start(config)
    assert u64(config, "intake_offset") == 2
    assert {:ok, "node-a"} = Store.get(config.instance, :default, Keys.meta("intake_node"))
  end

  describe "the offset never runs ahead of the store" do
    test "it stays below the lowest record that is only in the log" do
      t = Pending.create(unique_instance())

      Pending.bump_confirmed(t, 10)
      assert Pending.offset(t) == 10

      Pending.enter(t, [{5, "a", 1, nil, %{}}], 5, 0)
      Pending.carry(t, %{local: :missing, log: :ok, position: 6}, [{5, "bin", %{}}], batch(5), 0)

      assert Pending.offset(t) == 5
      assert Pending.watermark(t, 20) == 4

      Pending.local_done(t, 5)
      assert Pending.offset(t) == 10
      assert Pending.watermark(t, 20) == 19
    end

    defp batch(id), do: %{id: id, ops: [], probe_seq: id, seqs: [id], at: 0}
  end
end
