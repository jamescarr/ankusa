defmodule Ankusa.DedupStoreTest do
  @moduledoc """
  The idempotent receiver's rules, pinned where they are decided.

  These are the non-negotiable properties from the design: the store keeps
  `dedup_key -> first_seq` rather than a seen flag, a re-read of a record is
  delivered rather than dropped, expiry is measured between record commit times
  and never against the wall clock, and every copy of an event lands in the same
  partition.
  """

  use ExUnit.Case, async: true

  alias Ankusa.DedupStore
  alias Ankusa.DedupStore.ETS
  alias Ankusa.Dispatch.Receiver
  alias Ankusa.{Envelope, Source}

  # A commit time far in the past, so any rule that reached for the wall clock
  # instead of the records' own timeline would show up as a wrong answer.
  @t0 1_000_000_000
  @ttl 60_000

  defp source(opts), do: Source.new("src", opts)

  defp envelope(fields) do
    struct!(
      %Envelope{
        id: fields[:id] || "e#{System.unique_integer([:positive])}",
        source_id: "src",
        tenant_id: "acme",
        received_at: @t0,
        method: "POST",
        path: "/hooks/src",
        headers: [],
        body: "{}"
      },
      fields
    )
  end

  # ── the rule ──────────────────────────────────────────────────────────────
  #
  # Exercised directly as well as through a store: a wrong rule is the failure
  # mode that loses records, so it is worth pinning without any storage in the
  # way.

  test "the first copy is delivered and later copies are dropped" do
    assert {:deliver, entry} = DedupStore.decide(:error, 10, @t0, @ttl)
    assert entry == %{first_seq: 10, committed_at: @t0}

    assert {:drop, _} = DedupStore.decide(entry, 20, @t0 + 1, @ttl)
    assert {:drop, _} = DedupStore.decide(entry, 30, @t0 + 2, @ttl)
  end

  # A flag cannot tell a duplicate from a re-read; first_seq can. The dispatcher
  # resumes from its durable cursor, so after a crash it re-reads the record it
  # had already delivered — and losing it would lose the event.
  test "a re-read of the same record is delivered" do
    assert {:deliver, entry} = DedupStore.decide(:error, 10, @t0, @ttl)
    assert {:deliver, ^entry} = DedupStore.decide(entry, 10, @t0, @ttl)
  end

  test "seeing an earlier copy after a later one keeps the earlier one" do
    assert {:deliver, entry} = DedupStore.decide(:error, 20, @t0, @ttl)

    # A rewind, or a copy that arrived out of order: this is the earliest copy
    # we know of, so it goes through and becomes the one later copies compare to.
    assert {:deliver, entry} = DedupStore.decide(entry, 10, @t0, @ttl)
    assert entry.first_seq == 10

    assert {:drop, _} = DedupStore.decide(entry, 30, @t0, @ttl)
  end

  test "the window slides forward with the copies it drops" do
    {:deliver, entry} = DedupStore.decide(:error, 10, @t0, @ttl)

    # Just inside the window: dropped, and the window moves to this copy.
    {:drop, entry} = DedupStore.decide(entry, 20, @t0 + @ttl - 1, @ttl)
    assert entry.committed_at == @t0 + @ttl - 1

    # Measured from that copy, this one is inside the window too.
    assert {:drop, _} = DedupStore.decide(entry, 30, @t0 + @ttl + 1, @ttl)
  end

  # The rule the design calls out by name: expiry measured between the records,
  # never against the wall clock at read time. Both commits here are years
  # behind the wall clock, and the difference between them is inside the window,
  # so a lagging dispatcher still dedupes.
  test "expiry is measured between commit times, not against the wall clock" do
    assert System.system_time(:millisecond) - @t0 > 10 * @ttl

    {:deliver, entry} = DedupStore.decide(:error, 10, @t0, @ttl)
    assert {:drop, _} = DedupStore.decide(entry, 20, @t0 + @ttl - 1, @ttl)
  end

  test "copies whose commits are older than the window are new events" do
    {:deliver, entry} = DedupStore.decide(:error, 10, @t0, @ttl)
    assert {:deliver, fresh} = DedupStore.decide(entry, 20, @t0 + @ttl + 1, @ttl)
    assert fresh == %{first_seq: 20, committed_at: @t0 + @ttl + 1}
  end

  # ── the store ─────────────────────────────────────────────────────────────

  test "the ETS store applies the rule and remembers the first commit" do
    store = ETS.new()

    assert :deliver = DedupStore.record(store, {"acme", "src"}, "k1", 1, @t0, @ttl)
    assert :drop = DedupStore.record(store, {"acme", "src"}, "k1", 2, @t0 + 1, @ttl)
    assert :drop = DedupStore.record(store, {"acme", "src"}, "k1", 3, @t0 + 2, @ttl)

    # A different key is a different event; a different scope is a different
    # event even with the same key (keys are tenant- and source-scoped).
    assert :deliver = DedupStore.record(store, {"acme", "src"}, "k2", 4, @t0, @ttl)
    assert :deliver = DedupStore.record(store, {"other", "src"}, "k1", 5, @t0, @ttl)
  end

  test "the ETS store sweeps only entries the rule would ignore" do
    store = ETS.new(sweep_delta: 2)

    # One key from long ago, then enough traffic past the window to trigger a
    # sweep. The cutoff is the incoming record's own window, so an entry that
    # could still dedupe *this* record is kept however old it looks.
    :deliver = DedupStore.record(store, {"acme", "src"}, "old", 1, @t0, @ttl)

    for n <- 1..4 do
      :deliver =
        DedupStore.record(store, {"acme", "src"}, "new#{n}", 100 + n, @t0 + @ttl + 1, @ttl)
    end

    %ETS{table: table} = store
    keys = table |> :ets.tab2list() |> Enum.map(fn {{_scope, key}, _} -> key end)

    assert "old" not in keys
    assert Enum.sort(keys) == ["new1", "new2", "new3", "new4"]
  end

  # ── the receiver ──────────────────────────────────────────────────────────

  test "a source with a dedup key delivers the first copy of an event only" do
    receiver = receiver(store: {ETS, []})
    source = source(dedup_key: {Ankusa.DedupKey.Rules, json: ["event"]})

    first = envelope(body: ~s({"event":"E1"}))
    second = envelope(body: ~s({"event":"E1"}))

    refute Receiver.duplicate?(receiver, source, %{first | seq: 1, committed_at: @t0})
    assert Receiver.duplicate?(receiver, source, %{second | seq: 2, committed_at: @t0 + 1})
  end

  # `dedup: :none` is how a source says it wants every copy.
  test "dedup: :none delivers every copy" do
    receiver = receiver(store: {ETS, []})
    source = source(dedup: :none, dedup_key: {Ankusa.DedupKey.Rules, json: ["event"]})

    assert Receiver.key(source, envelope(body: ~s({"event":"E1"}))) == nil

    for seq <- 1..3 do
      env = envelope(body: ~s({"event":"E1"}), seq: seq, committed_at: @t0 + seq)
      refute Receiver.duplicate?(receiver, source, env)
    end
  end

  test "a source with no dedup key delivers every copy" do
    receiver = receiver(store: {ETS, []})
    source = source(dedup_key: nil)

    for seq <- 1..3 do
      env = envelope(body: ~s({"event":"E1"}), seq: seq, committed_at: @t0 + seq)
      refute Receiver.duplicate?(receiver, source, env)
    end
  end

  # Every copy of an event has to land in the same partition, or two consumers
  # would each see part of the copies and both would deliver. The partition is
  # the scope, so it does not depend on the key, the seq or anything else that
  # differs between copies.
  test "the partition follows the scope, not the key" do
    env = envelope([])

    for partitions <- [1, 4, 64] do
      scope = Receiver.scope(env)
      partition = Receiver.partition(scope, partitions)

      assert partition == Receiver.partition({"acme", "src"}, partitions)
      assert partition >= 0 and partition < partitions

      # Two copies of the same event, keyed differently by a badly-behaved
      # rule, still agree on the partition.
      other = %{env | id: "another-envelope", seq: 999, committed_at: @t0 + 5}
      assert Receiver.partition(Receiver.scope(other), partitions) == partition
    end
  end

  # ── boot ──────────────────────────────────────────────────────────────────

  # A source set to dedup with nothing to dedup on would deliver every copy of
  # every event without saying so. Boot is where an operator finds out.
  test "a source that cannot compute a key warns at boot" do
    config =
      Ankusa.TestHelpers.test_config(
        roles: [:dispatch],
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{"unlabelled" => [sinks: [{Ankusa.Sink.Log, []}]]}}
      )

    log = ExUnit.CaptureLog.capture_log(fn -> start_supervised!({Ankusa.Instance, config}) end)

    assert log =~ "unlabelled"
    assert log =~ "dedup: :auto but no dedup_key configured"
  end

  test "a source that says what it wants does not warn" do
    config =
      Ankusa.TestHelpers.test_config(
        roles: [:dispatch],
        source_store:
          {Ankusa.SourceStore.Static,
           sources: %{
             "no_dedup" => [dedup: :none, sinks: [{Ankusa.Sink.Log, []}]],
             "keyed" => [
               dedup_key: {Ankusa.DedupKey.Rules, json: ["id"]},
               sinks: [{Ankusa.Sink.Log, []}]
             ]
           }}
      )

    log = ExUnit.CaptureLog.capture_log(fn -> start_supervised!({Ankusa.Instance, config}) end)

    refute log =~ "no dedup_key configured"
  end

  defp receiver(opts), do: Receiver.new(0, Keyword.merge([ttl_ms: @ttl], opts))
end
