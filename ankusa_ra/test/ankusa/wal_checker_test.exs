defmodule Ankusa.WAL.CheckerTest do
  @moduledoc """
  The checker is the gate every chaos run and every fault drill reports through,
  and it is pure — so its own logic is tested here, without a cluster, and
  everywhere. The invariants themselves are exercised against a real cluster in
  `wal_ra_faults_test.exs`; what is pinned down here is that the checker reads
  its evidence the way the harness writes it, and that a malformed input fails
  loudly instead of quietly checking nothing.
  """

  use ExUnit.Case, async: true

  alias Ankusa.WAL.Checker

  # The evidence files are JSON, so a tagged operation arrives as
  # `{"tag":"observe","0":[...]}` and the verify task turns it into a tuple.
  # These helpers build the tuple form directly.
  defp edge(status, id, at, done), do: event("loadgen", {:edge, status, id}, at, done)
  defp observe(seqs, at), do: event("observer", {:observe, seqs}, at)

  defp event(client, op, at, done \\ nil) do
    %{client: client, op: op, invoked_at: at, completed_at: done || at, result: nil}
  end

  defp readable(rows), do: Enum.map(rows, fn {id, seq} -> %{id: id, seq: seq, sha256: id} end)

  @three [{"id1", 1}, {"id2", 2}, {"id3", 3}]

  test "a clean history reports nothing" do
    events =
      [
        observe([1], 1_000),
        observe([2, 3], 2_000),
        edge("2xx", "id1", 1_500, 1_520),
        edge("2xx", "id2", 1_600, 1_620)
      ]

    report = Checker.check(events, MapSet.new(["id1", "id2"]), readable(@three))

    assert report.missing == []
    assert report.violations == []
  end

  # I3 sees a cursor-following reader through `observe` events, which carry
  # seqs only: a read *plan* has no bodies in it, so requiring payloads here
  # would mean the harness could never check I3 at all.
  test "an observer that walks the log in order trips nothing" do
    events = [observe([1], 1_000), observe([2], 2_000), observe([3], 3_000)]

    report = Checker.check(events, MapSet.new(), readable(@three))

    assert report.violations == []
  end

  test "an observer that jumps over an unread seq trips I3" do
    events = [observe([1], 1_000), observe([3], 3_000)]

    report = Checker.check(events, MapSet.new(), readable(@three))

    assert {:i3, {:missed_below_last_seen, "observer", missed}} =
             Enum.find(report.violations, &match?({:i3, _}, &1))

    assert {"id2", 2} in missed
  end

  test "an observer that goes backwards trips I3" do
    events = [observe([2], 1_000), observe([1], 2_000)]

    report = Checker.check(events, MapSet.new(), readable(@three))

    assert Enum.any?(report.violations, &match?({:i3, {:not_ascending, _, _}}, &1))
  end

  # I8: inside an outage window the edge must answer 503, and never 2xx.
  test "a 2xx during a quorum outage trips I8" do
    events = [edge("2xx", "id1", 15_000, 15_020)]
    opts = [quorum_down: [{10_000, 20_000}]]

    report = Checker.check(events, MapSet.new(["id1"]), readable(@three), opts)

    assert {:i8, {:acked_without_quorum, "id1", 15_000, 15_020}} in report.violations
  end

  # A real power-loss run reported 35 of these: appends that were in flight when
  # the member was killed, retried by the adapter, and committed once it was
  # back. They are honest acks, and only the completion time tells them apart
  # from an ack that happened while there was no quorum.
  test "a 2xx invoked in the window but answered after it closed is not a violation" do
    events = [edge("2xx", "id1", 15_000, 25_000)]
    opts = [quorum_down: [{10_000, 20_000}]]

    report = Checker.check(events, MapSet.new(["id1"]), readable(@three), opts)

    assert report.violations == []
  end

  # The window's end is stamped by a once-a-second poll, so an ack completing in
  # the last moment of it may be an honest one: quorum was back and the poll had
  # not noticed yet.
  test "a 2xx answered in the window's closing grace gap is not a violation" do
    events = [edge("2xx", "id1", 15_000, 19_500)]
    opts = [quorum_down: [{10_000, 20_000}]]

    report = Checker.check(events, MapSet.new(["id1"]), readable(@three), opts)

    assert report.violations == []
  end

  test "a 2xx answered well inside the window is still a violation" do
    events = [edge("2xx", "id1", 12_000, 12_500)]
    opts = [quorum_down: [{10_000, 20_000}]]

    report = Checker.check(events, MapSet.new(["id1"]), readable(@three), opts)

    assert {:i8, {:acked_without_quorum, "id1", 12_000, 12_500}} in report.violations
  end

  test "a 2xx answered after its own window closed is not a violation" do
    # Invoked while the *first* window is open, answered after it closed — with
    # a second window later on, which must not be read as "answered in time".
    events = [edge("2xx", "id1", 15_000, 22_000)]
    opts = [quorum_down: [{10_000, 20_000}, {25_000, 30_000}]]

    report = Checker.check(events, MapSet.new(["id1"]), readable(@three), opts)

    assert report.violations == []
  end

  test "the same 2xx outside the window is fine" do
    events = [edge("2xx", "id1", 25_000, 25_020)]
    opts = [quorum_down: [{10_000, 20_000}]]

    report = Checker.check(events, MapSet.new(["id1"]), readable(@three), opts)

    assert report.violations == []
  end

  # The nemesis stamps the window before it applies the fault, so for a moment
  # the cluster is still healthy. An ack in that gap is honest and must not fail
  # the gate.
  test "a 2xx in the window's opening grace gap is not a violation" do
    events = [edge("2xx", "id1", 10_500, 10_520)]
    opts = [quorum_down: [{10_000, 20_000}]]

    report = Checker.check(events, MapSet.new(["id1"]), readable(@three), opts)

    assert report.violations == []
  end

  test "a 503 that took longer than the append timeout trips I8" do
    events = [edge("503", nil, 15_000, 40_000)]
    opts = [quorum_down: [{10_000, 60_000}], append_timeout_ms: 10_000]

    report = Checker.check(events, MapSet.new(), readable(@three), opts)

    assert {:i8, {:too_slow, nil, 25_000, 11_000}} in report.violations
  end

  # A power-loss run whose outage lasted 5.5s produced exactly this: the load
  # generator's own 10s receive timeout fired while the edge was still holding
  # and retrying, and quorum had been back for four seconds by then. Holding is
  # what I8's second sentence allows, so the client giving up is not the edge
  # failing to shed.
  test "a client timeout during an outage shorter than the bound is not a violation" do
    events = [edge("error", nil, 12_000, 22_000)]
    opts = [quorum_down: [{10_000, 20_000}], append_timeout_ms: 10_000]

    report = Checker.check(events, MapSet.new(), readable(@three), opts)

    assert report.violations == []
  end

  # Same client timeout, but the outage outlasts the bound: now the edge really
  # should have shed, and answering nothing is a violation.
  test "a client timeout during an outage longer than the bound is a violation" do
    events = [edge("error", nil, 12_000, 22_000)]
    opts = [quorum_down: [{10_000, 60_000}], append_timeout_ms: 10_000]

    report = Checker.check(events, MapSet.new(), readable(@three), opts)

    assert {:i8, {:not_503, nil, "error"}} in report.violations
  end

  test "a slow 503 is only late if the outage outlasted the bound" do
    events = [edge("503", nil, 15_000, 40_000)]
    opts = [quorum_down: [{10_000, 60_000}], append_timeout_ms: 10_000]
    report = Checker.check(events, MapSet.new(), readable(@three), opts)
    assert {:i8, {:too_slow, nil, 25_000, 11_000}} in report.violations

    # quorum returned at 20s, so shedding at 40s is not a bounded-unavailability
    # failure: the request could have been committed instead.
    opts = [quorum_down: [{10_000, 20_000}], append_timeout_ms: 10_000]
    report = Checker.check(events, MapSet.new(), readable(@three), opts)
    assert report.violations == []
  end

  test "502 inside a window is not 503" do
    events = [edge("502", nil, 15_000, 15_010)]
    opts = [quorum_down: [{10_000, 60_000}]]

    report = Checker.check(events, MapSet.new(), readable(@three), opts)

    assert {:i8, {:not_503, nil, "502"}} in report.violations
  end

  # A window the checker cannot read must fail the run: comparing against `nil`
  # would make every event fall outside every window and I8 would check nothing.
  test "a malformed outage window is an error, not a silent skip" do
    events = [edge("2xx", "id1", 15_000, 15_020)]

    for windows <- [[{nil, nil}], [{10_000, nil}], [:nope]] do
      assert_raise ArgumentError, ~r/quorum_down window/, fn ->
        Checker.check(events, MapSet.new(["id1"]), readable(@three), quorum_down: windows)
      end
    end
  end

  # The counts exist so a gate cannot report "passed" for an invariant it never
  # looked at. This is the shape the chaos harness produces — edges and an
  # observer, no append or lease events — and the invariants that only the fault
  # drills can exercise must show up as zeros rather than as clean passes.
  test "the report says which invariants the evidence did not reach" do
    events = [observe([1], 1_000), edge("2xx", "id1", 1_500, 1_520)]
    report = Checker.check(events, MapSet.new(["id1"]), readable(@three))

    assert report.evaluated[:i1] == 1
    assert report.evaluated[:i3] == 1
    assert report.evaluated[:i10] == 3

    for invariant <- [:i2, :i4, :i6, :i7, :i8, :i9] do
      assert report.evaluated[invariant] == 0, "#{invariant} should be unexercised here"
    end
  end

  test "an append history exercises I4 and I2" do
    meta = %{tenant: "t", source: "s", sha256: "id1", batch_id: "b1"}

    events = [
      %{
        client: "edge",
        op: {:append, "id1", meta},
        invoked_at: 1_000,
        completed_at: 1_010,
        result: {:ok, 1}
      },
      %{
        client: "edge",
        op: {:read, 0, 10},
        invoked_at: 1_100,
        completed_at: 1_110,
        result: [%{seq: 1, id: "id1", sha256: "id1"}]
      }
    ]

    report = Checker.check(events, MapSet.new(["id1"]), readable(@three))

    assert report.evaluated[:i4] == 1
    # I2 inspected two things: the read event's records, and the stored row whose
    # digest it could hold against what the append said was sent.
    assert report.evaluated[:i2] == 2
    assert report.violations == []
  end

  # A map of `id => digest` is the load generator's acked CSV. Both columns are
  # evidence, so I2 and I10 can speak on a run that never emits an `append`
  # event — the shape every chaos scenario has.
  test "acked digests make I2 and I10 real without append events" do
    events = [observe([1, 2], 1_000)]

    # id2's stored digest (its id, from `readable/1`) does not match what the
    # client acked, and id9 was never readable at all.
    acked = %{"id1" => "id1", "id2" => "changed", "id9" => "id9"}
    report = Checker.check(events, acked, readable(@three))

    assert report.evaluated[:i2] > 0
    assert report.missing == ["id9"]
    assert {:i2, {:stored_sha_mismatch, "id2"}} in report.violations
  end

  test "an id with no known digest is an ack, not a digest mismatch" do
    events = [observe([1], 1_000)]

    report = Checker.check(events, %{"id1" => nil}, readable(@three))

    assert report.missing == []
    assert report.violations == []
  end

  test "a readable record nobody acked is unattributed" do
    events = [observe([1], 1_000)]

    report = Checker.check(events, %{"id1" => "id1"}, readable(@three))

    assert report.extra == []
    assert Enum.sort(report.unattributed) == ["id2", "id3"]
  end

  test "a readable record nobody acked, committed inside a fault window, is ambiguous" do
    events = [observe([1], 1_000), edge("2xx", "id2", 15_000, 15_020)]

    report =
      Checker.check(events, %{"id1" => "id1"}, readable(@three), quorum_down: [{10_000, 20_000}])

    assert report.extra == ["id2"]
    assert report.unattributed == ["id3"]
  end

  test "an ack with no readable record is missing" do
    events = [edge("2xx", "id9", 1_500, 1_520)]

    report = Checker.check(events, MapSet.new(["id9"]), readable(@three))

    assert report.missing == ["id9"]
    assert Enum.any?(report.violations, &match?({:i1, _}, &1))
  end

  # ── I6, I7, I9 ────────────────────────────────────────────────────────────

  defp put_cursor(name, seq, token, at, result \\ :ok) do
    %{
      client: "pipeline",
      op: {:put_cursor, name, seq, token},
      invoked_at: at,
      completed_at: at,
      result: result
    }
  end

  defp truncate(seq, token, at) do
    %{
      client: "compactor",
      op: {:truncate, seq, token},
      invoked_at: at,
      completed_at: at,
      result: :ok
    }
  end

  defp append(id, seq, at) do
    meta = %{tenant: "t", source: "s", sha256: id, batch_id: "b"}

    %{
      client: "edge",
      op: {:append, id, meta},
      invoked_at: at,
      completed_at: at,
      result: {:ok, seq}
    }
  end

  defp acquire(name, holder, token, at) do
    %{
      client: "c",
      op: {:acquire_lease, name, holder},
      invoked_at: at,
      completed_at: at,
      result: {:ok, token}
    }
  end

  test "a cursor that goes backwards trips I6" do
    events = [put_cursor(:dispatch, 3, 1, 1_000), put_cursor(:dispatch, 1, 2, 2_000)]

    report = Checker.check(events, MapSet.new(), readable(@three))

    assert {:i6, {:cursor_went_backwards, :dispatch, 1, 3}} in report.violations
  end

  test "a cursor past the highest committed seq trips I6" do
    events = [put_cursor(:dispatch, 7, 1, 1_000)]

    report = Checker.check(events, MapSet.new(), readable(@three))

    assert {:i6, {:cursor_past_max_acked, :dispatch, 7, 3}} in report.violations
  end

  test "a cursor that only advances reports nothing for I6" do
    events = [put_cursor(:dispatch, 2, 1, 1_000), put_cursor(:dispatch, 3, 2, 2_000)]

    report = Checker.check(events, MapSet.new(), readable(@three))

    refute Enum.any?(report.violations, &match?({:i6, _}, &1))
  end

  test "a truncate past a reader with no cursor event is a truncation past 0" do
    events = [truncate(2, 1, 1_000)]

    report = Checker.check(events, MapSet.new(), readable(@three))

    assert {:i7, {:truncated_past_a_reader, 2, 0}} in report.violations
  end

  test "a truncate that removes a committed record above it trips I7" do
    events = [
      append("id1", 1, 900),
      append("id2", 3, 950),
      put_cursor(:dispatch, 3, 1, 960),
      truncate(2, 1, 1_000)
    ]

    # id2 (seq 3) was committed but is not readable: the truncate dropped it.
    report = Checker.check(events, MapSet.new(), readable([{"id1", 1}]))

    assert {:i7, {:truncated_above_records, 2, [3]}} in report.violations
  end

  test "a stale-token cursor write trips I9" do
    events = [
      acquire(:dispatch, "a", 1, 1_000),
      acquire(:dispatch, "b", 2, 1_100),
      put_cursor(:dispatch, 5, 1, 1_200)
    ]

    report = Checker.check(events, MapSet.new(), readable(@three))

    assert {:i9, {:accepted_stale_token, {:put_cursor, :dispatch}, 1, 2}} in report.violations
  end
end
