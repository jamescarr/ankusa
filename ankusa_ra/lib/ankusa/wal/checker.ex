defmodule Ankusa.WAL.Checker do
  @moduledoc """
  The invariant checker: one implementation, used by the Level-2 fault drills,
  the Level-3 property suite and the Level-4 chaos harness.

  It takes three things — an event history, the set of ids the edge acked with
  `2xx`, and the records readable once the faults stop — and reports every
  invariant the history violates. It never touches the running system: a
  report is a pure function of the evidence, which is what makes it usable
  after a process has been killed.

  ## Events

  One map per operation a client attempted:

      %{
        client: term(),          # who did it (a pipeline, an edge, a test task)
        op: term(),              # see below
        invoked_at: integer(),   # monotonic-ish ms
        completed_at: integer(),
        result: term()
      }

  | `op` | `result` |
  | --- | --- |
  | `{:append, id, meta}` | `{:ok, seq}` \\| `{:error, reason}` |
  | `{:read, after_seq, limit}` | `[%{seq: seq, id: id, sha256: sha}]` |
  | `{:observe, seqs}` | — (a cursor observer's own progress, for I3) |
  | `{:put_cursor, name, seq, token}` | `:ok` \\| `{:error, :fenced}` |
  | `{:truncate, seq, token}` | `:ok` \\| `{:error, :fenced}` |
  | `{:acquire_lease, name, holder}` | `{:ok, token}` \\| `{:error, :held}` |
  | `{:renew_lease, name, token}` | `:ok` \\| `{:error, :lost}` |
  | `{:release_lease, name, token}` | `:ok` |
  | `{:edge, status, id}` | `2xx` \\| `503` \\| `4xx` \\| `5xx` |

  `meta` is `%{tenant:, source:, dedup_key:, sha256:}` — the sha of the body, so
  a byte-mismatch is detectable without keeping the bodies.

  ## Invariants

  | Id | Invariant |
  | --- | --- |
  | I1 | every `2xx`-acked id is readable from the WAL or from segments |
  | I2 | every record read back matches the sha that was sent, and nothing appears that nobody sent |
  | I3 | a cursor-following reader sees strictly increasing seqs and misses nothing below what it saw |
  | I4 | no seq belongs to two ids; a `batch_id` retry allocates nothing new |
  | I6 | stored cursors never decrease, and a stale-token write never takes effect |

  There is no I5 any more. It read "each `(tenant, source, dedup_key)` has
  exactly one committed seq", which stopped being a property of the log when
  dedup moved off the ack path: the WAL appends every copy it is handed, and
  deciding that a copy is a duplicate of an event already delivered is the
  idempotent receiver's job (`Ankusa.Dispatch.Receiver`). That rule is pinned by
  `Ankusa.DedupStoreTest` and by the deliveries reaching a sink exactly once.
  | I7 | `truncate_through(n)` removes nothing above n |
  | I8 | without quorum, edges answer `503` within `append_timeout_ms + 1000` and never `2xx` |
  | I9 | for each lease name, only the latest token's writes are accepted |
  | I10 | missing deliveries are 0; extra deliveries are reported and attributed |
  """

  @type event :: %{
          client: term(),
          op: term(),
          invoked_at: integer(),
          completed_at: integer(),
          result: term()
        }

  @type report :: %{
          missing: [String.t()],
          extra: [String.t()],
          unattributed: [String.t()],
          violations: [{atom(), term()}],
          evaluated: %{atom() => non_neg_integer()}
        }

  @default_append_timeout_ms 10_000

  @doc """
  Check a history.

  `final_records` is what is readable once everything has settled:
  `[%{id: id, seq: seq, sha256: sha}]` — from the WAL *or* from compacted
  segments, because "readable" is what the Log contract promises.

  Options:

    * `:append_timeout_ms` — the client's command deadline (default
      `#{@default_append_timeout_ms}`), used by I8.
    * `:quorum_down` — `{from_ms, to_ms}` windows during which no quorum
      existed, also I8.
    * `:final_cursors` — `%{name => seq}` as stored at the end, for I6.

  `acked` is the set of ids the edge answered `2xx` for. Pass a `MapSet` of ids,
  or a map of `id => digest` when the caller also has the payload digests — the
  load generator's acked CSV has both, and I2 and I10 need the digests to check
  anything.

  The report's `:evaluated` maps each invariant to how many pieces of evidence
  it inspected, so a caller can tell "clean" from "nothing to look at" (see
  the caller can list them by looking for zeros).
  """
  @spec check([event()], MapSet.t() | %{optional(String.t()) => String.t()}, [map()], keyword()) ::
          report()
  def check(events, acked, final_records, opts \\ []) do
    readable = Map.new(final_records, &{&1.id, &1})
    {acked_ids, acked_digests} = split_acked(acked)

    # What the client believed it sent. The `append` events are the WAL's own
    # record of that; `acked` can carry the same information as `id => digest`
    # when the caller has it — the load generator's CSV does — which is what
    # lets I2 and I10 say anything at all on a run that drives the system over
    # HTTP and so never emits an `append` event. A conflict resolves to the
    # event, because that is the caller's own account of the payload.
    sent = Map.merge(acked_digests, sent_by_id(events))

    {extra, unattributed} = extra_records(events, readable, sent, acked_ids, opts)

    violations =
      i1(acked_ids, readable) ++
        i2(events, readable, sent) ++
        i3(events, readable) ++
        i4(events) ++
        i6(events, readable, opts) ++
        i7(events, readable) ++
        i8(events, opts) ++
        i9(events) ++
        []

    %{
      missing: Enum.reject(MapSet.to_list(acked_ids), &Map.has_key?(readable, &1)),
      extra: extra,
      unattributed: unattributed,
      violations: Enum.uniq(violations),
      evaluated:
        evaluated(
          events,
          %{acked: acked_ids, sent: sent, readable: readable},
          windows(opts),
          Keyword.get(opts, :final_cursors)
        )
    }
  end

  # `acked` is a set of ids, or a map of `id => digest` when the caller also
  # knows what each payload was. An id whose digest is unknown is still an ack —
  # it just cannot be checked against a body, and a `nil` digest entering `sent`
  # would report every readable row for it as a mismatch.
  defp split_acked(%MapSet{} = acked), do: {acked, %{}}

  defp split_acked(acked) when is_map(acked) do
    digests = for {id, sha} <- acked, is_binary(sha), into: %{}, do: {id, sha}
    {MapSet.new(Map.keys(acked)), digests}
  end

  defp windows(opts), do: List.wrap(Keyword.get(opts, :quorum_down, []))

  # How many pieces of evidence each invariant actually inspected. An invariant
  # with nothing to look at cannot fail, and that is exactly how a gate goes
  # quiet without anyone noticing — so the count is part of the report, and a
  # zero is called `not_exercised` rather than passed off as a clean result.
  #
  # The chaos scenarios do not emit `append` events (they drive the edge over
  # HTTP), so I4/I7/I9 are legitimately not exercised there; the Level-2
  # drills emit them and do exercise them.
  defp evaluated(events, %{acked: acked, sent: sent, readable: readable}, windows, final_cursors) do
    appends = Enum.count(events, &match?(%{op: {:append, _, _}}, &1))

    %{
      i1: MapSet.size(acked),
      # The `read` events it walked, plus the stored rows whose digest it could
      # compare against what a client said it sent.
      i2:
        Enum.count(events, &match?(%{op: {:read, _, _}}, &1)) +
          Enum.count(readable, fn {id, _row} -> Map.has_key?(sent, id) end),
      i3:
        Enum.count(events, fn
          %{op: {:read, _, _}} -> true
          %{op: {:observe, _}} -> true
          _ -> false
        end),
      i4: appends,
      i6:
        Enum.count(events, &match?(%{op: {:put_cursor, _, _, _}}, &1)) +
          map_size(final_cursors || %{}),
      i7:
        Enum.count(events, fn
          %{op: {:put_cursor, _, _, _}} -> true
          %{op: {:truncate, _, _}} -> true
          _ -> false
        end),
      i8:
        Enum.count(events, fn
          %{op: {:edge, _, _}, invoked_at: at} ->
            in_window?(at, windows)

          _ ->
            false
        end),
      i9:
        Enum.count(events, fn
          %{op: {:acquire_lease, _, _}} -> true
          %{op: {:release_lease, _, _}} -> true
          %{op: {:put_cursor, _, _, _}, result: :ok} -> true
          %{op: {:truncate, _, _}, result: :ok} -> true
          _ -> false
        end),
      i10: map_size(readable)
    }
  end

  # ── I1 ────────────────────────────────────────────────────────────────────

  defp i1(acked, readable) do
    missing = Enum.reject(MapSet.to_list(acked), &Map.has_key?(readable, &1))

    if missing == [] do
      []
    else
      [{:i1, %{missing: Enum.take(missing, 20), count: length(missing)}}]
    end
  end

  # ── I2 ────────────────────────────────────────────────────────────────────

  defp i2(events, readable, sent) do
    read_violations =
      events
      |> Enum.filter(&match?(%{op: {:read, _, _}}, &1))
      |> Enum.flat_map(fn %{result: records, client: client} ->
        for %{id: id, sha256: sha} <- List.wrap(records) do
          cond do
            not Map.has_key?(sent, id) -> {:i2, {:read_unknown, client, id}}
            Map.fetch!(sent, id) != sha -> {:i2, {:sha_mismatch, client, id}}
            true -> nil
          end
        end
      end)
      |> Enum.reject(&is_nil/1)

    final_violations =
      for {id, %{sha256: sha}} <- readable, Map.has_key?(sent, id), Map.fetch!(sent, id) != sha do
        {:i2, {:stored_sha_mismatch, id}}
      end

    read_violations ++ final_violations
  end

  # ── I3 ────────────────────────────────────────────────────────────────────

  # A cursor-following reader sees strictly increasing seqs and misses nothing
  # below the highest seq it saw. Two shapes feed this: a client's `read` calls
  # (with payloads, so I2 can check them too) and a cursor observer's
  # `observe` calls, which carry seqs only — a read *plan* has no bodies in it,
  # which is exactly why reading the bytes is a separate step.
  defp i3(events, readable) do
    events
    |> Enum.group_by(& &1.client)
    |> Enum.flat_map(fn {client, client_events} ->
      read_seqs =
        for %{op: {:read, _, _}, result: records} <- client_events,
            record <- List.wrap(records),
            do: record.seq

      observed =
        for %{op: {:observe, seqs}} <- client_events, seq <- List.wrap(seqs), do: seq

      order =
        ascending(read_seqs, {:read, client}) ++ ascending(observed, {:observe, client})

      seen = MapSet.new(read_seqs ++ observed)

      missed =
        case seen |> Enum.max(fn -> 0 end) do
          0 ->
            MapSet.new()

          max_seen ->
            readable
            |> Enum.filter(fn {_id, row} -> row.seq <= max_seen end)
            |> Enum.reject(fn {_id, row} -> MapSet.member?(seen, row.seq) end)
            |> Enum.map(fn {id, row} -> {id, row.seq} end)
            |> MapSet.new()
        end

      if MapSet.size(missed) > 0 do
        [{:i3, {:missed_below_last_seen, client, Enum.take(MapSet.to_list(missed), 20)}} | order]
      else
        order
      end
    end)
  end

  defp ascending(seqs, what) do
    if seqs == Enum.sort(seqs) and Enum.uniq(seqs) == seqs do
      []
    else
      [{:i3, {:not_ascending, what, Enum.take(seqs, 50)}}]
    end
  end

  # ── I4 ────────────────────────────────────────────────────────────────────

  defp i4(events) do
    committed = for %{op: {:append, id, _meta}, result: {:ok, seq}} <- events, do: {id, seq}

    shared =
      committed
      |> Enum.group_by(&elem(&1, 1))
      |> Enum.flat_map(fn {seq, pairs} ->
        ids = pairs |> Enum.map(&elem(&1, 0)) |> Enum.uniq()

        if length(ids) > 1, do: [{:i4, {:seq_shared, seq, ids}}], else: []
      end)

    # A retry of the same (batch_id, id) must return the same seq: anything else
    # means the record was committed twice under one client id.
    retried =
      events
      |> Enum.filter(&match?(%{op: {:append, _, %{batch_id: _}}}, &1))
      |> Enum.group_by(fn %{op: {:append, id, meta}} -> {meta.batch_id, id} end)
      |> Enum.flat_map(fn {{batch_id, id}, group} ->
        seqs = for %{result: {:ok, seq}} <- group, do: seq

        if length(Enum.uniq(seqs)) > 1,
          do: [{:i4, {:batch_id_reallocated, batch_id, id, Enum.uniq(seqs)}}],
          else: []
      end)

    shared ++ retried
  end

  # ── I6 ────────────────────────────────────────────────────────────────────

  # Cursors are a maximum, so the *stored* value never decreases: a write that
  # returns `:ok` for a lower seq is legal, but it must not lower the stored
  # value, and a fenced write must not raise it. Replaying the history and
  # folding with `max` reproduces the stored value; comparing it with what was
  # actually stored at the end catches a write that took effect when it should
  # not have.
  #
  # The replay also watches the observations themselves: a cursor that goes
  # backwards (an `:ok` write below the value already stored) or past the
  # highest committed seq (a cursor pointing beyond the end of the log) is a
  # violation even when the drift check is silent, because neither can be a
  # faithful account of what the pipeline consumed.
  defp i6(events, readable, opts) do
    max_acked = readable |> Enum.map(fn {_id, row} -> row.seq end) |> Enum.max(fn -> 0 end)

    {stored, violations} =
      events
      |> Enum.filter(&match?(%{op: {:put_cursor, _, _, _}}, &1))
      |> Enum.sort_by(& &1.invoked_at)
      |> Enum.reduce({%{}, []}, fn %{op: {:put_cursor, name, seq, _token}, result: result},
                                   {stored, violations} ->
        current = Map.get(stored, name, 0)

        violations =
          case result do
            :ok when seq > max_acked ->
              [{:i6, {:cursor_past_max_acked, name, seq, max_acked}} | violations]

            :ok when seq < current ->
              [{:i6, {:cursor_went_backwards, name, seq, current}} | violations]

            _ ->
              violations
          end

        next = if result == :ok, do: max(current, seq), else: current
        {Map.put(stored, name, next), violations}
      end)

    mismatches =
      case Keyword.get(opts, :final_cursors) do
        nil ->
          []

        final ->
          for {name, seq} <- final, Map.get(stored, name, 0) != seq do
            {:i6, {:cursor_drift, name, Map.get(stored, name, 0), seq}}
          end
      end

    violations ++ mismatches
  end

  # ── I7 ────────────────────────────────────────────────────────────────────

  # Truncation is a prefix operation, and the compactor only ever truncates
  # through `min(dispatch, compactor)`: a truncate past what dispatch has
  # consumed would drop a record nobody has delivered yet. A cursor name that
  # never appears in the history is a reader that has not advanced — its cursor
  # is 0, so a truncation past it is a violation too.
  #
  # `readable` is the surviving log (WAL plus segments). After a
  # `truncate_through(n)`, every committed record above `n` must still be
  # readable; one that is not was removed by a truncation that promised to
  # remove nothing above `n`.
  defp i7(events, readable) do
    committed_above =
      for %{op: {:append, _id, _meta}, result: {:ok, seq}} <- events,
          seq > 0,
          into: MapSet.new(),
          do: seq

    readable_seqs = MapSet.new(readable, fn {_id, row} -> row.seq end)

    events
    |> Enum.sort_by(& &1.invoked_at)
    |> Enum.reduce({%{}, []}, fn event, {cursors, violations} ->
      case event do
        %{op: {:put_cursor, name, seq, _token}, result: :ok} ->
          {Map.put(cursors, name, max(Map.get(cursors, name, 0), seq)), violations}

        %{op: {:truncate, seq, _token}, result: :ok} ->
          floor = cursors |> Map.values() |> Enum.min(fn -> 0 end)

          removed_above =
            committed_above
            |> Enum.filter(&(&1 > seq))
            |> Enum.reject(&MapSet.member?(readable_seqs, &1))

          violations =
            cond do
              seq > floor ->
                [{:i7, {:truncated_past_a_reader, seq, floor}} | violations]

              removed_above != [] ->
                [
                  {:i7, {:truncated_above_records, seq, Enum.take(removed_above, 20)}}
                  | violations
                ]

              true ->
                violations
            end

          {cursors, violations}

        _ ->
          {cursors, violations}
      end
    end)
    |> elem(1)
  end

  # ── I8 ────────────────────────────────────────────────────────────────────

  defp i8(events, opts) do
    timeout = Keyword.get(opts, :append_timeout_ms, @default_append_timeout_ms) + 1_000
    windows = List.wrap(Keyword.get(opts, :quorum_down, []))

    events
    |> Enum.filter(&match?(%{op: {:edge, _, _}}, &1))
    |> Enum.flat_map(fn %{op: {:edge, status, id}, invoked_at: at, completed_at: done} ->
      cond do
        # An ack is the response, so it is the *completion* that has to fall
        # inside the outage. A request invoked as the member was killed and
        # completed once it was back was answered legitimately — the adapter
        # keeps retrying for `append_timeout_ms` — and judging it on
        # `invoked_at` alone reported 35 honest acks in a real power-loss run.
        status == "2xx" and acked_in_window?(at, done, windows) ->
          [{:i8, {:acked_without_quorum, id, at, done}}]

        status == "2xx" ->
          []

        in_window?(at, windows) and status != "503" and deadline_in_outage?(at + timeout, windows) ->
          [{:i8, {:not_503, id, status}}]

        in_window?(at, windows) and done - at > timeout and
            deadline_in_outage?(at + timeout, windows) ->
          [{:i8, {:too_slow, id, done - at, timeout}}]

        true ->
          []
      end
    end)
  end

  # How far inside a window an event has to be before the window can convict it.
  # Both stamps are coarser than the truth: the window opens *before* the nemesis
  # applies the fault (kill, partition, fill), so the cluster is briefly still
  # healthy, and it closes after a once-a-second poll notices a leader, so the
  # cluster is briefly healthy again. Inside a second of either edge an ack can be
  # honest and still read as in-window. The cost is that a violation in the first
  # or last second of an outage goes unreported, which is the right way for a
  # nightly gate to be wrong.
  @window_grace_ms 1_000

  # The bound only says something when the outage outlasted it. A request whose
  # deadline falls after quorum returned may legitimately be held and committed —
  # that is I8's second sentence, "after quorum returns, 2xx resumes within 2 ×
  # the election timeout plus the client retry backoff" — so a load generator
  # that gave up on its own 10s timeout during a 5s outage is not evidence that
  # the edge failed to shed.
  defp deadline_in_outage?(deadline, windows) do
    Enum.any?(windows!(windows), fn {_from, to} -> deadline <= to - @window_grace_ms end)
  end

  # Both ends of the request inside a window: it was invoked while quorum was
  # plausibly gone *and* answered while it still was. An ack completing after the
  # window closed means the cluster was answering again by then — the nemesis
  # closes a window only once a leader is back.
  #
  # The grace applies at both edges because the stamps are coarser than the
  # truth: the window opens before the fault is applied and closes after a
  # once-a-second poll notices a leader. Within a second of either edge an ack
  # can be honest while still reading as in-window. Erring this way leaves a
  # violation in the first or last second of an outage unreported, which is the
  # right way for a nightly gate to be wrong.
  defp acked_in_window?(_at, _done, []), do: false

  defp acked_in_window?(at, done, windows) do
    Enum.any?(windows!(windows), fn {from, to} ->
      at >= from + @window_grace_ms and done <= to - @window_grace_ms
    end)
  end

  defp in_window?(_at, []), do: false

  defp in_window?(at, windows) do
    Enum.any?(windows!(windows), fn {from, to} ->
      at >= from + @window_grace_ms and at <= to
    end)
  end

  # One validation point for both window predicates: a window the checker cannot
  # read must fail the run, because comparing against `nil` would put every event
  # outside every window and I8 would check nothing at all.
  defp windows!(windows) do
    Enum.map(windows, fn
      {from, to} when is_integer(from) and is_integer(to) ->
        {from, to}

      other ->
        raise ArgumentError,
              "quorum_down window must be {from_ms, to_ms}, got: #{inspect(other)}"
    end)
  end

  # ── I9 ────────────────────────────────────────────────────────────────────

  # Only the newest token for a lease name may take effect. Replaying the
  # acquire/release history gives the live token at each write; a write that was
  # accepted with any other token is a zombie that got through.
  defp i9(events) do
    events
    |> Enum.sort_by(& &1.invoked_at)
    |> Enum.reduce({%{}, []}, fn event, {live, violations} ->
      case event do
        # The token is allocated by the *cluster* and comes back in the reply,
        # so that is where the live token is read from — never from the request.
        %{op: {:acquire_lease, name, _holder}, result: {:ok, token}} ->
          {Map.put(live, name, token), violations}

        %{op: {:release_lease, name, _token}, result: :ok} ->
          {Map.delete(live, name), violations}

        %{op: {:put_cursor, name, _seq, token}, result: :ok} ->
          stale(live, name, token, {:put_cursor, name}, violations)

        %{op: {:truncate, _seq, token}, result: :ok} ->
          stale(live, :storage, token, :truncate, violations)

        _ ->
          {live, violations}
      end
    end)
    |> elem(1)
  end

  defp stale(live, name, token, what, violations) do
    case Map.get(live, name) do
      ^token -> {live, violations}
      other -> {live, [{:i9, {:accepted_stale_token, what, token, other}} | violations]}
    end
  end

  # ── I10 ───────────────────────────────────────────────────────────────────

  # A readable record nobody sent and nobody acked is `extra`. It splits two
  # ways. A record the edge answered `2xx` for *inside* an outage window is an
  # ambiguous commit: the ack may simply have been lost when quorum went away,
  # so it is reported but not held against the run. Anything else — a record
  # that appeared with no client operation at all, inside no outage — is
  # unattributed, and that fails the gate.
  defp extra_records(events, readable, sent, acked_ids, opts) do
    windows = windows(opts)

    {ambiguous, unattributed} =
      for {id, _row} <- readable,
          not Map.has_key?(sent, id),
          not MapSet.member?(acked_ids, id),
          reduce: {[], []} do
        {ambiguous, unattributed} ->
          if committed_in_window?(id, events, windows) do
            {[id | ambiguous], unattributed}
          else
            {ambiguous, [id | unattributed]}
          end
      end

    {Enum.sort(ambiguous), Enum.sort(unattributed)}
  end

  defp committed_in_window?(_id, _events, []), do: false

  defp committed_in_window?(id, events, windows) do
    Enum.any?(events, fn
      %{op: {:edge, "2xx", ^id}, completed_at: done} -> in_window?(done, windows)
      _ -> false
    end)
  end

  # ── shared ────────────────────────────────────────────────────────────────

  defp sent_by_id(events) do
    for %{op: {:append, id, meta}} <- events, into: %{}, do: {id, meta.sha256}
  end
end
