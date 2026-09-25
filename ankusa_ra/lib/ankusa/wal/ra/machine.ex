defmodule Ankusa.WAL.Ra.Machine do
  @moduledoc """
  The `:ra_machine` behind `Ankusa.WAL.Ra`: a replicated, ordered log of
  envelopes plus the cursors, dedup ledger and leases that make it usable as a
  shared WAL.

  Everything that must be identical on every replica lives here: seq
  allocation, dedup, cursors, the truncation floor and the leases. The adapter
  (`Ankusa.WAL.Ra`) is only bootstrap, command shaping and read plumbing — the
  rules are in this module, and `apply/3` is a pure function of
  `(command, meta, state)`.

  ## Why the log holds no payloads

  The raft log entry carries each record's envelope bytes, and the machine state
  stores only *where* that entry is: `entries` maps a `seq` to
  `{raft_index, position_in_command, payload_size}`. So a Ra snapshot contains
  sequence numbers and bookkeeping, not megabytes of webhook bodies — snapshot
  size is independent of payload size. Readers pull the bytes straight out of the
  log through `ra_server_proc:read_entries/4`, using the index Ra would otherwise
  drop; `live_indexes/1` is what stops Ra dropping the entries still holding live
  (un-truncated) records.

  ## Commands

  | Command | Reply |
  | --- | --- |
  | `{:append, batch_id, records}` | `{:ok, [{:committed, seq} \\| {:duplicate, seq}]}` |
  | `{:put_cursor, name, seq, token}` | `:ok` \\| `{:error, :fenced}` |
  | `{:truncate_through, seq, token}` | `:ok` \\| `{:error, :fenced}` |
  | `{:acquire_lease, name, holder, ttl_ms}` | `{:ok, lease}` \\| `{:error, {:held, holder}}` |
  | `{:renew_lease, name, holder, token, ttl_ms}` | `{:ok, lease}` \\| `{:error, :lost}` |
  | `{:release_lease, name, holder, token}` | `:ok` |
  | `{:import, floor, cursors}` | `:ok` |
  | `{:import_dedup, pairs}` | `{:ok, inserted}` |

  `records` are `{event_id, tenant_id, source_id, dedup_key | nil, envelope}`,
  encoded by the adapter so the machine never has to know the envelope format.

  ## Idempotent appends

  `batch_id` is the whole point of the append command. A client that times out
  does not know whether its command applied; re-sending it with the same
  `batch_id` returns the *stored* results and allocates nothing. Without it, a
  leader change or a lost reply would double-allocate seqs for records that were
  already committed.

  ## Determinism and time

  `apply/3` takes time only from `meta.system_time`, the wall-clock stamp Ra
  puts on the log entry. Every replica therefore computes the same expiry for a
  lease. `config.time_offset_ms` shifts the cluster's *view* of that clock
  uniformly (it is a per-cluster setting, not a per-member one — a member-local
  offset would make the replicas diverge); it exists so the clock-skew fault
  tests can drive a skewed clock through the real code path.
  """

  @behaviour :ra_machine

  alias Ankusa.WAL

  @batch_retention_ms 600_000
  @max_batches 1024
  # How far the truncation floor must advance before the machine asks Ra to take
  # a snapshot and drop log segments. Small enough that a long run reclaims disk
  # steadily, large enough that it is not happening on every truncate.
  @release_interval 10_000

  # ── ra_machine ────────────────────────────────────────────────────────────

  @impl true
  def init(config) do
    %{
      next_seq: Map.get(config, :next_seq, 1),
      # seq => {raft_index, position within that command, payload size}
      entries: :gb_trees.empty(),
      # raft_index => how many of its records are still live
      live: %{},
      # sum of the payload sizes still live (not the whole log)
      bytes: 0,
      dedup: %{},
      batches: %{},
      batch_order: :queue.new(),
      cursors: %{},
      leases: %{},
      floor: 0,
      released_floor: 0,
      time_offset_ms: Map.get(config, :time_offset_ms, 0)
    }
  end

  @impl true
  def version, do: 1

  # Ra asks for a machine version before one has been recorded on a fresh log
  # (`0`), and for the latest version afterwards. This module implements both:
  # there is one machine, and `version/0` is what advertises it.
  @impl true
  def which_module(_version), do: __MODULE__

  @impl true
  def live_indexes(state), do: :ra_seq.from_list(Map.keys(state.live))

  @impl true
  def overview(state) do
    {min_seq, max_seq} = seq_bounds(state.entries)

    %{
      records: :gb_trees.size(state.entries),
      bytes: state.bytes,
      next_seq: state.next_seq,
      floor: state.floor,
      min_seq: min_seq,
      max_seq: max_seq,
      dedup_keys: map_size(state.dedup),
      cursors: state.cursors,
      leases: state.leases
    }
  end

  @impl true
  def init_aux(_name), do: %{}

  # Aux queries run on the leader and see everything it has applied. They exist
  # so a read can be planned (`{:read_plan, …}`) before the bytes are pulled out
  # of the log, and so `get_cursor/2` and `stats/1` never have to wait for a
  # write to be applied locally.
  #
  # Ra also sends this module cast-shaped aux events (a periodic `:tick`); they
  # have nothing to do with the WAL, but they must be handled — an unhandled one
  # takes the whole Raft server down.
  @impl true
  def handle_aux(_raft_state, :cast, _command, aux, internal) do
    {:no_reply, aux, internal}
  end

  def handle_aux(_raft_state, {_call, _from}, command, aux, internal) do
    state = :ra_aux.machine_state(internal)

    reply =
      case command do
        {:read_plan, after_seq, limit} -> read_plan(state, after_seq, limit)
        {:cursor, name} -> Map.get(state.cursors, name, 0)
        :overview -> overview(state)
      end

    {:reply, reply, aux, internal}
  end

  @impl true
  def apply(meta, {:append, batch_id, records}, state) do
    case Map.fetch(state.batches, batch_id) do
      {:ok, results} ->
        # A retry of a command whose reply was lost. Return the recorded
        # results; allocate nothing, apply nothing.
        {state, {:ok, results}, []}

      :error ->
        append(meta, batch_id, records, state)
    end
  end

  # Ra applies the machine-version bump through `apply/3` as a user command
  # (`{machine_version, From, To}`), so it must be handled even though it carries
  # no WAL meaning: an unhandled command takes the whole Raft server down, and
  # this one is applied by the first `noop` a new leader writes.
  def apply(_meta, {:machine_version, _from, _to}, state), do: {state, :ok, []}

  def apply(meta, {:put_cursor, name, seq, token}, state) do
    if fenced?(state, WAL.lease_for_cursor(name), token, meta) do
      {state, {:error, :fenced}, []}
    else
      # Cursors are monotonic: a write can never move one backwards.
      cursors = Map.update(state.cursors, name, seq, &max(&1, seq))
      {%{state | cursors: cursors}, :ok, []}
    end
  end

  def apply(meta, {:truncate_through, seq, token}, state) do
    if fenced?(state, :storage, token, meta) do
      {state, {:error, :fenced}, []}
    else
      truncate(meta, seq, state)
    end
  end

  def apply(meta, {:acquire_lease, name, holder, ttl_ms}, state) do
    now = now(meta, state)

    case Map.get(state.leases, name) do
      %{expires_at: expires_at, holder: existing}
      when is_integer(expires_at) and expires_at >= now and existing != holder ->
        {state, {:error, {:held, existing}}, []}

      existing ->
        # Always a fresh token, even for the same holder and even for a released
        # (expired) lease: the counter only ever climbs.
        token = ((existing && existing.token) || 0) + 1
        lease = %{holder: holder, token: token, expires_at: now + ttl_ms}
        state = %{state | leases: Map.put(state.leases, name, lease)}
        {state, {:ok, lease(name, holder, token, ttl_ms, lease)}, []}
    end
  end

  def apply(meta, {:renew_lease, name, holder, token, ttl_ms}, state) do
    now = now(meta, state)

    case Map.get(state.leases, name) do
      %{holder: ^holder, token: ^token, expires_at: expires_at}
      when is_integer(expires_at) and expires_at >= now ->
        lease = %{holder: holder, token: token, expires_at: now + ttl_ms}
        state = %{state | leases: Map.put(state.leases, name, lease)}
        {state, {:ok, lease(name, holder, token, ttl_ms, lease)}, []}

      _ ->
        {state, {:error, :lost}, []}
    end
  end

  def apply(_meta, {:release_lease, name, holder, token}, state) do
    case Map.get(state.leases, name) do
      %{holder: ^holder, token: ^token} ->
        # Expired, not deleted: the token counter must survive so the next
        # holder cannot reuse this one's token.
        leases = Map.put(state.leases, name, %{holder: holder, token: token, expires_at: nil})
        {%{state | leases: leases}, :ok, []}

      _ ->
        {state, :ok, []}
    end
  end

  def apply(_meta, {:import, floor, cursors}, state) do
    state = %{state | next_seq: floor + 1, floor: floor, released_floor: floor}

    cursors =
      Enum.reduce(cursors, state.cursors, fn {name, seq}, acc ->
        Map.update(acc, name, seq, &max(&1, seq))
      end)

    {%{state | cursors: cursors}, :ok, []}
  end

  def apply(_meta, {:import_dedup, pairs}, state) do
    {dedup, inserted} =
      Enum.reduce(pairs, {state.dedup, 0}, fn {key, seq}, {acc, n} ->
        if Map.has_key?(acc, key), do: {acc, n}, else: {Map.put(acc, key, seq), n + 1}
      end)

    {%{state | dedup: dedup}, {:ok, inserted}, []}
  end

  # ── append ────────────────────────────────────────────────────────────────

  defp append(meta, batch_id, records, state) do
    {results, state} =
      records
      |> Enum.with_index(1)
      |> Enum.reduce({[], state}, fn {record, pos}, {acc, st} ->
        {result, st} = commit(record, meta.index, pos, st)
        {[result | acc], st}
      end)

    results = Enum.reverse(results)
    now = now(meta, state)

    state = %{
      state
      | batches: Map.put(state.batches, batch_id, results),
        batch_order: :queue.in({now, batch_id}, state.batch_order)
    }

    {prune_batches(state, now), {:ok, results}, []}
  end

  defp commit({_event_id, tenant_id, source_id, dedup_key, envelope}, index, pos, state) do
    key = if dedup_key, do: {tenant_id, source_id, dedup_key}, else: nil

    case key && Map.get(state.dedup, key) do
      nil ->
        seq = state.next_seq

        state = %{
          state
          | next_seq: seq + 1,
            entries: :gb_trees.insert(seq, {index, pos, byte_size(envelope)}, state.entries),
            live: Map.update(state.live, index, 1, &(&1 + 1)),
            bytes: state.bytes + byte_size(envelope),
            dedup: if(key, do: Map.put(state.dedup, key, seq), else: state.dedup)
        }

        {{:committed, seq}, state}

      existing ->
        {{:duplicate, existing}, state}
    end
  end

  # The batch table is a cache of replies, not state: drop the oldest entries
  # once it is large, or once they are old enough that a client can no longer be
  # retrying.
  defp prune_batches(state, now) do
    if map_size(state.batches) > @max_batches or expired_head?(state.batch_order, now) do
      case :queue.out(state.batch_order) do
        {{:value, {_ts, batch_id}}, order} ->
          state = %{state | batches: Map.delete(state.batches, batch_id), batch_order: order}
          prune_batches(state, now)

        {:empty, _order} ->
          state
      end
    else
      state
    end
  end

  defp expired_head?(order, now) do
    case :queue.peek(order) do
      {:value, {ts, _batch_id}} -> now - ts > @batch_retention_ms
      :empty -> false
    end
  end

  # ── truncation ────────────────────────────────────────────────────────────

  defp truncate(meta, seq, state) do
    {entries, live, bytes, removed} = drop_through(seq, state.entries, state.live, state.bytes, 0)

    # The floor is the highest seq that has actually been handed out, never
    # more: a truncate past `next_seq` would otherwise leave the machine about
    # to re-issue a seq it has already declared gone, and a reader sitting at
    # that cursor would never see the new record.
    floor = min(max(state.floor, seq), state.next_seq - 1)
    state = %{state | entries: entries, live: live, bytes: bytes, floor: floor}

    cond do
      removed == 0 ->
        {state, :ok, []}

      floor - state.released_floor >= @release_interval or :gb_trees.is_empty(entries) ->
        # Ask Ra to snapshot here and drop the log segments nothing live points
        # into any more. `live_indexes/1` keeps the entries that still hold
        # un-truncated records.
        state = %{state | released_floor: floor}
        {state, :ok, [{:release_cursor, meta.index, state}]}

      true ->
        {state, :ok, []}
    end
  end

  defp drop_through(seq, entries, live, bytes, removed) do
    if :gb_trees.is_empty(entries) do
      {entries, live, bytes, removed}
    else
      {key, _value} = :gb_trees.smallest(entries)

      if key <= seq do
        {_key, {index, _pos, size}, rest} = :gb_trees.take_smallest(entries)

        live =
          case Map.get(live, index) do
            1 -> Map.delete(live, index)
            n -> Map.put(live, index, n - 1)
          end

        drop_through(seq, rest, live, bytes - size, removed + 1)
      else
        {entries, live, bytes, removed}
      end
    end
  end

  # ── leases ────────────────────────────────────────────────────────────────

  defp fenced?(state, lease_name, token, meta) do
    now = now(meta, state)

    case Map.get(state.leases, lease_name) do
      %{token: ^token, expires_at: expires_at}
      when is_integer(expires_at) and expires_at >= now ->
        false

      _ ->
        true
    end
  end

  defp lease(name, holder, token, ttl_ms, %{expires_at: expires_at}) do
    %{name: name, holder: holder, token: token, ttl_ms: ttl_ms, expires_at: expires_at}
  end

  # Run this after the state update, so the reply and the ledger agree.
  defp now(meta, state), do: meta.system_time + state.time_offset_ms

  # ── reads ─────────────────────────────────────────────────────────────────

  defp read_plan(state, after_seq, limit) do
    (after_seq + 1)
    |> :gb_trees.iterator_from(state.entries)
    |> collect_plan(limit, [])
  end

  defp collect_plan(_iter, 0, acc), do: Enum.reverse(acc)

  defp collect_plan(iter, remaining, acc) do
    case :gb_trees.next(iter) do
      :none ->
        Enum.reverse(acc)

      {seq, {index, pos, _size}, next} ->
        collect_plan(next, remaining - 1, [{seq, index, pos} | acc])
    end
  end

  defp seq_bounds(entries) do
    if :gb_trees.is_empty(entries) do
      {nil, nil}
    else
      {key, _value} = :gb_trees.smallest(entries)
      {last_key, _last_value} = :gb_trees.largest(entries)
      {key, last_key}
    end
  end
end
