defmodule Ankusa.Intake.Pending do
  @moduledoc """
  The pending set of `wal: :dual` (ADR 0002, section 5): one entry per record not
  yet confirmed in **both** the local store and the intake log, and the small
  state the dual commit shares between the writer, the reconciler, the breaker
  and readiness.

  One public ETS `ordered_set` per instance, created by `Ankusa.Instance` (so it
  outlives the processes that use it). Its keys, by tag:

    * `{:p, seq}` — the entry (see `@pos_*`); in flight until its batch's
      publish returns, *carried* afterwards if a half failed
    * `{:id, id}` — seq of a carried entry, for `Ankusa.Intake.Reconciler.hold/3`
    * `{:k, {tenant, source, dedupe_key}}` — seq of a keyed entry: a retry that
      meets it is never answered `201` (ADR section 3, rule 2)
    * `{:b, batch_id}` — the ops of a batch whose local half is missing, to
      re-apply
    * `{:c, next_at, seq}` — the reconciler's schedule
    * `{:h, position, seq}` — carried entries whose record is in the log but not
      the local store: the offset never passes the lowest (section 5)
    * `{:meta, name}` — counters and flags (`:records`, `:bytes`, `:carried`,
      `:stuck`, `:confirmed_max`, `:state`, `:partition`, `:flushed_at`)

  Entries are small tuples updated in place (`:ets.update_element/3`), so the
  writer, the reconciler and `hold/3` race safely: a completed entry is removed
  with one atomic `:ets.take/2`.
  """

  alias Ankusa.Config

  # Entry tuple positions: `{key, id, size, local, log, first_at, bin, batch,
  # claim, triple, position, attempts, stuck, next_at, meta}`.
  @pos_local 4
  @pos_log 5
  @pos_bin 7
  @pos_claim 9
  @pos_position 11
  @pos_attempts 12
  @pos_stuck 13
  @pos_next_at 14

  @type table :: atom()
  @type entry :: tuple()

  @doc "The table's name (the instance name is operator config, never request input)."
  # sobelow_skip ["DOS.BinToAtom"]
  @spec table(atom()) :: table()
  def table(instance), do: :"ankusa_intake_#{instance}"

  @doc "Creates the instance's table. Called from the instance supervisor's `init/1`."
  @spec create(atom()) :: table()
  def create(instance) do
    t = table(instance)

    if :ets.whereis(t) == :undefined do
      :ets.new(t, [:ordered_set, :public, :named_table, read_concurrency: true])
    end

    for name <- [:records, :bytes, :carried, :stuck], do: :ets.insert(t, {{:meta, name}, 0})
    :ets.insert(t, {{:meta, :state}, :up})
    t
  end

  @doc "Whether the instance has a table (it runs `wal: :dual`)."
  @spec exists?(atom()) :: boolean()
  def exists?(instance), do: :ets.whereis(table(instance)) != :undefined

  # ── meta ──────────────────────────────────────────────────────────────────

  @spec put_meta(table(), atom(), term()) :: true
  def put_meta(t, name, value), do: :ets.insert(t, {{:meta, name}, value})

  @spec meta(table(), atom(), term()) :: term()
  def meta(t, name, default \\ nil) do
    case :ets.lookup(t, {:meta, name}) do
      [{_, value}] -> value
      [] -> default
    end
  end

  @doc "`:recovering` while boot recovery runs, `:up` otherwise."
  @spec recovering?(table()) :: boolean()
  def recovering?(t), do: meta(t, :state) == :recovering

  @doc "The partition this node writes, or `nil` (adapter without one, or no dual boot yet)."
  @spec partition(atom()) :: non_neg_integer() | nil
  def partition(instance) do
    t = table(instance)
    if :ets.whereis(t) == :undefined, do: nil, else: meta(t, :partition)
  end

  # ── admission ─────────────────────────────────────────────────────────────

  @doc "Whether `count` more records of `bytes` fit under the pending ceilings."
  @spec room?(table(), Config.t(), non_neg_integer(), non_neg_integer()) :: boolean()
  def room?(t, %Config{intake: intake}, count, bytes) do
    meta(t, :records, 0) + count <= intake.pending.max_records and
      meta(t, :bytes, 0) + bytes <= intake.pending.max_bytes
  end

  @doc "Whether an unconfirmed record holds this `{tenant, source, dedupe_key}`."
  @spec keyed?(table(), {String.t(), String.t(), String.t()}) :: boolean()
  def keyed?(t, triple), do: :ets.member(t, {:k, triple})

  @doc """
  Enters the records of one batch before either half is touched. `records` are
  `{seq, id, size, triple | nil, meta}`.
  """
  @spec enter(table(), [tuple()], term(), integer()) :: :ok
  def enter(t, records, batch_id, now) do
    rows =
      Enum.flat_map(records, fn {seq, id, size, triple, rmeta} ->
        entry =
          {{:p, seq}, id, size, :missing, :missing, now, nil, batch_id, nil, triple, nil, 0,
           false, :infinity, rmeta}

        if triple == nil, do: [entry], else: [entry, {{:k, triple}, seq}]
      end)

    :ets.insert(t, rows)
    :ets.update_counter(t, {:meta, :records}, length(records))
    :ets.update_counter(t, {:meta, :bytes}, Enum.sum(Enum.map(records, &elem(&1, 2))))
    :ok
  end

  @doc """
  Both halves of the batch's records are confirmed: remove them and advance the
  highest confirmed position.
  """
  @spec confirm(table(), [non_neg_integer()], non_neg_integer() | nil) :: :ok
  def confirm(t, seqs, position) do
    Enum.each(seqs, fn seq ->
      with [entry] <- :ets.take(t, {:p, seq}), do: forget(t, entry)
    end)

    bump_confirmed(t, position)
  end

  # ── carrying ──────────────────────────────────────────────────────────────

  @doc """
  A half of the batch failed: the records stay, carried. `local` and `log` are
  `:ok | :missing` for the whole batch; `records` are `{seq, bin, meta}` (kept
  while the log half is missing), `batch` is `%{id, ops, probe_seq, seqs, at}`
  (kept while the local half is missing).
  """
  @spec carry(
          table(),
          %{local: atom(), log: atom(), position: integer() | nil},
          [tuple()],
          map(),
          integer()
        ) ::
          :ok
  def carry(t, %{local: local, log: log, position: position}, records, batch, now) do
    if local == :missing do
      :ets.insert(
        t,
        {{:b, batch.id}, batch.ops, batch.probe_seq, batch.seqs, batch.at}
      )
    end

    Enum.each(records, fn {seq, bin, _meta} ->
      case :ets.lookup(t, {:p, seq}) do
        [entry] ->
          id = elem(entry, 1)

          :ets.update_element(t, {:p, seq}, [
            {@pos_local, local},
            {@pos_log, log},
            {@pos_bin, if(log == :missing, do: bin, else: nil)},
            {@pos_position, position},
            {@pos_next_at, now}
          ])

          :ets.insert(t, [{{:id, id}, seq}, {{:c, now, seq}}])
          :ets.update_counter(t, {:meta, :carried}, 1)

          if log == :ok and local == :missing,
            do: :ets.insert(t, {{:h, position, seq}})

        [] ->
          :ok
      end
    end)

    :ok
  end

  @doc "Carried entries due at or before `now`, oldest schedule first (at most `limit`)."
  @spec due(table(), integer(), pos_integer()) :: [non_neg_integer()]
  def due(t, now, limit) do
    case :ets.select(t, [{{{:c, :"$1", :"$2"}}, [{:"=<", :"$1", now}], [:"$2"]}], limit) do
      {seqs, _continuation} -> seqs
      :"$end_of_table" -> []
    end
  end

  @doc "Every carried seq (the reconciler's stuck sweep)."
  @spec carried(table()) :: [non_neg_integer()]
  def carried(t), do: :ets.select(t, [{{{:c, :_, :"$1"}}, [], [:"$1"]}])

  @spec lookup(table(), non_neg_integer()) :: entry() | nil
  def lookup(t, seq) do
    case :ets.lookup(t, {:p, seq}) do
      [entry] -> entry
      [] -> nil
    end
  end

  @doc "Schedules the next attempt of a carried entry."
  @spec reschedule(table(), non_neg_integer(), integer(), boolean()) :: :ok
  def reschedule(t, seq, next_at, count_attempt? \\ true) do
    case lookup(t, seq) do
      nil ->
        :ok

      entry ->
        :ets.delete(t, {:c, elem(entry, @pos_next_at - 1), seq})
        :ets.insert(t, {{:c, next_at, seq}})
        :ets.update_element(t, {:p, seq}, {@pos_next_at, next_at})
        if count_attempt?, do: :ets.update_counter(t, {:p, seq}, {@pos_attempts, 1})
        :ok
    end
  end

  @doc "Marks an entry stuck; `true` when it was not already."
  @spec mark_stuck(table(), non_neg_integer()) :: boolean()
  def mark_stuck(t, seq) do
    case lookup(t, seq) do
      nil ->
        false

      entry ->
        if elem(entry, @pos_stuck - 1) do
          false
        else
          :ets.update_element(t, {:p, seq}, {@pos_stuck, true})
          :ets.update_counter(t, {:meta, :stuck}, 1)
          true
        end
    end
  end

  @doc "The claim a stuck entry held, cleared so it is released once."
  @spec take_claim(table(), non_neg_integer()) :: term()
  def take_claim(t, seq) do
    case lookup(t, seq) do
      nil ->
        nil

      entry ->
        claim = elem(entry, @pos_claim - 1)
        if claim != nil, do: :ets.update_element(t, {:p, seq}, {@pos_claim, nil})
        claim
    end
  end

  @doc "The batch row (ops to re-apply) of a batch, if its local half is still missing."
  @spec batch(table(), term()) :: map() | nil
  def batch(t, id) do
    case :ets.lookup(t, {:b, id}) do
      [{_, ops, probe_seq, seqs, at}] ->
        %{id: id, ops: ops, probe_seq: probe_seq, seqs: seqs, at: at}

      [] ->
        nil
    end
  end

  @doc """
  The local half of a batch is in the store. Returns the entries that this
  completed (their log half was already confirmed).
  """
  @spec local_done(table(), term()) :: [entry()]
  def local_done(t, batch_id) do
    case batch(t, batch_id) do
      nil ->
        []

      %{seqs: seqs} ->
        :ets.delete(t, {:b, batch_id})

        Enum.flat_map(seqs, fn seq ->
          case lookup(t, seq) do
            nil ->
              []

            entry ->
              position = elem(entry, @pos_position - 1)
              :ets.update_element(t, {:p, seq}, {@pos_local, :ok})
              :ets.delete(t, {:h, position, seq})
              finish(t, seq)
          end
        end)
    end
  end

  @doc "The log half of one entry is confirmed at `position`; the entries it completed."
  @spec log_done(table(), non_neg_integer(), non_neg_integer()) :: [entry()]
  def log_done(t, seq, position) do
    case lookup(t, seq) do
      nil ->
        []

      entry ->
        :ets.update_element(t, {:p, seq}, [
          {@pos_log, :ok},
          {@pos_bin, nil},
          {@pos_position, position}
        ])

        if elem(entry, @pos_local - 1) == :missing,
          do: :ets.insert(t, {{:h, position, seq}})

        finish(t, seq)
    end
  end

  # Removes the entry when both halves are confirmed. One atomic `take`: the
  # writer, the reconciler and `hold/3` may all arrive here.
  defp finish(t, seq) do
    case :ets.lookup(t, {:p, seq}) do
      [entry] when elem(entry, @pos_local - 1) == :ok and elem(entry, @pos_log - 1) == :ok ->
        case :ets.take(t, {:p, seq}) do
          [taken] ->
            forget(t, taken)
            bump_confirmed(t, elem(taken, @pos_position - 1))
            [taken]

          [] ->
            []
        end

      _ ->
        []
    end
  end

  # Drops what indexes an entry that is gone, and gives its share back.
  defp forget(t, entry) do
    {{:p, seq}, id, size, _local, _log, _first_at, _bin, _batch, _claim, triple, _position,
     _attempts, stuck, next_at, _meta} = entry

    :ets.delete(t, {:id, id})
    if triple != nil, do: :ets.delete(t, {:k, triple})

    if next_at != :infinity do
      :ets.delete(t, {:c, next_at, seq})
      :ets.update_counter(t, {:meta, :carried}, -1)
    end

    if stuck, do: :ets.update_counter(t, {:meta, :stuck}, -1)
    :ets.update_counter(t, {:meta, :records}, -1)
    :ets.update_counter(t, {:meta, :bytes}, -size)
    :ok
  end

  # ── hold ──────────────────────────────────────────────────────────────────

  @doc """
  Lets the reconciler settle `claim` once both halves of hook `id` are
  confirmed. `:completed` when it already finished (the caller promotes),
  `:stuck` when it has been past `reconcile_deadline_ms` (the caller releases).
  """
  @spec hold(table(), String.t(), term()) :: :held | :completed | :stuck
  def hold(t, id, claim) do
    with [{_, seq}] <- :ets.lookup(t, {:id, id}),
         [entry] <- :ets.lookup(t, {:p, seq}) do
      if elem(entry, @pos_stuck - 1) do
        :stuck
      else
        if :ets.update_element(t, {:p, seq}, {@pos_claim, claim}) do
          # `mark_stuck/2` may have run between the read and the update.
          case lookup(t, seq) do
            nil -> :completed
            now -> if elem(now, @pos_stuck - 1), do: :stuck, else: :held
          end
        else
          :completed
        end
      end
    else
      _gone -> :completed
    end
  end

  # ── watermark, offset, gauges ─────────────────────────────────────────────

  @doc "The seq below which every record is confirmed in both places (ADR section 5)."
  @spec watermark(table(), pos_integer()) :: non_neg_integer()
  def watermark(t, next_seq) do
    case :ets.next(t, {:p, 0}) do
      {:p, seq} -> seq - 1
      _end -> next_seq - 1
    end
  end

  @doc """
  The highest log position whose record is also in the local store: the highest
  confirmed position, held below the lowest record that is only in the log.
  `nil` when none is known.
  """
  @spec offset(table()) :: non_neg_integer() | nil
  def offset(t) do
    confirmed = meta(t, :confirmed_max)

    case {confirmed, :ets.next(t, {:h, -1, 0})} do
      {nil, _} -> nil
      {confirmed, {:h, position, _seq}} -> clamp(min(confirmed, position - 1))
      {confirmed, _end} -> confirmed
    end
  end

  defp clamp(position) when position < 0, do: nil
  defp clamp(position), do: position

  @doc "Raises the highest confirmed position (never lowers it)."
  @spec bump_confirmed(table(), non_neg_integer() | nil) :: :ok
  def bump_confirmed(_t, nil), do: :ok

  def bump_confirmed(t, position) do
    case meta(t, :confirmed_max) do
      nil ->
        # insert_new keeps a concurrent higher value
        unless :ets.insert_new(t, {{:meta, :confirmed_max}, position}),
          do: bump_confirmed(t, position)

      current when current >= position ->
        :ok

      current ->
        :ets.select_replace(t, [
          {{{:meta, :confirmed_max}, current}, [], [{{{{:meta, :confirmed_max}}, position}}]}
        ])

        bump_confirmed(t, position)
    end
  end

  @doc "Counts for the gauges and `GET /v1/wal`."
  @spec stats(table(), integer()) :: map()
  def stats(t, now) do
    oldest =
      case :ets.next(t, {:p, 0}) do
        {:p, seq} ->
          case lookup(t, seq) do
            nil -> 0
            entry -> max(0, now - elem(entry, 5))
          end

        _end ->
          0
      end

    %{
      records: meta(t, :records, 0),
      bytes: meta(t, :bytes, 0),
      carried: meta(t, :carried, 0),
      stuck: meta(t, :stuck, 0),
      oldest_age_ms: oldest
    }
  end
end
