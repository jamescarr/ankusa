defmodule Ankusa.Queue.Deliveries do
  @moduledoc false

  # Delivery rows: one `deliveries` row per hook and sink, plus an `index` key
  # that says where the row is.
  #
  #   pending, due at T      deliveries(seq, i)  +  ?d(T, seq, i)
  #   claimed by the Pipeline deliveries(seq, i)  +  ?f(seq, i)
  #   dead (the DLQ)          deliveries(seq, i)  +  ?x(dead_at, seq, i)
  #
  # Every transition is one batch, so a row always has exactly one index key.
  # The row value is a term: `%{module, state, attempts, at, error, size}`;
  # `at` is the due time while pending and the dead-letter time once dead, and
  # `size` the stored hook's byte size. A row revived by a replay job
  # (`Ankusa.Dispatch.Replayer`) additionally carries `replay: replay_id`, the
  # job id the delivery is attributed to. Rows bind to `(sink index, module)`;
  # their opts are resolved from the *current* source at delivery time, so a
  # config fix applies to the backlog and no fun or secret is ever persisted.
  #
  # Op builders are pure and return `[Ankusa.Store.op()]`; the caller decides
  # when (and with what durability) to write them.

  require Logger

  alias Ankusa.{Envelope, Store}
  alias Ankusa.Store.Keys

  # Sink indexes that exist only for rows imported from a 0.3 data dir, where
  # the sink a row belonged to was never recorded. The Pipeline expands them
  # into one row per current sink of the source.
  @unresolved_dead 0xFFFE
  @unresolved_pending 0xFFFF

  def unresolved_pending, do: @unresolved_pending
  def unresolved_dead, do: @unresolved_dead
  def unresolved?(sink), do: sink >= @unresolved_dead

  # Plain `binary_to_term`: rows hold module atoms, the same reason
  # `Envelope.from_binary/1` is not `:safe`.
  def encode_row(row), do: :erlang.term_to_binary(row)
  def decode_row(bin), do: :erlang.binary_to_term(bin)

  @type due :: %{
          key: binary(),
          at: integer(),
          seq: pos_integer(),
          sink: integer(),
          size: integer()
        }

  # ── reads ─────────────────────────────────────────────────────────────────

  @doc false
  # One delivery row, decoded. For tests and probes; dispatch reads rows in
  # batches through `load/2`.
  @spec row(atom(), pos_integer(), integer()) :: {:ok, map()} | :not_found | {:error, term()}
  def row(instance, seq, sink) do
    case Store.get(instance, :deliveries, Keys.delivery(seq, sink)) do
      {:ok, bin} -> {:ok, decode_row(bin)}
      other -> other
    end
  end

  @doc """
  Pending rows due at or before `now`, oldest first, from `floor` on. Stops at
  `limit` rows, or before the row that would push the total stored size past
  `byte_budget` — but always takes at least one, so a hook bigger than the
  budget still makes progress.
  """
  @spec due(atom(), integer(), integer(), pos_integer(), integer()) ::
          {:ok, [due()]} | {:error, term()}
  def due(instance, now, floor, limit, byte_budget) do
    range = {<<?d, floor::64>>, <<?d, now + 1::64>>}

    result =
      Store.fold(instance, :due, range, {0, 0, []}, fn key, <<size::32>>, {n, bytes, acc} ->
        cond do
          n == 0 -> {:cont, {1, size, [due_row(key, size)]}}
          n >= limit or bytes + size > byte_budget -> {:halt, {n, bytes, acc}}
          true -> {:cont, {n + 1, bytes + size, [due_row(key, size) | acc]}}
        end
      end)

    case result do
      {:ok, {_n, _bytes, acc}} -> {:ok, Enum.reverse(acc)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp due_row(key, size) do
    {at, seq, sink} = Keys.decode_due(key)
    %{key: key, at: at, seq: seq, sink: sink, size: size}
  end

  @doc "The due time of the earliest pending row at or after `floor`, or `nil`."
  @spec next_due_at(atom(), integer()) :: {:ok, integer() | nil} | {:error, term()}
  def next_due_at(instance, floor) do
    %{hi: hi} = Keys.family(:due)

    Store.fold(instance, :due, {<<?d, floor::64>>, hi}, nil, fn key, _value, _acc ->
      {at, _seq, _sink} = Keys.decode_due(key)
      {:halt, at}
    end)
  end

  @doc """
  Everything a claimed row needs to run: its row, its hook (`nil` if gone) and
  the hook's stored claim-check ref (`nil` if none). Same order as `dues`.
  """
  @spec load(atom(), [due()]) :: {:ok, [map()]} | {:error, term()}
  def load(_instance, []), do: {:ok, []}

  def load(instance, dues) do
    seqs = dues |> Enum.map(& &1.seq) |> Enum.uniq()

    with {:ok, rows} <-
           Store.multi_get(instance, :deliveries, Enum.map(dues, &Keys.delivery(&1.seq, &1.sink))),
         {:ok, hooks} <- Store.multi_get(instance, :hooks, Enum.map(seqs, &Keys.hook/1)),
         {:ok, claims} <- Store.multi_get(instance, :index, Enum.map(seqs, &Keys.claim/1)) do
      envs =
        seqs |> Enum.zip(hooks) |> Map.new(fn {seq, hook} -> {seq, decode_hook(seq, hook)} end)

      claims =
        seqs |> Enum.zip(claims) |> Map.new(fn {seq, claim} -> {seq, decode_claim(claim)} end)

      {:ok,
       dues
       |> Enum.zip(rows)
       |> Enum.map(fn {due, row} ->
         %{
           seq: due.seq,
           sink: due.sink,
           size: due.size,
           row: decode_stored_row(row),
           env: Map.fetch!(envs, due.seq),
           claim: Map.fetch!(claims, due.seq)
         }
       end)}
    end
  end

  defp decode_hook(seq, {:ok, bin}), do: %{Envelope.from_binary(bin) | seq: seq}
  defp decode_hook(_seq, :not_found), do: nil

  defp decode_claim({:ok, bin}), do: :erlang.binary_to_term(bin)
  defp decode_claim(:not_found), do: nil

  defp decode_stored_row({:ok, bin}), do: decode_row(bin)
  defp decode_stored_row(:not_found), do: nil

  @doc "The concrete sink indexes that already have a row for `seq`."
  @spec existing_sinks(atom(), pos_integer()) :: {:ok, MapSet.t()} | {:error, term()}
  def existing_sinks(instance, seq) do
    range = {Keys.delivery(seq, 0), <<seq + 1::64>>}

    Store.fold(instance, :deliveries, range, MapSet.new(), fn key, _value, acc ->
      {_seq, sink} = Keys.decode_delivery(key)
      if unresolved?(sink), do: {:cont, acc}, else: {:cont, MapSet.put(acc, sink)}
    end)
  end

  # ── transitions ───────────────────────────────────────────────────────────

  @doc """
  After a restart nothing is running, so every claimed row is due again. The
  claim key's value is the stored size, which the due key needs.
  """
  @spec recover_inflight(atom(), integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def recover_inflight(instance, now) do
    %{lo: lo, hi: hi} = Keys.family(:inflight)

    with {:ok, ops} <-
           Store.fold(instance, :inflight, {lo, hi}, [], fn key, size, acc ->
             {seq, sink} = Keys.decode_inflight(key)

             {:cont,
              [{:delete, :index, key}, {:put, :index, Keys.due(now, seq, sink), size} | acc]}
           end),
         :ok <- Store.write(instance, ops, sync: true) do
      {:ok, div(length(ops), 2)}
    end
  end

  @doc "Move rows from pending to claimed, in one batch."
  @spec claim(atom(), [due()]) :: :ok | {:error, term()}
  def claim(instance, dues) do
    ops =
      Enum.flat_map(dues, fn due ->
        [
          {:delete, :index, due.key},
          {:put, :index, Keys.inflight(due.seq, due.sink), <<due.size::32>>}
        ]
      end)

    Store.write(instance, ops, sync: false)
  end

  # ── op builders ───────────────────────────────────────────────────────────

  @doc "A delivered row is gone; the marker tells `Ankusa.Queue.Reclaim` to look at its hook."
  def delivered_ops(seq, sink) do
    [
      {:delete, :deliveries, Keys.delivery(seq, sink)},
      {:delete, :index, Keys.inflight(seq, sink)},
      {:put, :index, Keys.cleared(seq, 0, sink), <<>>}
    ]
  end

  @doc "Back to pending at `at`. `row` carries the new attempt count."
  def retry_ops(seq, sink, row, at, error) do
    row = %{row | at: at, error: error}

    [
      {:put, :deliveries, Keys.delivery(seq, sink), encode_row(row)},
      {:delete, :index, Keys.inflight(seq, sink)},
      {:put, :index, Keys.due(at, seq, sink), <<row.size::32>>}
    ]
  end

  @doc "Dead-letter the row. `row` carries the final attempt count."
  def dead_ops(seq, sink, row, at, error, %Envelope{} = env) do
    row = %{row | state: :dead, at: at, error: error}

    [
      {:put, :deliveries, Keys.delivery(seq, sink), encode_row(row)},
      {:delete, :index, Keys.inflight(seq, sink)},
      {:put, :index, Keys.dead(at, seq, sink), :erlang.term_to_binary({env.source_id, env.id})}
    ]
  end

  @doc """
  Replace an imported row whose sink was never recorded by one pending row for
  every current sink of its source that has none yet. A source with no sinks
  has nothing to deliver, so the row just clears.
  """
  def expand_ops(seq, unresolved_sink, size, source_sinks, existing, now) do
    rows =
      source_sinks
      |> Enum.with_index()
      |> Enum.reject(fn {_sink, index} -> MapSet.member?(existing, index) end)
      |> Enum.flat_map(fn {{mod, _opts}, index} ->
        row = %{module: mod, state: :pending, attempts: 0, at: now, error: nil, size: size}

        [
          {:put, :deliveries, Keys.delivery(seq, index), encode_row(row)},
          {:put, :index, Keys.due(now, seq, index), <<size::32>>}
        ]
      end)

    cleared =
      if source_sinks == [] do
        [{:put, :index, Keys.cleared(seq, 0, unresolved_sink), <<>>}]
      else
        []
      end

    rows ++
      [
        {:delete, :deliveries, Keys.delivery(seq, unresolved_sink)},
        {:delete, :index, Keys.inflight(seq, unresolved_sink)}
      ] ++ cleared
  end

  @doc "Persist the hook's claim-check ref, so every sink and every retry reuses it."
  def claim_ops(seq, claim), do: [{:put, :index, Keys.claim(seq), :erlang.term_to_binary(claim)}]

  # ── the DLQ ───────────────────────────────────────────────────────────────

  # A paged scan over dead rows in `range`, for the replay engine. Keeps keys
  # matching `filter` (`:source_id`, `:id`) — the dead-letter time is bounded
  # by the range itself. Stops after `limit` hits or `max_scan` keys examined.
  # Returns the hits (ascending), the last key seen (`nil` when the range held
  # nothing), whether the range was exhausted, and the keys scanned.
  @spec dead_page(atom(), {binary(), binary()}, map(), pos_integer(), pos_integer()) ::
          {:ok, [{binary(), pos_integer(), non_neg_integer()}], binary() | nil, boolean(),
           non_neg_integer()}
          | {:error, term()}
  def dead_page(instance, {lower, upper}, filter, limit, max_scan) do
    result =
      Store.fold(instance, :dead, {lower, upper}, {0, 0, [], nil}, fn key,
                                                                      value,
                                                                      {scanned, hit_n, hits, last} ->
        # The scan budget is enforced before the filter: a filter that matches
        # little must not make one tick fold the whole range.
        if scanned >= max_scan do
          {:halt, {scanned, hit_n, hits, last}}
        else
          {_at, seq, sink} = Keys.decode_dead(key)
          {source_id, id} = :erlang.binary_to_term(value)

          cond do
            not dead_match?(filter, source_id, id) ->
              {:cont, {scanned + 1, hit_n, hits, key}}

            hit_n >= limit ->
              {:halt, {scanned, hit_n, hits, last}}

            true ->
              {:cont, {scanned + 1, hit_n + 1, [{key, seq, sink} | hits], key}}
          end
        end
      end)

    case result do
      {:ok, {scanned, hit_n, hits, last}} ->
        # The fold ends by halt (limit/max_scan reached) or by running off the
        # range; only the latter means every dead row in range was examined.
        exhausted? = hit_n < limit and scanned < max_scan
        {:ok, Enum.reverse(hits), last, exhausted?, scanned}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # `matches?/4` minus the `:since` bound: the page's range carries that.
  defp dead_match?(filter, source_id, id) do
    keep?(filter, :source_id, source_id) and keep?(filter, :id, id)
  end

  # Move dead rows back to pending for a replay job: the revived row keeps its
  # hook and gets `replay: replay_id`, `attempts: 0` and a due time of `now`.
  # A dead key whose row is gone is deleted as an orphan. Returns the batch ops
  # and the number of rows actually revived.
  @spec revive_ops(atom(), [{binary(), pos_integer(), non_neg_integer()}], integer(), String.t()) ::
          {:ok, [Ankusa.Store.op()], non_neg_integer()} | {:error, term()}
  def revive_ops(instance, hits, now, replay_id) do
    keys = Enum.map(hits, fn {_key, seq, sink} -> Keys.delivery(seq, sink) end)

    with {:ok, rows} <- Store.multi_get(instance, :deliveries, keys) do
      {ops, revived} =
        hits
        |> Enum.zip(rows)
        |> Enum.reduce({[], 0}, fn {{key, seq, sink}, result}, {ops, n} ->
          {row_ops, n} = revive_row(key, seq, sink, result, now, replay_id, n)
          {row_ops ++ ops, n}
        end)

      {:ok, ops, revived}
    end
  end

  # Older rows (written before the `replay` field existed) lack the key, so
  # `Map.put`, not `%{row | replay: ...}`. A row that does not decode can never
  # be revived: it stays in the DLQ (still visible to `GET /v1/dlq`) and the
  # job's cursor simply moves past it, so one corrupt row cannot wedge the
  # replay engine on it forever.
  defp revive_row(key, seq, sink, {:ok, bin}, now, replay_id, n) do
    row =
      decode_row(bin)
      |> Map.merge(%{state: :pending, attempts: 0, at: now, error: nil})
      |> Map.put(:replay, replay_id)

    {[
       {:delete, :index, key},
       {:put, :deliveries, Keys.delivery(seq, sink), encode_row(row)},
       {:put, :index, Keys.due(now, seq, sink), <<row.size::32>>}
     ], n + 1}
  rescue
    error ->
      Logger.warning(
        "[ankusa] dead row #{seq}/#{sink} does not decode and was left in the DLQ: " <>
          Exception.message(error)
      )

      {[], n}
  end

  defp revive_row(key, _seq, _sink, :not_found, _now, _replay_id, n) do
    {[{:delete, :index, key}], n}
  end

  @doc """
  The dead rows, newest first: `total` counts every match, `entries` holds the
  first `:limit`. `opts`: `:source_id`, `:since`, `:limit`.
  """
  @spec dead(atom(), keyword()) ::
          {:ok, %{total: non_neg_integer(), entries: [map()]}} | {:error, term()}
  def dead(instance, opts) do
    %{lo: lo, hi: hi} = Keys.family(:dead)
    limit = Keyword.fetch!(opts, :limit)
    filter = opts |> Keyword.take([:source_id, :since]) |> Map.new()

    scan =
      Store.fold(
        instance,
        :dead,
        {lo, hi},
        {0, 0, []},
        fn key, value, {total, kept_n, kept} = acc ->
          {at, seq, sink} = Keys.decode_dead(key)
          {source_id, id} = :erlang.binary_to_term(value)

          cond do
            not matches?(filter, source_id, id, at) -> {:cont, acc}
            kept_n < limit -> {:cont, {total + 1, kept_n + 1, [{at, seq, sink} | kept]}}
            true -> {:cont, {total + 1, kept_n, kept}}
          end
        end,
        reverse: true
      )

    with {:ok, {total, _kept_n, kept}} <- scan,
         kept = Enum.reverse(kept),
         {:ok, rows} <-
           Store.multi_get(
             instance,
             :deliveries,
             Enum.map(kept, fn {_, seq, sink} -> Keys.delivery(seq, sink) end)
           ),
         {:ok, hooks} <-
           Store.multi_get(instance, :hooks, Enum.map(kept, fn {_, seq, _} -> Keys.hook(seq) end)) do
      {entries, orphans} =
        [kept, rows, hooks]
        |> Enum.zip()
        |> Enum.reduce({[], []}, fn
          {{at, seq, _sink}, {:ok, row}, {:ok, hook}}, {entries, orphans} ->
            entry = %{
              envelope: %{Envelope.from_binary(hook) | seq: seq},
              reason: row_error(row),
              at: at
            }

            {[entry | entries], orphans}

          {{_at, seq, sink}, _row, _hook}, {entries, orphans} ->
            {entries, [{seq, sink} | orphans]}
        end)

      # A dead key whose row or hook is gone lists nothing, so orphans on the
      # returned page are not counted either, and are logged since it should
      # not happen; replay deletes them. One past the page is still counted,
      # so `total` is exact unless corruption hides beyond it.
      if orphans != [] do
        Logger.warning(
          "[ankusa] DLQ key(s) with no delivery row or hook, not listed: #{inspect(Enum.reverse(orphans))}"
        )
      end

      {:ok, %{total: total - length(orphans), entries: Enum.reverse(entries)}}
    end
  end

  # A delivery row that does not decode still lists: the replay engine leaves
  # such a row in the DLQ (see `revive_row/7`), so listing must not raise on
  # it and take the whole page down.
  defp row_error(bin) do
    decode_row(bin).error
  rescue
    _ -> "undecodable delivery row"
  end

  defp matches?(filter, source_id, id, at) do
    keep?(filter, :source_id, source_id) and keep?(filter, :id, id) and since_ok?(filter, at)
  end

  defp keep?(filter, key, actual) do
    case Map.get(filter, key) do
      nil -> true
      expected -> actual == expected
    end
  end

  defp since_ok?(filter, at) do
    case Map.get(filter, :since) do
      nil -> true
      since -> at >= since
    end
  end
end
