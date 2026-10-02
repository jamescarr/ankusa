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
  # `size` the stored hook's byte size. Rows bind to `(sink index, module)`;
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

  @doc """
  Move dead rows back to pending, in batches. `filter` may carry `:source_id`,
  `:id` and `:since` (a unix-ms lower bound on the dead-letter time).
  """
  @spec replay(atom(), map(), integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def replay(instance, filter, now) do
    %{lo: lo, hi: hi} = Keys.family(:dead)

    scan =
      Store.fold(instance, :dead, {lo, hi}, [], fn key, value, acc ->
        {at, seq, sink} = Keys.decode_dead(key)
        {source_id, id} = :erlang.binary_to_term(value)

        if matches?(filter, source_id, id, at),
          do: {:cont, [{key, seq, sink} | acc]},
          else: {:cont, acc}
      end)

    with {:ok, hits} <- scan do
      hits
      |> Enum.reverse()
      |> Enum.chunk_every(1_000)
      |> Enum.reduce_while({:ok, 0}, fn chunk, {:ok, replayed} ->
        case replay_chunk(instance, chunk, now) do
          {:ok, n} -> {:cont, {:ok, replayed + n}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  defp replay_chunk(instance, chunk, now) do
    keys = Enum.map(chunk, fn {_key, seq, sink} -> Keys.delivery(seq, sink) end)

    with {:ok, rows} <- Store.multi_get(instance, :deliveries, keys) do
      {ops, flipped} =
        chunk
        |> Enum.zip(rows)
        |> Enum.flat_map_reduce(0, fn
          {{key, seq, sink}, {:ok, bin}}, flipped ->
            row = %{decode_row(bin) | state: :pending, attempts: 0, at: now, error: nil}

            {[
               {:delete, :index, key},
               {:put, :deliveries, Keys.delivery(seq, sink), encode_row(row)},
               {:put, :index, Keys.due(now, seq, sink), <<row.size::32>>}
             ], flipped + 1}

          # The row is gone but its dead key is not: drop the orphan.
          {{key, _seq, _sink}, :not_found}, flipped ->
            {[{:delete, :index, key}], flipped}
        end)

      with :ok <- Store.write(instance, ops, sync: true), do: {:ok, flipped}
    end
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
              reason: decode_row(row).error,
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
