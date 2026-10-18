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
  # The row value is a term: `%{module, state, attempts, at, error, size,
  # source_id}`; `at` is the due time while pending and the dead-letter time
  # once dead, `size` the stored hook's byte size, and `source_id` the hook's
  # source (rows written before it existed lack the key: read the hook). A row
  # revived by a replay job (`Ankusa.Dispatch.Replayer`) additionally carries
  # `replay: replay_id`, the job id the delivery is attributed to. Rows bind to
  # `(sink index, module)`; their opts are resolved from the *current* source
  # at delivery time, so a config fix applies to the backlog and no fun or
  # secret is ever persisted.
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

  # Row encoding, versioned like `Ankusa.Envelope.to_binary/2`:
  #
  #   v0  the bare term, as every release up to 0.6 wrote it (the default here)
  #   v1  `<<"AD", 1, term::binary>>` (`version: 1`)
  #
  # This release reads both and still writes v0, so a rollback to the previous
  # release keeps reading every row; the default flips to 1 one minor later
  # (`docs/deployment.md`, "Rolling back a minor"). A row is a plain map and
  # its keys pass through untouched: a key this node does not know (a field a
  # newer node added) survives every rewrite of the row.
  @write_version 0

  @spec encode_row(map(), keyword()) :: binary()
  def encode_row(row, opts \\ []) do
    term = :erlang.term_to_binary(row)

    case Keyword.get(opts, :version, @write_version) do
      0 -> term
      1 -> <<"AD", 1, term::binary>>
    end
  end

  @spec decode_row(binary()) ::
          {:ok, map()} | {:error, :unsupported_delivery_row_version | :undecodable}
  def decode_row(<<"AD", 1, term::binary>>), do: decode_term(term)

  def decode_row(<<"AD", _version, _term::binary>>),
    do: {:error, :unsupported_delivery_row_version}

  def decode_row(bin) when is_binary(bin), do: decode_term(bin)

  # Plain `binary_to_term`: rows hold module atoms, the same reason
  # `Envelope.from_binary/1` is not `:safe`.
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp decode_term(term) do
    case :erlang.binary_to_term(term) do
      %{} = row -> {:ok, row}
      _ -> {:error, :undecodable}
    end
  rescue
    ArgumentError -> {:error, :undecodable}
  end

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
      {:ok, bin} -> decode_row(bin)
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
  the hook's stored claim-check ref (`nil` if none). Same order as `dues`. A
  row or hook that does not decode is `{:error, reason}` instead (see
  `decode_row/1`, `Envelope.from_binary/1`): the Pipeline dead-letters it.
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

  defp decode_hook(seq, {:ok, bin}) do
    case Envelope.from_binary(bin) do
      {:ok, env} -> %{env | seq: seq}
      {:error, _reason} = error -> error
    end
  end

  defp decode_hook(_seq, :not_found), do: nil

  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp decode_claim({:ok, bin}), do: :erlang.binary_to_term(bin)
  defp decode_claim(:not_found), do: nil

  defp decode_stored_row({:ok, bin}) do
    case decode_row(bin) do
      {:ok, row} -> row
      {:error, _reason} = error -> error
    end
  end

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

  # ── one source's rows ─────────────────────────────────────────────────────

  # Index keys examined per read of their rows (and of the hooks of rows that
  # predate `source_id`).
  @source_chunk 1_000

  @type source_row :: %{key: binary(), seq: pos_integer(), sink: integer(), row: map()}

  @doc """
  The undelivered rows of `source_id` in this node's queue: `pending` rows sit
  in the due index (due now or waiting out a backoff), `inflight` rows are
  claimed by the Pipeline. Walks both indexes, the cost of one queue-gauge
  tick.
  """
  @spec pending_for_source(atom(), String.t()) ::
          {:ok, %{pending: non_neg_integer(), inflight: non_neg_integer()}} | {:error, term()}
  def pending_for_source(instance, source_id) do
    count = fn rows, n -> {:cont, n + length(rows)} end

    with {:ok, pending} <- fold_source(instance, :due, source_id, 0, count),
         {:ok, inflight} <- fold_source(instance, :inflight, source_id, 0, count) do
      {:ok, %{pending: pending, inflight: inflight}}
    end
  end

  @doc """
  Folds the rows of `source_id` held in index `family` (`:due` or
  `:inflight`): `fun.(rows, acc)` runs once per #{@source_chunk} index keys
  examined, with the matching rows decoded, and returns `{:cont, acc}` or
  `{:halt, acc}`. A row written before rows carried `source_id` is matched by
  its hook's; an index key whose row or hook is gone is skipped. The scan
  reads a snapshot, so `fun` may move the rows it is given.
  """
  @spec fold_source(
          atom(),
          :due | :inflight,
          String.t(),
          acc,
          ([source_row()], acc -> {:cont, acc} | {:halt, acc})
        ) :: {:ok, acc} | {:error, term()}
        when acc: term()
  def fold_source(instance, family, source_id, acc, fun) when family in [:due, :inflight] do
    %{lo: lo, hi: hi} = Keys.family(family)

    scan =
      Store.fold(instance, family, {lo, hi}, {[], 0, acc}, fn key, _value, {chunk, n, acc} ->
        chunk = [index_entry(family, key) | chunk]

        if n + 1 < @source_chunk do
          {:cont, {chunk, n + 1, acc}}
        else
          case source_step(instance, chunk, source_id, acc, fun) do
            {:cont, acc} -> {:cont, {[], 0, acc}}
            {:halt, acc} -> {:halt, {:halted, acc}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end
      end)

    case scan do
      {:ok, {:halted, acc}} ->
        {:ok, acc}

      {:ok, {:error, reason}} ->
        {:error, reason}

      {:ok, {chunk, _n, acc}} ->
        case source_step(instance, chunk, source_id, acc, fun) do
          {:error, reason} -> {:error, reason}
          {_cont_or_halt, acc} -> {:ok, acc}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp index_entry(:due, key) do
    {_at, seq, sink} = Keys.decode_due(key)
    %{key: key, seq: seq, sink: sink}
  end

  defp index_entry(:inflight, key) do
    {seq, sink} = Keys.decode_inflight(key)
    %{key: key, seq: seq, sink: sink}
  end

  # `chunk` is newest-first, as the fold built it.
  defp source_step(_instance, [], _source_id, acc, _fun), do: {:cont, acc}

  defp source_step(instance, chunk, source_id, acc, fun) do
    entries = Enum.reverse(chunk)
    keys = Enum.map(entries, &Keys.delivery(&1.seq, &1.sink))

    with {:ok, rows} <- Store.multi_get(instance, :deliveries, keys),
         found = for({entry, {:ok, bin}} <- Enum.zip(entries, rows), do: with_row(entry, bin)),
         found = Enum.reject(found, &is_nil/1),
         {tagged, legacy} = Enum.split_with(found, &Map.has_key?(&1.row, :source_id)),
         {:ok, legacy} <- legacy_of_source(instance, legacy, source_id) do
      fun.(Enum.filter(tagged, &(&1.row.source_id == source_id)) ++ legacy, acc)
    end
  end

  # A row that does not decode belongs to no source this scan can name: it is
  # skipped (and said so), and the Pipeline dead-letters it when it comes due.
  defp with_row(entry, bin) do
    case decode_row(bin) do
      {:ok, row} ->
        Map.put(entry, :row, row)

      {:error, reason} ->
        Logger.warning(
          "[ankusa] delivery row #{entry.seq}/#{entry.sink} does not decode (#{reason}); " <>
            "left out of the source scan"
        )

        nil
    end
  end

  defp legacy_of_source(_instance, [], _source_id), do: {:ok, []}

  defp legacy_of_source(instance, entries, source_id) do
    seqs = entries |> Enum.map(& &1.seq) |> Enum.uniq()

    with {:ok, hooks} <- Store.multi_get(instance, :hooks, Enum.map(seqs, &Keys.hook/1)) do
      mine =
        for {seq, {:ok, bin}} <- Enum.zip(seqs, hooks),
            match?({:ok, %{source_id: ^source_id}}, Envelope.from_binary(bin)),
            into: MapSet.new(),
            do: seq

      {:ok, Enum.filter(entries, &MapSet.member?(mine, &1.seq))}
    end
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

  @unknown_source "unknown"

  @doc """
  The `{source_id, id}` a dead row is filed under. `env` is the hook, or what
  `Envelope.from_binary/1` answered when it did not decode: then the source is
  the row's (`"unknown"` when the row is no help either) and there is no id.
  """
  @spec owner(Envelope.t() | term(), map()) :: {String.t(), String.t() | nil}
  def owner(%Envelope{source_id: source_id, id: id}, _row), do: {source_id, id}
  def owner(_undecoded, row), do: {Map.get(row, :source_id) || @unknown_source, nil}

  @doc """
  Dead-letter the row. `row` carries the final attempt count; `env` is the
  hook or its decode error (see `owner/2`).
  """
  def dead_ops(seq, sink, row, at, error, env) do
    row = %{row | state: :dead, at: at, error: error}

    [
      {:put, :deliveries, Keys.delivery(seq, sink), encode_row(row)},
      {:delete, :index, Keys.inflight(seq, sink)},
      {:put, :index, Keys.dead(at, seq, sink), :erlang.term_to_binary(owner(env, row))}
    ]
  end

  @doc """
  Replace an imported row whose sink was never recorded by one pending row for
  every current sink of its source that has none yet. A source with no sinks
  has nothing to deliver, so the row just clears.
  """
  def expand_ops(seq, unresolved_sink, size, source_id, source_sinks, existing, now) do
    rows =
      source_sinks
      |> Enum.with_index()
      |> Enum.reject(fn {_sink, index} -> MapSet.member?(existing, index) end)
      |> Enum.flat_map(fn {{mod, _opts}, index} ->
        row = %{
          module: mod,
          state: :pending,
          attempts: 0,
          at: now,
          error: nil,
          size: size,
          source_id: source_id
        }

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
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
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

        # A tenant-filtered page may come back short of `limit` without being
        # exhausted: the cursor still moves past the rows it left out.
        with {:ok, hits} <- tenant_hits(instance, Enum.reverse(hits), filter) do
          {:ok, hits, last, exhausted?, scanned}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Until the dead index carries the tenant (TEN-1), a tenant filter reads the
  # hooks of the page's hits.
  defp tenant_hits(instance, hits, %{tenants: tenants}) when is_list(tenants) do
    keys = Enum.map(hits, fn {_key, seq, _sink} -> Keys.hook(seq) end)

    with {:ok, bins} <- Store.multi_get(instance, :hooks, keys) do
      kept = for {hit, {:ok, bin}} <- Enum.zip(hits, bins), hook_tenant_in?(bin, tenants), do: hit
      {:ok, kept}
    end
  end

  defp tenant_hits(_instance, hits, _filter), do: {:ok, hits}

  # The hook's tenant; an envelope without one belongs to `"default"`, as in
  # `Envelope.idempotency_key/1`. A hook this node cannot decode names no
  # tenant, so it matches none.
  defp hook_tenant_in?(bin, tenants) do
    case Envelope.from_binary(bin) do
      {:ok, env} -> (env.tenant_id || "default") in tenants
      {:error, _reason} -> false
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
    case decode_row(bin) do
      {:ok, %{size: size} = row} when is_integer(size) ->
        row =
          row
          |> Map.merge(%{state: :pending, attempts: 0, at: now, error: nil})
          |> Map.put(:replay, replay_id)

        {[
           {:delete, :index, key},
           {:put, :deliveries, Keys.delivery(seq, sink), encode_row(row)},
           {:put, :index, Keys.due(now, seq, sink), <<size::32>>}
         ], n + 1}

      {:error, reason} ->
        left_in_dlq(seq, sink, reason, n)

      {:ok, _no_size} ->
        left_in_dlq(seq, sink, :malformed_row, n)
    end
  end

  defp revive_row(key, _seq, _sink, :not_found, _now, _replay_id, n) do
    {[{:delete, :index, key}], n}
  end

  defp left_in_dlq(seq, sink, reason, n) do
    Logger.warning(
      "[ankusa] dead row #{seq}/#{sink} does not decode and was left in the DLQ: #{reason}"
    )

    {[], n}
  end

  @doc """
  The dead rows, newest first: `total` counts every match, `entries` holds the
  first `:limit`. `opts`: `:source_id`, `:since`, `:tenants` (only rows of
  hooks owned by one of these tenants; `nil` is every tenant), `:limit`.
  """
  @spec dead(atom(), keyword()) ::
          {:ok, %{total: non_neg_integer(), entries: [map()]}} | {:error, term()}
  def dead(instance, opts) do
    %{lo: lo, hi: hi} = Keys.family(:dead)
    limit = Keyword.fetch!(opts, :limit)
    filter = opts |> Keyword.take([:source_id, :since]) |> Map.new()
    tenants = Keyword.get(opts, :tenants)

    scan =
      if is_list(tenants),
        do: dead_scan_tenants(instance, {lo, hi}, filter, tenants, limit),
        else: dead_scan(instance, {lo, hi}, filter, limit)

    with {:ok, {total, _kept_n, kept}} <- scan,
         kept = Enum.reverse(kept),
         {:ok, rows} <-
           Store.multi_get(
             instance,
             :deliveries,
             Enum.map(kept, fn {_, seq, sink, _} -> Keys.delivery(seq, sink) end)
           ),
         {:ok, hooks} <-
           Store.multi_get(
             instance,
             :hooks,
             Enum.map(kept, fn {_, seq, _, _} -> Keys.hook(seq) end)
           ) do
      {entries, orphans} =
        [kept, rows, hooks]
        |> Enum.zip()
        |> Enum.reduce({[], []}, fn
          {{at, seq, _sink, owner}, {:ok, row}, {:ok, hook}}, {entries, orphans} ->
            entry = %{
              envelope: listed_envelope(hook, seq, at, owner, row),
              reason: row_error(row),
              at: at
            }

            {[entry | entries], orphans}

          {{_at, seq, sink, _owner}, _row, _hook}, {entries, orphans} ->
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

  # `{total, kept_n, kept}` of the dead keys matching `filter`, newest first:
  # every match is counted, the first `limit` are kept (as a reversed list).
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp dead_scan(instance, range, filter, limit) do
    Store.fold(
      instance,
      :dead,
      range,
      {0, 0, []},
      fn key, value, {total, kept_n, kept} = acc ->
        {at, seq, sink} = Keys.decode_dead(key)
        {source_id, id} = :erlang.binary_to_term(value)

        cond do
          not matches?(filter, source_id, id, at) ->
            {:cont, acc}

          kept_n < limit ->
            {:cont, {total + 1, kept_n + 1, [{at, seq, sink, {source_id, id}} | kept]}}

          true ->
            {:cont, {total + 1, kept_n, kept}}
        end
      end,
      reverse: true
    )
  end

  # `dead_scan/4` limited to hooks owned by one of `tenants`. The dead index
  # does not carry the tenant (TEN-1 will), so the keys that pass `filter` are
  # checked against their hooks 1 000 at a time: the scan holds one
  # chunk of keys, never the whole DLQ.
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp dead_scan_tenants(instance, range, filter, tenants, limit) do
    scan =
      Store.fold(
        instance,
        :dead,
        range,
        {{0, 0, []}, [], 0},
        fn key, value, {acc, chunk, n} ->
          {at, seq, sink} = Keys.decode_dead(key)
          {source_id, id} = :erlang.binary_to_term(value)

          if matches?(filter, source_id, id, at) do
            chunk = [{at, seq, sink, {source_id, id}} | chunk]

            if n + 1 < @source_chunk do
              {:cont, {acc, chunk, n + 1}}
            else
              case tenant_step(instance, chunk, tenants, limit, acc) do
                {:ok, acc} -> {:cont, {acc, [], 0}}
                {:error, reason} -> {:halt, {:error, reason}}
              end
            end
          else
            {:cont, {acc, chunk, n}}
          end
        end,
        reverse: true
      )

    case scan do
      {:ok, {:error, reason}} -> {:error, reason}
      {:ok, {acc, chunk, _n}} -> tenant_step(instance, chunk, tenants, limit, acc)
      {:error, reason} -> {:error, reason}
    end
  end

  # Fold one chunk of dead keys (oldest first, as the reverse scan built it)
  # into the `dead_scan/4` accumulator, keeping those whose hook belongs to
  # `tenants`. A key whose hook is gone names no tenant and is left out.
  defp tenant_step(_instance, [], _tenants, _limit, acc), do: {:ok, acc}

  defp tenant_step(instance, chunk, tenants, limit, acc) do
    entries = Enum.reverse(chunk)

    with {:ok, bins} <-
           Store.multi_get(
             instance,
             :hooks,
             Enum.map(entries, fn {_, seq, _, _} -> Keys.hook(seq) end)
           ) do
      {:ok,
       entries
       |> Enum.zip(bins)
       |> Enum.reduce(acc, fn
         {entry, {:ok, bin}}, {total, kept_n, kept} = acc ->
           cond do
             not hook_tenant_in?(bin, tenants) -> acc
             kept_n < limit -> {total + 1, kept_n + 1, [entry | kept]}
             true -> {total + 1, kept_n, kept}
           end

         {_entry, :not_found}, acc ->
           acc
       end)}
    end
  end

  # A hook this node cannot decode (an envelope version it does not know, say)
  # still lists, as a stub built from what the dead key and the row hold: an
  # operator must see the dead letter and why it died.
  defp listed_envelope(hook, seq, at, {source_id, id}, row) do
    case Envelope.from_binary(hook) do
      {:ok, env} ->
        %{env | seq: seq}

      {:error, _reason} ->
        size =
          case decode_row(row) do
            {:ok, %{size: size}} when is_integer(size) -> size
            _ -> 0
          end

        %Envelope{
          id: id,
          source_id: source_id,
          received_at: at,
          method: "",
          path: "",
          headers: [],
          body: "",
          size: size,
          seq: seq
        }
    end
  end

  # A delivery row that does not decode still lists: the replay engine leaves
  # such a row in the DLQ (see `revive_row/7`), so listing must not raise on
  # it and take the whole page down.
  defp row_error(bin) do
    case decode_row(bin) do
      {:ok, row} -> Map.get(row, :error)
      {:error, :unsupported_delivery_row_version} -> "unsupported delivery row version"
      {:error, :undecodable} -> "undecodable delivery row"
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
