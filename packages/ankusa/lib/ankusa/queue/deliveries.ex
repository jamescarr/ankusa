defmodule Ankusa.Queue.Deliveries do
  @moduledoc false

  # Delivery rows: one `deliveries` row per hook and sink, plus an `index` key
  # that says where the row is. `ord` is the hook's tenant ordinal
  # (`Ankusa.Queue.Tenants`); ADR 0001 has the layout.
  #
  #   pending, due at T      deliveries(seq, i)  +  ?D(ord, T, seq, i)  +  ?A(T/1000, ord)
  #   claimed by the Pipeline deliveries(seq, i)  +  ?F(ord, seq, i)
  #   dead (the DLQ)          deliveries(seq, i)  +  ?x(dead_at, seq, i)  +  ?X(ord, dead_at, seq, i)
  #
  # Every transition is one batch, so a row always has exactly one index key,
  # and the batch carries the counter deltas of the transition
  # (`Deliveries.Counters`, blind merge operands) and, for a due row, its
  # active-tenant marker. Nothing outside this module builds a `?D`, `?F`,
  # `?X` or `?A` key.
  # The row value is a term: `%{module, state, attempts, at, error, size,
  # source_id}`; `at` is the due time while pending and the dead-letter time
  # once dead, `size` the stored hook's byte size, and `source_id` the hook's
  # source (rows written before it existed lack the key: read the hook).
  # `first_attempt_at` (ms) is when dispatch first took the row, the start of
  # its retry horizon (`Ankusa.RetryPolicy`); dispatch adds it the first time it
  # resolves a row, so rows written before it existed get it then, and it is an
  # additive key under the row versioning below. A row
  # revived by a replay job (`Ankusa.Dispatch.Replayer`) additionally carries
  # `replay: replay_id`, the job id the delivery is attributed to. Rows bind to
  # `(sink index, module)`; their opts are resolved from the *current* source
  # at delivery time, so a config fix applies to the backlog and no fun or
  # secret is ever persisted.
  #
  # Op builders are pure and return `[Ankusa.Store.op()]`; the caller decides
  # when (and with what durability) to write them.

  require Logger

  alias Ankusa.ClaimCheck.Ref
  alias Ankusa.{Envelope, Store}
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Queue.{Reclaim, Tenants}
  alias Ankusa.Store.{Keys, Sweep}

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
          size: integer(),
          ord: pos_integer(),
          kind: 0 | 1
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

  # A due row's `kind`: 0 a first attempt (ingest, replay revival, recovery,
  # expansion), 1 a retry. It is what keeps the `pending` and `scheduled`
  # counters exact without a clock.
  @first_attempt 0
  @retry 1

  # Markers examined per read of the earliest due time, and tenants looked at
  # in the first bucket, and markers of skipped tenants passed over.
  @next_due_stale_max 32
  @next_due_tenants 16
  @next_due_skip_max 4_096
  # Rows a marker cleanup looks at to restore a marker a racing writer needs.
  @verify_max 256

  @doc """
  The due rows of tenant `ord` at or before `now`, oldest first, from `floor`
  on. Stops at `limit` rows, or before the row that would push the total
  stored size past `byte_budget` — but always takes at least one, so a hook
  bigger than the budget still makes progress.
  """
  @spec due_for_tenant(atom(), pos_integer(), integer(), integer(), pos_integer(), integer()) ::
          {:ok, [due()]} | {:error, term()}
  def due_for_tenant(instance, ord, now, floor, limit, byte_budget) do
    range = {<<?D, ord::32, floor::64>>, <<?D, ord::32, now + 1::64>>}

    result =
      Store.fold(instance, :due, range, {0, 0, []}, fn key,
                                                       <<size::32, kind::8>>,
                                                       {n, bytes, acc} ->
        cond do
          n == 0 -> {:cont, {1, size, [due_row(key, size, kind)]}}
          n >= limit or bytes + size > byte_budget -> {:halt, {n, bytes, acc}}
          true -> {:cont, {n + 1, bytes + size, [due_row(key, size, kind) | acc]}}
        end
      end)

    case result do
      {:ok, {_n, _bytes, acc}} -> {:ok, Enum.reverse(acc)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp due_row(key, size, kind) do
    {ord, at, seq, sink} = Keys.decode_due(key)
    %{key: key, at: at, seq: seq, sink: sink, size: size, ord: ord, kind: kind}
  end

  @doc """
  The tenants with a due-row marker at or before `now`'s second, in marker
  order (oldest second first), from the first `limit` markers: `[{ord,
  [bucket]}]`, each tenant once with the buckets of its markers that were read.
  It never touches a tenant with nothing due, so its cost does not depend on
  the number of tenants or the size of the backlog.
  """
  @spec active_tenants(atom(), integer(), pos_integer()) ::
          {:ok, [{pos_integer(), [integer()]}]} | {:error, term()}
  def active_tenants(instance, now, limit) do
    %{lo: lo} = Keys.family(:active)
    range = {lo, <<?A, div(now, 1000) + 1::64>>}

    with {:ok, {_n, markers}} <-
           Store.fold(instance, :active, range, {0, []}, fn key, _value, {n, acc} ->
             acc = [Keys.decode_active(key) | acc]
             if n + 1 >= limit, do: {:halt, {n + 1, acc}}, else: {:cont, {n + 1, acc}}
           end) do
      {:ok, group_markers(Enum.reverse(markers))}
    end
  end

  defp group_markers(markers) do
    {order, buckets} =
      Enum.reduce(markers, {[], %{}}, fn {bucket, ord}, {order, buckets} ->
        case buckets do
          %{^ord => seen} -> {order, %{buckets | ord => [bucket | seen]}}
          _ -> {[ord | order], Map.put(buckets, ord, [bucket])}
        end
      end)

    for ord <- Enum.reverse(order), do: {ord, Enum.reverse(Map.fetch!(buckets, ord))}
  end

  @doc """
  The due time of tenant `ord`'s earliest due row at or after `floor`, or
  `nil`: one seek.
  """
  @spec first_due(atom(), pos_integer(), integer()) ::
          {:ok, integer() | nil} | {:error, term()}
  def first_due(instance, ord, floor) do
    {_lower, upper} = Keys.range(<<?D, ord::32>>)

    case Store.first(instance, :due, {<<?D, ord::32, floor::64>>, upper}) do
      {:ok, {key, _value}} ->
        {_ord, at, _seq, _sink} = Keys.decode_due(key)
        {:ok, at}

      {:ok, nil} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  The due time of the earliest pending row, or `nil`, read from the active
  index: the first markers are checked against their tenants' earliest rows
  (`floors` maps a tenant to the scan floor to seek from), skipping at most 32
  stale ones, and the earliest of the tenants sharing the first live second wins.
  `opts`: `:tenants` (16), how many tenants of that second are looked at; the
  gauge asks for one, since an age exact to the second needs no more, and a seek
  into a tenant's range costs more the bigger the range. `:skip` (a `MapSet` of
  tenant ordinals), the tenants not looked at: dispatch passes the ones at their
  own window, whose due rows it cannot claim, so they do not keep it awake. At
  most 4 096 markers of skipped tenants are passed over, after which nothing is
  reported due (the outcome that frees a tenant's window wakes dispatch itself).
  """
  @spec next_due_at(atom(), %{optional(pos_integer()) => integer()}, keyword()) ::
          {:ok, integer() | nil} | {:error, term()}
  def next_due_at(instance, floors \\ %{}, opts \\ []) do
    %{lo: lo, hi: hi} = Keys.family(:active)

    acc = %{
      bucket: nil,
      at: nil,
      stale: 0,
      seen: 0,
      error: nil,
      tenants: Keyword.get(opts, :tenants, @next_due_tenants),
      skip: Keyword.get(opts, :skip, MapSet.new()),
      skipped: 0
    }

    scan =
      Store.fold(instance, :active, {lo, hi}, acc, fn key, _value, acc ->
        {bucket, ord} = Keys.decode_active(key)

        cond do
          acc.bucket != nil and bucket != acc.bucket ->
            {:halt, acc}

          MapSet.member?(acc.skip, ord) ->
            skipped = acc.skipped + 1

            if skipped >= @next_due_skip_max,
              do: {:halt, acc},
              else: {:cont, %{acc | skipped: skipped}}

          true ->
            next_due_marker(instance, bucket, ord, Map.get(floors, ord, 0), acc)
        end
      end)

    case scan do
      {:ok, %{error: nil, at: at}} -> {:ok, at}
      {:ok, %{error: reason}} -> {:error, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  defp next_due_marker(instance, bucket, ord, floor, acc) do
    case first_due(instance, ord, floor) do
      {:ok, at} when at == nil or div(at, 1000) > bucket ->
        acc = %{acc | stale: acc.stale + 1}

        # Too many dead markers in a row: report where they begin; the next
        # dispatch scan removes them.
        if acc.stale >= @next_due_stale_max,
          do: {:halt, %{acc | at: acc.at || bucket * 1000}},
          else: {:cont, acc}

      {:ok, at} ->
        acc = %{acc | bucket: bucket, at: min(acc.at || at, at), seen: acc.seen + 1}
        if acc.seen >= acc.tenants, do: {:halt, acc}, else: {:cont, acc}

      {:error, reason} ->
        {:halt, %{acc | error: reason}}
    end
  end

  @doc """
  Marker cleanup, "delete, then verify" (ADR 0001, section 5), for tenant
  `ord` after the Pipeline visited it. `buckets` are its markers that the visit
  read. A marker is stale when its second is over and the tenant's earliest due
  row is in a later one. Stale markers are deleted, then the tenant is read again
  and the marker of any due row that a concurrent commit slipped into a deleted
  second is put back. A writer's batch holds its row and its own marker, so no
  interleaving leaves a due row without one. It reads the tenant from the start
  of its range, not from a scan cursor: a row a stalled commit made visible
  beneath the cursor must keep its marker. Returns whether the tenant has no due
  row at all. `opts` (`:after_read`, `:after_delete`: a function called between
  the steps) exist for the test that interleaves a commit with each step.
  """
  @spec sweep_markers(atom(), pos_integer(), [integer()], integer(), keyword()) ::
          {:ok, boolean()} | {:error, term()}
  def sweep_markers(instance, ord, buckets, now, opts \\ []) do
    current = div(now, 1000)
    candidates = Enum.filter(buckets, &(&1 < current))

    with {:ok, first} <- first_due(instance, ord, 0),
         :ok <- between(opts, :after_read) do
      first_bucket = if first, do: div(first, 1000), else: current

      case Enum.filter(candidates, &(&1 < first_bucket)) do
        [] ->
          {:ok, first == nil}

        stale ->
          with :ok <-
                 Store.write(instance, Enum.map(stale, &{:delete, :index, Keys.active(&1, ord)}),
                   sync: false
                 ),
               :ok <- between(opts, :after_delete),
               :ok <- restore_markers(instance, ord, stale, first_bucket) do
            {:ok, first == nil}
          end
      end
    end
  end

  defp between(opts, step) do
    if fun = Keyword.get(opts, step), do: fun.()
    :ok
  end

  # Rows due before `first_bucket`'s second exist only if a commit landed
  # between the two reads; each deleted second one of them is in gets its marker
  # back.
  defp restore_markers(instance, ord, stale, first_bucket) do
    range = {<<?D, ord::32, 0::64>>, <<?D, ord::32, first_bucket * 1000::64>>}

    with {:ok, {_n, buckets}} <-
           Store.fold(instance, :due, range, {0, MapSet.new()}, fn key, _value, {n, acc} ->
             {_ord, at, _seq, _sink} = Keys.decode_due(key)
             acc = MapSet.put(acc, div(at, 1000))
             if n + 1 >= @verify_max, do: {:halt, {n + 1, acc}}, else: {:cont, {n + 1, acc}}
           end) do
      puts =
        for bucket <- stale,
            MapSet.member?(buckets, bucket),
            do: {:put, :index, Keys.active(bucket, ord), <<>>}

      Store.write(instance, puts, sync: false)
    end
  end

  @doc "A due row, and its active marker: a due row is never written without one."
  @spec due_ops(pos_integer(), integer(), pos_integer(), integer(), integer(), 0 | 1) ::
          [Store.op()]
  def due_ops(ord, at, seq, sink, size, kind) do
    [
      {:put, :index, Keys.due(ord, at, seq, sink), <<size::32, kind::8>>},
      {:put, :index, Keys.active(div(at, 1000), ord), <<>>}
    ]
  end

  @doc """
  Folds a batch's counter merges into one per key, and its repeated active
  markers into one, so a batch of many rows of few tenants writes few operands.
  """
  @spec coalesce([Store.op()]) :: [Store.op()]
  def coalesce(ops) do
    {rest, merges, _seen} =
      Enum.reduce(ops, {[], %{}, MapSet.new()}, fn
        {:merge, :counters, key, delta}, {rest, merges, seen} ->
          {rest, Map.update(merges, key, delta, &(&1 + delta)), seen}

        {:put, :index, <<?A, _::binary>> = key, _value} = op, {rest, merges, seen} ->
          if MapSet.member?(seen, key),
            do: {rest, merges, seen},
            else: {[op | rest], merges, MapSet.put(seen, key)}

        op, {rest, merges, seen} ->
          {[op | rest], merges, seen}
      end)

    Enum.reverse(rest) ++
      for({key, delta} <- merges, delta != 0, do: {:merge, :counters, key, delta})
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
           ord: due.ord,
           kind: due.kind,
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
  claimed by the Pipeline. Walks both indexes — O(backlog) — unless `tenant:`
  names the one tenant the source's rows belong to (an API-managed source owned
  by a tenant): then it reads only that tenant's ranges.
  """
  @spec pending_for_source(atom(), String.t(), keyword()) ::
          {:ok, %{pending: non_neg_integer(), inflight: non_neg_integer()}} | {:error, term()}
  def pending_for_source(instance, source_id, opts \\ []) do
    count = fn rows, n -> {:cont, n + length(rows)} end

    with {:ok, pending} <- fold_source(instance, :due, source_id, 0, count, opts),
         {:ok, inflight} <- fold_source(instance, :inflight, source_id, 0, count, opts) do
      {:ok, %{pending: pending, inflight: inflight}}
    end
  end

  @doc """
  Folds the rows of `source_id` held in index `family` (`:due` or
  `:inflight`): `fun.(rows, acc)` runs once per #{@source_chunk} index keys
  examined, with the matching rows decoded, and returns `{:cont, acc}` or
  `{:halt, acc}`. A row written before rows carried `source_id` is matched by
  its hook's; an index key whose row or hook is gone is skipped. The scan
  reads a snapshot, so `fun` may move the rows it is given. `opts[:tenant]`
  restricts the scan to one tenant's range (a tenant with no rows reads none).
  """
  @spec fold_source(
          atom(),
          :due | :inflight,
          String.t(),
          acc,
          ([source_row()], acc -> {:cont, acc} | {:halt, acc}),
          keyword()
        ) :: {:ok, acc} | {:error, term()}
        when acc: term()
  def fold_source(instance, family, source_id, acc, fun, opts \\ [])
      when family in [:due, :inflight] do
    case source_range(instance, family, Keyword.get(opts, :tenant)) do
      :empty -> {:ok, acc}
      {:error, reason} -> {:error, reason}
      {:ok, range} -> fold_source_range(instance, family, range, source_id, acc, fun)
    end
  end

  defp source_range(instance, family, nil) do
    %{lo: lo, hi: hi} = Keys.family(family)
    _ = instance
    {:ok, {lo, hi}}
  end

  defp source_range(instance, family, tenant) do
    case Tenants.ord(instance, tenant) do
      {:ok, ord} -> {:ok, Keys.range(<<if(family == :due, do: ?D, else: ?F), ord::32>>)}
      :not_found -> :empty
      {:error, reason} -> {:error, reason}
    end
  end

  defp fold_source_range(instance, family, range, source_id, acc, fun) do
    scan =
      Store.fold(instance, family, range, {[], 0, acc}, fn key, value, {chunk, n, acc} ->
        chunk = [index_entry(family, key, value) | chunk]

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

  defp index_entry(:due, key, <<size::32, kind::8>>) do
    {ord, at, seq, sink} = Keys.decode_due(key)
    %{key: key, ord: ord, at: at, seq: seq, sink: sink, size: size, kind: kind}
  end

  defp index_entry(:inflight, key, <<size::32>>) do
    {ord, seq, sink} = Keys.decode_inflight(key)
    %{key: key, ord: ord, seq: seq, sink: sink, size: size}
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
  After a restart nothing is running, so every claimed row is due again, as a
  first attempt. The claim key's value is the stored size, which the due key
  needs.
  """
  @spec recover_inflight(atom(), integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def recover_inflight(instance, now) do
    %{lo: lo, hi: hi} = Keys.family(:inflight)

    with {:ok, ops} <-
           Store.fold(instance, :inflight, {lo, hi}, [], fn key, <<size::32>>, acc ->
             {ord, seq, sink} = Keys.decode_inflight(key)

             {:cont,
              [
                {:delete, :index, key}
                | due_ops(ord, now, seq, sink, size, @first_attempt) ++
                    Counters.ops([{ord, :inflight, -1}, {ord, :pending, 1}]) ++ acc
              ]}
           end),
         :ok <- Store.write(instance, coalesce(ops), sync: true) do
      {:ok, Enum.count(ops, &match?({:delete, :index, <<?F, _::binary>>}, &1))}
    end
  end

  @doc "Move rows from pending to claimed, in one batch."
  @spec claim(atom(), [due()]) :: :ok | {:error, term()}
  def claim(instance, dues) do
    ops =
      Enum.flat_map(dues, fn due ->
        [
          {:delete, :index, due.key},
          {:put, :index, Keys.inflight(due.ord, due.seq, due.sink), <<due.size::32>>}
          | Counters.ops([{due.ord, due_field(due.kind), -1}, {due.ord, :inflight, 1}])
        ]
      end)

    Store.write(instance, coalesce(ops), sync: false)
  end

  defp due_field(@first_attempt), do: :pending
  defp due_field(@retry), do: :scheduled

  # ── op builders ───────────────────────────────────────────────────────────

  @doc """
  The rows of a new delivery (ingest, archive replay, expansion): its row, a
  first-attempt due row with its marker, and the `pending` and `bytes`
  deltas.
  """
  @spec new_ops(pos_integer(), integer(), pos_integer(), integer(), integer(), binary()) ::
          [Store.op()]
  def new_ops(ord, at, seq, sink, size, row_bin) do
    [{:put, :deliveries, Keys.delivery(seq, sink), row_bin}] ++
      due_ops(ord, at, seq, sink, size, @first_attempt) ++
      Counters.ops([{ord, :pending, 1}, {ord, :bytes, size}])
  end

  @doc "An archive obligation of tenant `ord`'s hook."
  @spec archive_ops(pos_integer(), pos_integer(), integer()) :: [Store.op()]
  def archive_ops(ord, seq, size) do
    [{:put, :index, Keys.archive_pending(seq), <<size::32, ord::32>>}] ++
      Counters.ops([{ord, :archive_pending, 1}])
  end

  @doc "A delivered row is gone; the marker tells `Ankusa.Queue.Reclaim` to look at its hook."
  def delivered_ops(ord, seq, sink, size) do
    [
      {:delete, :deliveries, Keys.delivery(seq, sink)},
      {:delete, :index, Keys.inflight(ord, seq, sink)},
      {:put, :index, Keys.cleared(seq, 0, sink), <<>>}
    ] ++ Counters.ops([{ord, :inflight, -1}, {ord, :bytes, -size}])
  end

  @doc "A claimed row back to pending at `at`, as a retry. `row` carries the new attempt count."
  def retry_ops(ord, seq, sink, row, at, error) do
    row = %{row | at: at, error: error}

    [
      {:put, :deliveries, Keys.delivery(seq, sink), encode_row(row)},
      {:delete, :index, Keys.inflight(ord, seq, sink)}
    ] ++
      due_ops(ord, at, seq, sink, row.size, @retry) ++
      Counters.ops([{ord, :inflight, -1}, {ord, :scheduled, 1}])
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

  @typedoc "Where a row leaves from: the claim, or a due key (and its `kind`)."
  @type from :: :inflight | {:due, binary(), 0 | 1}

  defp leave(:inflight, ord, seq, sink),
    do: {[{:delete, :index, Keys.inflight(ord, seq, sink)}], {ord, :inflight, -1}}

  defp leave({:due, key, kind}, ord, _seq, _sink),
    do: {[{:delete, :index, key}], {ord, due_field(kind), -1}}

  @doc """
  Dead-letter the row. `row` carries the final attempt count; `env` is the
  hook or its decode error (see `owner/2`).
  """
  @spec dead_ops(
          from(),
          pos_integer(),
          pos_integer(),
          integer(),
          map(),
          integer(),
          term(),
          term()
        ) ::
          [Store.op()]
  def dead_ops(from, ord, seq, sink, row, at, error, env) do
    row = %{row | state: :dead, at: at, error: error}
    {source_id, id} = owner(env, row)
    {leaves, delta} = leave(from, ord, seq, sink)

    [{:put, :deliveries, Keys.delivery(seq, sink), encode_row(row)}] ++
      leaves ++
      [
        {:put, :index, Keys.dead(at, seq, sink), :erlang.term_to_binary({source_id, id, ord})},
        {:put, :index, Keys.dead_by_tenant(ord, at, seq, sink), <<>>}
      ] ++ Counters.ops([delta, {ord, :dead, 1}, {ord, :dead_bytes, row_size(row)}])
  end

  defp row_size(%{size: size}) when is_integer(size), do: size
  defp row_size(_row), do: 0

  @doc "A row whose delivery row or hook is gone: drop it and what it counted."
  @spec orphan_ops(from(), pos_integer(), pos_integer(), integer(), integer()) :: [Store.op()]
  def orphan_ops(from, ord, seq, sink, size) do
    {leaves, delta} = leave(from, ord, seq, sink)

    [{:delete, :deliveries, Keys.delivery(seq, sink)}] ++
      leaves ++ Counters.ops([delta, {ord, :bytes, -size}])
  end

  @doc """
  Replace an imported row whose sink was never recorded by one pending row for
  every current sink of its source that has none yet. A source with no sinks
  has nothing to deliver, so the row just clears. The imported row is claimed
  (`inflight`) when this runs.
  """
  def expand_ops(ord, seq, unresolved_sink, size, source_id, source_sinks, existing, now) do
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

        new_ops(ord, now, seq, index, size, encode_row(row))
      end)

    cleared =
      if source_sinks == [] do
        [{:put, :index, Keys.cleared(seq, 0, unresolved_sink), <<>>}]
      else
        []
      end

    rows ++ orphan_ops(:inflight, ord, seq, unresolved_sink, size) ++ cleared
  end

  @doc """
  Persist the hook's claim-check ref, so every sink and every retry reuses it,
  and lease the claim's pack to the hook (`Keys.claim_lease/3`, held until the
  hook is reclaimed) with its count, in the same batch: the retention sweeper
  (`Ankusa.ClaimCheck.Sweeper`) never deletes a leased pack.
  """
  def claim_ops(seq, ord, claim) do
    [
      {:put, :index, Keys.claim(seq), :erlang.term_to_binary(claim)},
      {:put, :index, claim_lease_key(claim, seq), <<0::64>>}
      | Counters.ops([{ord, :claim_leases, 1}])
    ]
  end

  @doc "The lease key of the pack `claim` points into, for the hook `seq`."
  @spec claim_lease_key(%{ref: Ref.t()}, pos_integer()) :: binary()
  def claim_lease_key(%{ref: %Ref{} = ref}, seq) do
    {:ok, pack_id, _index} = Ref.locate(ref.claim_id)
    Keys.claim_lease(ref.tenant_id, pack_id, seq)
  end

  # ── the DLQ ───────────────────────────────────────────────────────────────

  # A paged scan over dead rows in `range`, for the replay engine. Keeps keys
  # matching `filter` (`:source_id`, `:id`, `:tenants`) — the dead-letter time is
  # bounded by the range itself. Stops after `limit` hits or `max_scan` keys
  # examined. Returns the hits (ascending), the last key seen (`nil` when the
  # range held nothing), whether the range was exhausted, and the keys scanned.
  # The tenant of a dead row is the ordinal in its value: no hook is read.
  @type hit :: {binary(), pos_integer(), non_neg_integer(), pos_integer()}
  @spec dead_page(atom(), {binary(), binary()}, map(), pos_integer(), pos_integer()) ::
          {:ok, [hit()], binary() | nil, boolean(), non_neg_integer()} | {:error, term()}
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  def dead_page(instance, {lower, upper}, filter, limit, max_scan) do
    with {:ok, ords} <- filter_ords(instance, filter) do
      dead_page_scan(instance, {lower, upper}, filter, ords, limit, max_scan)
    end
  end

  # A tenant filter names tenants that may have no ordinal: those have no rows.
  defp filter_ords(instance, %{tenants: tenants}) when is_list(tenants),
    do: tenant_ords(instance, tenants)

  defp filter_ords(_instance, _filter), do: {:ok, nil}

  defp tenant_ords(instance, tenants) do
    with {:ok, found} <-
           Tenants.lookup(instance, tenants |> Enum.map(&Tenants.normalize/1) |> Enum.uniq()) do
      {:ok, found |> Map.values() |> Enum.sort()}
    end
  end

  defp dead_page_scan(_instance, _range, _filter, [], _limit, _max_scan),
    do: {:ok, [], nil, true, 0}

  # sobelow_skip ["Misc.BinToTerm"]
  defp dead_page_scan(instance, {lower, upper}, filter, ords, limit, max_scan) do
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
          {source_id, id, ord} = :erlang.binary_to_term(value)

          cond do
            not (dead_match?(filter, source_id, id) and tenant_match?(ords, ord)) ->
              {:cont, {scanned + 1, hit_n, hits, key}}

            hit_n >= limit ->
              {:halt, {scanned, hit_n, hits, last}}

            true ->
              {:cont, {scanned + 1, hit_n + 1, [{key, seq, sink, ord} | hits], key}}
          end
        end
      end)

    case result do
      {:ok, {scanned, hit_n, hits, last}} ->
        # The fold ends by halt (limit/max_scan reached) or by running off the
        # range; only the latter means every dead row in range was examined.
        {:ok, Enum.reverse(hits), last, hit_n < limit and scanned < max_scan, scanned}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp tenant_match?(nil, _ord), do: true
  defp tenant_match?(ords, ord), do: ord in ords

  # `matches?/4` minus the `:since` bound: the page's range carries that.
  defp dead_match?(filter, source_id, id) do
    keep?(filter, :source_id, source_id) and keep?(filter, :id, id)
  end

  @typedoc "A dead row a reaper or an operator may evict: `{key, seq, sink, ord, source_id, id}`."
  @type candidate ::
          {binary(), pos_integer(), non_neg_integer(), pos_integer(), String.t(),
           String.t() | nil}

  # The dead rows of `range` (`?x` keys, oldest first) that match `filter`
  # (`:source_id`, `:tenants`): at most `limit` of them, after examining at most
  # `max_scan` keys. Returns the candidates (ascending), the last key examined
  # (`nil` when the range held nothing) and whether the range was exhausted.
  @spec dead_candidates(atom(), {binary(), binary()}, map(), pos_integer(), pos_integer()) ::
          {:ok, [candidate()], binary() | nil, boolean()} | {:error, term()}
  def dead_candidates(instance, range, filter, limit, max_scan) do
    with {:ok, ords} <- filter_ords(instance, filter) do
      candidates_scan(instance, range, filter, ords, limit, max_scan)
    end
  end

  defp candidates_scan(_instance, _range, _filter, [], _limit, _max_scan),
    do: {:ok, [], nil, true}

  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp candidates_scan(instance, range, filter, ords, limit, max_scan) do
    result =
      Store.fold(instance, :dead, range, {0, 0, [], nil, false}, fn key,
                                                                    value,
                                                                    {scanned, n, hits, _last, _} ->
        {_at, seq, sink} = Keys.decode_dead(key)
        {source_id, id, ord} = :erlang.binary_to_term(value)

        {n, hits} =
          if dead_match?(filter, source_id, id) and tenant_match?(ords, ord),
            do: {n + 1, [{key, seq, sink, ord, source_id, id} | hits]},
            else: {n, hits}

        if n >= limit or scanned + 1 >= max_scan,
          do: {:halt, {scanned + 1, n, hits, key, true}},
          else: {:cont, {scanned + 1, n, hits, key, false}}
      end)

    case result do
      {:ok, {_scanned, _n, hits, last, halted?}} -> {:ok, Enum.reverse(hits), last, not halted?}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  The batch that evicts the dead rows `candidates` for good: both dead keys, the
  delivery row, a cleared marker (so `Ankusa.Queue.Reclaim` deletes a hook that
  nobody owes any more) and the counters (`dead`, `bytes`, `dead_bytes`). Returns
  the ops and, per row, what the audit row and the telemetry report
  (`%{key, seq, sink, ord, source_id, id, size}`). Whether a row *may* be evicted
  is the caller's decision (`Ankusa.Queue.DlqReaper`): this only builds the batch.
  """
  @spec evict_ops(atom(), [candidate()]) ::
          {:ok, [Store.op()], [map()]} | {:error, term()}
  def evict_ops(_instance, []), do: {:ok, [], []}

  def evict_ops(instance, candidates) do
    keys =
      Enum.map(candidates, fn {_key, seq, sink, _ord, _source_id, _id} ->
        Keys.delivery(seq, sink)
      end)

    with {:ok, rows} <- Store.multi_get(instance, :deliveries, keys) do
      evicted =
        for {{key, seq, sink, ord, source_id, id}, row} <- Enum.zip(candidates, rows) do
          size =
            with {:ok, bin} <- row,
                 {:ok, %{size: size}} when is_integer(size) <- decode_row(bin) do
              size
            else
              _ -> 0
            end

          %{key: key, seq: seq, sink: sink, ord: ord, source_id: source_id, id: id, size: size}
        end

      {:ok, coalesce(Enum.flat_map(evicted, &evict_row_ops/1)), evicted}
    end
  end

  defp evict_row_ops(%{key: key, seq: seq, sink: sink, ord: ord, size: size}) do
    {at, ^seq, ^sink} = Keys.decode_dead(key)

    [
      {:delete, :index, key},
      {:delete, :index, Keys.dead_by_tenant(ord, at, seq, sink)},
      {:delete, :deliveries, Keys.delivery(seq, sink)},
      {:put, :index, Keys.cleared(seq, 0, sink), <<>>}
      | Counters.ops([{ord, :dead, -1}, {ord, :bytes, -size}, {ord, :dead_bytes, -size}])
    ]
  end

  # Move dead rows back to pending for a replay job: the revived row keeps its
  # hook and gets `replay: replay_id`, `attempts: 0`, a due time of `now` and a
  # fresh retry horizon (`first_attempt_at` dropped: dispatch stamps it again).
  # A dead key whose row is gone is deleted as an orphan. Returns the batch ops
  # and the number of rows actually revived.
  @spec revive_ops(atom(), [hit()], integer(), String.t()) ::
          {:ok, [Ankusa.Store.op()], non_neg_integer()} | {:error, term()}
  def revive_ops(instance, hits, now, replay_id) do
    keys = Enum.map(hits, fn {_key, seq, sink, _ord} -> Keys.delivery(seq, sink) end)

    with {:ok, rows} <- Store.multi_get(instance, :deliveries, keys) do
      {ops, revived} =
        hits
        |> Enum.zip(rows)
        |> Enum.reduce({[], 0}, fn {{key, seq, sink, ord}, result}, {ops, n} ->
          {row_ops, n} = revive_row(key, seq, sink, ord, result, now, replay_id, n)
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
  defp revive_row(key, seq, sink, ord, {:ok, bin}, now, replay_id, n) do
    case decode_row(bin) do
      {:ok, %{size: size} = row} when is_integer(size) ->
        row =
          row
          |> Map.merge(%{state: :pending, attempts: 0, at: now, error: nil})
          |> Map.delete(:first_attempt_at)
          |> Map.put(:replay, replay_id)

        {[
           {:put, :deliveries, Keys.delivery(seq, sink), encode_row(row)}
           | dead_gone(key, ord, seq, sink)
         ] ++
           due_ops(ord, now, seq, sink, size, @first_attempt) ++
           Counters.ops([{ord, :dead, -1}, {ord, :dead_bytes, -size}, {ord, :pending, 1}]), n + 1}

      {:error, reason} ->
        left_in_dlq(seq, sink, reason, n)

      {:ok, _no_size} ->
        left_in_dlq(seq, sink, :malformed_row, n)
    end
  end

  # The row is gone, so what it counted in `bytes` cannot be taken back.
  defp revive_row(key, seq, sink, ord, :not_found, _now, _replay_id, n) do
    {dead_gone(key, ord, seq, sink) ++ Counters.ops([{ord, :dead, -1}]), n}
  end

  # Both dead keys of a row, from the global key.
  defp dead_gone(key, ord, seq, sink) do
    {at, ^seq, ^sink} = Keys.decode_dead(key)
    [{:delete, :index, key}, {:delete, :index, Keys.dead_by_tenant(ord, at, seq, sink)}]
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
  hooks owned by one of these tenants, read from the by-tenant index with no
  hook read; `nil` is every tenant), `:limit`.
  """
  @spec dead(atom(), keyword()) ::
          {:ok, %{total: non_neg_integer(), entries: [map()]}} | {:error, term()}
  def dead(instance, opts) do
    limit = Keyword.fetch!(opts, :limit)

    filter =
      for {k, v} <- Keyword.take(opts, [:source_id, :since]), v != nil, into: %{}, do: {k, v}

    tenants = Keyword.get(opts, :tenants)

    scan =
      if is_list(tenants),
        do: dead_scan_tenants(instance, filter, tenants, limit),
        else: dead_scan(instance, filter, limit)

    with {:ok, {total, kept}} <- scan,
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

  # `{total, kept}` of the dead keys matching `filter`, newest first: every
  # match is counted, the first `limit` are kept as `{at, seq, sink, owner}`.
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp dead_scan(instance, filter, limit) do
    %{lo: lo, hi: hi} = Keys.family(:dead)

    scan =
      Store.fold(
        instance,
        :dead,
        {lo, hi},
        {0, 0, []},
        fn key, value, {total, kept_n, kept} = acc ->
          {at, seq, sink} = Keys.decode_dead(key)
          {source_id, id, _ord} = :erlang.binary_to_term(value)

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

    with {:ok, {total, _kept_n, kept}} <- scan, do: {:ok, {total, Enum.reverse(kept)}}
  end

  # `dead_scan/3` limited to `tenants`: one reverse range scan of each tenant's
  # by-tenant dead index, never a hook read. Only a `:source_id` filter needs the
  # global dead row's value (the owner), read 1 000 keys at a time; the page's
  # owners are read once at the end.
  defp dead_scan_tenants(instance, filter, tenants, limit) do
    with {:ok, ords} <- tenant_ords(instance, tenants),
         {:ok, {total, kept}} <- scan_tenants(instance, ords, filter, limit),
         {:ok, kept} <- fill_owners(instance, kept) do
      {:ok, {total, kept}}
    end
  end

  defp scan_tenants(instance, ords, filter, limit) do
    Enum.reduce_while(ords, {:ok, {0, []}}, fn ord, {:ok, {total, kept}} ->
      case tenant_dead(instance, ord, filter, limit) do
        {:ok, {t, k}} ->
          merged =
            (kept ++ k)
            |> Enum.sort_by(fn {at, seq, sink} -> {at, seq, sink} end, :desc)
            |> Enum.take(limit)

          {:cont, {:ok, {total + t, merged}}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  # `{total, kept}` for one tenant, `kept` newest first, as `{at, seq, sink}`.
  defp tenant_dead(instance, ord, filter, limit) do
    {_lower, hi} = Keys.range(<<?X, ord::32>>)
    range = {<<?X, ord::32, Map.get(filter, :since, 0)::64>>, hi}
    need_owner? = Map.has_key?(filter, :source_id)

    scan =
      Store.fold(
        instance,
        :dead_by_tenant,
        range,
        {{0, 0, []}, [], 0},
        fn key, _value, {acc, chunk, n} ->
          {_ord, at, seq, sink} = Keys.decode_dead_by_tenant(key)

          cond do
            not need_owner? ->
              {:cont, {push_dead(acc, {at, seq, sink}, limit), [], 0}}

            n + 1 < @source_chunk ->
              {:cont, {acc, [{at, seq, sink} | chunk], n + 1}}

            true ->
              case owner_step(instance, [{at, seq, sink} | chunk], filter, limit, acc) do
                {:ok, acc} -> {:cont, {acc, [], 0}}
                {:error, reason} -> {:halt, {:error, reason}}
              end
          end
        end,
        reverse: true
      )

    case scan do
      {:ok, {:error, reason}} ->
        {:error, reason}

      {:ok, {acc, chunk, _n}} ->
        with {:ok, {total, _kept_n, kept}} <- owner_step(instance, chunk, filter, limit, acc),
             do: {:ok, {total, Enum.reverse(kept)}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp push_dead({total, kept_n, kept}, entry, limit) when kept_n < limit,
    do: {total + 1, kept_n + 1, [entry | kept]}

  defp push_dead({total, kept_n, kept}, _entry, _limit), do: {total + 1, kept_n, kept}

  # Folds one chunk of dead keys (`chunk` is oldest first, as the reverse scan
  # built it) into the accumulator, keeping those whose owner matches `filter`.
  defp owner_step(_instance, [], _filter, _limit, acc), do: {:ok, acc}

  # Trusted, internally written data (this node's own store rows).
  # sobelow_skip ["Misc.BinToTerm"]
  defp owner_step(instance, chunk, filter, limit, acc) do
    entries = Enum.reverse(chunk)

    with {:ok, values} <-
           Store.multi_get(
             instance,
             :index,
             Enum.map(entries, fn {at, seq, sink} -> Keys.dead(at, seq, sink) end)
           ) do
      {:ok,
       entries
       |> Enum.zip(values)
       |> Enum.reduce(acc, fn
         {entry, {:ok, value}}, acc ->
           {source_id, id, _ord} = :erlang.binary_to_term(value)
           {at, _seq, _sink} = entry

           if matches?(filter, source_id, id, at), do: push_dead(acc, entry, limit), else: acc

         {_entry, :not_found}, acc ->
           acc
       end)}
    end
  end

  # The owners of the kept page, from the global dead rows.
  # sobelow_skip ["Misc.BinToTerm"]
  defp fill_owners(instance, kept) do
    with {:ok, values} <-
           Store.multi_get(
             instance,
             :index,
             Enum.map(kept, fn {at, seq, sink} -> Keys.dead(at, seq, sink) end)
           ) do
      {:ok,
       for {{at, seq, sink}, value} <- Enum.zip(kept, values) do
         owner =
           case value do
             {:ok, bin} ->
               {source_id, id, _ord} = :erlang.binary_to_term(bin)
               {source_id, id}

             :not_found ->
               {@unknown_source, nil}
           end

         {at, seq, sink, owner}
       end}
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

  # ── what other modules need without building a key ───────────────────────

  @doc """
  The distinct tenants with a marker at or before `now`'s second, oldest marker
  first, at most `max` of them (`Ankusa.Metrics.Gauges` reads their counters).
  """
  @spec active_ords(atom(), integer(), pos_integer()) ::
          {:ok, [pos_integer()]} | {:error, term()}
  def active_ords(instance, now, max) do
    %{lo: lo} = Keys.family(:active)

    with {:ok, {_seen, ords}} <-
           Store.fold(
             instance,
             :active,
             {lo, <<?A, div(now, 1000) + 1::64>>},
             {MapSet.new(), []},
             fn key, _value, {seen, ords} = acc ->
               {_bucket, ord} = Keys.decode_active(key)

               cond do
                 MapSet.member?(seen, ord) -> {:cont, acc}
                 MapSet.size(seen) + 1 >= max -> {:halt, {MapSet.put(seen, ord), [ord | ords]}}
                 true -> {:cont, {MapSet.put(seen, ord), [ord | ords]}}
               end
             end
           ) do
      {:ok, Enum.reverse(ords)}
    end
  end

  @doc """
  `{ord, at}` when `op` puts a due row, else `nil`: the Pipeline lowers a
  tenant's scan cursor to the time of a row it has just made due.
  """
  @spec due_time(Store.op()) :: {pos_integer(), integer()} | nil
  def due_time({:put, :index, <<?D, ord::32, at::64, _::binary>>, _value}), do: {ord, at}
  def due_time(_op), do: nil

  @doc false
  # The format 2 -> 3 migration's rows: a claimed row and a dead row's by-tenant key.
  def inflight_put(ord, seq, sink, size),
    do: {:put, :index, Keys.inflight(ord, seq, sink), <<size::32>>}

  @doc false
  def dead_by_tenant_put(ord, at, seq, sink),
    do: {:put, :index, Keys.dead_by_tenant(ord, at, seq, sink), <<>>}

  # ── per-tenant reads and purge ────────────────────────────────────────────

  @doc "`%{pending, scheduled, inflight, bytes}` of `tenant`: four counter reads."
  @spec pending_for_tenant(atom(), String.t()) ::
          {:ok,
           %{pending: integer(), scheduled: integer(), inflight: integer(), bytes: integer()}}
          | {:error, term()}
  def pending_for_tenant(instance, tenant) do
    counters(instance, tenant, [:pending, :scheduled, :inflight, :bytes])
  end

  @doc "`%{dead}` of `tenant`: one counter read."
  @spec dead_for_tenant(atom(), String.t()) :: {:ok, %{dead: integer()}} | {:error, term()}
  def dead_for_tenant(instance, tenant), do: counters(instance, tenant, [:dead])

  defp counters(instance, tenant, fields) do
    case Tenants.ord(instance, tenant) do
      {:ok, ord} ->
        with {:ok, all} <- Counters.read(instance, ord), do: {:ok, Map.take(all, fields)}

      :not_found ->
        {:ok, Map.new(fields, &{&1, 0})}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @purge_chunk 500

  @doc """
  Removes tenant `tenant`'s `:pending` (every due row, first attempts and
  retries), `:inflight` (claimed) and/or `:dead` rows (`kinds` a subset of
  `[:pending, :inflight, :dead]`), 500 at a time, one synced batch per chunk: the
  index keys, the delivery row, a cleared marker so `Ankusa.Queue.Reclaim`
  removes hooks nobody owes, and the counter deltas. Idempotent, so a run that
  stopped part-way resumes by being called again. Each chunk runs inside the
  dispatch Pipeline when it runs on this node, so it cannot race a claim; a
  replay job over the tenant must not run at the same time (it moves dead rows
  the same way).

  `:inflight` is for a tenant dispatch has stopped claiming (`Ankusa.Tenant.Gate`)
  and whose attempts have all settled (`Ankusa.Tenant.Erase` waits for its
  `inflight` counter to reach 0): a row claimed by a running attempt would be
  settled over by that attempt's outcome. What it removes is what a crashed
  node left claimed. Returns the rows removed per kind (`:pending` and `:dead`
  always, `:inflight` when asked for).
  """
  @spec purge_tenant(atom(), String.t(), [:pending | :inflight | :dead]) ::
          {:ok,
           %{
             required(:pending) => non_neg_integer(),
             required(:dead) => non_neg_integer(),
             optional(:inflight) => non_neg_integer()
           }}
          | {:error, term()}
  def purge_tenant(instance, tenant, kinds) do
    zero = Map.merge(%{pending: 0, dead: 0}, Map.new(kinds, &{&1, 0}))

    case Tenants.ord(instance, tenant) do
      {:ok, ord} ->
        Enum.reduce_while(kinds, {:ok, zero}, fn kind, {:ok, removed} ->
          case purge_kind(instance, ord, kind, 0) do
            {:ok, n} -> {:cont, {:ok, Map.put(removed, kind, n)}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)

      :not_found ->
        {:ok, zero}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Removes the active-tenant markers (`?A`) of ordinal `ord`: a tenant's due rows
  are gone, so a marker for it only makes a dispatch tick look at an empty
  range. Idempotent; `{:ok, markers removed}`.
  """
  @spec purge_markers(atom(), pos_integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def purge_markers(instance, ord) do
    Sweep.run(
      instance,
      :active,
      fn key, _value ->
        case Keys.decode_active(key) do
          {_bucket, ^ord} -> {:ok, {:delete, :index, key}}
          _ -> :skip
        end
      end,
      fn deletes ->
        with :ok <- Store.write(instance, deletes, sync: true), do: {:ok, length(deletes)}
      end,
      @purge_chunk
    )
  end

  @doc """
  Settles the archive obligations (`?a`) of tenant ordinal `ord`: the rows go,
  with their `archive_pending` counts, and `Ankusa.Queue.Reclaim` deletes the
  hooks nothing else holds. The obligations are found by a walk of the whole
  family (the key carries the sequence, not the tenant), so the cost is the
  archive backlog, once per erase. Idempotent; `{:ok, obligations removed}`.
  """
  @spec purge_archive(atom(), pos_integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def purge_archive(instance, ord) do
    Sweep.run(
      instance,
      :archive_pending,
      fn
        <<?a, seq::64>>, <<_size::32, ^ord::32>> -> {:ok, seq}
        _key, _value -> :skip
      end,
      fn seqs ->
        ops =
          Enum.flat_map(seqs, fn seq ->
            [
              {:delete, :index, Keys.archive_pending(seq)},
              {:put, :index, Keys.cleared(seq, 1, 0), <<>>}
            ]
          end) ++ Counters.ops([{ord, :archive_pending, -length(seqs)}])

        with :ok <- Store.write(instance, coalesce(ops), sync: true) do
          _ = Reclaim.run(instance, Enum.map(seqs, &{&1, Keys.cleared(&1, 1, 0)}))
          {:ok, length(seqs)}
        end
      end,
      @purge_chunk
    )
  end

  defp purge_kind(instance, ord, kind, removed) do
    case exclusive(instance, fn -> purge_chunk(instance, ord, kind) end) do
      {:ok, 0} -> {:ok, removed}
      {:ok, n} -> purge_kind(instance, ord, kind, removed + n)
      {:error, reason} -> {:error, reason}
    end
  end

  # Runs `fun` in the Pipeline process, the only one that claims rows.
  defp exclusive(instance, fun) do
    case Ankusa.whereis(instance, :dispatch) do
      pid when is_pid(pid) -> GenServer.call(pid, {:exclusive, fun}, :infinity)
      nil -> fun.()
    end
  end

  defp purge_chunk(instance, ord, :pending) do
    {lower, upper} = Keys.range(<<?D, ord::32>>)

    with {:ok, {_n, rows}} <-
           Store.fold(instance, :due, {lower, upper}, {0, []}, fn key,
                                                                  <<size::32, kind::8>>,
                                                                  {n, acc} ->
             acc = [{key, size, kind} | acc]
             if n + 1 >= @purge_chunk, do: {:halt, {n + 1, acc}}, else: {:cont, {n + 1, acc}}
           end) do
      ops =
        Enum.flat_map(rows, fn {key, size, kind} ->
          {_ord, _at, seq, sink} = Keys.decode_due(key)

          [
            {:delete, :index, key},
            {:delete, :deliveries, Keys.delivery(seq, sink)},
            {:put, :index, Keys.cleared(seq, 0, sink), <<>>}
            | Counters.ops([{ord, due_field(kind), -1}, {ord, :bytes, -size}])
          ]
        end)

      finish_purge(instance, rows, ops, fn {key, _size, _kind} ->
        {_ord, _at, seq, sink} = Keys.decode_due(key)
        {seq, sink}
      end)
    end
  end

  defp purge_chunk(instance, ord, :inflight) do
    {lower, upper} = Keys.range(<<?F, ord::32>>)

    with {:ok, {_n, rows}} <-
           Store.fold(instance, :inflight, {lower, upper}, {0, []}, fn key,
                                                                       <<size::32>>,
                                                                       {n, acc} ->
             acc = [{key, size} | acc]
             if n + 1 >= @purge_chunk, do: {:halt, {n + 1, acc}}, else: {:cont, {n + 1, acc}}
           end) do
      ops =
        Enum.flat_map(rows, fn {key, size} ->
          {_ord, seq, sink} = Keys.decode_inflight(key)

          [
            {:delete, :index, key},
            {:delete, :deliveries, Keys.delivery(seq, sink)},
            {:put, :index, Keys.cleared(seq, 0, sink), <<>>}
            | Counters.ops([{ord, :inflight, -1}, {ord, :bytes, -size}])
          ]
        end)

      finish_purge(instance, rows, ops, fn {key, _size} ->
        {_ord, seq, sink} = Keys.decode_inflight(key)
        {seq, sink}
      end)
    end
  end

  defp purge_chunk(instance, ord, :dead) do
    {lower, upper} = Keys.range(<<?X, ord::32>>)

    with {:ok, {_n, keys}} <-
           Store.fold(instance, :dead_by_tenant, {lower, upper}, {0, []}, fn key,
                                                                             _value,
                                                                             {n, acc} ->
             acc = [Keys.decode_dead_by_tenant(key) | acc]
             if n + 1 >= @purge_chunk, do: {:halt, {n + 1, acc}}, else: {:cont, {n + 1, acc}}
           end),
         {:ok, sizes} <- row_sizes(instance, keys) do
      ops =
        keys
        |> Enum.zip(sizes)
        |> Enum.flat_map(fn {{_ord, at, seq, sink}, size} ->
          [
            {:delete, :index, Keys.dead_by_tenant(ord, at, seq, sink)},
            {:delete, :index, Keys.dead(at, seq, sink)},
            {:delete, :deliveries, Keys.delivery(seq, sink)},
            {:put, :index, Keys.cleared(seq, 0, sink), <<>>}
            | Counters.ops([{ord, :dead, -1}, {ord, :bytes, -size}, {ord, :dead_bytes, -size}])
          ]
        end)

      finish_purge(instance, keys, ops, fn {_ord, _at, seq, sink} -> {seq, sink} end)
    end
  end

  defp row_sizes(instance, keys) do
    with {:ok, rows} <-
           Store.multi_get(
             instance,
             :deliveries,
             Enum.map(keys, fn {_ord, _at, seq, sink} -> Keys.delivery(seq, sink) end)
           ) do
      {:ok,
       Enum.map(rows, fn
         {:ok, bin} ->
           case decode_row(bin) do
             {:ok, %{size: size}} when is_integer(size) -> size
             _ -> 0
           end

         :not_found ->
           0
       end)}
    end
  end

  defp finish_purge(_instance, [], _ops, _pair), do: {:ok, 0}

  defp finish_purge(instance, rows, ops, pair) do
    pairs =
      Enum.map(rows, fn row ->
        {seq, sink} = pair.(row)
        {seq, Keys.cleared(seq, 0, sink)}
      end)

    with :ok <- Store.write(instance, coalesce(ops), sync: true) do
      # A failed reclaim only defers: the markers stay, and the sweep finds them.
      _ = Reclaim.run(instance, pairs)
      {:ok, length(rows)}
    end
  end
end
