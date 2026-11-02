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
  # Ordering lanes (ADR 0004, `Deliveries.Lanes`): a row of an ordered sink also
  # carries `lane` (16 bytes) and `ordering_key`. It is a member (`?O`, waiting its
  # turn, in `seq` order, with no due row), the head of its lane (`?H` plus a due
  # row of kind 2 or 3, or a claim) or dead. Only the head is ever claimed; its
  # terminal transition (`delivered_ops/5`, `dead_ops/8`, `orphan_ops/6`) carries a
  # `{:lane_release, ord, lane}` placeholder that the Pipeline expands
  # (`Lanes.expand/3`) in the step that writes the batch.
  #
  # Op builders are pure and return `[Ankusa.Store.op()]`; the caller decides
  # when (and with what durability) to write them.

  require Logger

  alias Ankusa.ClaimCheck.Ref
  alias Ankusa.{Envelope, Store}
  alias Ankusa.Queue.Deliveries.{Counters, Lanes}
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
          kind: 0 | 1 | 2 | 3
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
  # expansion), 1 a retry, 2 the first attempt of a lane head and 3 a retry of
  # one (ADR 0004). It is what keeps the `pending` and `scheduled` counters exact
  # without a clock, and `lanes_blocked` exact for the heads waiting out a retry.
  @first_attempt 0
  @retry 1
  @lane_first Lanes.first_attempt_kind()
  @lane_retry Lanes.retry_kind()

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
  the steps) exist for the test that interleaves a commit with each step. With
  `lanes: true` a tenant that still has a lane marker (`?P`: a member waiting for
  its fsync) keeps its active markers, so the next scan visits it and the sweep
  promotes the member (ADR 0004, section 4.6); it is reported as not drained.
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
          with {:ok, held?} <- lanes_held?(instance, ord, opts) do
            if held?,
              do: {:ok, false},
              else: delete_stale(instance, ord, stale, first, first_bucket, opts)
          end
      end
    end
  end

  defp lanes_held?(instance, ord, opts) do
    if Keyword.get(opts, :lanes, false), do: Lanes.any_marker?(instance, ord), else: {:ok, false}
  end

  defp delete_stale(instance, ord, stale, first, first_bucket, opts) do
    with :ok <-
           Store.write(instance, Enum.map(stale, &{:delete, :index, Keys.active(&1, ord)}),
             sync: false
           ),
         :ok <- between(opts, :after_delete),
         :ok <- restore_markers(instance, ord, stale, first_bucket) do
      {:ok, first == nil}
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
  @spec due_ops(pos_integer(), integer(), pos_integer(), integer(), integer(), 0..3) ::
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
  Whether `op` puts an active-tenant marker (`due_ops/6`) or a lane marker
  (`Lanes.member_ops/6`). A marker is shared by every due row (or member) of its
  tenant (or lane), so undoing a batch (`Ankusa.Queue.Writer`, a failed fsync)
  must leave it: a stale one is swept.
  """
  @spec active_marker_put?(Store.op()) :: boolean()
  def active_marker_put?({:put, :index, <<?A, _::binary>>, _value}), do: true
  def active_marker_put?({:put, :index, <<?P, _::binary>>, _value}), do: true
  def active_marker_put?(_op), do: false

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
  in the due index (due now or waiting out a backoff) or, in an ordering lane,
  wait their turn as members; `inflight` rows are claimed by the Pipeline. Walks
  the indexes — O(backlog) — unless `tenant:` names the one tenant the source's
  rows belong to (an API-managed source owned by a tenant): then it reads only
  that tenant's ranges.
  """
  @spec pending_for_source(atom(), String.t(), keyword()) ::
          {:ok, %{pending: non_neg_integer(), inflight: non_neg_integer()}} | {:error, term()}
  def pending_for_source(instance, source_id, opts \\ []) do
    count = fn rows, n -> {:cont, n + length(rows)} end

    with {:ok, due} <- fold_source(instance, :due, source_id, 0, count, opts),
         {:ok, members} <- fold_source(instance, :lane_members, source_id, 0, count, opts),
         {:ok, inflight} <- fold_source(instance, :inflight, source_id, 0, count, opts) do
      {:ok, %{pending: due + members, inflight: inflight}}
    end
  end

  @doc """
  Folds the rows of `source_id` held in index `family` (`:due`, `:inflight` or
  `:lane_members`): `fun.(rows, acc)` runs once per #{@source_chunk} index keys
  examined, with the matching rows decoded, and returns `{:cont, acc}` or
  `{:halt, acc}`. A row written before rows carried `source_id` is matched by
  its hook's; an index key whose row or hook is gone is skipped. The scan
  reads a snapshot, so `fun` may move the rows it is given. `opts[:tenant]`
  restricts the scan to one tenant's range (a tenant with no rows reads none).
  A lane member's entry has `kind: :member`; `source_from/1` names where it
  leaves from.
  """
  @spec fold_source(
          atom(),
          :due | :inflight | :lane_members,
          String.t(),
          acc,
          ([source_row()], acc -> {:cont, acc} | {:halt, acc}),
          keyword()
        ) :: {:ok, acc} | {:error, term()}
        when acc: term()
  def fold_source(instance, family, source_id, acc, fun, opts \\ [])
      when family in [:due, :inflight, :lane_members] do
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
      {:ok, ord} -> {:ok, Keys.range(<<source_prefix(family), ord::32>>)}
      :not_found -> :empty
      {:error, reason} -> {:error, reason}
    end
  end

  defp source_prefix(:due), do: ?D
  defp source_prefix(:inflight), do: ?F
  defp source_prefix(:lane_members), do: ?O

  @doc "Where a row of `fold_source/6` leaves from, as `dead_ops/8` and `orphan_ops/6` take it."
  @spec source_from(map()) :: from()
  def source_from(%{kind: :member, key: key}), do: {:member, key}
  def source_from(%{kind: kind, key: key}), do: {:due, key, kind}

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

  defp index_entry(:lane_members, key, <<size::32>>) do
    {ord, lane, seq, sink} = Keys.decode_lane_member(key)
    %{key: key, ord: ord, lane: lane, seq: seq, sink: sink, size: size, kind: :member}
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

  # Claimed rows read per `deliveries` read when `recover_inflight/2` looks for
  # lane heads.
  @recover_chunk 500

  @doc """
  After a restart nothing is running, so every claimed row is due again, as a
  first attempt (the first attempt of a lane head, kind 2, when its row names a
  lane: its `?H` is untouched and the members behind it stay, ADR 0004 section
  4.7). The claim key's value is the stored size, which the due key needs.
  """
  @spec recover_inflight(atom(), integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def recover_inflight(instance, now) do
    %{lo: lo, hi: hi} = Keys.family(:inflight)

    with {:ok, claimed} <-
           Store.fold(instance, :inflight, {lo, hi}, [], fn key, <<size::32>>, acc ->
             {:cont, [{key, size} | acc]}
           end),
         {:ok, kinds} <- recover_kinds(instance, claimed),
         ops = Enum.flat_map(Enum.zip(claimed, kinds), &recover_ops(&1, now)),
         :ok <- Store.write(instance, coalesce(ops), sync: true) do
      {:ok, length(claimed)}
    end
  end

  defp recover_kinds(_instance, []), do: {:ok, []}

  defp recover_kinds(instance, claimed) do
    claimed
    |> Enum.chunk_every(@recover_chunk)
    |> Enum.reduce_while({:ok, []}, fn chunk, {:ok, acc} ->
      keys =
        Enum.map(chunk, fn {key, _size} ->
          {_ord, seq, sink} = Keys.decode_inflight(key)
          Keys.delivery(seq, sink)
        end)

      case Store.multi_get(instance, :deliveries, keys) do
        {:ok, rows} -> {:cont, {:ok, [Enum.map(rows, &recover_kind/1) | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, kinds} -> {:ok, kinds |> Enum.reverse() |> List.flatten()}
      {:error, reason} -> {:error, reason}
    end
  end

  defp recover_kind({:ok, bin}) do
    case decode_row(bin) do
      {:ok, %{lane: lane}} when is_binary(lane) -> @lane_first
      _ -> @first_attempt
    end
  end

  defp recover_kind(:not_found), do: @first_attempt

  defp recover_ops({{key, size}, kind}, now) do
    {ord, seq, sink} = Keys.decode_inflight(key)

    [
      {:delete, :index, key}
      | due_ops(ord, now, seq, sink, size, kind) ++
          Counters.ops([{ord, :inflight, -1}, {ord, :pending, 1}])
    ]
  end

  @doc "Move rows from pending to claimed, in one batch."
  @spec claim(atom(), [due()]) :: :ok | {:error, term()}
  def claim(instance, dues) do
    ops =
      Enum.flat_map(dues, fn due ->
        [
          {:delete, :index, due.key},
          {:put, :index, Keys.inflight(due.ord, due.seq, due.sink), <<due.size::32>>}
          | Counters.ops(
              [{due.ord, due_field(due.kind), -1}, {due.ord, :inflight, 1}] ++
                blocked(due.ord, due.kind)
            )
        ]
      end)

    Store.write(instance, coalesce(ops), sync: false)
  end

  defp due_field(kind) when kind in [@first_attempt, @lane_first], do: :pending
  defp due_field(kind) when kind in [@retry, @lane_retry], do: :scheduled

  # A lane head leaving the due index as a retry is a head no longer waiting.
  defp blocked(ord, @lane_retry), do: [{ord, :lanes_blocked, -1}]
  defp blocked(_ord, _kind), do: []

  defp lane?(%{lane: lane}) when is_binary(lane), do: true
  defp lane?(_row), do: false

  # ── op builders ───────────────────────────────────────────────────────────

  @doc """
  A new delivery row: `module` bound at ack time, due at `at`, the stored hook's
  `size` and its `source_id`. `opts`: `:replay` (a job id, on a row a replay
  job or a released quarantine hook makes) and `:lane` (`{lane, key}`, on a row
  of an ordered sink, ADR 0004). The one place a delivery row's shape is built
  for a new hook.
  """
  @spec new_row(module(), integer(), integer(), String.t(), keyword()) :: map()
  def new_row(module, at, size, source_id, opts \\ []) do
    base = %{
      module: module,
      state: :pending,
      attempts: 0,
      at: at,
      error: nil,
      size: size,
      source_id: source_id
    }

    base =
      case Keyword.get(opts, :replay) do
        replay when is_binary(replay) -> Map.put(base, :replay, replay)
        _ -> base
      end

    case Keyword.get(opts, :lane) do
      {lane, key} when is_binary(lane) and is_binary(key) ->
        Map.merge(base, %{lane: lane, ordering_key: key})

      nil ->
        base
    end
  end

  @doc """
  The rows of a new delivery (ingest, archive replay, expansion): its row, a
  first-attempt due row with its marker, and the `pending` and `bytes`
  deltas. With a `lane` (ADR 0004: the row of an ordered sink) the delivery
  joins the lane instead: a member (`?O`) and the lane's marker, no due row, and
  `lane_members +1`; the Pipeline promotes it when it is the lane's turn.
  """
  @spec new_ops(
          pos_integer(),
          integer(),
          pos_integer(),
          integer(),
          integer(),
          binary(),
          Lanes.lane() | nil
        ) :: [Store.op()]
  def new_ops(ord, at, seq, sink, size, row_bin, lane \\ nil)

  def new_ops(ord, at, seq, sink, size, row_bin, nil) do
    [{:put, :deliveries, Keys.delivery(seq, sink), row_bin}] ++
      due_ops(ord, at, seq, sink, size, @first_attempt) ++
      Counters.ops([{ord, :pending, 1}, {ord, :bytes, size}])
  end

  def new_ops(ord, at, seq, sink, size, row_bin, lane) do
    [{:put, :deliveries, Keys.delivery(seq, sink), row_bin}] ++
      Lanes.member_ops(ord, at, seq, sink, size, lane) ++
      Counters.ops([{ord, :pending, 1}, {ord, :bytes, size}])
  end

  @doc "An archive obligation of tenant `ord`'s hook."
  @spec archive_ops(pos_integer(), pos_integer(), integer()) :: [Store.op()]
  def archive_ops(ord, seq, size) do
    [{:put, :index, Keys.archive_pending(seq), <<size::32, ord::32>>}] ++
      Counters.ops([{ord, :archive_pending, 1}])
  end

  @doc """
  A delivered row is gone; the marker tells `Ankusa.Queue.Reclaim` to look at its
  hook. With the `row` of a lane head the batch carries the lane's release.
  """
  def delivered_ops(ord, seq, sink, size, row \\ nil) do
    [
      {:delete, :deliveries, Keys.delivery(seq, sink)},
      {:delete, :index, Keys.inflight(ord, seq, sink)},
      {:put, :index, Keys.cleared(seq, 0, sink), <<>>}
    ] ++ Counters.ops([{ord, :inflight, -1}, {ord, :bytes, -size}]) ++ Lanes.release_op(ord, row)
  end

  @doc """
  A claimed row back to pending at `at`, as a retry. `row` carries the new attempt
  count. A lane head stays the head: its due row is kind 3 and `lanes_blocked`
  counts it until it is claimed again.
  """
  def retry_ops(ord, seq, sink, row, at, error) do
    row = %{row | at: at, error: error}

    {kind, blocked} =
      if lane?(row), do: {@lane_retry, [{ord, :lanes_blocked, 1}]}, else: {@retry, []}

    [
      {:put, :deliveries, Keys.delivery(seq, sink), encode_row(row)},
      {:delete, :index, Keys.inflight(ord, seq, sink)}
    ] ++
      due_ops(ord, at, seq, sink, row.size, kind) ++
      Counters.ops([{ord, :inflight, -1}, {ord, :scheduled, 1}] ++ blocked)
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

  @typedoc """
  Where a row leaves from: the claim, a due key (and its `kind`), or a lane's
  member key.
  """
  @type from :: :inflight | {:due, binary(), 0..3} | {:member, binary()}

  defp leave(:inflight, ord, seq, sink),
    do: {[{:delete, :index, Keys.inflight(ord, seq, sink)}], [{ord, :inflight, -1}]}

  defp leave({:due, key, kind}, ord, _seq, _sink),
    do: {[{:delete, :index, key}], [{ord, due_field(kind), -1} | blocked(ord, kind)]}

  defp leave({:member, key}, ord, _seq, _sink),
    do: {[{:delete, :index, key}], [{ord, :pending, -1}, {ord, :lane_members, -1}]}

  # Only a lane's head releases its lane when it leaves; a member leaves nothing behind.
  defp release(:inflight, ord, row), do: Lanes.release_op(ord, row)

  defp release({:due, _key, kind}, ord, row) when kind in [@lane_first, @lane_retry],
    do: Lanes.release_op(ord, row)

  defp release(_from, _ord, _row), do: []

  @doc """
  Dead-letter the row. `row` carries the final attempt count; `env` is the
  hook or its decode error (see `owner/2`). A lane head's batch carries the
  lane's release: a poison hook must not hold its key forever.

  `error` is the reason text, or `{class, text}` with a `t:Ankusa.Sink.error_class/0`
  `class`: the row then also carries `error_class`, which the DLQ listing and a
  replay job filter on. A row written without one (before the class existed, or
  by a caller that does not know it) has no `error_class` and matches no class
  filter.
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
    {error, class} = split_error(error)
    row = row |> Map.merge(%{state: :dead, at: at, error: error}) |> put_error_class(class)
    {source_id, id} = owner(env, row)
    {leaves, deltas} = leave(from, ord, seq, sink)

    [{:put, :deliveries, Keys.delivery(seq, sink), encode_row(row)}] ++
      leaves ++
      [
        {:put, :index, Keys.dead(at, seq, sink), :erlang.term_to_binary({source_id, id, ord})},
        {:put, :index, Keys.dead_by_tenant(ord, at, seq, sink), <<>>}
      ] ++
      Counters.ops(deltas ++ [{ord, :dead, 1}, {ord, :dead_bytes, row_size(row)}]) ++
      release(from, ord, row)
  end

  # `Ankusa.Sink.error_classes/0`, spelled out so this module has no compile-time
  # dependency on the sink behaviour.
  @error_classes [:permanent, :throttled, :transient]

  defp split_error({class, text}) when class in @error_classes and is_binary(text),
    do: {text, class}

  defp split_error(error), do: {error, nil}

  defp put_error_class(row, nil), do: Map.delete(row, :error_class)
  defp put_error_class(row, class), do: Map.put(row, :error_class, class)

  defp row_size(%{size: size}) when is_integer(size), do: size
  defp row_size(_row), do: 0

  @doc """
  A row whose delivery row or hook is gone: drop it and what it counted. Given
  the `row` of a lane head (the hook is gone, the row is not), the batch carries
  the lane's release.
  """
  @spec orphan_ops(from(), pos_integer(), pos_integer(), integer(), integer(), map() | nil) ::
          [Store.op()]
  def orphan_ops(from, ord, seq, sink, size, row \\ nil) do
    {leaves, deltas} = leave(from, ord, seq, sink)

    [{:delete, :deliveries, Keys.delivery(seq, sink)}] ++
      leaves ++ Counters.ops(deltas ++ [{ord, :bytes, -size}]) ++ release(from, ord, row)
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
  # matching `filter` (`:source_id`, `:id`, `:sink`, `:tenants`) — the dead-letter time is
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
            not (dead_match?(filter, source_id, id, sink) and tenant_match?(ords, ord)) ->
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

  # The filter's `:source_id`, `:id` and `:sink`: the page's range carries the
  # time bounds.
  defp dead_match?(filter, source_id, id, sink) do
    keep?(filter, :source_id, source_id) and keep?(filter, :id, id) and
      keep?(filter, :sink, sink)
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
          if dead_match?(filter, source_id, id, sink) and tenant_match?(ords, ord),
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
  # A dead key whose row is gone is deleted as an orphan. With `error_class`, a
  # row of another class (or none) stays dead and is not counted: the job's
  # cursor moves past it. Returns the batch ops and the number of rows actually
  # revived.
  @spec revive_ops(atom(), [hit()], integer(), String.t(), Ankusa.Sink.error_class() | nil) ::
          {:ok, [Ankusa.Store.op()], non_neg_integer()} | {:error, term()}
  def revive_ops(instance, hits, now, replay_id, error_class \\ nil) do
    keys = Enum.map(hits, fn {_key, seq, sink, _ord} -> Keys.delivery(seq, sink) end)

    with {:ok, rows} <- Store.multi_get(instance, :deliveries, keys) do
      {ops, revived} =
        hits
        |> Enum.zip(rows)
        |> Enum.reduce({[], 0}, fn {{key, seq, sink, ord}, result}, {ops, n} ->
          {row_ops, n} =
            revive_row({key, seq, sink, ord}, result, {now, replay_id, error_class}, n)

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
  defp revive_row({key, seq, sink, ord}, {:ok, bin}, {now, replay_id, error_class}, n) do
    case decode_row(bin) do
      {:ok, %{size: size} = row} when is_integer(size) ->
        if error_class == nil or Map.get(row, :error_class) == error_class do
          row =
            row
            |> Map.merge(%{state: :pending, attempts: 0, at: now, error: nil})
            |> Map.drop([:first_attempt_at, :error_class])
            |> Map.put(:replay, replay_id)

          {[
             {:put, :deliveries, Keys.delivery(seq, sink), encode_row(row)}
             | dead_gone(key, ord, seq, sink)
           ] ++
             requeue_ops(ord, now, seq, sink, size, row) ++
             Counters.ops([{ord, :dead, -1}, {ord, :dead_bytes, -size}, {ord, :pending, 1}]),
           n + 1}
        else
          {[], n}
        end

      {:error, reason} ->
        left_in_dlq(seq, sink, reason, n)

      {:ok, _no_size} ->
        left_in_dlq(seq, sink, :malformed_row, n)
    end
  end

  # The row is gone, so what it counted in `bytes` cannot be taken back.
  defp revive_row({key, seq, sink, ord}, :not_found, _job, n) do
    {dead_gone(key, ord, seq, sink) ++ Counters.ops([{ord, :dead, -1}]), n}
  end

  # A revived row of a lane re-enters as a member, not as a due row (ADR 0004,
  # section 4.8): it runs when it is the lowest waiting member, never beside the
  # head, and before any later member.
  defp requeue_ops(ord, now, seq, sink, size, %{lane: lane}) when is_binary(lane),
    do: Lanes.member_ops(ord, now, seq, sink, size, lane)

  defp requeue_ops(ord, now, seq, sink, size, _row),
    do: due_ops(ord, now, seq, sink, size, @first_attempt)

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

  # The most dead keys one call of `dead/2` examines, whatever the DLQ holds: a
  # page costs this much at worst, not the size of the DLQ. A tenant-scoped
  # listing splits the budget across the tenant indexes it scans
  # (`scan_sources/5`), so a page is this many keys and row reads at most
  # whatever the filters and the tenants.
  @list_max_scan 10_000
  # Candidate keys a filtered listing checks per batch of store reads.
  @list_chunk 1_000
  # A tenant-scoped listing reads each tenant's own index up to this many tenants;
  # beyond it, one scan of the global dead family with the tenants as a predicate.
  @list_tenant_indexes 8
  @list_cursor_bytes 18

  @doc """
  One page of the dead rows, newest first, in a bounded number of reads.

  `opts`: `:limit` (the page size), `:cursor` (the `next_cursor` of the previous
  page: the page resumes below it), `:source_id`, `:since` (dead-lettered at or
  after, ms), `:sink` (the sink index), `:error_class` (`t:Ankusa.Sink.error_class/0`),
  and `:tenants` (only rows of hooks owned by one of these tenants, read from the
  by-tenant index with no hook read; `nil` is every tenant).

  A call examines at most #{@list_max_scan} dead keys, split across the tenant
  indexes a tenant-scoped listing scans. A filter that matches little can
  therefore make a page short, or empty, with `next_cursor` set: keep asking
  until it is `nil`. `next_cursor` is `nil` on the last page.

  `total` is the number of matching rows when that is known without a scan: no
  filter but `:tenants` (a counter read), or a filter whose scan reached the end
  of the DLQ within this one page (no `:cursor`). Otherwise it is `nil`. A row
  written before dead letters carried a class has no `error_class` and matches no
  `:error_class` filter.
  """
  @spec dead(atom(), keyword()) ::
          {:ok, %{total: non_neg_integer() | nil, entries: [map()], next_cursor: binary() | nil}}
          | {:error, term()}
  def dead(instance, opts) do
    limit = Keyword.fetch!(opts, :limit)
    cursor = Keyword.get(opts, :cursor)
    tenants = Keyword.get(opts, :tenants)

    filter =
      for {k, v} <- Keyword.take(opts, [:source_id, :since, :sink, :error_class]),
          v != nil,
          into: %{},
          do: {k, v}

    if cursor != nil and not (is_binary(cursor) and byte_size(cursor) == @list_cursor_bytes) do
      {:error, :invalid_cursor}
    else
      with {:ok, sources} <- dead_sources(instance, tenants),
           {:ok, scans} <- scan_sources(instance, sources, filter, cursor, max(limit, 1)),
           {hits, frontier} = merge_scans(scans),
           {:ok, total} <- dead_total(instance, sources, filter, cursor, frontier, hits),
           {:ok, kept} <- fill_owners(instance, Enum.take(hits, limit)),
           {:ok, entries, orphans} <- list_entries(instance, kept) do
        # A dead key whose row or hook is gone lists nothing, so orphans on the
        # returned page are not counted either, and are logged since it should
        # not happen; replay deletes them.
        if orphans != [] do
          Logger.warning(
            "[ankusa] DLQ key(s) with no delivery row or hook, not listed: #{inspect(Enum.reverse(orphans))}"
          )
        end

        {:ok,
         %{
           total: total && total - length(orphans),
           entries: entries,
           next_cursor: next_cursor(hits, limit, frontier)
         }}
      end
    end
  end

  # Where `dead/2` reads: `:all` (the global dead family), `{:index, ords}` (one
  # by-tenant index per ordinal) or `{:filtered, ords}` (the global family, the
  # tenants a predicate on each row's own ordinal).
  defp dead_sources(_instance, nil), do: {:ok, :all}

  defp dead_sources(instance, tenants) when is_list(tenants) do
    with {:ok, ords} <- tenant_ords(instance, tenants) do
      if length(ords) <= @list_tenant_indexes,
        do: {:ok, {:index, ords}},
        else: {:ok, {:filtered, ords}}
    end
  end

  defp scan_sources(instance, :all, filter, cursor, limit) do
    with {:ok, scan} <- scan_dead(instance, :dead, nil, filter, cursor, limit, @list_max_scan),
         do: {:ok, [scan]}
  end

  defp scan_sources(instance, {:filtered, ords}, filter, cursor, limit) do
    with {:ok, scan} <-
           scan_dead(
             instance,
             :dead,
             nil,
             Map.put(filter, :ords, ords),
             cursor,
             limit,
             @list_max_scan
           ),
         do: {:ok, [scan]}
  end

  defp scan_sources(instance, {:index, ords}, filter, cursor, limit) do
    # The call's budget, split evenly: a page across several tenant indexes
    # costs `@list_max_scan` keys and row reads at most, not one budget per
    # index.
    budget = div(@list_max_scan, max(length(ords), 1))

    Enum.reduce_while(ords, {:ok, []}, fn ord, {:ok, scans} ->
      case scan_dead(instance, :dead_by_tenant, ord, filter, cursor, limit, budget) do
        {:ok, scan} -> {:cont, {:ok, [scan | scans]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  # One reverse range scan, newest first, that stops at `limit + 1` matches (the
  # extra one proves there is a next page) or at `budget` keys examined.
  # Returns `{hits, status}`: the hits as `{at, seq, sink, owner_source}` and
  # `:exhausted` when the range ended, or `{:more, {at, seq, sink}}`: every key
  # at or above that one was examined, nothing below it was.
  defp scan_dead(instance, family, ord, filter, cursor, limit, budget) do
    acc = %{
      scanned: 0,
      chunk: [],
      chunk_n: 0,
      hits: [],
      hit_n: 0,
      last: nil,
      stop: nil
    }

    deferred? = dead_deferred?(family, filter)

    scan =
      Store.fold(
        instance,
        family,
        dead_range(family, ord, filter, cursor),
        acc,
        fn key, value, acc ->
          dead_step(instance, {family, filter, deferred?, limit, budget}, key, value, acc)
        end,
        reverse: true
      )

    with {:ok, acc} when is_map(acc) <- scan,
         acc when is_map(acc) <- flush_dead(instance, family, filter, acc) do
      {:ok, dead_scan_result(acc, limit)}
    else
      {:ok, {:error, reason}} -> {:error, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  defp dead_range(:dead, _ord, filter, cursor) do
    %{hi: hi} = Keys.family(:dead)

    {<<?x, max(Map.get(filter, :since, 0), 0)::64>>,
     if(cursor, do: <<?x, cursor::binary>>, else: hi)}
  end

  defp dead_range(:dead_by_tenant, ord, filter, cursor) do
    {_lower, hi} = Keys.range(<<?X, ord::32>>)

    {<<?X, ord::32, max(Map.get(filter, :since, 0), 0)::64>>,
     if(cursor, do: <<?X, ord::32, cursor::binary>>, else: hi)}
  end

  # A predicate that needs a read per candidate: the row's class, or (from a
  # by-tenant key, which does not carry it) the hook's source.
  defp dead_deferred?(family, filter) do
    Map.has_key?(filter, :error_class) or
      (family == :dead_by_tenant and Map.has_key?(filter, :source_id))
  end

  defp dead_step(instance, {family, filter, deferred?, limit, budget}, key, value, acc) do
    {at, seq, sink} = decode_dead_key(family, key)
    cand = {at, seq, sink, if(family == :dead, do: {:value, value}, else: nil)}
    acc = %{acc | scanned: acc.scanned + 1, last: {at, seq, sink}}

    acc =
      cond do
        not (keep?(filter, :sink, sink) and owner_match?(filter, family, cand)) ->
          acc

        deferred? ->
          %{acc | chunk: [cand | acc.chunk], chunk_n: acc.chunk_n + 1}

        true ->
          add_dead_hits(acc, [cand])
      end

    acc =
      if acc.chunk_n >= @list_chunk or (deferred? and acc.scanned >= budget),
        do: flush_dead(instance, family, filter, acc),
        else: acc

    cond do
      not is_map(acc) -> {:halt, acc}
      acc.hit_n > limit -> {:halt, %{acc | stop: :limit}}
      acc.scanned >= budget -> {:halt, %{acc | stop: :budget}}
      true -> {:cont, acc}
    end
  end

  defp decode_dead_key(:dead, key), do: Keys.decode_dead(key)

  defp decode_dead_key(:dead_by_tenant, key) do
    {_ord, at, seq, sink} = Keys.decode_dead_by_tenant(key)
    {at, seq, sink}
  end

  # The global dead row carries the owner and the tenant ordinal: `:source_id`
  # and a tenant predicate are free there. A by-tenant key carries neither.
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp owner_match?(filter, :dead, {_at, _seq, _sink, {:value, bin}}) do
    if Map.has_key?(filter, :source_id) or Map.has_key?(filter, :ords) do
      {source_id, _id, ord} = :erlang.binary_to_term(bin)
      keep?(filter, :source_id, source_id) and tenant_match?(Map.get(filter, :ords), ord)
    else
      true
    end
  end

  defp owner_match?(_filter, _family, _cand), do: true

  defp add_dead_hits(acc, cands) do
    %{
      acc
      | hits: Enum.reduce(cands, acc.hits, &[&1 | &2]),
        hit_n: acc.hit_n + length(cands)
    }
  end

  # Applies the predicates that need reads to the pending candidates (newest
  # first once reversed) and moves the survivors to the hits. An acc, or
  # `{:error, reason}`.
  defp flush_dead(_instance, _family, _filter, %{chunk: []} = acc), do: acc

  defp flush_dead(instance, family, filter, acc) do
    cands = Enum.reverse(acc.chunk)

    with {:ok, cands} <- resolve_owners(instance, family, filter, cands),
         {:ok, cands} <- check_error_class(instance, filter, cands) do
      add_dead_hits(%{acc | chunk: [], chunk_n: 0}, cands)
    end
  end

  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp resolve_owners(instance, :dead_by_tenant, %{source_id: source_id}, cands) do
    with {:ok, values} <-
           Store.multi_get(
             instance,
             :index,
             Enum.map(cands, fn {at, seq, sink, _} -> Keys.dead(at, seq, sink) end)
           ) do
      {:ok,
       for {{at, seq, sink, _}, {:ok, bin}} <- Enum.zip(cands, values),
           {^source_id, _id, _ord} <- [:erlang.binary_to_term(bin)] do
         {at, seq, sink, {:value, bin}}
       end}
    end
  end

  defp resolve_owners(_instance, _family, _filter, cands), do: {:ok, cands}

  defp check_error_class(instance, %{error_class: class}, cands) do
    with {:ok, rows} <-
           Store.multi_get(
             instance,
             :deliveries,
             Enum.map(cands, fn {_at, seq, sink, _} -> Keys.delivery(seq, sink) end)
           ) do
      {:ok,
       for {cand, {:ok, bin}} <- Enum.zip(cands, rows),
           {:ok, %{error_class: ^class}} <- [decode_row(bin)] do
         cand
       end}
    end
  end

  defp check_error_class(_instance, _filter, cands), do: {:ok, cands}

  defp dead_scan_result(acc, limit) do
    hits = Enum.reverse(acc.hits)

    cond do
      acc.hit_n > limit ->
        kept = Enum.take(hits, limit + 1)
        {at, seq, sink, _} = List.last(kept)
        {kept, {:more, {at, seq, sink}}}

      acc.stop == :budget ->
        {hits, {:more, acc.last}}

      true ->
        {hits, :exhausted}
    end
  end

  # The hits of every scan, newest first, and the frontier: the highest key any
  # unfinished scan stopped at. Only what is at or above it is known to be
  # complete across the scans, so the page is cut there.
  defp merge_scans(scans) do
    frontier =
      scans
      |> Enum.flat_map(fn
        {_hits, {:more, key}} -> [key]
        {_hits, :exhausted} -> []
      end)
      |> Enum.max(fn -> nil end)

    hits =
      scans
      |> Enum.flat_map(fn {hits, _status} -> hits end)
      |> Enum.sort_by(fn {at, seq, sink, _} -> {at, seq, sink} end, :desc)

    hits =
      if frontier,
        do: Enum.filter(hits, fn {at, seq, sink, _} -> {at, seq, sink} >= frontier end),
        else: hits

    {hits, frontier}
  end

  defp next_cursor(_hits, 0, _frontier), do: nil

  defp next_cursor(hits, limit, frontier) do
    case Enum.split(hits, limit) do
      {page, [_ | _]} ->
        {at, seq, sink, _} = List.last(page)
        <<at::64, seq::64, sink::16>>

      {_page, []} when frontier == nil ->
        nil

      {_page, []} ->
        {at, seq, sink} = frontier
        <<at::64, seq::64, sink::16>>
    end
  end

  @total_filters [:source_id, :since, :sink, :error_class]

  defp dead_total(instance, sources, filter, cursor, frontier, hits) do
    cond do
      Enum.any?(@total_filters, &Map.has_key?(filter, &1)) ->
        {:ok, if(cursor == nil and frontier == nil, do: length(hits), else: nil)}

      sources == :all ->
        with {:ok, [total]} <- Counters.read_field(instance, [0], :dead), do: {:ok, total}

      true ->
        {_kind, ords} = sources

        with {:ok, totals} <- Counters.read_field(instance, ords, :dead),
             do: {:ok, Enum.sum(totals)}
    end
  end

  # The owners of the kept page: from the global dead row a scan already read,
  # or, for a by-tenant key, read now.
  defp fill_owners(instance, page) do
    missing = for {at, seq, sink, nil} <- page, do: Keys.dead(at, seq, sink)

    with {:ok, values} <- Store.multi_get(instance, :index, missing) do
      found = missing |> Enum.zip(values) |> Map.new()

      {:ok,
       for {at, seq, sink, source} <- page do
         {at, seq, sink, owner_of(source, Map.get(found, Keys.dead(at, seq, sink)))}
       end}
    end
  end

  defp owner_of({:value, bin}, _read), do: decode_owner(bin)
  defp owner_of(nil, {:ok, bin}), do: decode_owner(bin)
  defp owner_of(nil, _not_found), do: {@unknown_source, nil}

  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp decode_owner(bin) do
    {source_id, id, _ord} = :erlang.binary_to_term(bin)
    {source_id, id}
  end

  defp list_entries(instance, kept) do
    with {:ok, rows} <-
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
          {{at, seq, sink, owner}, {:ok, row}, {:ok, hook}}, {entries, orphans} ->
            {reason, class} = row_error(row)

            entry = %{
              envelope: listed_envelope(hook, seq, at, owner, row),
              reason: reason,
              at: at,
              sink: sink,
              error_class: class
            }

            {[entry | entries], orphans}

          {{_at, seq, sink, _owner}, _row, _hook}, {entries, orphans} ->
            {entries, [{seq, sink} | orphans]}
        end)

      {:ok, Enum.reverse(entries), orphans}
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
  # such a row in the DLQ (see `revive_row/4`), so listing must not raise on
  # it and take the whole page down. `{reason, error_class}`; a row without a
  # class (written before dead letters had one, or undecodable) has `nil`.
  defp row_error(bin) do
    case decode_row(bin) do
      {:ok, row} -> {Map.get(row, :error), Map.get(row, :error_class)}
      {:error, :unsupported_delivery_row_version} -> {"unsupported delivery row version", nil}
      {:error, :undecodable} -> {"undecodable delivery row", nil}
    end
  end

  defp keep?(filter, key, actual) do
    case Map.get(filter, key) do
      nil -> true
      expected -> actual == expected
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

  # Due rows first (a lane head's `?H` goes with its due row), then the lane
  # members (`?O`: pending rows with no due row), then the tenant's lane markers.
  defp purge_chunk(instance, ord, :pending) do
    case purge_due(instance, ord) do
      {:ok, 0} -> purge_members(instance, ord)
      other -> other
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
           end),
         {:ok, heads} <-
           head_ops(
             instance,
             ord,
             for({key, _size} <- rows) do
               {_ord, seq, sink} = Keys.decode_inflight(key)
               {seq, sink}
             end
           ) do
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

      finish_purge(instance, rows, heads ++ ops, fn {key, _size} ->
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

  defp purge_due(instance, ord) do
    {lower, upper} = Keys.range(<<?D, ord::32>>)

    with {:ok, {_n, rows}} <-
           Store.fold(instance, :due, {lower, upper}, {0, []}, fn key,
                                                                  <<size::32, kind::8>>,
                                                                  {n, acc} ->
             acc = [{key, size, kind} | acc]
             if n + 1 >= @purge_chunk, do: {:halt, {n + 1, acc}}, else: {:cont, {n + 1, acc}}
           end),
         {:ok, heads} <-
           head_ops(
             instance,
             ord,
             for({key, _size, kind} <- rows, kind in [@lane_first, @lane_retry]) do
               {_ord, _at, seq, sink} = Keys.decode_due(key)
               {seq, sink}
             end
           ) do
      ops =
        Enum.flat_map(rows, fn {key, size, kind} ->
          {_ord, _at, seq, sink} = Keys.decode_due(key)

          [
            {:delete, :index, key},
            {:delete, :deliveries, Keys.delivery(seq, sink)},
            {:put, :index, Keys.cleared(seq, 0, sink), <<>>}
            | Counters.ops(
                [{ord, due_field(kind), -1}, {ord, :bytes, -size}] ++ blocked(ord, kind)
              )
          ]
        end)

      finish_purge(instance, rows, heads ++ ops, fn {key, _size, _kind} ->
        {_ord, _at, seq, sink} = Keys.decode_due(key)
        {seq, sink}
      end)
    end
  end

  defp purge_members(instance, ord) do
    {lower, upper} = Keys.range(<<?O, ord::32>>)

    with {:ok, {_n, rows}} <-
           Store.fold(instance, :lane_members, {lower, upper}, {0, []}, fn key,
                                                                           <<size::32>>,
                                                                           {n, acc} ->
             acc = [{key, size} | acc]
             if n + 1 >= @purge_chunk, do: {:halt, {n + 1, acc}}, else: {:cont, {n + 1, acc}}
           end) do
      case rows do
        [] ->
          with :ok <- purge_lane_markers(instance, ord), do: {:ok, 0}

        _ ->
          ops =
            Enum.flat_map(rows, fn {key, size} ->
              {_ord, _lane, seq, sink} = Keys.decode_lane_member(key)

              [
                {:delete, :index, key},
                {:delete, :deliveries, Keys.delivery(seq, sink)},
                {:put, :index, Keys.cleared(seq, 0, sink), <<>>}
                | Counters.ops([
                    {ord, :pending, -1},
                    {ord, :bytes, -size},
                    {ord, :lane_members, -1}
                  ])
              ]
            end)

          finish_purge(instance, rows, ops, fn {key, _size} ->
            {_ord, _lane, seq, sink} = Keys.decode_lane_member(key)
            {seq, sink}
          end)
      end
    end
  end

  # Once the tenant has no member left: a marker with no member is stale.
  defp purge_lane_markers(instance, ord) do
    {lower, upper} = Keys.range(<<?P, ord::32>>)

    with {:ok, {_n, keys}} <-
           Store.fold(instance, :lane_markers, {lower, upper}, {0, []}, fn key,
                                                                           _value,
                                                                           {n, acc} ->
             acc = [key | acc]
             if n + 1 >= @purge_chunk, do: {:halt, {n + 1, acc}}, else: {:cont, {n + 1, acc}}
           end) do
      case keys do
        [] ->
          :ok

        _ ->
          with :ok <- Store.write(instance, Enum.map(keys, &{:delete, :index, &1}), sync: true),
               do: purge_lane_markers(instance, ord)
      end
    end
  end

  # The `?H` of the lane each of `pairs` (`{seq, sink}`, rows about to be purged
  # from `?D`/`?F`) heads, and `lanes -1` for it.
  defp head_ops(_instance, _ord, []), do: {:ok, []}

  defp head_ops(instance, ord, pairs) do
    keys = Enum.map(pairs, fn {seq, sink} -> Keys.delivery(seq, sink) end)

    with {:ok, rows} <- Store.multi_get(instance, :deliveries, keys) do
      {:ok,
       Enum.flat_map(rows, fn
         {:ok, bin} ->
           case decode_row(bin) do
             {:ok, %{lane: lane}} when is_binary(lane) ->
               [{:delete, :index, Keys.lane_head(ord, lane)} | Counters.ops([{ord, :lanes, -1}])]

             _ ->
               []
           end

         :not_found ->
           []
       end)}
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
