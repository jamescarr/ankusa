defmodule Ankusa.Queue.Deliveries.Lanes do
  @moduledoc false

  # Ordering lanes (ADR 0004): the part of `Ankusa.Queue.Deliveries` that builds
  # and reads the `?O`, `?H` and `?P` keys. A row of a lane is in exactly one
  # place: a member (`?O`, waiting its turn, in `seq` order), the head (`?H` plus
  # a `?D` of kind 2/3, or an `?F`) or the DLQ.
  #
  #   member   deliveries(seq, i) + ?O(ord, lane, seq, i)           (+ ?P, ?A)
  #   head     deliveries(seq, i) + ?H(ord, lane) + ?D(... kind 2|3) or ?F
  #
  # Lane state is moved only by the dispatch Pipeline, except for the writer's
  # blind puts of a new member and its `?P` marker. The invariant the proof tests:
  # *every `?O` member has an `?H` or a `?P`; every `?H` names exactly one row in
  # `?D` or `?F`*.
  #
  # A terminal transition of a head (delivered, dead-lettered, orphaned) carries a
  # `{:lane_release, ord, lane}` placeholder (`release_op/3`); `expand/3` turns
  # each into the ops that move the lane on, in the one step that writes the batch
  # (the Pipeline calls it from `write/2`), so the read of the next member and the
  # write that releases the head have no other lane work between them.

  alias Ankusa.Queue.Deliveries
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Queue.Syncer
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  # Kinds of a lane head's due row.
  @first_attempt 2
  @retry 3

  @type lane :: <<_::128>>

  @doc "The due kind of a lane head's first attempt."
  def first_attempt_kind, do: @first_attempt

  @doc "The due kind of a lane head waiting out a retry or a breaker park."
  def retry_kind, do: @retry

  @doc "Whether a due `kind` belongs to a lane head."
  @spec head_kind?(integer()) :: boolean()
  def head_kind?(kind), do: kind in [@first_attempt, @retry]

  @doc """
  A new member of `lane`: the `?O` key, the `?P` marker, the tenant's active
  marker and `lane_members +1`. The caller adds `pending` and `bytes`.
  """
  @spec member_ops(pos_integer(), integer(), pos_integer(), integer(), integer(), lane()) ::
          [Store.op()]
  def member_ops(ord, at, seq, sink, size, lane) do
    {puts, deltas} = member_entry(ord, seq, sink, size, lane)
    puts ++ [Ankusa.Queue.Deliveries.active_op(ord, at)] ++ Counters.ops(deltas)
  end

  @doc """
  `member_ops/6` in pieces (see `Ankusa.Queue.Deliveries.new_entry/7`): the `?O`
  key and the `?P` marker, and the `lane_members` delta, without the tenant's
  active marker.
  """
  @spec member_entry(pos_integer(), pos_integer(), integer(), integer(), lane()) ::
          {[Store.op()], [Counters.delta()]}
  def member_entry(ord, seq, sink, size, lane) do
    {[
       {:put, :index, Keys.lane_member(ord, lane, seq, sink), <<size::32>>},
       {:put, :index, Keys.lane_marker(ord, lane), <<>>}
     ], [{ord, :lane_members, 1}]}
  end

  @doc """
  The batch that makes member `seq`/`sink` the head of `lane`: the member goes,
  `?H` and a kind 2 due row come, the marker goes, `lane_members -1`. A lane
  that had no head gains one (`lanes +1`); with `replace?` the new head takes
  the place of the one just released, so the count does not move.
  """
  @spec promote_ops(
          pos_integer(),
          lane(),
          pos_integer(),
          integer(),
          integer(),
          integer(),
          boolean()
        ) :: [Store.op()]
  def promote_ops(ord, lane, seq, sink, size, at, replace?) do
    [
      {:delete, :index, Keys.lane_member(ord, lane, seq, sink)},
      {:put, :index, Keys.lane_head(ord, lane), <<seq::64, sink::16>>},
      {:delete, :index, Keys.lane_marker(ord, lane)}
    ] ++
      Deliveries.due_ops(ord, at, seq, sink, size, @first_attempt) ++
      Counters.ops([{ord, :lane_members, -1}] ++ if(replace?, do: [], else: [{ord, :lanes, 1}]))
  end

  @doc "The placeholder a head's terminal transition carries; `expand/3` resolves it."
  @spec release_op(pos_integer(), map() | nil) :: [tuple()]
  def release_op(ord, %{lane: lane}) when is_binary(lane), do: [{:lane_release, ord, lane}]
  def release_op(_ord, _row), do: []

  @doc "Whether `op` is an unresolved `release_op/2` placeholder."
  def placeholder?({:lane_release, _ord, _lane}), do: true
  def placeholder?(_op), do: false

  # ── reads ─────────────────────────────────────────────────────────────────

  @doc "The lowest member of `lane`, `%{seq, sink, size}`, or `nil`: one seek."
  @spec first_member(atom(), pos_integer(), lane()) ::
          {:ok, %{seq: pos_integer(), sink: integer(), size: integer()} | nil}
          | {:error, term()}
  def first_member(instance, ord, lane) do
    case Store.first(instance, :lane_members, Keys.range(Keys.lane_members_prefix(ord, lane))) do
      {:ok, {key, <<size::32>>}} ->
        {_ord, _lane, seq, sink} = Keys.decode_lane_member(key)
        {:ok, %{seq: seq, sink: sink, size: size}}

      {:ok, nil} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Whether tenant `ord` has a lane marker: one seek."
  @spec any_marker?(atom(), pos_integer()) :: {:ok, boolean()} | {:error, term()}
  def any_marker?(instance, ord) do
    case Store.first(instance, :lane_markers, Keys.range(<<?P, ord::32>>)) do
      {:ok, nil} -> {:ok, false}
      {:ok, _found} -> {:ok, true}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "The row `lane`'s head names, `{seq, sink}`, or `nil`."
  @spec head(atom(), pos_integer(), lane()) ::
          {:ok, {pos_integer(), integer()} | nil} | {:error, term()}
  def head(instance, ord, lane) do
    case Store.get(instance, :index, Keys.lane_head(ord, lane)) do
      {:ok, <<seq::64, sink::16>>} -> {:ok, {seq, sink}}
      :not_found -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  # ── release ───────────────────────────────────────────────────────────────

  @doc """
  The ops that move `lane` on after its head's row left `?D`/`?F`: the next
  member is the new head, or, with none (or one whose group is not yet durable:
  its `seq` is above `Syncer.durable_seq/1`), the head goes and `lanes -1`. A
  member waiting for its fsync keeps the lane's marker, so the sweep promotes it
  once the syncer wakes dispatch. The marker is never deleted when there is no
  member: a writer that committed one after this read has put it.
  """
  @spec release_ops(atom(), pos_integer(), lane(), integer(), non_neg_integer() | :infinity) ::
          {:ok, [Store.op()]} | {:error, term()}
  def release_ops(instance, ord, lane, at, durable) do
    case first_member(instance, ord, lane) do
      {:ok, %{seq: seq, sink: sink, size: size}} when seq <= durable ->
        {:ok, promote_ops(ord, lane, seq, sink, size, at, true)}

      {:ok, %{}} ->
        {:ok, vacate_ops(ord, lane) ++ [{:put, :index, Keys.lane_marker(ord, lane), <<>>}]}

      {:ok, nil} ->
        {:ok, vacate_ops(ord, lane)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp vacate_ops(ord, lane),
    do: [{:delete, :index, Keys.lane_head(ord, lane)}] ++ Counters.ops([{ord, :lanes, -1}])

  @doc """
  Replaces every `release_op/2` placeholder in `ops` by what `release_ops/5`
  reads now. One placeholder per lane is expanded (a lane has one head, so a
  second is a repeat of the first). Built in the step that writes the batch: a
  batch that failed to write keeps its placeholders and is rebuilt.
  """
  @spec expand(atom(), [Store.op()], integer()) :: {:ok, [Store.op()]} | {:error, term()}
  def expand(instance, ops, at) do
    if Enum.any?(ops, &placeholder?/1) do
      durable = Syncer.durable_seq(instance)

      ops
      |> Enum.reduce_while({:ok, [], MapSet.new()}, fn
        {:lane_release, ord, lane}, {:ok, acc, seen} ->
          if MapSet.member?(seen, {ord, lane}) do
            {:cont, {:ok, acc, seen}}
          else
            case release_ops(instance, ord, lane, at, durable) do
              {:ok, built} ->
                {:cont, {:ok, Enum.reverse(built) ++ acc, MapSet.put(seen, {ord, lane})}}

              {:error, reason} ->
                {:halt, {:error, reason}}
            end
          end

        op, {:ok, acc, seen} ->
          {:cont, {:ok, [op | acc], seen}}
      end)
      |> case do
        {:ok, acc, _seen} -> {:ok, Enum.reverse(acc)}
        {:error, reason} -> {:error, reason}
      end
    else
      {:ok, ops}
    end
  end

  # ── sweep ─────────────────────────────────────────────────────────────────

  @doc """
  The sweep of tenant `ord` (ADR 0004, section 4.6), once per visit and before
  the due fold, so a promoted row is claimed in the same scan. It reads up to
  `limit` of the tenant's `?P` markers and, for each:

    * a head exists and its row is alive: the marker goes (the head's release
      will read the members);
    * a head exists and its row is gone or dead: the head is stale, and the lane
      is released;
    * no head and a member: it is promoted, once its group is durable;
    * no head and no member: the marker goes, then the lane's members are read
      once more and the marker is put back if one appeared.

  Returns the ops the caller writes (promotions and releases) and whether the
  marker read hit `limit`, i.e. whether more may be waiting.
  """
  @spec sweep(atom(), pos_integer(), integer(), pos_integer()) ::
          {:ok, [Store.op()], boolean()} | {:error, term()}
  def sweep(instance, ord, at, limit) do
    with {:ok, lanes} <- markers(instance, ord, limit) do
      durable = Syncer.durable_seq(instance)

      lanes
      |> Enum.reduce_while({:ok, [], []}, fn lane, {:ok, ops, empty} ->
        case triage(instance, ord, lane, at, durable) do
          {:ops, more} -> {:cont, {:ok, more ++ ops, empty}}
          :empty -> {:cont, {:ok, ops, [lane | empty]}}
          :keep -> {:cont, {:ok, ops, empty}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
      |> case do
        {:ok, ops, empty} ->
          with :ok <- settle_empty(instance, ord, empty), do: {:ok, ops, length(lanes) >= limit}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp markers(instance, ord, limit) do
    with {:ok, {_n, lanes}} <-
           Store.fold(instance, :lane_markers, Keys.range(<<?P, ord::32>>), {0, []}, fn key,
                                                                                        _value,
                                                                                        {n, acc} ->
             {_ord, lane} = Keys.decode_lane_marker(key)

             if n + 1 >= limit,
               do: {:halt, {n + 1, [lane | acc]}},
               else: {:cont, {n + 1, [lane | acc]}}
           end) do
      {:ok, Enum.reverse(lanes)}
    end
  end

  defp triage(instance, ord, lane, at, durable) do
    with {:ok, head} <- head(instance, ord, lane) do
      case head do
        {seq, sink} -> triage_head(instance, ord, lane, seq, sink, at, durable)
        nil -> triage_vacant(instance, ord, lane, at, durable)
      end
    end
  end

  defp triage_head(instance, ord, lane, seq, sink, at, durable) do
    case Store.get(instance, :deliveries, Keys.delivery(seq, sink)) do
      {:ok, bin} ->
        case Deliveries.decode_row(bin) do
          {:ok, %{state: :dead}} -> stale(instance, ord, lane, at, durable)
          _alive_or_unreadable -> {:ops, [{:delete, :index, Keys.lane_marker(ord, lane)}]}
        end

      :not_found ->
        stale(instance, ord, lane, at, durable)

      {:error, reason} ->
        {:error, reason}
    end
  end

  # A head whose row is gone or dead-lettered will never release its lane.
  defp stale(instance, ord, lane, at, durable) do
    with {:ok, ops} <- release_ops(instance, ord, lane, at, durable), do: {:ops, ops}
  end

  defp triage_vacant(instance, ord, lane, at, durable) do
    case first_member(instance, ord, lane) do
      {:ok, %{seq: seq, sink: sink, size: size}} when seq <= durable ->
        {:ops, promote_ops(ord, lane, seq, sink, size, at, false)}

      # Committed but not yet fsynced: not acked, so not delivered. The marker
      # stays, and the syncer wakes dispatch when the fsync returns.
      {:ok, %{}} ->
        :keep

      {:ok, nil} ->
        :empty

      {:error, reason} ->
        {:error, reason}
    end
  end

  # "Delete, then verify": a writer's member put either came before the delete
  # (the second read sees it) or after it (its own marker put is back).
  defp settle_empty(_instance, _ord, []), do: :ok

  defp settle_empty(instance, ord, lanes) do
    deletes = Enum.map(lanes, &{:delete, :index, Keys.lane_marker(ord, &1)})

    with :ok <- Store.write(instance, deletes, sync: false),
         {:ok, appeared} <- appeared(instance, ord, lanes) do
      Store.write(
        instance,
        Enum.map(appeared, &{:put, :index, Keys.lane_marker(ord, &1), <<>>}),
        sync: false
      )
    end
  end

  defp appeared(instance, ord, lanes) do
    Enum.reduce_while(lanes, {:ok, []}, fn lane, {:ok, acc} ->
      case first_member(instance, ord, lane) do
        {:ok, nil} -> {:cont, {:ok, acc}}
        {:ok, %{}} -> {:cont, {:ok, [lane | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end
end
