defmodule Ankusa.Queue.DlqReaper do
  @moduledoc """
  Keeps the dead-letter queue bounded (`queue.dlq`), without ever discarding a
  hook the provider was told `201` for.

  A dead row is evicted when it is older than `queue.dlq.retention_days`, and
  oldest first while the DLQ holds more than `queue.dlq.max_rows` rows or
  `queue.dlq.max_bytes` stored bytes (the node's `dead` and `dead_bytes`
  counters, one read), and **only while its hook is still recoverable**: no
  archive obligation is left for it, and a segment the catalogue can find holds
  its id (`Ankusa.Storage.fetch/2` answers after the hook is reclaimed), or it is
  a hook a 0.3 node archived. A row that fails the test is skipped, never
  deleted: the compactor drains the archive, the next pass evicts what it
  archived, and that is what keeps the disk flat.

  A DLQ that is still over a ceiling after a pass has nothing left to evict, so
  the node cannot drain: `full?/1` turns `true` (the gauge `ankusa_dlq_full`, a
  warning in the log) and the edge answers new hooks `503 dlq_full` with
  `Retry-After: 60` until a pass finds the DLQ under its ceilings again. An
  acked hook is never deleted to make room.

  One process per instance, in the dispatch domain next to the Pipeline and the
  Replayer. Every eviction runs in this process and each batch inside the
  Replayer (`{:exclusive, fun}`), so a replay job cannot revive a row it is
  deleting. A batch is at most `@chunk` rows of one tenant, written in one
  synced batch with its audit row (`dlq.evict`, or `dlq.purge` for an operator's
  `DELETE /v1/dlq`): the audit row carries the count, the bytes and the range of
  seqs and ids, not one row per hook. Every batch emits `[:ankusa, :dlq,
  :evicted]` (`count`, `bytes`; metadata `tenant`, `reason` and the `ids` and
  `seqs` evicted).
  """

  use GenServer

  require Logger

  alias Ankusa.{Audit, Storage, Store, Telemetry}
  alias Ankusa.Audit.Context
  alias Ankusa.Queue.{Archive, Deliveries, Reclaim, Tenants}
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Store.Keys

  @interval_ms 10_000
  @backoff_ms 100
  @chunk 500
  @max_scan 5_000
  @max_chunks 40
  @day_ms 86_400_000
  @call_timeout_ms 120_000

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :dlq_reaper))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc """
  Whether the DLQ is over a ceiling with nothing left that may be evicted: the
  edge refuses new hooks while it is. A `:persistent_term` read, set by the
  reaper only when the answer changes, so the request path pays nothing.
  """
  @spec full?(atom()) :: boolean()
  def full?(instance), do: :persistent_term.get({__MODULE__, instance}, false)

  @doc """
  One pass, synchronously: evict what retention and the ceilings allow.
  `{:ok, %{evicted: n, full: boolean, more: boolean}}`; `more` says the pass's
  budget, not the end of the DLQ, stopped it.
  """
  @spec reap(atom()) ::
          {:ok, %{evicted: non_neg_integer(), full: boolean(), more: boolean()}}
          | {:error, term()}
  def reap(instance), do: call(instance, :reap)

  @doc """
  Evict the dead rows matching `filter` (`:source_id`; `:tenants`, a list of
  tenant ids or `nil`; `:since` / `:until`, inclusive bounds on the dead-letter
  time, ms), at most `limit` of them, the oldest first, under the same rule as
  the reaper: a row whose hook is not archived is skipped and counted in
  `unarchived`, never deleted. `opts[:audit]` is the caller's
  `Ankusa.Audit.Context`. `more` says matching rows may remain past `limit` or
  the pass's scan budget.
  """
  @spec evict(atom(), map(), pos_integer(), keyword()) ::
          {:ok,
           %{
             deleted: non_neg_integer(),
             bytes: non_neg_integer(),
             unarchived: non_neg_integer(),
             more: boolean()
           }}
          | {:error, term()}
  def evict(instance, filter, limit, opts \\ []) when is_map(filter) and limit > 0,
    do: call(instance, {:evict, filter, limit, Context.from_opts(opts)})

  defp call(instance, message) do
    GenServer.call(Ankusa.via(instance, :dlq_reaper), message, @call_timeout_ms)
  catch
    :exit, _reason -> {:error, :store_unavailable}
  end

  # ── GenServer ─────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    Process.flag(:trap_exit, true)
    set_full(instance, false)
    Process.send_after(self(), :tick, @interval_ms)
    {:ok, %{instance: instance}}
  end

  @impl true
  def terminate(_reason, %{instance: instance}),
    do: :persistent_term.erase({__MODULE__, instance})

  @impl true
  def handle_call(:reap, _from, state), do: {:reply, reap_pass(state.instance), state}

  def handle_call({:evict, filter, limit, context}, _from, state),
    do: {:reply, operator_evict(state.instance, filter, limit, context), state}

  @impl true
  def handle_info(:tick, state) do
    next =
      case reap_pass(state.instance) do
        {:ok, %{more: true}} ->
          @backoff_ms

        {:ok, _} ->
          @interval_ms

        {:error, reason} ->
          Logger.warning("[ankusa] dlq reaper pass failed, retrying: #{inspect(reason)}")
          @interval_ms
      end

    Process.send_after(self(), :tick, next)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # ── a pass ────────────────────────────────────────────────────────────────

  defp reap_pass(instance) do
    dlq = Ankusa.config(instance).queue.dlq
    cutoff = System.system_time(:millisecond) - dlq.retention_days * @day_ms
    %{lo: lo, hi: hi} = Keys.family(:dead)

    with {:ok, aged} <-
           evict_range(instance, {lo, <<?x, cutoff::64>>}, %{}, @max_chunks, :infinity, fn _ ->
             {:ok, "retention", @chunk}
           end),
         {:ok, trimmed} <-
           evict_range(
             instance,
             {lo, hi},
             %{},
             max(@max_chunks - aged.chunks, 0),
             :infinity,
             &over_reason(&1, dlq)
           ),
         {:ok, counters} <- Counters.read(instance, 0) do
      full = over_reason(counters, dlq) != :under
      set_full(instance, full)

      {:ok,
       %{
         evicted: aged.deleted + trimmed.deleted,
         full: full,
         more: aged.more or trimmed.more
       }}
    end
  end

  # `{:ok, reason}` while the DLQ is over a ceiling, `:under` when it is not.
  defp over_reason(counters, dlq) do
    cond do
      counters.dead > dlq.max_rows ->
        {:ok, "max_rows", counters.dead - dlq.max_rows}

      counters.dead_bytes > dlq.max_bytes ->
        average = max(div(counters.dead_bytes, max(counters.dead, 1)), 1)
        {:ok, "max_bytes", div(counters.dead_bytes - dlq.max_bytes, average) + 1}

      true ->
        :under
    end
  end

  defp set_full(instance, full) do
    key = {__MODULE__, instance}

    if :persistent_term.get(key, false) != full do
      if full do
        Logger.warning(
          "[ankusa] the dead-letter queue is over queue.dlq.max_rows / max_bytes and has no row " <>
            "left whose hook is archived: refusing new hooks with 503 dlq_full until the " <>
            "archive drains (an acked hook is never deleted to make room)"
        )
      else
        Logger.info("[ankusa] the dead-letter queue is under its ceilings; accepting hooks again")
      end

      :persistent_term.put(key, full)
    end

    :ok
  end

  # ── operator purge ────────────────────────────────────────────────────────

  defp operator_evict(instance, filter, limit, context) do
    %{lo: lo, hi: hi} = Keys.family(:dead)

    range = {
      case Map.get(filter, :since) do
        nil -> lo
        since -> <<?x, since::64>>
      end,
      case Map.get(filter, :until) do
        nil -> hi
        until -> <<?x, until + 1::64>>
      end
    }

    scan_filter = Map.take(filter, [:source_id, :tenants])

    with {:ok, done} <-
           evict_range(
             instance,
             range,
             scan_filter,
             @max_chunks,
             limit,
             fn _ ->
               {:ok, "purge", @chunk}
             end,
             context
           ) do
      {:ok, Map.take(done, [:deleted, :bytes, :unarchived, :more])}
    end
  end

  # ── the eviction loop ─────────────────────────────────────────────────────

  @empty %{deleted: 0, bytes: 0, unarchived: 0, chunks: 0, more: false}

  # Walks `range` oldest first, `@chunk` candidates at a time. `reason_fun` takes
  # the node's counters and answers `{:ok, reason}` to keep evicting on that
  # reason, `:under` to stop. `cap` bounds the rows evicted (`:infinity`: no
  # bound) and `chunks` the chunks walked; `more` in the result says a bound, not
  # the end of the range, stopped the walk.
  defp evict_range(
         instance,
         range,
         filter,
         chunks,
         cap,
         reason_fun,
         context \\ Context.system()
       ) do
    job = %{
      instance: instance,
      filter: filter,
      cap: cap,
      reason_fun: reason_fun,
      context: context
    }

    walk(job, range, chunks, @empty)
  end

  defp walk(_job, _range, 0, acc), do: {:ok, %{acc | more: true}}

  defp walk(job, range, chunks, acc) do
    want = if job.cap == :infinity, do: @chunk, else: min(@chunk, job.cap - acc.deleted)

    if want <= 0 do
      {:ok, %{acc | more: true}}
    else
      with {:ok, counters} <- Counters.read(job.instance, 0) do
        case job.reason_fun.(counters) do
          :under -> {:ok, acc}
          {:ok, reason, hint} -> walk_chunk(job, range, chunks, acc, min(want, hint), reason)
        end
      end
    end
  end

  defp walk_chunk(job, {_lower, upper} = range, chunks, acc, want, reason) do
    with {:ok, candidates, last, exhausted?} <-
           Deliveries.dead_candidates(job.instance, range, job.filter, @chunk, @max_scan),
         {:ok, done} <- evict_chunk(job.instance, candidates, reason, job.context, want) do
      acc = %{
        acc
        | deleted: acc.deleted + done.deleted,
          bytes: acc.bytes + done.bytes,
          unarchived: acc.unarchived + done.unarchived,
          chunks: acc.chunks + 1
      }

      if exhausted? or last == nil,
        do: {:ok, acc},
        else: walk(job, {last <> <<0>>, upper}, chunks - 1, acc)
    end
  end

  # One chunk: the rows whose hook is recoverable are evicted (a synced batch per
  # tenant with its audit row), the others are counted and left.
  defp evict_chunk(_instance, [], _reason, _context, _want),
    do: {:ok, %{deleted: 0, bytes: 0, unarchived: 0}}

  defp evict_chunk(instance, candidates, reason, context, want) do
    with {:ok, recoverable, unarchived} <- split_recoverable(instance, candidates),
         {:ok, evicted} <-
           evict_by_tenant(instance, Enum.take(recoverable, want), reason, context) do
      {:ok,
       %{
         deleted: length(evicted),
         bytes: evicted |> Enum.map(& &1.size) |> Enum.sum(),
         unarchived: unarchived
       }}
    end
  end

  defp evict_by_tenant(_instance, [], _reason, _context), do: {:ok, []}

  defp evict_by_tenant(instance, recoverable, reason, context) do
    recoverable
    |> Enum.group_by(fn {_key, _seq, _sink, ord, _source, _id} -> ord end)
    |> Enum.reduce_while({:ok, []}, fn {ord, group}, {:ok, done} ->
      case evict_group(instance, ord, group, reason, context) do
        {:ok, evicted} -> {:cont, {:ok, done ++ evicted}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp evict_group(instance, ord, group, reason, context) do
    tenant =
      case Tenants.name(instance, ord) do
        {:ok, name} -> name
        _ -> nil
      end

    with {:ok, ops, evicted} <- Deliveries.evict_ops(instance, group),
         :ok <- commit(instance, context, audit_attrs(tenant, reason, evicted), ops) do
      pairs = Enum.map(evicted, &{&1.seq, Keys.cleared(&1.seq, 0, &1.sink)})

      # A failed reclaim only defers: the markers stay, and the sweep finds them.
      with {:error, why} <- Reclaim.run(instance, pairs) do
        Logger.warning("[ankusa] hook reclaim after a dlq eviction deferred: #{inspect(why)}")
      end

      emit(instance, tenant, reason, evicted)
      {:ok, evicted}
    end
  end

  # The batch, inside the Replayer so no replay step revives a row it deletes.
  defp commit(instance, context, attrs, ops) do
    exclusive(instance, fn -> Audit.commit(instance, context, attrs, ops) end)
  end

  defp exclusive(instance, fun) do
    case Ankusa.whereis(instance, :replayer) do
      pid when is_pid(pid) -> GenServer.call(pid, {:exclusive, fun}, :infinity)
      nil -> fun.()
    end
  end

  defp audit_attrs(tenant, reason, evicted) do
    seqs = Enum.map(evicted, & &1.seq)
    ids = for %{id: id} when is_binary(id) <- evicted, do: id

    %{
      action: if(reason == "purge", do: "dlq.purge", else: "dlq.evict"),
      tenant: tenant,
      resource: "dlq",
      detail: %{
        "reason" => reason,
        "count" => length(evicted),
        "bytes" => evicted |> Enum.map(& &1.size) |> Enum.sum(),
        "seqs" => [Enum.min(seqs), Enum.max(seqs)],
        "ids" => if(ids == [], do: [], else: [Enum.min(ids), Enum.max(ids)])
      }
    }
  end

  defp emit(instance, tenant, reason, evicted) do
    Telemetry.emit(
      [:dlq, :evicted],
      %{count: length(evicted), bytes: evicted |> Enum.map(& &1.size) |> Enum.sum()},
      %{
        instance: instance,
        tenant_id: tenant,
        reason: reason,
        ids: Enum.map(evicted, & &1.id),
        seqs: Enum.map(evicted, & &1.seq)
      }
    )
  end

  # ── is the hook recoverable? ──────────────────────────────────────────────

  # `{:ok, recoverable, unarchived_count}`. A candidate is recoverable when its
  # hook has no archive obligation left and a segment index (or a 0.3 location)
  # holds its id.
  defp split_recoverable(instance, candidates) do
    seqs = Enum.map(candidates, fn {_key, seq, _sink, _ord, _source, _id} -> seq end)

    with {:ok, obligations} <-
           Store.multi_get(instance, :index, Enum.map(seqs, &Keys.archive_pending/1)) do
      {recoverable, _cache, skipped} =
        candidates
        |> Enum.zip(obligations)
        |> Enum.reduce({[], %{}, 0}, fn {candidate, obligation}, {keep, cache, skipped} ->
          {_key, seq, _sink, _ord, _source, id} = candidate

          if is_binary(id) and obligation == :not_found do
            {found?, cache} = archived?(instance, seq, id, cache)
            if found?, do: {[candidate | keep], cache, skipped}, else: {keep, cache, skipped + 1}
          else
            {keep, cache, skipped + 1}
          end
        end)

      {:ok, Enum.reverse(recoverable), skipped}
    end
  end

  # `{boolean, cache}`: `cache` holds the index object of every segment read so
  # far in this chunk.
  defp archived?(instance, seq, id, cache) do
    case Archive.legacy_location(instance, id) do
      {:ok, _location} ->
        {true, cache}

      :error ->
        case Archive.segment_at(instance, seq) do
          {:ok, %{last_seq: last, idx_key: idx_key} = row} when last >= seq ->
            {index, cache} = segment_index(instance, row, idx_key, cache)
            {is_map(index) and Map.has_key?(index, id), cache}

          _ ->
            {false, cache}
        end
    end
  end

  defp segment_index(instance, row, idx_key, cache) do
    case cache do
      %{^idx_key => index} ->
        {index, cache}

      _ ->
        index =
          case Storage.segment_index(instance, row) do
            {:ok, index} -> index
            :error -> nil
          end

        {index, Map.put(cache, idx_key, index)}
    end
  end
end
