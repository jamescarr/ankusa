defmodule Ankusa.Queue.Writer do
  @moduledoc """
  The only seq assigner: commits batches of hooks to the store, and answers a
  caller only once its hooks are on disk (`Ankusa.Queue.Syncer` fsyncs the WAL),
  so a `2xx` to the provider always means the bytes are on disk.

  One process per instance, under `Ankusa.via(instance, :queue_writer)`. The
  commit is a single atomic store batch: the hook, one pending delivery row and
  due key (with its active-tenant marker) per sink, the archive obligation when
  the `:storage` role runs, the counter deltas of all of them, the `m:next_seq`
  marker and, for a tenant seen for the first time, its ordinal (this process is
  the only one that allocates them, see `queue/tenants.ex`). A failed commit
  consumes its seqs and ordinals (gaps are allowed) but acks nothing. A hook
  whose tenant would pass `tenants.max` is refused with `{:error,
  :tenant_limit}` in its place of the reply, and nothing of it is written.

  ## Group commit

  A call carries what needs no seq, prepared by its caller (`Ankusa.Queue`: the
  hook's binary, its encoded delivery rows, the dedupe keys read from the store).
  The writer takes the call that woke it and every `:enqueue` call queued behind
  it, from every batcher partition, up to `batcher.group_commit_max_bytes` of
  bodies and `batcher.group_commit_max_records` hooks, and commits them as one
  **group**: one tenant admission, one dedupe validation, dense seqs across all
  of it, one store write (applied with `sync: false`) and, under `wal: :dual`,
  one publish. The writer hands every caller's reply to `Ankusa.Queue.Syncer`,
  which fsyncs the WAL once for all the groups applied so far and then answers
  them: while group N syncs, group N+1 is assembled and applied here. A group's
  rows are not visible to dispatch, the archive or a retry's `201 duplicate`
  before that fsync (`Syncer.visible/2`, ADR 0002 amendment 17).

  The dedupe values a caller read are a *prefetch*: the first occurrence of a key
  in the group wins, and a key this process wrote since the caller read (in an
  earlier group, or in flight under `wal: :dual`) is answered from `recent_*` and
  the pending set, never from the stale read, so two partitions that saw the same
  key as absent still commit one hook.

  Under `wal: :dual` (ADR 0002) the local commit runs **in parallel** with a
  confirmed publish to the intake log (`Ankusa.Intake`), and both are required
  before the `201`. The writer does not wait: it records the batch in the
  pending set, returns `{:noreply, _}`, and answers the batcher's call when the
  publish task's result and the syncer's confirm of the local half have both
  arrived, so the next batch's local write overlaps the previous batch's fsync
  and publish wait. A half that fails is **carried to completion**
  by `Ankusa.Intake.Reconciler`, never undone; the caller's reply for such a
  hook is `{:error, {:half, reason, hook_id}}`.
  """

  use GenServer

  require Logger

  alias Ankusa.Config
  alias Ankusa.Intake
  alias Ankusa.Intake.Gate, as: IntakeGate
  alias Ankusa.Intake.{Pending, Reconciler}
  alias Ankusa.Queue.Deliveries
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Queue.Syncer
  alias Ankusa.Queue.Tenants
  alias Ankusa.Store
  alias Ankusa.Store.Keys
  alias Ankusa.Tenant.Gate

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :queue_writer))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Ankusa.config(instance)

    # A seq is never reused: if either read fails, refuse to start rather than
    # guess a floor. The same for tenant ordinals.
    with {:ok, next_seq} <- next_seq(instance),
         {:ok, next_ord} <- Tenants.next_free_ord(instance) do
      Logger.info("[ankusa] store next_seq=#{next_seq} tenants=#{next_ord - 1}")

      # The durable watermark the syncer raises after each fsync; everything in
      # the store at boot is durable (it survived a restart).
      marks = Syncer.new_marks(next_seq - 1, System.system_time(:millisecond))
      {:ok, _} = Registry.register(Ankusa.Registry, {instance, :queue_marks}, marks)
      {:ok, syncer} = Syncer.start_link(instance: instance, marks: marks, writer: self())

      dedupe = config.dedupe
      mono = System.monotonic_time(:millisecond)

      state = %{
        instance: instance,
        config: config,
        next_seq: next_seq,
        next_ord: next_ord,
        tenants_max: config.tenants.max,
        dedupe_max: dedupe.max_entries,
        dedupe_per_tenant: dedupe.max_entries_per_tenant,
        # tenant id => ordinal; only this process reads or writes it, and
        # only after a commit succeeded
        ords: :ets.new(:ankusa_tenant_ords, [:set, :private]),
        archive?: Config.role?(config, :storage),
        last_at: 0,
        # `wal: :dual`: the pending set's table, the publish tasks' supervisor,
        # the batches whose publish is in flight (task ref => batch)
        dual: config.wal == :dual,
        table: nil,
        tasks: nil,
        inflight: %{},
        incarnation: "0",
        # group commit: the caps of one group, and the dedupe keys this process
        # wrote lately (two generations, `recent_since` is when the older began)
        group_max_bytes: config.batcher.group_commit_max_bytes,
        group_max_records: config.batcher.group_commit_max_records,
        group_delay_ms: config.batcher.group_commit_delay_ms,
        last_group: 0,
        recent_cur: :ets.new(:ankusa_dedupe_recent, [:set, :private]),
        recent_old: :ets.new(:ankusa_dedupe_recent, [:set, :private]),
        recent_gen_at: mono,
        recent_since: mono,
        recent_epoch: nil,
        marks: marks,
        syncer: syncer,
        # after a failed fsync the writer refuses work until it has deleted the
        # rows of the groups that were never acked (`fence/1`); `unsynced` holds
        # what it would delete, newest group first
        fenced: nil,
        unsynced: []
      }

      {:ok, if(state.dual, do: init_dual(state), else: state)}
    else
      {:error, reason} -> {:stop, {:queue_writer_init_failed, reason}}
    end
  end

  # The highest of the `m:next_seq` marker and one past the last stored hook.
  @doc false
  @spec next_seq(atom()) :: {:ok, pos_integer()} | {:error, term()}
  def next_seq(instance) do
    with {:ok, marker} <- marker_seq(instance),
         {:ok, last} <- last_hook_seq(instance) do
      {:ok, max(marker, last + 1)}
    end
  end

  defp init_dual(state) do
    # A clean stop writes the last watermark and offset (`terminate/2`).
    Process.flag(:trap_exit, true)
    t = Pending.table(state.instance)

    with {:ok, <<offset::64>>} <- Store.get(state.instance, :default, Keys.meta("intake_offset")),
         do: Pending.bump_confirmed(t, offset)

    Process.send_after(self(), :flush_intake, state.config.intake.offset_flush_ms)

    %{
      state
      | table: t,
        tasks: Ankusa.via(state.instance, :intake_tasks),
        incarnation: Ankusa.Intake.Recovery.incarnation(state.instance)
    }
  end

  defp marker_seq(instance) do
    case Store.get(instance, :default, Keys.meta("next_seq")) do
      {:ok, <<n::64>>} -> {:ok, n}
      :not_found -> {:ok, 1}
      {:error, reason} -> {:error, reason}
    end
  end

  defp last_hook_seq(instance) do
    %{hi: hi} = Keys.family(:hooks)

    Store.fold(
      instance,
      :hooks,
      {Keys.hook(1), hi},
      0,
      fn key, _v, _acc -> {:halt, :binary.decode_unsigned(key)} end,
      reverse: true
    )
  end

  @impl true
  # Fenced after a failed fsync: nothing is written until the rows of the groups
  # that were never acked are deleted (`fence/1`), so none of them is ever
  # delivered, and a retry meets no trace of them.
  def handle_call({:enqueue, _prepared, _deadline, _extra}, _from, %{fenced: f} = state)
      when f != nil,
      do: {:reply, {:error, :store_unavailable}, state}

  def handle_call({:redrive, _entries, _extra}, _from, %{fenced: f} = state) when f != nil,
    do: {:reply, {:error, :store_unavailable}, state}

  def handle_call({:expire_dedupe, _entries}, _from, %{fenced: f} = state) when f != nil,
    do: {:reply, {:error, :store_unavailable}, state}

  def handle_call({:complete_local, _ids}, _from, %{fenced: f} = state) when f != nil,
    do: {:reply, {:ok, [], []}, state}

  def handle_call({:enqueue, prepared, deadline, []}, from, state),
    do: group_commit({from, prepared, deadline}, state)

  # A quarantine release (`extra_ops`: the pen deletes and the job cursor) is a
  # group of its own: it waits for its outcome and carries ops that must commit
  # with exactly its hooks.
  def handle_call({:enqueue, prepared, deadline, extra_ops}, {caller, _tag} = from, state) do
    cond do
      expired?(deadline) ->
        # Not started, so nothing to undo: no seq consumed, no store touched.
        {:reply, {:error, :deadline_exceeded}, state}

      # The caller died while its call waited here (a commit task killed from
      # outside, or taken down with its batcher). The batcher has already
      # answered those hooks 503, so committing the batch would store hooks
      # nobody acked. Callers are always local: the Registry is node-local.
      not Process.alive?(caller) ->
        {:reply, {:error, :caller_gone}, state}

      true ->
        commit_batch(prepared.items, prepared.prefetch, extra_ops, begin_group(state), from)
    end
  end

  # The archive-replay batch: one synced commit for the entries and the job's
  # cursor, so a crash cannot leave hooks without their replay marker or a
  # cursor without its hooks.
  def handle_call({:redrive, entries, extra_ops}, {caller, _tag}, state) do
    if Process.alive?(caller) do
      redrive_batch(entries, extra_ops, state)
    else
      {:reply, {:error, :caller_gone}, state}
    end
  end

  # An erase has just closed a tenant (`Ankusa.Tenant.Gate`): this call returns
  # after the batch the writer was committing then, so the tenant's ordinal is
  # settled once it does, and nothing is committed for the tenant after it.
  def handle_call(:barrier, _from, state), do: {:reply, :ok, state}

  # One bounded batch of expired dedupe keys from `Ankusa.Queue.DedupeSweeper`.
  # Each key is deleted only if it still holds the expiry the sweeper saw, so a
  # refresh that landed since is never undone, and its counter goes down in the
  # same batch. Not synced: a lost batch is found again by the next sweep.
  def handle_call({:expire_dedupe, entries}, _from, state) do
    {:reply, expire_dedupe(state, entries), state}
  end

  # The local half of carried batches, from `Ankusa.Intake.Reconciler`: one point
  # read says whether the first write landed after all (an error returned for a
  # batch that persisted); only if not are the batch's ops applied, in one synced
  # batch. Counters ride in those ops as merge operands and a batch is atomic, so
  # applying an absent batch is exact and the read stops a present one from being
  # applied twice. Not on the request path.
  def handle_call({:complete_local, batch_ids}, _from, %{dual: true} = state) do
    t = state.table

    {done, completed} =
      Enum.reduce(batch_ids, {[], []}, fn id, {done, completed} ->
        case Pending.batch(t, id) do
          nil ->
            {[id | done], completed}

          batch ->
            if local_landed?(state.instance, batch) do
              # Applied without a sync, or by a write whose sync failed: done only
              # once an fsync has made it durable.
              case Store.sync_wal(state.instance) do
                :ok -> {[id | done], Pending.local_done(t, id) ++ completed}
                {:error, _reason} -> {done, completed}
              end
            else
              case Store.write(state.instance, batch.ops, sync: true) do
                :ok ->
                  wake_dispatch(state.instance, batch.at)
                  {[id | done], Pending.local_done(t, id) ++ completed}

                {:error, _reason} ->
                  {done, completed}
              end
            end
        end
      end)

    {:reply, {:ok, done, completed}, state}
  end

  defp local_landed?(_instance, %{probe_seq: nil}), do: false

  defp local_landed?(instance, %{probe_seq: seq}),
    do: match?({:ok, _}, Store.get(instance, :hooks, Keys.hook(seq)))

  defp expired?(:infinity), do: false
  defp expired?(deadline), do: System.monotonic_time(:millisecond) >= deadline

  # ── group commit ──────────────────────────────────────────────────────────

  @recent_ms 2_000
  @recent_max 100_000
  @busy_records 64

  # `first` and every `:enqueue` call already queued (no `extra_ops`), as long as
  # the group stays within its caps; the call that would pass one starts the next
  # group (`handle_continue/2`). The first call is always taken, however large.
  defp group_commit({_from, prepared, _deadline} = first, state) do
    state = state |> begin_group() |> linger()
    {calls, leftover} = drain([first], prepared.count, prepared.bytes, state)

    live =
      Enum.filter(calls, fn {{caller, _tag} = from, _prepared, deadline} ->
        cond do
          expired?(deadline) ->
            GenServer.reply(from, {:error, :deadline_exceeded})
            false

          not Process.alive?(caller) ->
            GenServer.reply(from, {:error, :caller_gone})
            false

          true ->
            true
        end
      end)

    state = if live == [], do: state, else: commit_group(live, state)

    if leftover,
      do: {:noreply, state, {:continue, {:group, leftover}}},
      else: {:noreply, state}
  end

  defp drain(calls, count, bytes, state) do
    receive do
      {:"$gen_call", from, {:enqueue, prepared, deadline, []}} ->
        call = {from, prepared, deadline}

        if count + prepared.count > state.group_max_records or
             bytes + prepared.bytes > state.group_max_bytes do
          {Enum.reverse(calls), call}
        else
          drain([call | calls], count + prepared.count, bytes + prepared.bytes, state)
        end
    after
      0 -> {Enum.reverse(calls), nil}
    end
  end

  # A writer whose last group held at least `@busy_records` hooks is saturated,
  # and the callers it just answered are about to call again: a short wait lets
  # them join the next group instead of forming the one after it (without it a
  # closed loop of clients splits into two cohorts that commit alternately, each
  # half the size it could be). Never at low concurrency (a 32-client closed loop
  # never fills a group of 64, and a wait there costs most of its throughput once
  # the fsync is cheap), where it would only add latency.
  defp linger(%{group_delay_ms: 0} = state), do: state

  defp linger(state) do
    if state.last_group >= @busy_records, do: Process.sleep(state.group_delay_ms)
    state
  end

  defp commit_group(live, %{fenced: f} = state) when f != nil do
    calls = Enum.map(live, fn {from, prepared, _deadline} -> {from, prepared.count} end)
    reply_calls(calls, {:error, :store_unavailable})
    state
  end

  defp commit_group(live, state) do
    items = Enum.flat_map(live, fn {_from, prepared, _deadline} -> prepared.items end)
    calls = Enum.map(live, fn {from, prepared, _deadline} -> {from, prepared.count} end)
    state = %{state | last_group: length(items)}

    case commit_batch(items, merge_prefetch(live, state), [], state, calls) do
      {:reply, reply, state} ->
        settle_reply(calls, reply, state)

      {:noreply, state} ->
        state
    end
  end

  # A reply that is not the outcome of a write of this group: refusals and errors
  # depend on nothing and go out at once; one that carries a `201 duplicate` waits
  # for the fsync of whatever group holds the original, unless nothing is unsynced.
  defp settle_reply(calls, {:ok, results} = reply, state) do
    if Syncer.unsynced?(state.marks) and Enum.any?(results, &match?({:duplicate, _}, &1)) do
      Syncer.submit(state.syncer, %{
        upto: Syncer.applied(state.marks),
        at: state.last_at,
        done: {:reply, calls, reply}
      })
    else
      reply_calls(calls, reply)
    end

    state
  end

  defp settle_reply(calls, reply, state) do
    reply_calls(calls, reply)
    state
  end

  # Each call gets the replies of its own hooks, in its own order.
  @doc false
  def reply_calls(calls, {:ok, results}) do
    _unanswered =
      Enum.reduce(calls, results, fn {from, count}, rest ->
        {mine, rest} = Enum.split(rest, count)
        GenServer.reply(from, {:ok, mine})
        rest
      end)

    :ok
  end

  def reply_calls(calls, {:error, _reason} = error),
    do: Enum.each(calls, fn {from, _count} -> GenServer.reply(from, error) end)

  # The newest read of a key wins. A call whose read began before the oldest
  # generation of `recent_*` may have missed a write the window no longer
  # remembers, so its values are dropped and the keys are read here.
  defp merge_prefetch(live, state) do
    live
    |> Enum.sort_by(fn {_from, prepared, _deadline} -> prepared.read_at end)
    |> Enum.reduce(%{}, fn {_from, prepared, _deadline}, acc ->
      if prepared.read_at >= state.recent_since, do: Map.merge(acc, prepared.prefetch), else: acc
    end)
  end

  defp begin_group(state), do: state |> sync_recent() |> rotate_recent()

  # A tenant closed or opened (`Ankusa.Tenant.Gate`) may have deleted dedupe keys
  # behind this process: forget what it wrote, and every read older than now.
  defp sync_recent(state) do
    epoch = Gate.state(state.instance).epoch

    case state.recent_epoch do
      ^epoch ->
        state

      nil ->
        %{state | recent_epoch: epoch}

      _moved ->
        :ets.delete_all_objects(state.recent_cur)
        :ets.delete_all_objects(state.recent_old)
        mono = System.monotonic_time(:millisecond)
        %{state | recent_epoch: epoch, recent_gen_at: mono, recent_since: mono}
    end
  end

  defp rotate_recent(state) do
    mono = System.monotonic_time(:millisecond)

    if mono - state.recent_gen_at >= @recent_ms or
         :ets.info(state.recent_cur, :size) >= @recent_max do
      :ets.delete_all_objects(state.recent_old)

      %{
        state
        | recent_cur: state.recent_old,
          recent_old: state.recent_cur,
          recent_since: state.recent_gen_at,
          recent_gen_at: mono
      }
    else
      state
    end
  end

  defp recent_get(state, key) do
    case :ets.lookup(state.recent_cur, key) do
      [{_, value}] ->
        {:ok, value}

      [] ->
        case :ets.lookup(state.recent_old, key) do
          [{_, value}] -> {:ok, value}
          [] -> nil
        end
    end
  end

  # The `?u` keys a committed batch wrote, by their value `<<expires_at::64, id>>`.
  defp record_recent(state, checked) do
    case for(
           {:put, :index, key, <<_::64, _::binary>> = value} <- checked.dedupe_ops,
           do: {key, value}
         ) do
      [] -> :ok
      entries -> :ets.insert(state.recent_cur, entries)
    end
  end

  defp recent_forget(state, keys) do
    Enum.each(keys, fn key ->
      :ets.delete(state.recent_cur, key)
      :ets.delete(state.recent_old, key)
    end)
  end

  # The stored value of each dedupe key, as `Store.multi_get/3` returns it: what
  # this process wrote lately, else the caller's prefetch, else a read here.
  defp stored_for(state, keys, prefetch) do
    {known, missing} =
      Enum.reduce(keys, {%{}, []}, fn key, {known, missing} ->
        case recent_get(state, key) do
          {:ok, value} ->
            {Map.put(known, key, {:ok, value}), missing}

          nil ->
            case Map.fetch(prefetch, key) do
              {:ok, result} -> {Map.put(known, key, result), missing}
              :error -> {known, [key | missing]}
            end
        end
      end)

    with {:ok, fetched} <- read_stored(state.instance, missing) do
      {:ok, missing |> Enum.zip(fetched) |> Enum.into(known)}
    end
  end

  defp redrive_batch(entries, extra_ops, state) do
    now = max(System.system_time(:millisecond), state.last_at)
    count = length(entries)
    tenants = entries |> Enum.map(&Tenants.normalize(&1.tenant_id)) |> Enum.uniq()

    with {:ok, admission} <- admit(tenants, state),
         :ok <- all_admitted(tenants, admission) do
      ops =
        entries
        |> Enum.with_index(state.next_seq)
        |> Enum.reduce([], fn {%{
                                 bin: bin,
                                 size: size,
                                 sinks: sinks,
                                 replay_id: replay_id,
                                 source_id: source_id
                               } = entry, seq},
                              ops ->
          ord = Map.fetch!(admission.ords, Tenants.normalize(entry.tenant_id))
          lanes = Map.get(entry, :lanes) || List.duplicate(nil, length(sinks))

          row_ops =
            sinks
            |> Enum.zip(lanes)
            |> Enum.flat_map(fn {{index, mod}, lane} ->
              row =
                Deliveries.encode_row(
                  Deliveries.new_row(mod, now, size, source_id, replay: replay_id, lane: lane)
                )

              Deliveries.new_ops(ord, now, seq, index, size, row, lane && elem(lane, 0))
            end)

          [{:put, :hooks, Keys.hook(seq), bin} | row_ops] ++ ops
        end)

      bytes = Enum.sum(Enum.map(entries, & &1.size))

      ops =
        Deliveries.coalesce(
          [{:put, :default, Keys.meta("next_seq"), <<state.next_seq + count::64>>} | ops] ++
            admission.ops ++ extra_ops
        )

      result = commit(state.instance, ops, count, bytes, true)

      state = %{
        state
        | next_seq: state.next_seq + count,
          next_ord: admission.next_ord,
          last_at: now
      }

      case result do
        :ok ->
          cache_put(state, admission.cache)
          Syncer.mark_durable(state.marks, state.next_seq - 1, now)
          wake_dispatch(state.instance, now)
          {:reply, {:ok, count}, state}

        {:error, reason} ->
          Logger.error(
            "[ankusa] store commit of #{count} replayed hook(s) failed, nothing acked: #{inspect(reason)}"
          )

          {:reply, {:error, reason}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  # `extra_ops` (a quarantine release's pen deletes and job cursor) ride in the
  # same synced batch as the hooks, and commit even when every item was a
  # duplicate.
  defp commit_batch(items, prefetch, extra_ops, state, from) do
    # Never behind a previous batch, even if the wall clock steps back: dispatch
    # keeps its scan floor just under the stamps it has seen, and relies on
    # stamps never going down.
    now = max(System.system_time(:millisecond), state.last_at)
    tenants = items |> Enum.map(&item_tenant/1) |> Enum.uniq()

    case admit(tenants, state) do
      {:ok, admission} ->
        tagged = Enum.map(items, &{&1, admitted(admission, item_tenant(&1))})

        if extra_ops != [] and Enum.any?(tagged, &(elem(&1, 1) != true)) do
          # The caller's ops (a quarantine release's pen deletes) cover every
          # item: refusing one would lose it, so refuse the call.
          {:reply, {:error, refusal(tenants, admission)}, state}
        else
          commit_admitted(tagged, prefetch, extra_ops, admission, now, state, from)
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp commit_admitted(tagged, prefetch, extra_ops, admission, now, state, from) do
    admitted = for {item, true} <- tagged, do: item

    case dedupe_check(state, admitted, now, admission.ords, extra_ops == [], prefetch) do
      {:error, reason} ->
        # The dedupe read failed: no seq consumed, nothing written, nothing
        # acked. Callers see the store error, not a guessed commit.
        {:reply, {:error, reason}, state}

      {:ok, %{fresh: []} = checked} when extra_ops == [] ->
        # Every item was a duplicate (or refused): write nothing, don't wake dispatch.
        {:reply, {:ok, with_refused(tagged, checked.results)}, state}

      {:ok, %{fresh: []} = checked} ->
        # No hook to commit, but the caller's own ops must still land.
        case Store.write(state.instance, extra_ops, sync: true) do
          :ok -> {:reply, {:ok, with_refused(tagged, checked.results)}, state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end

      {:ok, checked} ->
        case dual_gate(state, checked.fresh) do
          :ok -> build_and_commit(tagged, extra_ops, admission, now, state, from, checked)
          {:error, reason} -> refuse_dual(tagged, extra_ops, state, checked, reason)
        end
    end
  end

  defp build_and_commit(tagged, extra_ops, admission, now, state, from, checked) do
    {built, batch_size, bytes} =
      build_ops(checked.fresh, state.next_seq, now, state.archive?, admission.ords)

    ops = Deliveries.coalesce(checked.dedupe_ops ++ built ++ admission.ops ++ extra_ops)
    published = if state.dual, do: published_records(state, checked.fresh, ops), else: []

    cx = %{
      from: from,
      tagged: tagged,
      checked: checked,
      admission: admission,
      ops: ops,
      sizes: {batch_size, bytes},
      now: now,
      published: published,
      extra_ops: extra_ops,
      undo: [checked.dedupe_ops, built]
    }

    cond do
      published == [] ->
        # `disk`, or a batch whose hooks have no obligations (nothing to publish)
        if state.dual,
          do: IntakeGate.abort_probe(state.table, System.monotonic_time(:millisecond))

        disk_commit(cx, state)

      extra_ops == [] ->
        dual_commit(cx, state)

      true ->
        release_commit(cx, state)
    end
  end

  # A group is applied with `sync: false`, and its callers are answered by the
  # syncer once an fsync covers `upto`, the highest seq of the group. A quarantine
  # release (`extra_ops`) is not on the request path and keeps its own synced write.
  defp disk_commit(cx, state) do
    %{
      tagged: tagged,
      checked: checked,
      admission: admission,
      ops: ops,
      sizes: {batch_size, bytes},
      now: now
    } = cx

    pipelined? = cx.extra_ops == []
    first_seq = state.next_seq
    fresh = length(checked.fresh)
    upto = first_seq + fresh - 1
    prev = Syncer.applied(state.marks)

    # Before the write: a reader that can see the rows must see them as unsynced.
    if pipelined?, do: Syncer.set_applied(state.marks, upto)

    result = commit(state.instance, ops, batch_size, bytes, not pipelined?)

    state = %{state | next_seq: first_seq + fresh, next_ord: admission.next_ord, last_at: now}

    case result do
      :ok ->
        cache_put(state, admission.cache)
        record_recent(state, checked)

        committed =
          checked.fresh
          |> Enum.with_index(first_seq)
          |> Enum.map(fn {{env, _bin, _mods, _ttl, _replay_id}, seq} ->
            {:committed, %{env | seq: seq}}
          end)

        reply = {:ok, with_refused(tagged, reassemble(checked.results, committed))}

        if pipelined? do
          Syncer.submit(state.syncer, %{upto: upto, at: now, done: {:reply, cx.from, reply}})
          {:noreply, track(state, upto, cx.undo)}
        else
          Syncer.mark_durable(state.marks, upto, now)
          wake_dispatch(state.instance, now)
          {:reply, reply, state}
        end

      {:error, reason} ->
        if pipelined?, do: Syncer.set_applied(state.marks, prev)

        Logger.error(
          "[ankusa] store commit of #{length(tagged)} hook(s) failed, nothing acked: #{inspect(reason)}"
        )

        {:reply, {:error, reason}, state}
    end
  end

  # ── dual commit (ADR 0002) ────────────────────────────────────────────────

  # Refusals that happen before either half is touched, so nothing is stored:
  # the pending ceilings and the open breaker. (A record too large and a retry
  # that meets an unconfirmed original are refused per item in `dedupe_check/5`.)
  defp dual_gate(%{dual: false}, _fresh), do: :ok

  defp dual_gate(state, fresh) do
    bytes = fresh |> Enum.map(fn {_env, bin, _m, _t, _r} -> byte_size(bin) end) |> Enum.sum()

    cond do
      not Pending.room?(state.table, state.config, length(fresh), bytes) ->
        {:error, :backlog}

      IntakeGate.admit(state.table, state.config, System.monotonic_time(:millisecond)) != :ok ->
        {:error, :breaker}

      true ->
        :ok
    end
  end

  defp refuse_dual(_tagged, extra_ops, state, checked, reason) when extra_ops != [] do
    refused(state, reason, length(checked.fresh))
    {:reply, {:error, :intake_unavailable}, state}
  end

  defp refuse_dual(tagged, _extra_ops, state, checked, reason) do
    refused(state, reason, length(checked.fresh))
    {:reply, {:ok, with_refused(tagged, fresh_to_error(checked, :intake_unavailable))}, state}
  end

  # The fresh items (and duplicates of them within the batch) get `error`.
  defp fresh_to_error(checked, error) do
    fresh_ids = MapSet.new(checked.fresh, fn {env, _bin, _m, _t, _r} -> env.id end)

    Enum.map(checked.results, fn
      {:fresh, _item} ->
        {:error, error}

      {:duplicate, dup} = reply ->
        if MapSet.member?(fresh_ids, dup.id), do: {:error, error}, else: reply

      other ->
        other
    end)
  end

  defp refused(state, reason, count) do
    Ankusa.Telemetry.emit([:intake, :refused], %{count: count}, %{
      instance: state.instance,
      reason: reason
    })
  end

  # The records of the batch that have a `hooks` put (a hook with no
  # obligations consumes a seq and writes nothing, so it is not published).
  defp published_records(state, fresh, ops) do
    puts =
      for {:put, :hooks, key, _bin} <- ops, into: MapSet.new(), do: :binary.decode_unsigned(key)

    fresh
    |> Enum.with_index(state.next_seq)
    |> Enum.flat_map(fn {{env, bin, _mods, ttl, _replay_id}, seq} ->
      if MapSet.member?(puts, seq), do: [{seq, bin, record_meta(state, env), env, ttl}], else: []
    end)
  end

  defp record_meta(state, env) do
    %{
      id: env.id,
      tenant: tenant_of(env),
      source: env.source_id,
      incarnation: state.incarnation
    }
  end

  # Both halves start now: the publish in a task, the synced local write here.
  # The reply waits for the publish's result (`finish_dual/3`).
  defp dual_commit(cx, state) do
    {batch, state} = start_dual(cx, state)

    task = batch.task
    {:noreply, %{state | inflight: Map.put(state.inflight, task.ref, batch)}}
  end

  defp start_dual(cx, state) do
    %{
      from: from,
      tagged: tagged,
      checked: checked,
      admission: admission,
      ops: ops,
      sizes: {batch_size, bytes},
      now: now,
      published: published
    } = cx

    t = state.table
    instance = state.instance
    mono = System.monotonic_time(:millisecond)
    first_seq = state.next_seq
    seqs = Enum.map(published, &elem(&1, 0))
    records = for {seq, bin, meta, _env, _ttl} <- published, do: {seq, bin, meta}

    Pending.enter(
      t,
      for {seq, bin, meta, env, ttl} <- published do
        {seq, env.id, byte_size(bin), pending_triple(env, ttl), meta}
      end,
      first_seq,
      mono
    )

    task =
      Task.Supervisor.async_nolink(state.tasks, fn -> Intake.publish_batch(instance, records) end)

    # A group (`from` is its callers) is applied now and made durable by the
    # syncer, which tells this process; a release waits for its own synced write.
    pipelined? = from != nil
    upto = first_seq + length(checked.fresh) - 1
    prev = Syncer.applied(state.marks)
    if pipelined?, do: Syncer.set_applied(state.marks, upto)

    local = commit(instance, ops ++ meta_ops(state), batch_size, bytes, not pipelined?)

    state = %{
      state
      | next_seq: state.next_seq + length(checked.fresh),
        next_ord: admission.next_ord,
        last_at: now
    }

    # Carried or not, the ordinals of this batch are the ordinals of its hooks.
    cache_put(state, admission.cache)
    if local == :ok, do: record_recent(state, checked)

    {local, state} =
      cond do
        local != :ok ->
          if pipelined?, do: Syncer.set_applied(state.marks, prev)
          {local, state}

        pipelined? ->
          Syncer.submit(state.syncer, %{upto: upto, at: now, done: {:notify, self(), task.ref}})
          {:pending, track(state, upto, cx.undo)}

        true ->
          Syncer.mark_durable(state.marks, upto, now)
          wake_dispatch(instance, now)
          {:ok, state}
      end

    batch = %{
      from: from,
      tagged: tagged,
      checked: checked,
      local: local,
      log: :pending,
      first_seq: first_seq,
      records: records,
      task: task,
      plan: %{id: first_seq, ops: strip_ops(ops), probe_seq: hd(seqs), seqs: seqs, at: now}
    }

    {batch, state}
  end

  defp pending_triple(env, ttl),
    do: if(dedupe?(env, ttl), do: {tenant_of(env), env.source_id, env.dedupe_key})

  # `m:next_seq` and `m:next_ord` markers are the writer's, current at the time;
  # a batch re-applied later must not write a stale one.
  defp strip_ops(ops) do
    next_seq = Keys.meta("next_seq")
    next_ord = Keys.meta("next_ord")

    Enum.reject(ops, fn
      {:put, :default, key, _value} when key in [next_seq, next_ord] -> true
      _op -> false
    end)
  end

  # The watermark and the offset ride in every commit (not synced: a lost batch is
  # found again by the next one or the flush tick). Both only ever advance from
  # confirmed state.
  defp meta_ops(state) do
    t = state.table
    watermark = Pending.watermark(t, state.next_seq)
    Pending.put_meta(t, :flushed_at, System.monotonic_time(:millisecond))

    offset =
      case Pending.offset(t) do
        nil -> []
        offset -> [{:put, :default, Keys.meta("intake_offset"), <<offset::64>>}]
      end

    [{:put, :default, Keys.meta("intake_seq"), <<watermark::64>>} | offset]
  end

  # The publish task answered (or died). Both halves ok: confirm and `201`.
  # Otherwise carry the records to the reconciler and answer `503` per hook.
  defp finish_dual(batch, log_result, state) do
    t = state.table
    mono = System.monotonic_time(:millisecond)
    {log_ok?, position} = log_outcome(log_result)
    local_ok? = batch.local == :ok
    seqs = Enum.map(batch.records, &elem(&1, 0))

    IntakeGate.record(t, state.config, log_ok?, mono)

    if local_ok? and log_ok? do
      Pending.confirm(t, seqs, position)
      reply_calls(batch.from, {:ok, committed_replies(batch)})
    else
      first = if log_ok?, do: max(position - length(seqs) + 1, 0), else: nil

      Pending.carry(
        t,
        %{
          local: if(local_ok?, do: :ok, else: :missing),
          log: if(log_ok?, do: :ok, else: :missing),
          position: first
        },
        batch.records,
        batch.plan,
        mono
      )

      Reconciler.wake(state.instance)

      Logger.error(
        "[ankusa] dual commit of #{length(seqs)} hook(s): local #{if local_ok?, do: "ok", else: "failed"}, " <>
          "intake #{if log_ok?, do: "ok", else: "failed"}; nothing acked, carried to completion"
      )

      reply_calls(batch.from, {:ok, half_replies(batch, local_ok?)})
    end

    state
  end

  defp log_outcome({:ok, position}), do: {true, position}
  defp log_outcome(_error), do: {false, nil}

  defp committed_replies(batch) do
    committed =
      batch.checked.fresh
      |> Enum.with_index(batch.first_seq)
      |> Enum.map(fn {{env, _bin, _mods, _ttl, _replay_id}, seq} ->
        {:committed, %{env | seq: seq}}
      end)

    with_refused(batch.tagged, reassemble(batch.checked.results, committed))
  end

  # `{:error, {:half, reason, id}}` for a hook that is carried: the provider's
  # retry must not be answered `201` until the reconciler completes it. A
  # duplicate of one of this batch's hooks is `intake_pending` for the same reason.
  defp half_replies(batch, local_ok?) do
    reason = if local_ok?, do: :intake_unavailable, else: :store_unavailable
    carried = MapSet.new(batch.records, &elem(&1, 0))

    replies =
      batch.checked.fresh
      |> Enum.with_index(batch.first_seq)
      |> Enum.map(fn {{env, _bin, _mods, _ttl, _replay_id}, seq} ->
        cond do
          MapSet.member?(carried, seq) -> {:error, {:half, reason, env.id}}
          local_ok? -> {:committed, %{env | seq: seq}}
          true -> {:error, :store_unavailable}
        end
      end)

    results = reassemble(batch.checked.results, replies)
    fresh_ids = MapSet.new(batch.checked.fresh, fn {env, _b, _m, _t, _r} -> env.id end)

    results =
      Enum.map(results, fn
        {:duplicate, dup} = reply ->
          if MapSet.member?(fresh_ids, dup.id), do: {:error, :intake_pending}, else: reply

        other ->
          other
      end)

    with_refused(batch.tagged, results)
  end

  # A quarantine release (`extra_ops`: the pen deletes and the job cursor) is not
  # on the request path and must know its outcome: it waits for the publish. Its
  # hooks were acked long ago, so a log half that fails is carried and the call
  # still succeeds once the local half (which holds the cursor) is in; a local
  # half that fails is an error, the replayer retries the page, and a record that
  # reached the log first is replayed at boot as a hook with the same id.
  defp release_commit(cx, state) do
    {batch, state} = start_dual(%{cx | from: nil}, state)

    wait = state.config.intake.publish_timeout_ms + 1_000

    log =
      case Task.yield(batch.task, wait) || Task.shutdown(batch.task) do
        {:ok, result} -> result
        _ -> {:error, :unavailable, nil}
      end

    t = state.table
    seqs = Enum.map(batch.records, &elem(&1, 0))
    {log_ok?, position} = log_outcome(log)
    IntakeGate.record(t, state.config, log_ok?, System.monotonic_time(:millisecond))

    cond do
      batch.local != :ok ->
        Pending.confirm(t, seqs, nil)
        {:reply, {:error, :store_unavailable}, state}

      log_ok? ->
        Pending.confirm(t, seqs, position)
        {:reply, {:ok, committed_replies(batch)}, state}

      true ->
        Pending.carry(
          t,
          %{local: :ok, log: :missing, position: nil},
          batch.records,
          batch.plan,
          System.monotonic_time(:millisecond)
        )

        Reconciler.wake(state.instance)
        {:reply, {:ok, committed_replies(batch)}, state}
    end
  end

  # ── tenant ordinals ───────────────────────────────────────────────────────

  @ord_cache_max 65_536

  defp item_tenant({env, _bin, _mods, _ttl, _replay_id}), do: Tenants.normalize(env.tenant_id)

  # Resolves the ordinals of `tenants`: the cache, then one `multi_get` of the
  # rows of the uncached ones, then new ordinals for those with none (up to
  # `tenants.max`). `ops` are the puts of the new ordinals, for the commit batch.
  # A tenant an erase has closed (`Ankusa.Tenant.Gate`) gets none and is listed
  # in `erasing`; the cache is dropped whenever a tenant is closed or opened, so
  # an ordinal an erase retired is never used again.
  defp admit(tenants, state) do
    gate = Gate.state(state.instance)
    sync_epoch(state, gate.epoch)

    {erasing, tenants} =
      if map_size(gate.tenants) == 0 do
        {MapSet.new(), tenants}
      else
        {closed, open} = Enum.split_with(tenants, &is_map_key(gate.tenants, &1))
        {MapSet.new(closed), open}
      end

    {known, misses} =
      Enum.reduce(tenants, {%{}, []}, fn tenant, {known, misses} ->
        case :ets.lookup(state.ords, tenant) do
          [{_, ord}] -> {Map.put(known, tenant, ord), misses}
          [] -> {known, [tenant | misses]}
        end
      end)

    misses = Enum.reverse(misses)

    with {:ok, stored} <- Tenants.lookup(state.instance, misses) do
      {new, ops, next_ord, _refused} =
        Tenants.allocate(
          state.next_ord,
          Enum.reject(misses, &Map.has_key?(stored, &1)),
          state.tenants_max
        )

      cache = Map.merge(stored, new)

      {:ok,
       %{
         ords: Map.merge(known, cache),
         ops: ops,
         next_ord: next_ord,
         cache: cache,
         erasing: erasing
       }}
    end
  end

  defp sync_epoch(state, epoch) do
    case :ets.lookup(state.ords, :epoch) do
      [{:epoch, ^epoch}] ->
        :ok

      _moved ->
        :ets.delete_all_objects(state.ords)
        :ets.insert(state.ords, {:epoch, epoch})
    end
  end

  # `true` for an item that is admitted, else the reason it is refused.
  defp admitted(admission, tenant) do
    cond do
      Map.has_key?(admission.ords, tenant) -> true
      MapSet.member?(admission.erasing, tenant) -> :tenant_erasing
      true -> :tenant_limit
    end
  end

  # Why a whole call is refused: a tenant being erased, else the tenant cap.
  defp refusal(tenants, admission) do
    if Enum.any?(tenants, &MapSet.member?(admission.erasing, &1)),
      do: :tenant_erasing,
      else: :tenant_limit
  end

  defp all_admitted(tenants, admission) do
    if Enum.all?(tenants, &Map.has_key?(admission.ords, &1)),
      do: :ok,
      else: {:error, refusal(tenants, admission)}
  end

  # Cleared whole when full: a miss is one point read.
  defp cache_put(state, cache) do
    if :ets.info(state.ords, :size) + map_size(cache) > @ord_cache_max,
      do: :ets.delete_all_objects(state.ords)

    :ets.insert(state.ords, Map.to_list(cache))
  end

  # `results` holds the replies of the admitted items in item order; the refused
  # ones get `{:error, reason}` (`:tenant_limit` or `:tenant_erasing`) in their place.
  defp with_refused(tagged, results) do
    {merged, []} =
      Enum.map_reduce(tagged, results, fn
        {_item, true}, [result | rest] -> {result, rest}
        {_item, reason}, rest -> {{:error, reason}, rest}
      end)

    merged
  end

  # ── ingest dedupe ─────────────────────────────────────────────────────────

  # Checks `env.dedupe_key` for every item that also carries a ttl, against the
  # stored `?u` keys and the batch itself. Returns, in original item order,
  # `results` (a `:duplicate` per collapsed item, `{:error, :dedupe_quota}` per
  # item whose new key would pass `dedupe.max_entries` or
  # `dedupe.max_entries_per_tenant`, a `:fresh` marker per item that will
  # commit) plus the fresh items, their dedupe ops (the keys and the counter
  # deltas of the keys they add), and the count. A stored entry counts as a
  # duplicate only while `expires_at > now`. The quota is read from the
  # `dedupe` counters (one `multi_get` per batch that has a keyed item) and
  # counted up as the batch's new keys are admitted; refreshing an expired key
  # adds none. A call that carries `extra_ops` (a quarantine release) is never
  # refused: its hooks were acked long ago.
  defp dedupe_check(state, items, now, ords, enforce?, prefetch) do
    instance = state.instance
    keyed = Enum.filter(items, fn {env, _bin, _rows, ttl, _replay_id} -> dedupe?(env, ttl) end)

    keys =
      keyed
      |> Enum.map(fn {env, _bin, _rows, _ttl, _replay_id} -> u_key(env) end)
      |> Enum.uniq()

    with {:ok, stored} <- stored_for(state, keys, prefetch),
         {:ok, usage} <- read_usage(instance, keyed, ords) do
      {_seen, results, fresh, dedupe_ops, fresh_count, _usage, deltas} =
        Enum.reduce(items, {%{}, [], [], [], 0, usage, []}, fn item,
                                                               {seen, results, fresh, ops, n,
                                                                usage, deltas} ->
          {env, bin, _mods, ttl, _replay_id} = item

          key = if dedupe?(env, ttl), do: u_key(env)

          cond do
            state.dual and enforce? and byte_size(bin) > state.config.intake.max_record_bytes ->
              refused(state, :record_too_large, 1)
              {seen, [{:error, :intake_unavailable} | results], fresh, ops, n, usage, deltas}

            # A retry that meets an original not yet confirmed in the log is never
            # a `201` (ADR 0002, section 3, rule 2)
            state.dual and enforce? and key != nil and
                Pending.keyed?(state.table, {tenant_of(env), env.source_id, env.dedupe_key}) ->
              refused(state, :pending_dedupe, 1)
              {seen, [{:error, :intake_pending} | results], fresh, ops, n, usage, deltas}

            key != nil and Map.has_key?(seen, key) ->
              original_id = Map.fetch!(seen, key)

              {seen, [{:duplicate, %{env | id: original_id, seq: nil}} | results], fresh, ops, n,
               usage, deltas}

            key != nil ->
              case Map.fetch(stored, key) do
                {:ok, {:ok, <<expires_at::64, original_id::binary>>}} when expires_at > now ->
                  seen = Map.put(seen, key, original_id)

                  {seen, [{:duplicate, %{env | id: original_id, seq: nil}} | results], fresh, ops,
                   n, usage, deltas}

                {:ok, {:ok, <<old_at::64, _original_id::binary>>}} ->
                  # An expired key is replaced in place: the live count is unchanged.
                  seen = Map.put(seen, key, env.id)

                  new_ops = [
                    {:put, :index, key, <<now + ttl::64, env.id::binary>>},
                    {:put, :index, expiry_key(now + ttl, env), <<>>},
                    {:delete, :index, expiry_key(old_at, env)}
                  ]

                  {seen, [{:fresh, item} | results], [item | fresh], new_ops ++ ops, n + 1, usage,
                   deltas}

                {:ok, {:ok, _corrupt}} ->
                  # A value too short to hold its 8-byte expiry (the clauses above
                  # took every decodable one). It may not suppress a hook, and it
                  # is overwritten here so the key dedupes again, instead of
                  # staying broken for good. It was counted when it was written.
                  seen = Map.put(seen, key, env.id)

                  new_ops = [
                    {:put, :index, key, <<now + ttl::64, env.id::binary>>},
                    {:put, :index, expiry_key(now + ttl, env), <<>>}
                  ]

                  {seen, [{:fresh, item} | results], [item | fresh], new_ops ++ ops, n + 1, usage,
                   deltas}

                {:ok, :not_found} ->
                  ord = Map.fetch!(ords, item_tenant(item))

                  if enforce? and over_quota?(state, usage, ord) do
                    # Never stored without its key: the provider retries, and the
                    # keys expire on their own.
                    {seen, [{:error, :dedupe_quota} | results], fresh, ops, n, usage, deltas}
                  else
                    seen = Map.put(seen, key, env.id)

                    new_ops = [
                      {:put, :index, key, <<now + ttl::64, env.id::binary>>},
                      {:put, :index, expiry_key(now + ttl, env), <<>>}
                    ]

                    usage = usage |> Map.update(0, 1, &(&1 + 1)) |> Map.update(ord, 1, &(&1 + 1))

                    {seen, [{:fresh, item} | results], [item | fresh], new_ops ++ ops, n + 1,
                     usage, [{ord, :dedupe, 1} | deltas]}
                  end
              end

            true ->
              {seen, [{:fresh, item} | results], [item | fresh], ops, n + 1, usage, deltas}
          end
        end)

      {:ok,
       %{
         results: Enum.reverse(results),
         fresh: Enum.reverse(fresh),
         dedupe_ops: Counters.ops(deltas) ++ dedupe_ops,
         fresh_count: fresh_count
       }}
    end
  end

  # The live-key counts the batch's quota check starts from: the node's and each
  # keyed tenant's, in one `multi_get`. No keyed item, no read.
  defp read_usage(_instance, [], _ords), do: {:ok, %{}}

  defp read_usage(instance, keyed, ords) do
    ord_list =
      keyed |> Enum.map(&Map.fetch!(ords, item_tenant(&1))) |> Enum.uniq()

    with {:ok, counts} <- Counters.read_field(instance, [0 | ord_list], :dedupe) do
      {:ok, [0 | ord_list] |> Enum.zip(counts) |> Map.new()}
    end
  end

  defp over_quota?(state, usage, ord) do
    Map.get(usage, 0, 0) >= state.dedupe_max or
      Map.get(usage, ord, 0) >= state.dedupe_per_tenant
  end

  # `entries` come from `Ankusa.Queue.DedupeSweeper`:
  # `{:expired, expiry_key, u_key, at, ord | nil}` (`ord` is the tenant's
  # ordinal, `nil` when the tenant has none) and `{:undecodable, expiry_key}`.
  # Returns `{:ok, deleted}`, the live keys it removed.
  defp expire_dedupe(state, entries) do
    instance = state.instance
    expired = for {:expired, _expiry, _u_key, _at, _ord} = entry <- entries, do: entry

    with {:ok, stored} <- read_stored(instance, Enum.map(expired, &elem(&1, 2))) do
      {ops, deltas, node_only, deleted} =
        expired
        |> Enum.zip(stored)
        |> Enum.reduce({[], [], 0, 0}, fn
          {{:expired, expiry, u_key, at, ord}, {:ok, <<stored_at::64, _id::binary>>}},
          {ops, deltas, node_only, deleted}
          when stored_at == at ->
            ops = [{:delete, :index, expiry}, {:delete, :index, u_key} | ops]

            if ord == nil,
              do: {ops, deltas, node_only + 1, deleted + 1},
              else: {ops, [{ord, :dedupe, -1} | deltas], node_only, deleted + 1}

          # Refreshed, or already gone: only this expiry key is stale.
          {{:expired, expiry, _u_key, _at, _ord}, _other}, {ops, deltas, node_only, deleted} ->
            {[{:delete, :index, expiry} | ops], deltas, node_only, deleted}
        end)

      # A key written by a version we no longer understand is dropped rather
      # than wedging the sweep.
      undecodable = for {:undecodable, expiry} <- entries, do: {:delete, :index, expiry}

      all =
        Deliveries.coalesce(
          ops ++ undecodable ++ Counters.ops(deltas) ++ Counters.node_ops(dedupe: -node_only)
        )

      with :ok <- Store.write(instance, all, sync: false) do
        # Only the keys that were deleted: a refreshed one is still live.
        u_keys = MapSet.new(expired, &elem(&1, 2))

        recent_forget(
          state,
          for({:delete, :index, key} <- ops, MapSet.member?(u_keys, key), do: key)
        )

        {:ok, deleted}
      end
    end
  end

  # An entry dedupes only with a key, a ttl, and a tenant the key encoding can
  # carry. Ingest guarantees the last (it validates the tenant); a direct
  # `Queue.enqueue` caller may not, and a key builder raising inside the
  # writer's batch would crash the writer, so such an entry just skips dedupe.
  defp dedupe?(env, ttl) do
    is_binary(env.dedupe_key) and is_integer(ttl) and tenant_ok?(env.tenant_id)
  end

  # `nil` is the default tenant, as everywhere else in the edge. A NUL would
  # break the expiry key's tenant/source split, so it is refused.
  defp tenant_ok?(nil), do: true
  defp tenant_ok?(tenant), do: is_binary(tenant) and :binary.match(tenant, <<0>>) == :nomatch

  defp u_key(env), do: Keys.dedupe(tenant_of(env), env.source_id, env.dedupe_key)

  @doc false
  # The `?u` key of a hook that dedupes (`nil` if it does not), for the prefetch
  # `Ankusa.Queue` does before it calls the writer.
  def dedupe_key(env, ttl), do: if(dedupe?(env, ttl), do: u_key(env))

  defp expiry_key(at, env),
    do: Keys.dedupe_expiry(at, tenant_of(env), env.source_id, env.dedupe_key)

  defp tenant_of(env), do: env.tenant_id || "default"

  # No keys means no dedupe anywhere in the batch: skip the store read
  # entirely, so the zero-dedupe hot path never pays for it (and a
  # store-down failure still surfaces through the commit span).
  defp read_stored(_instance, []), do: {:ok, []}
  defp read_stored(instance, keys), do: Store.multi_get(instance, :index, keys)

  # `results` holds `:duplicate` replies and `:fresh` markers in item order;
  # `committed` holds the stamped envelopes of the fresh items in the same
  # order, so the markers are replaced in place.
  defp reassemble(results, committed) do
    {reassembled, []} =
      Enum.map_reduce(results, committed, fn
        {:fresh, _item}, [committed | rest] -> {committed, rest}
        {:duplicate, _} = reply, rest -> {reply, rest}
        {:error, _reason} = reply, rest -> {reply, rest}
      end)

    reassembled
  end

  # ── messages ──────────────────────────────────────────────────────────────

  # Expired dedupe keys are found and batched by `Ankusa.Queue.DedupeSweeper`;
  # the writer only applies a batch it is handed (`{:expire_dedupe, entries}`),
  # because a refresh and a delete of one key must not race.
  @impl true
  def handle_info({:EXIT, syncer, reason}, %{syncer: syncer} = state),
    do: {:stop, {:syncer_down, reason}, state}

  # An fsync failed: what this process cached from the groups it covered (tenant
  # ordinals, dedupe keys written) is not known to be on disk, so it is read again.
  def handle_info({:sync_failed, _reason}, state) do
    :ets.delete_all_objects(state.ords)
    :ets.delete_all_objects(state.recent_cur)
    :ets.delete_all_objects(state.recent_old)
    mono = System.monotonic_time(:millisecond)
    {:noreply, fence(%{state | recent_gen_at: mono, recent_since: mono, fenced: true})}
  end

  def handle_info(:retry_fence, %{fenced: f} = state) when f != nil,
    do: {:noreply, fence(state)}

  def handle_info(:retry_fence, state), do: {:noreply, state}

  # The publish task answered (or died), or the syncer says the local half is
  # durable (or that its fsync failed): the batch is answered when both are in.
  def handle_info({:local_synced, ref, result}, %{dual: true} = state),
    do: {:noreply, advance(state, ref, &%{&1 | local: result})}

  def handle_info({ref, result}, %{dual: true} = state) when is_reference(ref) do
    if is_map_key(state.inflight, ref), do: Process.demonitor(ref, [:flush])
    {:noreply, advance(state, ref, &%{&1 | log: {:done, result}})}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{dual: true} = state),
    do: {:noreply, advance(state, ref, &%{&1 | log: {:done, {:error, :unavailable, nil}}})}

  # The watermark and offset also move without traffic.
  def handle_info(:flush_intake, %{dual: true} = state) do
    flush_meta(state)
    Process.send_after(self(), :flush_intake, state.config.intake.offset_flush_ms)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def handle_continue({:group, call}, state), do: group_commit(call, state)

  @impl true
  def terminate(_reason, %{dual: true} = state), do: flush_meta(state)
  def terminate(_reason, _state), do: :ok

  defp flush_meta(state) do
    Store.write(state.instance, meta_ops(state), sync: false)
    :ok
  end

  # ── a failed fsync (DUR-4.b) ──────────────────────────────────────────────

  # `undo` of a group is the ops it was built from; the group stays in `unsynced`
  # until an fsync covers it.
  defp track(state, upto, undo) do
    durable = Syncer.durable(state.marks)
    kept = Enum.take_while(state.unsynced, &(&1.upto > durable))
    %{state | unsynced: [%{upto: upto, undo: undo} | kept]}
  end

  # Every group applied since the last fsync has been answered with an error (the
  # syncer refuses them all while fenced), so none of its rows may survive: they
  # were never acked and dispatch never saw them (`Syncer.visible/2`). They are
  # deleted in one synced write, and only then does the syncer resume, with the
  # watermark where the writer stands. A write that fails (the disk is still
  # failing, the store is reopening) is tried again; until it lands the writer
  # refuses every call.
  defp fence(state) do
    case Store.write(state.instance, undo_ops(state.unsynced), sync: true) do
      :ok ->
        Syncer.resume(state.syncer, Syncer.applied(state.marks), state.last_at)
        %{state | fenced: nil, unsynced: []}

      {:error, reason} ->
        Logger.error(
          "[ankusa] could not delete the rows of the groups a failed WAL sync left, retrying: " <>
            inspect(reason)
        )

        Process.send_after(self(), :retry_fence, 500)
        state
    end
  end

  # The inverse of what the unsynced groups wrote: their rows deleted and their
  # counters taken back. Kept: the seq marker (a seq is never reused), tenant
  # ordinals (they are not in the lists), and the active-tenant markers, which
  # other rows of the same second may rely on (a stale one is swept).
  defp undo_ops(unsynced) do
    next_seq = Keys.meta("next_seq")

    for group <- unsynced,
        list <- group.undo,
        op <- list,
        inverse <- invert(op, next_seq),
        do: inverse
  end

  defp invert({:put, :default, key, _value}, next_seq) when key == next_seq, do: []

  defp invert({:put, cf, key, _value} = op, _next_seq) do
    if Deliveries.active_marker_put?(op), do: [], else: [{:delete, cf, key}]
  end

  defp invert({:merge, cf, key, delta}, _next_seq), do: [{:merge, cf, key, -delta}]
  defp invert({:delete, _cf, _key}, _next_seq), do: []

  # ── batch construction ───────────────────────────────────────────────────

  defp build_ops(items, first_seq, now, archive?, ords) do
    items
    |> Enum.with_index(first_seq)
    |> Enum.reduce({[], 0, 0}, fn {{env, bin, rows, _ttl, _replay_id}, seq},
                                  {ops, count, bytes} ->
      size = byte_size(bin)
      ord = Map.fetch!(ords, Tenants.normalize(env.tenant_id))

      # `rows` are the `{delivery row, lane or nil}` `Ankusa.Queue` encoded, one per sink
      row_ops =
        rows
        |> Enum.with_index()
        |> Enum.flat_map(fn {{row, lane}, i} ->
          Deliveries.new_ops(ord, now, seq, i, size, row, lane)
        end)

      archive_ops = if archive?, do: Deliveries.archive_ops(ord, seq, size), else: []

      # A hook with no obligations (no sinks, archive off) consumes a seq but
      # writes nothing.
      ops =
        if row_ops == [] and archive_ops == [] do
          ops
        else
          [{:put, :hooks, Keys.hook(seq), bin} | row_ops] ++ archive_ops ++ ops
        end

      {ops, count + 1, bytes + size}
    end)
    |> then(fn {ops, count, bytes} ->
      {[{:put, :default, Keys.meta("next_seq"), <<first_seq + count::64>>} | ops], count, bytes}
    end)
  end

  # ── commit ───────────────────────────────────────────────────────────────

  # One atomic, synced batch covers every hook. Anything but `:ok` means the
  # batch is not durable — the span then emits `[:ankusa, :commit, :exception]`
  # (never a `:stop`), so the commit duration/batch-size series keep counting
  # only real commits.
  defp commit(instance, ops, batch_size, bytes, sync?) do
    Ankusa.Telemetry.span([:commit], %{instance: instance}, fn ->
      case Store.write(instance, ops, sync: sync?) do
        :ok -> {:ok, %{batch_size: batch_size, bytes: bytes}, %{}}
        {:error, reason} -> throw({:store_commit_failed, reason})
      end
    end)
  catch
    :throw, {:store_commit_failed, reason} -> {:error, reason}
  end

  # Records what arrived for `ref`'s batch; once both halves are in, answers it.
  defp advance(state, ref, update) do
    case Map.pop(state.inflight, ref) do
      {nil, _inflight} ->
        state

      {batch, inflight} ->
        batch = update.(batch)

        case batch do
          %{local: :pending} -> %{state | inflight: Map.put(inflight, ref, batch)}
          %{log: :pending} -> %{state | inflight: Map.put(inflight, ref, batch)}
          %{log: {:done, result}} -> finish_dual(batch, result, %{state | inflight: inflight})
        end
    end
  end

  # The stamp tells dispatch the earliest due time this batch can have: its rows
  # became visible only now, and may be older than anything it has scanned past.
  defp wake_dispatch(instance, at) do
    case Ankusa.whereis(instance, :dispatch) do
      pid when is_pid(pid) -> send(pid, {:wake, at})
      nil -> :ok
    end
  end
end
