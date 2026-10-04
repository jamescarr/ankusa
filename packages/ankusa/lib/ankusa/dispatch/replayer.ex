defmodule Ankusa.Dispatch.Replayer do
  @moduledoc """
  Durable, paced replay jobs over the delivery rows.

  A job (`Ankusa.Replay`) re-sends either dead rows (`kind: :dlq`) or archived
  hooks (`kind: :archive`), or releases hooks held in the quarantine pen that
  now pass verification (`kind: :quarantine`). It never bulk-flips rows: once
  a tick it drips up to `rate` rows into the existing due index, and only while
  the Pipeline's oldest-due lag is at most the job's `max_lag_ms` and its
  in-flight window is not full — so a replay only uses dispatch capacity live
  traffic leaves free, and inherits retries, the DLQ, claim check and
  at-least-once bookkeeping from the Pipeline untouched.

  Every job is durable: its record (id, filter, rate, state, cursor, counters)
  lives in the store, and the cursor is written in the same batch as the rows
  it moved, so a restart resumes from the last committed page and no row is
  skipped or doubled by the engine itself (a crash between dispatch claims and
  their outcomes redelivers, which at-least-once allows).

  Started in the `:dispatch` failure domain next to the Pipeline
  (`Ankusa.Instance`), registered as `Ankusa.via(instance, :replayer)`.
  """

  use GenServer

  require Logger

  alias Ankusa.{BlobStore, Dedupe, SourceStore, Store, Telemetry, UUIDv7, Verification, Verifier}
  alias Ankusa.Edge.Quarantine
  alias Ankusa.Queue
  alias Ankusa.Queue.{Archive, Deliveries}
  alias Ankusa.Store.Keys

  @tick_ms 200
  @max_items_per_tick 2_000
  @max_scan_per_tick 20_000
  @max_active 16
  @keep_finished 100
  @auto_pause_min_dead 100
  # How long a job with a transient store/blob error is skipped.
  @error_backoff_ms 1_000
  # Load retry after a store error at boot.
  @load_retry_ms 1_000

  @finished_states [:done, :cancelled, :failed]

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :replayer))
  end

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance)},
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  # ── GenServer ─────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Keyword.fetch!(opts, :config)

    send(self(), :load)

    {:ok,
     %{
       instance: instance,
       config: config,
       loaded?: false,
       jobs: %{},
       buckets: %{},
       windows: %{},
       segments: %{},
       backoff: %{},
       timer: nil
     }}
  end

  @impl true
  def handle_info(:load, state) do
    case load_jobs(state.instance) do
      {:ok, jobs} ->
        # Older jobs may have died mid-restart: their rows are due again from
        # the Pipeline's own recovery, and their cursors resume where the last
        # committed page stopped.
        {:noreply, arm(%{state | jobs: jobs, loaded?: true})}

      {:error, reason} ->
        Logger.warning("[ankusa] replay job load failed, retrying: #{inspect(reason)}")
        Process.send_after(self(), :load, @load_retry_ms)
        {:noreply, state}
    end
  end

  def handle_info(:tick, state) do
    now = System.monotonic_time(:millisecond)

    state =
      case Ankusa.Dispatch.Pipeline.pressure(state.instance) do
        {:ok, pressure} ->
          run_tick(%{state | timer: nil}, pressure, now)

        {:error, :unavailable} ->
          # The pipeline or the store is down: skip every job this tick.
          run_tick(%{state | timer: nil}, :unavailable, now)
      end

    {:noreply, arm(state)}
  end

  def handle_info({:replay_outcomes, counts}, state) do
    {state, to_persist} =
      Enum.reduce(counts, {state, []}, fn {id, counts}, {state, to_persist} ->
        case Map.fetch(state.jobs, id) do
          :error ->
            {state, to_persist}

          {:ok, job} ->
            job = %{
              job
              | delivered: job.delivered + Map.get(counts, :delivered, 0),
                dead: job.dead + Map.get(counts, :dead, 0)
            }

            win = Map.get(state.windows, id, %{delivered: 0, dead: 0})

            win = %{
              delivered: win.delivered + Map.get(counts, :delivered, 0),
              dead: win.dead + Map.get(counts, :dead, 0)
            }

            state = %{
              state
              | jobs: Map.put(state.jobs, id, job),
                windows: Map.put(state.windows, id, win)
            }

            state = maybe_auto_pause(state, id)

            # A finished job has no further progress writes to carry these
            # counts, so persist them now.
            if job.state in @finished_states,
              do: {state, [job | to_persist]},
              else: {state, to_persist}
        end
      end)

    case to_persist do
      [] ->
        :ok

      jobs ->
        case Store.write(state.instance, Enum.map(jobs, &job_put/1), sync: false) do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.warning("[ankusa] replay outcome persist failed: #{inspect(reason)}")
        end
    end

    {:noreply, arm(state)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def handle_call({:start, spec}, _from, state) do
    cond do
      not state.loaded? ->
        {:reply, {:error, :store_unavailable}, state}

      true ->
        active = active_jobs(state.jobs)

        existing = Enum.find(active, &(&1.kind == spec.kind and &1.filter == spec.filter))

        cond do
          existing != nil ->
            {:reply, {:ok, :existing, existing}, state}

          length(active) >= @max_active ->
            {:reply, {:error, :too_many_replays}, state}

          spec.kind in [:archive, :quarantine] and
              Ankusa.whereis(state.instance, :queue_writer) == nil ->
            {:reply, {:error, {:role_not_enabled, :edge}}, state}

          true ->
            now = System.system_time(:millisecond)

            job = %{
              id: UUIDv7.generate(),
              kind: spec.kind,
              filter: spec.filter,
              rate: spec.rate,
              max_lag_ms: spec.max_lag_ms,
              state: :running,
              created_at: now,
              updated_at: now,
              finished_at: nil,
              cursor: nil,
              upto: upto(spec, now),
              moved: 0,
              scanned: 0,
              skipped: 0,
              delivered: 0,
              dead: 0,
              error: nil
            }

            case Store.write(state.instance, [job_put(job)], sync: true) do
              :ok ->
                emit_state(state.instance, job)
                {:reply, {:ok, :created, job}, arm(put_job(state, job))}

              {:error, _reason} ->
                {:reply, {:error, :store_unavailable}, state}
            end
        end
    end
  end

  def handle_call(:list, _from, state) do
    if state.loaded? do
      jobs =
        state.jobs
        |> Map.values()
        |> Enum.sort_by(& &1.created_at, :desc)

      {:reply, {:ok, jobs}, state}
    else
      {:reply, {:error, :store_unavailable}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    if state.loaded? do
      reply =
        case Map.fetch(state.jobs, id) do
          {:ok, job} -> {:ok, job}
          :error -> {:error, :not_found}
        end

      {:reply, reply, state}
    else
      {:reply, {:error, :store_unavailable}, state}
    end
  end

  def handle_call({:update, id, patch}, _from, state) do
    if not state.loaded? do
      {:reply, {:error, :store_unavailable}, state}
    else
      case Map.fetch(state.jobs, id) do
        :error ->
          {:reply, {:error, :not_found}, state}

        {:ok, %{state: job_state}} when job_state in @finished_states ->
          {:reply, {:error, :finished}, state}

        {:ok, job} ->
          now = System.system_time(:millisecond)

          job =
            Enum.reduce(patch, job, fn
              {:state, :running}, job ->
                %{job | state: :running, error: nil, updated_at: now}

              {:state, state}, job ->
                %{job | state: state, updated_at: now}

              {:rate, rate}, job ->
                %{job | rate: rate, updated_at: now}

              {:max_lag_ms, lag}, job ->
                %{job | max_lag_ms: lag, updated_at: now}
            end)

          # Resuming resets the auto-pause window, so a job the operator has
          # fixed the sink behind gets a fresh look.
          state =
            if patch[:state] == :running,
              do: %{state | windows: Map.delete(state.windows, id)},
              else: state

          case Store.write(state.instance, [job_put(job)], sync: true) do
            :ok ->
              emit_state(state.instance, job)
              state = state |> put_job(job) |> prune_finished()
              {:reply, {:ok, job}, arm(state)}

            {:error, _reason} ->
              {:reply, {:error, :store_unavailable}, state}
          end
      end
    end
  end

  # ── the tick ──────────────────────────────────────────────────────────────

  defp run_tick(state, pressure, mono_now) do
    state.jobs
    |> Map.values()
    |> Enum.filter(&(&1.state == :running))
    |> Enum.sort_by(& &1.created_at)
    |> Enum.reduce(state, fn job, state ->
      bucket = Map.get(state.buckets, job.id, %{tokens: 0.0, at: mono_now})
      dt = max(mono_now - bucket.at, 0)

      # The bucket bursts at most one tick's worth of tokens, so the
      # long-run pace is the configured rate, not rate plus a full
      # second of burst. A rate below one item per tick still moves one.
      tokens =
        min(max(1.0, job.rate * @tick_ms / 1_000), bucket.tokens + job.rate * dt / 1_000)

      bucket = %{tokens: tokens, at: mono_now}

      cond do
        in_backoff?(state, job.id, mono_now) ->
          %{state | buckets: Map.put(state.buckets, job.id, bucket)}

        true ->
          case throttle_reason(pressure, job) do
            nil ->
              n = min(trunc(tokens), @max_items_per_tick)

              if n >= 1 do
                case step(state, job, n) do
                  {:ok, state, job, moved, _finished?} ->
                    # The step wrote the job's cursor/counters to the store; the
                    # in-memory copy follows it, so the next tick resumes from
                    # the committed cursor instead of re-running the page.
                    state = put_job(state, job)

                    %{
                      state
                      | buckets:
                          Map.put(state.buckets, job.id, %{
                            tokens: max(tokens - moved, 0),
                            at: mono_now
                          })
                    }

                  {:error, state, job, reason} ->
                    Logger.warning(
                      "[ankusa] replay job #{job.id} step failed, retrying: #{inspect(reason)}"
                    )

                    %{
                      state
                      | backoff: Map.put(state.backoff, job.id, mono_now + @error_backoff_ms)
                    }
                end
              else
                %{state | buckets: Map.put(state.buckets, job.id, bucket)}
              end

            reason ->
              emit_throttled(state.instance, job, reason)
              %{state | buckets: Map.put(state.buckets, job.id, bucket)}
          end
      end
    end)
  end

  defp throttle_reason(:unavailable, _job), do: :unavailable

  defp throttle_reason(%{lag_ms: lag_ms}, job) when lag_ms > job.max_lag_ms, do: :lag
  defp throttle_reason(%{window_full: true}, _job), do: :window_full
  defp throttle_reason(_pressure, _job), do: nil

  defp in_backoff?(%{backoff: backoff}, id, mono_now) do
    case Map.get(backoff, id) do
      nil -> false
      until -> mono_now < until
    end
  end

  # ── DLQ steps ─────────────────────────────────────────────────────────────

  defp step(state, %{kind: :dlq} = job, n) do
    instance = state.instance
    now = System.system_time(:millisecond)

    lower =
      case job.cursor do
        nil -> <<?x, Map.get(job.filter, :since) || 0::64>>
        cursor -> cursor <> <<0>>
      end

    upper = <<?x, job.upto + 1::64>>

    case Deliveries.dead_page(instance, {lower, upper}, job.filter, n, @max_scan_per_tick) do
      {:ok, hits, last, exhausted?, scanned} ->
        case Deliveries.revive_ops(instance, hits, now, job.id) do
          {:ok, ops, revived} ->
            job = %{
              job
              | cursor: last || job.cursor,
                moved: job.moved + revived,
                scanned: job.scanned + scanned,
                updated_at: now
            }

            finished? = exhausted?

            job =
              if finished?,
                do: %{job | state: :done, finished_at: now},
                else: job

            all_ops = ops ++ [job_put(job)]
            sync = if finished?, do: [sync: true], else: [sync: false]

            case Store.write(instance, all_ops, sync) do
              :ok ->
                case Ankusa.whereis(instance, :dispatch) do
                  pid when is_pid(pid) -> send(pid, {:wake, now})
                  nil -> :ok
                end

                if revived > 0 do
                  Telemetry.emit([:replay, :moved], %{count: revived}, %{
                    instance: instance,
                    replay_id: job.id,
                    kind: job.kind
                  })
                end

                if finished? do
                  emit_state(instance, job)
                end

                {:ok, state, job, revived, finished?}

              {:error, reason} ->
                {:error, state, job, reason}
            end

          {:error, reason} ->
            {:error, state, job, reason}
        end

      {:error, reason} ->
        {:error, state, job, reason}
    end
  end

  # ── archive steps ─────────────────────────────────────────────────────────

  defp step(state, %{kind: :archive} = job, n) do
    case load_segment(state, job) do
      {:ok, state, job, entries, bin, row} ->
        redrive_segment(state, job, entries, bin, row, n)

      {:done, state, job} ->
        finish_job(state, job)

      {:skip, state, job, row} ->
        # The segment's objects are gone (lifecycle): skip its records and
        # advance past it.
        advance_skipped(state, job, row)

      {:error, state, job, reason} ->
        {:error, state, job, reason}
    end
  end

  # Re-verify a page of held hooks against each source's current verifier.
  # The ones that pass commit as new hooks (original id, `replay: job.id` on
  # every delivery row) in one synced batch with their pen deletes and the
  # job's cursor; the rest stay in the pen, counted as skipped.
  defp step(state, %{kind: :quarantine} = job, n) do
    instance = state.instance
    now = System.system_time(:millisecond)

    {first, upper} = Quarantine.range(Map.get(job.filter, :since), job.upto)
    lower = if job.cursor, do: job.cursor <> <<0>>, else: first

    with {:ok, hits, last, exhausted?, scanned} <-
           Quarantine.page(instance, {lower, upper}, job.filter, n, @max_scan_per_tick),
         {:ok, entries, ops, bytes, failed} <- release_page(instance, hits, job.id) do
      job = %{
        job
        | cursor: last || job.cursor,
          scanned: job.scanned + scanned,
          skipped: job.skipped + failed,
          updated_at: now
      }

      job = if exhausted?, do: %{job | state: :done, finished_at: now}, else: job

      case commit_release(instance, job, entries, ops, exhausted?) do
        {:ok, moved, duplicates} ->
          job = %{job | moved: job.moved + moved, skipped: job.skipped + duplicates}
          Quarantine.released(instance, bytes)

          if moved > 0 do
            Telemetry.emit([:replay, :moved], %{count: moved}, %{
              instance: instance,
              replay_id: job.id,
              kind: job.kind
            })
          end

          state = if exhausted?, do: finish_release(state, job), else: state
          {:ok, state, job, moved, exhausted?}

        {:error, reason} ->
          {:error, state, job, reason}
      end
    else
      {:error, reason} -> {:error, state, job, reason}
    end
  end

  # Find (and load) the segment the cursor points at, or the next one covering
  # the job's window.
  defp load_segment(state, job) do
    case Map.get(state.segments, job.id) do
      nil ->
        after_first_seq =
          case job.cursor do
            {fs, _offset} -> fs
            _ -> 0
          end

        min_id = UUIDv7.min_for(max(Map.fetch!(job.filter, :from) - 1_000, 0))
        max_id = UUIDv7.max_for(Map.fetch!(job.filter, :to) + 1_000)

        case Archive.next_segment(state.instance, after_first_seq, min_id, max_id) do
          {:ok, nil} -> {:done, state, job}
          {:ok, row} -> fetch_segment(state, job, row)
          {:error, reason} -> {:error, state, job, reason}
        end

      %{row: row, entries: entries, bin: bin} when entries != [] ->
        {:ok, state, job, entries, bin, row}

      %{row: _row} ->
        # The cached segment is exhausted but not yet advanced; load the next.
        advance_loaded(state, job)
    end
  end

  defp fetch_segment(state, job, row) do
    instance = state.instance

    with {:ok, idx_bin} <- BlobStore.get(instance, row.idx_key),
         {:ok, index} <- decode_index(idx_bin),
         entries <- window_entries(index, job.filter),
         {:ok, bin} <- BlobStore.get(instance, row.key) do
      cache = %{row: row, entries: entries, bin: bin}
      {:ok, %{state | segments: Map.put(state.segments, job.id, cache)}, job, entries, bin, row}
    else
      # Lifecycle removed the segment's objects: the records are gone, so the
      # job skips them and carries on.
      {:error, :not_found} -> {:skip, state, job, row}
      {:error, :undecodable} -> {:skip, state, job, row}
      {:error, reason} -> {:error, state, job, reason}
    end
  end

  # An index that does not decode is a corrupt object: skip the segment rather
  # than crash the Replayer forever on it.
  defp decode_index(bin) do
    {:ok, :erlang.binary_to_term(bin)}
  rescue
    _ -> {:error, :undecodable}
  end

  defp advance_loaded(state, job) do
    {fs, _offset} = job.cursor

    min_id = UUIDv7.min_for(max(Map.fetch!(job.filter, :from) - 1_000, 0))
    max_id = UUIDv7.max_for(Map.fetch!(job.filter, :to) + 1_000)

    case Archive.next_segment(state.instance, fs, min_id, max_id) do
      {:ok, nil} -> {:done, state, job}
      {:ok, row} -> fetch_segment(state, job, row)
      {:error, reason} -> {:error, state, job, reason}
    end
  end

  defp window_entries(index, filter) do
    from = Map.fetch!(filter, :from)
    to = Map.fetch!(filter, :to)

    index
    |> Enum.flat_map(fn {id, {offset, length, _seq}} ->
      case UUIDv7.timestamp_ms(id) do
        {:ok, ms} when ms >= from - 1_000 and ms <= to + 1_000 -> [{ms, offset, length}]
        _ -> []
      end
    end)
    |> Enum.sort_by(&elem(&1, 1))
  end

  defp redrive_segment(state, job, entries, bin, row, n) do
    offset0 =
      case job.cursor do
        {_fs, :done} -> 0
        {_fs, offset} -> offset
        _ -> 0
      end

    from = Map.fetch!(job.filter, :from)
    to = Map.fetch!(job.filter, :to)
    {codec, _opts} = state.config.storage.codec

    {kept, skipped, scanned, done_all?, last_offset} =
      Enum.reduce_while(
        entries,
        {[], 0, 0, true, offset0},
        fn {_ms, offset, length}, {kept, skipped, scanned, done_all?, _last} ->
          cond do
            # Entries before the cursor were handled by an earlier page.
            offset < offset0 ->
              {:cont, {kept, skipped, scanned, done_all?, offset0}}

            # The page is full: stop here and resume at this entry next tick.
            length(kept) >= n ->
              {:halt, {kept, skipped, scanned, false, offset}}

            true ->
              case decode_entry(codec, bin, offset, length, from, to, job.filter) do
                {:keep, entry} ->
                  {:cont, {[entry | kept], skipped, scanned + 1, done_all?, offset + length}}

                :unusable ->
                  {:cont, {kept, skipped + 1, scanned + 1, done_all?, offset + length}}

                :skip ->
                  {:cont, {kept, skipped, scanned + 1, done_all?, offset + length}}
              end
          end
        end
      )

    commit_archive(state, job, Enum.reverse(kept), skipped, scanned, row, done_all?, last_offset)
  end

  defp decode_entry(codec, bin, offset, length, from, to, filter) do
    case codec.decode_record(binary_part(bin, offset, length)) do
      {:ok, payload} ->
        env = Ankusa.Envelope.from_binary(payload)

        cond do
          env.received_at < from or env.received_at > to -> :skip
          not matches_source?(filter, env.source_id) -> :skip
          true -> {:keep, %{bin: payload, size: byte_size(payload), env: env}}
        end

      {:error, _reason} ->
        :unusable
    end
  rescue
    _ -> :unusable
  end

  defp matches_source?(filter, source_id) do
    case Map.get(filter, :source_id) do
      nil -> true
      expected -> expected == source_id
    end
  end

  defp commit_archive(state, job, kept, skipped, scanned, row, done_all?, last_offset) do
    instance = state.instance
    now = System.system_time(:millisecond)
    filter = job.filter

    # Resolve each kept hook's source once, and keep only entries that bind to
    # at least one current sink.
    {entries, skipped} =
      Enum.reduce(kept, {[], skipped}, fn %{env: env, bin: bin, size: size}, {entries, skipped} ->
        case SourceStore.fetch(instance, env.source_id) do
          {:ok, source} ->
            case filter_sinks(filter, source.sinks) do
              [] ->
                {entries, skipped + 1}

              indexes ->
                {[%{bin: bin, size: size, sinks: indexes, replay_id: job.id} | entries], skipped}
            end

          :error ->
            {entries, skipped + 1}
        end
      end)

    moved = length(entries)

    job = %{
      job
      | cursor: {row.first_seq, if(done_all?, do: :done, else: last_offset)},
        moved: job.moved + moved,
        skipped: job.skipped + skipped,
        scanned: job.scanned + scanned,
        updated_at: now
    }

    state =
      if done_all?,
        do: %{state | segments: Map.delete(state.segments, job.id)},
        else: state

    result =
      case entries do
        [] ->
          Store.write(instance, [job_put(job)], sync: false)

        entries ->
          # The writer lives in the edge domain: a down or restarting writer
          # must back this job off like any transient error, never crash the
          # Replayer (and with it the dispatch domain).
          try do
            Queue.redrive(instance, entries, [job_put(job)])
          catch
            :exit, reason -> {:error, {:writer_down, reason}}
          end
      end

    case result do
      :ok -> {:ok, state, job, moved, false}
      {:ok, _n} -> {:ok, state, job, moved, false}
      {:error, reason} -> {:error, state, job, reason}
    end
  end

  defp filter_sinks(filter, sinks) do
    case Map.get(filter, :sinks) do
      nil ->
        sinks
        |> Enum.with_index()
        |> Enum.map(fn {{mod, _opts}, i} -> {i, mod} end)

      indexes ->
        Enum.flat_map(indexes, fn i ->
          case Enum.at(sinks, i) do
            nil -> []
            {mod, _opts} -> [{i, mod}]
          end
        end)
    end
  end

  defp finish_job(state, job) do
    now = System.system_time(:millisecond)
    job = %{job | state: :done, finished_at: now, updated_at: now}

    case Store.write(state.instance, [job_put(job)], sync: true) do
      :ok ->
        emit_state(state.instance, job)
        {:ok, prune_finished(state), job, 0, true}

      {:error, reason} ->
        {:error, state, job, reason}
    end
  end

  defp advance_skipped(state, job, row) do
    now = System.system_time(:millisecond)

    job = %{
      job
      | cursor: {row.first_seq, :done},
        skipped: job.skipped + row.count,
        updated_at: now
    }

    state = %{state | segments: Map.delete(state.segments, job.id)}

    case Store.write(state.instance, [job_put(job)], sync: false) do
      :ok -> {:ok, state, job, 0, false}
      {:error, reason} -> {:error, state, job, reason}
    end
  end

  # ── quarantine steps ──────────────────────────────────────────────────────

  # Each hit's envelope, re-verified. Returns the queue entries, the deletes for
  # their pen entries, the bytes those held, and how many hits stay held (a
  # failed check, an unknown source, a body gone or unreadable). A store error
  # fails the page so the tick retries it.
  defp release_page(instance, hits, replay_id) do
    result =
      Enum.reduce_while(hits, {[], [], 0, 0}, fn {key, summary} = hit,
                                                 {entries, ops, bytes, failed} ->
        case Quarantine.envelope(instance, hit) do
          {:ok, env} ->
            case reverify(instance, env) do
              {:ok, entry} ->
                entry = Map.put(entry, :replay_id, replay_id)
                size = Map.get(summary, :size, 0)

                {:cont,
                 {[entry | entries], Quarantine.delete_ops(key) ++ ops, bytes + size, failed}}

              :failed ->
                {:cont, {entries, ops, bytes, failed + 1}}
            end

          held when held in [:not_found, {:error, :undecodable}] ->
            {:cont, {entries, ops, bytes, failed + 1}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      end)

    case result do
      {:error, reason} -> {:error, reason}
      {entries, ops, bytes, failed} -> {:ok, Enum.reverse(entries), ops, bytes, failed}
    end
  end

  # The timestamp window is judged at the hook's receive time (`:now`, see
  # `Ankusa.Verifier.check_timestamp/2`): the release is late, the hook was
  # not. A verifier that raises fails the check like any other.
  defp reverify(instance, env) do
    with {:ok, source} <- SourceStore.fetch(instance, env.source_id) do
      env = %{env | tenant_id: env.tenant_id || source.tenant_id}
      {mod, opts} = source.verifier

      case safe_verify(mod, env, opts) do
        :ok ->
          verification = %Verification{
            status: :ok,
            provider: mod,
            scheme: Verifier.scheme_name(mod, opts)
          }

          env = %{env | verification: verification, seq: nil}

          {:ok,
           %{
             envelope: %{env | dedupe_key: Dedupe.key(source.dedupe, env)},
             sinks: source.sinks,
             dedupe_ttl_ms: source.dedupe && source.dedupe.ttl_ms
           }}

        _failed ->
          :failed
      end
    else
      :error -> :failed
    end
  end

  defp safe_verify(mod, env, opts) do
    mod.verify(env, Keyword.put(opts, :now, div(env.received_at, 1000)))
  rescue
    error -> {:error, {:raised, error}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # Nothing passed: only the cursor moves. Otherwise the writer commits the
  # hooks, the pen deletes and the cursor as one synced batch. A duplicate
  # (the provider's own retry already got through) still leaves the pen. The
  # record written here predates this page's moved/duplicate counts; the next
  # progress write (or `finish_release/2`) carries them.
  defp commit_release(instance, job, [], _ops, finished?) do
    case Store.write(instance, [job_put(job)], sync: finished?) do
      :ok -> {:ok, 0, 0}
      {:error, reason} -> {:error, reason}
    end
  end

  defp commit_release(instance, job, entries, ops, _finished?) do
    # The writer lives in the edge domain: a down or restarting writer must
    # back this job off like any transient error, never crash the Replayer.
    result =
      try do
        Queue.release(instance, entries, ops ++ [job_put(job)])
      catch
        :exit, reason -> {:error, {:writer_down, reason}}
      end

    case result do
      {:ok, results} ->
        {:ok, Enum.count(results, &match?({:committed, _}, &1)),
         Enum.count(results, &match?({:duplicate, _}, &1))}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # The counters the commit just settled (moved, duplicates) were not in the
  # record it wrote; a finished job gets no further progress write, so persist
  # them now.
  defp finish_release(state, job) do
    case Store.write(state.instance, [job_put(job)], sync: true) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("[ankusa] replay job #{job.id} final persist failed: #{inspect(reason)}")
    end

    emit_state(state.instance, job)
    prune_finished(put_job(state, job))
  end

  # ── job bookkeeping ───────────────────────────────────────────────────────

  # `:dlq` and `:quarantine` jobs only touch entries at or before their own
  # creation (or their `until`), so what fails again during the job is never
  # picked up a second time.
  defp upto(%{kind: kind, filter: filter}, now) when kind in [:dlq, :quarantine] do
    min(Map.get(filter, :until, now), now)
  end

  defp upto(_spec, _now), do: nil

  defp job_put(job) do
    {:put, :default, Keys.replay_job(job.id), :erlang.term_to_binary(job)}
  end

  defp put_job(state, job) do
    %{state | jobs: Map.put(state.jobs, job.id, job)}
  end

  defp active_jobs(jobs) do
    jobs
    |> Map.values()
    |> Enum.reject(&(&1.state in @finished_states))
  end

  # A job that just finished may leave finished jobs piling up: keep the newest
  # `@keep_finished` and delete the rest.
  defp prune_finished(state) do
    stale =
      state.jobs
      |> Map.values()
      |> Enum.filter(&(&1.state in @finished_states))
      |> Enum.sort_by(& &1.created_at, :desc)
      |> Enum.drop(@keep_finished)
      |> Enum.map(& &1.id)

    if stale == [] do
      state
    else
      case Store.write(
             state.instance,
             Enum.map(stale, &{:delete, :default, Keys.replay_job(&1)}),
             sync: false
           ) do
        :ok ->
          %{state | jobs: Map.drop(state.jobs, stale)}

        {:error, _reason} ->
          state
      end
    end
  end

  defp maybe_auto_pause(state, id) do
    case Map.fetch(state.jobs, id) do
      {:ok, %{state: :running} = job} ->
        win = Map.fetch!(state.windows, id)

        if win.dead >= @auto_pause_min_dead and win.dead > win.delivered do
          now = System.system_time(:millisecond)

          job = %{
            job
            | state: :paused,
              error:
                "auto-paused: #{win.dead} replayed deliveries dead-lettered again, " <>
                  "#{win.delivered} delivered, since it last resumed",
              updated_at: now
          }

          case Store.write(state.instance, [job_put(job)], sync: true) do
            :ok ->
              emit_state(state.instance, job)
              put_job(state, job)

            {:error, _reason} ->
              state
          end
        else
          state
        end

      _ ->
        state
    end
  end

  # ── timer ─────────────────────────────────────────────────────────────────

  defp arm(state) do
    any_running? = Enum.any?(state.jobs, fn {_id, job} -> job.state == :running end)

    cond do
      any_running? and state.timer == nil ->
        %{state | timer: Process.send_after(self(), :tick, @tick_ms)}

      not any_running? and state.timer != nil ->
        Process.cancel_timer(state.timer)
        %{state | timer: nil}

      true ->
        state
    end
  end

  # ── load ──────────────────────────────────────────────────────────────────

  defp load_jobs(instance) do
    %{lo: lo, hi: hi} = Keys.family(:replays)

    Store.fold(instance, :replays, {lo, hi}, %{}, fn _key, value, acc ->
      job = :erlang.binary_to_term(value)
      {:cont, Map.put(acc, job.id, job)}
    end)
  end

  # ── telemetry ─────────────────────────────────────────────────────────────

  defp emit_throttled(instance, job, reason) do
    Telemetry.emit([:replay, :throttled], %{}, %{
      instance: instance,
      replay_id: job.id,
      reason: reason
    })
  end

  defp emit_state(instance, job) do
    Telemetry.emit([:replay, :state], %{}, %{
      instance: instance,
      replay_id: job.id,
      kind: job.kind,
      state: job.state
    })
  end
end
