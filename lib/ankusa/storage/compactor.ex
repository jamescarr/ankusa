defmodule Ankusa.Storage.Compactor do
  @moduledoc """
  Packs committed WAL records into immutable segments and reclaims the WAL.

  Each tick reads WAL records past the compactor cursor in bounded chunks and
  packs them into segments of at most `config.storage.roll_bytes` of payload.
  A backlog therefore becomes *several* segments per tick instead of one
  unbounded one: peak memory is a chunk plus one segment, whatever the backlog
  and however long a storage node was unavailable.

  Each segment is encoded via `config.storage.codec`, `PUT` through the blob
  store, and followed by its per-record rows in `Ankusa.Storage.Index` and an
  advance of the durable compactor cursor.

  The WAL is then truncated through `min(compactor_seq, dispatch_seq)`: records
  the dispatch pipeline has not yet consumed are never dropped, preserving
  at-least-once delivery.

  ## The storage lease

  Compaction is single-writer, but *serving* `Ankusa.Storage.fetch/2` is not: any
  storage node may answer a lookup. So a storage node runs `Index.open/1`
  unconditionally and compacts only while it holds the `:storage` lease. A
  standby keeps serving lookups from its local index, and catches up by folding
  segment sidecars out of the blob store the moment it takes the lease over
  (`Index.repair/1`). Losing the lease (an expired one, or a `{:error, :fenced}`
  write) steps the node down after the segment it is writing — segments are
  deterministic and idempotent, so re-writing one is harmless.

  ## Write order and crash safety

  One segment is made durable in this order:

      encode → put segment → put .idx sidecar → Index.append → hwm → put_cursor → truncate

  A crash anywhere in there leaves the cursor behind the segment, so the same
  seqs are re-encoded, re-`PUT` (same deterministic key) and the sidecar is
  written again. That is why the sidecar needs no cleanup pass, and why the
  cursor is advanced only once the sidecar and the local rows exist.
  """

  use GenServer

  alias Ankusa.{Config, DurableLog, Envelope}
  alias Ankusa.Storage.Index
  alias Ankusa.WAL.LeaseHelpers

  # Read granularity: bounds one read's memory, and how much the roll check can
  # overshoot `roll_bytes` by at most.
  @read_chunk 256

  # ── lifecycle ─────────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :compactor))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc """
  Run one compaction synchronously; returns the number of segments written.

  A standby (a node that does not hold the storage lease) writes nothing and
  returns `{:ok, 0}` — another node owns the compactor cursor.
  """
  @spec tick(atom()) :: {:ok, non_neg_integer()}
  def tick(instance), do: GenServer.call(Ankusa.via(instance, :compactor), :tick)

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    %Config{} = config = Keyword.fetch!(opts, :config)
    storage = config.storage
    ttl_ms = storage.lease_ttl_ms

    # This process owns the live index: it is the only writer, it reloads the
    # table from the file whenever it (re)starts, and lookups elsewhere read it.
    # Every storage node opens it — serving lookups is not lease-gated, only
    # compacting is.
    Index.open(config)

    state = %{
      instance: instance,
      config: config,
      cursor: 0,
      interval: storage.interval_ms,
      lease: nil,
      holder: "#{node()}/#{inspect(self())}",
      ttl_ms: ttl_ms,
      renew_ms: div(ttl_ms, 3),
      safety_margin_ms: storage.lease_safety_margin_ms,
      lease_renew_at: 0,
      lease_deadline: 0
    }

    case try_acquire(state) do
      {:ok, state} ->
        state = activate(state)
        {:ok, schedule(state)}

      {:standby, state} ->
        {:ok, standby(state)}
    end
  end

  # ── ticks ─────────────────────────────────────────────────────────────────

  @impl true
  def handle_call(:tick, _from, %{lease: nil} = state) do
    {:reply, {:ok, 0}, state}
  end

  def handle_call(:tick, _from, state) do
    {n, state} = compact(state)
    {:reply, {:ok, n}, state}
  end

  @impl true
  def handle_info(:tick, %{lease: nil} = state) do
    {:noreply, schedule(state)}
  end

  def handle_info(:tick, state) do
    cond do
      mono_ms() > state.lease_deadline ->
        {:noreply, schedule(step_down(state))}

      mono_ms() >= state.lease_renew_at ->
        {:noreply, schedule(renew(state))}

      true ->
        {_n, state} = compact(state)
        {:noreply, schedule(state)}
    end
  end

  def handle_info(:acquire_lease, %{lease: nil} = state) do
    case try_acquire(state) do
      {:ok, state} -> {:noreply, schedule(activate(state))}
      {:standby, state} -> {:noreply, standby(state)}
    end
  end

  def handle_info(:acquire_lease, state), do: {:noreply, state}

  def handle_info(_message, state), do: {:noreply, state}

  # Best effort: releasing the lease on the way out makes a rolling restart hand
  # over immediately instead of leaving the standby waiting out the TTL. The WAL
  # may already be gone during a shutdown, hence the catch.
  @impl true
  def terminate(_reason, %{lease: nil}), do: :ok

  def terminate(_reason, state) do
    Ankusa.WAL.release_lease(state.instance, state.lease)
    :ok
  catch
    :exit, _ -> :ok
  end

  # ── lease ─────────────────────────────────────────────────────────────────

  defp try_acquire(state) do
    try do
      case Ankusa.WAL.acquire_lease(state.instance, :storage, state.holder, state.ttl_ms) do
        {:ok, lease} ->
          lease = Map.put(lease, :instance, state.instance)
          LeaseHelpers.emit(:acquired, lease)
          {:ok, arm(%{state | lease: lease})}

        {:error, {:held, _holder}} ->
          {:standby, %{state | lease: nil}}
      end
    catch
      # The WAL is unreachable: same as "someone else holds the lease".
      :exit, _ -> {:standby, %{state | lease: nil}}
    end
  end

  defp renew(state) do
    try do
      case Ankusa.WAL.renew_lease(state.instance, state.lease) do
        {:ok, lease} ->
          lease = Map.put(lease, :instance, state.instance)
          LeaseHelpers.emit(:renewed, lease)
          arm(%{state | lease: lease})

        {:error, :lost} ->
          LeaseHelpers.emit(:lost, state.lease)
          step_down(state)
      end
    catch
      :exit, _ ->
        LeaseHelpers.emit(:lost, state.lease)
        step_down(state)
    end
  end

  defp arm(state) do
    %{
      state
      | lease_deadline: mono_ms() + state.ttl_ms - state.safety_margin_ms,
        lease_renew_at: mono_ms() + state.renew_ms
    }
  end

  defp standby(state) do
    Process.send_after(self(), :acquire_lease, state.renew_ms)
    state
  end

  # A new holder never resumes from a stale in-memory cursor: re-read the
  # durable one (leader-consistent), then fold the segments written while this
  # node was a standby.
  defp activate(state) do
    case safe_get_cursor(state.instance, :compactor) do
      {:ok, cursor} ->
        Index.repair(state.config)
        %{state | cursor: cursor}

      :error ->
        step_down(state)
    end
  end

  defp safe_get_cursor(instance, name) do
    try do
      {:ok, Ankusa.WAL.get_cursor(instance, name)}
    catch
      :exit, _ -> :error
      :error, _ -> :error
    end
  end

  defp step_down(state) do
    Map.put(state, :lease, nil) |> standby()
  end

  defp mono_ms, do: System.monotonic_time(:millisecond)

  # ── compaction ────────────────────────────────────────────────────────────

  defp compact(state), do: compact(state, 0)

  defp compact(state, written) do
    {entries, next_cursor, more?} =
      collect(state, state.cursor, state.config.storage.roll_bytes)

    case entries do
      [] ->
        {written, state}

      entries ->
        case write_segment(state, entries) do
          :ok ->
            state = %{state | cursor: next_cursor}

            if more? do
              compact(state, written + 1)
            else
              {written + 1, state}
            end

          :fenced ->
            # The lease moved on. Stop after this segment; the new holder
            # re-encodes the same seqs into the same deterministic keys.
            {written + 1, step_down(state)}
        end
    end
  end

  # Collect envelopes up to a payload-byte budget. `more?` says whether the WAL
  # may still hold records past the returned cursor, which is what lets a tick
  # write several segments instead of stopping at the first one.
  defp collect(state, cursor, roll_bytes) do
    collect(state, cursor, roll_bytes, [], 0, false)
  end

  defp collect(state, cursor, roll_bytes, acc, bytes, more?) do
    # `acc != []` matters: with a budget of 0 the first check would otherwise
    # return nothing at all and no segment would ever be written.
    if acc != [] and bytes >= roll_bytes do
      {Enum.reverse(acc), cursor, more?}
    else
      case Ankusa.WAL.read(state.instance, cursor, @read_chunk) do
        [] ->
          {Enum.reverse(acc), cursor, more?}

        envelopes ->
          {entries, taken, leftover?} = take_within_budget(envelopes, roll_bytes - bytes)

          cursor =
            if taken == 0,
              do: cursor,
              else: envelopes |> Enum.at(taken - 1) |> Map.fetch!(:seq)

          bytes =
            bytes +
              Enum.reduce(entries, 0, fn {_env, record}, sum ->
                sum + byte_size(record.payload)
              end)

          more? = leftover? or length(envelopes) == @read_chunk

          collect(state, cursor, roll_bytes, Enum.reverse(entries) ++ acc, bytes, more?)
      end
    end
  end

  # Take records until the budget is spent — always at least one, so a tick
  # always makes progress even when a single record exceeds `roll_bytes`.
  defp take_within_budget(envelopes, budget), do: take_within_budget(envelopes, budget, [], 0)

  defp take_within_budget([env | rest], budget, acc, bytes) do
    record = %{key: env.id, payload: Envelope.to_binary(env)}
    acc = [{env, record} | acc]
    bytes = bytes + byte_size(record.payload)

    if bytes >= budget or rest == [] do
      {Enum.reverse(acc), length(acc), rest != []}
    else
      take_within_budget(rest, budget, acc, bytes)
    end
  end

  defp take_within_budget([], _budget, acc, _bytes), do: {Enum.reverse(acc), length(acc), false}

  defp write_segment(state, entries) do
    instance = state.instance
    config = state.config
    started = System.monotonic_time()

    first_seq = entries |> hd() |> elem(0) |> Map.fetch!(:seq)
    last_seq = entries |> List.last() |> elem(0) |> Map.fetch!(:seq)
    {codec, _} = config.storage.codec

    {segment, index} = codec.encode(Enum.map(entries, fn {_env, record} -> record end))

    key = "seg/#{pad(first_seq)}-#{pad(last_seq)}.seg"
    :ok = Ankusa.BlobStore.put(instance, key, segment)

    rows =
      Enum.zip(entries, index)
      |> Enum.map(fn {{env, _record}, entry} ->
        %{
          event_id: env.id,
          source_id: env.source_id,
          tenant_id: env.tenant_id,
          received_at: env.received_at,
          seq: env.seq,
          segment_key: key,
          offset: entry.offset,
          length: entry.length
        }
      end)

    # The sidecar is what a standby storage replica folds to serve lookups it
    # never itself compacted. Written before the cursor moves, so a crash
    # re-writes both (same key, same bytes).
    :ok = Ankusa.BlobStore.put(instance, sidecar_key(key), DurableLog.frame(rows))
    :ok = Index.append(config, rows)
    :ok = Index.put_hwm(config, key)

    try do
      case Ankusa.WAL.put_cursor(instance, :compactor, last_seq, state.lease.token) do
        :ok ->
          # never truncate past what dispatch has consumed — at-least-once
          dispatch_seq = Ankusa.WAL.get_cursor(instance, :dispatch)

          case Ankusa.WAL.truncate_through(
                 instance,
                 min(last_seq, dispatch_seq),
                 state.lease.token
               ) do
            :ok ->
              Ankusa.Telemetry.emit(
                [:compact, :stop],
                %{
                  records: length(entries),
                  bytes: byte_size(segment),
                  duration: System.monotonic_time() - started
                },
                %{instance: instance}
              )

              :ok

            {:error, :fenced} ->
              :fenced
          end

        {:error, :fenced} ->
          :fenced
      end
    catch
      # The WAL went away mid-compaction: same as being fenced, the new holder
      # re-encodes the same seqs into the same keys.
      :exit, _ -> :fenced
      :error, _ -> :fenced
    end
  end

  defp schedule(%{interval: interval} = state) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :tick, interval)
    state
  end

  defp schedule(state), do: state

  defp pad(seq), do: seq |> Integer.to_string() |> String.pad_leading(20, "0")

  defp sidecar_key(key), do: String.replace_suffix(key, ".seg", ".idx")
end
