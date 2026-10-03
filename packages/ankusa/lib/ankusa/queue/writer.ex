defmodule Ankusa.Queue.Writer do
  @moduledoc """
  The only seq assigner: commits batches of hooks to the store with
  `sync: true`, so a `2xx` to the provider always means the bytes are on disk.

  One process per instance, under `Ankusa.via(instance, :queue_writer)`. The
  commit is a single atomic store batch: the hook, one pending delivery row and
  due key per sink, the archive obligation when the `:storage` role runs, and
  the `m:next_seq` marker. A failed commit consumes its seqs (gaps are
  allowed) but acks nothing.
  """

  use GenServer

  require Logger

  alias Ankusa.Config
  alias Ankusa.Store
  alias Ankusa.Store.Keys

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
    config = Keyword.fetch!(opts, :config)

    # A seq is never reused: if either read fails, refuse to start rather than
    # guess a floor.
    with {:ok, marker} <- marker_seq(instance),
         {:ok, last} <- last_hook_seq(instance) do
      next_seq = max(marker, last + 1)

      Logger.info("[ankusa] store next_seq=#{next_seq}")

      Process.send_after(self(), :sweep_dedupe, 10_000)

      {:ok,
       %{
         instance: instance,
         next_seq: next_seq,
         archive?: Config.role?(config, :storage),
         last_reopen: nil,
         last_at: 0
       }}
    else
      {:error, reason} -> {:stop, {:queue_writer_init_failed, reason}}
    end
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
  def handle_call({:enqueue, items, deadline}, {caller, _tag}, state) do
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
        commit_batch(items, state)
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

  defp expired?(:infinity), do: false
  defp expired?(deadline), do: System.monotonic_time(:millisecond) >= deadline

  defp redrive_batch(entries, extra_ops, state) do
    now = max(System.system_time(:millisecond), state.last_at)
    count = length(entries)

    ops =
      entries
      |> Enum.with_index(state.next_seq)
      |> Enum.reduce([], fn {%{bin: bin, size: size, sinks: sinks, replay_id: replay_id}, seq},
                            ops ->
        row_ops =
          Enum.flat_map(sinks, fn {index, mod} ->
            row =
              :erlang.term_to_binary(%{
                module: mod,
                state: :pending,
                attempts: 0,
                at: now,
                error: nil,
                size: size,
                replay: replay_id
              })

            [
              {:put, :deliveries, Keys.delivery(seq, index), row},
              {:put, :index, Keys.due(now, seq, index), <<size::32>>}
            ]
          end)

        [{:put, :hooks, Keys.hook(seq), bin} | row_ops] ++ ops
      end)

    bytes = Enum.sum(Enum.map(entries, & &1.size))

    ops =
      [{:put, :default, Keys.meta("next_seq"), <<state.next_seq + count::64>>} | ops] ++ extra_ops

    result = commit(state.instance, ops, count, bytes)

    case result do
      :ok ->
        wake_dispatch(state.instance, now)
        {:reply, {:ok, count}, %{state | next_seq: state.next_seq + count, last_at: now}}

      {:error, reason} ->
        Logger.error(
          "[ankusa] store commit of #{count} replayed hook(s) failed, nothing acked: #{inspect(reason)}"
        )

        state = maybe_reopen(state)

        {:reply, {:error, reason}, %{state | next_seq: state.next_seq + count, last_at: now}}
    end
  end

  defp commit_batch(items, state) do
    # Never behind a previous batch, even if the wall clock steps back: dispatch
    # keeps its scan floor just under the stamps it has seen, and relies on
    # stamps never going down.
    now = max(System.system_time(:millisecond), state.last_at)

    case dedupe_check(state.instance, items, now) do
      {:error, reason} ->
        # The dedupe read failed: no seq consumed, nothing written, nothing
        # acked. Callers see the store error, not a guessed commit.
        {:reply, {:error, reason}, state}

      {:ok, %{fresh: []} = checked} ->
        # Every item was a duplicate: write nothing, don't wake dispatch.
        {:reply, {:ok, checked.results}, state}

      {:ok, checked} ->
        {ops, batch_size, bytes} =
          build_ops(checked.fresh, state.next_seq, now, state.archive?)

        ops = checked.dedupe_ops ++ ops

        result = commit(state.instance, ops, batch_size, bytes)

        case result do
          :ok ->
            wake_dispatch(state.instance, now)

            committed =
              checked.fresh
              |> Enum.with_index(state.next_seq)
              |> Enum.map(fn {{env, _bin, _mods, _ttl}, seq} ->
                {:committed, %{env | seq: seq}}
              end)

            results = reassemble(checked.results, committed)

            {:reply, {:ok, results},
             %{state | next_seq: state.next_seq + length(checked.fresh), last_at: now}}

          {:error, reason} ->
            Logger.error(
              "[ankusa] store commit of #{length(items)} hook(s) failed, nothing acked: #{inspect(reason)}"
            )

            state = maybe_reopen(state)

            {:reply, {:error, reason},
             %{state | next_seq: state.next_seq + length(checked.fresh), last_at: now}}
        end
    end
  end

  # ── ingest dedupe ─────────────────────────────────────────────────────────

  # Checks `env.dedupe_key` for every item that also carries a ttl, against the
  # stored `?u` keys and the batch itself. Returns, in original item order,
  # `results` (a `:duplicate` per collapsed item, a `:fresh` marker per item
  # that will commit) plus the fresh items, their dedupe ops, and the count.
  # A stored entry counts as a duplicate only while `expires_at > now`.
  defp dedupe_check(instance, items, now) do
    keyed = Enum.filter(items, fn {env, _bin, _mods, ttl} -> dedupe?(env, ttl) end)

    keys = Enum.map(keyed, fn {env, _bin, _mods, _ttl} -> u_key(env) end)

    case read_stored(instance, keys) do
      {:error, reason} ->
        {:error, reason}

      {:ok, stored_results} ->
        stored =
          keyed
          |> Enum.zip(stored_results)
          |> Map.new(fn {{env, _bin, _mods, _ttl}, result} -> {u_key(env), result} end)

        {_seen, results, fresh, dedupe_ops, fresh_count} =
          Enum.reduce(items, {%{}, [], [], [], 0}, fn item, {seen, results, fresh, ops, n} ->
            {env, _bin, _mods, ttl} = item

            key = if dedupe?(env, ttl), do: u_key(env)

            cond do
              key != nil and Map.has_key?(seen, key) ->
                original_id = Map.fetch!(seen, key)

                {seen, [{:duplicate, %{env | id: original_id, seq: nil}} | results], fresh, ops,
                 n}

              key != nil ->
                case Map.fetch(stored, key) do
                  {:ok, {:ok, <<expires_at::64, original_id::binary>>}} when expires_at > now ->
                    seen = Map.put(seen, key, original_id)

                    {seen, [{:duplicate, %{env | id: original_id, seq: nil}} | results], fresh,
                     ops, n}

                  {:ok, {:ok, <<old_at::64, _original_id::binary>>}} ->
                    seen = Map.put(seen, key, env.id)

                    new_ops = [
                      {:put, :index, key, <<now + ttl::64, env.id::binary>>},
                      {:put, :index, expiry_key(now + ttl, env), <<>>},
                      {:delete, :index, expiry_key(old_at, env)}
                    ]

                    {seen, [{:fresh, item} | results], [item | fresh], new_ops ++ ops, n + 1}

                  {:ok, _absent_or_corrupt} ->
                    # Nothing stored, or a value too short to hold its 8-byte
                    # expiry (the clauses above took every decodable one). Neither
                    # may suppress a hook, and a corrupt value is overwritten here
                    # so the key dedupes again, instead of staying broken for good.
                    seen = Map.put(seen, key, env.id)

                    new_ops = [
                      {:put, :index, key, <<now + ttl::64, env.id::binary>>},
                      {:put, :index, expiry_key(now + ttl, env), <<>>}
                    ]

                    {seen, [{:fresh, item} | results], [item | fresh], new_ops ++ ops, n + 1}
                end

              true ->
                {seen, [{:fresh, item} | results], [item | fresh], ops, n + 1}
            end
          end)

        {:ok,
         %{
           results: Enum.reverse(results),
           fresh: Enum.reverse(fresh),
           dedupe_ops: dedupe_ops,
           fresh_count: fresh_count
         }}
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
      end)

    reassembled
  end

  # ── dedupe expiry sweep ───────────────────────────────────────────────────

  @sweep_max 2_000
  @sweep_backoff_ms 100
  @sweep_interval_ms 10_000

  @impl true
  def handle_info(:sweep_dedupe, state) do
    state = sweep_dedupe(state)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # Deletes expired `?e`/`?u` keys. Runs inside the writer, so it can never
  # race a refresh: a batch either lands before this sweep reads (and is not
  # expired) or after it. `key/2` lookups also treat `expires_at <= now` as
  # absent, so sweep lag is harmless.
  defp sweep_dedupe(state) do
    now = System.system_time(:millisecond)
    range = {<<?e>>, <<?e, now + 1::64>>}

    result =
      case Store.fold(
             state.instance,
             :dedupe_expiry,
             range,
             {0, []},
             fn key, _value, {n, acc} ->
               if n < @sweep_max, do: {:cont, {n + 1, [key | acc]}}, else: {:halt, {n, acc}}
             end
           ) do
        {:ok, {taken, expiry_keys}} ->
          {u_keys, ops} =
            expiry_keys
            |> Enum.reverse()
            |> Enum.reduce({[], []}, fn expiry_key, {u_keys, ops} ->
              case Keys.decode_dedupe_expiry(expiry_key) do
                {at, tenant_id, source_id, key} ->
                  u_key = Keys.dedupe(tenant_id, source_id, key)
                  {[{u_key, at} | u_keys], [{:delete, :index, expiry_key} | ops]}

                :error ->
                  # A key written by a version we no longer understand: drop it
                  # rather than wedge the sweep.
                  {u_keys, [{:delete, :index, expiry_key} | ops]}
              end
            end)

          case Store.multi_get(state.instance, :index, Enum.map(u_keys, &elem(&1, 0))) do
            {:ok, stored} ->
              u_deletes =
                u_keys
                |> Enum.zip(stored)
                |> Enum.filter(fn {{_u_key, at}, result} ->
                  match?({:ok, <<^at::64, _id::binary>>}, result)
                end)
                |> Enum.map(fn {{u_key, _at}, _result} -> {:delete, :index, u_key} end)

              case Store.write(state.instance, u_deletes ++ ops, sync: false) do
                :ok -> {:swept, taken}
                {:error, reason} -> {:error, reason}
              end

            {:error, reason} ->
              {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end

    case result do
      {:swept, taken} ->
        rearm = if taken >= @sweep_max, do: @sweep_backoff_ms, else: @sweep_interval_ms
        Process.send_after(self(), :sweep_dedupe, rearm)

      {:error, reason} ->
        Logger.warning("[ankusa] dedupe sweep failed, retrying: #{inspect(reason)}")
        Process.send_after(self(), :sweep_dedupe, @sweep_interval_ms)
    end

    state
  end

  # ── batch construction ───────────────────────────────────────────────────

  defp build_ops(items, first_seq, now, archive?) do
    items
    |> Enum.with_index(first_seq)
    |> Enum.reduce({[], 0, 0}, fn {{_env, bin, mods, _ttl}, seq}, {ops, count, bytes} ->
      size = byte_size(bin)

      row_ops =
        mods
        |> Enum.with_index()
        |> Enum.flat_map(fn {mod, i} ->
          row =
            :erlang.term_to_binary(%{
              module: mod,
              state: :pending,
              attempts: 0,
              at: now,
              error: nil,
              size: size
            })

          [
            {:put, :deliveries, Keys.delivery(seq, i), row},
            {:put, :index, Keys.due(now, seq, i), <<size::32>>}
          ]
        end)

      archive_ops =
        if archive?, do: [{:put, :index, Keys.archive_pending(seq), <<size::32>>}], else: []

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
  defp commit(instance, ops, batch_size, bytes) do
    Ankusa.Telemetry.span([:commit], %{instance: instance}, fn ->
      case Store.write(instance, ops, sync: true) do
        :ok -> {:ok, %{batch_size: batch_size, bytes: bytes}, %{}}
        {:error, reason} -> throw({:store_commit_failed, reason})
      end
    end)
  catch
    :throw, {:store_commit_failed, reason} -> {:error, reason}
  end

  # After a failed commit the database may be latched in a background error
  # (ENOSPC on the WAL) that survives freeing the space. Reopening clears it.
  # At most once per interval, and a missing store never crashes the Writer.
  @reopen_interval_ms 5_000

  defp maybe_reopen(state) do
    now = System.monotonic_time(:millisecond)

    if state.last_reopen == nil or now - state.last_reopen >= @reopen_interval_ms do
      result =
        try do
          Store.reopen(state.instance)
        catch
          :exit, reason -> {:error, {:store_down, reason}}
        end

      case result do
        :ok -> Logger.warning("[ankusa] store reopened after a failed commit")
        {:error, reason} -> Logger.error("[ankusa] store reopen failed: #{inspect(reason)}")
      end

      %{state | last_reopen: now}
    else
      state
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
