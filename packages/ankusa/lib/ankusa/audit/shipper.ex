defmodule Ankusa.Audit.Shipper do
  @moduledoc """
  Ships the audit log (`Ankusa.Audit`) to `audit.sinks` and prunes it. It runs
  whenever `audit.enabled` is true, with no sinks too, because it also prunes.

  ## Shipping

  Each sink has a cursor, the key of the last entry it received, kept in the
  store (`m:audit_cursor:<sink index>`) so a restart resumes. Every second a
  sink that is not already shipping reads up to 100 entries after its cursor,
  oldest first, and delivers them one by one as CloudEvents
  (`io.ankusa.audit.<action>`, `id` = the entry id; same envelope shape as
  `Ankusa.Lifecycle`), advancing the cursor, synced, after each delivery. A
  sink with more waiting goes again at once.

  Delivery is **at least once and in order**: an entry is never skipped. A
  failed delivery stops that sink, is counted
  (`[:ankusa, :audit, :ship_failed]`), and is retried with `dispatch.retry`'s
  backoff; once the policy gives up, every 60 s, forever. One sink's trouble
  never holds another's back (each ships in its own task). A crash between a
  delivery and its cursor write re-delivers that entry, which is why the event
  `id` is the entry's: consumers drop duplicates.

  A `pending` entry younger than 60 s (a mutation still running) stops the batch
  so it ships once with its outcome; an older one ships as it is, so a crashed
  node's last requests are reported as `pending`.

  The cursor stores the sink's module with it. A sink index whose cursor is
  missing, or was written by a different module (sinks were reordered or
  replaced), starts from the oldest retained entry and says so in the log:
  re-shipping is safe, skipping is not.

  ## Pruning

  When `audit.retention_days` is set, once at start and then hourly, entries
  older than that are deleted, but never past the slowest sink's cursor: an
  entry some sink has not received outlives retention. A sink with no cursor yet
  holds back all pruning.
  """

  use GenServer

  require Logger

  alias Ankusa.{Audit, Envelope, Sink, Store, Telemetry}
  alias Ankusa.Store.Keys

  @tick_ms 1_000
  @batch 100
  @pending_grace_ms 60_000
  @gave_up_retry_ms 60_000
  @prune_ms 3_600_000
  @day_ms 86_400_000
  @content_type "application/cloudevents+json"

  @doc """
  Options: `:instance` (required). `:tick_ms`, `:pending_grace_ms` and
  `:prune_ms` exist for tests.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :audit_shipper))
  end

  @doc "Run the retention sweep now. `:ok`, or `{:error, reason}` if the store refused it."
  @spec prune(atom()) :: :ok | {:error, term()}
  def prune(instance), do: GenServer.call(Ankusa.via(instance, :audit_shipper), :prune, 30_000)

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Ankusa.config(instance)
    {:ok, sup} = Task.Supervisor.start_link()

    sinks =
      config.audit.sinks
      |> Enum.with_index()
      |> Enum.map(fn {{mod, sink_opts}, index} ->
        %{
          index: index,
          mod: mod,
          opts: sink_opts,
          cursor: :unloaded,
          attempt: 0,
          # monotonic ms before which the sink waits (a backoff), or nil
          not_before: nil,
          task: nil
        }
      end)

    state = %{
      instance: instance,
      sup: sup,
      sinks: sinks,
      retry: config.dispatch.retry,
      attempt_timeout_ms: config.dispatch.attempt_timeout_ms,
      retention_days: config.audit.retention_days,
      tick_ms: Keyword.get(opts, :tick_ms, @tick_ms),
      pending_grace_ms: Keyword.get(opts, :pending_grace_ms, @pending_grace_ms),
      prune_ms: Keyword.get(opts, :prune_ms, @prune_ms),
      tick_ref: nil
    }

    send(self(), :tick)
    send(self(), :prune)
    {:ok, state}
  end

  # Crash reports print the state: the sinks carry credentials.
  @impl true
  def format_status(%{state: %{sinks: sinks} = state} = status) do
    %{status | state: %{state | sinks: Enum.map(sinks, &{&1.index, &1.mod, &1.cursor})}}
  end

  def format_status(status), do: status

  @impl true
  def handle_call(:prune, _from, state), do: {:reply, do_prune(state), state}

  @impl true
  def handle_info(:tick, state) do
    # A `:tick` sent by hand (a test, an operator) replaces the pending one.
    if state.tick_ref, do: Process.cancel_timer(state.tick_ref)
    tick_ref = Process.send_after(self(), :tick, state.tick_ms)
    state = %{state | tick_ref: tick_ref}
    {:noreply, Enum.reduce(state.sinks, state, fn sink, acc -> start_sink(acc, sink.index) end)}
  end

  def handle_info(:prune, state) do
    Process.send_after(self(), :prune, state.prune_ms)
    do_prune(state)
    {:noreply, state}
  end

  def handle_info({:ship, index}, state), do: {:noreply, start_sink(state, index)}

  def handle_info({ref, result}, state) when is_reference(ref) do
    case sink_by_ref(state, ref) do
      nil ->
        {:noreply, state}

      sink ->
        Process.demonitor(ref, [:flush])
        {:noreply, finished(state, sink, result)}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case sink_by_ref(state, ref) do
      nil ->
        {:noreply, state}

      sink ->
        # The task may have written a cursor before it died: forget ours.
        {:noreply,
         finished(state, %{sink | cursor: :unloaded}, {:error, :unchanged, {:exit, reason}})}
    end
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp sink_by_ref(state, ref), do: Enum.find(state.sinks, &(&1.task && &1.task.ref == ref))

  # ── per-sink shipping ──────────────────────────────────────────────────────

  defp start_sink(state, index) do
    sink = Enum.at(state.sinks, index)
    now = System.monotonic_time(:millisecond)

    if sink.task == nil and (sink.not_before == nil or now >= sink.not_before) do
      case load_cursor(state.instance, sink) do
        {:ok, cursor} ->
          sink = %{sink | cursor: cursor}
          task = Task.Supervisor.async_nolink(state.sup, fn -> ship(state, sink) end)
          put_sink(state, %{sink | task: task})

        {:error, _reason} ->
          state
      end
    else
      state
    end
  end

  defp put_sink(state, sink),
    do: %{state | sinks: List.replace_at(state.sinks, sink.index, sink)}

  # A batch ended: `{:ok, cursor, shipped, more?}` or `{:error, cursor, reason}`
  # (`:unchanged` when nothing was delivered or the cursor is unknown).
  defp finished(state, sink, {:ok, cursor, shipped, more?}) do
    if more?, do: send(self(), {:ship, sink.index})
    attempt = if shipped > 0, do: 0, else: sink.attempt
    put_sink(state, %{sink | cursor: cursor, task: nil, attempt: attempt, not_before: nil})
  end

  defp finished(state, sink, {:error, cursor, reason}) do
    cursor = if cursor == :unchanged, do: sink.cursor, else: cursor
    attempt = sink.attempt + 1

    Telemetry.emit([:audit, :ship_failed], %{}, %{
      instance: state.instance,
      sink: inspect(sink.mod),
      reason: reason
    })

    Logger.warning(
      "[ankusa] audit sink #{sink.index} (#{inspect(sink.mod)}) failed on attempt #{attempt}: " <>
        inspect(reason)
    )

    {retry_mod, retry_opts} = state.retry

    delay =
      case retry_mod.backoff(attempt, retry_opts) do
        {:retry, delay} -> delay
        :give_up -> @gave_up_retry_ms
      end

    # The retry fires when the backoff is up, not at the next tick.
    not_before = System.monotonic_time(:millisecond) + delay
    Process.send_after(self(), {:ship, sink.index}, delay)

    put_sink(state, %{sink | cursor: cursor, task: nil, attempt: attempt, not_before: not_before})
  end

  # Runs in a task. Reads one batch after the cursor and delivers it in order,
  # stopping at the first failure.
  defp ship(state, sink) do
    now = System.system_time(:millisecond)

    case read_batch(state, sink.cursor, now) do
      {:ok, rows} ->
        deliver_rows(state, sink, rows)

      {:error, reason} ->
        {:error, :unchanged, {:store, reason}}
    end
  end

  defp read_batch(state, cursor, now) do
    %{lo: lo, hi: hi} = Keys.family(:audit)
    lower = if cursor, do: <<cursor::binary, 0>>, else: lo

    Store.fold(state.instance, :audit, {lower, hi}, [], fn key, value, rows ->
      cond do
        length(rows) >= @batch -> {:halt, rows}
        young_pending?(value, now, state.pending_grace_ms) -> {:halt, rows}
        true -> {:cont, [{key, value} | rows]}
      end
    end)
    |> case do
      {:ok, rows} -> {:ok, Enum.reverse(rows)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp young_pending?(value, now, grace) do
    case Audit.decode(value) do
      {:ok, %{"outcome" => "pending", "at" => at}} when is_integer(at) -> now - at < grace
      _ -> false
    end
  end

  defp deliver_rows(state, sink, rows) do
    result =
      Enum.reduce_while(rows, {sink.cursor, 0}, fn {key, value}, {cursor, shipped} ->
        case deliver_row(state, sink, key, value) do
          :ok -> {:cont, {key, shipped + 1}}
          {:error, reason} -> {:halt, {:error, cursor, shipped, reason}}
        end
      end)

    case result do
      {:error, cursor, _shipped, reason} -> {:error, cursor || :unchanged, reason}
      {cursor, shipped} -> {:ok, cursor, shipped, length(rows) >= @batch}
    end
  end

  defp deliver_row(state, sink, key, value) do
    with :ok <- deliver(state, sink, value),
         :ok <- save_cursor(state.instance, sink, key) do
      Telemetry.emit([:audit, :shipped], %{}, %{
        instance: state.instance,
        sink: inspect(sink.mod)
      })

      :ok
    end
  end

  # An entry that does not decode is corrupt and can never ship: the cursor
  # moves past it.
  defp deliver(state, sink, value) do
    case Audit.decode(value) do
      {:ok, entry} ->
        env = envelope(state.instance, entry)

        ctx = %{
          instance: state.instance,
          source_id: Audit.source_id(),
          tenant_id: env.tenant_id,
          attempt: sink.attempt + 1
        }

        deliver_bounded(sink, env, ctx, state.attempt_timeout_ms)

      :error ->
        Logger.error("[ankusa] audit entry that does not decode skipped by sink #{sink.index}")
        :ok
    end
  end

  defp deliver_bounded(sink, env, ctx, timeout) do
    task = Task.async(fn -> Sink.safe_deliver(sink.mod, env, ctx, sink.opts) end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      {:exit, reason} -> {:error, {:exit, reason}}
      nil -> {:error, {:attempt_timeout, timeout}}
    end
  end

  defp envelope(instance, entry) do
    body =
      JSON.encode!(%{
        "specversion" => "1.0",
        "id" => entry["id"],
        "source" => "urn:ankusa:instance:#{instance}",
        "type" => "io.ankusa.audit.#{entry["action"]}",
        "subject" => entry["target"] || entry["path"],
        "time" => entry["time"],
        "datacontenttype" => "application/json",
        "data" => entry
      })

    %Envelope{
      id: entry["id"],
      source_id: Audit.source_id(),
      tenant_id: entry["tenant"] || "default",
      received_at: entry["at"],
      method: "POST",
      path: "/_ankusa/audit",
      headers: [{"content-type", @content_type}],
      content_type: @content_type,
      body: body,
      size: byte_size(body)
    }
  end

  # ── cursors ────────────────────────────────────────────────────────────────

  # `{:ok, key | nil}`: nil starts from the oldest retained entry.
  defp load_cursor(_instance, %{cursor: cursor}) when cursor != :unloaded, do: {:ok, cursor}

  defp load_cursor(instance, sink) do
    case read_cursor(instance, sink) do
      {:ok, key} ->
        {:ok, key}

      :none ->
        Logger.info(
          "[ankusa] audit sink #{sink.index} (#{inspect(sink.mod)}) has no cursor; " <>
            "shipping from the oldest retained entry"
        )

        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # The stored cursor only counts if the same module is still at that index.
  defp read_cursor(instance, sink) do
    case Store.get(instance, :default, Keys.meta("audit_cursor:#{sink.index}")) do
      {:ok, json} ->
        module = inspect(sink.mod)

        with {:ok, %{"module" => ^module, "key" => encoded}} <- JSON.decode(json),
             {:ok, key} <- Base.url_decode64(encoded, padding: false) do
          {:ok, key}
        else
          _ -> :none
        end

      :not_found ->
        :none

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp save_cursor(instance, sink, key) do
    value =
      JSON.encode!(%{
        "module" => inspect(sink.mod),
        "key" => Base.url_encode64(key, padding: false)
      })

    Store.write(instance, [{:put, :default, Keys.meta("audit_cursor:#{sink.index}"), value}],
      sync: true
    )
  end

  # ── pruning ────────────────────────────────────────────────────────────────

  defp do_prune(%{retention_days: nil}), do: :ok

  defp do_prune(state) do
    cutoff_at = System.system_time(:millisecond) - state.retention_days * @day_ms
    cutoff = <<"a:", max(cutoff_at, 0)::64>>

    with {:ok, bounds} <- sink_bounds(state) do
      upper = Enum.min([cutoff | bounds])
      lower = <<"a:", 0::64>>

      if upper > lower do
        Store.write(state.instance, [{:delete_range, :default, lower, upper}], sync: true)
      else
        :ok
      end
    end
  end

  # The first key each sink has not yet received. A sink with no cursor on disk
  # has received nothing we can vouch for: nothing is pruned.
  defp sink_bounds(state) do
    Enum.reduce_while(state.sinks, {:ok, []}, fn sink, {:ok, bounds} ->
      case read_cursor(state.instance, sink) do
        {:ok, key} -> {:cont, {:ok, [<<key::binary, 0>> | bounds]}}
        :none -> {:halt, {:ok, [<<"a:", 0::64>>]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end
end
