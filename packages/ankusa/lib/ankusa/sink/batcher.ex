defmodule Ankusa.Sink.Batcher do
  @moduledoc """
  The batching worker behind `Ankusa.Sink.SQS` (`SendMessageBatch`, 10 entries)
  and `Ankusa.Sink.GooglePubSub` (`publish`, 1 000 messages): many deliveries
  to one destination, one HTTP request.

  A sink that batches (`c:Ankusa.Sink.batched?/1`) encodes its message exactly
  as it did for a single request, then hands it to `deliver/4`, which is a
  `GenServer.call` to the destination's batcher and returns when *that message's*
  result is known: the batch's response, mapped back to the entry. The caller is
  the dispatch attempt, so a delivery is answered only after its batch's
  response, its row stays claimed until then, and a batcher that dies (or an
  attempt that is killed) loses nothing: the call exits, the attempt fails, the
  row is retried, and the idempotency key travels as it always did.

  ## One batcher per destination

  `Ankusa.Instance` starts a `DynamicSupervisor` for them; the first delivery
  to a destination starts its batcher. A destination is the codec (the sink's
  `Ankusa.Sink.Batcher` implementation) plus a digest of everything that shapes
  the request (`c:batch_identity/1`: queue or topic, endpoint, region,
  credentials, transport options and the `batch_*` tuning), so two sources that
  name different credentials never share a request, and no secret is part of a
  registered name. A batcher with nothing queued or in flight for a minute stops;
  the next delivery starts a new one.

  ## When a batch goes out

  A batch is sent when the queue holds `max_entries` or `max_bytes`, or
  `linger_ms` after the queue became non-empty (or after the last batch left),
  whichever is first. At most `max_inflight` requests are in flight per
  destination: the instance's `dispatch.sink_concurrency` (default 8) counts
  *requests* for a batching sink, not messages (see `Ankusa.Dispatch.Pipeline`).
  Entries are taken round-robin across tenants, so one tenant's backlog is
  interleaved with, not ahead of, another's.

  Entries that share a `group` (a FIFO queue's `MessageGroupId`, a Pub/Sub
  ordering key) stay in the order they were admitted, and a group has at most
  one request in flight: the next entry of a group waits for the previous
  request's response. That is what keeps "the order Ankusa sent them" true; a
  retry after a per-entry failure is, as it always was, a later send.

  ## Bounded (ground rule 3)

  The queue holds at most `queue_max` entries, `queue_max_bytes` bytes, and
  `tenant_max` entries per tenant (defaults 4 096, 128 MiB, 1 024; the instance's
  dispatch windows are the same size, so these are the backstop, not the
  limit). A delivery that does not fit is answered at once with
  `{:error, {:retry_after, ms, {:batch_queue_full, :entries | :bytes | :tenant}}}`
  (`ms` is full jitter) and counted in `ankusa_sink_batch_refused_total`; nothing
  is queued. An entry whose caller died (the attempt timed out) is dropped when
  the queue is next swept, which every flush does; one already in a request
  cannot be recalled.

  ## Telemetry

    * `[:ankusa, :sink_batch, :flush]` — measurements `entries`, `bytes`,
      `requests` (HTTP requests the batch took), `bisections` (the extra
      requests spent isolating a refused message), `duration_ms`; metadata
      `instance`, `sink`.
    * `[:ankusa, :sink_batch, :refused]` — `instance`, `sink`, `reason`.
    * `[:ankusa, :sink_batch, :state]` — gauge: `batchers`, `queued`,
      `queued_bytes`, from `stats/1`.

  ## Writing a codec

  A codec is a module implementing this behaviour; `send_batch/2` runs in a task
  under the batcher's supervisor, never in the batcher itself.
  """

  use GenServer

  alias Ankusa.Telemetry

  @typedoc "What a sink hands `deliver/4`."
  @type entry :: %{
          required(:tenant) => String.t(),
          required(:bytes) => non_neg_integer(),
          required(:group) => String.t() | nil,
          required(:attempt) => pos_integer(),
          required(:item) => term()
        }

  @type config :: %{
          linger_ms: pos_integer(),
          max_entries: pos_integer(),
          max_bytes: pos_integer(),
          queue_max: pos_integer(),
          queue_max_bytes: pos_integer(),
          tenant_max: pos_integer(),
          max_inflight: pos_integer(),
          task_timeout_ms: pos_integer()
        }

  @type result :: :ok | {:error, term()}

  @doc "The sink module this codec batches for; the `sink` label of its telemetry."
  @callback sink() :: module()

  @doc """
  Everything in `opts` that shapes a request: two deliveries with equal identities
  may share one. Include credentials and transport options; the batcher only
  ever uses a digest of it.
  """
  @callback batch_identity(opts :: keyword()) :: term()

  @doc "The batching limits for `opts` (`config/3` reads the `batch_*` options)."
  @callback batch_config(opts :: keyword(), instance :: atom()) :: config()

  @doc """
  Send `items` (one request, or several when a refused message has to be isolated)
  and answer each, in order. The second element counts `requests` and `bisections`.
  """
  @callback send_batch(items :: [term()], opts :: keyword()) ::
              {[result()], %{requests: non_neg_integer(), bisections: non_neg_integer()}}

  @default_queue_max 4_096
  @default_queue_max_bytes 134_217_728
  @default_tenant_max 1_024
  @default_linger_ms 20
  @default_max_inflight 8
  @idle_ms 60_000
  # A batch scans at most this many times `max_entries` queued entries of one
  # tenant looking for one whose group is free.
  @scan_factor 4

  # ── API ───────────────────────────────────────────────────────────────────

  @doc """
  The `batch_*` options of a sink's `opts`, range-checked, over the codec's
  defaults. `entries` and `bytes` are `{default, maximum}`.

    * `:batch_linger_ms` (default 20), `:batch_max_entries`, `:batch_max_bytes`
    * `:batch_queue_max` (4 096), `:batch_queue_max_bytes` (128 MiB),
      `:batch_queue_tenant_max` (1 024)
    * `:batch_max_inflight` — default the instance's `dispatch.sink_concurrency`, or 8

  Raises `ArgumentError` naming the option.
  """
  @spec config(keyword(), atom(), keyword()) :: config()
  def config(opts, instance, limits) do
    {entries, entries_max} = Keyword.fetch!(limits, :entries)
    {bytes, bytes_max} = Keyword.fetch!(limits, :bytes)
    timeout = Keyword.fetch!(limits, :timeout_ms)
    bisect = Keyword.get(limits, :extra_requests, 0)

    %{
      linger_ms: option!(opts, :batch_linger_ms, @default_linger_ms, 1, 60_000),
      max_entries: option!(opts, :batch_max_entries, entries, 1, entries_max),
      max_bytes: option!(opts, :batch_max_bytes, bytes, 1, bytes_max),
      queue_max: option!(opts, :batch_queue_max, @default_queue_max, 1, 10_000_000),
      queue_max_bytes:
        option!(opts, :batch_queue_max_bytes, @default_queue_max_bytes, 1, 1_000_000_000_000),
      tenant_max: option!(opts, :batch_queue_tenant_max, @default_tenant_max, 1, 10_000_000),
      max_inflight: option!(opts, :batch_max_inflight, default_inflight(instance), 1, 1_024),
      # one request, plus the requests spent isolating a refused message, each
      # bounded by the sink's own timeout
      task_timeout_ms: timeout * (2 + bisect)
    }
  end

  defp default_inflight(nil), do: @default_max_inflight

  defp default_inflight(instance) do
    case Ankusa.config(instance).dispatch do
      %{sink_concurrency: n} when is_integer(n) and n >= 1 -> n
      _ -> @default_max_inflight
    end
  rescue
    _ -> @default_max_inflight
  end

  @doc """
  Raise `ArgumentError` naming the option unless `opts`' `:batch` is a boolean and
  its `batch_*` options are in range for `codec`. What `Ankusa.Sink.SQS.validate_opts!/1`
  and `Ankusa.Sink.GooglePubSub.validate_opts!/1` run, so a bad value fails at
  load; a delivery that reaches a batcher with one is dead-lettered with the same
  message.
  """
  @spec validate_opts!(module(), keyword()) :: :ok
  def validate_opts!(codec, opts) do
    case Keyword.get(opts, :batch, true) do
      batch when is_boolean(batch) ->
        :ok

      other ->
        raise ArgumentError, ":batch must be true or false, got #{inspect(other)}"
    end

    codec.batch_config(opts, nil)
    :ok
  end

  defp option!(opts, key, default, min, max) do
    case Keyword.get(opts, key, default) do
      n when is_integer(n) and n >= min and n <= max ->
        n

      other ->
        raise ArgumentError,
              "#{inspect(key)} must be an integer from #{min} to #{max}, got #{inspect(other)}"
    end
  end

  @doc """
  Is `instance`'s batcher supervisor running? When it is not (a sink called
  outside an instance), a batching sink sends each message in a request of its
  own.
  """
  @spec available?(term()) :: boolean()
  def available?(instance) when is_atom(instance) and instance != nil,
    do: Ankusa.whereis(instance, :sink_batchers) != nil

  def available?(_instance), do: false

  @doc """
  Queue `entry` for `codec`'s batcher of the destination `opts` names and wait for
  its result.
  """
  @spec deliver(module(), atom(), keyword(), entry()) :: result()
  def deliver(codec, instance, opts, entry) do
    # An option out of range is the operator's to fix, not a failure of the
    # destination: dead-lettered at once, like a refused credential.
    config =
      try do
        {:ok, codec.batch_config(opts, instance)}
      rescue
        error in ArgumentError ->
          {:error, {:permanent, {:config, {:batch_options, Exception.message(error)}}}}
      end

    with {:ok, config} <- config do
      key = {:sink_batcher, codec, digest(codec.batch_identity(opts), config)}
      message = {:enqueue, Map.put(entry, :callers, [self() | Process.get(:"$callers", [])])}

      call(instance, codec, opts, config, key, message, entry.attempt, 1)
    end
  end

  defp call(instance, codec, opts, config, key, message, attempt, retries) do
    with {:ok, pid} <- ensure(instance, codec, opts, config, key) do
      case GenServer.call(pid, message, :infinity) do
        {:refused, reason} ->
          {:error,
           {:retry_after, Ankusa.Sink.throttle_after_ms(attempt), {:batch_queue_full, reason}}}

        result ->
          result
      end
    end
  catch
    # The batcher stopped (idle, or killed) between the lookup and the call.
    :exit, {reason, {GenServer, :call, _}} when reason in [:noproc, :normal] and retries > 0 ->
      call(instance, codec, opts, config, key, message, attempt, retries - 1)

    :exit, {reason, {GenServer, :call, _}} ->
      {:error, {:batcher_down, reason}}
  end

  # A digest, so no credential is part of a registered name or a crash report.
  defp digest(identity, config) do
    :crypto.hash(:sha256, :erlang.term_to_binary({identity, config}))
    |> binary_part(0, 8)
    |> Base.encode16(case: :lower)
  end

  defp ensure(instance, codec, opts, config, key) do
    case Ankusa.whereis(instance, key) do
      pid when is_pid(pid) -> {:ok, pid}
      nil -> start(instance, codec, opts, config, key)
    end
  end

  # The settings carry credentials: the child spec and the batcher's state hold
  # them only inside closures, which `inspect` prints without their captured
  # values.
  defp start(instance, codec, opts, config, key) do
    boot = fn -> {instance, codec, fn -> opts end, config, key} end

    child = %{
      id: key,
      start: {__MODULE__, :start_link, [boot]},
      restart: :temporary
    }

    case DynamicSupervisor.start_child(Ankusa.via(instance, :sink_batchers), child) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, reason} -> {:error, {:batcher_start, reason}}
    end
  catch
    :exit, reason -> {:error, {:batcher_start, reason}}
  end

  @doc """
  What `instance`'s batchers hold right now: `%{batchers, queued, queued_bytes}`,
  summed from each batcher's registered counters (no scan of a queue, no call).
  """
  @spec stats(atom()) :: %{
          batchers: non_neg_integer(),
          queued: non_neg_integer(),
          queued_bytes: non_neg_integer()
        }
  def stats(instance) do
    spec = [{{{instance, {:sink_batcher, :_, :_}}, :_, :"$1"}, [], [:"$1"]}]

    Enum.reduce(
      Registry.select(Ankusa.Registry, spec),
      %{batchers: 0, queued: 0, queued_bytes: 0},
      fn {queued, bytes}, acc ->
        %{
          batchers: acc.batchers + 1,
          queued: acc.queued + queued,
          queued_bytes: acc.queued_bytes + bytes
        }
      end
    )
  end

  # ── supervisor ────────────────────────────────────────────────────────────

  defmodule Workers do
    @moduledoc false
    # One per instance: the batchers and the tasks that send their requests.
    use Supervisor

    def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      instance = Keyword.fetch!(opts, :instance)

      children = [
        {DynamicSupervisor, strategy: :one_for_one, name: Ankusa.via(instance, :sink_batchers)},
        {Task.Supervisor, name: Ankusa.via(instance, :sink_batch_tasks)}
      ]

      Supervisor.init(children, strategy: :one_for_all)
    end
  end

  # ── GenServer ─────────────────────────────────────────────────────────────

  def start_link(boot) when is_function(boot, 0) do
    {instance, _codec, _opts, _config, key} = boot.()

    GenServer.start_link(__MODULE__, boot,
      name: {:via, Registry, {Ankusa.Registry, {instance, key}, {0, 0}}}
    )
  end

  @impl true
  def init(boot) do
    {instance, codec, opts, config, key} = boot.()
    Process.send_after(self(), :idle_check, @idle_ms)

    {:ok,
     %{
       instance: instance,
       codec: codec,
       sink: codec.sink(),
       opts: opts,
       config: config,
       key: key,
       # tenant => :queue of entries; every tenant here is in `ring` once
       queues: %{},
       ring: :queue.new(),
       counts: %{},
       queued: 0,
       queued_bytes: 0,
       since: nil,
       timer: nil,
       # task ref => %{task, timer, froms, groups, entries, bytes, started}
       inflight: %{},
       inflight_groups: MapSet.new(),
       last_active: now()
     }}
  end

  @impl true
  def handle_call({:enqueue, entry}, {pid, _tag} = from, state) do
    case admit(state, entry) do
      :ok ->
        state =
          state
          |> push(Map.merge(entry, %{from: from, pid: pid}))
          |> flush(false)
          |> arm()
          |> sync()

        {:noreply, state}

      {:refused, reason} ->
        Telemetry.emit([:sink_batch, :refused], %{}, %{
          instance: state.instance,
          sink: state.sink,
          reason: reason
        })

        {:reply, {:refused, reason}, state}
    end
  end

  @impl true
  def handle_info({:linger, ref}, %{timer: ref} = state) do
    {:noreply, %{state | timer: nil} |> flush(false) |> arm() |> sync()}
  end

  def handle_info({:linger, _stale}, state), do: {:noreply, state}

  # The task finished: it has answered every caller itself.
  def handle_info({ref, {:done, stats}}, %{inflight: inflight} = state)
      when is_map_key(inflight, ref) do
    Process.demonitor(ref, [:flush])
    {batch, state} = finish(state, ref)

    Telemetry.emit(
      [:sink_batch, :flush],
      %{
        entries: batch.entries,
        bytes: batch.bytes,
        requests: stats.requests,
        bisections: stats.bisections,
        duration_ms: now() - batch.started
      },
      %{instance: state.instance, sink: state.sink}
    )

    {:noreply, state |> flush(false) |> arm() |> sync()}
  end

  # The task crashed: every caller of the batch gets an error, and retries.
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{inflight: inflight} = state)
      when is_map_key(inflight, ref) do
    {batch, state} = finish(state, ref)
    reply_all(batch.froms, {:error, {:batch_crashed, reason}})
    {:noreply, state |> flush(false) |> arm() |> sync()}
  end

  def handle_info({:batch_timeout, ref}, %{inflight: inflight} = state)
      when is_map_key(inflight, ref) do
    %{task: task} = Map.fetch!(inflight, ref)
    Process.demonitor(ref, [:flush])
    Task.Supervisor.terminate_child(Ankusa.via(state.instance, :sink_batch_tasks), task.pid)
    {batch, state} = finish(state, ref)

    reply_all(
      batch.froms,
      {:error, {:batch_timeout, state.config.task_timeout_ms}}
    )

    {:noreply, state |> flush(false) |> arm() |> sync()}
  end

  def handle_info(:idle_check, state) do
    if state.queued == 0 and map_size(state.inflight) == 0 and
         now() - state.last_active >= @idle_ms do
      {:stop, :normal, state}
    else
      Process.send_after(self(), :idle_check, @idle_ms)
      {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  # ── admission ─────────────────────────────────────────────────────────────

  defp admit(state, %{tenant: tenant, bytes: bytes}) do
    config = state.config

    cond do
      state.queued >= config.queue_max -> {:refused, :entries}
      state.queued_bytes + bytes > config.queue_max_bytes -> {:refused, :bytes}
      Map.get(state.counts, tenant, 0) >= config.tenant_max -> {:refused, :tenant}
      true -> :ok
    end
  end

  defp push(state, %{tenant: tenant, bytes: bytes} = entry) do
    {queues, ring} =
      case state.queues do
        %{^tenant => queue} ->
          {Map.put(state.queues, tenant, :queue.in(entry, queue)), state.ring}

        queues ->
          {Map.put(queues, tenant, :queue.from_list([entry])), :queue.in(tenant, state.ring)}
      end

    %{
      state
      | queues: queues,
        ring: ring,
        counts: Map.update(state.counts, tenant, 1, &(&1 + 1)),
        queued: state.queued + 1,
        queued_bytes: state.queued_bytes + bytes,
        since: state.since || now(),
        last_active: now()
    }
  end

  # ── flushing ──────────────────────────────────────────────────────────────

  defp flush(state, force?) do
    cond do
      state.queued == 0 ->
        %{state | since: nil}

      map_size(state.inflight) >= state.config.max_inflight ->
        state

      force? or full?(state) or expired?(state) ->
        case take_batch(state) do
          {[], state} ->
            state

          {batch, state} ->
            %{launch(state, Enum.reverse(batch)) | since: now()} |> flush(force?)
        end

      true ->
        state
    end
  end

  defp full?(%{queued: queued, queued_bytes: bytes, config: config}),
    do: queued >= config.max_entries or bytes >= config.max_bytes

  defp expired?(%{since: nil}), do: false
  defp expired?(%{since: since, config: config}), do: now() - since >= config.linger_ms

  # The wake-up for a queue that is not full: `linger_ms` after it became
  # non-empty, or after the last batch left. Nothing to arm while every slot is
  # busy (a finished request flushes), and never two timers.
  defp arm(%{queued: 0} = state), do: state
  defp arm(%{timer: timer} = state) when timer != nil, do: state

  defp arm(state) do
    if map_size(state.inflight) >= state.config.max_inflight do
      state
    else
      ref = make_ref()
      wait = max(1, (state.since || now()) + state.config.linger_ms - now())
      Process.send_after(self(), {:linger, ref}, wait)
      %{state | timer: ref}
    end
  end

  # Entries are taken round-robin across tenants, in admission order within one,
  # skipping those whose group already has a request in flight.
  defp take_batch(state), do: take(state, [], 0, 0, map_size(state.queues))

  defp take(state, acc, n, bytes, idle) do
    config = state.config

    cond do
      :queue.is_empty(state.ring) or n >= config.max_entries or bytes >= config.max_bytes or
          idle <= 0 ->
        {acc, state}

      true ->
        {{:value, tenant}, ring} = :queue.out(state.ring)
        state = %{state | ring: ring}

        case take_one(state, tenant, acc != [], bytes) do
          {:ok, entry, state} ->
            state = requeue(state, tenant)
            take(state, [entry | acc], n + 1, bytes + entry.bytes, map_size(state.queues))

          {:full, state} ->
            {acc, requeue(state, tenant)}

          {:none, state} ->
            take(requeue(state, tenant), acc, n, bytes, idle - 1)
        end
    end
  end

  defp requeue(state, tenant) do
    if is_map_key(state.queues, tenant),
      do: %{state | ring: :queue.in(tenant, state.ring)},
      else: state
  end

  defp take_one(state, tenant, started?, bytes) do
    queue = Map.fetch!(state.queues, tenant)
    scan(state, tenant, queue, [], started?, bytes, @scan_factor * state.config.max_entries)
  end

  defp scan(state, tenant, queue, held, started?, bytes, budget) do
    case :queue.out(queue) do
      {:empty, _queue} ->
        {:none, put_queue(state, tenant, restore(held, queue))}

      {{:value, entry}, rest} ->
        cond do
          # its caller is gone (the attempt timed out): nobody is waiting
          not Process.alive?(entry.pid) ->
            scan(drop(state, entry), tenant, rest, held, started?, bytes, budget)

          budget <= 0 ->
            {:none, put_queue(state, tenant, restore(held, queue))}

          entry.group != nil and MapSet.member?(state.inflight_groups, entry.group) ->
            scan(state, tenant, rest, [entry | held], started?, bytes, budget - 1)

          started? and bytes + entry.bytes > state.config.max_bytes ->
            {:full, put_queue(state, tenant, restore(held, queue))}

          true ->
            {:ok, entry, drop(put_queue(state, tenant, restore(held, rest)), entry)}
        end
    end
  end

  # `held` is newest first; they go back ahead of what was not scanned.
  defp restore([], queue), do: queue
  defp restore(held, queue), do: :queue.join(:queue.from_list(Enum.reverse(held)), queue)

  defp put_queue(state, tenant, queue) do
    if :queue.is_empty(queue),
      do: %{state | queues: Map.delete(state.queues, tenant)},
      else: %{state | queues: Map.put(state.queues, tenant, queue)}
  end

  # Takes an entry's counts out of the queue's.
  defp drop(state, %{tenant: tenant, bytes: bytes}) do
    counts =
      case Map.fetch!(state.counts, tenant) do
        1 -> Map.delete(state.counts, tenant)
        n -> Map.put(state.counts, tenant, n - 1)
      end

    %{state | counts: counts, queued: state.queued - 1, queued_bytes: state.queued_bytes - bytes}
  end

  defp launch(state, entries) do
    %{instance: instance, codec: codec, opts: opts} = state
    items = Enum.map(entries, & &1.item)
    froms = Enum.map(entries, & &1.from)
    groups = entries |> Enum.map(& &1.group) |> Enum.reject(&is_nil/1) |> MapSet.new()
    callers = hd(entries).callers

    task =
      Task.Supervisor.async_nolink(Ankusa.via(instance, :sink_batch_tasks), fn ->
        # the first caller's ancestry, so a test double owned by it applies
        Process.put(:"$callers", callers)
        {results, stats} = send_batch(codec, items, opts.())

        froms
        |> Enum.zip(results)
        |> Enum.each(fn {from, result} -> GenServer.reply(from, result) end)

        {:done, stats}
      end)

    timer = Process.send_after(self(), {:batch_timeout, task.ref}, state.config.task_timeout_ms)

    batch = %{
      task: task,
      timer: timer,
      froms: froms,
      groups: groups,
      entries: length(entries),
      bytes: Enum.reduce(entries, 0, &(&1.bytes + &2)),
      started: now()
    }

    %{
      state
      | inflight: Map.put(state.inflight, task.ref, batch),
        inflight_groups: MapSet.union(state.inflight_groups, groups)
    }
  end

  # Runs in the task: a codec that raises or answers wrongly fails the batch's
  # entries, never the batcher.
  defp send_batch(codec, items, opts) do
    case codec.send_batch(items, opts) do
      {results, %{requests: _, bisections: _} = stats}
      when is_list(results) and length(results) == length(items) ->
        {results, stats}

      other ->
        failure(items, {:bad_batch_results, other})
    end
  rescue
    error -> failure(items, {:raised, error})
  catch
    :exit, reason -> failure(items, {:exit, reason})
    :throw, value -> failure(items, {:throw, value})
  end

  defp failure(items, reason),
    do: {Enum.map(items, fn _ -> {:error, reason} end), %{requests: 0, bisections: 0}}

  defp finish(state, ref) do
    {batch, inflight} = Map.pop!(state.inflight, ref)
    Process.cancel_timer(batch.timer)

    # Another batch of a group can still be in flight only if it was started
    # after this one left the set, which `take/5` prevents: a group has one.
    state = %{
      state
      | inflight: inflight,
        inflight_groups: MapSet.difference(state.inflight_groups, batch.groups)
    }

    {batch, state}
  end

  defp reply_all(froms, reply), do: Enum.each(froms, &GenServer.reply(&1, reply))

  # The registered counters `stats/1` reads.
  defp sync(state) do
    Registry.update_value(Ankusa.Registry, {state.instance, state.key}, fn _ ->
      {state.queued, state.queued_bytes}
    end)

    state
  end

  defp now, do: System.monotonic_time(:millisecond)
end
