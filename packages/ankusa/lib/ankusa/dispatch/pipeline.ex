defmodule Ankusa.Dispatch.Pipeline do
  @moduledoc """
  Async dispatch pipeline: a scheduler over *delivery rows*. Every committed
  hook has one row per sink of its source (`Ankusa.Queue`); this process claims
  the rows that are due, runs each delivery in a `Task.Supervisor` task, and
  records the outcome as a single store batch. At-least-once: a row is only
  deleted after its sink answered `:ok`, and a restart makes every claimed row
  due again.

  ## What wakes it

  A commit sends it `:wake`, so a new hook is claimed at once, not at the next
  poll. Otherwise it sleeps until the earliest due row: a retry is a row due at
  `now + backoff`, so waiting costs no slot and no process — a sink that is
  down for an hour holds up nothing but its own rows.

  ## Concurrency

  Up to `dispatch.concurrency` deliveries run at a time. A window
  (`dispatch.max_inflight` rows, `dispatch.max_inflight_bytes` of stored hook)
  bounds claimed-but-unfinished work, so a dead sink cannot walk the pipeline
  into an OOM. Deliveries are not ordered unless a sink entry opts in: two hooks
  for one sink may run in either order, and a retry runs after whatever is due
  before it.
  An attempt that has not returned after `dispatch.attempt_timeout_ms` (default
  30 s) is killed and counts as a failed attempt, `{:attempt_timeout, ms}`, so a
  hung sink frees its slot; the fallback claim check-in runs inside the same
  deadline.

  ## Ordering lanes

  A sink entry with `ordered: true` (`Ankusa.Ordering`, ADR 0004) delivers the
  hooks of one lane (`{tenant, source, sink, key}`) one at a time, in the order
  they were committed on this node (`seq`). Only a lane's *head* is a due row or
  a claim; the hooks behind it are *members* (`Deliveries.Lanes`), in the store
  and nowhere else: a waiting lane holds no slot, no window room and no process.
  A retry or a breaker park rewrites the head's due row and blocks its lane
  alone. Every terminal transition of a head (delivered, dead-lettered,
  `{:permanent, _}`, `:sink_gone`, an orphaned row) releases its lane in the
  batch that writes it: the next member becomes the head, so a poison hook never
  holds its key forever. Once per visit of a tenant, before the due fold, a sweep
  promotes members that have no head (a writer committed one into an idle lane)
  and heals a head whose row is gone; it runs only while the node holds a member
  at all (`lane_members`), so a deployment with no ordered sink pays one counter
  read per scan. Only this process moves lane state, except the queue writer's
  blind puts of a new member and its marker.

  ## Per-tenant fairness

  A scan claims from the tenants that have work due
  (`Deliveries.active_tenants/3`) by deficit round-robin: the page of
  `dispatch.batch` rows is split into equal shares, a tenant with less due than
  its share leaves the rest to the others (further passes), and a tenant the page
  did not reach goes first in the next scan, so a tenant with a million queued
  rows is one share of a page, not the head of the line. Each tenant also has its
  own window beside the global one: `dispatch.tenants.default` and
  `dispatch.tenants[tenant_id]` set `max_inflight` and `max_inflight_bytes` of
  claimed, unfinished rows and `concurrency` attempts running at once. A tenant
  whose sink is slow fills its own window and nobody else's; a tenant at its
  window is skipped by the scan and by the wake-up timer until an outcome frees
  room. Slots go round-robin across tenants first, then across each tenant's
  keys.

  ## Per-sink isolation

  Claimed jobs wait in one queue per sink key `{tenant_id, source_id, sink
  index, sink module}`, and a tenant's slots go round-robin across the keys that
  have work, so one source's backlog cannot queue ahead of every other source's.
  With `dispatch.sink_concurrency` set (default 8), one key never runs more than
  that many attempts at once; the key carries the tenant, so the cap and the
  breaker below are per tenant.

  Each key has a circuit breaker. `dispatch.breaker_failures` consecutive
  failures (`0` disables breakers; a `{:permanent, _}` error does not count, see
  `Ankusa.Sink`) open it: the key's queued rows and every row of it claimed
  while it is open are *parked* — written back due when the breaker closes,
  with their attempt count unchanged — for `dispatch.breaker_open_ms`, doubling
  per consecutive open up to `dispatch.breaker_max_open_ms`. After that one
  attempt runs as a probe (rows claimed meanwhile are parked for
  `breaker_open_ms`): success closes the breaker, failure opens it again for
  longer. Breakers live in this process's memory; a restart closes them all.
  Parked rows spend no attempts, but not forever: a row's retry horizon
  (`retry.max_elapsed_ms`, default 24 h, from its first attempt) also ends a
  park. A parked row comes due no later than its horizon, and one that is parked
  again at or past it is dead-lettered as `{:retry_horizon, module}`.

  ## Batching sinks

  A sink that batches (`c:Ankusa.Sink.batched?/1`: `Sink.SQS`, `Sink.GooglePubSub`)
  has its attempts wait in the destination's `Ankusa.Sink.Batcher` for the
  batch's response, so counting them against `dispatch.sink_concurrency`, a
  tenant's `concurrency` or the global `dispatch.concurrency` would cap a batch
  at that many messages (8, not the 10 of an SQS batch, let alone Pub/Sub's
  1 000). Their attempts are not counted: the claimed-row windows
  (`dispatch.max_inflight`, `dispatch.tenants`) bound how many are waiting, the
  batcher bounds its queue (per tenant too) and counts `sink_concurrency` in
  in-flight *requests*. Each attempt is still its own: it settles its own row
  on its own entry's result, is timed out by `dispatch.attempt_timeout_ms`, and
  moves its own key's breaker, so a tenant whose breaker is open never has an
  entry in another tenant's batch.

  ## Binding

  A row binds to `(sink index, module)` at ack time. Its opts always come from
  the source as it is *now*, so a config fix applies to the backlog and no fun
  or secret is ever persisted. If the source's sinks were reordered the row
  falls back to the one sink with its module; if it cannot be bound it is
  dead-lettered as `{:sink_gone, index, module}`, and a deleted source as
  `{:source_gone, source_id}`.

  ## Claim check

  A body larger than a sink's `c:Ankusa.Sink.inline_max_bytes/1` is checked in
  **once** per hook, before any of its sinks run: the claims of a scan are
  packed per tenant and uploaded together (`Ankusa.ClaimCheck.check_in_batch/2`)
  in a task, and the ref is stored with the hook so every sink and every retry
  reuses it. If a pack fails, its jobs still run: the first attempt checks the
  body in on its own.

  `start_link/1` opts: `:instance` and `:config`.
  """

  use GenServer

  require Logger

  alias Ankusa.{ClaimCheck, Envelope, Ordering, RetryPolicy, Sink, SourceStore, Store, Telemetry}
  alias Ankusa.Queue.{Deliveries, Reclaim, Syncer, Tenants}
  alias Ankusa.Queue.Deliveries.{Counters, Lanes}
  alias Ankusa.Sink.Message
  alias Ankusa.Store.Keys
  alias Ankusa.Tenant.Gate

  @housekeeping_ms 1_000
  # Housekeeping ticks between `Reclaim.sweep/1` runs.
  @sweep_every 60
  # Outcome writes are buffered and written as one batch: one store write and one
  # reclaim probe per `@flush_entries` outcomes (or per `@flush_ms`), not one
  # each. A crash inside the window redelivers those hooks, which at-least-once
  # allows and the next synced commit would have flushed anyway.
  @flush_ms 10
  # An outcome that releases a lane's head is written this soon: the lane's next
  # hook waits for it.
  @lane_flush_ms 1
  @flush_entries 256
  # A scan visits the tenants the active index names (`Deliveries.active_tenants/3`)
  # and claims from each one's due range, from that tenant's cursor on, so it
  # does not walk the tombstones of every row claimed before it (that made
  # draining a backlog quadratic). A cursor follows the claims, trailing the last
  # claimed due time by `@floor_lag_ms` (clock jitter), and is kept under every
  # row that can still become visible beneath it: a commit's wake carries the
  # batch stamp, a retry or a replay lowers its tenant's cursor to its own due
  # time. A cursor is dropped when its tenant drains; a reset of all of them every
  # `@floor_reset_ms` is the backstop for anything unforeseen. Cursors are never
  # persisted: after a restart a tenant starts from 0, which only re-reads work
  # already known to be done.
  @floor_lag_ms 1_000
  @floor_reset_ms 300_000
  # Markers read per scan.
  @active_limit 4_096
  @holdoff_ms 1_000
  # Looks again this soon at rows that are due but wait for the queue's fsync; the
  # syncer wakes dispatch when it returns, so this only covers an fsync that failed.
  @unsynced_retry_ms 50
  @max_sleep_ms 30_000

  # ── public API ────────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :dispatch))
  end

  def child_spec(opts) do
    instance = Keyword.fetch!(opts, :instance)

    %{
      id: {__MODULE__, instance},
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @doc """
  Block until nothing is claimed, running, or due; returns the rows settled
  (delivered or dead-lettered) during the call. A row waiting out a backoff is
  not due, so this does not wait for it.
  """
  @spec tick(atom()) :: {:ok, non_neg_integer()}
  def tick(instance) do
    GenServer.call(Ankusa.via(instance, :dispatch), :tick, 30_000)
  end

  @doc """
  The spare capacity a replay job may use right now: the lag of the oldest due
  row (`lag_ms`, 0 when nothing is due) and whether the in-flight window is
  full. `{:error, :unavailable}` while the pipeline is down or the store
  cannot answer.
  """
  @spec pressure(atom()) ::
          {:ok, %{lag_ms: non_neg_integer(), window_full: boolean()}} | {:error, :unavailable}
  def pressure(instance) do
    GenServer.call(Ankusa.via(instance, :dispatch), :pressure, 1_000)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @doc """
  Dead-letter every pending row of `source_id` now, as `{:source_gone,
  source_id}` (what dispatch does to a deleted source's rows), and return how
  many; for deleting a source that still has a backlog. A row whose hook is
  gone is dropped and not counted. Rows claimed by a running attempt are left
  to settle: one that comes back pending after the source is deleted is
  dead-lettered when it is next claimed. Runs inside this process, the only
  one that moves pending rows, so no claim can race it. `{:error,
  :unavailable}` while the pipeline is down or busy past the call's deadline.
  """
  @spec dead_letter_source(atom(), String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def dead_letter_source(instance, source_id) do
    GenServer.call(Ankusa.via(instance, :dispatch), {:dead_letter_source, source_id}, 60_000)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @doc """
  Take `dispatch.concurrency`, `dispatch.sink_concurrency` and `dispatch.tenants`
  from the instance's current config (`Ankusa.Reload`), without a restart: the
  tenants' limits are rebuilt, and rows already claimed or running are never
  dropped (a lowered limit only stops the next start until the count falls below
  it). `:ok` also when the pipeline is down: it reads the config when it starts.
  """
  @spec reconfigure(atom()) :: :ok
  def reconfigure(instance) do
    GenServer.call(Ankusa.via(instance, :dispatch), :reconfigure, 30_000)
  catch
    :exit, _ -> :ok
  end

  @doc """
  Check the `config.dispatch` keys this module reads; raises `ArgumentError`
  naming the key.
  """
  @spec validate_config!(Ankusa.Config.t()) :: :ok
  def validate_config!(%Ankusa.Config{dispatch: dispatch}) do
    ms = dispatch.attempt_timeout_ms

    unless is_integer(ms) and ms >= 1 do
      raise ArgumentError,
            "dispatch.attempt_timeout_ms must be a positive integer, got #{inspect(ms)}"
    end

    cap = Map.get(dispatch, :sink_concurrency)

    unless cap == nil or (is_integer(cap) and cap >= 1) do
      raise ArgumentError,
            "dispatch.sink_concurrency must be nil or a positive integer, got #{inspect(cap)}"
    end

    failures = Map.get(dispatch, :breaker_failures, 0)

    unless is_integer(failures) and failures >= 0 do
      raise ArgumentError,
            "dispatch.breaker_failures must be a non-negative integer, got #{inspect(failures)}"
    end

    open = Map.get(dispatch, :breaker_open_ms, 1)

    unless is_integer(open) and open >= 1 do
      raise ArgumentError,
            "dispatch.breaker_open_ms must be a positive integer, got #{inspect(open)}"
    end

    max_open = Map.get(dispatch, :breaker_max_open_ms, open)

    unless is_integer(max_open) and max_open >= open do
      raise ArgumentError,
            "dispatch.breaker_max_open_ms must be an integer >= dispatch.breaker_open_ms " <>
              "(#{inspect(open)}), got #{inspect(max_open)}"
    end

    validate_retry!(Map.get(dispatch, :retry))

    max_after = Map.get(dispatch, :max_retry_after_ms, 3_600_000)

    unless is_integer(max_after) and max_after in 1_000..86_400_000 do
      raise ArgumentError,
            "dispatch.max_retry_after_ms must be an integer from 1000 to 86400000, " <>
              "got #{inspect(max_after)}"
    end

    validate_tenants!(Map.get(dispatch, :tenants))

    :ok
  end

  @retry_keys [:base_ms, :max_ms, :max_attempts, :jitter, :max_elapsed_ms]

  # `{module, opts}`; the options the shipped keys name are range-checked (a
  # custom policy may carry others), `max_elapsed_ms` among them: the horizon
  # every policy is held to.
  defp validate_retry!({mod, opts} = retry) when is_atom(mod) and is_list(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "dispatch.retry must be {module, keyword_opts}, got #{inspect(retry)}"
    end

    for key <- @retry_keys, Keyword.has_key?(opts, key) do
      Ankusa.RetryPolicy.validate_opt!(key, Keyword.fetch!(opts, key), "dispatch.retry")
    end

    :ok
  end

  defp validate_retry!(other) do
    raise ArgumentError, "dispatch.retry must be {module, keyword_opts}, got #{inspect(other)}"
  end

  @tenant_limit_keys [:max_inflight, :max_inflight_bytes, :concurrency]

  defp validate_tenants!(tenants) do
    unless is_map(tenants) and is_map(Map.get(tenants, :default)) do
      raise ArgumentError,
            "dispatch.tenants must be a map with a :default entry, got #{inspect(tenants)}"
    end

    Enum.each(tenants, fn
      {:default, limits} ->
        validate_tenant_limits!("dispatch.tenants.default", limits, @tenant_limit_keys)

      {tenant, limits} when is_binary(tenant) and tenant != "" ->
        validate_tenant_limits!("dispatch.tenants[#{inspect(tenant)}]", limits, [])

      {tenant, _limits} ->
        raise ArgumentError,
              "dispatch.tenants keys must be :default or a non-empty tenant id string, " <>
                "got #{inspect(tenant)}"
    end)
  end

  defp validate_tenant_limits!(name, limits, required) do
    unless is_map(limits) do
      raise ArgumentError, "#{name} must be a map of limits, got #{inspect(limits)}"
    end

    for key <- required, not is_map_key(limits, key) do
      raise ArgumentError, "#{name}.#{key} is required"
    end

    for {key, value} <- limits do
      unless key in @tenant_limit_keys do
        raise ArgumentError,
              "#{name} has unknown key #{inspect(key)}; expected #{inspect(@tenant_limit_keys)}"
      end

      unless is_integer(value) and value >= 1 do
        raise ArgumentError, "#{name}.#{key} must be a positive integer, got #{inspect(value)}"
      end
    end

    :ok
  end

  # ── GenServer ─────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Ankusa.config(instance)

    # Linked to us on purpose: nobody else knows about it, and it must not
    # outlive the pipeline. Tests that start the Pipeline alone still get it.
    {:ok, task_sup} = Task.Supervisor.start_link()

    Process.flag(:trap_exit, true)

    {default_limits, overrides} = tenant_limits(config)

    send(self(), :recover)
    Process.send_after(self(), :housekeeping, @housekeeping_ms)

    {:ok,
     %{
       instance: instance,
       config: config,
       task_sup: task_sup,
       recovered?: false,
       # tenant ordinal => where its due scan starts, see @floor_lag_ms; and an
       # upper bound of the cursors, so a wake that is not below any is O(1)
       cursors: %{},
       cursor_max: 0,
       floor_reset_at: mono_ms(),
       # the most tenants one scan visited since the last report
       tenants_visited: 0,
       # rows claimed and not yet finished, and their stored hook bytes
       claimed: 0,
       claimed_bytes: 0,
       window_full?: false,
       # a failed scan backs off instead of spinning
       holdoff?: false,
       # a :scan is in the mailbox: wakes until it runs only lower the cursors
       scan_queued?: false,
       # The default share of the global windows and each named tenant's (a named
       # tenant inherits the default's other keys), see `dispatch.tenants`
       default_limits: default_limits,
       tenant_overrides: overrides,
       # tenant ordinal => {rows, bytes, tenant id} claimed and not yet finished
       tenant_claimed: %{},
       # ordinal => limits, kept only for tenants with claimed rows, and only when
       # some tenant has an override (otherwise every tenant has the default)
       tenant_limits: %{},
       # ordinals at their own window: a scan cannot claim from them, and the
       # wake-up timer must not spin on their due rows
       tenant_full: MapSet.new(),
       # tenants a scan has reached since it last reached every active one
       served: MapSet.new(),
       # tenants whose `ankusa_dispatch_tenant_inflight` was last reported nonzero
       reported_tenants: MapSet.new(),
       # whether the node holds any lane member, read once per scan: the sweep of
       # lane markers runs only then
       lanes?: false,
       # sink key => queue of claimed jobs ready to run; `rings` maps a tenant
       # ordinal to the queue of its keys whose queue is non-empty (once each, in
       # round-robin order) and `ring` holds every tenant that has such a key,
       # once, in round-robin order
       queues: %{},
       rings: %{},
       ring: :queue.new(),
       queued: 0,
       running: %{},
       # attempts counted against the concurrency limits (not a batching sink's)
       running_counted: 0,
       running_by_key: %{},
       running_by_tenant: %{},
       # sink key => breaker; a key without an entry is closed with no failures
       breakers: %{},
       # pack task ref => the jobs it will give claims to
       packing: %{},
       packing_seqs: MapSet.new(),
       # seq => jobs of a hook whose claim is still being packed
       waiting: %{},
       # outcome batches the store refused, in order: [{ops, reclaim_pairs}]
       unrecorded: [],
       # outcomes not yet written, newest first: [{ops, reclaim_pairs}]
       buffer: [],
       buffered: 0,
       flush_timer: nil,
       timer: nil,
       settled: 0,
       waiters: [],
       sweep_ticks: 0,
       # replay job id => %{delivered: n, dead: n} since the last housekeeping
       # report to `Ankusa.Dispatch.Replayer`
       replay_outcomes: %{}
     }}
  end

  # A crash report prints the state. `config` carries every sink's options
  # (credentials), and the jobs in `queues`/`running`/`waiting`/`packing`
  # carry the same options plus the hook's body: report sizes, not contents.
  @impl true
  def format_status(%{state: %{config: _} = state} = status) do
    %{
      status
      | state: %{
          state
          | config: :redacted,
            queues: state.queued,
            ring: :queue.len(state.ring),
            running: map_size(state.running),
            breakers: breakers_open(state),
            waiting: map_size(state.waiting),
            packing: map_size(state.packing)
        }
    }
  end

  def format_status(status), do: status

  @impl true
  def handle_call(:tick, from, state) do
    state = state |> fill() |> start_jobs()
    maybe_reply_waiters(%{state | waiters: state.waiters ++ [{from, state.settled}]})
  end

  def handle_call(:pressure, _from, state) do
    reply =
      case Deliveries.next_due_at(state.instance, state.cursors) do
        {:ok, nil} -> {:ok, %{lag_ms: 0, window_full: at_capacity?(state)}}
        {:ok, at} -> {:ok, %{lag_ms: max(0, now_ms() - at), window_full: at_capacity?(state)}}
        {:error, _reason} -> {:error, :unavailable}
      end

    {:reply, reply, state}
  end

  # Runs `fun` here, between two of this process's own steps: the one process
  # that claims rows, so `fun` cannot race a claim (`Deliveries.purge_tenant/3`).
  def handle_call({:exclusive, fun}, _from, state), do: {:reply, fun.(), state}

  def handle_call(:reconfigure, _from, state) do
    config = Ankusa.config(state.instance)
    {default_limits, overrides} = tenant_limits(config)

    state = %{
      state
      | config: config,
        default_limits: default_limits,
        tenant_overrides: overrides,
        tenant_limits: %{},
        tenant_full: MapSet.new()
    }

    # What `tenant_add/2` keeps for a tenant that holds claimed rows: its limits
    # (only when some tenant has an override) and whether it is at its window.
    state =
      Enum.reduce(state.tenant_claimed, state, fn {ord, {_rows, _bytes, tenant} = entry}, acc ->
        acc =
          if map_size(overrides) > 0,
            do: %{
              acc
              | tenant_limits:
                  Map.put(acc.tenant_limits, ord, Map.get(overrides, tenant, default_limits))
            },
            else: acc

        mark_full(acc, ord, entry)
      end)

    send(self(), :wake)
    {:reply, :ok, state}
  end

  def handle_call({:dead_letter_source, source_id}, _from, state) do
    # A buffered retry outcome is a row about to be pending again: write it
    # first, so the scan below sees that row too.
    state = flush(state)
    now = now_ms()
    error = inspect({:source_gone, source_id}, limit: 50, printable_limit: 4096)

    dead_letter = fn family, acc ->
      Deliveries.fold_source(state.instance, family, source_id, acc, fn
        rows, {state, n, :ok} ->
          case dead_letter_rows(state, rows, now, error) do
            {:ok, state, dead} -> {:cont, {state, n + dead, :ok}}
            {:error, reason} -> {:halt, {state, n, {:error, reason}}}
          end
      end)
    end

    # The members of the source's lanes first, so a dead head's release finds
    # none of the source's own hooks behind it.
    result =
      with {:ok, {state, n, :ok}} <- dead_letter.(:lane_members, {state, 0, :ok}),
           do: dead_letter.(:due, {state, n, :ok})

    case result do
      {:ok, {state, n, :ok}} -> {:reply, {:ok, n}, state}
      {:ok, {state, _n, error}} -> {:reply, error, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  # Stopped by its supervisor: write what is buffered, so a graceful stop does
  # not redeliver hooks that were already delivered. The store may already be
  # gone; then they are redelivered, which is the at-least-once contract.
  @impl true
  def terminate(_reason, state) do
    state |> flush() |> flush_unrecorded()
    :ok
  end

  @impl true
  def handle_info(:recover, state) do
    with {:ok, claimed} <- Deliveries.recover_inflight(state.instance, now_ms()),
         :ok <- Reclaim.sweep(state.instance) do
      if claimed > 0 do
        Logger.info(
          "[ankusa] dispatch made #{claimed} claimed delivery row(s) due again after a restart"
        )
      end

      send(self(), :wake)
      {:noreply, %{state | recovered?: true}}
    else
      {:error, reason} ->
        Logger.error(
          "[ankusa] dispatch could not recover from the store, retrying: #{inspect(reason)}"
        )

        Process.send_after(self(), :recover, @holdoff_ms)
        {:noreply, state}
    end
  end

  # Every synced commit wakes this process (`Ankusa.Queue.Syncer.wake_dispatch/2`),
  # so under load wakes arrive faster than a scan runs. One scan serves every
  # wake in the mailbox: the first queues `:scan` behind what is already there,
  # the rest only lower the cursors, and the scan reads the store once they are
  # all in. A scan per wake starved the attempts' outcomes behind the wakes of a
  # 200 req/s backlog (the soak lost 694 704 of 720 000 hooks).
  def handle_info(:wake, state), do: {:noreply, queue_scan(state)}

  # The stamp is the batch's: its rows became visible only now (the fsync in
  # between), and a scan may already have gone past that stamp.
  def handle_info({:wake, at}, state),
    do: {:noreply, state |> lower_cursors(at) |> queue_scan()}

  def handle_info(:scan, state) do
    state =
      %{state | scan_queued?: false, holdoff?: false}
      |> fill()
      |> start_jobs()
      |> schedule_next()

    maybe_reply_waiters(state)
  end

  # A flush makes retries and expansions visible, and the capacity the outcomes
  # freed is claimed here, once, rather than once per outcome.
  def handle_info(:flush, state) do
    state = %{state | flush_timer: nil} |> flush() |> refill() |> start_jobs() |> schedule_next()
    maybe_reply_waiters(state)
  end

  def handle_info(:housekeeping, state) do
    Process.send_after(self(), :housekeeping, @housekeeping_ms)

    # `schedule_next` is here because a flush cancels the flush timer, and with
    # it the refill that would have followed: whatever the outcomes freed is
    # claimed by the wake this arms. It also re-arms a lost wake, once a second.
    state =
      state
      |> flush()
      |> flush_unrecorded()
      |> report_replay_outcomes()
      |> report_state()
      |> schedule_next()

    ticks = state.sweep_ticks + 1

    state =
      if ticks >= @sweep_every do
        sweep(state)
        %{state | sweep_ticks: 0}
      else
        %{state | sweep_ticks: ticks}
      end

    maybe_reply_waiters(state)
  end

  def handle_info({ref, {:packed, results}}, %{packing: packing} = state)
      when is_map_key(packing, ref) do
    Process.demonitor(ref, [:flush])

    state
    |> packed(ref, results)
    |> start_jobs()
    |> maybe_reply_waiters()
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{packing: packing} = state)
      when is_map_key(packing, ref) do
    # The pack task died without a result: its jobs fall back to checking
    # their bodies in one at a time.
    state
    |> packed(ref, %{})
    |> start_jobs()
    |> maybe_reply_waiters()
  end

  def handle_info({ref, {result, fresh_claim}}, %{running: running} = state)
      when is_map_key(running, ref) do
    Process.demonitor(ref, [:flush])
    outcome(state, ref, result, fresh_claim)
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{running: running} = state)
      when is_map_key(running, ref) do
    # No result ever arrived: the task was killed. That is a failed attempt like
    # any other, and the row's own retry policy decides what happens next.
    outcome(state, ref, {:error, {:exit, reason}}, nil)
  end

  # The attempt outlasted `dispatch.attempt_timeout_ms`: kill it and count a
  # failed attempt. A reply that landed in the mailbox meanwhile still counts.
  def handle_info({:attempt_timeout, ref}, %{running: running} = state)
      when is_map_key(running, ref) do
    %{job: job, task: task} = Map.fetch!(running, ref)
    timeout = state.config.dispatch.attempt_timeout_ms

    case Task.shutdown(task, :brutal_kill) do
      {:ok, {result, fresh_claim}} ->
        outcome(state, ref, result, fresh_claim)

      {:exit, reason} ->
        outcome(state, ref, {:error, {:exit, reason}}, nil)

      nil ->
        {mod, _opts} = job.spec

        Logger.warning(
          "[ankusa] sink #{inspect(mod)} did not finish hook #{job.env.id} within " <>
            "#{timeout}ms; attempt #{job.row.attempts + 1} failed (the sink may still " <>
            "complete it; consumers dedupe on the idempotency key)"
        )

        outcome(state, ref, {:error, {:attempt_timeout, timeout}}, nil)
    end
  end

  def handle_info({:EXIT, _pid, reason}, state), do: {:stop, reason, state}

  # Stray messages (e.g. a DOWN from a task already demonitored) must never take
  # down a process other parts of the tree are waiting on.
  def handle_info(_message, state), do: {:noreply, state}

  # Replay jobs learn how many of their deliveries settled, so the Replayer can
  # count and auto-pause. Sent on housekeeping, so at most once a second; a
  # missing Replayer (no :dispatch role tree) just drops them.
  defp report_replay_outcomes(%{replay_outcomes: outcomes} = state)
       when map_size(outcomes) == 0,
       do: state

  defp report_replay_outcomes(state) do
    case Ankusa.whereis(state.instance, :replayer) do
      pid when is_pid(pid) -> send(pid, {:replay_outcomes, state.replay_outcomes})
      nil -> :ok
    end

    %{state | replay_outcomes: %{}}
  end

  # ── claiming ──────────────────────────────────────────────────────────────

  # Nothing is claimed before the restart recovery has made the rows a previous
  # life left claimed due again.
  defp fill(%{recovered?: false} = state), do: state
  defp fill(%{holdoff?: true} = state), do: state

  defp fill(state) do
    if at_capacity?(state) do
      %{state | window_full?: true}
    else
      scan(maybe_reset_cursors(state))
    end
  end

  defp at_capacity?(state) do
    dispatch = state.config.dispatch
    state.claimed >= dispatch.max_inflight or state.claimed_bytes >= dispatch.max_inflight_bytes
  end

  defp scan(state) do
    dispatch = state.config.dispatch
    state = lanes_present(state)
    # A group the queue writer has applied but the syncer has not fsynced yet is
    # not due: the hook was not acked, and a delivery of it would outrun its
    # durability (`Ankusa.Queue.Syncer.visible/2`). The syncer wakes this process
    # when the fsync returns.
    now = Syncer.visible(state.instance, now_ms())
    limit = min(dispatch.batch, dispatch.max_inflight - state.claimed)
    budget = dispatch.max_inflight_bytes - state.claimed_bytes

    case state.instance |> Deliveries.active_tenants(now, @active_limit) |> unfenced(state) do
      {:ok, []} ->
        %{state | window_full?: false, served: MapSet.new()}

      {:ok, tenants} ->
        case claim_round(state, tenants, now, limit, budget) do
          {:ok, state, claimed, visited, blocked?} ->
            state = %{state | tenants_visited: max(state.tenants_visited, visited)}

            # A full page means there is probably more; a short one means
            # nothing else is due right now, or only what a tenant's own
            # window does not let it claim yet (an outcome frees that).
            if claimed >= limit,
              do: fill(state),
              else: %{state | window_full?: at_capacity?(state) or blocked?}

          {:error, state} ->
            holdoff(state)
        end

      {:error, reason} ->
        Logger.error("[ankusa] dispatch could not scan the store, retrying: #{inspect(reason)}")
        holdoff(state)
    end
  end

  # Tenants an erase has closed (`Ankusa.Tenant.Gate`) are not claimed from: their
  # rows are about to be deleted, and a claimed one would be settled over by its
  # attempt's outcome. Nothing is closed in the usual case: one map read.
  defp unfenced({:ok, tenants}, state) do
    ords = Gate.state(state.instance).ords

    if MapSet.size(ords) == 0,
      do: {:ok, tenants},
      else: {:ok, Enum.reject(tenants, fn {ord, _buckets} -> MapSet.member?(ords, ord) end)}
  end

  defp unfenced({:error, reason}, _state), do: {:error, reason}

  # Deficit round-robin over the active tenants. A pass gives each tenant, in
  # turn, an equal share of what is left of the page (`share`, at least one row)
  # of its due rows, within its own window; a tenant that took its whole share
  # may have more and goes into the next pass with the share of what is left,
  # one that took less has nothing more due and forfeits the rest. Tenants not
  # reached since the last full round go first, so when there are more active
  # tenants than rows in a page every one of them still gets its turn.
  defp claim_round(state, tenants, now, limit, budget) do
    {fresh, reached} =
      Enum.split_with(tenants, fn {ord, _buckets} -> not MapSet.member?(state.served, ord) end)

    acc = %{
      left: limit,
      budget: budget,
      claimed: 0,
      reached: MapSet.new(),
      read: MapSet.new(),
      blocked?: false,
      wanting: []
    }

    case drr(state, fresh ++ reached, now, acc) do
      {:ok, state, acc} ->
        state = mark_served(state, tenants, acc.reached)
        {:ok, state, acc.claimed, MapSet.size(acc.read), acc.blocked?}

      {:error, state} ->
        {:error, state}
    end
  end

  defp drr(state, tenants, now, acc) do
    share = max(1, div(acc.left, length(tenants)))

    case pass(state, tenants, now, share, %{acc | wanting: []}) do
      {:ok, state, acc} ->
        wanting = Enum.reverse(acc.wanting)

        if wanting == [] or acc.left <= 0 or (acc.claimed > 0 and acc.budget <= 0),
          do: {:ok, state, acc},
          else: drr(state, wanting, now, acc)

      {:error, state} ->
        {:error, state}
    end
  end

  defp pass(state, [], _now, _share, acc), do: {:ok, state, acc}

  defp pass(state, [{ord, buckets} | rest], now, share, acc) do
    if acc.left <= 0 or (acc.claimed > 0 and acc.budget <= 0) do
      {:ok, state, acc}
    else
      case pass_tenant(state, ord, buckets, now, share, acc) do
        {:ok, state, acc} -> pass(state, rest, now, share, acc)
        {:error, state} -> {:error, state}
      end
    end
  end

  defp pass_tenant(state, ord, buckets, now, share, acc) do
    limits = limits_of(state, ord)
    {rows, bytes} = tenant_window(state, ord)
    acc = %{acc | reached: MapSet.put(acc.reached, ord)}

    if rows >= limits.max_inflight or bytes >= limits.max_inflight_bytes do
      {:ok, state, %{acc | blocked?: true}}
    else
      ask = Enum.min([share, acc.left, limits.max_inflight - rows])
      budget = min(acc.budget, limits.max_inflight_bytes - bytes)
      # Before the due fold, so a member promoted now is claimed in this scan.
      {state, more?} = sweep_lanes(state, ord, now, share)
      take(state, ord, buckets, now, {ask, share, budget, more?}, acc)
    end
  end

  defp take(state, ord, buckets, now, {ask, share, budget, more?}, acc) do
    floor = Map.get(state.cursors, ord, 0)

    case Deliveries.due_for_tenant(state.instance, ord, now, floor, ask, budget) do
      {:ok, dues} ->
        took(state, ord, buckets, now, {share, more?}, dues, acc)

      {:error, reason} ->
        Logger.error("[ankusa] dispatch could not scan the store, retrying: #{inspect(reason)}")
        {:error, state}
    end
  end

  defp took(state, ord, buckets, now, {share, more?}, dues, acc) do
    case claim_tenant(state, ord, dues, now) do
      {:ok, state} ->
        n = length(dues)
        bytes = Enum.reduce(dues, 0, &(&1.size + &2))
        state = tidy_tenant(state, ord, buckets, now)
        wanting = if n >= share or more?, do: [{ord, []} | acc.wanting], else: acc.wanting

        {:ok, state,
         %{
           acc
           | left: acc.left - n,
             budget: acc.budget - bytes,
             claimed: acc.claimed + n,
             read: MapSet.put(acc.read, ord),
             wanting: wanting
         }}

      {:error, state} ->
        {:error, state}
    end
  end

  # The node's lane members, one counter read per scan: `lanes?` gates every
  # other cost of the lanes (the sweep, the marker check of the tidy).
  defp lanes_present(state) do
    case Counters.read_field(state.instance, [0], :lane_members) do
      {:ok, [members]} -> %{state | lanes?: members > 0}
      {:error, _reason} -> state
    end
  end

  # ADR 0004, section 4.6: promote members of idle lanes, heal stale heads. The
  # ops go through `write/2` like every other Pipeline write, so the cursor of
  # the tenant comes down to the rows just made due. `more?` says the sweep hit
  # its limit with work done, so the visit goes again.
  defp sweep_lanes(%{lanes?: false} = state, _ord, _now, _share), do: {state, false}

  defp sweep_lanes(state, ord, now, share) do
    case Lanes.sweep(state.instance, ord, now, share) do
      {:ok, ops, more?} ->
        case write(state, ops) do
          {:ok, state} ->
            {state, more?}

          {:error, reason} ->
            Logger.warning("[ankusa] lane sweep of tenant #{ord} failed: #{inspect(reason)}")
            {state, false}
        end

      {:error, reason} ->
        Logger.warning("[ankusa] lane sweep of tenant #{ord} failed: #{inspect(reason)}")
        {state, false}
    end
  end

  # The tenants reached by the scans since the last full round; the round starts
  # over once every active tenant has been reached.
  defp mark_served(state, tenants, reached) do
    active = MapSet.new(tenants, fn {ord, _buckets} -> ord end)
    served = state.served |> MapSet.union(reached) |> MapSet.intersection(active)
    %{state | served: if(MapSet.equal?(served, active), do: MapSet.new(), else: served)}
  end

  defp claim_tenant(state, _ord, [], _now), do: {:ok, state}

  defp claim_tenant(state, ord, dues, now) do
    case claim(state, dues, now) do
      # Past the rows just claimed, and only once they are: a failed load or
      # claim leaves them due, and the cursor has to stay beneath them.
      {:ok, state} -> {:ok, raise_cursor(state, ord, List.last(dues).at)}
      {:error, state} -> {:error, state}
    end
  end

  # ── per-tenant windows ────────────────────────────────────────────────────

  # The default share and each named tenant's, a named tenant inheriting the
  # default's other keys.
  defp tenant_limits(config) do
    {default, named} = Map.pop!(config.dispatch.tenants, :default)
    {default, Map.new(named, fn {tenant, limits} -> {tenant, Map.merge(default, limits)} end)}
  end

  # A tenant's limits. Only a tenant with an override needs its name, which a
  # tenant with claimed rows already has in `tenant_limits`; the others cost one
  # point read of the `tenants` family the first time a scan meets them.
  defp limits_of(state, ord) do
    case state.tenant_limits do
      %{^ord => limits} ->
        limits

      _ when map_size(state.tenant_overrides) == 0 ->
        state.default_limits

      _ ->
        case Tenants.name(state.instance, ord) do
          {:ok, name} -> Map.get(state.tenant_overrides, name, state.default_limits)
          _ -> state.default_limits
        end
    end
  end

  defp tenant_window(state, ord) do
    case state.tenant_claimed do
      %{^ord => {rows, bytes, _tenant}} -> {rows, bytes}
      _ -> {0, 0}
    end
  end

  # A job enters the tenant's window when it is staged, and leaves it when it is
  # parked or has an outcome (the three places `claimed` moves).
  defp tenant_add(state, %{ord: ord, size: size, key: {tenant, _, _, _}}) do
    {rows, bytes, _tenant} = Map.get(state.tenant_claimed, ord, {0, 0, tenant})
    entry = {rows + 1, bytes + size, tenant}
    state = %{state | tenant_claimed: Map.put(state.tenant_claimed, ord, entry)}

    state =
      if rows == 0 and map_size(state.tenant_overrides) > 0 do
        limits = Map.get(state.tenant_overrides, tenant, state.default_limits)
        %{state | tenant_limits: Map.put(state.tenant_limits, ord, limits)}
      else
        state
      end

    mark_full(state, ord, entry)
  end

  defp tenant_sub(state, ord, size) do
    case state.tenant_claimed do
      %{^ord => {1, _bytes, _tenant}} ->
        %{
          state
          | tenant_claimed: Map.delete(state.tenant_claimed, ord),
            tenant_limits: Map.delete(state.tenant_limits, ord),
            tenant_full: MapSet.delete(state.tenant_full, ord)
        }

      %{^ord => {rows, bytes, tenant}} ->
        entry = {rows - 1, bytes - size, tenant}

        mark_full(
          %{state | tenant_claimed: Map.put(state.tenant_claimed, ord, entry)},
          ord,
          entry
        )

      _ ->
        state
    end
  end

  defp mark_full(state, ord, {rows, bytes, _tenant}) do
    limits = Map.get(state.tenant_limits, ord, state.default_limits)
    full? = rows >= limits.max_inflight or bytes >= limits.max_inflight_bytes

    full =
      if full?,
        do: MapSet.put(state.tenant_full, ord),
        else: MapSet.delete(state.tenant_full, ord)

    %{state | tenant_full: full}
  end

  # After a visit: stale markers go (at most once a second a tenant, it is a
  # seek from the start of the tenant's range), and a tenant found to have
  # nothing due loses its cursor (the scan starts again from 0 when it next has
  # work).
  defp tidy_tenant(state, ord, buckets, now) do
    if Enum.any?(buckets, &(&1 < div(now, 1000))) do
      case Deliveries.sweep_markers(state.instance, ord, buckets, now, lanes: state.lanes?) do
        {:ok, true} ->
          %{state | cursors: Map.delete(state.cursors, ord)}

        {:ok, false} ->
          state

        {:error, reason} ->
          Logger.warning("[ankusa] dispatch could not tidy tenant #{ord}: #{inspect(reason)}")
          state
      end
    else
      state
    end
  end

  # Load before claiming: a failed read leaves every row due, with nothing to
  # undo. This process is the only one that moves pending rows, so nothing can
  # change them in between.
  defp claim(state, dues, now) do
    with {:ok, loaded} <- Deliveries.load(state.instance, dues),
         :ok <- Deliveries.claim(state.instance, dues) do
      {:ok, resolve(state, loaded, now)}
    else
      {:error, reason} ->
        Logger.error(
          "[ankusa] dispatch could not claim delivery rows, retrying: #{inspect(reason)}"
        )

        {:error, state}
    end
  end

  defp queue_scan(%{scan_queued?: true} = state), do: state

  defp queue_scan(state) do
    send(self(), :scan)
    %{state | scan_queued?: true}
  end

  defp holdoff(state) do
    state = cancel_timer(state)
    timer = Process.send_after(self(), :wake, @holdoff_ms)
    %{state | holdoff?: true, timer: timer}
  end

  defp maybe_reset_cursors(state) do
    if mono_ms() - state.floor_reset_at >= @floor_reset_ms do
      %{state | cursors: %{}, cursor_max: 0, floor_reset_at: mono_ms()}
    else
      state
    end
  end

  # ── resolving claimed rows ────────────────────────────────────────────────

  # Every claimed row ends up in one of three places: a job to run, a
  # non-delivery outcome written right here (dead-lettered, expanded, dropped),
  # or both ends of a missing row cleaned up.
  defp resolve(state, loaded, now) do
    acc = {state, [], [], [], %{}}

    {state, ops, pairs, jobs, _sources} =
      Enum.reduce(loaded, acc, fn item, acc -> resolve_one(item, acc, now) end)

    state
    |> record(ops, pairs)
    |> stage(Enum.reverse(jobs))
  end

  defp resolve_one(%{row: nil} = item, {state, ops, pairs, jobs, sources}, _now) do
    Logger.warning("[ankusa] delivery row #{item.seq}/#{item.sink} vanished; dropping its claim")
    drop = Deliveries.orphan_ops(:inflight, item.ord, item.seq, item.sink, item.size)
    {state, drop ++ ops, pairs, jobs, sources}
  end

  defp resolve_one(%{env: nil} = item, {state, ops, pairs, jobs, sources}, _now) do
    Logger.error("[ankusa] hook #{item.seq} is missing; dropping its delivery row #{item.sink}")

    drop = Deliveries.orphan_ops(:inflight, item.ord, item.seq, item.sink, item.size, item.row)

    {state, drop ++ ops, pairs, jobs, sources}
  end

  # A row this node cannot read (a version it does not know, or damage) is
  # dead-lettered, never raised on: it is rebuilt from what the claim knows, so
  # it lists in the DLQ with the decode error as its reason.
  defp resolve_one(%{row: {:error, reason}} = item, acc, now) do
    Logger.error(
      "[ankusa] delivery row #{item.seq}/#{item.sink} does not decode (#{reason}); dead-lettering it"
    )

    {source_id, _id} = Deliveries.owner(item.env, %{})

    row = %{
      module: nil,
      state: :pending,
      attempts: 0,
      at: now,
      error: nil,
      size: item.size,
      source_id: source_id
    }

    dead(%{item | row: row}, reason, :permanent, acc, now)
  end

  # The same for a hook this node cannot decode, e.g. an envelope version newer
  # than it knows (`:unsupported_envelope_version`): its delivery dead-letters
  # with that reason and the hook stays in the store for a node that can read it.
  defp resolve_one(%{env: {:error, reason}} = item, acc, now) do
    Logger.error(
      "[ankusa] hook #{item.seq} does not decode (#{reason}); " <>
        "dead-lettering its delivery row #{item.sink}"
    )

    dead(item, reason, :permanent, acc, now)
  end

  defp resolve_one(item, {state, ops, pairs, jobs, sources}, now) do
    {source, sources} = fetch_source(state.instance, item.env.source_id, sources)

    cond do
      source == :error ->
        dead(
          item,
          {:source_gone, item.env.source_id},
          :permanent,
          {state, ops, pairs, jobs, sources},
          now
        )

      # The source store could not answer: neither a delivery nor a verdict.
      # The row waits a moment and keeps its attempt count.
      source == :unavailable ->
        later =
          Deliveries.retry_ops(
            item.ord,
            item.seq,
            item.sink,
            item.row,
            now + @holdoff_ms,
            "source store unavailable"
          )

        {state, later ++ ops, pairs, jobs, sources}

      Deliveries.unresolved?(item.sink) ->
        expand(item, source, {state, ops, pairs, jobs, sources}, now)

      true ->
        # The first time a row is seen is when its retry horizon starts; the
        # stamp is written with whatever the row is rewritten as (a retry, a park,
        # the dead letter), so it survives a restart.
        item = %{item | row: Map.put_new(item.row, :first_attempt_at, now)}
        policy = RetryPolicy.for_source(state.config.dispatch.retry, source.retry)

        key =
          {Tenants.normalize(item.env.tenant_id), item.env.source_id, item.sink, item.row.module}

        case admit(state, key) do
          {:park, at, state} ->
            deadline = RetryPolicy.deadline(item.row.first_attempt_at, policy)

            # Parked rows spend no attempts, so the clock is what ends them: a row
            # whose horizon passed behind an open breaker is dead-lettered, not
            # parked again.
            if now >= deadline do
              reason = {:retry_horizon, item.row.module}
              # `admit/2` parks only for a key whose breaker is `:open` or
              # `:half_open`, so the entry exists.
              %{class: class} = Map.fetch!(state.breakers, key)
              dead(item, reason, class, {state, ops, pairs, jobs, sources}, now)
            else
              state = bump_replay(state, item.row, :parked)
              {state, park_ops(item, min(at, deadline)) ++ ops, pairs, jobs, sources}
            end

          {:pass, state} ->
            case bind(source.sinks, item.sink, item.row.module) do
              {:ok, spec} ->
                job = %{
                  key: key,
                  ord: item.ord,
                  seq: item.seq,
                  sink: item.sink,
                  size: item.size,
                  row: item.row,
                  env: item.env,
                  spec: spec,
                  batched?: Sink.batched?(elem(spec, 0), elem(spec, 1)),
                  policy: policy,
                  claim: item.claim,
                  forward_headers: source.forward_headers,
                  max_headers: source.max_headers,
                  forward_headers_max_bytes: source.forward_headers_max_bytes
                }

                {state, ops, pairs, [job | jobs], sources}

              :error ->
                reason = {:sink_gone, item.sink, item.row.module}
                dead(item, reason, :permanent, {state, ops, pairs, jobs, sources}, now)
            end
        end
    end
  end

  # An imported row never recorded its sink: give the hook one row per current
  # sink of its source, and wake up to claim them.
  defp expand(item, source, {state, ops, pairs, jobs, sources}, now) do
    case Deliveries.existing_sinks(state.instance, item.seq) do
      {:ok, existing} ->
        send(self(), :wake)

        expansion =
          Deliveries.expand_ops(
            item.ord,
            item.seq,
            item.sink,
            item.size,
            item.env.source_id,
            source.sinks,
            existing,
            now
          )

        pairs =
          if source.sinks == [], do: [cleared(item.seq, item.sink) | pairs], else: pairs

        {state, expansion ++ ops, pairs, jobs, sources}

      {:error, reason} ->
        Logger.warning("[ankusa] could not expand imported row #{item.seq}: #{inspect(reason)}")

        retry_later =
          Deliveries.retry_ops(item.ord, item.seq, item.sink, item.row, now + @holdoff_ms, nil)

        {state, retry_later ++ ops, pairs, jobs, sources}
    end
  end

  defp dead(item, reason, class, {state, ops, pairs, jobs, sources}, now) do
    error = inspect(reason, limit: 50, printable_limit: 4096)

    dead_ops =
      Deliveries.dead_ops(
        :inflight,
        item.ord,
        item.seq,
        item.sink,
        item.row,
        now,
        {class, error},
        item.env
      )

    state =
      state
      |> settle_dead(item.env, item.row, item.row.module, item.row.attempts)
      |> bump_replay(item.row, :dead)

    {state, dead_ops ++ ops, pairs, jobs, sources}
  end

  # Pending rows of a source being deleted (`dead_letter_source/2`): the same
  # dead row, dead key, telemetry and replay count as `dead/5`, from the due
  # index instead of a claim, in one synced batch. A row whose hook is missing
  # is dropped, as `resolve_one/3` drops it.
  defp dead_letter_rows(state, rows, now, error) do
    seqs = rows |> Enum.map(& &1.seq) |> Enum.uniq()

    with {:ok, hooks} <- Store.multi_get(state.instance, :hooks, Enum.map(seqs, &Keys.hook/1)) do
      envs =
        seqs
        |> Enum.zip(hooks)
        |> Map.new(fn
          {seq, {:ok, bin}} ->
            case Envelope.from_binary(bin) do
              {:ok, env} -> {seq, %{env | seq: seq}}
              {:error, _reason} = undecoded -> {seq, undecoded}
            end

          {seq, :not_found} ->
            {seq, nil}
        end)

      {ops, dead} =
        Enum.reduce(rows, {[], []}, fn %{seq: seq, sink: sink, row: row} = entry, {ops, dead} ->
          from = Deliveries.source_from(entry)

          case Map.fetch!(envs, seq) do
            nil ->
              Logger.error("[ankusa] hook #{seq} is missing; dropping its delivery row #{sink}")
              {Deliveries.orphan_ops(from, entry.ord, seq, sink, entry.size, row) ++ ops, dead}

            env ->
              dead_ops =
                Deliveries.dead_ops(
                  from,
                  entry.ord,
                  seq,
                  sink,
                  row,
                  now,
                  {:permanent, error},
                  env
                )

              {dead_ops ++ ops, [{env, row} | dead]}
          end
        end)

      with {:ok, ops} <- Lanes.expand(state.instance, ops, now_ms()),
           :ok <- Store.write(state.instance, Deliveries.coalesce(ops), sync: true) do
        state =
          Enum.reduce(dead, state, fn {env, row}, state ->
            state
            |> settle_dead(env, row, row.module, row.attempts)
            |> bump_replay(row, :dead)
          end)

        {:ok, state, length(dead)}
      end
    end
  end

  defp settle_dead(state, env, row, module, attempts, attempt_ms \\ nil) do
    {source_id, _id} = Deliveries.owner(env, row)
    tenant_id = tenant_of(env)

    Telemetry.emit([:dispatch, :stop], stop_measurements(env, row, attempt_ms), %{
      instance: state.instance,
      result: :dlq,
      attempts: attempts,
      sink: module,
      source_id: source_id,
      ordered: ordered?(row),
      tenant_id: tenant_id
    })

    Telemetry.emit([:dispatch, :dlq], %{}, %{
      instance: state.instance,
      source_id: source_id,
      tenant_id: tenant_id,
      sink: module
    })

    %{state | settled: state.settled + 1}
  end

  # A row of an ordering lane (ADR 0004): `ordered` in the metadata of the stop
  # event.
  defp ordered?(row), do: is_binary(Map.get(row, :lane))

  # The tenant a dead row's hook belonged to; a hook that did not decode has no
  # tenant to name, so it is the default tenant's.
  defp tenant_of(%Envelope{tenant_id: tenant_id}), do: Tenants.normalize(tenant_id)
  defp tenant_of(_undecoded), do: Tenants.normalize(nil)

  # What `[:ankusa, :dispatch, :stop]` measures: the final attempt's time (none
  # for a row dead-lettered without an attempt) and the hook's age. A replayed
  # delivery has no age: it was received before the replay, which is the
  # operator's doing and not the delivery's latency.
  defp stop_measurements(env, row, attempt_ms) do
    measurements = if attempt_ms, do: %{attempt_ms: attempt_ms}, else: %{}

    with nil <- Map.get(row, :replay),
         ms when is_integer(ms) <- Telemetry.since_received_ms(env) do
      Map.put(measurements, :since_received_ms, ms)
    else
      _ -> measurements
    end
  end

  defp attempt_result(:ok), do: :ok
  defp attempt_result({:error, {:attempt_timeout, _ms}}), do: :timeout
  defp attempt_result({:error, _reason}), do: :error

  # Sources are looked up once per scan (`sources` is the scan's cache). A store
  # that cannot answer is cached as `:unavailable` for the rest of the scan,
  # and said once.
  defp fetch_source(instance, source_id, sources) do
    case sources do
      %{^source_id => source} ->
        {source, sources}

      _ ->
        source =
          case SourceStore.fetch(instance, source_id) do
            {:ok, source} ->
              source

            :error ->
              :error

            {:error, :unavailable} ->
              Logger.warning(
                "[ankusa] source store unavailable for #{inspect(source_id)}; " <>
                  "its deliveries wait #{@holdoff_ms} ms"
              )

              :unavailable
          end

        {source, Map.put(sources, source_id, source)}
    end
  end

  # The sink at the row's index, if it is still the same module; else the only
  # sink with that module; else the row cannot be bound.
  defp bind(sinks, index, module) do
    case Enum.at(sinks, index) do
      {^module, _opts} = sink ->
        {:ok, sink}

      _ ->
        case Enum.filter(sinks, fn {mod, _opts} -> mod == module end) do
          [sink] -> {:ok, sink}
          _ -> :error
        end
    end
  end

  defp cleared(seq, sink), do: {seq, Keys.cleared(seq, 0, sink)}

  # ── claim check staging ───────────────────────────────────────────────────

  defp stage(state, []), do: state

  defp stage(state, jobs) do
    bytes = Enum.reduce(jobs, 0, &(&1.size + &2))

    state =
      %{
        state
        | claimed: state.claimed + length(jobs),
          claimed_bytes: state.claimed_bytes + bytes
      }
      |> then(fn state -> Enum.reduce(jobs, state, &tenant_add(&2, &1)) end)

    {needing, ready} = Enum.split_with(jobs, &needs_claim?/1)
    state = Enum.reduce(ready, state, &enqueue(&2, &1))

    # A hook whose claim is already being packed waits for that pack.
    {waiting, to_pack} = Enum.split_with(needing, &MapSet.member?(state.packing_seqs, &1.seq))

    state =
      Enum.reduce(waiting, state, fn job, state ->
        %{state | waiting: Map.update(state.waiting, job.seq, [job], &[job | &1])}
      end)

    pack(state, to_pack)
  end

  # A body over the sink's inline limit needs a claim, unless the hook already
  # has a stored one. Runs in the pipeline process: a sink whose threshold
  # callback fails needs no claim here — the job runs, and its task reports the
  # failure (`ensure_claim/2`).
  defp needs_claim?(%{claim: claim}) when claim != nil, do: false

  defp needs_claim?(%{spec: {mod, opts}, env: env}) do
    case Sink.inline_max_bytes(mod, opts) do
      {:ok, nil} -> false
      {:ok, max} -> env.size > max
      {:error, _reason} -> false
    end
  end

  defp pack(state, []), do: state

  defp pack(state, jobs) do
    by_seq = Enum.group_by(jobs, & &1.seq)
    items = Enum.map(by_seq, fn {_seq, [job | _]} -> Message.claim_item(job.env) end)
    instance = state.instance

    task =
      Task.Supervisor.async_nolink(state.task_sup, fn ->
        {:packed, ClaimCheck.check_in_batch(instance, items)}
      end)

    %{
      state
      | packing: Map.put(state.packing, task.ref, jobs),
        packing_seqs: MapSet.union(state.packing_seqs, MapSet.new(Map.keys(by_seq)))
    }
  end

  # Persist each hook's ref, give it to the pack's jobs and to the jobs that
  # waited on it (a failed pack leaves a job without one, so it checks its body
  # in itself), and let them all run.
  defp packed(state, ref, results) do
    {jobs, packing} = Map.pop(state.packing, ref)
    seqs = jobs |> Enum.map(& &1.seq) |> Enum.uniq()

    claims =
      jobs
      |> Enum.uniq_by(& &1.seq)
      |> Enum.flat_map(fn job ->
        case Map.get(results, job.env.id) do
          {:ok, claim} -> [{job.seq, claim}]
          _ -> []
        end
      end)
      |> Map.new()

    ords = Map.new(jobs, &{&1.seq, &1.ord})

    claim_ops =
      Enum.flat_map(claims, fn {seq, claim} ->
        Deliveries.claim_ops(seq, Map.fetch!(ords, seq), claim)
      end)

    state = record(state, claim_ops, [])

    {waiting, released} =
      Enum.reduce(seqs, {state.waiting, []}, fn seq, {waiting, released} ->
        {jobs, waiting} = Map.pop(waiting, seq, [])
        {waiting, jobs ++ released}
      end)

    state = %{
      state
      | packing: packing,
        packing_seqs: MapSet.difference(state.packing_seqs, MapSet.new(seqs)),
        waiting: waiting
    }

    Enum.reduce(jobs ++ released, state, fn job, state ->
      job =
        case Map.fetch(claims, job.seq) do
          {:ok, claim} -> %{job | claim: claim}
          :error -> job
        end

      enqueue(state, job)
    end)
  end

  defp enqueue(state, job) do
    case admit(state, job.key) do
      {:park, at, state} -> park(state, job, at)
      {:pass, state} -> push(state, job)
    end
  end

  defp push(state, %{key: key, ord: ord} = job) do
    queue = Map.get(state.queues, key, :queue.new())
    state = if :queue.is_empty(queue), do: ring_in(state, ord, key), else: state

    %{state | queues: Map.put(state.queues, key, :queue.in(job, queue)), queued: state.queued + 1}
  end

  # `key` has a first queued job: it joins its tenant's keys, and the tenant the
  # ring of tenants if it had no queued key.
  defp ring_in(state, ord, key) do
    case state.rings do
      %{^ord => keys} ->
        %{state | rings: Map.put(state.rings, ord, :queue.in(key, keys))}

      _ ->
        %{
          state
          | rings: Map.put(state.rings, ord, :queue.from_list([key])),
            ring: :queue.in(ord, state.ring)
        }
    end
  end

  # ── breakers ──────────────────────────────────────────────────────────────

  # Whether a key may run now, or its rows are parked until `at` (wall clock).
  # An open breaker whose period is over turns half-open here: the next job of
  # the key to start is its probe.
  defp admit(state, key) do
    case Map.get(state.breakers, key) do
      %{state: :open, until: until} = breaker ->
        now = mono_ms()

        if now < until do
          {:park, now_ms() + (until - now), state}
        else
          {:pass, put_breaker(state, key, %{breaker | state: :half_open, probe: nil})}
        end

      %{state: :half_open, probe: probe} when probe != nil ->
        {:park, now_ms() + state.config.dispatch.breaker_open_ms, state}

      _closed_or_unprobed ->
        {:pass, state}
    end
  end

  defp put_breaker(state, key, breaker),
    do: %{state | breakers: Map.put(state.breakers, key, breaker)}

  defp breakers_open(state),
    do: Enum.count(state.breakers, fn {_key, b} -> b.state != :closed end)

  # A claimed job goes back to its row, due at `at`, with its attempt count
  # unchanged, and leaves the window.
  # A replayed row parked behind an open breaker is reported as `:parked`, so a
  # replay job aimed at a destination that is still down auto-pauses instead of
  # moving the whole DLQ into rows that wait on the breaker.
  defp park(state, job, at) do
    state = %{state | claimed: state.claimed - 1, claimed_bytes: state.claimed_bytes - job.size}
    state = tenant_sub(state, job.ord, job.size)
    at = min(at, RetryPolicy.deadline(job.row.first_attempt_at, job.policy))
    state |> bump_replay(job.row, :parked) |> append(park_ops(job, at))
  end

  defp park_ops(%{ord: ord, seq: seq, sink: sink, row: row}, at),
    do: Deliveries.retry_ops(ord, seq, sink, row, at, "breaker open")

  # Buffer outcome ops without the refill a full buffer otherwise triggers:
  # parking runs while a scan is still being resolved.
  defp append(state, ops) do
    state = %{state | buffer: [{ops, []} | state.buffer], buffered: state.buffered + 1}
    if state.buffered >= @flush_entries, do: flush(state), else: arm_flush(state)
  end

  # Every attempt's outcome moves its key's breaker. `:ok` closes it. A failure
  # that is not `{:permanent, _}` counts; the `breaker_failures`-th in a row, or
  # a failed probe, opens it, and whatever of the key is still queued is parked.
  # A counted failure also records its `Sink.error_class/1`, and that is the
  # class a row parked behind the breaker is dead-lettered as at its horizon.
  # A `{:permanent, _}` answer to a probe closes it: the destination answered,
  # it just refused that one hook (and leaving the probe slot set would park
  # the key forever).
  defp trip(state, job, :ok, _probe?) do
    case Map.pop(state.breakers, job.key) do
      {nil, _breakers} ->
        state

      {%{state: :closed}, breakers} ->
        %{state | breakers: breakers}

      {_open_or_half_open, breakers} ->
        breaker_event(state, job, :closed)
        %{state | breakers: breakers}
    end
  end

  defp trip(state, job, {:error, reason}, probe?) do
    dispatch = state.config.dispatch
    class = Sink.classify(reason)

    cond do
      dispatch.breaker_failures == 0 ->
        state

      match?({:permanent, _}, class) and probe? ->
        trip(state, job, :ok, probe?)

      match?({:permanent, _}, class) ->
        state

      true ->
        breaker =
          Map.get(state.breakers, job.key, %{
            state: :closed,
            failures: 0,
            opens: 0,
            until: 0,
            probe: nil,
            class: :transient
          })

        breaker = %{breaker | failures: breaker.failures + 1, class: Sink.error_class(class)}

        open? =
          probe? or (breaker.state == :closed and breaker.failures >= dispatch.breaker_failures)

        if open?,
          do: open_breaker(state, job, breaker),
          else: put_breaker(state, job.key, breaker)
    end
  end

  defp open_breaker(state, job, breaker) do
    dispatch = state.config.dispatch
    opens = breaker.opens + 1

    period =
      min(dispatch.breaker_open_ms * Integer.pow(2, opens - 1), dispatch.breaker_max_open_ms)

    until = mono_ms() + period

    breaker_event(state, job, :open)

    {mod, _opts} = job.spec

    Logger.warning(
      "[ankusa] sink #{inspect(mod)} of source #{inspect(job.env.source_id)} failed " <>
        "#{breaker.failures} time(s) in a row; its deliveries pause for #{period} ms",
      source_id: job.env.source_id,
      tenant_id: job.env.tenant_id,
      sink: mod
    )

    state =
      put_breaker(state, job.key, %{
        breaker
        | state: :open,
          opens: opens,
          until: until,
          probe: nil
      })

    park_queued(state, job.ord, job.key, now_ms() + period)
  end

  defp park_queued(state, ord, key, at) do
    case Map.pop(state.queues, key) do
      {nil, _queues} ->
        state

      {queue, queues} ->
        jobs = :queue.to_list(queue)
        state = drop_key(state, ord, key)
        state = %{state | queues: queues, queued: state.queued - length(jobs)}
        Enum.reduce(jobs, state, &park(&2, &1, at))
    end
  end

  # `key` has no queued job any more: it leaves its tenant's keys, and the
  # tenant the ring of tenants once it has none.
  defp drop_key(state, ord, key) do
    case state.rings do
      %{^ord => keys} ->
        keys = :queue.delete(key, keys)

        if :queue.is_empty(keys),
          do: %{state | rings: Map.delete(state.rings, ord), ring: :queue.delete(ord, state.ring)},
          else: %{state | rings: Map.put(state.rings, ord, keys)}

      _ ->
        state
    end
  end

  defp breaker_event(state, job, to) do
    {mod, _opts} = job.spec

    Telemetry.emit([:dispatch, :breaker], %{}, %{
      instance: state.instance,
      sink: mod,
      source_id: job.env.source_id,
      tenant_id: Tenants.normalize(job.env.tenant_id),
      state: to
    })
  end

  # ── running ───────────────────────────────────────────────────────────────

  # Round-robin over the tenants with queued jobs, and within a tenant over its
  # keys: one job per tenant per turn, so a tenant with a hundred keys gets no
  # more turns than one with a single key. A tenant at its `concurrency`, a key
  # at its `sink_concurrency` or half-open with its probe running is skipped; a
  # full turn that starts nothing stops the loop.
  defp start_jobs(state), do: start_jobs(state, :queue.len(state.ring))

  defp start_jobs(state, 0), do: state

  defp start_jobs(state, budget) do
    if state.running_counted >= state.config.dispatch.concurrency do
      state
    else
      case :queue.out(state.ring) do
        {:empty, _ring} ->
          state

        {{:value, ord}, ring} ->
          state = %{state | ring: ring}

          if at_tenant_cap?(state, ord) do
            start_jobs(%{state | ring: :queue.in(ord, ring)}, budget - 1)
          else
            case start_from_tenant(state, ord, :queue.len(Map.fetch!(state.rings, ord))) do
              {:started, state} -> start_jobs(state, :queue.len(state.ring))
              {:none, state} -> start_jobs(state, budget - 1)
            end
          end
      end
    end
  end

  # `ord` is already out of the ring of tenants; `finish_tenant/2` puts it back
  # if it still has a queued key.
  defp start_from_tenant(state, ord, 0), do: {:none, finish_tenant(state, ord)}

  defp start_from_tenant(state, ord, turns) do
    case :queue.out(Map.fetch!(state.rings, ord)) do
      {:empty, _keys} ->
        {:none, finish_tenant(state, ord)}

      {{:value, key}, keys} ->
        state = %{state | rings: Map.put(state.rings, ord, keys)}

        case admit(state, key) do
          {:park, at, state} ->
            state |> park_queued_from_ring(key, at) |> start_from_tenant(ord, turns - 1)

          {:pass, state} ->
            if at_sink_cap?(state, key) do
              rings = Map.put(state.rings, ord, :queue.in(key, keys))
              start_from_tenant(%{state | rings: rings}, ord, turns - 1)
            else
              {:started, state |> start_one(ord, key) |> finish_tenant(ord)}
            end
        end
    end
  end

  defp finish_tenant(state, ord) do
    if :queue.is_empty(Map.fetch!(state.rings, ord)),
      do: %{state | rings: Map.delete(state.rings, ord)},
      else: %{state | ring: :queue.in(ord, state.ring)}
  end

  # `key` is already out of its tenant's keys.
  defp park_queued_from_ring(state, key, at) do
    {queue, queues} = Map.pop(state.queues, key)
    jobs = :queue.to_list(queue)
    state = %{state | queues: queues, queued: state.queued - length(jobs)}
    Enum.reduce(jobs, state, &park(&2, &1, at))
  end

  defp at_sink_cap?(state, key) do
    case state.config.dispatch.sink_concurrency do
      nil -> false
      cap -> Map.get(state.running_by_key, key, 0) >= cap
    end
  end

  defp at_tenant_cap?(state, ord) do
    limits = Map.get(state.tenant_limits, ord, state.default_limits)
    Map.get(state.running_by_tenant, ord, 0) >= limits.concurrency
  end

  # `key` is out of its tenant's keys and has a non-empty queue.
  defp start_one(state, ord, key) do
    {{:value, job}, queue} = :queue.out(Map.fetch!(state.queues, key))

    {queues, rings} =
      if :queue.is_empty(queue),
        do: {Map.delete(state.queues, key), state.rings},
        else:
          {Map.put(state.queues, key, queue), Map.update!(state.rings, ord, &:queue.in(key, &1))}

    # Bind what the task needs *before* building the closure. Reaching into
    # `state.instance`/`state.config` inside it captures the whole state
    # map, so every spawn would copy the queued jobs — thousands of admitted
    # envelopes — into the new process. That copy, not the delivery, was
    # what capped throughput (measured: ~580µs per spawn, 1.5k/s; 46µs and
    # 5.1k/s once hoisted).
    instance = state.instance
    timeout = state.config.dispatch.attempt_timeout_ms

    task =
      Task.Supervisor.async_nolink(state.task_sup, fn ->
        run_job(job, instance)
      end)

    timer = Process.send_after(self(), {:attempt_timeout, task.ref}, timeout)

    # The first job of a half-open key is its probe.
    {probe?, state} =
      case Map.get(state.breakers, key) do
        %{state: :half_open, probe: nil} = breaker ->
          {true, put_breaker(state, key, %{breaker | probe: task.ref})}

        _ ->
          {false, state}
      end

    counted? = not job.batched?

    entry = %{
      job: job,
      task: task,
      timer: timer,
      probe?: probe?,
      counted?: counted?,
      started: System.monotonic_time()
    }

    %{
      state
      | queues: queues,
        rings: rings,
        queued: state.queued - 1,
        running: Map.put(state.running, task.ref, entry)
    }
    |> count(job, counted?)
  end

  # Runs in the task. Returns `{result, fresh_claim}` and never raises or
  # touches the store — the pipeline process owns every row transition.
  defp run_job(%{env: env, spec: {mod, opts}} = job, instance) do
    # One `ankusa.dispatch.attempt` span per attempt, a child of the hook's stored
    # trace and linked to it; the claim-check write (`ensure_claim/2`) is inside.
    Sink.attempt(mod, env, job.row.attempts + 1, Map.get(job.row, :replay), fn ->
      case ensure_claim(job, instance) do
        {:ok, claim, fresh} ->
          result = Sink.safe_deliver(mod, env, ctx(job, instance, claim), opts)

          {result, fresh}

        {:error, reason} ->
          {{:error, reason}, nil}
      end
    end)
  end

  # A job that has no ref yet but needs one (its pack failed) checks its body in
  # here, once; the new ref is reported back and persisted with the outcome. The
  # sink's threshold callback is user code: evaluated here, in the task, its
  # failure is this attempt's failure (retry policy, DLQ), never the pipeline's.
  defp ensure_claim(%{claim: claim}, _instance) when claim != nil, do: {:ok, claim, nil}

  defp ensure_claim(%{spec: {mod, opts}, env: %{size: size} = env}, instance) do
    case Sink.inline_max_bytes(mod, opts) do
      {:ok, max} when is_integer(max) and size > max ->
        case Message.check_in(instance, env) do
          {:ok, claim} -> {:ok, claim, claim}
          {:error, reason} -> {:error, {:claim_check, reason}}
        end

      {:ok, _max} ->
        {:ok, nil, nil}

      {:error, reason} ->
        {:error, {:inline_max_bytes, reason}}
    end
  end

  defp ctx(%{env: env, row: row} = job, instance, claim) do
    ctx = %{
      instance: instance,
      source_id: env.source_id,
      tenant_id: env.tenant_id,
      attempt: row.attempts + 1,
      forward_headers: job.forward_headers,
      max_headers: job.max_headers,
      forward_headers_max_bytes: job.forward_headers_max_bytes
    }

    ctx =
      case Map.get(row, :replay) do
        r when is_binary(r) -> Map.put(ctx, :replay_id, r)
        _ -> ctx
      end

    # A lane head: the key the sink orders by (`"tenant/source"` for the keyless
    # lane) and the lane's id, for logs (ADR 0004, section 6).
    ctx =
      case Map.get(row, :lane) do
        lane when is_binary(lane) ->
          Map.merge(ctx, %{
            ordering_key:
              Ordering.sink_key(Map.get(row, :ordering_key, ""), env.tenant_id, env.source_id),
            lane: Base.encode16(lane, case: :lower)
          })

        _ ->
          ctx
      end

    ctx = if job.batched?, do: Map.put(ctx, :batch, true), else: ctx

    if claim, do: Map.put(ctx, :claim, claim), else: ctx
  end

  # An attempt of a batching sink waits in its destination's batcher for the
  # batch's response, so `sink_concurrency`, a tenant's `concurrency` and the
  # global `concurrency` do not count it (see `c:Ankusa.Sink.batched?/1`): the
  # claimed-row windows bound how many there are, the batcher bounds the requests.
  defp count(state, %{key: key, ord: ord}, true) do
    %{
      state
      | running_counted: state.running_counted + 1,
        running_by_key: Map.update(state.running_by_key, key, 1, &(&1 + 1)),
        running_by_tenant: Map.update(state.running_by_tenant, ord, 1, &(&1 + 1))
    }
  end

  defp count(state, _job, false), do: state

  defp uncount(state, _job, false), do: state

  defp uncount(state, %{key: key, ord: ord}, true) do
    running_by_key =
      case Map.fetch!(state.running_by_key, key) do
        1 -> Map.delete(state.running_by_key, key)
        n -> Map.put(state.running_by_key, key, n - 1)
      end

    running_by_tenant =
      case Map.fetch!(state.running_by_tenant, ord) do
        1 -> Map.delete(state.running_by_tenant, ord)
        n -> Map.put(state.running_by_tenant, ord, n - 1)
      end

    %{
      state
      | running_counted: state.running_counted - 1,
        running_by_key: running_by_key,
        running_by_tenant: running_by_tenant
    }
  end

  # ── outcomes ──────────────────────────────────────────────────────────────

  defp outcome(state, ref, result, fresh_claim) do
    {%{job: job, timer: timer, probe?: probe?, counted?: counted?, started: started}, running} =
      Map.pop!(state.running, ref)

    Process.cancel_timer(timer)

    state =
      %{
        state
        | running: running,
          claimed: state.claimed - 1,
          claimed_bytes: state.claimed_bytes - job.size
      }
      |> uncount(job, counted?)
      |> tenant_sub(job.ord, job.size)

    attempts = job.row.attempts + 1
    now = now_ms()
    claim_ops = if fresh_claim, do: Deliveries.claim_ops(job.seq, job.ord, fresh_claim), else: []
    attempt_ms = Telemetry.elapsed_ms(started)
    {mod, _opts} = job.spec
    tenant_id = Tenants.normalize(job.env.tenant_id)

    # Every attempt, retried or not, so a failing or hung sink shows in the
    # attempt histogram even when the delivery never settles.
    Telemetry.emit([:dispatch, :attempt], %{attempt_ms: attempt_ms}, %{
      instance: state.instance,
      result: attempt_result(result),
      attempt: attempts,
      sink: mod,
      source_id: job.env.source_id,
      tenant_id: tenant_id
    })

    {state, ops, pairs} =
      case result do
        :ok ->
          Telemetry.emit([:dispatch, :stop], stop_measurements(job.env, job.row, attempt_ms), %{
            instance: state.instance,
            result: :ok,
            attempts: attempts,
            sink: mod,
            source_id: job.env.source_id,
            ordered: ordered?(job.row),
            tenant_id: tenant_id
          })

          {%{state | settled: state.settled + 1} |> bump_replay(job.row, :delivered),
           Deliveries.delivered_ops(job.ord, job.seq, job.sink, job.size, job.row),
           [cleared(job.seq, job.sink)]}

        {:error, reason} ->
          failed(state, job, reason, attempts, now, attempt_ms)
      end

    state = trip(state, job, result, probe?)

    state =
      state
      |> record(claim_ops, [])
      |> buffer(ops, pairs)
      |> start_jobs()
      # While outcomes are buffered the store cannot say what is due; the flush
      # asks, once, instead of every outcome asking.
      |> then(fn state -> if state.buffered == 0, do: schedule_next(state), else: state end)

    maybe_reply_waiters(state)
  end

  defp failed(state, job, reason, attempts, now, attempt_ms) do
    row = %{job.row | attempts: attempts}
    class = Sink.classify(reason)
    max_after = state.config.dispatch.max_retry_after_ms

    case RetryPolicy.schedule(job.policy, class, row, now, max_after) do
      {:retry, at} ->
        error = inspect(reason, limit: 50, printable_limit: 4096)
        {state, Deliveries.retry_ops(job.ord, job.seq, job.sink, row, at, error), []}

      :give_up ->
        {mod, _opts} = job.spec
        error = inspect({:sink, mod, reason}, limit: 50, printable_limit: 4096)

        with {:permanent, {:config, _}} <- class do
          Telemetry.emit([:dispatch, :config_error], %{}, %{
            instance: state.instance,
            sink: mod,
            source_id: job.env.source_id,
            tenant_id: Tenants.normalize(job.env.tenant_id)
          })
        end

        state =
          state
          |> settle_dead(job.env, row, mod, attempts, attempt_ms)
          |> bump_replay(row, :dead)

        {state,
         Deliveries.dead_ops(
           :inflight,
           job.ord,
           job.seq,
           job.sink,
           row,
           now,
           {Sink.error_class(class), error},
           job.env
         ), []}
    end
  end

  # Dispatch's own gauges (`Ankusa.Metrics`), once per housekeeping tick.
  defp report_state(state) do
    Telemetry.emit(
      [:dispatch, :state],
      %{
        running: map_size(state.running),
        claimed: state.claimed,
        claimed_bytes: state.claimed_bytes,
        runnable: state.queued,
        breakers_open: breakers_open(state),
        active_tenants: state.tenants_visited
      },
      %{instance: state.instance}
    )

    report_tenants(%{state | tenants_visited: 0})
  end

  @top_tenants 100

  # `ankusa_dispatch_tenant_inflight{tenant}`: the claimed, unfinished rows of
  # the tenants holding the most of them, which is how close each is to its
  # window. A tenant that leaves the top, or drains, is reported once as 0 so its
  # series does not keep a stale value (TEN-7 owns how many series may exist).
  defp report_tenants(state) do
    top =
      state.tenant_claimed
      |> Enum.map(fn {_ord, {rows, _bytes, tenant}} -> {tenant, rows} end)
      |> Enum.sort_by(fn {_tenant, rows} -> rows end, :desc)
      |> Enum.take(@top_tenants)

    current = MapSet.new(top, fn {tenant, _rows} -> tenant end)

    for {tenant, rows} <- top, do: emit_tenant(state, tenant, rows)

    for tenant <- MapSet.difference(state.reported_tenants, current),
        do: emit_tenant(state, tenant, 0)

    %{state | reported_tenants: current}
  end

  defp emit_tenant(state, tenant, rows) do
    Telemetry.emit([:dispatch, :tenant_state], %{inflight: rows}, %{
      instance: state.instance,
      tenant: tenant
    })
  end

  # A delivery belonging to a replay job: counted so the Replayer can report
  # and auto-pause. Rows without a `replay` key are live deliveries and skip.
  defp bump_replay(state, row, kind) do
    case Map.get(row, :replay) do
      r when is_binary(r) ->
        counts = Map.get(state.replay_outcomes, r, %{delivered: 0, dead: 0, parked: 0})
        counts = Map.update!(counts, kind, &(&1 + 1))
        %{state | replay_outcomes: Map.put(state.replay_outcomes, r, counts)}

      _ ->
        state
    end
  end

  # Write a batch. The Pipeline's writes are not synced: losing one means a
  # redelivery, and the next synced commit flushes everything before it.
  defp record(state, [], _pairs), do: state

  defp record(state, ops, pairs) do
    case write(state, ops) do
      {:ok, state} ->
        reclaim(state.instance, pairs)
        state

      {:error, reason} ->
        Logger.warning("[ankusa] dispatch outcome not recorded, retrying: #{inspect(reason)}")
        %{state | unrecorded: state.unrecorded ++ [{ops, pairs}]}
    end
  end

  # A failed reclaim only defers: the marker stays, and `Reclaim.sweep/1` finds it.
  defp reclaim(_instance, []), do: :ok

  defp reclaim(instance, pairs) do
    with {:error, reason} <- Reclaim.run(instance, pairs) do
      Logger.warning("[ankusa] hook reclaim deferred to the next sweep: #{inspect(reason)}")
    end

    :ok
  end

  # A wake carries the stamp of a commit whose rows may be of any tenant and may
  # sit under a cursor: every cursor above it comes down to it. `cursor_max`
  # makes the usual case (no cursor above the stamp) one comparison.
  defp lower_cursors(%{cursor_max: max} = state, at) when at >= max, do: state

  defp lower_cursors(state, at) do
    cursors = Map.new(state.cursors, fn {ord, floor} -> {ord, min(floor, at)} end)
    %{state | cursors: cursors, cursor_max: min(state.cursor_max, at)}
  end

  defp lower_cursor(state, ord, at) do
    case state.cursors do
      %{^ord => floor} when floor > at -> %{state | cursors: Map.put(state.cursors, ord, at)}
      _ -> state
    end
  end

  defp raise_cursor(state, ord, at) do
    floor = max(Map.get(state.cursors, ord, 0), at - @floor_lag_ms)

    %{
      state
      | cursors: Map.put(state.cursors, ord, floor),
        cursor_max: max(state.cursor_max, floor)
    }
  end

  # Every Pipeline write goes through here. A lane head's release is still a
  # placeholder until now (`Lanes.expand/3`): the next member is read in the step
  # that writes the batch, and a batch that fails is rebuilt on the next try. Once
  # it is in, the rows it made due (a retry, an expansion, a promoted member) are
  # visible to scans, so their tenants' cursors must be at or under the earliest of
  # them: a scan that ran while the write was buffered, or failing and waiting for
  # housekeeping, may have raised one past.
  defp write(state, ops) do
    with {:ok, ops} <- Lanes.expand(state.instance, ops, now_ms()),
         :ok <- Store.write(state.instance, Deliveries.coalesce(ops), sync: false) do
      {:ok, lower_cursors_for(state, ops)}
    end
  end

  defp lower_cursors_for(state, ops) do
    Enum.reduce(ops, state, fn op, state ->
      case Deliveries.due_time(op) do
        {ord, at} -> lower_cursor(state, ord, at)
        nil -> state
      end
    end)
  end

  defp refill(%{window_full?: true} = state), do: fill(state)
  defp refill(state), do: state

  defp buffer(state, ops, pairs) do
    state = %{state | buffer: [{ops, pairs} | state.buffer], buffered: state.buffered + 1}

    cond do
      state.buffered >= @flush_entries -> state |> flush() |> refill()
      # A release makes the lane's next hook due: do not make it wait the whole
      # flush window, a lane's throughput is one delivery round trip.
      Enum.any?(ops, &Lanes.placeholder?/1) -> flush_soon(state)
      true -> arm_flush(state)
    end
  end

  defp flush_soon(state) do
    if state.flush_timer, do: Process.cancel_timer(state.flush_timer)
    %{state | flush_timer: Process.send_after(self(), :flush, @lane_flush_ms)}
  end

  defp arm_flush(%{flush_timer: nil} = state),
    do: %{state | flush_timer: Process.send_after(self(), :flush, @flush_ms)}

  defp arm_flush(state), do: state

  # Everything buffered goes out as one batch, in the order it happened (two
  # outcomes may touch one hook), then one reclaim for the lot.
  defp flush(%{buffer: []} = state), do: state

  defp flush(state) do
    if state.flush_timer, do: Process.cancel_timer(state.flush_timer)

    entries = Enum.reverse(state.buffer)
    ops = Enum.flat_map(entries, &elem(&1, 0))
    pairs = Enum.flat_map(entries, &elem(&1, 1))
    state = %{state | buffer: [], buffered: 0, flush_timer: nil}

    case write(state, ops) do
      {:ok, state} ->
        reclaim(state.instance, pairs)
        state

      {:error, reason} ->
        Logger.warning("[ankusa] dispatch outcomes not recorded, retrying: #{inspect(reason)}")
        %{state | unrecorded: state.unrecorded ++ [{ops, pairs}]}
    end
  end

  # ── housekeeping ──────────────────────────────────────────────────────────

  defp flush_unrecorded(%{unrecorded: []} = state), do: state

  defp flush_unrecorded(%{unrecorded: [{ops, pairs} | rest]} = state) do
    case write(state, ops) do
      {:ok, state} ->
        reclaim(state.instance, pairs)
        flush_unrecorded(%{state | unrecorded: rest})

      {:error, _reason} ->
        state
    end
  end

  defp sweep(state) do
    with {:error, reason} <- Reclaim.sweep(state.instance) do
      Logger.warning("[ankusa] hook reclaim sweep failed: #{inspect(reason)}")
    end
  end

  # ── scheduling ────────────────────────────────────────────────────────────

  defp schedule_next(%{holdoff?: true} = state), do: state
  defp schedule_next(%{recovered?: false} = state), do: state
  # `window_full?` is a hint from the last look: outcomes since may have emptied
  # the window, so what decides is whether it is full now. Believing the flag
  # would cancel the wake of a window that has since drained.
  defp schedule_next(%{window_full?: true} = state) do
    if at_capacity?(state),
      do: cancel_timer(state),
      else: schedule_next(%{state | window_full?: false})
  end

  defp schedule_next(state) do
    skip = MapSet.union(state.tenant_full, Gate.state(state.instance).ords)

    case Deliveries.next_due_at(state.instance, state.cursors, skip: skip) do
      {:ok, nil} ->
        cancel_timer(state)

      {:ok, at} ->
        now = now_ms()

        cond do
          at > now ->
            set_timer(state, min(at - now, @max_sleep_ms))

          # Due, but the rows wait for an fsync (the syncer wakes dispatch when it
          # returns; the timer is the fallback when that fsync failed).
          at > Syncer.visible(state.instance, now) ->
            set_timer(state, @unsynced_retry_ms)

          true ->
            send(self(), :wake)
            cancel_timer(state)
        end

      {:error, _reason} ->
        set_timer(state, @holdoff_ms)
    end
  end

  defp set_timer(state, ms) do
    state = cancel_timer(state)
    %{state | timer: Process.send_after(self(), :wake, ms)}
  end

  defp cancel_timer(%{timer: nil} = state), do: state

  defp cancel_timer(%{timer: ref} = state) do
    Process.cancel_timer(ref)
    %{state | timer: nil}
  end

  # ── waiters ───────────────────────────────────────────────────────────────

  defp maybe_reply_waiters(%{waiters: []} = state), do: {:noreply, state}

  defp maybe_reply_waiters(state) do
    if quiet?(state) do
      # Nothing is running: record what is buffered before saying so.
      state = state |> flush() |> schedule_next()

      if idle?(state) do
        Enum.each(state.waiters, fn {from, settled0} ->
          GenServer.reply(from, {:ok, state.settled - settled0})
        end)

        {:noreply, %{state | waiters: []}}
      else
        {:noreply, state}
      end
    else
      {:noreply, state}
    end
  end

  defp quiet?(state) do
    state.recovered? and state.claimed == 0 and map_size(state.running) == 0 and
      map_size(state.packing) == 0 and state.queued == 0
  end

  defp idle?(state), do: quiet?(state) and state.buffered == 0 and not due_now?(state)

  # A store that cannot answer must not hang `tick/1` callers forever.
  defp due_now?(state) do
    skip = Gate.state(state.instance).ords

    case Deliveries.next_due_at(state.instance, state.cursors, skip: skip) do
      {:ok, at} when is_integer(at) -> at <= now_ms()
      _ -> false
    end
  end

  defp now_ms, do: System.system_time(:millisecond)
  defp mono_ms, do: System.monotonic_time(:millisecond)
end
