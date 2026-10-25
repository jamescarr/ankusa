defmodule Ankusa.Audit do
  @moduledoc """
  The audit trail: one row for every change an operator (or code) makes to the
  node's configuration or operational state, written with the change.

  ## What is recorded

  A row says who (`actor`, `remote_ip`, `request_id`, from `Ankusa.Audit.Context`),
  what (`action`, `tenant`, `resource`, `detail`), and what it did to the
  subject: `before_hash` and `after_hash`, an HMAC-SHA256 of the subject before
  and after, `revision` (the subject's own change counter, where it has one) and
  `result`. **A row never holds a secret or a spec**: a hash is keyed with a
  random per-store key (`m:audit_key`), so it shows that a source's secret
  changed without giving anyone with `audit:read` a way to test a guess
  against it, and `detail` carries only what the caller puts there (counts,
  filters, a rate).

  | action | change |
  | --- | --- |
  | `source.create`, `source.update`, `source.delete` | an API-managed source |
  | `rate_limit.put`, `rate_limit.delete` | a tenant's rate-limit override |
  | `quarantine.purge` | held hooks deleted |
  | `replay.create`, `replay.update`, `replay.cancel` | a replay job |
  | `route.create`, `route.update`, `route.delete` | a route definition |
  | `tenant.delete` | a tenant erase (`Ankusa.Tenant`): one row, with what each stage removed as its `detail`; the erase's own source, route and rate-limit deletions are rows of their own |
  | `ip_rules.put` | the global IP rules |
  | `log_level.put` | the node's log level (`PUT /v1/log-level`): `detail` has the new `level` and the `previous` one |

  A request that changes nothing (a validation error, a missing subject) writes
  no row; one that is refused by the store writes none either.

  ## Order and durability

  Rows live in the store's `audit` column family under
  `<<?a, seq::64, at::64>>`. This process hands out `seq` **and commits the
  batch**, so key order is commit order whatever the wall clock does, and a
  reader that remembers the last `seq` it saw (SEC-2.b's lifecycle outbox) can
  never miss a row that lands later with a smaller key.

  There are two ways in:

    * `commit/4` is for a change that lives in this node's `Ankusa.Store`
      (API-managed sources, rate-limit overrides, quarantine, replay jobs): the
      change's own ops and the audit row go in **one** `sync: true` batch.
      Either both survive a `kill -9` or neither does.

    * `begin/3` + `finish/3` (or `record/4`) is for a change that lives
      somewhere else (route definitions, whose store is pluggable: ETS, or
      Redis shared by a fleet; the Redis source store). That is a dual write,
      and there is no transaction across the two. The row is written first, as
      a *pending* row (`<<?p, seq::64>>`, synced) that says what is about to
      happen; the change is applied; `finish/3` then moves it into the trail
      with the outcome, in one batch. A crash in between leaves the pending row,
      and the next boot (or the caller dying, while this node runs) settles it
      as `result: "interrupted"`: *the change may or may not have been
      applied*; `after_hash` is the hash of what was about to be written. For a
      source or route, `Ankusa.Lifecycle.reconcile/2` does better than that: it
      reads the subject's present state and compares its hash to the row's
      `after_hash` (the change was applied: `"ok"`) and `before_hash` (it was
      not: `"failed"`); a state that is neither (someone else changed it
      meanwhile) stays `"interrupted"` and is flagged for a state-refresh
      event. A boot has to wait for the subject's store to load, so such rows
      are settled by a tick (every second, for a minute, then as
      `"interrupted"`). The trail never loses a change that was applied; it can
      show one that was not only as `"interrupted"`.

  A change whose audit row cannot be written is not made: the caller gets
  `{:error, :store_unavailable}`.

  ## The lifecycle outbox

  When lifecycle events are on (`Ankusa.Lifecycle`), the row of a source or
  route change that took effect is flagged `publish: true`, and the redacted
  entity its event carries is written beside it at `<<?o, seq::64>>` in the
  same batch. The entity is not in the row: a row never holds a spec, and even
  redacted an entity is one. `Ankusa.Lifecycle.Publisher` reads the flagged
  rows in `seq` order with `page/3` and keeps its place in `m:lifecycle_cursor`.

  ## Restores and event identity

  A store that came back from a backup (`Ankusa.Store` leaves
  `m:incarnation_pending`) re-issues the `seq` numbers of every change the lost
  one made after the backup, and some of those were already published. So the
  first thing this process does on such a boot is open a new *incarnation*:
  one history entry `{from seq, fresh id}` in `m:incarnation`, written with an
  `instance.restore` row (event `io.ankusa.instance.restored`, carrying the
  incarnation and the restored `seq`) in one synced batch, before any change is
  taken. `incarnation/2` tells `Ankusa.Lifecycle` which id a row's event id is
  keyed with: rows of the restored trail keep the ids they were published
  under, every row from the new incarnation on gets a different one.


  ## Retention

  `audit.retention_days` (default 365). An hourly sweep deletes rows (and the
  lifecycle entities beside them) older than that, oldest first, in bounded
  batches, and emits `[:ankusa, :audit, :pruned]`. While `lifecycle.sinks` is
  set it never deletes a row at or above `m:lifecycle_cursor`, however old: a
  lifecycle sink that is down longer than the retention keeps the events it has
  not seen. The expired rows it holds back are emitted as `blocked_by_cursor` on
  `[:ankusa, :audit, :retention]` after every sweep. Every committed row emits
  `[:ankusa, :audit]`.

  ## Ceilings

  The trail is bounded in `audit.max_rows` and `audit.max_bytes`, from counters
  kept in every batch (`m:audit_usage`; recounted at boot when it does not match
  the trail). Before a change is accepted the trail is pruned to them, oldest
  first and never past `m:lifecycle_cursor`; a trail that is still over a ceiling,
  or a lifecycle outbox holding `audit.outbox_max_backlog` or more events no sink
  has confirmed (`m:audit_seq` minus the cursor), refuses the *new* change with
  `{:error, :audit_backlog}` (`503 audit_backlog`, `Retry-After: 60` on the admin
  APIs). An unpublished row is never dropped to make room, and a change already
  begun is always recorded. A change the node makes itself (`Context.system/0`) is
  not refused.
  """

  use GenServer

  require Logger

  alias Ankusa.Audit.Context
  alias Ankusa.Lifecycle
  alias Ankusa.Lifecycle.Publisher
  alias Ankusa.Store
  alias Ankusa.Store.Keys
  alias Ankusa.Telemetry

  @row_version 1
  @call_timeout 15_000

  @prune_interval_ms 3_600_000
  @prune_batch 1_000
  @prune_batches 50
  @day_ms 86_400_000

  # Rows one `list/2` call looks at before it hands back a cursor, so a filter
  # that matches nothing costs a bounded scan, not the whole trail.
  @scan_budget 10_000
  # `since` stops a scan once rows are this much older: rows are in commit
  # order, and the wall clock may step back a little between two of them.
  @skew_ms 300_000
  @default_limit 100
  @max_limit 1_000

  @updatable [
    :result,
    :before_hash,
    :after_hash,
    :revision,
    :detail,
    :tenant,
    :resource,
    :entity,
    :publish,
    :event
  ]

  # How often, and for how long after boot, a pending row of a source or route
  # is reconciled against the subject's store, which starts after this process.
  @reconcile_tick_ms 1_000
  @reconcile_ms 60_000

  @typedoc "What a caller says about its change. `action` is required."
  @type attrs :: %{
          required(:action) => String.t(),
          optional(:tenant) => String.t() | nil,
          optional(:resource) => String.t() | nil,
          optional(:before_hash) => String.t() | nil,
          optional(:after_hash) => String.t() | nil,
          optional(:revision) => pos_integer() | nil,
          optional(:detail) => map() | nil,
          optional(:result) => String.t(),
          optional(:entity) => map() | nil
        }

  @type row :: %{required(atom()) => term()}

  # ── lifecycle ─────────────────────────────────────────────────────────────

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :audit))
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance)},
      start: {__MODULE__, :start_link, [opts]},
      shutdown: 15_000
    }
  end

  # ── writing ───────────────────────────────────────────────────────────────

  @doc """
  Commit `ops` (the change, for the node's own `Ankusa.Store`) and its audit row
  in one synced batch.
  """
  @spec commit(atom(), Context.t(), attrs(), [Store.op()]) ::
          :ok | {:error, :store_unavailable | :audit_backlog}
  def commit(instance, %Context{} = context, %{action: _} = attrs, ops) when is_list(ops),
    do: call(instance, {:commit, context, attrs, ops})

  @doc """
  Write the pending row of a change that is about to be made elsewhere. Returns
  the handle `finish/3` takes. The caller is monitored: if it dies first, the
  row is finished as `"interrupted"`.
  """
  @spec begin(atom(), Context.t(), attrs()) ::
          {:ok, pos_integer()} | {:error, :store_unavailable | :audit_backlog}
  def begin(instance, %Context{} = context, %{action: _} = attrs),
    do: call(instance, {:begin, context, attrs})

  @doc """
  Move a pending row into the trail. `update` may set `:result` (`"ok"`,
  `"failed"`), `:after_hash`, `:before_hash`, `:revision`, `:detail`, `:tenant`
  and `:resource`; the row keeps what `begin/3` recorded for anything left out.
  """
  @spec finish(atom(), pos_integer(), map()) :: :ok | {:error, term()}
  def finish(instance, pending, update) when is_integer(pending) and is_map(update),
    do: call(instance, {:finish, pending, update})

  @doc """
  Drop a pending row whose change turned out to be no change at all (the store
  rejected the request): no row reaches the trail.
  """
  @spec discard(atom(), pos_integer()) :: :ok | {:error, term()}
  def discard(instance, pending) when is_integer(pending),
    do: call(instance, {:discard, pending})

  @doc """
  Run `fun`, the change itself, between a pending row and its finish. `fun`
  returns `{reply, update}` (see `finish/3`), or `{reply, :discard}` when it
  changed nothing (see `discard/2`); `record/4` returns `reply`, or
  `{:error, :store_unavailable}` without running `fun` when the pending row
  could not be written. A `fun` that raises, throws or exits leaves the row
  `"interrupted"`.
  """
  @spec record(atom(), Context.t(), attrs(), (-> {reply, map() | :discard})) ::
          reply | {:error, :store_unavailable | :audit_backlog}
        when reply: term()
  def record(instance, %Context{} = context, attrs, fun) when is_function(fun, 0) do
    with {:ok, pending} <- begin(instance, context, attrs) do
      {reply, update} =
        try do
          fun.()
        catch
          kind, reason ->
            _ = finish(instance, pending, %{result: "interrupted"})
            :erlang.raise(kind, reason, __STACKTRACE__)
        end

      settled =
        if update == :discard,
          do: discard(instance, pending),
          else: finish(instance, pending, update)

      with {:error, reason} <- settled do
        Logger.error(
          "[ankusa] the audit row of #{attrs.action} could not be settled: " <>
            "#{inspect(reason)}; it stays pending and is recorded as interrupted"
        )
      end

      reply
    end
  end

  @doc """
  The keyed hash of `term` an audit row carries (`nil` for `nil`). Computed by
  the caller from the key in an ETS table, so a secret never travels to this
  process in a message that a crash report could print.
  """
  @spec digest(atom(), term()) :: String.t() | nil
  def digest(_instance, nil), do: nil

  def digest(instance, term) do
    case :ets.lookup(table(instance), :key) do
      [{:key, key}] ->
        :hmac
        |> :crypto.mac(:sha256, key, :erlang.term_to_binary(term, [:deterministic]))
        |> Base.encode16(case: :lower)

      [] ->
        nil
    end
  rescue
    ArgumentError -> nil
  end

  # ── reading ───────────────────────────────────────────────────────────────

  @doc """
  The trail, newest first.

  Options: `:since` / `:until` (inclusive bounds on `at`, ms), `:actor`,
  `:action`, `:tenant` (exact matches), `:tenants` (`:all`, or the tenants the
  reader may see: rows with no tenant need `:all`), `:cursor` (the
  `next_cursor` of the previous page) and `:limit` (default 100, at most 1000).
  `next_cursor` is `nil` on the last page; a page can be short when
  the scan budget (10 000 rows) ran out, and `next_cursor` says to go on.
  """
  @spec list(atom(), keyword()) ::
          {:ok, %{entries: [row()], next_cursor: String.t() | nil}} | {:error, term()}
  def list(instance, opts \\ []) do
    %{lo: lo, hi: hi} = Keys.family(:audit)
    limit = opts |> Keyword.get(:limit, @default_limit) |> max(1) |> min(@max_limit)

    upper =
      case Keyword.get(opts, :cursor) do
        nil -> hi
        seq when is_integer(seq) and seq > 0 -> <<?a, seq::64>>
      end

    filters = %{
      since: opts[:since],
      until: opts[:until],
      actor: opts[:actor],
      action: opts[:action],
      tenant: opts[:tenant],
      tenants: Keyword.get(opts, :tenants, :all),
      limit: limit
    }

    scan = %{entries: [], count: 0, examined: 0, cursor: nil, more: false, error: nil}

    case Store.fold(
           instance,
           :audit,
           {lo, upper},
           scan,
           fn key, value, acc -> scan_row(key, value, acc, filters) end,
           reverse: true
         ) do
      {:ok, %{error: nil} = done} ->
        {:ok,
         %{
           entries: Enum.reverse(done.entries),
           next_cursor: if(done.more, do: Integer.to_string(done.cursor))
         }}

      {:ok, %{error: error}} ->
        {:error, error}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Rows after `after_seq`, oldest first: at most `max_rows` of them, as
  `{seq, {:ok, row} | {:error, reason}}` (a row that cannot be decoded is
  reported, not skipped, so the reader decides). The lifecycle outbox
  (`Ankusa.Lifecycle.Publisher`) and its replay read the trail this way: `seq`
  is commit order, so a reader that keeps the last one it handled cannot miss
  a row.
  """
  @spec page(atom(), non_neg_integer(), pos_integer()) ::
          {:ok, [{pos_integer(), {:ok, row()} | {:error, term()}}]} | {:error, term()}
  def page(instance, after_seq, max_rows) when is_integer(after_seq) and after_seq >= 0 do
    %{hi: hi} = Keys.family(:audit)

    result =
      Store.fold(
        instance,
        :audit,
        {<<?a, after_seq + 1::64>>, hi},
        {[], 0},
        fn key, value, {rows, count} ->
          {seq, _at} = Keys.decode_audit(key)
          acc = {[{seq, decode_row(value)} | rows], count + 1}
          if count + 1 >= max_rows, do: {:halt, acc}, else: {:cont, acc}
        end
      )

    with {:ok, {rows, _count}} <- result, do: {:ok, Enum.reverse(rows)}
  end

  @doc "The last `seq` the trail handed out (`0` for an empty trail)."
  @spec last_seq(atom()) :: {:ok, non_neg_integer()} | {:error, term()}
  def last_seq(instance), do: load_seq(instance)

  @doc """
  Whether a control-plane change is refused right now (`503 audit_backlog`,
  `Retry-After: 60`): the trail holds more than `audit.max_rows` rows or
  `audit.max_bytes` bytes (and pruning it to its ceilings, which the trail is then
  asked to do, may not pass the lifecycle cursor), or the lifecycle outbox holds
  `audit.outbox_max_backlog` events or more that no sink has confirmed. Read
  from counters, never a scan; `false` where no trail runs.
  """
  @spec refusing?(atom()) :: boolean()
  def refusing?(instance) do
    audit = Ankusa.config(instance).audit
    %{rows: rows, bytes: bytes, outbox_backlog: backlog} = stats(instance)

    cond do
      rows > audit.max_rows or bytes > audit.max_bytes ->
        GenServer.cast(Ankusa.via(instance, :audit), :make_room)
        true

      backlog != nil ->
        backlog >= audit.outbox_max_backlog

      true ->
        false
    end
  end

  @doc """
  The trail's size and the lifecycle outbox's lag, from counters: `rows`,
  `bytes`, and `outbox_backlog`, the audit `seq`s the lifecycle publisher has not
  yet confirmed (`nil` without lifecycle sinks, or before the publisher wrote
  its cursor). All zero where no trail runs.
  """
  @spec stats(atom()) :: %{
          rows: non_neg_integer(),
          bytes: non_neg_integer(),
          outbox_backlog: non_neg_integer() | nil
        }
  def stats(instance) do
    with [{:usage, rows, bytes}] <- :ets.lookup(table(instance), :usage),
         [{:last_seq, last}] <- :ets.lookup(table(instance), :last_seq) do
      %{rows: rows, bytes: bytes, outbox_backlog: backlog(instance, last)}
    else
      _ -> %{rows: 0, bytes: 0, outbox_backlog: nil}
    end
  rescue
    ArgumentError -> %{rows: 0, bytes: 0, outbox_backlog: nil}
  end

  defp backlog(instance, last_seq) do
    if Ankusa.config(instance).lifecycle.sinks == [] do
      nil
    else
      case Store.get(instance, :default, Keys.lifecycle_cursor()) do
        {:ok, <<cursor::64>>} -> max(last_seq - cursor, 0)
        _ -> nil
      end
    end
  end

  @doc """
  The incarnation the audit row `seq` was written in: the id of the newest
  restore at or before it, `nil` for a store that was never restored from a
  backup (and for the rows before its first restore).
  """
  @spec incarnation(atom(), pos_integer()) :: String.t() | nil
  def incarnation(instance, seq) do
    case :ets.lookup(table(instance), :incarnations) do
      [{:incarnations, history}] ->
        Enum.find_value(history, fn %{"from" => from, "id" => id} -> if seq >= from, do: id end)

      [] ->
        nil
    end
  rescue
    ArgumentError -> nil
  end

  @doc """
  The redacted entity of the row `seq` (a row flagged `publish: true`), or
  `:not_found` when it was pruned.
  """
  @spec entity(atom(), pos_integer()) :: {:ok, map()} | :not_found | {:error, term()}
  def entity(instance, seq) when is_integer(seq) do
    case Store.get(instance, :audit, Keys.lifecycle_entity(seq)) do
      {:ok, json} ->
        case JSON.decode(json) do
          {:ok, %{} = entity} -> {:ok, entity}
          _other -> {:error, :undecodable}
        end

      :not_found ->
        :not_found

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp scan_row(key, value, acc, filters) do
    {seq, _at} = Keys.decode_audit(key)
    acc = %{acc | examined: acc.examined + 1}

    case decode_row(value) do
      {:ok, row} -> scan_decoded(row, seq, acc, filters)
      {:error, reason} -> {:halt, %{acc | error: {:corrupt_audit_row, seq, reason}}}
    end
  end

  defp scan_decoded(row, seq, acc, filters) do
    cond do
      too_old?(row, filters.since) ->
        {:halt, acc}

      not row_matches?(row, filters) ->
        consumed(acc, seq)

      acc.count == filters.limit ->
        {:halt, %{acc | more: true}}

      true ->
        acc = %{acc | entries: [row | acc.entries], count: acc.count + 1}
        consumed(acc, seq)
    end
  end

  # A row is consumed once the cursor may move past it; the budget is only
  # checked there, so a page never ends between a row and its cursor.
  defp consumed(acc, seq) do
    acc = %{acc | cursor: seq}
    if acc.examined >= @scan_budget, do: {:halt, %{acc | more: true}}, else: {:cont, acc}
  end

  defp too_old?(_row, nil), do: false
  defp too_old?(row, since), do: Map.get(row, :at, 0) < since - @skew_ms

  defp row_matches?(row, filters) do
    at = Map.get(row, :at, 0)

    (is_nil(filters.since) or at >= filters.since) and
      (is_nil(filters.until) or at <= filters.until) and
      (is_nil(filters.actor) or Map.get(row, :actor) == filters.actor) and
      (is_nil(filters.action) or Map.get(row, :action) == filters.action) and
      (is_nil(filters.tenant) or Map.get(row, :tenant) == filters.tenant) and
      visible?(Map.get(row, :tenant), filters.tenants)
  end

  defp visible?(_tenant, :all), do: true
  defp visible?(nil, _tenants), do: false
  defp visible?(tenant, tenants), do: tenant in tenants

  @doc "The JSON view of a row, as `GET /v1/audit` returns it."
  @spec to_json(row()) :: map()
  def to_json(row) do
    %{
      "seq" => row.seq,
      "at" => row.at |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601(),
      "actor" => Map.get(row, :actor),
      "remote_ip" => Map.get(row, :remote_ip),
      "action" => Map.get(row, :action),
      "tenant" => Map.get(row, :tenant),
      "resource" => Map.get(row, :resource),
      "before_hash" => Map.get(row, :before_hash),
      "after_hash" => Map.get(row, :after_hash),
      "request_id" => Map.get(row, :request_id),
      "result" => Map.get(row, :result),
      "revision" => Map.get(row, :revision),
      "detail" => Map.get(row, :detail)
    }
  end

  # ── row encoding ──────────────────────────────────────────────────────────

  # Versioned like every persisted format (ground rule 9): `<<"AA", 1, term>>`.
  # A row is a plain map, so a field a newer node added passes through reads and
  # is ignored.
  defp encode(row), do: <<"AA", @row_version, :erlang.term_to_binary(row)::binary>>

  @doc false
  @spec decode_row(binary()) ::
          {:ok, row()} | {:error, :unsupported_audit_row_version | :undecodable}
  def decode_row(<<"AA", @row_version, term::binary>>), do: decode_term(term)
  def decode_row(<<"AA", _version, _term::binary>>), do: {:error, :unsupported_audit_row_version}
  def decode_row(_other), do: {:error, :undecodable}

  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp decode_term(term) do
    case :erlang.binary_to_term(term) do
      %{seq: _, at: _} = row -> {:ok, row}
      _ -> {:error, :undecodable}
    end
  rescue
    ArgumentError -> {:error, :undecodable}
  end

  # ── maintenance ───────────────────────────────────────────────────────────

  @doc """
  Delete the rows older than `audit.retention_days`, oldest first. Runs on the
  hourly tick; callable on its own. `{:ok, deleted}`.
  """
  @spec prune(atom()) :: {:ok, non_neg_integer()} | {:error, term()}
  def prune(instance), do: call(instance, :prune)

  # ── GenServer ─────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    prune_interval = Keyword.get(opts, :prune_interval_ms, @prune_interval_ms)

    with {:ok, key} <- load_key(instance),
         {:ok, last_seq} <- load_seq(instance),
         {:ok, history} <- load_incarnations(instance),
         {:ok, restored} <- load_incarnation_pending(instance),
         {:ok, leftovers} <- load_pending(instance),
         {:ok, {rows, bytes, counted}} <- load_usage(instance) do
      :ets.new(table(instance), [:named_table, :protected, read_concurrency: true])
      true = :ets.insert(table(instance), [{:key, key}, {:incarnations, history}])

      # A source or a route is settled once its store has loaded (see
      # `handle_info(:reconcile, _)`); everything else is settled now.
      {deferred, now} =
        Enum.split_with(leftovers, fn {_seq, row} -> Lifecycle.reconcilable?(row) end)

      state =
        publish_usage(%{
          instance: instance,
          next_seq: last_seq + 1,
          pending: %{},
          prune_interval: prune_interval,
          leftovers: deferred,
          reconcile_until: System.monotonic_time(:millisecond) + @reconcile_ms,
          rows: rows,
          bytes: bytes,
          counted: counted
        })

      # The restore row goes first: it takes `last_seq + 1`, the first seq of
      # the new incarnation, before any leftover is settled or change taken.
      with {:ok, state} <- announce_restore(state, restored, history, last_seq),
           {:ok, state} <- resolve_leftovers(state, now) do
        Process.send_after(self(), :prune, prune_interval)
        if deferred != [], do: Process.send_after(self(), :reconcile, @reconcile_tick_ms)
        {:ok, state}
      else
        {:error, reason} -> {:stop, {:audit_recovery_failed, reason}}
      end
    else
      {:error, reason} -> {:stop, {:audit_open_failed, reason}}
    end
  end

  # A crash report prints the last message: a commit carries the change's own
  # ops, which hold a source's secrets.
  @impl true
  def format_status(status) do
    Map.new(status, fn
      {:message, {:commit, context, attrs, _ops}} ->
        {:message, {:commit, context, attrs, :redacted}}

      other ->
        other
    end)
  end

  @impl true
  def handle_call({:commit, context, attrs, ops}, _from, state) do
    case ensure_room(state, context) do
      {:ok, state} ->
        case commit_row(state, context, attrs, ops) do
          {:ok, state} ->
            {:reply, :ok, state}

          {:error, reason} ->
            Logger.error("[ankusa] could not commit #{attrs.action}: #{inspect(reason)}")
            {:reply, {:error, :store_unavailable}, state}
        end

      {{:error, :audit_backlog} = refused, state} ->
        {:reply, refused, state}
    end
  end

  def handle_call({:begin, context, attrs}, {pid, _tag}, state) do
    case ensure_room(state, context) do
      {:ok, state} -> begin_row(state, context, attrs, pid)
      {{:error, :audit_backlog} = refused, state} -> {:reply, refused, state}
    end
  end

  def handle_call({:finish, seq, update}, _from, state) do
    case Map.pop(state.pending, seq) do
      {nil, _pending} ->
        {:reply, {:error, :not_pending}, state}

      {%{ref: ref} = entry, rest} ->
        Process.demonitor(ref, [:flush])
        state = %{state | pending: rest}

        # A caller that says "interrupted" does not know what its change did:
        # the subject's state does.
        update =
          if Map.get(update, :result) == "interrupted",
            do: settle_interrupted(state.instance, entry.row, update),
            else: update

        # A failed write leaves the row pending on disk; the next boot finishes it.
        case finalize(state, seq, entry.row, update) do
          {:ok, state} -> {:reply, :ok, state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:discard, seq}, _from, state) do
    case Map.pop(state.pending, seq) do
      {nil, _pending} ->
        {:reply, {:error, :not_pending}, state}

      {%{ref: ref}, rest} ->
        Process.demonitor(ref, [:flush])
        state = %{state | pending: rest}

        case Store.write(state.instance, [{:delete, :audit, Keys.audit_pending(seq)}], sync: true) do
          :ok -> {:reply, :ok, state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call(:prune, _from, state) do
    {result, state} = do_prune(state)
    {:reply, result, state}
  end

  # A mutation was refused for want of room (`refusing?/1`): prune to the
  # ceilings now, so the retry finds it.
  @impl true
  def handle_cast(:make_room, state) do
    config = Ankusa.config(state.instance)

    state =
      with {:ok, floor} <- outbox_floor(state.instance, config),
           true <- over?(state, config.audit) do
        prune_to_ceiling(state, config.audit, floor)
      else
        _ -> state
      end

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Enum.find(state.pending, fn {_seq, entry} -> entry.ref == ref end) do
      nil ->
        {:noreply, state}

      {seq, entry} ->
        state = %{state | pending: Map.delete(state.pending, seq)}

        update = settle_interrupted(state.instance, entry.row, %{result: "interrupted"})

        case finalize(state, seq, entry.row, update) do
          {:ok, state} ->
            {:noreply, state}

          # The row stays pending on disk: the next boot finishes it.
          {:error, _reason} ->
            {:noreply, state}
        end
    end
  end

  def handle_info(:prune, state) do
    {_result, state} = do_prune(state)
    Process.send_after(self(), :prune, state.prune_interval)
    {:noreply, state}
  end

  # The boot's pending rows of sources and routes: settle each as soon as its
  # store has loaded, or as "interrupted" once the minute is up.
  def handle_info(:reconcile, state) do
    expired? = System.monotonic_time(:millisecond) >= state.reconcile_until

    {left, state} =
      Enum.reduce(state.leftovers, {[], state}, fn {seq, row} = leftover, {left, state} ->
        case reconcile_update(state.instance, row, expired?) do
          nil ->
            {[leftover | left], state}

          update ->
            case finalize(state, seq, row, update) do
              {:ok, state} -> {left, state}
              {:error, _reason} -> {[leftover | left], state}
            end
        end
      end)

    state = %{state | leftovers: Enum.reverse(left)}
    if state.leftovers != [], do: Process.send_after(self(), :reconcile, @reconcile_tick_ms)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # ── internals ─────────────────────────────────────────────────────────────

  defp call(instance, message) do
    GenServer.call(Ankusa.via(instance, :audit), message, @call_timeout)
  catch
    :exit, _reason -> {:error, :store_unavailable}
  end

  defp begin_row(state, context, attrs, pid) do
    seq = state.next_seq
    row = row(context, attrs, seq)
    batch = [{:put, :audit, Keys.audit_pending(seq), encode(row)}, seq_op(seq)]

    case Store.write(state.instance, batch, sync: true) do
      :ok ->
        ref = Process.monitor(pid)
        pending = Map.put(state.pending, seq, %{row: row, ref: ref})
        {:reply, {:ok, seq}, publish_usage(%{state | next_seq: seq + 1, pending: pending})}

      {:error, reason} ->
        Logger.error("[ankusa] could not record #{attrs.action}: #{inspect(reason)}")
        {:reply, {:error, :store_unavailable}, state}
    end
  end

  # The pending row is deleted and its final row written in one batch.
  defp finalize(state, pending_seq, row, update) do
    seq = state.next_seq
    at = System.system_time(:millisecond)
    merged = row |> Map.merge(Map.take(update, @updatable)) |> Map.merge(%{seq: seq, at: at})
    {final, entity_ops} = split_entity(merged)
    encoded = encode(final)
    counted = counted(state, seq, byte_size(encoded) + entity_bytes(entity_ops))

    batch =
      [
        {:delete, :audit, Keys.audit_pending(pending_seq)},
        {:put, :audit, Keys.audit(seq, at), encoded},
        seq_op(seq),
        usage_op(counted)
      ] ++ entity_ops

    case Store.write(state.instance, batch, sync: true) do
      :ok ->
        emit(state.instance, final)
        {:ok, publish_usage(%{counted | next_seq: seq + 1})}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # The row as it is stored, and the op that stores its lifecycle entity beside
  # it: only a row that is flagged for publication has one.
  defp split_entity(row) do
    {entity, row} = Map.pop(row, :entity)
    publish? = Map.get(row, :publish, false) == true and is_map(entity)

    ops =
      if publish?,
        do: [{:put, :audit, Keys.lifecycle_entity(row.seq), JSON.encode!(entity)}],
        else: []

    {Map.put(row, :publish, publish?), ops}
  end

  # What a pending row of a source or route turned out to be, from the
  # subject's present state; `update` when that cannot be read.
  defp settle_interrupted(instance, row, update) do
    case reconcile(instance, row) do
      {:ok, settled} -> settled
      _unknown -> update
    end
  end

  defp reconcile_update(instance, row, expired?) do
    case reconcile(instance, row) do
      {:ok, update} ->
        update

      :not_ready when expired? ->
        Logger.error(
          "[ankusa] the store of #{row.action} #{row.resource} did not load in time; " <>
            "recording the change as interrupted, with no lifecycle event"
        )

        %{result: "interrupted"}

      _not_ready ->
        nil
    end
  end

  # Reading a subject must never take the trail down with it.
  defp reconcile(instance, row) do
    Lifecycle.reconcile(instance, row)
  rescue
    _error -> :not_ready
  catch
    :exit, _reason -> :not_ready
  end

  defp row(context, attrs, seq) do
    %{
      seq: seq,
      at: System.system_time(:millisecond),
      actor: context.actor,
      remote_ip: context.remote_ip,
      request_id: context.request_id,
      action: attrs.action,
      tenant: Map.get(attrs, :tenant),
      resource: Map.get(attrs, :resource),
      before_hash: Map.get(attrs, :before_hash),
      after_hash: Map.get(attrs, :after_hash),
      result: Map.get(attrs, :result, "ok"),
      revision: Map.get(attrs, :revision),
      detail: Map.get(attrs, :detail),
      publish: false,
      entity: Map.get(attrs, :entity)
    }
  end

  defp seq_op(seq), do: {:put, :default, Keys.audit_seq(), <<seq::64>>}

  defp emit(instance, row) do
    Telemetry.emit([:audit], %{count: 1}, %{
      instance: instance,
      seq: row.seq,
      action: row.action,
      actor: row.actor,
      tenant: row.tenant,
      resource: row.resource,
      result: row.result
    })

    if row.publish, do: Publisher.wake(instance)
  end

  defp load_key(instance) do
    case Store.get(instance, :default, Keys.audit_key()) do
      {:ok, key} ->
        {:ok, key}

      :not_found ->
        key = :crypto.strong_rand_bytes(32)

        with :ok <- Store.write(instance, [{:put, :default, Keys.audit_key(), key}], sync: true),
             do: {:ok, key}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp load_seq(instance) do
    case Store.get(instance, :default, Keys.audit_seq()) do
      {:ok, <<seq::64>>} -> {:ok, seq}
      :not_found -> {:ok, 0}
      {:ok, other} -> {:error, {:bad_audit_seq, other}}
      {:error, reason} -> {:error, reason}
    end
  end

  # The history of incarnations, newest first: `[%{"from" => seq, "id" => id}]`.
  defp load_incarnations(instance) do
    case Store.get(instance, :default, Keys.meta("incarnation")) do
      :not_found ->
        {:ok, []}

      {:ok, json} ->
        with {:ok, history} when is_list(history) <- JSON.decode(json),
             true <- Enum.all?(history, &valid_incarnation?/1) do
          {:ok, history}
        else
          _ -> {:error, :bad_incarnation}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp valid_incarnation?(%{"from" => from, "id" => id}),
    do: is_integer(from) and from >= 1 and is_binary(id)

  defp valid_incarnation?(_other), do: false

  # The id a restore left for this boot to announce (`nil` when none).
  defp load_incarnation_pending(instance) do
    case Store.get(instance, :default, Keys.meta("incarnation_pending")) do
      {:ok, id} -> {:ok, id}
      :not_found -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp announce_restore(state, nil, _history, _last_seq), do: {:ok, state}

  defp announce_restore(state, incarnation, history, last_seq) do
    instance = state.instance
    history = [%{"from" => last_seq + 1, "id" => incarnation} | history]
    facts = %{"incarnation" => incarnation, "restored_sequence" => last_seq}

    attrs =
      Map.merge(
        %{action: "instance.restore", resource: to_string(instance), detail: facts},
        Lifecycle.outbox(instance, fn -> facts end)
      )

    ops = [
      {:put, :default, Keys.meta("incarnation"), JSON.encode!(history)},
      {:delete, :default, Keys.meta("incarnation_pending")}
    ]

    # In the table before the row is committed: the row is the new incarnation's
    # first event, and its id is keyed with it.
    true = :ets.insert(table(instance), {:incarnations, history})

    with {:ok, state} <- commit_row(state, Context.system(), attrs, ops) do
      Logger.warning(
        "[ankusa] store restored from a backup: audit trail resumes after seq #{last_seq} " <>
          "as incarnation #{incarnation}"
      )

      {:ok, state}
    end
  end

  # The change's ops and its audit row in one synced batch.
  defp commit_row(state, context, attrs, ops) do
    seq = state.next_seq
    row = context |> row(attrs, seq) |> then(&%{&1 | publish: is_map(&1.entity)})
    {stored, entity_ops} = split_entity(row)
    encoded = encode(stored)
    counted = counted(state, seq, byte_size(encoded) + entity_bytes(entity_ops))

    batch =
      ops ++
        [
          {:put, :audit, Keys.audit(seq, row.at), encoded},
          seq_op(seq),
          usage_op(counted) | entity_ops
        ]

    with :ok <- Store.write(state.instance, batch, sync: true) do
      emit(state.instance, stored)
      {:ok, publish_usage(%{counted | next_seq: seq + 1})}
    end
  end

  # Pending rows a previous run left: it died between the change's first and
  # last step.
  defp load_pending(instance) do
    %{lo: lo, hi: hi} = Keys.family(:audit_pending)

    Store.fold(instance, :audit_pending, {lo, hi}, [], fn key, value, acc ->
      case decode_row(value) do
        {:ok, row} -> {:cont, [{Keys.decode_audit_pending(key), row} | acc]}
        {:error, reason} -> {:halt, {:error, {:bad_pending_row, key, reason}}}
      end
    end)
    |> case do
      {:ok, {:error, reason}} -> {:error, reason}
      {:ok, rows} -> {:ok, Enum.reverse(rows)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp resolve_leftovers(state, leftovers) do
    Enum.reduce_while(leftovers, {:ok, state}, fn {seq, row}, {:ok, state} ->
      case finalize(state, seq, row, %{result: "interrupted"}) do
        {:ok, state} -> {:cont, {:ok, state}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  # Retention never passes the lifecycle outbox: a row (and its entity) is kept
  # while its `seq` is at or above `m:lifecycle_cursor`, however old it is, so
  # one lifecycle sink that is down for longer than `audit.retention_days`
  # cannot lose the `source.*` and `route.*` events no sink has seen. The rows
  # that are old enough but held back are counted into
  # `[:ankusa, :audit, :retention]` (`blocked_by_cursor`, `ankusa_audit_retention_blocked_by_cursor`)
  # and logged, so the backlog is alarmed on rather than silently growing. The
  # same floor holds for the row and byte ceilings (`audit.max_rows`,
  # `audit.max_bytes`): they prune oldest first, but never past the cursor.
  defp do_prune(state) do
    instance = state.instance
    config = Ankusa.config(instance)
    cutoff = System.system_time(:millisecond) - config.audit.retention_days * @day_ms

    with {:ok, floor} <- outbox_floor(instance, config),
         {:ok, count, held, state} <-
           prune_batches(state, config.audit, cutoff, floor, @prune_batches, 0) do
      if count > 0,
        do: Telemetry.emit([:audit, :pruned], %{count: count}, %{instance: instance})

      Telemetry.emit([:audit, :retention], %{blocked_by_cursor: held}, %{instance: instance})

      if held > 0 do
        Logger.warning(
          "[ankusa] audit retention is held back by the lifecycle outbox: at least #{held} " <>
            "row(s) older than audit.retention_days are kept because no lifecycle sink has " <>
            "confirmed them (m:lifecycle_cursor); a lifecycle sink is down or stuck"
        )
      end

      {{:ok, count}, state}
    else
      {:error, reason, state} ->
        Logger.warning("[ankusa] pruning the audit trail failed: #{inspect(reason)}")
        {{:error, reason}, state}

      {:error, reason} ->
        Logger.warning("[ankusa] pruning the audit trail failed: #{inspect(reason)}")
        {{:error, reason}, state}
    end
  end

  # The `seq` retention may not reach: the lifecycle cursor while lifecycle
  # sinks are configured and the publisher has written one, else no floor.
  defp outbox_floor(_instance, %{lifecycle: %{sinks: []}}), do: {:ok, nil}

  defp outbox_floor(instance, _config) do
    case Store.get(instance, :default, Keys.lifecycle_cursor()) do
      {:ok, <<seq::64>>} -> {:ok, seq}
      {:ok, _bad} -> {:error, {:corrupt_value, :lifecycle_cursor}}
      :not_found -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  @held_max 10_000

  # Walks the trail oldest first and deletes what retention allows: a row older
  # than `cutoff`, or any row while the trail is over `audit.max_rows` /
  # `audit.max_bytes` (a `cutoff` of `0` is the ceilings alone). Never a row at
  # or above `floor` (the lifecycle cursor). `{:ok, deleted, held, state}`; an
  # error carries the state of the batches that did commit, so the counters
  # stay true to the trail.
  defp prune_batches(state, _audit, _cutoff, _floor, 0, total), do: {:ok, total, 0, state}

  defp prune_batches(state, audit, cutoff, floor, left, total) do
    %{lo: lo, hi: hi} = Keys.family(:audit)

    scan =
      Store.fold(state.instance, :audit, {lo, hi}, {0, [], 0, 0}, fn key,
                                                                     value,
                                                                     {n, keys, held, bytes} =
                                                                       acc ->
        {seq, at} = Keys.decode_audit(key)
        old? = at < cutoff
        over? = state.rows - n > audit.max_rows or state.bytes - bytes > audit.max_bytes

        cond do
          not old? and not over? ->
            {:halt, acc}

          floor != nil and seq >= floor and old? and held < @held_max ->
            {:cont, {n, keys, held + 1, bytes}}

          floor != nil and seq >= floor ->
            {:halt, acc}

          n < @prune_batch ->
            {:cont, {n + 1, [key | keys], held, bytes + byte_size(value)}}

          true ->
            {:halt, acc}
        end
      end)

    with {:ok, {n, keys, held, bytes}} <- scan,
         {:ok, state} <- delete_rows(state, keys, n, bytes) do
      if n < @prune_batch,
        do: {:ok, total + n, held, state},
        else: prune_batches(state, audit, cutoff, floor, left - 1, total + n)
    else
      {:error, reason} -> {:error, reason, state}
    end
  end

  # The rows `keys` and the lifecycle entity beside each, deleted in one batch
  # with the new usage (the entities' bytes are read first, so the count stays
  # exact).
  defp delete_rows(state, [], _n, _bytes), do: {:ok, state}

  defp delete_rows(state, keys, n, bytes) do
    entity_keys =
      Enum.map(keys, fn key ->
        {seq, _at} = Keys.decode_audit(key)
        Keys.lifecycle_entity(seq)
      end)

    with {:ok, entities} <- Store.multi_get(state.instance, :audit, entity_keys) do
      entity_bytes = for {:ok, bin} <- entities, reduce: 0, do: (acc -> acc + byte_size(bin))

      state = %{
        state
        | rows: max(state.rows - n, 0),
          bytes: max(state.bytes - bytes - entity_bytes, 0)
      }

      ops = Enum.flat_map(keys, &prune_ops/1) ++ [usage_op(state)]

      with :ok <- Store.write(state.instance, ops, sync: false), do: {:ok, publish_usage(state)}
    end
  end

  # A row and the lifecycle entity beside it (a delete of an absent key is a no-op).
  defp prune_ops(key) do
    {seq, _at} = Keys.decode_audit(key)
    [{:delete, :audit, key}, {:delete, :audit, Keys.lifecycle_entity(seq)}]
  end

  defp over?(state, audit), do: state.rows > audit.max_rows or state.bytes > audit.max_bytes

  # Room for one more mutation, or `{:error, :audit_backlog}`: the trail is pruned
  # to its ceilings first (oldest first, never past the lifecycle cursor), and a
  # trail that is still over one, or a lifecycle outbox holding
  # `audit.outbox_max_backlog` events or more that no sink has confirmed, refuses.
  # An unpublished row is never dropped to make room. A change the node makes
  # itself (`Context.system/0`) is not refused: it is one row for a whole batch of
  # work, and refusing it could stop the very sweeps that make room.
  defp ensure_room(state, %Context{actor: "system"}), do: {:ok, state}

  defp ensure_room(state, _context) do
    config = Ankusa.config(state.instance)
    audit = config.audit

    case outbox_floor(state.instance, config) do
      {:ok, floor} ->
        state = if over?(state, audit), do: prune_to_ceiling(state, audit, floor), else: state

        cond do
          over?(state, audit) ->
            refuse(state, "the trail is over audit.max_rows / audit.max_bytes")

          outbox_full?(state, config, floor) ->
            refuse(
              state,
              "the lifecycle outbox holds audit.outbox_max_backlog unconfirmed events"
            )

          true ->
            {:ok, state}
        end

      {:error, _reason} ->
        # The cursor cannot be read, so the write that follows fails on its own.
        {:ok, state}
    end
  end

  defp refuse(state, why) do
    Logger.warning("[ankusa] refusing a control-plane change: #{why}; nothing is dropped")
    {{:error, :audit_backlog}, state}
  end

  defp outbox_full?(state, %{lifecycle: %{sinks: sinks}, audit: audit}, floor),
    do: sinks != [] and floor != nil and state.next_seq - 1 - floor >= audit.outbox_max_backlog

  defp prune_to_ceiling(state, audit, floor) do
    case prune_batches(state, audit, 0, floor, 1, 0) do
      {:ok, count, _held, state} ->
        if count > 0,
          do: Telemetry.emit([:audit, :pruned], %{count: count}, %{instance: state.instance})

        state

      {:error, _reason, state} ->
        state
    end
  end

  # ── the size of the trail ─────────────────────────────────────────────────

  defp usage_op(state),
    do:
      {:put, :default, Keys.meta("audit_usage"),
       <<state.rows::64, state.bytes::64, state.counted::64>>}

  defp entity_bytes(entity_ops),
    do: for({:put, :audit, _key, bin} <- entity_ops, reduce: 0, do: (acc -> acc + byte_size(bin)))

  # `state` after one more row of `size` bytes with sequence number `seq`.
  defp counted(state, seq, size),
    do: %{state | rows: state.rows + 1, bytes: state.bytes + size, counted: seq}

  # The counters, readable without a call: the row and byte counts, and the last
  # `seq` handed out (what the lifecycle backlog is measured from).
  defp publish_usage(state) do
    true =
      :ets.insert(table(state.instance), [
        {:usage, state.rows, state.bytes},
        {:last_seq, state.next_seq - 1}
      ])

    state
  end

  # The counters persisted by the last batch, if they describe the trail as it is
  # (the newest row is the last one counted); else counted from the trail, once.
  # A store that a release without the ceilings wrote to meanwhile fails the check
  # and is recounted.
  defp load_usage(instance) do
    %{lo: lo, hi: hi} = Keys.family(:audit)

    with {:ok, newest} <- Store.first(instance, :audit, {lo, hi}, reverse: true),
         newest_seq = if(newest, do: elem(Keys.decode_audit(elem(newest, 0)), 0), else: 0),
         {:ok, stored} <- Store.get(instance, :default, Keys.meta("audit_usage")) |> usage_value() do
      case stored do
        {rows, bytes, ^newest_seq} -> {:ok, {rows, bytes, newest_seq}}
        _stale_or_absent -> recount(instance, newest_seq)
      end
    end
  end

  defp usage_value({:ok, <<rows::64, bytes::64, counted::64>>}), do: {:ok, {rows, bytes, counted}}
  defp usage_value({:ok, _bad}), do: {:ok, :bad}
  defp usage_value(:not_found), do: {:ok, :absent}
  defp usage_value({:error, reason}), do: {:error, reason}

  defp recount(instance, newest_seq) do
    count = fn family, {rows, bytes} ->
      %{lo: lo, hi: hi} = Keys.family(family)

      Store.fold(instance, family, {lo, hi}, {rows, bytes}, fn _key, value, {r, b} ->
        {:cont, {r + if(family == :audit, do: 1, else: 0), b + byte_size(value)}}
      end)
    end

    with {:ok, acc} <- count.(:audit, {0, 0}),
         {:ok, {rows, bytes}} <- count.(:lifecycle_entities, acc),
         :ok <-
           Store.write(
             instance,
             [
               {:put, :default, Keys.meta("audit_usage"), <<rows::64, bytes::64, newest_seq::64>>}
             ],
             sync: true
           ) do
      {:ok, {rows, bytes, newest_seq}}
    end
  end

  # The instance name is operator config: one atom per instance, never request input.
  # sobelow_skip ["DOS.BinToAtom"]
  defp table(instance), do: :"ankusa_audit_#{instance}"
end
