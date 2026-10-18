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
  | `ip_rules.put` | the global IP rules |

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
      and the next boot (or the caller dying, while this node runs) moves it
      into the trail with `result: "interrupted"`: *the change may or may not
      have been applied*; `after_hash` is the hash of what was about to be
      written, so an operator can compare it to the subject's present state.
      The trail never loses a change that was applied; it can show one that was
      not (`"interrupted"`).

  A change whose audit row cannot be written is not made: the caller gets
  `{:error, :store_unavailable}`.

  ## Retention

  `audit.retention_days` (default 365). An hourly sweep deletes rows older than
  that, oldest first, in bounded batches, and emits `[:ankusa, :audit, :pruned]`.
  Every committed row emits `[:ankusa, :audit]`.
  """

  use GenServer

  require Logger

  alias Ankusa.Audit.Context
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

  @updatable [:result, :before_hash, :after_hash, :revision, :detail, :tenant, :resource]

  @typedoc "What a caller says about its change. `action` is required."
  @type attrs :: %{
          required(:action) => String.t(),
          optional(:tenant) => String.t() | nil,
          optional(:resource) => String.t() | nil,
          optional(:before_hash) => String.t() | nil,
          optional(:after_hash) => String.t() | nil,
          optional(:revision) => pos_integer() | nil,
          optional(:detail) => map() | nil,
          optional(:result) => String.t()
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
  @spec commit(atom(), Context.t(), attrs(), [Store.op()]) :: :ok | {:error, :store_unavailable}
  def commit(instance, %Context{} = context, %{action: _} = attrs, ops) when is_list(ops),
    do: call(instance, {:commit, context, attrs, ops})

  @doc """
  Write the pending row of a change that is about to be made elsewhere. Returns
  the handle `finish/3` takes. The caller is monitored: if it dies first, the
  row is finished as `"interrupted"`.
  """
  @spec begin(atom(), Context.t(), attrs()) :: {:ok, pos_integer()} | {:error, :store_unavailable}
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
          reply | {:error, :store_unavailable}
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
         {:ok, leftovers} <- load_pending(instance) do
      :ets.new(table(instance), [:named_table, :protected, read_concurrency: true])
      true = :ets.insert(table(instance), {:key, key})

      state = %{
        instance: instance,
        next_seq: last_seq + 1,
        pending: %{},
        prune_interval: prune_interval
      }

      case resolve_leftovers(state, leftovers) do
        {:ok, state} ->
          Process.send_after(self(), :prune, prune_interval)
          {:ok, state}

        {:error, reason} ->
          {:stop, {:audit_recovery_failed, reason}}
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
    seq = state.next_seq
    row = row(context, attrs, seq)
    batch = ops ++ [{:put, :audit, Keys.audit(seq, row.at), encode(row)}, seq_op(seq)]

    case Store.write(state.instance, batch, sync: true) do
      :ok ->
        emit(state.instance, row)
        {:reply, :ok, %{state | next_seq: seq + 1}}

      {:error, reason} ->
        Logger.error("[ankusa] could not commit #{attrs.action}: #{inspect(reason)}")
        {:reply, {:error, :store_unavailable}, state}
    end
  end

  def handle_call({:begin, context, attrs}, {pid, _tag}, state) do
    seq = state.next_seq
    row = row(context, attrs, seq)
    batch = [{:put, :audit, Keys.audit_pending(seq), encode(row)}, seq_op(seq)]

    case Store.write(state.instance, batch, sync: true) do
      :ok ->
        ref = Process.monitor(pid)
        pending = Map.put(state.pending, seq, %{row: row, ref: ref})
        {:reply, {:ok, seq}, %{state | next_seq: seq + 1, pending: pending}}

      {:error, reason} ->
        Logger.error("[ankusa] could not record #{attrs.action}: #{inspect(reason)}")
        {:reply, {:error, :store_unavailable}, state}
    end
  end

  def handle_call({:finish, seq, update}, _from, state) do
    case Map.pop(state.pending, seq) do
      {nil, _pending} ->
        {:reply, {:error, :not_pending}, state}

      {%{ref: ref} = entry, rest} ->
        Process.demonitor(ref, [:flush])
        state = %{state | pending: rest}

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

  def handle_call(:prune, _from, state), do: {:reply, do_prune(state.instance), state}

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Enum.find(state.pending, fn {_seq, entry} -> entry.ref == ref end) do
      nil ->
        {:noreply, state}

      {seq, entry} ->
        state = %{state | pending: Map.delete(state.pending, seq)}

        case finalize(state, seq, entry.row, %{result: "interrupted"}) do
          {:ok, state} ->
            {:noreply, state}

          # The row stays pending on disk: the next boot finishes it.
          {:error, _reason} ->
            {:noreply, state}
        end
    end
  end

  def handle_info(:prune, state) do
    _ = do_prune(state.instance)
    Process.send_after(self(), :prune, state.prune_interval)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # ── internals ─────────────────────────────────────────────────────────────

  defp call(instance, message) do
    GenServer.call(Ankusa.via(instance, :audit), message, @call_timeout)
  catch
    :exit, _reason -> {:error, :store_unavailable}
  end

  # The pending row is deleted and its final row written in one batch.
  defp finalize(state, pending_seq, row, update) do
    seq = state.next_seq
    at = System.system_time(:millisecond)
    final = row |> Map.merge(Map.take(update, @updatable)) |> Map.merge(%{seq: seq, at: at})

    batch = [
      {:delete, :audit, Keys.audit_pending(pending_seq)},
      {:put, :audit, Keys.audit(seq, at), encode(final)},
      seq_op(seq)
    ]

    case Store.write(state.instance, batch, sync: true) do
      :ok ->
        emit(state.instance, final)
        {:ok, %{state | next_seq: seq + 1}}

      {:error, reason} ->
        {:error, reason}
    end
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
      detail: Map.get(attrs, :detail)
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

  defp do_prune(instance) do
    days = Ankusa.config(instance).audit.retention_days
    cutoff = System.system_time(:millisecond) - days * @day_ms
    deleted = prune_batches(instance, cutoff, @prune_batches, 0)

    case deleted do
      {:ok, count} ->
        if count > 0,
          do: Telemetry.emit([:audit, :pruned], %{count: count}, %{instance: instance})

        {:ok, count}

      {:error, reason} ->
        Logger.warning("[ankusa] pruning the audit trail failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp prune_batches(_instance, _cutoff, 0, total), do: {:ok, total}

  defp prune_batches(instance, cutoff, left, total) do
    %{lo: lo, hi: hi} = Keys.family(:audit)

    scan =
      Store.fold(instance, :audit, {lo, hi}, {0, []}, fn key, _value, {n, keys} ->
        {_seq, at} = Keys.decode_audit(key)

        if at < cutoff and n < @prune_batch,
          do: {:cont, {n + 1, [key | keys]}},
          else: {:halt, {n, keys}}
      end)

    with {:ok, {n, keys}} <- scan,
         :ok <- Store.write(instance, Enum.map(keys, &{:delete, :audit, &1}), sync: false) do
      if n < @prune_batch,
        do: {:ok, total + n},
        else: prune_batches(instance, cutoff, left - 1, total + n)
    end
  end

  # The instance name is operator config: one atom per instance, never request input.
  # sobelow_skip ["DOS.BinToAtom"]
  defp table(instance), do: :"ankusa_audit_#{instance}"
end
