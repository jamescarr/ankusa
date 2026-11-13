defmodule Ankusa.Gateway.Authz.Table.Snapshot do
  @moduledoc """
  One `table` authorizer's snapshot: loads the source into a fresh ETS table
  at start and every `refresh_ms` (skipping the load when the source's
  `version/2` is unchanged), then publishes it atomically as
  `{name, tid, generation, loaded_at_ms, has_deny?}` in
  `:"ankusa_gateway_tables_<instance>"`, which `Ankusa.Gateway.Supervisor`
  creates and the authorizer's decision reads.

  A load that exceeds `max_rows` or `max_bytes`, has more than 1 % bad rows,
  or fails is discarded: the previous table keeps serving until the next
  tick. The previous generation is deleted one second after a new one is
  published, so a request that read the old row finishes against it.

  `ready_timeout_ms` is published for the authorizer's readiness check:
  `/ready` is `503` only while no load has ever succeeded and less than that
  has passed since this process started.

  Telemetry: `[:ankusa, :gateway, :table_refresh]` `%{duration, rows, bytes}`
  `%{instance, authorizer, result}`.
  """

  use GenServer

  alias Ankusa.Gateway.Authz.Table
  alias Ankusa.Telemetry

  @drop_delay_ms 1_000
  @load_deadline_ms 300_000
  # Checking the table's size mid-stream needs the table to hold at least a
  # page's worth, or a tiny max_bytes would refuse every load.
  @bytes_check_every 1_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    name = Keyword.fetch!(opts, :name)
    state = Keyword.fetch!(opts, :state)

    GenServer.start_link(__MODULE__, state, name: Ankusa.via(instance, {:gateway_table, name}))
  end

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance), Keyword.fetch!(opts, :name)},
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @doc """
  Ask `instance`'s snapshot `name` to load now. The next scheduled load also
  runs; an unchanged version makes it cheap.
  """
  @spec refresh(atom(), String.t()) :: :ok
  def refresh(instance, name) do
    case Ankusa.whereis(instance, {:gateway_table, name}) do
      nil -> :ok
      pid -> send(pid, :load)
    end

    :ok
  end

  @doc """
  Wait until `instance`'s snapshot `name` has published a table, or `timeout`
  passes: `:ok` or `:timeout`. For tests and benches.
  """
  @spec await(atom(), String.t(), non_neg_integer()) :: :ok | :timeout
  def await(instance, name, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_until(instance, name, deadline)
  end

  defp await_until(instance, name, deadline) do
    case :ets.lookup(Table.tables_table(instance), name) do
      [{^name, _tid, _generation, _loaded_ms, _has_deny?}] ->
        :ok

      [] ->
        if System.monotonic_time(:millisecond) >= deadline do
          :timeout
        else
          Process.sleep(10)
          await_until(instance, name, deadline)
        end
    end
  rescue
    ArgumentError ->
      if System.monotonic_time(:millisecond) >= deadline do
        :timeout
      else
        Process.sleep(10)
        await_until(instance, name, deadline)
      end
  end

  # ── server ──────────────────────────────────────────────────────────────────

  @impl true
  def init(state) do
    started_ms = System.monotonic_time(:millisecond)
    ensure_table(state.instance)
    write_ready(state, false, started_ms)
    send(self(), :load)

    {:ok, %{state: state, started_ms: started_ms, last_version: :unknown, published: nil}}
  end

  @impl true
  def handle_info(:load, snapshot) do
    snapshot = load(snapshot)
    Process.send_after(self(), :load, snapshot.state.refresh_ms)
    {:noreply, snapshot}
  end

  def handle_info({:drop, tid}, snapshot) do
    if snapshot.published != nil and elem(snapshot.published, 1) != tid do
      delete_table(tid)
    end

    {:noreply, snapshot}
  end

  def handle_info(_message, snapshot), do: {:noreply, snapshot}

  @impl true
  def terminate(_reason, snapshot) do
    case snapshot.published do
      nil ->
        :ok

      {generation, tid} ->
        delete_table(tid)

        try do
          case :ets.lookup(Table.tables_table(snapshot.state.instance), snapshot.state.name) do
            [{name, ^tid, ^generation, _loaded_ms, _has_deny?}] ->
              :ets.delete(Table.tables_table(snapshot.state.instance), name)

            _other ->
              :ok
          end
        rescue
          ArgumentError -> :ok
        end
    end

    :ok
  end

  # ── one load ────────────────────────────────────────────────────────────────

  defp load(snapshot) do
    started_ms = System.monotonic_time(:millisecond)
    state = snapshot.state
    ctx = %{instance: state.instance, name: state.name, deadline: started_ms + @load_deadline_ms}

    case version(state.source, ctx) do
      {:ok, version} ->
        if version == snapshot.last_version and snapshot.published != nil do
          emit(snapshot, started_ms, :unchanged)
          snapshot
        else
          build(snapshot, ctx, started_ms, version)
        end

      :unknown ->
        build(snapshot, ctx, started_ms, :unknown)

      {:error, _reason} ->
        emit(snapshot, started_ms, :error)
        snapshot
    end
  end

  defp version(%{module: module} = source, ctx) do
    if function_exported?(module, :version, 2),
      do: module.version(source.state, ctx),
      else: :unknown
  end

  defp build(snapshot, ctx, started_ms, version) do
    state = snapshot.state
    tid = :ets.new(:ankusa_gateway_table, [:set, :protected, read_concurrency: true])
    acc = %{rows: 0, bad: 0, has_deny?: false, aborted: nil}

    result =
      try do
        state.source.module.load(state.source.state, ctx, acc, reducer(tid, state))
      rescue
        exception -> {:error, exception}
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    case result do
      {:ok, acc} -> publish(snapshot, tid, started_ms, version, acc)
      _error -> discard(snapshot, tid, started_ms, :error)
    end
  end

  # The reducer closes over `acc` only as its starting value; every return is
  # the caller's. A row that does not hold five valid fields is counted, not
  # stored.
  defp reducer(tid, state) do
    fn row, acc ->
      case table_row(row, state.columns) do
        {:ok, key, effect} ->
          acc = put_row(tid, key, effect, acc)
          acc = cap(acc, state, tid)
          if acc.aborted, do: {:halt, acc}, else: {:cont, acc}

        :bad ->
          {:cont, %{acc | bad: acc.bad + 1}}
      end
    end
  end

  defp table_row(row, columns) when is_map(row) do
    with principal when is_binary(principal) <- Table.row_field(row, columns.principal),
         true <- principal != "",
         action when is_binary(action) <- Table.row_field(row, columns.action),
         true <- Table.action(action),
         space when is_binary(space) <- Table.row_field(row, columns.space),
         true <- Table.space(space),
         prefix when is_binary(prefix) <- Table.row_field(row, columns.prefix),
         :ok <- Ankusa.Gateway.Spaces.validate_prefix(prefix),
         effect when effect in [:allow, :deny] <-
           Table.effect(Table.row_field(row, columns.effect)) do
      {:ok, {principal, space, action, prefix}, effect}
    else
      _invalid -> :bad
    end
  end

  defp table_row(_row, _columns), do: :bad

  defp put_row(tid, key, :deny, acc) do
    :ets.insert(tid, {key, :deny})
    %{acc | rows: acc.rows + 1, has_deny?: true}
  end

  defp put_row(tid, key, :allow, acc) do
    :ets.insert_new(tid, {key, :allow})
    %{acc | rows: acc.rows + 1}
  end

  defp cap(acc, state, tid) do
    cond do
      acc.rows > state.max_rows ->
        %{acc | aborted: :too_large}

      rem(acc.rows, @bytes_check_every) == 0 and bytes(tid) > state.max_bytes ->
        %{acc | aborted: :too_large}

      true ->
        acc
    end
  end

  defp publish(snapshot, tid, started_ms, _version, %{aborted: :too_large}) do
    discard(snapshot, tid, started_ms, :too_large)
  end

  defp publish(snapshot, tid, started_ms, version, acc) do
    total = acc.rows + acc.bad
    table_bytes = bytes(tid)

    cond do
      table_bytes > snapshot.state.max_bytes ->
        discard(snapshot, tid, started_ms, :too_large)

      acc.bad > 0 and acc.bad * 100 > total ->
        discard(snapshot, tid, started_ms, :invalid)

      true ->
        state = snapshot.state
        generation = System.unique_integer([:monotonic, :positive])
        loaded_ms = System.monotonic_time(:millisecond)

        :ets.insert(Table.tables_table(state.instance), {
          state.name,
          tid,
          generation,
          loaded_ms,
          acc.has_deny?
        })

        write_ready(state, true, snapshot.started_ms)
        emit(snapshot, started_ms, :ok, acc.rows, table_bytes)

        if snapshot.published != nil do
          Process.send_after(self(), {:drop, elem(snapshot.published, 1)}, @drop_delay_ms)
        end

        %{snapshot | last_version: version, published: {generation, tid}}
    end
  end

  defp discard(snapshot, tid, started_ms, result) do
    delete_table(tid)
    emit(snapshot, started_ms, result)
    snapshot
  end

  defp delete_table(tid) do
    :ets.delete(tid)
  rescue
    ArgumentError -> :ok
  end

  # ── publication and telemetry ───────────────────────────────────────────────

  defp ensure_table(instance) do
    name = Table.tables_table(instance)

    if :ets.whereis(name) == :undefined do
      :ets.new(name, [:named_table, :public, :set, read_concurrency: true])
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp write_ready(state, loaded?, started_ms) do
    :ets.insert(Table.tables_table(state.instance), {{:ready, state.name}, {loaded?, started_ms}})
  end

  defp bytes(tid) do
    :ets.info(tid, :memory) * :erlang.system_info(:wordsize)
  rescue
    ArgumentError -> 0
  end

  defp emit(snapshot, started_ms, result, rows \\ nil, bytes \\ nil) do
    state = snapshot.state

    Telemetry.emit(
      [:gateway, :table_refresh],
      %{
        duration:
          System.monotonic_time() - System.convert_time_unit(started_ms, :millisecond, :native),
        rows: rows || published_rows(snapshot),
        bytes: bytes || published_bytes(snapshot)
      },
      %{instance: state.instance, authorizer: state.name, result: result}
    )
  end

  defp published_rows(snapshot) do
    case snapshot.published do
      nil -> 0
      {_generation, tid} -> :ets.info(tid, :size) || 0
    end
  end

  defp published_bytes(snapshot) do
    case snapshot.published do
      nil -> 0
      {_generation, tid} -> bytes(tid)
    end
  end
end
