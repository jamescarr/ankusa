defmodule Ankusa.Health.SinkErrors do
  @moduledoc """
  The last failed publish of every sink under `wal: :none`, for
  `Ankusa.Health.Check.Sinks`.

  The direct ack path (`Ankusa.Edge.Publish`) has no queue and no retry, so a
  broker that refuses every publish shows nowhere but in the `503`s. It calls
  `record/3` with each sink's outcome: a failure stores the monotonic time under
  the sink's identity (its module and options, hashed), a success removes the
  entry, so a sink that came back reads healthy at once instead of after the
  window. Both are one ETS write in the request process, no message to a
  singleton.

  One process per instance owns the table and sweeps entries older than
  `listener.ready_sink_error_window_ms` every window, so the table holds at most the
  sinks that failed within it. It is also capped at `@max_entries`: a failure of
  a sink beyond the cap is not recorded (a node with that many failing sinks is
  unready already, and the sweep makes room as they age out).
  """

  use GenServer

  @max_entries 10_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, instance, name: Ankusa.via(instance, :health_sink_errors))
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc "The table of one instance (the instance name is operator config, never request input)."
  # sobelow_skip ["DOS.BinToAtom"]
  @spec table(atom()) :: atom()
  def table(instance), do: :"ankusa_health_sinks_#{instance}"

  @doc """
  Note that the sink `{mod, opts}` just published with `outcome` (`:ok` or
  `:error`). `:ok` when no tracker runs (an instance that is not `wal: :none`).
  """
  @spec record(atom(), {module(), keyword()}, :ok | :error) :: :ok
  def record(instance, {_mod, _opts} = sink, outcome) do
    table = table(instance)
    key = :erlang.phash2(sink)

    case outcome do
      :ok ->
        :ets.delete(table, key)

      :error ->
        if :ets.member(table, key) or :ets.info(table, :size) < @max_entries,
          do: :ets.insert(table, {key, System.monotonic_time(:millisecond)})
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "How many sinks failed their last publish within `window_ms`."
  @spec failing(atom(), pos_integer()) :: non_neg_integer()
  def failing(instance, window_ms) do
    cutoff = System.monotonic_time(:millisecond) - window_ms
    :ets.select_count(table(instance), [{{:_, :"$1"}, [{:>=, :"$1", cutoff}], [true]}])
  rescue
    ArgumentError -> 0
  end

  @impl true
  def init(instance) do
    :ets.new(table(instance), [:named_table, :public, :set, write_concurrency: true])
    window = Ankusa.config(instance).listener.ready_sink_error_window_ms
    Process.send_after(self(), :sweep, window)
    {:ok, %{instance: instance, window: window}}
  end

  @impl true
  def handle_info(:sweep, %{instance: instance, window: window} = state) do
    cutoff = System.monotonic_time(:millisecond) - window
    :ets.select_delete(table(instance), [{{:_, :"$1"}, [{:<, :"$1", cutoff}], [true]}])
    Process.send_after(self(), :sweep, window)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}
end
