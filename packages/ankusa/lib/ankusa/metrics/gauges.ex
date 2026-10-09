defmodule Ankusa.Metrics.Gauges do
  @moduledoc """
  Periodic state measurements for `Ankusa.Metrics`: what an operator alarms on,
  as opposed to the event counters.

  A `:telemetry_poller` per instance calls `measure/1` every
  `admin.gauge_interval_ms` (default 15 s) and emits:

    * `[:ankusa, :store, :state]` — `hooks`, `deliveries` (RocksDB key
      estimates), `disk_bytes`, `next_seq` (`Ankusa.Queue.stats/1`); on a node
      that runs the store.
    * `[:ankusa, :queue, :state]` — `pending` (due now), `scheduled` (waiting
      for a retry), `inflight`, `dead`, `archive_pending`, and
      `oldest_due_age_ms` (how long the oldest due row has been waiting; `0`
      with nothing due). Counted by scanning the index keys, so the cost grows
      with the backlog (about 1 µs a key); on a node that runs the store.
    * `[:ankusa, :quarantine, :state]` — the pen's `bytes` and `entries`; on an
      `:edge` node.
    * `[:ankusa, :disk, :state]` — `free_bytes` and `total_bytes` of the file
      system holding `data_dir`, only while `:disksup` (`:os_mon`) runs. Core
      does not start `:os_mon`; `ankusa_server` does.

  Each probe runs on its own: one that fails (the store is reopening) emits
  nothing, so its gauges keep their last value, and logs at `:debug`; the
  others still run. Dispatch reports its own scheduler state
  (`[:ankusa, :dispatch, :state]`) from its housekeeping tick.
  """

  require Logger

  # `:os_mon` is optional: core never starts it, and only calls `:disksup` when
  # its process is running.
  @compile {:no_warn_undefined, :disksup}

  alias Ankusa.{Config, Instance, Queue, Store}
  alias Ankusa.Queue.Deliveries
  alias Ankusa.Store.Keys

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Keyword.fetch!(opts, :config)

    Supervisor.child_spec(
      {:telemetry_poller,
       measurements: [{__MODULE__, :measure, [instance]}],
       period: config.admin.gauge_interval_ms,
       init_delay: config.admin.gauge_interval_ms,
       name: Ankusa.via(instance, :metrics_poller)},
      id: {__MODULE__, instance}
    )
  end

  @doc "Take every measurement once, now. The poller calls this; so may a test."
  @spec measure(atom()) :: :ok
  def measure(instance) do
    config = Ankusa.config(instance)
    store? = Instance.store?(config)

    if store?, do: probe(instance, :store, &store/1)
    if store?, do: probe(instance, :queue, &queue/1)
    if Config.role?(config, :edge), do: probe(instance, :quarantine, &quarantine/1)
    probe(instance, :disk, &disk(&1, config))
    :ok
  end

  defp probe(instance, name, fun) do
    case fun.(instance) do
      :ok -> :ok
      {:error, reason} -> skipped(name, reason)
    end
  rescue
    exception -> skipped(name, Exception.message(exception))
  catch
    kind, reason -> skipped(name, {kind, reason})
  end

  defp skipped(name, reason) do
    Logger.debug("[ankusa] #{name} gauge skipped: #{inspect(reason)}")
    :ok
  end

  defp store(instance) do
    with {:ok, stats} <- Queue.stats(instance) do
      emit(instance, :store, Map.take(stats, [:hooks, :deliveries, :disk_bytes, :next_seq]))
    end
  end

  defp queue(instance) do
    now = System.system_time(:millisecond)

    with {:ok, {pending, scheduled}} <- count_due(instance, now),
         {:ok, inflight} <- count(instance, :inflight),
         {:ok, dead} <- count(instance, :dead),
         {:ok, archive_pending} <- count(instance, :archive_pending),
         {:ok, oldest} <- Deliveries.next_due_at(instance, 0) do
      emit(instance, :queue, %{
        pending: pending,
        scheduled: scheduled,
        inflight: inflight,
        dead: dead,
        archive_pending: archive_pending,
        oldest_due_age_ms: if(oldest, do: max(0, now - oldest), else: 0)
      })
    end
  end

  defp count_due(instance, now) do
    %{lo: lo, hi: hi} = Keys.family(:due)

    Store.fold(instance, :due, {lo, hi}, {0, 0}, fn key, _value, {pending, scheduled} ->
      {at, _seq, _sink} = Keys.decode_due(key)
      if at <= now, do: {:cont, {pending + 1, scheduled}}, else: {:cont, {pending, scheduled + 1}}
    end)
  end

  defp count(instance, family) do
    %{lo: lo, hi: hi} = Keys.family(family)
    Store.fold(instance, family, {lo, hi}, 0, fn _key, _value, n -> {:cont, n + 1} end)
  end

  defp quarantine(instance) do
    with {:ok, held} <- Ankusa.Edge.Quarantine.held(instance) do
      emit(instance, :quarantine, held)
    end
  end

  defp disk(instance, config) do
    if is_pid(Process.whereis(:disksup)) do
      case :disksup.get_disk_info(String.to_charlist(Path.expand(config.data_dir))) do
        [{_id, total_kib, available_kib, _capacity} | _] when total_kib > 0 ->
          emit(instance, :disk, %{
            free_bytes: available_kib * 1024,
            total_bytes: total_kib * 1024
          })

        other ->
          {:error, {:disk_info, other}}
      end
    else
      :ok
    end
  end

  defp emit(instance, name, measurements) do
    :telemetry.execute([:ankusa, name, :state], measurements, %{instance: instance})
  end
end
