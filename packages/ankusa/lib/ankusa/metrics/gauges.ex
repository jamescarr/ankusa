defmodule Ankusa.Metrics.Gauges do
  @moduledoc """
  Periodic state measurements for `Ankusa.Metrics`: what an operator alarms on,
  as opposed to the event counters.

  A `:telemetry_poller` per instance calls `measure/1` every
  `admin.gauge_interval_ms` (default 15 s) and emits:

    * `[:ankusa, :store, :state]` — `hooks`, `deliveries` (RocksDB key
      estimates), `disk_bytes`, `next_seq` (`Ankusa.Queue.stats/1`); on a node
      that runs the store.
    * `[:ankusa, :queue, :state]` — `pending` (first-attempt rows waiting),
      `scheduled` (retry rows waiting, including one whose backoff has just
      ended and is not yet claimed), `inflight`, `dead`, `archive_pending`,
      `tenants` (how many tenants the store has handed an ordinal to) and
      `oldest_due_age_ms` (how long the oldest due row has been waiting; `0`
      with nothing due). Read from the node's queue counters (six point reads)
      and the active-tenant index, so the cost is independent of the backlog and
      of the number of tenants; on a node that runs the store.
    * `[:ankusa, :tenant, :state]` — one event per tenant among the 100 with the
      most `pending` rows (`pending`, and the `tenant` in the metadata), taken
      from the counters of the first 10 000 tenants of the active index.
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

  alias Ankusa.{Config, Instance, Queue}
  alias Ankusa.Queue.Deliveries
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Queue.Tenants

  @top_tenants 100
  @candidate_tenants 10_000

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Ankusa.config(instance)

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

    with {:ok, counters} <- Counters.read(instance, 0),
         {:ok, tenants} <- Tenants.count(instance),
         {:ok, oldest} <- Deliveries.next_due_at(instance) do
      emit(instance, :queue, %{
        pending: max(counters.pending, 0),
        scheduled: max(counters.scheduled, 0),
        inflight: max(counters.inflight, 0),
        dead: max(counters.dead, 0),
        archive_pending: max(counters.archive_pending, 0),
        tenants: tenants,
        oldest_due_age_ms: if(oldest, do: max(0, now - oldest), else: 0)
      })

      tenant_pending(instance, now)
    end
  end

  # `ankusa_tenant_pending{tenant}`: the top tenants by waiting first attempts.
  # A tenant with `pending > 0` has a marker at or before the current second, so
  # the first tenants of the active index (those that have waited longest) are
  # the candidates, and their counters are one `multi_get`. A tenant that fell
  # out of the top since the last tick is reported once as `0`, so its series
  # does not keep a stale value (the poller process remembers the last set).
  defp tenant_pending(instance, now) do
    key = {__MODULE__, instance, :tenant_pending}

    with {:ok, ords} <- Deliveries.active_ords(instance, now, @candidate_tenants),
         {:ok, pending} <- Counters.read_field(instance, ords, :pending),
         {:ok, named} <- ords |> Enum.zip(pending) |> top_pending() |> name_tenants(instance) do
      current = MapSet.new(named, &elem(&1, 0))
      previous = Process.get(key, MapSet.new())

      for {tenant, count} <- named, do: emit_tenant(instance, tenant, count)
      for tenant <- MapSet.difference(previous, current), do: emit_tenant(instance, tenant, 0)

      Process.put(key, current)
      :ok
    end
  end

  defp top_pending(pairs) do
    pairs
    |> Enum.filter(fn {_ord, count} -> count > 0 end)
    |> Enum.sort_by(fn {_ord, count} -> count end, :desc)
    |> Enum.take(@top_tenants)
  end

  defp name_tenants(top, instance) do
    top
    |> Enum.reduce_while({:ok, []}, fn {ord, count}, {:ok, acc} ->
      case Tenants.name(instance, ord) do
        {:ok, tenant} -> {:cont, {:ok, [{tenant, count} | acc]}}
        :not_found -> {:cont, {:ok, acc}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp emit_tenant(instance, tenant, count) do
    :telemetry.execute([:ankusa, :tenant, :state], %{pending: count}, %{
      instance: instance,
      tenant: tenant
    })
  end

  defp quarantine(instance) do
    with {:ok, held} <- Ankusa.Edge.Quarantine.held(instance) do
      emit(instance, :quarantine, held)
    end
  end

  # `:disksup` is os_mon's, which core does not start or depend on (the server
  # image does); this reads it only when its process exists.
  @dialyzer {:no_unknown, disk: 2}
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
