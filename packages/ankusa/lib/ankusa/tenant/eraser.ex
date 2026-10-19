defmodule Ankusa.Tenant.Eraser do
  @moduledoc """
  Runs tenant erases (`Ankusa.Tenant`) in the background, one stage at a time,
  and keeps the progress of each in the store (`m:erase:<tenant>`, synced after
  every stage) so a node that is killed resumes them at its next boot.

  Started behind the store and the audit trail, before any listener: its `init/1`
  closes (`Ankusa.Tenant.Gate`) every tenant that has a progress row, so a tenant
  whose erase is half done never takes a hook in the window between the node
  accepting requests and the job resuming. The job itself starts a second after
  boot, and a stage that fails because a component it needs has not started yet
  simply runs again.
  """

  use GenServer

  require Logger

  alias Ankusa.Queue.Tenants
  alias Ankusa.Store
  alias Ankusa.Store.Keys
  alias Ankusa.Tenant
  alias Ankusa.Tenant.{Erase, Gate}

  @resume_ms 1_000
  @retry_ms 5_000

  # ── lifecycle ──────────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :tenant_eraser))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc false
  # `Ankusa.Tenant.delete/3`, after it has checked the request.
  @spec start(atom(), String.t(), Ankusa.Audit.Context.t()) ::
          {:ok, :started | :erasing} | {:error, :store_unavailable}
  def start(instance, tenant, context) do
    GenServer.call(Ankusa.via(instance, :tenant_eraser), {:start, tenant, context}, 30_000)
  catch
    :exit, _reason -> {:error, :store_unavailable}
  end

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)

    case resume_rows(instance) do
      {:ok, rows} ->
        {:ok, tasks} = Task.Supervisor.start_link()

        Enum.each(rows, fn {tenant, _progress} -> close(instance, tenant) end)
        if rows != [], do: Process.send_after(self(), :resume, @resume_ms)

        {:ok,
         %{
           instance: instance,
           tasks: tasks,
           pending: Map.new(rows),
           # tenant => %{ref: task ref, stage: stage, progress: progress}
           jobs: %{}
         }}

      {:error, reason} ->
        {:stop, {:tenant_eraser_init_failed, reason}}
    end
  end

  defp resume_rows(instance) do
    %{lo: lo, hi: hi} = Keys.family(:erases)

    Store.fold(instance, :erases, {lo, hi}, [], fn key, value, acc ->
      {:cont, [{Keys.decode_erase(key), Tenant.decode_progress(value)} | acc]}
    end)
  end

  # ── calls ──────────────────────────────────────────────────────────────────

  @impl true
  def handle_call({:start, tenant, context}, _from, state) do
    cond do
      Map.has_key?(state.jobs, tenant) ->
        {:reply, {:ok, :erasing}, state}

      Map.has_key?(state.pending, tenant) ->
        {:reply, {:ok, :erasing}, state |> pop_pending(tenant) |> run_stage(tenant)}

      true ->
        progress = %{
          tenant: tenant,
          stage: hd(Erase.stages()),
          started_at: System.system_time(:millisecond),
          counts: %{},
          context: context
        }

        case write_progress(state.instance, progress) do
          :ok ->
            close(state.instance, tenant)
            {:reply, {:ok, :started}, put_job(state, tenant, progress) |> run_stage(tenant)}

          {:error, reason} ->
            Logger.error("[ankusa] could not record the erase of #{tenant}: #{inspect(reason)}")
            {:reply, {:error, :store_unavailable}, state}
        end
    end
  end

  @impl true
  def handle_info(:resume, state) do
    state =
      Enum.reduce(state.pending, %{state | pending: %{}}, fn {tenant, progress}, state ->
        state |> put_job(tenant, progress) |> run_stage(tenant)
      end)

    {:noreply, state}
  end

  def handle_info({:retry, tenant}, state) do
    {:noreply, if(Map.has_key?(state.jobs, tenant), do: run_stage(state, tenant), else: state)}
  end

  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    case job_of(state, ref) do
      {tenant, job} -> {:noreply, stage_result(state, tenant, job, result)}
      nil -> {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case job_of(state, ref) do
      {tenant, job} -> {:noreply, stage_result(state, tenant, job, {:error, {:exit, reason}})}
      nil -> {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  # ── stages ─────────────────────────────────────────────────────────────────

  defp pop_pending(state, tenant) do
    progress = Map.fetch!(state.pending, tenant)
    put_job(%{state | pending: Map.delete(state.pending, tenant)}, tenant, progress)
  end

  defp put_job(state, tenant, progress),
    do: %{state | jobs: Map.put(state.jobs, tenant, %{ref: nil, progress: progress})}

  defp job_of(state, ref) do
    Enum.find_value(state.jobs, fn {tenant, job} -> job.ref == ref && {tenant, job} end)
  end

  # Runs the stage `progress` is at in a task of its own: a stage can take as
  # long as a sink's attempt, and the eraser must keep answering `start/3`.
  defp run_stage(state, tenant) do
    %{progress: progress} = job = Map.fetch!(state.jobs, tenant)
    instance = state.instance

    job_arg = %{
      instance: instance,
      tenant: tenant,
      context: progress.context,
      counts: progress.counts
    }

    task =
      Task.Supervisor.async_nolink(state.tasks, fn ->
        guarded(progress.stage, job_arg)
      end)

    %{state | jobs: Map.put(state.jobs, tenant, %{job | ref: task.ref})}
  end

  # A stage that raises or exits (a component not started yet, a store that is
  # restarting) is a failed attempt to run again, like any other.
  defp guarded(stage, job_arg) do
    Erase.run(stage, job_arg)
  rescue
    exception -> {:error, {:exception, Exception.message(exception)}}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp stage_result(state, tenant, %{progress: progress} = job, {:ok, counts}) do
    state = %{state | jobs: Map.put(state.jobs, tenant, %{job | ref: nil})}

    case Erase.next(progress.stage) do
      :done ->
        finish(state, tenant)

      next ->
        progress = %{
          progress
          | stage: next,
            counts: Map.merge(progress.counts, counts, fn _key, a, b -> a + b end)
        }

        case write_progress(state.instance, progress) do
          :ok ->
            state |> put_job(tenant, progress) |> run_stage(tenant)

          {:error, reason} ->
            Logger.warning(
              "[ankusa] erase of #{tenant} could not record stage #{progress.stage}, " <>
                "repeating the last: #{inspect(reason)}"
            )

            retry(state, tenant)
        end
    end
  end

  defp stage_result(state, tenant, %{progress: progress} = job, {:error, reason}) do
    Logger.warning(
      "[ankusa] erase of #{tenant}: stage #{progress.stage} failed, retrying: #{inspect(reason)}"
    )

    state = %{state | jobs: Map.put(state.jobs, tenant, %{job | ref: nil})}
    retry(state, tenant)
  end

  defp retry(state, tenant) do
    Process.send_after(self(), {:retry, tenant}, @retry_ms)
    state
  end

  # The commit stage wrote the tombstone and removed the progress row in one
  # batch: what is left is to let the tenant's traffic in again, as a new tenant.
  defp finish(state, tenant) do
    Gate.open(state.instance, tenant)
    Logger.info("[ankusa] tenant #{tenant} erased")
    %{state | jobs: Map.delete(state.jobs, tenant)}
  end

  # ── the gate ───────────────────────────────────────────────────────────────

  # Close the tenant, wait for the batch the writer may be committing right now,
  # and only then read its ordinal: a batch that started before the gate closed
  # may have allocated one, and none can after.
  defp close(instance, tenant) do
    Gate.close(instance, tenant, nil)

    case Ankusa.whereis(instance, :queue_writer) do
      nil -> :ok
      writer -> GenServer.call(writer, :barrier, :infinity)
    end

    Gate.close(instance, tenant, ord_of(instance, tenant))
  end

  # The ordinal the tenant has now, `nil` when it has none.
  defp ord_of(instance, tenant) do
    case Tenants.ord(instance, tenant) do
      {:ok, ord} -> ord
      _ -> nil
    end
  end

  defp write_progress(instance, progress) do
    Store.write(
      instance,
      [{:put, :default, Keys.erase(progress.tenant), Tenant.encode_progress(progress)}],
      sync: true
    )
  end
end
