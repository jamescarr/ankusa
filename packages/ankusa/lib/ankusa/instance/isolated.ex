defmodule Ankusa.Instance.Isolated do
  @moduledoc """
  Owns one optional subtree of an instance (dispatch, storage, lifecycle,
  metrics, or one of the secondary listeners) and never escalates its failure.

  A plain child of `Ankusa.Instance` shares the root's restart budget: a
  subtree that crashes in a loop (a sink whose adapter keeps raising at boot, a
  port another process took) exhausts it and takes the whole instance down —
  edge listener included, so the node stops acking hooks because something it
  did not need to ack them is broken. This manager starts the subtree under its
  own `:one_for_one` supervisor and, when that supervisor gives up (its restart
  intensity is exhausted), does not exit. It waits and starts the subtree again,
  backing off from `:base_backoff_ms` doubling up to `:max_backoff_ms` while
  restarts keep failing, and starting over once the subtree has stayed up for a
  minute.

  A child that cannot start when the instance boots still fails the boot: a
  misconfigured listener or sink should stop a deploy, not be retried quietly.

  Every outage emits `[:ankusa, :instance, :subtree_down]` and every recovery
  `[:ankusa, :instance, :subtree_up]` (see `Ankusa.Telemetry`).

  ## Options

    * `:instance` — the instance name (required)
    * `:domain` — an atom naming the domain, e.g. `:dispatch` (required)
    * `:children` — the child specs of the subtree (required)
    * `:base_backoff_ms` — first restart delay, default `1_000`
    * `:max_backoff_ms` — restart delay ceiling, default `60_000`

  The two backoff options exist for tests.
  """

  use GenServer

  require Logger

  # A subtree that has been up this long is healthy again: the next outage
  # starts the backoff over.
  @stable_ms 60_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    domain = Keyword.fetch!(opts, :domain)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, {:isolated, domain}))
  end

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :domain)},
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      # The subtree flushes on the way down (dispatch records its outcomes
      # before the store closes): the root waits for it, however long.
      shutdown: :infinity
    }
  end

  @doc """
  The pid of the domain's supervisor, or `nil` while it is down (or the
  manager is not running).
  """
  @spec subtree(atom(), atom()) :: pid() | nil
  def subtree(instance, domain) do
    case Ankusa.whereis(instance, {:isolated, domain}) do
      nil -> nil
      manager -> GenServer.call(manager, :subtree)
    end
  catch
    :exit, _ -> nil
  end

  # ── server ────────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    children = Keyword.fetch!(opts, :children)

    case start_subtree(children) do
      {:ok, sup} ->
        {:ok,
         %{
           instance: Keyword.fetch!(opts, :instance),
           domain: Keyword.fetch!(opts, :domain),
           children: children,
           sup: sup,
           failures: 0,
           started_at: now(),
           base: Keyword.get(opts, :base_backoff_ms, 1_000),
           max: Keyword.get(opts, :max_backoff_ms, 60_000)
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call(:subtree, _from, state), do: {:reply, state.sup, state}

  @impl true
  def handle_info({:EXIT, sup, reason}, %{sup: sup} = state) do
    # The subtree's own restart budget is spent (or something killed its
    # supervisor): the only things that stop it.
    failures = if now() - state.started_at >= @stable_ms, do: 1, else: state.failures + 1
    {:noreply, schedule_restart(state, reason, failures)}
  end

  def handle_info(:restart, %{sup: nil} = state) do
    case start_subtree(state.children) do
      {:ok, sup} ->
        Logger.info("[ankusa] #{state.domain} subtree restarted")

        Ankusa.Telemetry.emit([:instance, :subtree_up], %{}, %{
          instance: state.instance,
          domain: state.domain
        })

        {:noreply, %{state | sup: sup, started_at: now()}}

      {:error, reason} ->
        {:noreply, schedule_restart(state, reason, state.failures + 1)}
    end
  end

  # The EXIT of a supervisor that failed inside `start_link/2` (its reason is
  # already in the `{:error, _}` we got), and stale timers.
  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{sup: sup}) when is_pid(sup) do
    # The root waits for this: the subtree finishes stopping (dispatch flushes
    # its outcomes) before whatever it depends on, the store, closes.
    try do
      Supervisor.stop(sup, :shutdown, :infinity)
    catch
      :exit, _ -> :ok
    end

    :ok
  end

  def terminate(_reason, _state), do: :ok

  # Crash reports print a process's state, and the subtree's child specs carry
  # sink options (broker URLs, credentials, tokens).
  @impl true
  def format_status(%{state: %{children: children} = state} = status) do
    ids = Enum.map(children, fn child -> Supervisor.child_spec(child, []).id end)
    %{status | state: %{state | children: ids}}
  end

  def format_status(status), do: status

  # ── subtree ───────────────────────────────────────────────────────────────

  defp start_subtree(children), do: Supervisor.start_link(children, strategy: :one_for_one)

  defp schedule_restart(state, reason, failures) do
    delay = min(state.base * 2 ** (failures - 1), state.max)

    Logger.error(
      "[ankusa] #{state.domain} subtree stopped (#{inspect(reason)}) after exhausting its " <>
        "restart budget; restarting it in #{delay} ms"
    )

    Ankusa.Telemetry.emit([:instance, :subtree_down], %{delay_ms: delay}, %{
      instance: state.instance,
      domain: state.domain,
      reason: reason
    })

    Process.send_after(self(), :restart, delay)
    %{state | sup: nil, failures: failures}
  end

  defp now, do: System.monotonic_time(:millisecond)
end
