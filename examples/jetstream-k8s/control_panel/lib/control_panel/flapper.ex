defmodule ControlPanel.Flapper do
  @moduledoc """
  Timed chaos, for a sink that is intermittently unavailable rather than down:

  * A **flap** cycles one `ControlPanel.Chaos` lever: DENY for `down_s`, ALLOW
    for `up_s`, until stopped. Stopping it leaves the lever allowed.
  * A **rolling kill** SIGKILLs a random running pod of a group now and then
    every `every_s`.

  Starting either one again while it runs replaces its timings; the phase it
  is in restarts with the new duration.

  The Kubernetes calls run in supervised tasks, so a slow API never delays a
  timer, and the calls for one lever or group run one after another, so a
  stop's ALLOW cannot overtake the DENY before it. A failed call is logged.

  Publishes `{:flapper, snapshot}` on the `"flapper"` topic after every change
  and every transition.
  """

  use GenServer

  require Logger

  alias ControlPanel.{Chaos, K8s}

  @flap_range 5..300
  @kill_range 10..600
  @kill_groups ~w(nats ankusa ankusa-durable consumer)

  # flaps: lever => %{down_s, up_s, phase, cycles, timer, token}
  # kills: group => %{every_s, kills, timer, token}
  # workers: lever/group => the pid of its last Kubernetes call
  defstruct flaps: %{}, kills: %{}, workers: %{}

  @type snapshot :: %{
          flaps: %{
            String.t() => %{
              down_s: pos_integer(),
              up_s: pos_integer(),
              phase: :down | :up,
              cycles: pos_integer()
            }
          },
          kills: %{String.t() => %{every_s: pos_integer(), kills: non_neg_integer()}}
        }

  ## API

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @doc "Cycle `lever`: denied for `down_s`, then allowed for `up_s` (each clamped to 5-300 s)."
  @spec start_flap(String.t(), integer(), integer()) :: :ok | {:error, :unknown_lever}
  def start_flap(lever, down_s, up_s),
    do:
      GenServer.call(
        __MODULE__,
        {:start_flap, lever, clamp(down_s, @flap_range), clamp(up_s, @flap_range)}
      )

  @spec stop_flap(String.t()) :: :ok
  def stop_flap(lever), do: GenServer.call(__MODULE__, {:stop_flap, lever})

  @doc "Kill a random pod of `group` now, then every `every_s` (clamped to 10-600 s)."
  @spec start_rolling_kill(String.t(), integer()) :: :ok | {:error, :unknown_group}
  def start_rolling_kill(group, every_s),
    do: GenServer.call(__MODULE__, {:start_kill, group, clamp(every_s, @kill_range)})

  @spec stop_rolling_kill(String.t()) :: :ok
  def stop_rolling_kill(group), do: GenServer.call(__MODULE__, {:stop_kill, group})

  @spec snapshot() :: snapshot()
  def snapshot, do: GenServer.call(__MODULE__, :snapshot)

  @doc "The groups a rolling kill can target."
  @spec kill_groups() :: [String.t()]
  def kill_groups, do: @kill_groups

  defp clamp(value, first..last//_) when is_integer(value), do: value |> max(first) |> min(last)

  ## Server

  @impl true
  def init(_), do: {:ok, %__MODULE__{}}

  @impl true
  def handle_call(:snapshot, _from, state), do: {:reply, snapshot_of(state), state}

  def handle_call({:start_flap, lever, down_s, up_s}, _from, state) do
    cond do
      lever not in Chaos.levers() ->
        {:reply, {:error, :unknown_lever}, state}

      flap = state.flaps[lever] ->
        cancel(flap)
        flap = %{flap | down_s: down_s, up_s: up_s} |> schedule_flap(lever)
        {:reply, :ok, state |> put_in([Access.key(:flaps), lever], flap) |> broadcast()}

      true ->
        flap =
          %{down_s: down_s, up_s: up_s, phase: :down, cycles: 1, timer: nil, token: nil}
          |> schedule_flap(lever)

        state =
          state
          |> run(lever, "deny #{lever}", fn -> Chaos.deny(lever) end)
          |> put_in([Access.key(:flaps), lever], flap)

        {:reply, :ok, broadcast(state)}
    end
  end

  def handle_call({:stop_flap, lever}, _from, state) do
    case Map.pop(state.flaps, lever) do
      {nil, _flaps} ->
        {:reply, :ok, state}

      {flap, flaps} ->
        cancel(flap)

        state =
          %{state | flaps: flaps} |> run(lever, "allow #{lever}", fn -> Chaos.allow(lever) end)

        {:reply, :ok, broadcast(state)}
    end
  end

  def handle_call({:start_kill, group, _every_s}, _from, state) when group not in @kill_groups,
    do: {:reply, {:error, :unknown_group}, state}

  def handle_call({:start_kill, group, every_s}, _from, state) do
    state =
      case state.kills[group] do
        nil ->
          kill = schedule_kill(%{every_s: every_s, kills: 1, timer: nil, token: nil}, group)

          state
          |> run(group, "kill a #{group} pod", fn -> Chaos.kill_random(group) end)
          |> put_in([Access.key(:kills), group], kill)

        kill ->
          cancel(kill)

          put_in(
            state,
            [Access.key(:kills), group],
            schedule_kill(%{kill | every_s: every_s}, group)
          )
      end

    {:reply, :ok, broadcast(state)}
  end

  def handle_call({:stop_kill, group}, _from, state) do
    case Map.pop(state.kills, group) do
      {nil, _kills} ->
        {:reply, :ok, state}

      {kill, kills} ->
        cancel(kill)
        {:reply, :ok, broadcast(%{state | kills: kills})}
    end
  end

  @impl true
  def handle_info({:flap, lever, token}, state) do
    case state.flaps[lever] do
      %{token: ^token, phase: :down} = flap ->
        flap = schedule_flap(%{flap | phase: :up}, lever)

        state
        |> run(lever, "allow #{lever}", fn -> Chaos.allow(lever) end)
        |> put_in([Access.key(:flaps), lever], flap)
        |> broadcast()
        |> noreply()

      %{token: ^token, phase: :up} = flap ->
        flap = schedule_flap(%{flap | phase: :down, cycles: flap.cycles + 1}, lever)

        state
        |> run(lever, "deny #{lever}", fn -> Chaos.deny(lever) end)
        |> put_in([Access.key(:flaps), lever], flap)
        |> broadcast()
        |> noreply()

      # Stopped or restarted since this timer was set.
      _stale ->
        {:noreply, state}
    end
  end

  def handle_info({:kill, group, token}, state) do
    case state.kills[group] do
      %{token: ^token} = kill ->
        kill = schedule_kill(%{kill | kills: kill.kills + 1}, group)

        state
        |> run(group, "kill a #{group} pod", fn -> Chaos.kill_random(group) end)
        |> put_in([Access.key(:kills), group], kill)
        |> broadcast()
        |> noreply()

      _stale ->
        {:noreply, state}
    end
  end

  defp noreply(state), do: {:noreply, state}

  ## Timers

  # A fresh token per timer: a message from a cancelled timer that was already
  # in the mailbox is recognised as stale and dropped.
  defp schedule_flap(flap, lever) do
    seconds = if flap.phase == :down, do: flap.down_s, else: flap.up_s
    token = make_ref()
    timer = Process.send_after(self(), {:flap, lever, token}, seconds * 1_000)
    %{flap | timer: timer, token: token}
  end

  defp schedule_kill(kill, group) do
    token = make_ref()
    timer = Process.send_after(self(), {:kill, group, token}, kill.every_s * 1_000)
    %{kill | timer: timer, token: token}
  end

  defp cancel(%{timer: nil}), do: :ok
  defp cancel(%{timer: timer}), do: Process.cancel_timer(timer)

  ## Kubernetes calls

  # Runs `fun` in a task that first waits for the previous call on the same
  # key to finish.
  defp run(state, key, label, fun) do
    previous = state.workers[key]

    {:ok, pid} =
      Task.Supervisor.start_child(ControlPanel.TaskSupervisor, fn ->
        if previous, do: await_exit(previous)

        case fun.() do
          :ok -> :ok
          {:error, reason} -> Logger.warning("flapper: #{label}: #{describe(reason)}")
        end
      end)

    put_in(state, [Access.key(:workers), key], pid)
  end

  defp await_exit(pid) do
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, :process, _pid, _reason} -> :ok
    end
  end

  defp describe(:none), do: "no running pod in that group"
  defp describe(reason), do: K8s.describe(reason)

  ## Snapshot

  defp broadcast(state) do
    Phoenix.PubSub.broadcast(ControlPanel.PubSub, "flapper", {:flapper, snapshot_of(state)})
    state
  end

  defp snapshot_of(state) do
    %{
      flaps:
        Map.new(state.flaps, fn {lever, flap} ->
          {lever, Map.take(flap, [:down_s, :up_s, :phase, :cycles])}
        end),
      kills:
        Map.new(state.kills, fn {group, kill} -> {group, Map.take(kill, [:every_s, :kills])} end)
    }
  end
end
