defmodule Ankusa.Instance.RegistryWatch do
  @moduledoc """
  Stops an instance when `Ankusa.Registry` restarts, so its supervisor starts
  it again.

  Every instance process registers by name in `Ankusa.Registry`, and
  registering links it to the Registry's partition. A partition that dies takes
  every registrant that does not trap exits (batchers, writer, rate limiter,
  quarantine, the route cache…) down with it, and their supervisors restart
  them, registered anew once the partition is back. A registrant that traps
  exits (the store, every supervisor, `Ankusa.Instance.Isolated`) survives
  instead, unregistered: `Ankusa.whereis/2` returns `nil` for it and
  `Ankusa.via/2` calls to it fail, and nothing puts it back.

  The watcher monitors the Registry supervisor and every partition under it,
  because a partition crash restarts the partitions while the supervisor stays
  up. Its own exit shuts the whole instance down: the instance's root supervisor
  lists it as a *significant* child with `auto_shutdown: :any_significant`, and
  whatever supervises the instance starts it again, every process registered
  in the new Registry.
  """

  use GenServer

  require Logger

  def start_link(instance), do: GenServer.start_link(__MODULE__, instance)

  def child_spec(instance) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [instance]},
      restart: :temporary,
      significant: true
    }
  end

  @impl true
  def init(instance) do
    with sup when is_pid(sup) <- Process.whereis(Ankusa.Registry),
         {:ok, partitions} <- partitions(sup) do
      Enum.each([sup | partitions], &Process.monitor/1)
      {:ok, %{instance: instance}}
    else
      _not_running -> {:stop, :registry_not_running}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, reason}, %{instance: instance} = state) do
    Logger.error(
      "[ankusa] Ankusa.Registry restarted (#{inspect(reason)}); stopping instance " <>
        "#{instance} so its supervisor restarts it re-registered"
    )

    {:stop, :normal, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # A Registry mid-restart has no partition to monitor yet: refuse to start
  # (the instance's supervisor retries) rather than watch half of it.
  defp partitions(sup) do
    partitions = for {_id, pid, _type, _modules} <- Supervisor.which_children(sup), do: pid

    if Enum.all?(partitions, &is_pid/1), do: {:ok, partitions}, else: :restarting
  catch
    :exit, _ -> :not_running
  end
end
