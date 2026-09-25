defmodule Ankusa.WAL.Conformance.Ra do
  @moduledoc """
  `Ankusa.WAL.Conformance.Adapter` for `Ankusa.WAL.Ra`, running a one-member
  cluster in-process.

  `stop/1` tears the GenServer, the Ra member *and* the Ra system down, so the
  suite's `restart/2` genuinely proves durability: the member recovers its seq
  floor, cursors, dedup ledger and lease tokens from its own log and snapshot,
  not from memory.
  """

  @behaviour Ankusa.WAL.Conformance.Adapter

  alias Ankusa.WAL.Ra

  @impl true
  def start(instance, config) do
    {:ok, _pid} = Ra.start_link(instance: instance, config: config)
    :ok
  end

  @impl true
  def stop(instance) do
    case Ankusa.whereis(instance, :wal) do
      nil ->
        :ok

      pid ->
        try do
          GenServer.stop(pid)
        catch
          :exit, _ -> :ok
        end
    end

    system = :"ankusa_ra_#{instance}"
    cluster = :"ankusa_wal_#{instance}"

    stop_server(system, cluster)
    :ra_system.stop(system)
    :ok
  end

  @impl true
  def restart(instance, config) do
    :ok = stop(instance)
    start(instance, config)
  end

  defp stop_server(system, cluster) do
    _ = :ra.stop_server(system, {cluster, node()})
    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end
end
