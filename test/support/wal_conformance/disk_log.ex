defmodule Ankusa.WAL.Conformance.DiskLog do
  @moduledoc """
  `Ankusa.WAL.Conformance.Adapter` for `Ankusa.WAL.DiskLog`, so the shared
  conformance suite can run against the default local WAL.

  The WAL is started linked to the calling test process rather than under
  `start_supervised!/2`, because the suite's `restart/2` has to stop and start
  it again to prove that state (records, cursors, dedup, seq floor, lease
  tokens) really is durable.
  """

  @behaviour Ankusa.WAL.Conformance.Adapter

  alias Ankusa.WAL.DiskLog

  @impl true
  def start(instance, config) do
    {:ok, _pid} = DiskLog.start_link(instance: instance, config: config)
    :ok
  end

  @impl true
  def stop(instance) do
    case Ankusa.whereis(instance, :wal) do
      nil ->
        :ok

      pid ->
        # The WAL is linked to the test process, so it may already be dying by
        # the time the suite's `on_exit` runs.
        try do
          GenServer.stop(pid)
        catch
          :exit, _ -> :ok
        end
    end

    :ok
  end

  @impl true
  def restart(instance, config) do
    :ok = stop(instance)
    start(instance, config)
  end
end
