defmodule Ankusa.Instance.Drain do
  @moduledoc """
  What a node does before its supervision tree stops, so a stop never turns a
  request that is being answered, or a hook that is being committed, into a
  connection reset. Both applications run it from `prep_stop/1` (core's for every
  instance in its own tree, `AnkusaServer.Application`'s for the one it owns),
  which the VM calls when it is told to stop: SIGTERM, which is what `docker stop`
  and a Kubernetes pod deletion send.

  In order:

    1. `Ankusa.Health.drain/1`: `/ready` answers `503` with `"status": "draining"`
       from this moment, so a load balancer stops choosing the node.
    2. Wait `listener.drain_delay_ms` (5 s by default), still serving: a load
       balancer needs a probe interval or two to notice, and a request it already
       routed must find the listener open.
    3. On a node with the `:edge` role, stop the ingress listener alone. It stops
       accepting, then waits `Ankusa.Listener.ingress_shutdown_timeout_ms/1` (the
       batcher's record deadline, the store commit and a margin) for the requests
       it is serving, so a commit that starts late is not killed mid-fsync, and
       closes what is left.
    4. Wait until every batcher holds nothing (`Ankusa.Edge.Batcher.queued/2`),
       bounded by `listener.drain_timeout_ms` (30 s) from the end of the delay.
       Under `wal: :none` there are no batchers: a request waits for its sinks
       inside the listener's own wait.

  It returns, and the tree stops in its own order: dispatch writes the outcomes it
  still holds when it terminates, the backup takes its final
  checkpoint (`Ankusa.Store.Backup`, bounded by `backup.final_timeout_ms`), and the
  store closes last. A node's grace period therefore has to cover the delay, the
  drain timeout and the stop itself: set `terminationGracePeriodSeconds` to at
  least `drain_delay_ms + drain_timeout_ms`, in seconds, plus 10 (and
  `backup.final_timeout_ms` when the node backs up).

  `run/1` never raises or exits: a drain that cannot finish logs why and lets the
  stop go on.
  """

  require Logger

  alias Ankusa.Config
  alias Ankusa.Edge.Batcher
  alias Ankusa.Health

  @poll_ms 20

  @doc """
  Drain `instance` (see the moduledoc). Blocks until it is done, and answers `:ok`
  whatever happened; an instance that never started has nothing to drain.
  """
  @spec run(atom()) :: :ok
  def run(instance) do
    case Ankusa.fetch_config(instance) do
      {:ok, config} -> drain(config)
      :error -> :ok
    end
  rescue
    exception ->
      Logger.warning("[ankusa] drain of #{instance} failed: #{Exception.message(exception)}")
      :ok
  catch
    kind, reason ->
      Logger.warning("[ankusa] drain of #{instance} failed: #{inspect({kind, reason})}")
      :ok
  end

  defp drain(%Config{instance: instance, listener: listener} = config) do
    started = now()
    Health.drain(instance)

    Logger.info(
      "[ankusa] draining #{instance}: /ready answers 503 draining, " <>
        "the ingress listener closes in #{listener.drain_delay_ms} ms"
    )

    Process.sleep(listener.drain_delay_ms)

    if Config.role?(config, :edge) do
      deadline = now() + listener.drain_timeout_ms
      close_ingress(instance)
      wait_for_batchers(config, deadline)
    end

    Logger.info("[ankusa] drained #{instance} in #{now() - started} ms; stopping")
    :ok
  end

  # The one child of the edge subtree that takes requests. `terminate_child` returns
  # when its connections are done or killed at `ingress_shutdown_timeout_ms/1`.
  defp close_ingress(instance) do
    case Supervisor.terminate_child(Ankusa.via(instance, :edge), :ingress) do
      :ok -> :ok
      {:error, :not_found} -> :ok
    end
  catch
    :exit, reason ->
      Logger.warning(
        "[ankusa] could not close the ingress listener of #{instance}: #{inspect(reason)}"
      )
  end

  defp wait_for_batchers(%Config{wal: :none}, _deadline), do: :ok

  defp wait_for_batchers(config, deadline) do
    case queued(config) do
      0 ->
        :ok

      held ->
        if now() >= deadline do
          Logger.warning(
            "[ankusa] #{config.instance} still holds #{held} record(s) in its batchers after " <>
              "listener.drain_timeout_ms (#{config.listener.drain_timeout_ms}); stopping anyway"
          )
        else
          Process.sleep(@poll_ms)
          wait_for_batchers(config, deadline)
        end
    end
  end

  defp queued(config) do
    Enum.sum(for partition <- 0..(config.batcher.partitions - 1), do: queued(config, partition))
  end

  # A batcher that is gone holds nothing.
  defp queued(config, partition) do
    Batcher.queued(config.instance, partition)
  catch
    :exit, _ -> 0
  end

  defp now, do: System.monotonic_time(:millisecond)
end
