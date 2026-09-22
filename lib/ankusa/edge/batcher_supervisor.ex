defmodule Ankusa.Edge.BatcherSupervisor do
  @moduledoc """
  A fixed pool of `Ankusa.Edge.Batcher` processes, one per partition (defaults to
  one per scheduler). A single commit process would be a throughput ceiling and a
  single point of failure; partitioning removes both.
  """

  use Supervisor

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    Supervisor.start_link(__MODULE__, opts, name: Ankusa.via(instance, :batcher_sup))
  end

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance)},
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    partitions = config.batcher.partitions

    children =
      for p <- 0..(partitions - 1) do
        Supervisor.child_spec(
          {Ankusa.Edge.Batcher, instance: config.instance, config: config, partition: p},
          id: {Ankusa.Edge.Batcher, p}
        )
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc "Partition an ingest key across the batcher pool."
  @spec partition(atom(), term()) :: non_neg_integer()
  def partition(instance, key) do
    partitions = Ankusa.config(instance).batcher.partitions
    :erlang.phash2(key, partitions)
  end
end
