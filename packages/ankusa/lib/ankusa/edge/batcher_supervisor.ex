defmodule Ankusa.Edge.BatcherSupervisor do
  @moduledoc """
  A fixed pool of `Ankusa.Edge.Batcher` processes, one per partition (four by
  default). A partition holds its callers' bodies and prepares their commit
  (`Ankusa.Queue.enqueue/3`: the encoded rows and the dedupe read) in its own
  task, in parallel with the others; `Ankusa.Queue.Writer` merges whatever every
  partition has queued into one group commit, so more partitions are not more
  fsyncs. The pool also buys fault isolation: one wedged partition leaves the
  others committing.
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
    instance = Keyword.fetch!(opts, :instance)
    partitions = Ankusa.config(instance).batcher.partitions

    children =
      for p <- 0..(partitions - 1) do
        Supervisor.child_spec(
          {Ankusa.Edge.Batcher, instance: instance, partition: p},
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
