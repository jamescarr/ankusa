defmodule Ankusa.Dispatch do
  @moduledoc """
  Dispatch-facing helpers. The running pipeline lives in
  `Ankusa.Dispatch.Pipeline`; this module hosts operator affordances such as
  replaying dead-lettered hooks.
  """

  @doc """
  Re-deliver dead-lettered hooks: their delivery rows go back to pending with a
  fresh attempt count, and the pipeline delivers them through the source's
  *current* sinks and options.

  `filter` (map or keyword) may narrow by `:source_id`, `:id`, and/or `:since`
  (a unix-ms lower bound on when the entry was dead-lettered). Returns the
  number of rows moved back to pending; delivery itself is asynchronous, and a
  row that fails again is dead-lettered again.

  Needs the `:dispatch` role on this node. While the pipeline is restarting (or
  has just crashed) there is nobody to ask, which answers
  `{:error, :store_unavailable}` like an unreachable store rather than exiting
  the caller.
  """
  @spec replay(atom(), map() | keyword()) ::
          {:ok, non_neg_integer()} | {:error, :store_unavailable}
  def replay(instance, filter \\ %{}) do
    GenServer.call(Ankusa.via(instance, :dispatch), {:replay, Map.new(filter)}, 60_000)
  catch
    :exit, _reason -> {:error, :store_unavailable}
  end
end
