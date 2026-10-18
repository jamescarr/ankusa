defmodule Ankusa.Store.Migration do
  @moduledoc false

  # One step of the store's format registry (`Ankusa.Store.Migrate`): the module
  # that moves a store from format `from` to format `to`.
  #
  # `step/2` is called again and again until it answers `:done`. Each call
  # returns one bounded batch of store ops and where the next call resumes;
  # `Ankusa.Store.Migrate` commits the batch and the resume cursor in one synced
  # write. A migration killed anywhere therefore restarts at the last cursor it
  # committed, with that batch's ops either all applied or not at all. A step
  # must be idempotent for the same cursor: replaying one whose commit never
  # reached the disk is the normal resume path.

  @typedoc "Where the next `c:step/2` call resumes; `nil` on the first call."
  @type cursor :: binary() | nil

  @callback step(instance :: atom(), cursor()) ::
              {:ok, [Ankusa.Store.op()], :done | {:more, binary()}} | {:error, term()}

  @doc "One line for `mix ankusa.store.migrate --dry-run`."
  @callback describe() :: String.t()
end
