defmodule Hook.RetryPolicy do
  @moduledoc "Backoff and give-up rules for dispatch."

  @doc """
  Given the attempt number (1-based, the attempt that just failed), return
  `{:retry, delay_ms}` to retry after a delay, or `:give_up` to dead-letter.
  """
  @callback backoff(attempt :: pos_integer(), opts :: keyword()) ::
              {:retry, non_neg_integer()} | :give_up
end
