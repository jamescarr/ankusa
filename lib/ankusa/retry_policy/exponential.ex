defmodule Ankusa.RetryPolicy.Exponential do
  @moduledoc """
  Capped binary exponential backoff with optional full jitter.

  `opts`:

    * `:base_ms`      — base delay (default `100`)
    * `:max_ms`       — delay ceiling before jitter (default `30_000`)
    * `:max_attempts` — give up once `attempt` reaches this (default `12`)
    * `:jitter`       — multiply the delay by a random factor in `[0.5, 1.0]`
      (default `true`)
  """

  @behaviour Ankusa.RetryPolicy

  @impl true
  def backoff(attempt, opts) do
    base_ms = Keyword.get(opts, :base_ms, 100)
    max_ms = Keyword.get(opts, :max_ms, 30_000)
    max_attempts = Keyword.get(opts, :max_attempts, 12)
    jitter = Keyword.get(opts, :jitter, true)

    if attempt >= max_attempts do
      :give_up
    else
      delay = min(max_ms, base_ms * :math.pow(2, attempt - 1))
      delay = if jitter, do: delay * (0.5 + :rand.uniform() * 0.5), else: delay
      {:retry, round(delay)}
    end
  end
end
