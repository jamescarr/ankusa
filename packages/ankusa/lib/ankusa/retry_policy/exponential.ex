defmodule Ankusa.RetryPolicy.Exponential do
  @moduledoc """
  Capped binary exponential backoff with optional full jitter.

  `opts`:

    * `:base_ms`        — base delay (default `100`)
    * `:max_ms`         — delay ceiling before jitter (default `300_000`)
    * `:max_attempts`   — give up once `attempt` reaches this (default `nil`:
      no attempt limit, the horizon below is the bound)
    * `:jitter`         — multiply the delay by a random factor in `[0.5, 1.0]`
      (default `true`)
    * `:max_elapsed_ms` — the horizon (default `86_400_000`, 24 hours): dispatch
      dead-letters a delivery that is still failing this long after its first
      attempt (`Ankusa.RetryPolicy.schedule/5`). It is not read here; it is the
      same key every policy and every source's `retry:` uses.

  Defaults retry for 24 hours: 100 ms doubling to the 5-minute cap by attempt
  13, then 5 to 2.5 minutes apart (jitter halves a delay at worst), so roughly
  290 to 580 attempts before the dead letter. Set `max_elapsed_ms` lower to
  dead-letter sooner, or `max_attempts` to bound the count.
  """

  @behaviour Ankusa.RetryPolicy

  @impl true
  def backoff(attempt, opts) do
    base_ms = Keyword.get(opts, :base_ms, 100)
    max_ms = Keyword.get(opts, :max_ms, 300_000)
    max_attempts = Keyword.get(opts, :max_attempts)
    jitter = Keyword.get(opts, :jitter, true)

    if max_attempts != nil and attempt >= max_attempts do
      :give_up
    else
      delay = min(max_ms, base_ms * :math.pow(2, min(attempt - 1, 40)))
      delay = if jitter, do: delay * (0.5 + :rand.uniform() * 0.5), else: delay
      {:retry, round(delay)}
    end
  end
end
