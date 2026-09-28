defmodule Ankusa.RetryPolicy.ExponentialTest do
  use ExUnit.Case, async: true

  alias Ankusa.RetryPolicy.Exponential

  test "gives up once attempt reaches max_attempts" do
    assert {:retry, _} = Exponential.backoff(1, max_attempts: 2)
    assert Exponential.backoff(2, max_attempts: 2) == :give_up
    assert Exponential.backoff(3, max_attempts: 2) == :give_up
  end

  test "grows exponentially (jitter disabled) and caps at max_ms" do
    opts = [jitter: false, base_ms: 100, max_ms: 30_000, max_attempts: 100]

    delays =
      Enum.map(1..8, fn attempt ->
        {:retry, delay} = Exponential.backoff(attempt, opts)
        delay
      end)

    assert delays == [100, 200, 400, 800, 1600, 3200, 6400, 12_800]
    # non-decreasing
    assert delays == Enum.sort(delays)

    {:retry, capped} = Exponential.backoff(20, opts)
    assert capped == 30_000
  end

  test "jitter keeps the delay within [0.5, 1.0] of the uncapped value" do
    opts = [jitter: true, base_ms: 100, max_ms: 30_000, max_attempts: 100]

    for attempt <- 1..6 do
      full = 100 * :math.pow(2, attempt - 1)
      {:retry, delay} = Exponential.backoff(attempt, opts)
      assert delay >= round(full * 0.5)
      assert delay <= round(full)
    end
  end
end
