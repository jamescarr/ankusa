defmodule Ankusa.RetryPolicy.ExponentialTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ankusa.RetryPolicy
  alias Ankusa.RetryPolicy.Exponential

  @hour 3_600_000

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

  test "the defaults have no attempt limit: the 24 h horizon is the bound, not an attempt count" do
    # The attempt count of the old default (84, about 6 h) is not a limit now,
    # and a very high one does not overflow the exponent.
    assert {:retry, _} = Exponential.backoff(84, [])
    assert {:retry, 300_000} = Exponential.backoff(10_000, jitter: false)
    assert RetryPolicy.max_elapsed_ms([]) == 24 * @hour
  end

  describe "the horizon (RetryPolicy.schedule/5)" do
    @policy {Exponential, [jitter: false, base_ms: 100, max_ms: 300_000]}

    defp row(attempts, first), do: %{attempts: attempts, first_attempt_at: first}

    test "the default policy keeps retrying for 24 hours, not 6, then gives up" do
      first = 1_000_000

      # 7 hours in: the old 84 attempts would have run out by now.
      assert {:retry, at} =
               RetryPolicy.schedule(
                 @policy,
                 {:transient, :x},
                 row(90, first),
                 first + 7 * @hour,
                 @hour
               )

      assert at == first + 7 * @hour + 300_000

      # 23 h 59 m 59 s in: still retrying, and the retry is clamped to the horizon
      now = first + 24 * @hour - 1_000

      assert {:retry, at} =
               RetryPolicy.schedule(@policy, {:transient, :x}, row(300, first), now, @hour)

      assert at == first + 24 * @hour

      # at the horizon: the dead letter
      assert RetryPolicy.schedule(
               @policy,
               {:transient, :x},
               row(301, first),
               first + 24 * @hour,
               @hour
             ) ==
               :give_up
    end

    test "a permanent failure gives up whatever time is left" do
      assert RetryPolicy.schedule(@policy, {:permanent, :gone}, row(1, 0), 1, @hour) == :give_up
    end

    test "a retry-after is capped at max_retry_after_ms and never beats the policy's backoff" do
      assert {:retry, at} =
               RetryPolicy.schedule(
                 @policy,
                 {:retry_after, 10 * @hour, :slow},
                 row(1, 0),
                 0,
                 @hour
               )

      assert at == @hour

      # the policy's own backoff (300 s at attempt 20) is longer than a 1 s ask
      assert {:retry, at} =
               RetryPolicy.schedule(@policy, {:retry_after, 1_000, :slow}, row(20, 0), 0, @hour)

      assert at == 300_000
    end

    test "a retry-after past the horizon is clamped to it" do
      horizon = 2 * @hour
      policy = {Exponential, [max_elapsed_ms: horizon]}

      assert {:retry, at} =
               RetryPolicy.schedule(
                 policy,
                 {:retry_after, 3 * @hour, :slow},
                 row(1, 0),
                 @hour + 1,
                 3 * @hour
               )

      assert at == horizon
    end

    test "the policy's own :give_up (max_attempts) still ends it before the horizon" do
      policy = {Exponential, [max_attempts: 3]}
      assert {:retry, _} = RetryPolicy.schedule(policy, {:transient, :x}, row(2, 0), 1, @hour)
      assert RetryPolicy.schedule(policy, {:transient, :x}, row(3, 0), 1, @hour) == :give_up
    end

    property "no schedule is due after first_attempt_at + max_elapsed_ms, whatever the policy and the failures" do
      check all(
              base_ms <- integer(0..5_000),
              max_ms <- integer(1..600_000),
              jitter <- boolean(),
              horizon <- integer(1_000..172_800_000),
              max_after <- integer(1_000..7_200_000),
              max_attempts <- one_of([constant(nil), integer(1..50)]),
              first <- integer(0..2_000_000_000_000),
              classes <-
                list_of(
                  one_of([
                    constant({:transient, :down}),
                    constant({:permanent, :gone}),
                    map(integer(1..50_000_000), &{:retry_after, &1, :slow})
                  ]),
                  min_length: 1,
                  max_length: 40
                ),
              max_runs: 200
            ) do
        opts = [
          base_ms: base_ms,
          max_ms: max_ms,
          jitter: jitter,
          max_elapsed_ms: horizon,
          max_attempts: max_attempts
        ]

        deadline = first + horizon

        # Walk the whole life of one delivery: attempt, fail, schedule, wait.
        {_now, last} =
          Enum.reduce_while(1..2_000, {first, :running}, fn attempt, {now, _} ->
            class = Enum.at(classes, rem(attempt - 1, length(classes)))

            case RetryPolicy.schedule(
                   {Exponential, opts},
                   class,
                   row(attempt, first),
                   now,
                   max_after
                 ) do
              {:retry, at} ->
                assert at >= now
                assert at <= deadline
                {:cont, {at, :running}}

              :give_up ->
                {:halt, {now, :gave_up}}
            end
          end)

        # A delivery past its horizon is always the dead letter, for any class.
        for class <- classes do
          assert RetryPolicy.schedule(
                   {Exponential, opts},
                   class,
                   row(1, first),
                   deadline,
                   max_after
                 ) ==
                   :give_up
        end

        assert last in [:running, :gave_up]
      end
    end
  end
end
