defmodule Ankusa.RetryPolicy do
  @moduledoc """
  Backoff and give-up rules for dispatch.

  A policy decides *how long to wait* (`c:backoff/2`) and may give up early
  (`:give_up`). The wall-clock horizon is not the policy's to enforce: dispatch
  gives every delivery `max_elapsed_ms` from its first attempt (the
  `:max_elapsed_ms` policy option, default 24 hours; a source's `retry:`
  overrides it) and dead-letters a delivery that is still failing once that has
  passed, whatever `c:backoff/2` answers. `schedule/5` is that rule as a pure
  function: no retry it returns is due later than the horizon.
  """

  @doc """
  Given the attempt number (1-based, the attempt that just failed), return
  `{:retry, delay_ms}` to retry after a delay, or `:give_up` to dead-letter.
  """
  @callback backoff(attempt :: pos_integer(), opts :: keyword()) ::
              {:retry, non_neg_integer()} | :give_up

  @default_max_elapsed_ms 86_400_000
  @max_max_elapsed_ms 2_592_000_000
  @min_max_elapsed_ms 1_000

  # The options a source's `retry:` may carry; the policy module stays the
  # instance's.
  @keys [:base_ms, :max_ms, :max_attempts, :jitter, :max_elapsed_ms]

  @doc "The default horizon: 24 hours from a delivery's first attempt."
  @spec default_max_elapsed_ms() :: pos_integer()
  def default_max_elapsed_ms, do: @default_max_elapsed_ms

  @doc "The horizon, in milliseconds, of a policy's `opts`."
  @spec max_elapsed_ms(keyword()) :: pos_integer()
  def max_elapsed_ms(opts) when is_list(opts),
    do: Keyword.get(opts, :max_elapsed_ms, @default_max_elapsed_ms)

  @doc """
  Validate a per-source `retry:` (a keyword list or a map with atom keys) and
  return it as a keyword list. `nil` is no override. Raises `ArgumentError`
  naming `label` for an unknown key or a value out of range.
  """
  @spec normalize_opts!(nil | keyword() | map(), String.t()) :: keyword() | nil
  def normalize_opts!(nil, _label), do: nil

  def normalize_opts!(opts, label) when is_map(opts) and not is_struct(opts),
    do: normalize_opts!(Map.to_list(opts), label)

  def normalize_opts!(opts, label) when is_list(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError, "#{label}: retry must be a keyword list or a map, got #{inspect(opts)}"
    end

    for {key, value} <- opts do
      unless key in @keys do
        raise ArgumentError,
              "#{label}: unknown retry key #{inspect(key)}; expected one of: " <>
                Enum.map_join(@keys, ", ", &Atom.to_string/1)
      end

      validate_opt!(key, value, label <> ": retry")
    end

    opts
  end

  def normalize_opts!(other, label) do
    raise ArgumentError, "#{label}: retry must be a keyword list or a map, got #{inspect(other)}"
  end

  @doc """
  The range check of one retry option, shared by `normalize_opts!/2` and
  `Ankusa.Config`. `label` is the dotted path of the `retry` block. Raises
  `ArgumentError`.
  """
  @spec validate_opt!(atom(), term(), String.t()) :: :ok
  def validate_opt!(:base_ms, value, label), do: int!(value, 0, 3_600_000, label <> ".base_ms")
  def validate_opt!(:max_ms, value, label), do: int!(value, 1, 86_400_000, label <> ".max_ms")

  def validate_opt!(:max_attempts, nil, _label), do: :ok

  def validate_opt!(:max_attempts, value, label),
    do: int!(value, 1, 1_000_000, label <> ".max_attempts")

  def validate_opt!(:max_elapsed_ms, value, label),
    do: int!(value, @min_max_elapsed_ms, @max_max_elapsed_ms, label <> ".max_elapsed_ms")

  def validate_opt!(:jitter, value, _label) when is_boolean(value), do: :ok

  def validate_opt!(:jitter, value, label),
    do: raise(ArgumentError, "#{label}.jitter must be true or false, got #{inspect(value)}")

  defp int!(value, lo, hi, _key) when is_integer(value) and value >= lo and value <= hi, do: :ok

  defp int!(value, lo, hi, key) do
    raise ArgumentError, "#{key} must be an integer from #{lo} to #{hi}, got #{inspect(value)}"
  end

  @doc """
  The policy a source runs under: the instance's `{module, opts}` with the
  source's `retry:` options laid over `opts`.
  """
  @spec for_source({module(), keyword()}, keyword() | nil) :: {module(), keyword()}
  def for_source({mod, opts}, nil), do: {mod, opts}
  def for_source({mod, opts}, override), do: {mod, Keyword.merge(opts, override)}

  @doc "When a delivery whose first attempt was at `first_attempt_at` runs out of time."
  @spec deadline(integer(), {module(), keyword()}) :: integer()
  def deadline(first_attempt_at, {_mod, opts}), do: first_attempt_at + max_elapsed_ms(opts)

  @doc """
  What to do with a delivery that just failed, as the absolute time of its
  next attempt (`now` is the clock in milliseconds): `{:retry, at}` or
  `:give_up`.

  `class` is `Ankusa.Sink.classify/1`'s answer. A `{:permanent, _}` failure, a
  delivery that is past its horizon (`first_attempt_at + max_elapsed_ms`) and a
  policy that answers `:give_up` are all `:give_up`. A `{:retry_after, ms, _}`
  failure waits the later of the policy's backoff and `ms`, with `ms` capped at
  `max_retry_after_ms`. `at` is never later than the horizon, so the last
  attempt runs on it and the one after that is the dead letter.
  """
  @spec schedule(
          {module(), keyword()},
          Ankusa.Sink.class(),
          %{attempts: pos_integer(), first_attempt_at: integer()},
          integer(),
          pos_integer()
        ) :: {:retry, integer()} | :give_up
  def schedule({mod, opts} = policy, class, row, now, max_retry_after_ms) do
    deadline = deadline(row.first_attempt_at, policy)

    cond do
      match?({:permanent, _}, class) ->
        :give_up

      now >= deadline ->
        :give_up

      true ->
        case mod.backoff(row.attempts, opts) do
          :give_up ->
            :give_up

          {:retry, delay} ->
            delay =
              case class do
                {:retry_after, ms, _term} -> max(delay, min(ms, max_retry_after_ms))
                _ -> delay
              end

            {:retry, min(now + delay, deadline)}
        end
    end
  end
end
