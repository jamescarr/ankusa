defmodule Ankusa.Gateway.Authz.Remote do
  @moduledoc """
  The options every `:remote` authorizer takes, validated in one place
  (`options!/2`) and kept in its state under `:remote`, where the engine reads
  them:

  | Key | Default | Range |
  | --- | --- | --- |
  | `timeout_ms` | `200` | 10..30000 |
  | `max_inflight` | `64` | 1..10000 |
  | `queue_max` | `1024` | 0..100000 |
  | `batch_max` | `50` | 1..1000 |
  | `batch_window_ms` | `2` | 0..100 |
  | `breaker_failures` | `5` | 0..1000 (`0` = off) |
  | `breaker_open_ms` | `5000` | 100..600000 |
  | `allow_ttl_ms`, `deny_ttl_ms` | `nil` (the global `authz.cache` ones) | 0..3600000 |
  | `allow_ttl_max_ms` | `60000` | 0..3600000 |

  A `:cached` authorizer takes the three TTL keys alone (`cache_options!/2`).
  """

  alias Ankusa.Gateway.Options

  @ranges %{
    timeout_ms: {200, 10..30_000},
    max_inflight: {64, 1..10_000},
    queue_max: {1024, 0..100_000},
    batch_max: {50, 1..1000},
    batch_window_ms: {2, 0..100},
    breaker_failures: {5, 0..1000},
    breaker_open_ms: {5000, 100..600_000},
    allow_ttl_ms: {nil, 0..3_600_000},
    deny_ttl_ms: {nil, 0..3_600_000},
    allow_ttl_max_ms: {60_000, 0..3_600_000}
  }
  @cache_keys [:allow_ttl_ms, :deny_ttl_ms, :allow_ttl_max_ms]

  @type t :: %{
          optional(:timeout_ms) => pos_integer(),
          optional(:max_inflight) => pos_integer(),
          optional(:queue_max) => non_neg_integer(),
          optional(:batch_max) => pos_integer(),
          optional(:batch_window_ms) => non_neg_integer(),
          optional(:breaker_failures) => non_neg_integer(),
          optional(:breaker_open_ms) => pos_integer(),
          allow_ttl_ms: non_neg_integer() | nil,
          deny_ttl_ms: non_neg_integer() | nil,
          allow_ttl_max_ms: non_neg_integer()
        }

  @doc "The common keys a `:remote` authorizer understands."
  @spec keys() :: [atom()]
  def keys, do: Map.keys(@ranges)

  @doc """
  The remote options of `opts` (a map; other keys are ignored: the authorizer
  checks its own), with defaults. Raises `ArgumentError` naming `key.<option>`.
  """
  @spec options!(map(), String.t()) :: t()
  def options!(opts, key), do: take!(opts, key, Map.keys(@ranges))

  @doc "The TTL keys alone, for a `:cached` authorizer."
  @spec cache_options!(map(), String.t()) :: t()
  def cache_options!(opts, key), do: take!(opts, key, @cache_keys)

  defp take!(opts, key, keys) do
    Map.new(keys, fn k ->
      {default, range} = Map.fetch!(@ranges, k)

      value =
        case Map.get(opts, k, default) do
          nil -> nil
          value -> Options.int_in!(value, range, "#{key}.#{k}")
        end

      {k, value}
    end)
  end
end
