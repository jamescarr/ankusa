defmodule Ankusa.BlobStore.Breaker do
  @moduledoc """
  A circuit breaker per `{endpoint, bucket}` for the HTTP object stores, shared
  by every instance and every store on the node that talks to that bucket.

  It opens on either of two signals:

    * **failures**: `breaker_failures` (default 5) consecutive requests that
      could not be answered (a transport error, `408`, `500`, `502`, `504`).
      A throttled answer (`429`, `503`) is an answer from a reachable store: it
      resets the count and is `Ankusa.BlobStore.Retry`'s to back off from.
    * **latency**: the p99 of the last 100 metadata-sized requests
      (`breaker_slow_ms`, default 5 s) once at least 20 are known. A bucket
      that still answers, but slowly, would otherwise hold every caller's
      connection for the full timeout until the pool is spent.

  Open, it answers `{:error, :breaker_open}` without sending anything, for
  `breaker_open_ms` (default 30 s). Then one request goes through as a probe:
  an answer closes the breaker, a failure opens it again. Each transition emits
  `[:ankusa, :blob_store, :breaker]` (see `Ankusa.Telemetry`).

  The state is one ETS row per `{endpoint, bucket}`, in a table
  `Ankusa.Application` owns. Concurrent requests update it without a lock: the
  worst a race does is lose a latency sample or let a second probe through.
  `breaker_failures: 0` turns the breaker off, the latency trip with it.
  """

  @table :ankusa_blob_store_breakers
  @samples 100
  @min_samples 20

  @type scope :: {endpoint :: String.t(), bucket :: String.t()}
  @type config :: %{failures: non_neg_integer(), open_ms: pos_integer(), slow_ms: pos_integer()}
  @type outcome :: :ok | :answered | :failure

  @doc false
  # Created once by `Ankusa.Application`, so the state outlives the request and
  # task processes that write it.
  @spec create_table() :: :ok
  def create_table do
    :ets.new(@table, [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ok
  end

  @doc false
  # Close every breaker (tests).
  @spec reset() :: :ok
  def reset do
    if :ets.whereis(@table) != :undefined, do: :ets.delete_all_objects(@table)
    :ok
  end

  @doc "The breaker settings of a store's options."
  @spec config(keyword()) :: config()
  def config(opts) do
    %{
      failures: Ankusa.BlobStore.Options.get(opts, :breaker_failures),
      open_ms: Ankusa.BlobStore.Options.get(opts, :breaker_open_ms),
      slow_ms: Ankusa.BlobStore.Options.get(opts, :breaker_slow_ms)
    }
  end

  @doc "May a request to `scope` be sent? An open breaker past its time lets one probe through."
  @spec allow?(scope(), config()) :: boolean()
  def allow?(_scope, %{failures: 0}), do: true

  def allow?(scope, %{open_ms: open_ms}) do
    case lookup(scope) do
      {_scope, :open, _failures, until, _samples} ->
        now = now_ms()

        if now < until do
          false
        else
          # Half-open: this request is the probe; the others wait out another window.
          :ets.update_element(@table, scope, {4, now + open_ms})
          true
        end

      _closed_or_unknown ->
        true
    end
  end

  @doc """
  Record the outcome of one request to `scope`: `:ok` (answered, `latency_ms`
  a sample unless `nil`), `:answered` (a throttle: reachable, no sample) or
  `:failure`.
  """
  @spec record(atom(), scope(), config(), outcome(), non_neg_integer() | nil) :: :ok
  def record(_instance, _scope, %{failures: 0}, _outcome, _latency), do: :ok

  def record(instance, scope, cfg, :failure, _latency) do
    {_scope, state, failures, _until, samples} = lookup(scope) || fresh(scope)
    failures = failures + 1

    if state == :open or failures >= cfg.failures do
      open(instance, scope, cfg, {:failures, failures}, samples)
    else
      :ets.insert(@table, {scope, :closed, failures, 0, samples})
      :ok
    end
  end

  def record(instance, scope, cfg, outcome, latency) when outcome in [:ok, :answered] do
    {_scope, state, _failures, _until, samples} = lookup(scope) || fresh(scope)
    samples = if outcome == :ok and latency != nil, do: sample(samples, latency), else: samples

    cond do
      state == :open ->
        # The probe was answered: close, and start the latency window afresh.
        :ets.insert(@table, {scope, :closed, 0, 0, []})
        emit(instance, scope, :closed, nil)

      too_slow?(samples, cfg.slow_ms) ->
        open(instance, scope, cfg, {:slow, p99(samples)}, [])

      true ->
        :ets.insert(@table, {scope, :closed, 0, 0, samples})
        :ok
    end
  end

  defp open(instance, {_endpoint, _bucket} = scope, cfg, reason, samples) do
    was_open? = match?({_scope, :open, _, _, _}, lookup(scope))
    :ets.insert(@table, {scope, :open, 0, now_ms() + cfg.open_ms, samples})
    if not was_open?, do: emit(instance, scope, :open, reason)
    :ok
  end

  defp emit({instance, {endpoint, bucket}, state, reason}) do
    Ankusa.Telemetry.emit([:blob_store, :breaker], %{}, %{
      instance: instance,
      endpoint: endpoint,
      bucket: bucket,
      state: state,
      reason: reason
    })
  end

  defp emit(instance, scope, state, reason), do: emit({instance, scope, state, reason})

  defp lookup(scope) do
    case :ets.lookup(@table, scope) do
      [row] -> row
      [] -> nil
    end
  rescue
    # No table (the application is not running): no breakers.
    ArgumentError -> nil
  end

  defp fresh(scope), do: {scope, :closed, 0, 0, []}

  defp sample(samples, latency), do: Enum.take([latency | samples], @samples)

  defp too_slow?(samples, slow_ms), do: length(samples) >= @min_samples and p99(samples) > slow_ms

  # Nearest rank over the window.
  defp p99(samples) do
    sorted = Enum.sort(samples)
    Enum.at(sorted, max(ceil(0.99 * length(sorted)) - 1, 0))
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
