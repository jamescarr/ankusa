defmodule Ankusa.BlobStore.Retry do
  @moduledoc """
  Retries one object-store request after a transient failure, for the HTTP
  blob stores (`Ankusa.BlobStore.S3`, `GCS`, `Azure`, `OCI`), under one
  deadline and a per-`{endpoint, bucket}` breaker (`Ankusa.BlobStore.Breaker`).

  A transient failure is a transport error (refused, reset, timed out) or a
  `408`, `429`, `500`, `502`, `503` or `504`. Anything else — success, `404`, a
  `403` — is returned at once. The adapter option `:retries` (default `2`) is
  the number of extra attempts, `0` for a single attempt. Each retry waits a
  **full-jitter** delay: a uniformly random time between 0 and 200 ms, then 1 s,
  then 5 s for every later one, so nodes throttled together do not retry in
  lockstep. Each retry emits `[:ankusa, :blob_store, :retry]` (see
  `Ankusa.Telemetry`).

  ## One deadline

  `opts[:deadline]` (an absolute `System.monotonic_time(:millisecond)`; see
  `Ankusa.BlobStore.Options.with_deadline/1`) bounds the whole operation. Each
  attempt runs under the smaller of its timeout (`:timeout_ms` for a `:small`
  request, `:upload_timeout_ms` for a `:bulk` one) and the time left; nothing
  is sent, and no retry is slept, once it has passed (`{:error, :deadline}`), and
  a retry whose wait would not fit returns the last answer instead.

  ## `Retry-After`

  A `429` or `503` with a `Retry-After` (seconds, or an HTTP date) makes the
  next wait at least that long, **if it fits in the time left**. If it does not,
  nothing sleeps: the call returns `{:error, {:throttled, seconds}}` at once, for
  the caller (dispatch, which has its own retry and backoff) to schedule. The
  same is returned when the attempts are spent and the last answer was such a
  throttle. A throttle with no `Retry-After` is an ordinary `{:status, 429 | 503,
  body}`.

  ## Breaker

  An open breaker answers `{:error, :breaker_open}` without a request, and is
  not retried; every answered or failed attempt is recorded in it. A failure of
  this node (the connection pool gone, or out of free connections) is retried but
  not recorded: it says nothing about the bucket.

  Every object-store call is idempotent (a `PUT` writes the same bytes to the
  same key; a conditional one is settled by `Ankusa.BlobStore.same_object/5`),
  so a timed-out request that did land is safe to send again. The adapters
  build and sign the request inside `fun`, so each attempt carries a fresh date
  and signature. `Ankusa.HttpClient` itself never retries (`retry: false`): the
  sinks' retries are the dispatch pipeline's.
  """

  require Logger

  alias Ankusa.BlobStore.{Breaker, Options}

  @transient_statuses [408, 429, 500, 502, 503, 504]
  @backoff_ms [200, 1_000, 5_000]

  @type http_result ::
          {:ok, non_neg_integer(), %{String.t() => [String.t()]}, term()} | {:error, term()}

  @doc """
  Run `fun` — one `Ankusa.HttpClient.request_with_headers/6`, given the timeout
  of this attempt — for `instance` against the breaker `scope`
  (`{endpoint, bucket}`), retrying it per `opts[:retries]`. `kind` is `:small`
  or `:bulk` (see `Ankusa.BlobStore.Options.kind/1`). Raises `ArgumentError` on
  a `:retries` that is not a non-negative integer.
  """
  @spec run(atom(), keyword(), Breaker.scope(), :small | :bulk, (pos_integer() -> http_result())) ::
          http_result() | {:error, :deadline | :breaker_open | {:throttled, non_neg_integer()}}
  def run(instance, opts, scope, kind, fun)
      when kind in [:small, :bulk] and is_function(fun, 1) do
    retries =
      case Keyword.get(opts, :retries, Options.default(:retries)) do
        retries when is_integer(retries) and retries >= 0 ->
          retries

        other ->
          raise ArgumentError,
                "blob store :retries must be a non-negative integer, got #{inspect(other)}"
      end

    ctx = %{
      instance: instance,
      scope: scope,
      kind: kind,
      fun: fun,
      deadline:
        Keyword.get_lazy(opts, :deadline, fn -> Options.with_deadline(opts)[:deadline] end),
      timeout: Options.get(opts, if(kind == :small, do: :timeout_ms, else: :upload_timeout_ms)),
      breaker: Breaker.config(opts)
    }

    attempt(ctx, retries, 1)
  end

  defp attempt(ctx, retries, n) do
    remaining = ctx.deadline - now_ms()

    cond do
      remaining <= 0 ->
        {:error, :deadline}

      not Breaker.allow?(ctx.scope, ctx.breaker) ->
        {:error, :breaker_open}

      true ->
        started = now_ms()
        result = ctx.fun.(min(ctx.timeout, remaining))
        latency = if ctx.kind == :small, do: now_ms() - started

        record(ctx, outcome(result), latency)
        settle(ctx, result, retries, n)
    end
  end

  defp record(_ctx, :local, _latency), do: :ok

  defp record(ctx, outcome, latency),
    do: Breaker.record(ctx.instance, ctx.scope, ctx.breaker, outcome, latency)

  defp settle(ctx, result, retries, n) do
    retry_after = retry_after_ms(result)
    remaining = ctx.deadline - now_ms()

    cond do
      not transient?(result) ->
        result

      retry_after != nil and (retries == 0 or retry_after >= remaining) ->
        {:error, {:throttled, div(retry_after + 999, 1000)}}

      retries == 0 ->
        result

      true ->
        delay = max(jitter(n), retry_after || 0)

        if delay >= remaining do
          result
        else
          emit_retry(ctx.instance, result, n, delay)
          Process.sleep(delay)
          attempt(ctx, retries - 1, n + 1)
        end
    end
  end

  # Full jitter: anywhere from no wait to the whole step.
  defp jitter(n) do
    base = Enum.at(@backoff_ms, n - 1, List.last(@backoff_ms))
    round(base * :rand.uniform())
  end

  defp emit_retry(instance, result, n, delay) do
    Ankusa.Telemetry.emit(
      [:blob_store, :retry],
      %{delay_ms: delay},
      %{instance: instance, attempt: n, status: status(result)}
    )

    Logger.debug(
      "[ankusa] object store request failed (#{describe(result)}); retry #{n} in #{delay} ms"
    )
  end

  defp transient?({:ok, status, _headers, _body}), do: status in @transient_statuses
  defp transient?({:error, _reason}), do: true

  # What the breaker learns: the store could not answer; answered, but throttling
  # (reachable); or answered.
  # A failure of this node, not of the store: a connection pool process that is
  # gone (an `:exit`), or a pool with no free connection (Finch raises a
  # `RuntimeError`). It is retried like any transient failure, but says nothing
  # about the bucket, so it is not recorded.
  defp outcome({:error, {:exit, _reason}}), do: :local
  defp outcome({:error, %RuntimeError{}}), do: :local
  defp outcome({:error, _reason}), do: :failure
  defp outcome({:ok, status, _headers, _body}) when status in [429, 503], do: :answered
  defp outcome({:ok, status, _headers, _body}) when status in [408, 500, 502, 504], do: :failure
  defp outcome({:ok, _status, _headers, _body}), do: :ok

  defp status({:ok, status, _headers, _body}), do: status
  defp status({:error, _reason}), do: :transport

  defp describe({:ok, status, _headers, _body}), do: "status #{status}"
  defp describe({:error, reason}), do: inspect(reason, limit: 10, printable_limit: 256)

  # Only a throttle's `Retry-After` is honoured: seconds, or an HTTP date in the
  # future (`Ankusa.Sink.Http.retry_after_ms/1`, the parser the HTTP sink uses);
  # absent, zero, past or unparseable is no hint.
  defp retry_after_ms({:ok, status, headers, _body}) when status in [429, 503],
    do: Ankusa.Sink.Http.retry_after_ms(headers)

  defp retry_after_ms(_result), do: nil

  defp now_ms, do: System.monotonic_time(:millisecond)
end
