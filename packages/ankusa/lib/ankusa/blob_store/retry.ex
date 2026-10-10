defmodule Ankusa.BlobStore.Retry do
  @moduledoc """
  Retries one object-store request after a transient failure, for the HTTP
  blob stores (`Ankusa.BlobStore.S3`, `GCS`, `Azure`, `OCI`).

  A transient failure is a transport error (refused, reset, timed out) or a
  `408`, `429`, `500`, `502`, `503` or `504`. Anything else — success, `404`, a
  `403` — is returned at once. The adapter option `:retries` (default `2`) is
  the number of extra attempts, `0` for a single attempt; they wait 200 ms,
  then 1 s, then 5 s each after that. The last result is returned as it came,
  so each adapter maps its status the same as before.

  Every object-store call is idempotent (a `PUT` writes the same bytes to the
  same key), so a timed-out request that did land is safe to send again. The
  adapters build and sign the request inside `fun`, so each attempt carries a
  fresh date and signature. `Ankusa.HttpClient` itself never retries
  (`retry: false`): the sinks' retries are the dispatch pipeline's.
  """

  require Logger

  @transient_statuses [408, 429, 500, 502, 503, 504]
  @backoff_ms [200, 1_000, 5_000]

  @doc """
  Run `fun` — one `Ankusa.HttpClient.request/6` — retrying it per
  `opts[:retries]`. Raises `ArgumentError` on a `:retries` that is not a
  non-negative integer.
  """
  @spec run(keyword(), (-> result)) :: result
        when result: {:ok, non_neg_integer(), term()} | {:error, term()}
  def run(opts, fun) when is_function(fun, 0) do
    case Keyword.get(opts, :retries, 2) do
      retries when is_integer(retries) and retries >= 0 ->
        attempt(fun, retries, 1)

      other ->
        raise ArgumentError,
              "blob store :retries must be a non-negative integer, got #{inspect(other)}"
    end
  end

  defp attempt(fun, retries, n) do
    result = fun.()

    if retries > 0 and transient?(result) do
      delay = Enum.at(@backoff_ms, n - 1, List.last(@backoff_ms))

      Logger.debug(
        "[ankusa] object store request failed (#{describe(result)}); retry #{n} in #{delay} ms"
      )

      Process.sleep(delay)
      attempt(fun, retries - 1, n + 1)
    else
      result
    end
  end

  defp transient?({:ok, status, _body}), do: status in @transient_statuses
  defp transient?({:error, _reason}), do: true

  defp describe({:ok, status, _body}), do: "status #{status}"
  defp describe({:error, reason}), do: inspect(reason, limit: 10, printable_limit: 256)
end
