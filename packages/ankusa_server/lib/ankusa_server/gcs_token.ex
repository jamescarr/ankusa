defmodule AnkusaServer.GcsToken do
  @moduledoc """
  Bearer-token sources for `Ankusa.BlobStore.GCS`, wired up by `storage.gcs.auth`
  in the config file.

  Core's GCS adapter deliberately carries no credential dependency: it takes a
  `:token_provider` callback and calls it per request. For the container there is
  exactly one sensible default implementation, so it ships here rather than
  asking every operator to write one:

    * `static/1` — the operator set `auth: token`, and the token came from
      `${GCS_TOKEN}` or their secret store.
    * `metadata/0` — the node runs on GCE/GKE and gets a token from the instance
      metadata server.

  The metadata token lives in this process (started by
  `AnkusaServer.Application`), cached until under a minute of life remains.
  Refreshes are single-flight: callers queue on the one process, the first
  fetches, the rest get the token it fetched. A fetch is one request (1 s to
  connect, 5 s to answer, no retries); a failure is `:error`, which the blob
  store reports as an unauthenticated request. For a second after a failure
  every caller gets `:error` without another request, so a queue of callers
  behind a dead metadata server costs one fetch, not one each; after that the
  next call tries again.

  Anything else (Goth, workload identity, a vault agent) stays a matter of
  writing one function — the adapter's contract is `{:ok, token} | :error`.
  """

  use GenServer

  require Logger

  @metadata_url "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"

  # Refresh with a margin: a token that expires mid-request is the same as no
  # token, and the metadata server is cheap to ask.
  @refresh_margin_seconds 60

  # Longer than one fetch (1 s connect + 5 s receive), so a caller queued
  # behind a refresh gets its result rather than a timeout.
  @call_timeout_ms 10_000

  # How long a failed fetch answers for everyone before the next one is tried.
  @failure_backoff_ms 1_000

  @doc """
  Start the token cache. `:req_options` are merged into the metadata request
  (tests point it at a `Req.Test` stub); `:failure_backoff_ms` (default 1 000)
  is how long a failed fetch answers for every caller.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "A fixed token, as configured. Never expires."
  @spec static(String.t()) :: {:ok, String.t()}
  def static(token), do: {:ok, token}

  @doc """
  A token from the GCE/GKE instance metadata server, cached until it is close to
  expiring. `:error` on any failure, or when the cache process is not running.
  """
  @spec metadata() :: {:ok, String.t()} | :error
  def metadata do
    GenServer.call(__MODULE__, :token, @call_timeout_ms)
  catch
    :exit, reason ->
      Logger.warning("[ankusa] GCS metadata token unavailable: #{inspect(reason)}")
      :error
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       token: nil,
       expires_at: 0,
       failed_until: nil,
       failure_backoff_ms: Keyword.get(opts, :failure_backoff_ms, @failure_backoff_ms),
       req_options: Keyword.get(opts, :req_options, [])
     }}
  end

  @impl true
  def handle_call(:token, _from, state) do
    now = System.monotonic_time(:second)
    now_ms = System.monotonic_time(:millisecond)

    cond do
      state.token != nil and state.expires_at - now > @refresh_margin_seconds ->
        {:reply, {:ok, state.token}, state}

      # Callers queued behind a failed fetch get its answer instead of each
      # running another one against a metadata server that is down.
      state.failed_until != nil and now_ms < state.failed_until ->
        {:reply, :error, state}

      true ->
        case fetch(state.req_options) do
          {:ok, token, expires_in} ->
            {:reply, {:ok, token},
             %{state | token: token, expires_at: now + expires_in, failed_until: nil}}

          :error ->
            failed_until = System.monotonic_time(:millisecond) + state.failure_backoff_ms
            {:reply, :error, %{state | failed_until: failed_until}}
        end
    end
  end

  defp fetch(req_options) do
    options =
      Keyword.merge(
        [
          headers: [{"metadata-flavor", "Google"}],
          connect_options: [timeout: 1_000],
          receive_timeout: 5_000,
          retry: false
        ],
        req_options
      )

    case Req.get(@metadata_url, options) do
      {:ok, %{status: 200, body: body}} ->
        case token_and_expiry(body) do
          {:ok, token, expires_in} ->
            {:ok, token, expires_in}

          :error ->
            Logger.warning("[ankusa] GCS metadata token response had no access_token/expires_in")
            :error
        end

      {:ok, %{status: status}} ->
        Logger.warning("[ankusa] GCS metadata token request failed: HTTP #{status}")
        :error

      {:error, reason} ->
        Logger.warning("[ankusa] GCS metadata token request failed: #{inspect(reason)}")
        :error
    end
  end

  defp token_and_expiry(%{"access_token" => token, "expires_in" => expires_in})
       when is_binary(token) do
    case to_seconds(expires_in) do
      nil -> :error
      seconds -> {:ok, token, seconds}
    end
  end

  defp token_and_expiry(_body), do: :error

  # `expires_in` is seconds, sometimes quoted, depending on the metadata server.
  defp to_seconds(value) when is_integer(value), do: value

  defp to_seconds(value) when is_binary(value) do
    case Integer.parse(value) do
      {seconds, ""} -> seconds
      _ -> nil
    end
  end

  defp to_seconds(_value), do: nil
end
