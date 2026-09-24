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
      metadata server. Tokens are cached in `:persistent_term` and refreshed
      when under a minute of life remains.

  Anything else (Goth, workload identity, a vault agent) stays a matter of
  writing one function — the adapter's contract is `{:ok, token} | :error`.
  """

  require Logger

  @metadata_url "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"

  # Refresh with a margin: a token that expires mid-request is the same as no
  # token, and the metadata server is cheap to ask.
  @refresh_margin_seconds 60

  @cache_key {__MODULE__, :token}

  @doc "A fixed token, as configured. Never expires."
  @spec static(String.t()) :: {:ok, String.t()}
  def static(token), do: {:ok, token}

  @doc """
  A token from the GCE/GKE instance metadata server, cached until it is close to
  expiring. `:error` on any failure, which the blob store reports as an
  unauthenticated request.
  """
  @spec metadata() :: {:ok, String.t()} | :error
  def metadata do
    now = System.monotonic_time(:second)

    case cached(now) do
      {:ok, token} -> {:ok, token}
      :error -> fetch(now)
    end
  end

  defp cached(now) do
    case :persistent_term.get(@cache_key, nil) do
      {token, expires_at} when expires_at - now > @refresh_margin_seconds -> {:ok, token}
      _ -> :error
    end
  end

  defp fetch(now) do
    options = [
      headers: [{"metadata-flavor", "Google"}],
      connect_options: [timeout: 1_000],
      receive_timeout: 5_000
    ]

    case Req.get(@metadata_url, options) do
      {:ok, %{status: 200, body: body}} ->
        case token_and_expiry(body) do
          {:ok, token, expires_in} ->
            :persistent_term.put(@cache_key, {token, now + expires_in})
            {:ok, token}

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
