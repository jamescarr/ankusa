defmodule Ankusa.BlobStore.GCS.MetadataServer do
  @moduledoc """
  Access tokens for `Ankusa.BlobStore.GCS` from the GCE/GKE metadata server: the
  best credential for a service running on Google Cloud, with no key to store or
  rotate (the node's service account, granted `roles/storage.objectAdmin` on the
  bucket).

      {Ankusa.BlobStore.GCS,
       bucket: "ankusa-segments",
       token_provider: {Ankusa.BlobStore.GCS.MetadataServer, :token, []}}

  The token comes from
  `http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/<account>/token`
  (the `Metadata-Flavor: Google` header is required; the address only answers
  inside Google Cloud). It is cached node-wide until a minute before it expires, so
  the adapter's per-request call is an ETS read, and refreshed on demand: a `401`
  makes the adapter call `invalidate/1`, and its retry fetches a new token.

  Arguments (a keyword list, the one optional argument of `token/1` and
  `invalidate/1`):

    * `:service_account` — default `"default"`
    * `:endpoint` — default the metadata server's token URL (override in tests)
    * `:timeout_ms` — default `5_000`
    * `:req_options` — transport options, e.g. `plug:` for `Req.Test`
  """

  alias Ankusa.BlobStore.TokenCache
  alias Ankusa.HttpClient

  @window_s 60
  @default_host "http://metadata.google.internal"

  @doc "`{:ok, bearer_token}`, or `:error` when the metadata server did not give one."
  @spec token(keyword()) :: {:ok, String.t()} | :error
  def token(opts \\ []) do
    case TokenCache.fetch(key(opts), @window_s, fn -> fetch(opts) end) do
      {:ok, token} -> {:ok, token}
      _error -> :error
    end
  end

  @doc "Drop the cached token: the next `token/1` fetches a new one."
  @spec invalidate(keyword()) :: :ok
  def invalidate(opts \\ []), do: TokenCache.invalidate(key(opts))

  defp key(opts), do: {:gcs_metadata, url(opts)}

  defp url(opts) do
    Keyword.get_lazy(opts, :endpoint, fn ->
      account = Keyword.get(opts, :service_account, "default")
      "#{@default_host}/computeMetadata/v1/instance/service-accounts/#{account}/token"
    end)
  end

  defp fetch(opts) do
    timeout = Keyword.get(opts, :timeout_ms, 5_000)
    req_options = Keyword.get(opts, :req_options, [])

    with {:ok, 200, body} <-
           HttpClient.request(
             :get,
             url(opts),
             [{"metadata-flavor", "Google"}],
             nil,
             timeout,
             req_options
           ),
         {:ok, %{"access_token" => token, "expires_in" => expires_in}}
         when is_binary(token) and is_integer(expires_in) <- JSON.decode(body) do
      {:ok, token, System.system_time(:second) + expires_in}
    else
      _failed -> :error
    end
  end
end
