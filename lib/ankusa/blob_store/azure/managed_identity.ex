defmodule Ankusa.BlobStore.Azure.ManagedIdentity do
  @moduledoc """
  Azure AD access tokens from the Instance Metadata Service (IMDS) — Azure's
  *managed identity*, the best credential for a service running on Azure: no
  secret to store or rotate, short-lived tokens, and least privilege via a
  role assignment (`Storage Blob Data Contributor`) on the identity rather
  than an account key.

  Used as the `:token_provider` for `Ankusa.BlobStore.Azure`:

      # system-assigned identity — no config at all
      {Ankusa.BlobStore.Azure.ManagedIdentity, :token, []}

      # user-assigned identity
      {Ankusa.BlobStore.Azure.ManagedIdentity,
       :token, [client_id: System.get_env("AZURE_CLIENT_ID")]}

  The token is fetched from `http://169.254.169.254/metadata/identity/oauth2/token`
  (only reachable from inside Azure) with the required `Metadata: true` header.
  It is cached and refreshed five minutes before expiry, so the adapter's
  per-request `:token_provider` call is an ETS read, not an IMDS round-trip.

  opts:

    * `:client_id` — a user-assigned managed identity's client id; omit for the
                      system-assigned identity
    * `:resource`  — default `"https://storage.azure.com"` (Blob Storage)
    * `:endpoint`  — default the IMDS token URL; override in tests
    * `:timeout_ms` — default `10_000`
    * `:req_options` — transport options, e.g. `plug:` for `Req.Test`
  """

  alias Ankusa.HttpClient

  @table __MODULE__
  @refresh_window 5 * 60

  @default_endpoint "http://169.254.169.254/metadata/identity/oauth2/token"
  @default_resource "https://storage.azure.com"
  @api_version "2018-02-01"

  @doc false
  @spec token(keyword()) :: {:ok, String.t()} | :error
  def token(opts) do
    key =
      {Keyword.get(opts, :endpoint, @default_endpoint),
       Keyword.get(opts, :resource, @default_resource), Keyword.get(opts, :client_id)}

    case cached(key, System.system_time(:second)) do
      {:ok, token} ->
        {:ok, token}

      :miss ->
        case fetch(opts) do
          {:ok, token, expires_at} ->
            :ets.insert(table(), {key, token, expires_at})
            {:ok, token}

          :error ->
            :error
        end
    end
  end

  defp cached(key, now) do
    case :ets.lookup(table(), key) do
      [{^key, token, expires_at}] when expires_at - now > @refresh_window -> {:ok, token}
      _ -> :miss
    end
  end

  defp table do
    case :ets.whereis(@table) do
      :undefined ->
        # First token fetch of the VM's life: two processes can race here, so
        # the loser of `:ets.new` (name already taken) just adopts the winner's.
        try do
          :ets.new(@table, [
            :named_table,
            :public,
            :set,
            read_concurrency: true,
            write_concurrency: true
          ])
        rescue
          ArgumentError -> :ets.whereis(@table)
        end

      tid ->
        tid
    end
  end

  defp fetch(opts) do
    timeout = Keyword.get(opts, :timeout_ms, 10_000)
    req_opts = Keyword.get(opts, :req_options, [])

    case HttpClient.request(:get, url(opts), [{"metadata", "true"}], nil, timeout, req_opts) do
      {:ok, 200, body} ->
        case JSON.decode(body) do
          {:ok, %{"access_token" => token, "expires_on" => expires_on}} ->
            {:ok, token, parse_expiry(expires_on)}

          _ ->
            :error
        end

      _ ->
        :error
    end
  end

  defp url(opts) do
    endpoint = Keyword.get(opts, :endpoint, @default_endpoint)
    resource = Keyword.get(opts, :resource, @default_resource)

    base = endpoint <> "?api-version=#{@api_version}&resource=" <> URI.encode_www_form(resource)

    case Keyword.get(opts, :client_id) do
      nil -> base
      client_id -> base <> "&client_id=" <> URI.encode_www_form(client_id)
    end
  end

  # `expires_on` is a Unix-epoch string for api-version 2018-02-01+. Anything
  # unparseable reads as already-expired, so the next call simply refetches.
  defp parse_expiry(expires_on) when is_binary(expires_on) do
    case Integer.parse(expires_on) do
      {n, ""} -> n
      _ -> System.system_time(:second)
    end
  end

  defp parse_expiry(_), do: System.system_time(:second)
end
