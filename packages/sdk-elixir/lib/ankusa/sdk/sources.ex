defmodule Ankusa.SDK.Sources do
  @moduledoc """
  The tenant-scoped source-management API: `Ankusa.Admin.Router` on
  `admin.port` (default `4002`) serves list/get/create/update/delete for a
  tenant's ingest sources.

  A source is addressed as `<tenant>.<name>`; this client speaks in those terms
  and builds the paths for you. Only `[A-Za-z0-9_-]{1,64}` tenants and names are
  accepted, and both are checked before any path is built, so a caller-supplied
  name cannot escape its tenant through URL normalization.

  ## The version latch

  `:expected_version` is an optional safety latch: when set, every API call
  first makes sure `GET /health` reported that version, and a mismatch is an
  `Ankusa.SDK.VersionMismatchError` before the request is sent.

  The client is immutable, so the fetched version is returned rather than
  remembered. Run `verify_version/1` once at startup and keep the returned
  client; every later call then re-checks the cached version without another
  `/health` request:

  ```elixir
  {:ok, sources} = Ankusa.SDK.Sources.verify_version(Ankusa.SDK.Sources.new(url, expected_version: "0.3.0"))
  {:ok, list} = Ankusa.SDK.Sources.list_sources(sources, "acme")
  ```

  Failures are `Ankusa.SDK.SourceNotFoundError` (`404`),
  `Ankusa.SDK.SourceConflictError` (`409`), `Ankusa.SDK.SourceStoreReadOnlyError`
  (`409 source_store_read_only`: the deployment's source store is a static seed),
  `Ankusa.SDK.SourceInvalidError` (`400`, or an invalid tenant/name caught before
  any request), `Ankusa.SDK.VersionMismatchError`, and
  `Ankusa.SDK.SourcesUnavailableError` (unreachable, timed out, or `5xx`). Every
  one carries `:status` and `:body`.
  """

  alias Ankusa.SDK.{HTTP, SourceConflictError, SourceInvalidError, SourceNotFoundError}
  alias Ankusa.SDK.{SourceStoreReadOnlyError, SourcesUnavailableError, VersionMismatchError}
  alias Ankusa.SDK.Sources.{Source, Spec}

  # The same rule `Ankusa.ClaimCheck.Ref` uses for its tenant. Anything outside
  # it is rejected before a path is built: an unvalidated ".." or "a/b" would
  # escape the tenant scope through URL normalization.
  @safe_id ~r/\A[A-Za-z0-9_-]{1,64}\z/

  defstruct [:http, :expected_version, :server_version]

  @type t :: %__MODULE__{
          http: term(),
          expected_version: String.t() | nil,
          server_version: String.t() | nil
        }

  @doc """
  Build a client for `base_url`.

  Options are `:headers` (a map or `{name, value}` list sent on every request),
  `:timeout_ms` (default `10_000`, applied to connect *and* receive — to receive
  only with a `:finch` pool, which owns its connect options),
  `:req_options` (transport tuning: `:finch`, `:connect_options`,
  `:pool_timeout`, `:plug`; `:finch` and `:connect_options` are exclusive), and
  `:expected_version` (a version string to latch against, or `nil` for no latch).
  """
  @spec new(String.t(), keyword()) :: t()
  def new(base_url, opts \\ []) do
    expected_version = Keyword.get(opts, :expected_version)

    unless is_nil(expected_version) or is_binary(expected_version) do
      raise ArgumentError,
            ":expected_version must be a string, got: #{inspect(expected_version)}"
    end

    %__MODULE__{
      http: HTTP.new(base_url, opts, [:expected_version]),
      expected_version: expected_version
    }
  end

  @doc """
  Fetch `GET /health` when the version isn't cached yet, enforce
  `:expected_version`, and return the client with the version cached.

  Run it once at startup and keep the returned client; later calls then skip the
  probe.
  """
  @spec verify_version(t()) ::
          {:ok, t()} | {:error, SourcesUnavailableError.t() | VersionMismatchError.t()}
  def verify_version(%__MODULE__{} = client) do
    with {:ok, client} <- fetch_version(client),
         :ok <- check_version(client) do
      {:ok, client}
    end
  end

  @doc "The deployment's Ankusa version: `verify_version/1` then the `\"version\"` field."
  @spec server_version(t()) ::
          {:ok, String.t()} | {:error, SourcesUnavailableError.t() | VersionMismatchError.t()}
  def server_version(%__MODULE__{} = client) do
    with {:ok, client} <- verify_version(client) do
      {:ok, client.server_version}
    end
  end

  @doc "List a tenant's sources: `GET /v1/tenants/{tenant}/sources`."
  @spec list_sources(t(), term()) :: {:ok, [Source.t()]} | {:error, Exception.t()}
  def list_sources(%__MODULE__{} = client, tenant) do
    with :ok <- validate_tenant(tenant),
         {:ok, client} <- latch(client) do
      case call(client, :get, "/v1/tenants/#{tenant}/sources") do
        {:ok, body} -> decode_entries(body)
        {:error, error} -> {:error, error}
      end
    end
  end

  @doc "Fetch one source: `GET /v1/tenants/{tenant}/sources/{name}`."
  @spec get_source(t(), term(), term()) :: {:ok, Source.t()} | {:error, Exception.t()}
  def get_source(%__MODULE__{} = client, tenant, name) do
    with :ok <- validate_tenant(tenant),
         :ok <- validate_name(name),
         {:ok, client} <- latch(client) do
      case call(client, :get, "/v1/tenants/#{tenant}/sources/#{name}") do
        {:ok, body} -> decode_source(body)
        {:error, error} -> {:error, error}
      end
    end
  end

  @doc """
  Create a source: `POST /v1/tenants/{tenant}/sources`.

  The name travels in the body alongside the spec; the tenant comes from the
  URL.
  """
  @spec create_source(t(), term(), term(), Spec.t()) ::
          {:ok, Source.t()} | {:error, Exception.t()}
  def create_source(%__MODULE__{} = client, tenant, name, %Spec{} = spec) do
    with :ok <- validate_tenant(tenant),
         :ok <- validate_name(name),
         {:ok, client} <- latch(client) do
      body = spec |> Spec.to_json() |> Map.put("name", name)

      case call(client, :post, "/v1/tenants/#{tenant}/sources", json: body) do
        {:ok, response} -> decode_source(response)
        {:error, error} -> {:error, error}
      end
    end
  end

  @doc """
  Replace a source: `PUT /v1/tenants/{tenant}/sources/{name}`.

  The name comes from the URL; the spec carries no name.
  """
  @spec update_source(t(), term(), term(), Spec.t()) ::
          {:ok, Source.t()} | {:error, Exception.t()}
  def update_source(%__MODULE__{} = client, tenant, name, %Spec{} = spec) do
    with :ok <- validate_tenant(tenant),
         :ok <- validate_name(name),
         {:ok, client} <- latch(client) do
      case call(client, :put, "/v1/tenants/#{tenant}/sources/#{name}", json: Spec.to_json(spec)) do
        {:ok, body} -> decode_source(body)
        {:error, error} -> {:error, error}
      end
    end
  end

  @doc """
  Delete a source: `DELETE /v1/tenants/{tenant}/sources/{name}`.

  Returns `:ok` on `2xx` (`204` with an empty body).
  """
  @spec delete_source(t(), term(), term()) :: :ok | {:error, Exception.t()}
  def delete_source(%__MODULE__{} = client, tenant, name) do
    with :ok <- validate_tenant(tenant),
         :ok <- validate_name(name),
         {:ok, client} <- latch(client) do
      case call(client, :delete, "/v1/tenants/#{tenant}/sources/#{name}") do
        {:ok, _body} -> :ok
        {:error, error} -> {:error, error}
      end
    end
  end

  ## the version latch

  defp latch(%__MODULE__{expected_version: nil} = client), do: {:ok, client}

  defp latch(%__MODULE__{} = client) do
    with {:ok, client} <- fetch_version(client),
         :ok <- check_version(client) do
      {:ok, client}
    end
  end

  defp fetch_version(%__MODULE__{server_version: version} = client) when is_binary(version),
    do: {:ok, client}

  defp fetch_version(%__MODULE__{} = client) do
    case HTTP.request(client.http, :get, "/health") do
      {:ok, %{status: 200, body: body}} ->
        case HTTP.decode_json(body) do
          {:ok, %{"version" => version}} when is_binary(version) ->
            {:ok, %{client | server_version: version}}

          _other ->
            {:error,
             unavailable(
               "ankusa admin API health check returned no version (200): #{inspect(HTTP.error_body(body))}",
               200,
               HTTP.error_body(body)
             )}
        end

      {:ok, %{status: status, body: body}} ->
        {:error,
         unavailable(
           "ankusa admin API health check failed (#{status}): #{inspect(HTTP.error_body(body))}",
           status,
           HTTP.error_body(body)
         )}

      {:error, reason} ->
        {:error, unavailable("ankusa admin API unreachable: #{inspect(reason)}", nil, nil)}
    end
  end

  defp check_version(%__MODULE__{expected_version: nil}), do: :ok

  defp check_version(%__MODULE__{expected_version: expected, server_version: expected}), do: :ok

  defp check_version(%__MODULE__{expected_version: expected, server_version: actual}) do
    {:error,
     %VersionMismatchError{
       message: "expected Ankusa version #{inspect(expected)}, server reports #{inspect(actual)}",
       status: nil,
       body: nil
     }}
  end

  ## requests

  defp call(client, method, path, opts \\ []) do
    case HTTP.request(client.http, method, path, opts) do
      {:ok, %{status: status, body: body}} ->
        classify(status, body)

      {:error, reason} ->
        {:error, unavailable("ankusa admin API unreachable: #{inspect(reason)}", nil, nil)}
    end
  end

  defp classify(status, body) when status >= 200 and status <= 299, do: {:ok, body}

  defp classify(404, body) do
    decoded = HTTP.error_body(body)

    {:error,
     %SourceNotFoundError{
       message: "source not found (404): #{inspect(decoded)}",
       status: 404,
       body: decoded
     }}
  end

  defp classify(400, body) do
    decoded = HTTP.error_body(body)

    {:error,
     %SourceInvalidError{message: invalid_message(decoded, 400), status: 400, body: decoded}}
  end

  defp classify(409, body) do
    decoded = HTTP.error_body(body)

    if is_map(decoded) and decoded["error"] == "source_store_read_only" do
      {:error,
       %SourceStoreReadOnlyError{
         message: "source store is read-only (409): #{inspect(decoded)}",
         status: 409,
         body: decoded
       }}
    else
      {:error,
       %SourceConflictError{
         message: "source already exists (409): #{inspect(decoded)}",
         status: 409,
         body: decoded
       }}
    end
  end

  defp classify(status, body) do
    decoded = HTTP.error_body(body)

    {:error,
     unavailable("ankusa admin API error (#{status}): #{inspect(decoded)}", status, decoded)}
  end

  defp invalid_message(decoded, status) when is_map(decoded) do
    case decoded["message"] || decoded["error"] do
      message when is_binary(message) -> message
      _other -> "invalid source (#{status}): #{inspect(decoded)}"
    end
  end

  defp invalid_message(decoded, status), do: "invalid source (#{status}): #{inspect(decoded)}"

  defp unavailable(message, status, body) do
    %SourcesUnavailableError{message: message, status: status, body: body}
  end

  ## decoding

  defp decode_entries(body) do
    case HTTP.decode_json(body) do
      {:ok, %{"entries" => entries}} when is_list(entries) ->
        decode_sources(entries, body)

      _other ->
        {:error,
         unavailable(
           "ankusa admin API returned a malformed list body (200): #{inspect(HTTP.error_body(body))}",
           200,
           HTTP.error_body(body)
         )}
    end
  end

  defp decode_sources(entries, body) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, acc} ->
      case Source.from_json(entry) do
        {:ok, source} ->
          {:cont, {:ok, [source | acc]}}

        :error ->
          {:halt,
           {:error, unavailable("malformed source in response", 200, HTTP.error_body(body))}}
      end
    end)
    |> case do
      {:ok, sources} -> {:ok, Enum.reverse(sources)}
      {:error, error} -> {:error, error}
    end
  end

  defp decode_source(body) do
    case HTTP.decode_json(body) do
      {:ok, data} ->
        case Source.from_json(data) do
          {:ok, source} ->
            {:ok, source}

          :error ->
            {:error, unavailable("malformed source in response", 200, HTTP.error_body(body))}
        end

      :error ->
        {:error,
         unavailable(
           "ankusa admin API returned a non-JSON body (200)",
           200,
           HTTP.error_body(body)
         )}
    end
  end

  ## input validation

  defp validate_tenant(tenant) when is_binary(tenant) do
    if Regex.match?(@safe_id, tenant) do
      :ok
    else
      {:error, invalid_tenant(tenant)}
    end
  end

  defp validate_tenant(tenant), do: {:error, invalid_tenant(tenant)}

  defp invalid_tenant(tenant) do
    %SourceInvalidError{message: "invalid tenant: #{inspect(tenant)}", status: nil, body: nil}
  end

  defp validate_name(name) when is_binary(name) do
    if Regex.match?(@safe_id, name) do
      :ok
    else
      {:error, invalid_name(name)}
    end
  end

  defp validate_name(name), do: {:error, invalid_name(name)}

  defp invalid_name(name) do
    %SourceInvalidError{
      message: "invalid source name: #{inspect(name)}",
      status: nil,
      body: nil
    }
  end
end
