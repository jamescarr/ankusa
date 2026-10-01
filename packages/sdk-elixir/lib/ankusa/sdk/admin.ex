defmodule Ankusa.SDK.Admin do
  @moduledoc """
  The operator listener (`admin.port`, default `4002`): health, Prometheus
  metrics, the redacted configuration, the dead-letter queue, and the quarantine
  list.

  Responses are node-local by design, so a fleet operator scrapes every node's
  admin port. The listener performs no authentication of its own — `:headers` is
  for whatever a deployer's boundary expects in front of it.

  Failures carry `retryable`: `false` for a rejection (`409 role_not_enabled`,
  another `4xx`), `true` for a `5xx`, a redirect, a non-JSON success body, or an
  unreachable listener.

  ```elixir
  admin = Ankusa.SDK.Admin.new("http://localhost:4002")
  {:ok, %{"status" => "ok"}} = Ankusa.SDK.Admin.health(admin)
  {:ok, %{"total" => total}} = Ankusa.SDK.Admin.list_dead_letters(admin, limit: 10)
  ```
  """

  alias Ankusa.SDK.{AdminRejectedError, AdminUnavailableError, HTTP, RoleNotEnabledError}

  defstruct [:http]

  @type t :: %__MODULE__{http: term()}

  @doc """
  Build a client for `base_url`.

  Options are `:headers` (a map or `{name, value}` list sent on every request),
  `:timeout_ms` (default `10_000`, applied to connect *and* receive — to receive
  only with a `:finch` pool, which owns its connect options), and
  `:req_options` (transport tuning: `:finch`, `:connect_options`,
  `:pool_timeout`, `:plug`; `:finch` and `:connect_options` are exclusive).
  """
  @spec new(String.t(), keyword()) :: t()
  def new(base_url, opts \\ []), do: %__MODULE__{http: HTTP.new(base_url, opts)}

  @doc "Liveness probe: `GET /health` → `{\"status\", \"instance\", \"roles\"}`."
  @spec health(t()) :: {:ok, term()} | {:error, Exception.t()}
  def health(%__MODULE__{} = client), do: json(client, :get, "/health")

  @doc "The Prometheus text exposition body: `GET /metrics`."
  @spec metrics(t()) :: {:ok, binary()} | {:error, Exception.t()}
  def metrics(%__MODULE__{} = client), do: request(client, :get, "/metrics")

  @doc "The effective, redacted configuration: `GET /v1/config`."
  @spec config(t()) :: {:ok, term()} | {:error, Exception.t()}
  def config(%__MODULE__{} = client), do: json(client, :get, "/v1/config")

  @doc """
  A page of dead-lettered hooks, newest first: `GET /v1/dlq`.

  `params` may carry `source_id`, `since` and `limit`; `nil` values are left out
  of the query string rather than sent as `=`.
  """
  @spec list_dead_letters(t(), term()) :: {:ok, term()} | {:error, Exception.t()}
  def list_dead_letters(%__MODULE__{} = client, params \\ []),
    do: json(client, :get, "/v1/dlq", query: params)

  @doc """
  Replay dead letters: `POST /v1/dlq/replay` → `{\"replayed\", n}`.

  `filter` is a filter, not a payload: `nil` and `%{}` both replay everything.
  """
  @spec replay_dead_letters(t(), term()) :: {:ok, term()} | {:error, Exception.t()}
  def replay_dead_letters(%__MODULE__{} = client, filter \\ nil),
    do: json(client, :post, "/v1/dlq/replay", json: filter || %{})

  @doc """
  Recent quarantined hooks, newest first: `GET /v1/quarantine`.

  `params` may carry `limit`; `nil` values are left out of the query string.
  """
  @spec list_quarantined(t(), term()) :: {:ok, term()} | {:error, Exception.t()}
  def list_quarantined(%__MODULE__{} = client, params \\ []),
    do: json(client, :get, "/v1/quarantine", query: params)

  defp json(client, method, path, opts \\ []) do
    case request(client, method, path, opts) do
      {:ok, body} ->
        case HTTP.decode_json(body) do
          {:ok, data} ->
            {:ok, data}

          :error ->
            {:error, unavailable("admin listener returned a non-JSON body", :invalid_json)}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp request(client, method, path, opts \\ []) do
    case HTTP.request(client.http, method, path, opts) do
      {:ok, %{status: status, body: body}} ->
        classify(status, body)

      {:error, reason} ->
        {:error, unavailable("admin listener unreachable: #{inspect(reason)}", reason)}
    end
  end

  defp classify(status, body) when status >= 200 and status <= 299, do: {:ok, body}

  defp classify(409, body) do
    decoded = HTTP.error_body(body)

    if is_map(decoded) and decoded["error"] == "role_not_enabled" do
      {:error,
       %RoleNotEnabledError{
         message: "role not enabled on this node: #{inspect(decoded["role"])}",
         role: decoded["role"]
       }}
    else
      rejected(409, decoded)
    end
  end

  defp classify(status, body) when status >= 400 and status <= 499 do
    rejected(status, HTTP.error_body(body))
  end

  defp classify(status, body) do
    {:error,
     unavailable(
       "admin listener error (#{status}): #{inspect(HTTP.error_body(body))}",
       {:status, status}
     )}
  end

  defp rejected(status, decoded) do
    code = if is_map(decoded), do: decoded["error"]

    {:error,
     %AdminRejectedError{
       message: "admin listener rejected the request (#{status}): #{inspect(decoded)}",
       status: status,
       code: code
     }}
  end

  defp unavailable(message, reason), do: %AdminUnavailableError{message: message, reason: reason}
end
