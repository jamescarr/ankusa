defmodule Ankusa.SDK.Routes do
  @moduledoc """
  The route-management listener (`routes.admin.port`, default `4003`): the route
  table the edge enforces, plus the global IP rules and a dry run that replays
  the guard's decision.

  The listener performs no authentication of its own — `:headers` is for
  whatever a deployer's boundary (a service mesh, an API gateway) expects in
  front of it.

  Route ids are percent-encoded as one path segment, so `/`, `?`, `#` and `%` in
  an id cannot reshape the URL; an id that isn't a string, is empty, or is
  `.`/`..` is refused with `Ankusa.SDK.InvalidRouteIdError` before any request is
  sent.

  Failures carry `retryable`, so a caller needs one bit to decide whether to try
  again or surface the rejection:

  | Error | `retryable` | Cause |
  | --- | --- | --- |
  | `Ankusa.SDK.InvalidRouteIdError` | `false` | unusable id, caught before the request |
  | `Ankusa.SDK.RouteNotFoundError` | `false` | `404` |
  | `Ankusa.SDK.RoutesRejectedError` | `false` | any other `4xx` |
  | `Ankusa.SDK.RoutesUnavailableError` | `true` | `5xx`, a redirect, a non-JSON body, or unreachable |

  ```elixir
  routes = Ankusa.SDK.Routes.new("http://localhost:4003")
  {:ok, page} = Ankusa.SDK.Routes.list_routes(routes, enabled: true, limit: 10)
  ```
  """

  alias Ankusa.SDK.{HTTP, InvalidRouteIdError, RouteNotFoundError}
  alias Ankusa.SDK.{RoutesRejectedError, RoutesUnavailableError}

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

  @doc "Liveness probe: `GET /health` → `{\"status\", \"routes\"}`."
  @spec health(t()) :: {:ok, term()} | {:error, Exception.t()}
  def health(%__MODULE__{} = client), do: json(client, :get, "/health")

  @doc """
  A page of route definitions: `GET /admin/routes`.

  `params` may carry `enabled`, `limit` and `cursor`; `nil` values are left out
  of the query string rather than sent as `=`.
  """
  @spec list_routes(t(), term()) :: {:ok, term()} | {:error, Exception.t()}
  def list_routes(%__MODULE__{} = client, params \\ []),
    do: json(client, :get, "/admin/routes", query: params)

  @doc "Store a route: `POST /admin/routes` → the route, timestamps included."
  @spec create_route(t(), map()) :: {:ok, term()} | {:error, Exception.t()}
  def create_route(%__MODULE__{} = client, input) when is_map(input),
    do: json(client, :post, "/admin/routes", json: input)

  @doc "Fetch one route: `GET /admin/routes/{id}`."
  @spec get_route(t(), term()) :: {:ok, term()} | {:error, Exception.t()}
  def get_route(%__MODULE__{} = client, id), do: with_id(id, &json(client, :get, &1))

  @doc "Replace a route: `PUT /admin/routes/{id}`."
  @spec replace_route(t(), term(), map()) :: {:ok, term()} | {:error, Exception.t()}
  def replace_route(%__MODULE__{} = client, id, input) when is_map(input),
    do: with_id(id, &json(client, :put, &1, json: input))

  @doc "Patch a route: `PATCH /admin/routes/{id}`."
  @spec update_route(t(), term(), map()) :: {:ok, term()} | {:error, Exception.t()}
  def update_route(%__MODULE__{} = client, id, patch) when is_map(patch),
    do: with_id(id, &json(client, :patch, &1, json: patch))

  @doc """
  Delete a route: `DELETE /admin/routes/{id}`.

  Returns `:ok` on `2xx`; the body is not parsed (`204` carries none).
  """
  @spec delete_route(t(), term()) :: :ok | {:error, Exception.t()}
  def delete_route(%__MODULE__{} = client, id), do: with_id(id, &delete(client, &1))

  @doc "The global IP rules: `GET /admin/ip-rules`."
  @spec get_ip_rules(t()) :: {:ok, term()} | {:error, Exception.t()}
  def get_ip_rules(%__MODULE__{} = client), do: json(client, :get, "/admin/ip-rules")

  @doc "Replace the global IP rules: `PUT /admin/ip-rules`."
  @spec put_ip_rules(t(), map()) :: {:ok, term()} | {:error, Exception.t()}
  def put_ip_rules(%__MODULE__{} = client, rules) when is_map(rules),
    do: json(client, :put, "/admin/ip-rules", json: rules)

  @doc """
  The dry-run decision for one request: `POST /admin/routes/test`.

  Nothing is captured; the response replays what the guard would have decided.
  """
  @spec test_route(t(), map()) :: {:ok, term()} | {:error, Exception.t()}
  def test_route(%__MODULE__{} = client, request) when is_map(request),
    do: json(client, :post, "/admin/routes/test", json: request)

  defp delete(client, path) do
    case request(client, :delete, path, []) do
      {:ok, _body} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp json(client, method, path, opts \\ []) do
    case request(client, method, path, opts) do
      {:ok, body} ->
        case HTTP.decode_json(body) do
          {:ok, data} ->
            {:ok, data}

          :error ->
            {:error, unavailable("routes listener returned a non-JSON body", :invalid_json)}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp request(client, method, path, opts) do
    case HTTP.request(client.http, method, path, opts) do
      {:ok, %{status: status, body: body}} ->
        classify(status, body)

      {:error, reason} ->
        {:error, unavailable("routes listener unreachable: #{inspect(reason)}", reason)}
    end
  end

  defp classify(status, body) when status >= 200 and status <= 299, do: {:ok, body}

  defp classify(404, _body) do
    {:error, %RouteNotFoundError{message: "route not found (404)"}}
  end

  defp classify(status, body) when status >= 400 and status <= 499 do
    decoded = HTTP.error_body(body)

    {:error,
     %RoutesRejectedError{
       status: status,
       code: field(decoded, "error"),
       field: field(decoded, "field"),
       message: field(decoded, "message"),
       conflicting_id: field(decoded, "conflicting_id"),
       max_routes: field(decoded, "max_routes")
     }}
  end

  defp classify(status, body) do
    {:error,
     unavailable(
       "routes listener error (#{status}): #{inspect(HTTP.error_body(body))}",
       {:status, status}
     )}
  end

  defp field(decoded, key) when is_map(decoded), do: decoded[key]
  defp field(_decoded, _key), do: nil

  defp unavailable(message, reason), do: %RoutesUnavailableError{message: message, reason: reason}

  defp with_id(id, fun) do
    case route_path(id) do
      {:ok, path} -> fun.(path)
      {:error, error} -> {:error, error}
    end
  end

  defp route_path(id) when is_binary(id) do
    if id in ["", ".", ".."] do
      {:error, %InvalidRouteIdError{message: "invalid route id #{inspect(id)}"}}
    else
      {:ok, "/admin/routes/" <> URI.encode(id, &URI.char_unreserved?/1)}
    end
  end

  defp route_path(id) do
    {:error, %InvalidRouteIdError{message: "route id must be a string, got: #{inspect(id)}"}}
  end
end
