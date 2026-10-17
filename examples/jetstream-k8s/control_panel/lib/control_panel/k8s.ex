defmodule ControlPanel.K8s do
  @moduledoc """
  A thin Kubernetes API client: Req plus the pod's ServiceAccount.

  No client library: the panel makes five kinds of calls (list pods, read HPAs,
  delete a pod, create/delete an `AuthorizationPolicy`, JSON-patch the
  `VirtualService`), and each is one HTTP request. Every function returns
  `{:ok, decoded_body}` or `{:error, {status, body}}` / `{:error, exception}`;
  outside a cluster (no token file) it is `{:error, :no_service_account}`.
  """

  @sa_dir "/var/run/secrets/kubernetes.io/serviceaccount"
  @base_url "https://kubernetes.default.svc"

  @type result :: {:ok, term()} | {:error, term()}

  @doc "The namespace every call is made in."
  @spec namespace() :: String.t()
  def namespace, do: Application.fetch_env!(:control_panel, :k8s_namespace)

  @spec get(String.t(), keyword()) :: result()
  def get(path, params \\ []), do: request(:get, path, params: params)

  @spec post(String.t(), map()) :: result()
  def post(path, body),
    do: request(:post, path, body: JSON.encode!(body), type: "application/json")

  @doc "`gracePeriodSeconds=0` in `params` is a SIGKILL: the pod is gone, not asked to leave."
  @spec delete(String.t(), keyword()) :: result()
  def delete(path, params \\ []), do: request(:delete, path, params: params)

  @spec json_patch(String.t(), [map()]) :: result()
  def json_patch(path, ops),
    do: request(:patch, path, body: JSON.encode!(ops), type: "application/json-patch+json")

  defp request(method, path, opts) do
    # Bound ServiceAccount tokens rotate, so read the file on every call.
    with {:ok, token} <- read_token() do
      req_opts =
        [
          method: method,
          url: @base_url <> path,
          headers: [{"authorization", "Bearer " <> token}, {"accept", "application/json"}],
          connect_options: [transport_opts: [cacertfile: Path.join(@sa_dir, "ca.crt")]],
          retry: false,
          receive_timeout: 5_000,
          # Decoded below with the built-in JSON module.
          decode_body: false
        ]
        |> put_body(opts)
        |> Keyword.put(:params, Keyword.get(opts, :params, []))

      case Req.request(req_opts) do
        {:ok, %Req.Response{status: status, body: body}} when status in 200..299 ->
          {:ok, decode(body)}

        {:ok, %Req.Response{status: status, body: body}} ->
          {:error, {status, decode(body)}}

        {:error, exception} ->
          {:error, exception}
      end
    end
  end

  defp put_body(req_opts, opts) do
    case Keyword.fetch(opts, :body) do
      {:ok, body} ->
        req_opts
        |> Keyword.put(:body, body)
        |> Keyword.update!(:headers, &[{"content-type", Keyword.fetch!(opts, :type)} | &1])

      :error ->
        req_opts
    end
  end

  defp read_token do
    case File.read(Path.join(@sa_dir, "token")) do
      {:ok, token} -> {:ok, String.trim(token)}
      {:error, _} -> {:error, :no_service_account}
    end
  end

  defp decode(""), do: nil

  defp decode(body) when is_binary(body) do
    case JSON.decode(body) do
      {:ok, decoded} -> decoded
      {:error, _} -> body
    end
  end

  @doc "Short human text for an error returned by this module."
  @spec describe(term()) :: String.t()
  def describe({status, %{"message" => message}}) when is_integer(status),
    do: "HTTP #{status}: #{message}"

  def describe({status, _body}) when is_integer(status), do: "HTTP #{status}"
  def describe(:no_service_account), do: "not running in a cluster (no service account token)"
  def describe(%{__exception__: true} = e), do: Exception.message(e)
  def describe(other), do: inspect(other)
end
