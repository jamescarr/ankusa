defmodule Ankusa.SDK.HTTP do
  @moduledoc false

  # The one place the SDK issues requests. It copies the conventions `Ankusa
  # core's own `Ankusa.HttpClient` uses rather than depending on it: the SDK
  # must load without core, and the settings that matter are part of the
  # contract, not an implementation detail.
  #
  #   * `retry: false` — one request per call. A caller (and the conformance
  #     vectors) counts requests, so a hidden retry would break both the count
  #     and the latency.
  #   * `redirect: false` — a followed redirect turns a 3xx into whatever the
  #     next hop answers, and 3xx is classified as "unavailable" here.
  #   * `http_errors: :return` — a non-2xx status is a value each client maps
  #     to its own error, not an exception.
  #   * `decode_body: false` — bodies are claim bytes, JSON to classify, or
  #     Prometheus text; every caller decides how to read them.
  #
  # `:req_options` is an allowlist for the same reason core's is: the keys that
  # can change the request have already been decided here.

  @transport_options [:finch, :connect_options, :pool_timeout, :plug]
  @own_options [:headers, :timeout_ms, :req_options]

  defstruct [:base_url, headers: [], timeout_ms: 10_000, req_options: []]

  @type t :: %__MODULE__{
          base_url: String.t(),
          headers: [{String.t(), String.t()}],
          timeout_ms: pos_integer(),
          req_options: keyword()
        }

  @spec new(String.t(), keyword()) :: t()
  def new(base_url, opts), do: new(base_url, opts, [])

  @spec new(String.t(), keyword(), [atom()]) :: t()
  def new(base_url, opts, extra_keys) when is_list(opts) do
    validate_base_url!(base_url)
    validate_options!(opts, extra_keys)

    %__MODULE__{
      base_url: String.trim_trailing(base_url, "/"),
      headers: normalize_headers!(Keyword.get(opts, :headers)),
      timeout_ms: validate_timeout!(Keyword.get(opts, :timeout_ms, 10_000)),
      req_options: validate_req_options!(Keyword.get(opts, :req_options, []))
    }
  end

  @doc """
  Issue one request, returning the status and raw body.

  `opts[:query]` is a `{key, value}` collection (map, keyword, or pair list)
  whose `nil` values are dropped and whose input order is preserved; the URL is
  assembled here, never handed to Req's `:params`. `opts[:json]` encodes the
  given term as the request body.
  """
  @spec request(t(), atom(), String.t(), keyword()) ::
          {:ok, %{status: non_neg_integer(), body: binary()}} | {:error, Exception.t()}
  def request(%__MODULE__{} = http, method, path, opts \\ []) do
    json = Keyword.fetch(opts, :json)

    request =
      [
        method: method,
        url: http.base_url <> path <> query_string(Keyword.get(opts, :query)),
        headers: request_headers(http.headers, json),
        decode_body: false,
        http_errors: :return,
        retry: false,
        redirect: false,
        receive_timeout: http.timeout_ms
      ] ++ transport_options(http) ++ body_option(json)

    case Req.request(request) do
      {:ok, %Req.Response{status: status, body: body}} -> {:ok, %{status: status, body: body}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Decode a JSON body, or `:error` when it is not JSON."
  @spec decode_json(binary()) :: {:ok, term()} | :error
  def decode_json(body) when is_binary(body) do
    case JSON.decode(body) do
      {:ok, term} -> {:ok, term}
      {:error, _reason} -> :error
    end
  end

  @doc "The body as JSON when it parses, else the raw bytes (`\"\"` for an empty body)."
  @spec error_body(binary()) :: term()
  def error_body(body) when is_binary(body) do
    case JSON.decode(body) do
      {:ok, term} -> term
      {:error, _reason} -> body
    end
  end

  ## construction

  defp validate_base_url!(base_url) do
    unless is_binary(base_url) do
      raise ArgumentError, "base URL must be a string, got: #{inspect(base_url)}"
    end

    case URI.new(base_url) do
      {:ok, %URI{scheme: scheme, host: host}}
      when scheme in ["http", "https"] and is_binary(host) and host != "" ->
        :ok

      _ ->
        raise ArgumentError,
              "base URL must be an absolute http(s) URL, got: #{inspect(base_url)}"
    end
  end

  defp validate_options!(opts, extra_keys) do
    case Keyword.keys(opts) -- (@own_options ++ extra_keys) do
      [] ->
        :ok

      [key | _] ->
        raise ArgumentError,
              "unknown option #{inspect(key)} — expected one of " <>
                "#{Enum.map_join(@own_options ++ extra_keys, ", ", &inspect/1)}"
    end
  end

  defp normalize_headers!(nil), do: []

  defp normalize_headers!(headers) when is_map(headers) do
    headers |> Map.to_list() |> normalize_headers!()
  end

  defp normalize_headers!(headers) when is_list(headers) do
    Enum.map(headers, fn
      {name, value} when is_binary(name) and is_binary(value) ->
        {String.downcase(name), value}

      other ->
        raise ArgumentError,
              "headers must be a map or a list of {name, value} pairs, got: #{inspect(other)}"
    end)
  end

  defp normalize_headers!(other) do
    raise ArgumentError,
          "headers must be a map or a list of {name, value} pairs, got: #{inspect(other)}"
  end

  defp validate_timeout!(timeout) when is_integer(timeout) and timeout > 0, do: timeout

  defp validate_timeout!(timeout) do
    raise ArgumentError, "timeout_ms must be a positive integer, got: #{inspect(timeout)}"
  end

  defp validate_req_options!(req_options) when is_list(req_options) do
    case Keyword.keys(req_options) -- @transport_options do
      [] ->
        validate_finch_options!(req_options)

      [key | _] ->
        raise ArgumentError,
              "unsupported :req_options key #{inspect(key)} — the SDK accepts " <>
                "#{Enum.map_join(@transport_options, ", ", &inspect/1)}. Keys that " <>
                "change the request itself (:params, :base_url, :auth, :headers) " <>
                "would alter a URL or replace the headers carrying a credential."
    end
  end

  defp validate_req_options!(other) do
    raise ArgumentError, "req_options must be a keyword list, got: #{inspect(other)}"
  end

  # Req refuses to take both, because a Finch pool owns its connect options:
  # catch it here, at construction, rather than on the first request.
  defp validate_finch_options!(req_options) do
    if Keyword.has_key?(req_options, :finch) and Keyword.has_key?(req_options, :connect_options) do
      raise ArgumentError,
            "req_options cannot combine :finch and :connect_options — a Finch pool owns " <>
              "its connect options; set them where the pool is started"
    end

    req_options
  end

  ## requests

  # `timeout_ms` bounds the connection and the response. A caller-supplied Finch
  # pool owns its connect options (Req refuses `:finch` together with
  # `:connect_options`), so with one it bounds the response only.
  defp transport_options(%__MODULE__{req_options: req_options, timeout_ms: timeout_ms}) do
    if Keyword.has_key?(req_options, :finch) do
      req_options
    else
      connect_options =
        Keyword.merge([timeout: timeout_ms], Keyword.get(req_options, :connect_options, []))

      Keyword.put(req_options, :connect_options, connect_options)
    end
  end

  defp body_option({:ok, json}) when not is_nil(json), do: [body: JSON.encode!(json)]
  defp body_option(_json), do: []

  defp request_headers(headers, {:ok, json}) when not is_nil(json) do
    [{"content-type", "application/json"} | drop_header(headers, "content-type")]
  end

  defp request_headers(headers, _json), do: headers

  defp drop_header(headers, name) do
    Enum.reject(headers, fn {header, _value} -> header == name end)
  end

  defp query_string(params) do
    case params |> pairs() |> Enum.reject(fn {_key, value} -> is_nil(value) end) do
      [] -> ""
      pairs -> "?" <> URI.encode_query(pairs)
    end
  end

  defp pairs(nil), do: []
  defp pairs(params) when is_map(params), do: pairs(Map.to_list(params))

  defp pairs(params) when is_list(params) do
    Enum.map(params, fn
      {key, value} when is_binary(key) -> {key, value}
      {key, value} when is_atom(key) -> {Atom.to_string(key), value}
      other -> raise ArgumentError, "invalid query parameter: #{inspect(other)}"
    end)
  end

  defp pairs(other), do: raise(ArgumentError, "invalid query parameters: #{inspect(other)}")
end
