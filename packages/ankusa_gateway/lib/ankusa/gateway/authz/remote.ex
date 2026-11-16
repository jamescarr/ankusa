defmodule Ankusa.Gateway.Authz.Remote do
  @moduledoc """
  The options every `:remote` authorizer takes, validated in one place
  (`options!/2`) and kept in its state under `:remote`, where the engine reads
  them:

  | Key | Default | Range |
  | --- | --- | --- |
  | `timeout_ms` | `200` | 10..30000 |
  | `max_inflight` | `64` | 1..10000 |
  | `queue_max` | `1024` | 0..100000 |
  | `batch_max` | `50` | 1..1000 |
  | `batch_window_ms` | `2` | 0..100 |
  | `breaker_failures` | `5` | 0..1000 (`0` = off) |
  | `breaker_open_ms` | `5000` | 100..600000 |
  | `allow_ttl_ms`, `deny_ttl_ms` | `nil` (the global `authz.cache` ones) | 0..3600000 |
  | `allow_ttl_max_ms` | `60000` | 0..3600000 |

  A `:cached` authorizer takes the three TTL keys alone (`cache_options!/2`).

  ## The backend's address

  An authorizer that calls a policy decision point validates its destination
  here too, so every one of them behaves alike:

    * `url!/3` — a literal URL (a `{{` is refused: the origin is operator
      configuration, never request data), `https://`, or `http://` to a
      loopback host, or `http://` anywhere with `allow_insecure_http: true`.
    * `headers!/2` — a string map of at most 16 header names to values, sent
      with every request (redacted in `print-config` like every header value).
    * `req_options!/2` — the transport options `Ankusa.HttpClient` accepts
      (`:finch`, `:connect_options`, `:pool_timeout`, `:plug`), for Elixir
      config and tests; it can never change the request itself.
    * `batch_unavailable?/2` and `mark_batch_unavailable/2` — the five-minute
      memo that a decision point's bulk endpoint answered `404`, `405` or
      `501`: the authorizer then sends one request per decision instead of
      retrying a bulk call that is not there.
  """

  alias Ankusa.Gateway.{Cache, Options}

  @ranges %{
    timeout_ms: {200, 10..30_000},
    max_inflight: {64, 1..10_000},
    queue_max: {1024, 0..100_000},
    batch_max: {50, 1..1000},
    batch_window_ms: {2, 0..100},
    breaker_failures: {5, 0..1000},
    breaker_open_ms: {5000, 100..600_000},
    allow_ttl_ms: {nil, 0..3_600_000},
    deny_ttl_ms: {nil, 0..3_600_000},
    allow_ttl_max_ms: {60_000, 0..3_600_000}
  }
  @cache_keys [:allow_ttl_ms, :deny_ttl_ms, :allow_ttl_max_ms]

  @max_headers 16
  @max_header_bytes 4_096
  @header_name ~r/\A[A-Za-z0-9!#$%&'*+.^_`|~-]+\z/
  @req_options [:finch, :connect_options, :pool_timeout, :plug]
  @no_batch_ttl_ms 300_000

  @type t :: %{
          optional(:timeout_ms) => pos_integer(),
          optional(:max_inflight) => pos_integer(),
          optional(:queue_max) => non_neg_integer(),
          optional(:batch_max) => pos_integer(),
          optional(:batch_window_ms) => non_neg_integer(),
          optional(:breaker_failures) => non_neg_integer(),
          optional(:breaker_open_ms) => pos_integer(),
          allow_ttl_ms: non_neg_integer() | nil,
          deny_ttl_ms: non_neg_integer() | nil,
          allow_ttl_max_ms: non_neg_integer()
        }

  @doc "The common keys a `:remote` authorizer understands."
  @spec keys() :: [atom()]
  def keys, do: Map.keys(@ranges)

  @doc """
  The remote options of `opts` (a map; other keys are ignored: the authorizer
  checks its own), with defaults. Raises `ArgumentError` naming `key.<option>`.
  """
  @spec options!(map(), String.t()) :: t()
  def options!(opts, key), do: take!(opts, key, Map.keys(@ranges))

  @doc "The TTL keys alone, for a `:cached` authorizer."
  @spec cache_options!(map(), String.t()) :: t()
  def cache_options!(opts, key), do: take!(opts, key, @cache_keys)

  @doc """
  The `url` of a remote backend: a non-empty literal URL. `https://` always;
  `http://` only to a loopback host, or anywhere with
  `allow_insecure_http: true`. Raises `ArgumentError` naming `key.url`.
  """
  @spec url!(term(), String.t(), keyword()) :: String.t()
  def url!(value, key, opts \\ []) do
    value = Options.string!(value, "#{key}.url")

    if :binary.match(value, "{{") != :nomatch do
      raise ArgumentError,
            "#{key}.url must be a literal URL, without any {{ template }}, got #{inspect(value)}"
    end

    uri = URI.parse(value)
    insecure? = Keyword.get(opts, :allow_insecure_http, false)

    cond do
      uri.scheme == "https" and present?(uri.host) ->
        value

      uri.scheme == "http" and (insecure? or loopback_host?(uri.host)) ->
        value

      insecure? ->
        raise ArgumentError,
              "#{key}.url must be an http or https URL, got #{inspect(value)}"

      true ->
        raise ArgumentError,
              "#{key}.url must be an https URL, or http to a loopback host " <>
                "(set #{key}.allow_insecure_http: true to accept http anywhere), " <>
                "got #{inspect(value)}"
    end
  end

  @doc """
  The `headers` sent with every request to the backend: a map of at most 16
  non-empty names to values, no control character in either. Raises
  `ArgumentError` naming `key.headers`.
  """
  @spec headers!(term(), String.t()) :: %{optional(String.t()) => String.t()}
  def headers!(nil, _key), do: %{}

  def headers!(value, key) when is_map(value) and not is_struct(value) do
    if map_size(value) > @max_headers do
      raise ArgumentError,
            "#{key}.headers must hold at most #{@max_headers} entries, " <>
              "got #{map_size(value)}"
    end

    Map.new(value, fn {name, header_value} ->
      {header_name!(name, key), header_value!(header_value, key)}
    end)
  end

  def headers!(other, key) do
    raise ArgumentError,
          "#{key}.headers must be a map of header names to values, got #{inspect(other)}"
  end

  @doc """
  The `req_options` of a backend: the transport options
  `Ankusa.HttpClient.request_with_headers/6` accepts, in Elixir config (or a
  test's `plug:` stub). Raises `ArgumentError` naming `key.req_options`.
  """
  @spec req_options!(term(), String.t()) :: keyword()
  def req_options!(nil, _key), do: []

  def req_options!(value, key) when is_list(value) do
    unless Keyword.keyword?(value) do
      raise ArgumentError, "#{key}.req_options must be a keyword list, got #{inspect(value)}"
    end

    case Enum.find(Keyword.keys(value), &(&1 not in @req_options)) do
      nil ->
        value

      unknown ->
        raise ArgumentError,
              "#{key}.req_options does not accept #{inspect(unknown)} " <>
                "(known: #{Enum.map_join(@req_options, ", ", &inspect/1)})"
    end
  end

  def req_options!(other, key) do
    raise ArgumentError,
          "#{key}.req_options must be a keyword list, got #{inspect(other)}"
  end

  @doc """
  Is `name`'s decision point known to answer its bulk endpoint with `404`,
  `405` or `501`? The memo lives `@no_batch_ttl_ms`.
  """
  @spec batch_unavailable?(atom(), String.t()) :: boolean()
  def batch_unavailable?(instance, name),
    do: match?({:ok, _}, Cache.fetch(instance, :authz, {:no_batch, name}))

  @doc "Remember that `name`'s decision point has no bulk endpoint."
  @spec mark_batch_unavailable(atom(), String.t()) :: :ok
  def mark_batch_unavailable(instance, name),
    do: Cache.put(instance, :authz, {:no_batch, name}, true, @no_batch_ttl_ms)

  @doc "The statuses that mean \"this decision point has no bulk endpoint\"."
  @spec no_batch_status?(non_neg_integer()) :: boolean()
  def no_batch_status?(status), do: status in [404, 405, 501]

  defp present?(value), do: is_binary(value) and value != ""

  defp loopback_host?(host) when is_binary(host) do
    cond do
      host == "localhost" -> true
      String.ends_with?(host, ".localhost") -> true
      true -> parse_loopback?(host)
    end
  end

  defp loopback_host?(_host), do: false

  defp parse_loopback?(host) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, address} -> Options.loopback?(address)
      {:error, _reason} -> false
    end
  end

  defp header_name!(name, key) when is_binary(name) and name != "" do
    if Regex.match?(@header_name, name),
      do: name,
      else: invalid_header_name!(name, key)
  end

  defp header_name!(name, key), do: invalid_header_name!(name, key)

  defp invalid_header_name!(name, key) do
    raise ArgumentError,
          "#{key}.headers has an invalid header name #{inspect(name)} " <>
            "(an HTTP token, no whitespace or \":\")"
  end

  defp header_value!(value, key) when is_binary(value) do
    if :binary.match(value, ["\r", "\n", <<0>>]) == :nomatch and
         byte_size(value) <= @max_header_bytes,
       do: value,
       else:
         raise(
           ArgumentError,
           "#{key}.headers has an invalid header value " <>
             "(a string of at most #{@max_header_bytes} bytes, no line break)"
         )
  end

  defp header_value!(other, key) do
    raise ArgumentError,
          "#{key}.headers must map header names to strings, got #{inspect(other)}"
  end

  defp take!(opts, key, keys) do
    Map.new(keys, fn k ->
      {default, range} = Map.fetch!(@ranges, k)

      value =
        case Map.get(opts, k, default) do
          nil -> nil
          value -> Options.int_in!(value, range, "#{key}.#{k}")
        end

      {k, value}
    end)
  end
end
