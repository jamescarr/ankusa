defmodule Ankusa.BlobStore.HTTP do
  @moduledoc false
  # The request path every HTTP object store shares: the instance's connection
  # pool for the endpoint (`Ankusa.BlobStore.Pool`), then `Ankusa.BlobStore.Retry`
  # (deadline, jittered retries, `Retry-After`, breaker) around
  # `Ankusa.HttpClient`. The adapters keep what differs: how a request is built
  # and signed, and what a status means.

  alias Ankusa.BlobStore.{Pool, Retry}
  alias Ankusa.HttpClient

  @doc """
  Send the request `build` returns — `{method, url, headers, body}`, called
  afresh on every attempt so a signature carries the time of the attempt and a
  token is the current one. `scope` is `{endpoint, bucket}`; `kind` is `:small`
  or `:bulk`. Returns `{:ok, status, headers, body}` or `{:error, reason}`
  (`:deadline`, `:breaker_open`, `{:throttled, seconds}` among them).
  """
  @spec request(
          atom(),
          keyword(),
          {String.t(), String.t()},
          :small | :bulk,
          (-> {atom(), String.t(), [{String.t(), String.t()}], iodata() | nil})
        ) :: Retry.http_result() | {:error, term()}
  def request(instance, opts, {endpoint, _bucket} = scope, kind, build) do
    req_options = Pool.req_options(instance, endpoint, opts)

    Retry.run(instance, opts, scope, kind, fn timeout ->
      {method, url, headers, body} = build.()
      HttpClient.request_with_headers(method, url, headers, body, timeout, req_options)
    end)
  end

  @doc """
  Run `fun` (a whole request); on a `401` ask the credential provider to drop its
  cached token (`invalidate/1`) and run it once more, which fetches a new one.
  A `401` stays a `401` when the provider keeps nothing to drop.
  """
  @spec with_refresh({module(), atom(), list()} | nil, (-> result)) :: result
        when result: Retry.http_result() | {:error, term()}
  def with_refresh(provider, fun) do
    case fun.() do
      {:ok, 401, _headers, _body} = unauthorized ->
        if invalidate(provider), do: fun.(), else: unauthorized

      other ->
        other
    end
  end

  @doc """
  Call `invalidate` with the arguments of `{module, fun, args}` when the module
  exports it for them. Returns whether it did.
  """
  @spec invalidate({module(), atom(), list()} | nil) :: boolean()
  def invalidate({module, _fun, args}) when is_atom(module) and is_list(args) do
    if Code.ensure_loaded?(module) and function_exported?(module, :invalidate, length(args)) do
      apply(module, :invalidate, args)
      true
    else
      false
    end
  end

  def invalidate(_provider), do: false

  @doc "A response as the `Ankusa.BlobStore` contract returns it: the body, `:not_found`, or the status."
  @spec reply(Retry.http_result() | {:error, term()}) :: {:ok, binary()} | {:error, term()}
  def reply({:ok, status, _headers, body}) when status in 200..299, do: {:ok, body}
  def reply({:ok, 404, _headers, _body}), do: {:error, :not_found}
  def reply({:ok, status, _headers, body}), do: {:error, {:status, status, body}}
  def reply({:error, reason}), do: {:error, reason}

  @doc "A write's response: `:ok`, or `{:error, :already_exists}` when `exists` lists its status."
  @spec write_reply(Retry.http_result() | {:error, term()}, [non_neg_integer()]) ::
          :ok | {:error, term()}
  def write_reply({:ok, status, _headers, _body}, _exists) when status in 200..299, do: :ok

  def write_reply({:ok, status, headers, body}, exists) do
    if status in exists,
      do: {:error, :already_exists},
      else: reply({:ok, status, headers, body})
  end

  def write_reply({:error, reason}, _exists), do: {:error, reason}

  @doc """
  An object operation's response (`Ankusa.BlobStore`'s optional callbacks):
  a 2xx as it came, `{:ok, status, headers, body}`; `404` `:not_found`; `412`
  `:already_exists` when the request carried the `:absent` condition, else
  `:version_mismatch`; `409` on a conditional request likewise (Azure and S3
  answer an `If-None-Match: *` that lost with `409`, as `put_if_absent/4`
  already reads it);
  `416` `:range_not_satisfiable`; anything else as `reply/1`.
  """
  @spec object_reply(
          Retry.http_result() | {:error, term()},
          nil | :absent | {:version, String.t()}
        ) ::
          Retry.http_result() | {:error, term()}
  def object_reply({:ok, status, _headers, _body} = ok, _condition) when status in 200..299,
    do: ok

  def object_reply({:ok, 404, _headers, _body}, _condition), do: {:error, :not_found}
  def object_reply({:ok, 412, _headers, _body}, :absent), do: {:error, :already_exists}
  def object_reply({:ok, 412, _headers, _body}, _condition), do: {:error, :version_mismatch}

  def object_reply({:ok, 409, _headers, _body}, :absent), do: {:error, :already_exists}

  def object_reply({:ok, 409, _headers, _body}, {:version, _version}),
    do: {:error, :version_mismatch}

  def object_reply({:ok, 416, _headers, _body}, _condition), do: {:error, :range_not_satisfiable}
  def object_reply(other, _condition), do: reply(other)

  @doc "The first value of response header `name` (lowercase), or `nil`."
  @spec header(%{String.t() => [String.t()]}, String.t()) :: String.t() | nil
  def header(headers, name) do
    case Map.get(headers, name) do
      [value | _] -> value
      _ -> nil
    end
  end

  @doc "A version as the object API reports it: an ETag without its surrounding double quotes."
  @spec version(String.t() | nil) :: String.t() | nil
  def version(nil), do: nil
  def version(etag), do: etag |> String.trim() |> String.trim_leading("W/") |> String.trim("\"")

  @doc "The user metadata of the response headers that start with `prefix` (`x-amz-meta-`, ...), names lowercase."
  @spec metadata(%{String.t() => [String.t()]}, String.t()) :: %{String.t() => String.t()}
  def metadata(headers, prefix) do
    for {name, [value | _]} <- headers,
        name = String.downcase(name),
        String.starts_with?(name, prefix),
        into: %{},
        do: {String.replace_prefix(name, prefix, ""), value}
  end

  @doc """
  The whole object's size of a response: the total of `content-range`
  (`bytes 0-9/1234`) on a ranged read, else `content-length`.
  """
  @spec object_size(%{String.t() => [String.t()]}) :: non_neg_integer() | nil
  def object_size(headers) do
    with range when is_binary(range) <- header(headers, "content-range"),
         [_, total] <- String.split(range, "/", parts: 2),
         {size, ""} <- Integer.parse(total) do
      size
    else
      _ ->
        case Integer.parse(header(headers, "content-length") || "") do
          {size, ""} -> size
          _ -> nil
        end
    end
  end

  @months ~w(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec)

  @doc "An HTTP date (`last-modified`, IMF-fixdate: `Sun, 06 Nov 1994 08:49:37 GMT`) as Unix seconds, or `nil`."
  @spec http_date(String.t() | nil) :: integer() | nil
  def http_date(value) when is_binary(value) do
    with [_weekday, day, month, year, time, "GMT"] <- String.split(value, " ", trim: true),
         month when is_integer(month) <- Enum.find_index(@months, &(&1 == month)),
         {:ok, date} <- Date.new(String.to_integer(year), month + 1, String.to_integer(day)),
         {:ok, time} <- Time.from_iso8601(time),
         {:ok, datetime} <- DateTime.new(date, time, "Etc/UTC") do
      DateTime.to_unix(datetime)
    else
      _ -> nil
    end
  rescue
    ArgumentError -> nil
  end

  def http_date(_value), do: nil

  @doc "A `range` request header for `{first, last}` (inclusive)."
  @spec range_header({non_neg_integer(), non_neg_integer()}) :: {String.t(), String.t()}
  def range_header({first, last}), do: {"range", "bytes=#{first}-#{last}"}

  @doc "`data` cut into slices of at most `part_bytes`, in order."
  @spec chunks(binary(), pos_integer()) :: [binary()]
  def chunks(data, part_bytes) when is_binary(data) do
    size = byte_size(data)

    0
    |> Stream.iterate(&(&1 + part_bytes))
    |> Enum.take_while(&(&1 < size))
    |> Enum.map(&binary_part(data, &1, min(part_bytes, size - &1)))
  end
end
