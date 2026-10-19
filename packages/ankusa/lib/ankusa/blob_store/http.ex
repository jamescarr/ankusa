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
