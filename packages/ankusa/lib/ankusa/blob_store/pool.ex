defmodule Ankusa.BlobStore.Pool do
  @moduledoc """
  The connection pool of each HTTP object store's endpoint, owned by the
  instance (`Ankusa.Instance` starts one [`Finch`](https://hex.pm/packages/finch)
  per distinct endpoint of `storage.blob_store`, `backup.blob_store`,
  `backup.secondary` and `claim_check.blob_stores`).

  Req's default pool is one shared Finch with 50 connections for every origin
  the VM talks to: under claim-check load it is the throughput ceiling, and a slow
  bucket takes connections from every other client. A pool per endpoint is sized
  by the store's options (`pool_size` connections, `pool_count` pools,
  `pool_timeout` ms to check one out; see `Ankusa.BlobStore.Options`), so a
  slow bucket exhausts its own pool and no other. Stores that share an endpoint
  share the pool, sized by the largest of them.

  The pool carries the transport: TLS peer verification (TLS 1.2/1.3, the system
  trust store, or the store's `req_options` `cacertfile`), the connect timeout
  (`timeout_ms`) and the store's `connect_options`. A store that names its own
  transport (`req_options: [finch: [name: MyFinch]]`, or `plug:` in tests) gets no pool.
  """

  alias Ankusa.BlobStore.Options
  alias Ankusa.Config

  @adapters [
    Ankusa.BlobStore.S3,
    Ankusa.BlobStore.GCS,
    Ankusa.BlobStore.Azure,
    Ankusa.BlobStore.OCI
  ]
  @tls_versions [:"tlsv1.3", :"tlsv1.2"]

  @doc "The name of `instance`'s pool for `endpoint`."
  @spec name(atom(), String.t()) :: atom()
  def name(instance, endpoint) when is_atom(instance) and is_binary(endpoint),
    do: Module.concat(__MODULE__, "#{instance}_#{:erlang.phash2(endpoint)}")

  @doc "Child specs of the pools `config`'s stores need, one per distinct endpoint."
  @spec child_specs(Config.t()) :: [Supervisor.child_spec()]
  def child_specs(%Config{instance: instance} = config) do
    config
    |> stores()
    |> Enum.flat_map(fn {mod, opts} ->
      req = Keyword.get(opts, :req_options, [])

      if mod in @adapters and not Keyword.has_key?(req, :finch) and
           not Keyword.has_key?(req, :plug),
         do: [{mod.endpoint(opts), opts}],
         else: []
    end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.map(fn {endpoint, all_opts} -> child_spec(instance, endpoint, all_opts) end)
  end

  defp stores(%Config{storage: storage, backup: backup, claim_check: claim_check}) do
    ([storage.blob_store, backup.blob_store, Map.get(backup, :secondary)] ++
       List.wrap(claim_check.blob_stores))
    |> Enum.filter(&match?({mod, opts} when is_atom(mod) and is_list(opts), &1))
  end

  defp child_spec(instance, endpoint, all_opts) do
    [first | _] = all_opts
    req = Keyword.get(first, :req_options, [])
    size = all_opts |> Enum.map(&Options.get(&1, :pool_size)) |> Enum.max()
    count = all_opts |> Enum.map(&Options.get(&1, :pool_count)) |> Enum.max()
    connect_timeout = all_opts |> Enum.map(&Options.get(&1, :timeout_ms)) |> Enum.max()

    pool = [
      protocols: [:http1],
      size: size,
      count: count,
      pool_max_idle_time: 60_000,
      conn_opts: conn_opts(endpoint, connect_timeout, req)
    ]

    Supervisor.child_spec(
      {Finch, name: name(instance, endpoint), pools: %{default: pool}},
      id: {__MODULE__, instance, endpoint}
    )
  end

  # Mint's `transport_opts` are `:gen_tcp` options for an `http` endpoint, so the
  # TLS ones go to `https` only.
  defp conn_opts(endpoint, connect_timeout, req) do
    caller = req |> Keyword.get(:connect_options, []) |> Keyword.get(:transport_opts, [])

    transport =
      if String.starts_with?(endpoint, "https://"),
        do: Keyword.merge(tls_options(req), caller),
        else: caller

    req
    |> Keyword.get(:connect_options, [])
    |> Keyword.delete(:transport_opts)
    |> Keyword.put_new(:timeout, connect_timeout)
    |> Keyword.put(:transport_opts, transport)
  end

  defp tls_options(req) do
    if Keyword.get(req, :allow_insecure_tls, false) do
      [verify: :verify_none]
    else
      [verify: :verify_peer, versions: @tls_versions] ++
        case Keyword.get(req, :cacertfile) do
          nil -> []
          cacertfile -> [cacertfile: cacertfile]
        end
    end
  end

  @doc """
  The `:req_options` a request to `endpoint` is sent with: the store's own, plus
  `finch:` this instance's pool for the endpoint (and `pool_timeout`) when that
  pool is running. A store that names its own transport is left alone.
  """
  @spec req_options(atom(), String.t(), keyword()) :: keyword()
  def req_options(instance, endpoint, opts) do
    req = Keyword.get(opts, :req_options, [])

    if Keyword.has_key?(req, :finch) or Keyword.has_key?(req, :plug) do
      req
    else
      pool = name(instance, endpoint)

      if Process.whereis(pool) do
        # Req's own spelling: `finch: [name: pool, pool_timeout: ms]`.
        checkout = Keyword.get(req, :pool_timeout, Options.get(opts, :pool_timeout))

        req
        |> Keyword.drop([:connect_options, :cacertfile, :allow_insecure_tls, :pool_timeout])
        |> Keyword.put(:finch, name: pool, pool_timeout: checkout)
      else
        req
      end
    end
  end
end
