defmodule Ankusa.Sink.Http.Pool do
  @moduledoc """
  The connection pool computed `Ankusa.Sink.Http` destinations share: one
  [`Finch`](https://hex.pm/packages/finch) per instance, started by
  `Ankusa.Instance`.

  A static sink has one origin. A `:url` computed per hook (see
  `Ankusa.Sink.Http`) has one origin per tenant receiver, and Finch's default
  keeps a pool per origin for the life of the VM, so 10⁵ tenants would be 10⁵
  pools and their sockets until the node ran out of file descriptors. This pool
  is bounded instead:

    * **Per origin**: at most 4 connections (`size`), HTTP/1 only, so one
      tenant's slow receiver can hold at most that many of them. A delivery that
      cannot check one out within Finch's 5 s checkout timeout fails that attempt
      as a transport error, which the retry policy retries.
    * **Idle**: an origin with no delivery for 30 s has its pool terminated
      (`pool_max_idle_time`) and its connections closed; a connection idle for
      15 s is not reused (`conn_max_idle_time`). Origins the node stopped talking
      to cost nothing.
    * **Origins**: `admit/3` refuses a delivery to a *new* origin while 4,096
      are open (`{:error, :pool_origins_exhausted}`, retried after the policy's
      backoff); a delivery to an origin that is already open is never refused.
    * **Gauge**: `ankusa_sink_http_pool_origins` (`[:ankusa, :sink_http, :state]`,
      `:origins`), from `origins/1`.

  The pool verifies peers (`verify_peer`, TLS 1.2 and 1.3, the system trust
  store) and has one connect timeout (5 s); a sink's own `timeout_ms` still
  bounds the response. There is no per-destination TLS exception on it: a sink
  that needs a private CA or `allow_insecure_tls` for a computed URL names its own
  pool with `req_options: [finch: [name: MyFinch]]`.

  This is an interim owner for these connections: bounded, owned pools for every
  outbound HTTP client are the hardening plan's DEL-3, which replaces it.
  """

  @size 4
  @pool_max_idle_ms 30_000
  @conn_max_idle_ms 15_000
  @connect_timeout_ms 5_000
  @max_origins 4_096

  @doc "The name of `instance`'s pool."
  @spec name(atom()) :: atom()
  def name(instance) when is_atom(instance), do: Module.concat(__MODULE__, instance)

  @doc """
  Child spec for `Ankusa.Instance`. Options: `:instance` (required) and, for
  tests, `:pool_max_idle_ms` and `:transport_opts` (merged over the TLS and
  connect-timeout defaults).
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    instance = Keyword.fetch!(opts, :instance)

    transport =
      Keyword.merge(
        [timeout: @connect_timeout_ms, verify: :verify_peer, versions: [:"tlsv1.3", :"tlsv1.2"]],
        Keyword.get(opts, :transport_opts, [])
      )

    pool = [
      protocols: [:http1],
      size: @size,
      count: 1,
      pool_max_idle_time: Keyword.get(opts, :pool_max_idle_ms, @pool_max_idle_ms),
      conn_max_idle_time: @conn_max_idle_ms,
      conn_opts: [transport_opts: transport]
    ]

    Supervisor.child_spec({Finch, name: name(instance), pools: %{default: pool}},
      id: {__MODULE__, instance}
    )
  end

  @doc "Is `instance`'s pool running?"
  @spec started?(term()) :: boolean()
  def started?(nil), do: false
  def started?(instance) when is_atom(instance), do: Process.whereis(name(instance)) != nil
  def started?(_other), do: false

  @doc "How many origins have an open pool right now."
  @spec origins(atom()) :: non_neg_integer()
  def origins(instance), do: Registry.count(name(instance))

  @doc """
  `:ok` to deliver to `url`'s origin, or `{:error, :pool_origins_exhausted}` when
  `max` origins are open and this is not one of them.
  """
  @spec admit(atom(), String.t(), pos_integer()) :: :ok | {:error, :pool_origins_exhausted}
  def admit(instance, url, max \\ @max_origins) do
    if origins(instance) < max or match?({:ok, _}, Finch.get_pool_count(name(instance), url)),
      do: :ok,
      else: {:error, :pool_origins_exhausted}
  end
end
