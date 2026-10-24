defmodule Ankusa.HttpClient.Pools do
  @moduledoc """
  The outbound HTTP connection pools: one [`Finch`](https://hex.pm/packages/finch)
  for the node (`Ankusa.HttpClient.Finch`, started with the application) that
  every `Ankusa.HttpClient` request without a transport of its own goes through,
  and the bookkeeping that keeps its pools bounded.

  Req's default Finch gives each origin a pool of 50 connections that is never
  closed (`pool_max_idle_time: :infinity`) and is HTTP/1 only. A node delivering
  to one origin per tenant (`Ankusa.Sink.Http`'s computed URLs) would hold 10⁵
  pools and their sockets until it ran out of file descriptors. Here every pool
  is started on demand, sized, counted and swept by this module:

    * **Pool**: one per *origin and connection profile* (the connection options
      a request needs: connect timeout, TLS, the `hostname` a pinned address is
      verified against) and per instance. `pool_size` HTTP/1 connections
      (`pool_count` shards) each, from the instance's `egress.pools` config or a
      sink's `pool:` override; a request that cannot check one out within
      `pool_timeout` fails that attempt as a transport error.
    * **Sweep**: every 30 s a pool with no request in flight and none for
      `pool_max_idle_ms` is stopped and its connections closed (the mirror of
      `Ankusa.Sink.Reaper`); a connection idle for `conn_max_idle_ms` is not
      reused. A row whose pool died on its own is dropped.
    * **Ceilings**: `max_pools` live pools per instance and
      `max_pools_per_tenant` per tenant (the tenant whose delivery opened the
      pool). At a ceiling the least recently used *idle* pool (of that tenant for
      its own ceiling, so a tenant with a thousand origins only ever churns its
      own pools) is closed to make room; when every pool in scope has a request in
      flight the request is refused as `{:error, {:unavailable, :pool_limit}}`,
      which the retry policy retries after its backoff.
    * **Source delete**: `close_source/3` stops the idle pools a deleted source's
      deliveries last used.
    * **Metrics**: gauge `ankusa_http_pools` (`[:ankusa, :http, :state]`, `:pools`),
      from `count/1`; counters `ankusa_http_pool_closed_total{reason}` and
      `ankusa_http_pool_refused_total{scope}`.

  ## The race it accepts

  A request counts itself in (`:ets.update_counter/3` on the pool's row) before it
  touches the pool, and a sweep deletes a row only while that count is zero, so a
  pool is never stopped under a request that found it. A request that arrives
  after the row was deleted asks this process to open the pool again, which is
  serialised behind the sweep.

  A pool is created from `pool_opts/1` with its connection options in the pool
  itself, so every connection it opens uses them: for a pinned destination that
  is the address `Ankusa.HttpClient.Egress` validated, with the original name as
  the TLS `hostname` (SNI and certificate check). Nothing in a pool resolves a
  name on its own.
  """

  use GenServer

  require Logger

  alias Ankusa.Config

  @finch Ankusa.HttpClient.Finch
  @table __MODULE__
  @tick_ms 30_000
  @call_timeout_ms 10_000
  @defaults %Config{}.egress.pools

  # Row: {key, instance, tenant, source, last_used_ms, in_flight}.
  @last 5
  @in_flight 6

  @tls_versions [:"tlsv1.3", :"tlsv1.2"]
  @override_keys [:pool_size, :pool_count, :pool_timeout, :protocols]

  @typedoc "What `run/2` needs to find or open the pool for one request."
  @type request :: %{
          required(:url) => String.t(),
          required(:connect_options) => keyword(),
          optional(:instance) => atom() | nil,
          optional(:tenant) => String.t() | nil,
          optional(:source) => term(),
          optional(:pool) => keyword(),
          optional(:pool_timeout) => timeout() | nil
        }

  # ── The Finch ───────────────────────────────────────────────────────────────

  @doc "The name of the node's Finch."
  @spec finch() :: atom()
  def finch, do: @finch

  @doc """
  Child spec of the node's Finch. Its `default` pool is only what Finch falls back
  to for a pool nobody started through `run/2`: the bounds the plan names (8
  connections, idle pool 60 s, idle connection 30 s, HTTP/1 and HTTP/2), and TLS
  verification on, so a stray pool fails closed.
  """
  @spec finch_child_spec() :: {module(), keyword()}
  def finch_child_spec do
    default = [
      size: 8,
      count: 1,
      pool_max_idle_time: 60_000,
      conn_max_idle_time: 30_000,
      protocols: [:http1, :http2],
      conn_opts: [transport_opts: [verify: :verify_peer, versions: @tls_versions]]
    ]

    {Finch, name: @finch, pools: %{default: default}}
  end

  @doc """
  Create the row table. The application calls it, so the rows outlive a restart
  of this process: the pools it opened are still running.
  """
  @spec create_table() :: :ok
  def create_table do
    :ets.new(@table, [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ok
  end

  # ── The request path ────────────────────────────────────────────────────────

  @doc """
  Run `fun` with the Finch options (`name`, `pool_tag`, `pool_timeout`) of the pool
  `request` belongs to, opening it if need be, and release it afterwards.

  Returns `{:error, {:unavailable, :pool_limit}}` without calling `fun` when a
  ceiling is reached and nothing can be closed, and `{:error, {:unavailable,
  :pools_down}}` when this process cannot be reached.
  """
  @spec run(request(), (keyword() -> result)) :: result | {:error, {:unavailable, atom()}}
        when result: term()
  def run(request, fun) when is_function(fun, 1) do
    instance = Map.get(request, :instance)
    limits = limits(instance)
    settings = settings(limits, Map.get(request, :pool, []))
    base = Finch.Pool.new(request.url)
    tag = tag(instance, settings, request.connect_options)
    key = {base.scheme, base.host, base.port, tag}

    finch_opts = [
      name: @finch,
      pool_tag: tag,
      pool_timeout: Map.get(request, :pool_timeout) || settings.pool_timeout
    ]

    case acquire(key, base, tag, request, settings, limits) do
      :ok ->
        try do
          fun.(finch_opts)
        after
          release(key)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp acquire(key, base, tag, request, settings, limits) do
    if check_in(key) do
      :ok
    else
      message =
        {:open, key, %{base | tag: tag}, request, settings, limits}

      try do
        GenServer.call(__MODULE__, message, @call_timeout_ms)
      catch
        :exit, _reason -> {:error, {:unavailable, :pools_down}}
      end
    end
  end

  # Counts the request in. `false` when the pool has no row (never opened, or
  # swept): the caller asks the server to open it.
  defp check_in(key) do
    :ets.update_counter(@table, key, {@in_flight, 1})
    :ets.update_element(@table, key, {@last, now_ms()})
    true
  rescue
    ArgumentError -> false
  end

  defp release(key) do
    :ets.update_counter(@table, key, {@in_flight, -1})
    :ets.update_element(@table, key, {@last, now_ms()})
    :ok
  rescue
    ArgumentError -> :ok
  end

  # The instance's `egress.pools`, with a sink's `pool:` override on top.
  defp settings(limits, override) do
    Map.merge(limits, Map.new(override))
  end

  defp limits(instance) when is_atom(instance) and instance != nil do
    case Ankusa.fetch_config(instance) do
      {:ok, %Config{egress: %{pools: pools}}} -> pools
      _no_config -> @defaults
    end
  end

  defp limits(_no_instance), do: @defaults

  # A pool is identified by what makes its connections different: the instance
  # (whose ceilings apply), the shape (size, shards, protocols, idle time), and
  # the connection options. Hashed, so the registry key stays small.
  defp tag(instance, settings, connect_options) do
    shape =
      {settings.pool_size, settings.pool_count, settings.protocols, settings.conn_max_idle_ms}

    :crypto.hash(:md5, :erlang.term_to_binary({instance, shape, connect_options}))
  end

  @doc """
  How many pools `instance` has open: a read of the maintained rows, no Finch call.
  """
  @spec count(atom() | nil) :: non_neg_integer()
  def count(instance) do
    :ets.select_count(@table, [{{:_, instance, :_, :_, :_, :_}, [], [true]}])
  rescue
    ArgumentError -> 0
  end

  @doc """
  Stop the idle pools the deliveries of source `source` of `tenant` last used.
  Called when the source is deleted; a pool with a request in flight is left to the
  idle sweep. Asynchronous.
  """
  @spec close_source(atom(), String.t() | nil, String.t()) :: :ok
  def close_source(instance, tenant, source) do
    GenServer.cast(__MODULE__, {:close_source, instance, {tenant, source}})
  end

  @doc false
  # Tests: one sweep now, instead of waiting for the tick. Also returns after
  # any `close_source/3` cast sent before it.
  @spec sweep_now() :: :ok
  def sweep_now, do: GenServer.call(__MODULE__, :sweep)

  # ── Config ──────────────────────────────────────────────────────────────────

  @doc """
  Raise `ArgumentError` unless `pool` is a usable per-sink `:pool` override: a
  keyword list of `:pool_size` (1 to 1024), `:pool_count` (1 to 64),
  `:pool_timeout` (1 to 600000 ms) and `:protocols` (`:http1`, `:http2`).
  """
  @spec validate_override!(term()) :: :ok
  def validate_override!(pool) do
    unless Keyword.keyword?(pool) do
      raise ArgumentError, "Sink.Http pool must be a keyword list, got: #{inspect(pool)}"
    end

    case Keyword.keys(pool) -- @override_keys do
      [] ->
        :ok

      [key | _] ->
        raise ArgumentError,
              "Sink.Http pool: unknown key #{inspect(key)} (known: " <>
                "#{Enum.map_join(@override_keys, ", ", &inspect/1)})"
    end

    for {key, max} <- [pool_size: 1_024, pool_count: 64, pool_timeout: 600_000],
        {:ok, value} <- [Keyword.fetch(pool, key)] do
      unless is_integer(value) and value in 1..max do
        raise ArgumentError,
              "Sink.Http pool #{key} must be an integer from 1 to #{max}, got: #{inspect(value)}"
      end
    end

    case Keyword.fetch(pool, :protocols) do
      :error ->
        :ok

      {:ok, protocols} ->
        unless is_list(protocols) and protocols != [] and
                 Enum.all?(protocols, &(&1 in [:http1, :http2])) do
          raise ArgumentError,
                "Sink.Http pool protocols must be a non-empty list of :http1 and :http2, " <>
                  "got: #{inspect(protocols)}"
        end

        :ok
    end
  end

  @doc """
  Log what the instance's pool settings cannot do: a pool smaller than the
  dispatch concurrency that can reach one origin (several sinks posting to one
  receiver would queue on checkout, silently capping throughput), and a ceiling
  whose worst case is more connections than the process may open. Returns `:ok`;
  the ranges themselves are `Ankusa.Config`'s.
  """
  @spec validate_config!(Config.t()) :: :ok
  def validate_config!(%Config{egress: %{pools: pools}, dispatch: dispatch} = config) do
    per_origin = pools.pool_size * pools.pool_count

    if Config.role?(config, :dispatch) and per_origin < dispatch.concurrency do
      Logger.warning(
        "[ankusa] egress.pools.pool_size * pool_count (#{per_origin}) is below " <>
          "dispatch.concurrency (#{dispatch.concurrency}): deliveries to one origin " <>
          "will queue on a free connection, which caps their throughput. Raise " <>
          "egress.pools.pool_size, or set pool: on the sink."
      )
    end

    worst = pools.max_pools * per_origin

    case fd_limit() do
      limit when is_integer(limit) and worst > limit ->
        Logger.warning(
          "[ankusa] egress.pools.max_pools (#{pools.max_pools}) * pool_size * pool_count " <>
            "(#{per_origin}) can open #{worst} connections, above this process's " <>
            "file descriptor limit (#{limit}). Raise `ulimit -n` or lower egress.pools.max_pools."
        )

      _within_limit_or_unknown ->
        :ok
    end

    :ok
  end

  defp fd_limit do
    case :persistent_term.get({__MODULE__, :fd_limit}, :unset) do
      :unset ->
        limit = read_fd_limit()
        :persistent_term.put({__MODULE__, :fd_limit}, limit)
        limit

      limit ->
        limit
    end
  end

  defp read_fd_limit do
    case System.cmd("sh", ["-c", "ulimit -n"], stderr_to_stdout: true) do
      {out, 0} ->
        case Integer.parse(String.trim(out)) do
          {limit, ""} -> limit
          _unlimited -> nil
        end

      _failed ->
        nil
    end
  rescue
    _error -> nil
  end

  # ── GenServer ───────────────────────────────────────────────────────────────

  @doc false
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    # A restart finds the rows of the previous run, and their pools still open
    # (this module is what closes them): close those first, so none is orphaned.
    for {key, _instance, _tenant, _source, _last, _in_flight} <- :ets.tab2list(@table),
        do: stop(key)

    :ets.delete_all_objects(@table)
    tick_ms = Keyword.get(opts, :tick_ms, @tick_ms)
    schedule(tick_ms)
    {:ok, %{tick_ms: tick_ms}}
  end

  @impl true
  def handle_call({:open, key, pool, request, settings, limits}, _from, state) do
    {:reply, open(key, pool, request, settings, limits), state}
  end

  def handle_call(:sweep, _from, state) do
    sweep()
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:close_source, instance, source}, state) do
    rows =
      :ets.select(@table, [
        {{:"$1", instance, :_, source, :"$2", 0}, [], [{{:"$1", :"$2"}}]}
      ])

    close_all(rows, instance, :source)
    {:noreply, state}
  end

  @impl true
  def handle_info(:tick, state) do
    sweep()
    schedule(state.tick_ms)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp open(key, pool, request, settings, limits) do
    instance = Map.get(request, :instance)
    tenant = Map.get(request, :tenant)

    if check_in(key) do
      :ok
    else
      with :ok <- make_room(instance, tenant, limits),
           :ok <- start_pool(pool, request.connect_options, settings) do
        :ets.insert(
          @table,
          {key, instance, tenant, Map.get(request, :source), now_ms(), 1}
        )

        :ok
      end
    end
  end

  defp start_pool(pool, connect_options, settings) do
    opts =
      [connect_options: connect_options]
      |> Req.Finch.pool_options()
      |> Keyword.merge(
        protocols: settings.protocols,
        size: settings.pool_size,
        count: settings.pool_count,
        # This module closes pools; Finch's own idle timer would close one behind
        # its row's back.
        pool_max_idle_time: :infinity,
        conn_max_idle_time: settings.conn_max_idle_ms
      )

    Finch.start_pool(@finch, pool, opts)
  rescue
    error ->
      Logger.warning("[ankusa] could not start an HTTP pool: #{Exception.message(error)}")
      {:error, {:unavailable, :pool_start}}
  end

  # Room for one more pool of `tenant` and of `instance`: close idle ones, oldest
  # first, when a ceiling is reached; refuse when none can be closed.
  defp make_room(instance, tenant, limits) do
    with :ok <- room(instance, tenant, limits.max_pools_per_tenant, :tenant) do
      room(instance, :_, limits.max_pools, :instance)
    end
  end

  defp room(_instance, nil, _max, :tenant), do: :ok

  defp room(instance, tenant, max, scope) do
    open = :ets.select_count(@table, [{{:_, instance, tenant, :_, :_, :_}, [], [true]}])

    if open < max do
      :ok
    else
      need = open - max + 1

      idle =
        :ets.select(@table, [
          {{:"$1", instance, tenant, :_, :"$2", 0}, [], [{{:"$1", :"$2"}}]}
        ])

      if length(idle) < need do
        :telemetry.execute([:ankusa, :http_pool, :refused], %{count: 1}, %{
          instance: instance,
          scope: scope
        })

        {:error, {:unavailable, :pool_limit}}
      else
        # A batch, so a full node does not scan its rows for every new origin.
        batch = max(need, div(max, 16))
        oldest = idle |> Enum.sort_by(&elem(&1, 1)) |> Enum.take(batch)
        close_all(oldest, instance, :evicted)
        :ok
      end
    end
  end

  # Deletes each row only if it is still the one judged (idle, not used since),
  # then stops its pool.
  defp close_all(rows, instance, reason) do
    closed =
      Enum.count(rows, fn {key, last} ->
        take(key, last) and stop(key)
      end)

    if closed > 0 do
      :telemetry.execute([:ankusa, :http_pool, :closed], %{count: closed}, %{
        instance: instance,
        reason: reason
      })
    end

    :ok
  end

  defp take(key, last) do
    :ets.select_delete(@table, [{{key, :_, :_, :_, last, 0}, [], [true]}]) == 1
  end

  defp stop({scheme, host, port, tag}) do
    _ = Finch.stop_pool(@finch, %Finch.Pool{scheme: scheme, host: host, port: port, tag: tag})
    true
  catch
    :exit, _gone -> true
  end

  defp sweep do
    now = now_ms()

    {idle, dead} =
      :ets.foldl(
        fn {key, instance, _tenant, _source, last, in_flight}, {idle, dead} ->
          cond do
            in_flight > 0 ->
              {idle, dead}

            gone?(key) ->
              {idle, [{key, last, instance} | dead]}

            now - last >= limits(instance).pool_max_idle_ms ->
              {[{key, last, instance} | idle], dead}

            true ->
              {idle, dead}
          end
        end,
        {[], []},
        @table
      )

    for {key, last, _instance} <- dead, do: take(key, last)

    idle
    |> Enum.group_by(&elem(&1, 2), fn {key, last, _instance} -> {key, last} end)
    |> Enum.each(fn {instance, rows} -> close_all(rows, instance, :idle) end)
  end

  # The pool died on its own: only its row is left.
  defp gone?({scheme, host, port, tag}) do
    Finch.find_pool(@finch, %Finch.Pool{scheme: scheme, host: host, port: port, tag: tag}) ==
      :error
  end

  defp schedule(tick_ms), do: Process.send_after(self(), :tick, tick_ms)

  defp now_ms, do: System.monotonic_time(:millisecond)
end
