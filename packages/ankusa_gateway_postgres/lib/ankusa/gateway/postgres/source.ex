defmodule Ankusa.Gateway.Postgres.Source do
  @moduledoc """
  A `table` source in a Postgres database (`type: postgres`): the permission
  rows are the result of the operator's own query, so permissions that live in
  a customer's existing tables (a users/groups/folders join) reach the gateway
  without a denormalised copy.

      authz:
        authorizers:
          team:
            type: table
            mode: snapshot
            source:
              type: postgres
              url: postgres://permissions:${DB_PASSWORD}@db.internal:5432/permissions?sslmode=require
              query: SELECT * FROM gateway_permissions
              version_query: SELECT max(updated_at) FROM role_bindings

  ## Options

  | Key | Default | |
  | --- | --- | --- |
  | `url` | — | `postgres://user:password@host:port/database` (redacted) |
  | `hostname`, `port`, `database`, `username`, `password` | `port` 5432 | instead of `url` |
  | `ssl` | `true`, unless the host is loopback | TLS to the server |
  | `query` | required in `:snapshot` mode | the rows query; returns the configured columns |
  | `version_query` | — | one row, one column: equal to the last load and nothing is read |
  | `lookup_query` | required in `:lookup` mode | parameters `$1 text[]` identities, `$2 text` action, `$3 text` space, `$4 text` key |
  | `pool_size` | `4` | 1..64 |
  | `timeout_ms` | `15000` | per statement, and the connection checkout |

  Every statement runs inside `Postgrex.transaction/3` after `SET TRANSACTION
  READ ONLY`: a query that tries to write fails, the previous snapshot stays,
  and the operator's SQL is sent as written — request data reaches the database
  only as bound parameters (`lookup_query`), never by building SQL text.
  `load/4` streams with `max_rows: 5000`, so a million rows never sit in one
  result page.

  The pool is one `Postgrex` connection pool per authorizer, started as its
  child and named `Ankusa.via(instance, {:gateway_postgres, name})`.
  """

  @behaviour Ankusa.Gateway.Authz.Table.Source

  alias Ankusa.Gateway.Options

  @keys [
    :url,
    :hostname,
    :port,
    :database,
    :username,
    :password,
    :ssl,
    :query,
    :version_query,
    :lookup_query,
    :pool_size,
    :timeout_ms
  ]
  @default_port 5432
  @stream_max_rows 5_000

  @impl true
  @spec init!(map(), String.t()) :: map()
  def init!(opts, key) do
    given = Options.map!(opts, key)
    Options.unknown_keys!(given, @keys, key)

    state = %{
      conn: conn!(given, key),
      query: sql!(given, :query, key),
      version_query: sql!(given, :version_query, key),
      lookup_query: sql!(given, :lookup_query, key),
      pool_size: Options.int_in!(Map.get(given, :pool_size, 4), 1..64, "#{key}.pool_size"),
      timeout_ms:
        Options.int_in!(Map.get(given, :timeout_ms, 15_000), 100..600_000, "#{key}.timeout_ms")
    }

    if state.query == nil and state.lookup_query == nil do
      raise ArgumentError, "#{key}: one of query (snapshot) and lookup_query (lookup) is required"
    end

    state
  end

  defp sql!(given, name, key) do
    case Map.get(given, name) do
      nil -> nil
      sql when is_binary(sql) and sql != "" -> sql
      other -> raise ArgumentError, "#{key}.#{name} must be a SQL string, got #{inspect(other)}"
    end
  end

  # ── the connection ──────────────────────────────────────────────────────────

  defp conn!(given, key) do
    conn =
      case {Map.get(given, :url), Map.get(given, :hostname)} do
        {nil, nil} -> raise ArgumentError, "#{key}: url or hostname is required"
        {url, nil} -> from_url(url, key)
        {nil, _hostname} -> from_keys(given, key)
        {_url, _hostname} -> raise ArgumentError, "#{key}: set url or hostname, not both"
      end

    ssl =
      case Map.get(given, :ssl) do
        nil -> not loopback?(to_string(conn[:hostname]))
        value -> Options.boolean!(value, "#{key}.ssl")
      end

    Keyword.put(conn, :ssl, ssl)
  end

  defp from_url(url, key) when is_binary(url) do
    uri = URI.parse(url)

    cond do
      uri.scheme not in ["postgres", "postgresql"] ->
        raise ArgumentError, "#{key}.url must be a postgres:// URL, got #{inspect(url)}"

      uri.host in [nil, ""] ->
        raise ArgumentError, "#{key}.url has no host"

      true ->
        {username, password} = userinfo(uri)

        [
          hostname: uri.host,
          port: uri.port || @default_port,
          database: uri.path |> to_string() |> String.trim_leading("/"),
          username: username,
          password: password
        ]
        |> Enum.reject(fn {_k, v} -> v in [nil, ""] end)
    end
  end

  defp from_url(other, key),
    do: raise(ArgumentError, "#{key}.url must be a string, got #{inspect(other)}")

  defp userinfo(%URI{userinfo: nil}), do: {nil, nil}

  defp userinfo(%URI{userinfo: userinfo}) do
    case String.split(userinfo, ":", parts: 2) do
      [username] -> {URI.decode_www_form(username), nil}
      [username, password] -> {URI.decode_www_form(username), URI.decode_www_form(password)}
    end
  end

  defp from_keys(given, key) do
    [
      hostname: Options.string!(Map.get(given, :hostname), "#{key}.hostname"),
      port: Options.int_in!(Map.get(given, :port, @default_port), 1..65_535, "#{key}.port"),
      database: Options.string!(Map.get(given, :database), "#{key}.database"),
      username: optional_string(Map.get(given, :username), "#{key}.username"),
      password: optional_string(Map.get(given, :password), "#{key}.password")
    ]
    |> Enum.reject(fn {_k, v} -> v in [nil, ""] end)
  end

  defp optional_string(nil, _key), do: nil
  defp optional_string(value, key), do: Options.string!(value, key)

  defp loopback?(host) when host in ["localhost", "::1", "[::1]"], do: true

  defp loopback?(host) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, {127, _, _, _}} -> true
      _other -> false
    end
  end

  # ── the pool ────────────────────────────────────────────────────────────────

  @impl true
  @spec child_specs(atom(), String.t(), map()) :: [Supervisor.child_spec()]
  def child_specs(instance, name, state) do
    opts =
      state.conn ++
        [
          name: Ankusa.via(instance, {:gateway_postgres, name}),
          pool_size: state.pool_size,
          timeout: state.timeout_ms
        ]

    [Supervisor.child_spec({Postgrex, opts}, id: {Postgrex, name})]
  end

  # ── version ─────────────────────────────────────────────────────────────────

  @impl true
  @spec version(map(), map()) :: {:ok, term()} | :unknown | {:error, term()}
  def version(%{version_query: nil}, _ctx), do: :unknown

  def version(%{version_query: sql} = state, ctx) do
    case query(state, ctx, sql, []) do
      {:ok, %Postgrex.Result{rows: [[value]]}} -> {:ok, value}
      {:ok, _empty} -> :unknown
      {:error, reason} -> {:error, reason}
    end
  end

  # ── snapshot load ───────────────────────────────────────────────────────────

  @impl true
  @spec load(map(), map(), acc, (map(), acc -> {:cont, acc} | {:halt, acc})) ::
          {:ok, acc} | {:error, term()}
        when acc: term()
  def load(%{query: nil}, _ctx, _acc, _fun), do: {:error, :no_query}

  def load(%{query: sql} = state, ctx, acc, fun) do
    transaction(state, ctx, fn conn -> stream(conn, sql, state, acc, fun) end)
  end

  defp stream(conn, sql, state, acc, fun) do
    Postgrex.stream(conn, sql, [], max_rows: @stream_max_rows, timeout: state.timeout_ms)
    |> Enum.reduce_while({:cont, acc}, fn result, {:cont, acc} ->
      case stream_rows(result, acc, fun) do
        {:cont, acc} -> {:cont, {:cont, acc}}
        {:halt, acc} -> {:halt, {:halt, acc}}
      end
    end)
    |> case do
      {:cont, acc} -> {:ok, acc}
      {:halt, acc} -> {:ok, acc}
    end
  end

  defp stream_rows(%Postgrex.Result{columns: columns, rows: rows}, acc, fun) do
    Enum.reduce_while(rows, {:cont, acc}, fn row, {:cont, acc} ->
      case fun.(Map.new(Enum.zip(columns, row)), acc) do
        {:cont, acc} -> {:cont, {:cont, acc}}
        {:halt, acc} -> {:halt, {:halt, acc}}
      end
    end)
  end

  # ── lookup ──────────────────────────────────────────────────────────────────

  @impl true
  @spec lookup(map(), map(), [String.t()], String.t(), String.t(), String.t()) ::
          {:ok, [map()]} | {:error, term()}
  def lookup(%{lookup_query: nil}, _ctx, _identities, _action, _space, _key),
    do: {:error, :no_lookup_query}

  def lookup(%{lookup_query: sql} = state, ctx, identities, action, space, key) do
    case query(state, ctx, sql, [identities, action, space, key]) do
      {:ok, %Postgrex.Result{columns: columns, rows: rows}} ->
        {:ok, Enum.map(rows, &Map.new(Enum.zip(columns, &1)))}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ── statements ──────────────────────────────────────────────────────────────

  defp query(state, ctx, sql, params) do
    transaction(state, ctx, fn conn ->
      {:ok, Postgrex.query!(conn, sql, params, timeout: state.timeout_ms)}
    end)
  end

  # Every statement is read-only: the query is the operator's, and the gateway
  # has no business writing to the customer's database.
  defp transaction(state, ctx, fun) do
    case Ankusa.whereis(ctx.instance, {:gateway_postgres, ctx.name}) do
      nil ->
        {:error, :not_running}

      pool ->
        try do
          Postgrex.transaction(
            pool,
            fn conn ->
              _ =
                Postgrex.query!(conn, "SET TRANSACTION READ ONLY", [], timeout: state.timeout_ms)

              fun.(conn)
            end,
            timeout: state.timeout_ms * 2
          )
          |> case do
            {:ok, result} -> result
            {:error, reason} -> {:error, reason}
          end
        rescue
          exception -> {:error, exception}
        catch
          kind, reason -> {:error, {kind, reason}}
        end
    end
  end
end
