defmodule Ankusa.Redis.Scan do
  @moduledoc """
  Read a Redis hash a page at a time with `HSCAN`, for the stores that mirror a
  whole hash into memory (`Ankusa.Routes.Store.Redis`,
  `Ankusa.SourceStore.Redis`).

  `HGETALL` is one command whose cost grows with the hash: at a million entries
  it holds Redis's single thread for the whole read and the connection for as
  long, so it either hits the command timeout or stalls every other client. A
  scan is many short commands (`COUNT 1000` entries each), each bounded by its
  own timeout, with a deadline over the whole walk, and the caller handles
  every page as it arrives instead of building a million-entry list first.

  `HSCAN` is not a snapshot: an entry is returned at least once if it was in
  the hash for the whole walk, possibly more than once, and an entry written or
  deleted meanwhile may or may not appear. A reducer must therefore be
  idempotent (a put by key), and a caller that needs a consistent read compares
  the hash's version before and after (both stores bump one in the same step as
  every write).
  """

  @default_count 1_000

  @type reducer(acc) :: ([{String.t(), String.t()}], acc -> {:ok, acc} | {:error, term()})

  @doc """
  Fold `fun` over the field/value pairs of the hash at `key`, one `HSCAN` page
  at a time.

  Options: `:timeout` (required, milliseconds for each command), `:deadline`
  (required, an absolute `System.monotonic_time(:millisecond)` the walk must be
  done by) and `:count` (the page-size hint, default #{@default_count}).

  A command failure is `{:error, {:redis_unavailable, reason}}`, a missed
  deadline `{:error, {:redis_unavailable, :load_timeout}}`, and whatever `fun`
  returns as an error ends the walk with that error.
  """
  @spec reduce(GenServer.server(), String.t(), acc, reducer(acc), keyword()) ::
          {:ok, acc} | {:error, term()}
        when acc: term()
  def reduce(conn, key, acc, fun, opts) do
    timeout = Keyword.fetch!(opts, :timeout)
    deadline = Keyword.fetch!(opts, :deadline)
    count = Integer.to_string(Keyword.get(opts, :count, @default_count))

    page(conn, key, "0", acc, fun, {count, timeout, deadline})
  end

  defp page(conn, key, cursor, acc, fun, {count, timeout, deadline} = limits) do
    if System.monotonic_time(:millisecond) > deadline do
      {:error, {:redis_unavailable, :load_timeout}}
    else
      case Redix.command(conn, ["HSCAN", key, cursor, "COUNT", count], timeout: timeout) do
        {:ok, [next, flat]} ->
          with {:ok, acc} <- fun.(pairs(flat), acc) do
            if next == "0", do: {:ok, acc}, else: page(conn, key, next, acc, fun, limits)
          end

        {:error, reason} ->
          {:error, {:redis_unavailable, reason}}
      end
    end
  end

  defp pairs(flat),
    do: flat |> Enum.chunk_every(2) |> Enum.map(fn [field, value] -> {field, value} end)
end
