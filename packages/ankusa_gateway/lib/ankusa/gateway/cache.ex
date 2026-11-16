defmodule Ankusa.Gateway.Cache do
  @moduledoc """
  The gateway's bounded caches, three per instance: `:authn` (authenticated
  credentials and enricher results), `:authz` (authorizer decisions) and
  `:acl` (native-grant documents). Each is a `Nebulex.Adapters.Local` cache
  bounded in entries and in bytes, its memory checked every second; an evicted
  entry only costs a recomputation, never a wrong answer.

  Instance-scoped: a cache registers under
  `Ankusa.via(instance, {:gateway_cache, name})` and the Nebulex API is called
  with its pid. Every function is a miss (or a no-op) while the cache is not
  running, so a request never fails because a cache restarted.
  """

  alias Ankusa.Gateway.Cache.Store

  @type name :: :authn | :authz | :acl

  @doc """
  The child spec of `instance`'s cache `name`, holding at most `max_entries`
  entries and `max_bytes` bytes. `max_ttl_ms` is the longest TTL it is asked to
  keep: the generational sweep must not run sooner than twice that.
  """
  @spec child_spec_for(atom(), name(), keyword()) :: Supervisor.child_spec()
  def child_spec_for(instance, name, opts) do
    max_ttl = Keyword.fetch!(opts, :max_ttl_ms)

    Supervisor.child_spec(
      {Store,
       [
         name: Ankusa.via(instance, {:gateway_cache, name}),
         max_size: Keyword.fetch!(opts, :max_entries),
         allocated_memory: Keyword.fetch!(opts, :max_bytes),
         gc_interval: max(60_000, 2 * max_ttl),
         # The adapter enforces the bounds in this periodic check, not on each
         # write: once a second.
         gc_memory_check_interval: 1_000,
         # No per-command spans: the gateway's own events are the surface.
         telemetry: false
       ]},
      id: {__MODULE__, instance, name}
    )
  end

  @doc "The value under `key`, or `:error` (a miss, or the cache is not running)."
  @spec fetch(atom(), name(), term()) :: {:ok, term()} | :error
  def fetch(instance, name, key) do
    # Nebulex 3 reports a miss as `{:ok, default}`: `nil` is never stored.
    with pid when is_pid(pid) <- whereis(instance, name),
         {:ok, value} when value != nil <- Store.get(pid, key, nil, []) do
      {:ok, value}
    else
      _miss -> :error
    end
  catch
    :exit, _ -> :error
  end

  @doc "Store `value` under `key` for `ttl_ms` (no-op for a TTL <= 0 or a cache that is not running)."
  @spec put(atom(), name(), term(), term(), integer()) :: :ok
  def put(_instance, _name, _key, _value, ttl_ms) when ttl_ms <= 0, do: :ok

  def put(instance, name, key, value, ttl_ms) do
    case whereis(instance, name) do
      nil -> :ok
      pid -> Store.put(pid, key, value, ttl: ttl_ms)
    end

    :ok
  catch
    :exit, _ -> :ok
  end

  @doc "Drop `key`."
  @spec delete(atom(), name(), term()) :: :ok
  def delete(instance, name, key) do
    case whereis(instance, name) do
      nil -> :ok
      pid -> Store.delete(pid, key, [])
    end

    :ok
  catch
    :exit, _ -> :ok
  end

  @doc "Entries held and bytes used, `%{entries: 0, bytes: 0}` when not running."
  @spec stats(atom(), name()) :: %{entries: non_neg_integer(), bytes: non_neg_integer()}
  def stats(instance, name) do
    with pid when is_pid(pid) <- whereis(instance, name),
         {:ok, entries} <- Store.count_all(pid, [], []),
         {:ok, %{used: bytes}} <- Store.info(pid, :memory, []) do
      %{entries: entries, bytes: bytes}
    else
      _ -> %{entries: 0, bytes: 0}
    end
  catch
    :exit, _ -> %{entries: 0, bytes: 0}
  end

  defp whereis(instance, name), do: Ankusa.whereis(instance, {:gateway_cache, name})
end
