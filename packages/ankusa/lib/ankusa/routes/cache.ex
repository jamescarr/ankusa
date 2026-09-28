defmodule Ankusa.Routes.Cache do
  @moduledoc """
  The decision cache: `{version, method, segments}` → matched route id, or the
  reason no route matched.

  Pattern routes can't be found by one key lookup, so the guard would otherwise
  scan the compiled pattern list on every request. Caching the **decision**
  turns the common case — the same provider POSTing to the same path over and
  over — into one ETS read.

  ## Only matching is cached

  IP decisions are not cached: they are per-client-address, and the rule list is
  a short in-memory scan of bitmasks. Caching them would mean a key space
  proportional to the number of distinct senders, for a saving smaller than the
  cache lookup itself.

  ## Version-keyed, plus a TTL

  The key carries the snapshot version, so a mutation makes every entry
  unreachable immediately — no delete pass, no invalidation broadcast. Stale
  entries age out on their own TTL and are collected by the adapter's
  generational sweep; nothing depends on them expiring, so the TTL only has to
  bound memory.

  A cached rejection uses a shorter TTL than a cached match: a rejection is what
  a scanner hitting random paths produces, and there are far more of those than
  there are real routes.

  ## Why Nebulex

  `nebulex_local` is a generation-based ETS cache with `max_size` enforcement;
  the alternative was hand-rolling eviction, TTL expiry, and a sweep timer for
  the same thing. Core carries the dependency because every deployment that
  turns routes on wants it — the Redis *store* is the part that stays in an
  adapter package, since only multi-node deployments need it.

  Instance-scoped: the cache process registers under
  `Ankusa.via(instance, :routes_cache)`, and the Nebulex API is addressed with
  that process's pid (Nebulex resolves a cache by atom or pid, and a
  `Registry`-based name is neither).
  """

  use Nebulex.Cache, otp_app: :ankusa, adapter: Nebulex.Adapters.Local

  @type decision :: {:match, String.t()} | {:reject, :no_route | :method}

  @doc "Look up the cached decision for this exact request shape."
  @spec lookup(atom(), pos_integer(), String.t(), [String.t()]) :: {:ok, decision()} | :error
  def lookup(instance, version, method, segments) do
    # Nebulex 3 returns `{:ok, value}` and reports a miss as `{:ok, default}`, so
    # `nil` — which is never a cached decision — is the miss marker here.
    with pid when is_pid(pid) <- cache(instance),
         {:ok, decision} when decision != nil <-
           get(pid, key(version, method, segments), nil, []) do
      {:ok, decision}
    else
      _miss -> :error
    end
  end

  @doc "Cache a decision for `ttl_ms`, or do nothing if the cache isn't running."
  @spec store(atom(), pos_integer(), String.t(), [String.t()], decision(), pos_integer()) :: :ok
  def store(instance, version, method, segments, decision, ttl_ms) do
    case cache(instance) do
      nil -> :ok
      pid -> put(pid, key(version, method, segments), decision, ttl: ttl_ms)
    end
  end

  defp key(version, method, segments), do: {version, method, segments}

  defp cache(instance), do: Ankusa.whereis(instance, :routes_cache)
end
