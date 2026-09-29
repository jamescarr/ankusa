defmodule Ankusa.Routes.Cache do
  @max_segments 16
  @max_bytes 256

  @moduledoc """
  The decision cache: `{epoch, method, segments}` → matched route id, or the
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

  ## Epoch-keyed, plus a TTL

  The key carries the snapshot's `epoch`: a number `Ankusa.Routes.Snapshot`
  draws afresh for every snapshot it publishes and never reuses. Publishing a
  snapshot therefore makes every earlier entry unreachable at once — no delete
  pass, no invalidation broadcast.

  The epoch, not the store's `version`, is the key because versions *are*
  reused: an in-memory store that restarts starts over at 1, and a Redis that is
  flushed starts its counter again, and either would let an entry cached against
  the old table answer for the new one. Stale entries age out on their own TTL
  and are collected by the adapter's generational sweep; nothing depends on them
  expiring, so the TTL only has to bound memory.

  A cached rejection uses a shorter TTL than a cached match: a rejection is what
  a scanner hitting random paths produces, and there are far more of those than
  there are real routes.

  ## Bounded keys

  The key holds the request's own path, which the sender chooses. Only requests
  of at most #{@max_segments} segments and #{@max_bytes} bytes of path are cached
  (see `cacheable?/1`); anything longer is matched by a scan and never stored, so
  a scanner sending long random paths can't fill the cache with large keys.

  ## Why Nebulex

  `nebulex_local` is a generation-based ETS cache with `max_size`; the
  alternative was hand-rolling eviction, TTL expiry, and a sweep timer for the
  same thing. It enforces `max_size` in a periodic memory check
  (`gc_memory_check_interval`, one second here) rather than on every write, so
  the size is approximate between checks — and an evicted entry only costs a
  re-scan, never a wrong decision. Core carries the dependency because every
  deployment that turns routes on wants it — the Redis *store* is the part that
  stays in an adapter package, since only multi-node deployments need it.

  Instance-scoped: the cache process registers under
  `Ankusa.via(instance, :routes_cache)`, and the Nebulex API is addressed with
  that process's pid (Nebulex resolves a cache by atom or pid, and a
  `Registry`-based name is neither).
  """

  use Nebulex.Cache, otp_app: :ankusa, adapter: Nebulex.Adapters.Local

  @type decision :: {:match, String.t()} | {:reject, :no_route | :method}

  @doc """
  Is this request path short enough to be a cache key? The limits are on the
  segment count and the summed segment bytes, walked without allocating.
  """
  @spec cacheable?([String.t()]) :: boolean()
  def cacheable?(segments), do: within?(segments, @max_segments, @max_bytes)

  defp within?([], _segments_left, _bytes_left), do: true
  defp within?(_segments, 0, _bytes_left), do: false

  defp within?([segment | rest], segments_left, bytes_left) do
    bytes_left = bytes_left - byte_size(segment)
    bytes_left >= 0 and within?(rest, segments_left - 1, bytes_left)
  end

  @doc "Look up the cached decision for this exact request shape."
  @spec lookup(atom(), pos_integer(), String.t(), [String.t()]) :: {:ok, decision()} | :error
  def lookup(instance, epoch, method, segments) do
    # Nebulex 3 returns `{:ok, value}` and reports a miss as `{:ok, default}`, so
    # `nil` — which is never a cached decision — is the miss marker here.
    with pid when is_pid(pid) <- cache(instance),
         {:ok, decision} when decision != nil <-
           get(pid, key(epoch, method, segments), nil, []) do
      {:ok, decision}
    else
      _miss -> :error
    end
  end

  @doc "Cache a decision for `ttl_ms`, or do nothing if the cache isn't running."
  @spec store(atom(), pos_integer(), String.t(), [String.t()], decision(), pos_integer()) :: :ok
  def store(instance, epoch, method, segments, decision, ttl_ms) do
    case cache(instance) do
      nil -> :ok
      pid -> put(pid, key(epoch, method, segments), decision, ttl: ttl_ms)
    end
  end

  defp key(epoch, method, segments), do: {epoch, method, segments}

  defp cache(instance), do: Ankusa.whereis(instance, :routes_cache)
end
