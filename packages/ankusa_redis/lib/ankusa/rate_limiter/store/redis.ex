defmodule Ankusa.RateLimiter.Store.Redis do
  @moduledoc """
  The fleet store in Redis: one tenant quota and one dedupe window for every edge
  node that shares a `namespace`.

      fleet: %{
        store: {Ankusa.RateLimiter.Store.Redis,
                url: "redis://redis:6379", namespace: "ankusa:fleet", pool_size: 4},
        timeout_ms: 20
      },
      rate_limits: %{scope: :fleet, default: %{rate: 10, burst: 20}},
      dedupe: %{scope: :fleet}

  It implements `Ankusa.RateLimiter.Store` (`hit/4`, `reset/3`) and
  `Ankusa.Dedupe.Store` (`settle/3`); `Ankusa.Edge.Fleet` is its only caller.

  ## Options

    * `:url` (required) — the Redis URL
    * `:namespace` — key prefix, default `"ankusa:fleet:<instance>"`. Nodes share
      limits and dedupe **only if they use the same namespace** (and the same
      limits, which are read from each node's own config)
    * `:pool_size` — connections per node, default `4`. A request picks one by its
      pid, so a node's hooks pipeline over them instead of queueing on one socket

  ## Key layout

  | Key | Type | Contents |
  | --- | --- | --- |
  | `<ns>:rl:{<tenant>}:h` | string | the tenant's hook bucket: its GCRA theoretical arrival time, microseconds on Redis's clock; expires when the bucket is full again |
  | `<ns>:rl:{<tenant>}:b` | string | the tenant's byte bucket, same encoding |
  | `<ns>:dd:{<tenant>}:<len>:<source>:<key>` | string | `pending:<node>:<hook id>` (expires after `fleet.pending_ttl_ms`) or `committed:<hook id>` (expires after the source's dedupe TTL) |

  Every key of one hook carries the same `{<tenant>}` hash tag, so the one script
  that touches them is valid on a Redis Cluster. Time is Redis's own (`TIME`
  inside the script), so nodes with skewed clocks still share one bucket.

  ## One round trip

  `hit/4` is one `EVALSHA` for the whole hook: GCRA on every bucket, then the
  event-key claim. A denied bucket writes nothing and claims nothing; a key that
  is already claimed spends no bucket. The scripts are loaded with `SCRIPT LOAD`
  at boot, and a `NOSCRIPT` (Redis restarted) is answered with `EVAL`, which
  loads it again, inside the same call budget. `settle/3` pipelines a batch of
  claim outcomes the same way.

  ## Budget

  Every command carries the caller's `timeout_ms` as Redix's own command timeout,
  so a Redis that answers in 300 ms holds a hook for `timeout_ms`, not 5 s.
  Connecting never blocks the boot (`sync_connect: false`): a node starts, and
  limits and dedupes per node, until Redis answers.
  """

  use Supervisor

  @behaviour Ankusa.RateLimiter.Store
  @behaviour Ankusa.Dedupe.Store

  @default_pool_size 4

  # The reply is `[code, value]`: 0 = a bucket denied (value: retry after, ms),
  # 1 = admitted and claimed, 2 = a committed duplicate (value: its hook id),
  # 3 = claimed by a hook still in flight (value: retry after, ms, at most the
  # last argument).
  @hit_script """
  local n = tonumber(ARGV[1])
  local t = redis.call('TIME')
  local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
  local tats = {}
  for i = 1, n do
    local inc = tonumber(ARGV[2 * i])
    local cap = tonumber(ARGV[2 * i + 1])
    local tat = tonumber(redis.call('GET', KEYS[i]) or '0')
    local new_tat = math.max(tat, now) + inc
    local allow_at = new_tat - cap
    if now < allow_at then
      return {0, math.max(1, math.ceil((allow_at - now) / 1000))}
    end
    tats[i] = new_tat
  end
  local base = 2 * n + 2
  if ARGV[base] == '1' then
    local dk = KEYS[n + 1]
    if not redis.call('SET', dk, ARGV[base + 1], 'NX', 'PX', ARGV[base + 2]) then
      local cur = redis.call('GET', dk)
      if cur and string.sub(cur, 1, 10) == 'committed:' then
        return {2, string.sub(cur, 11)}
      end
      local hint = tonumber(ARGV[base + 3])
      local ttl = redis.call('PTTL', dk)
      if ttl < 1 or ttl > hint then ttl = hint end
      return {3, ttl}
    end
  end
  for i = 1, n do
    redis.call('SET', KEYS[i], string.format('%.0f', tats[i]), 'PX', math.ceil((tats[i] - now) / 1000) + 1)
  end
  return {1, 0}
  """

  # Promote (`p`): the key becomes committed unless another node's claim holds
  # it. Release (`r`): delete only this claim's own pending entry.
  @settle_script """
  local cur = redis.call('GET', KEYS[1])
  if ARGV[1] == 'p' then
    if cur == false or cur == ARGV[2] then
      redis.call('SET', KEYS[1], 'committed:' .. ARGV[3], 'PX', ARGV[4])
      return 1
    end
    return 0
  end
  if cur == ARGV[2] then
    redis.call('DEL', KEYS[1])
    return 1
  end
  return 0
  """

  @hit_sha :crypto.hash(:sha, @hit_script) |> Base.encode16(case: :lower)
  @settle_sha :crypto.hash(:sha, @settle_script) |> Base.encode16(case: :lower)

  # The most a `pending` answer tells a sender to wait: a claim normally settles
  # in milliseconds.
  @pending_hint_ms 1_000

  # ── process ─────────────────────────────────────────────────────────────────

  @spec start_link(keyword()) :: Supervisor.on_start()
  @impl Ankusa.RateLimiter.Store
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    Supervisor.start_link(__MODULE__, instance, name: Ankusa.via(instance, :fleet_store_sup))
  end

  @impl Supervisor
  def init(instance) do
    {url, _namespace, pool_size} = settings(instance)

    # The children's start arguments are printed by this supervisor's reports:
    # the password travels as an MFA read back on connect, never in the spec.
    redis = Ankusa.Redis.Options.start_opts(url, {__MODULE__, :password, [instance]})

    connections =
      for index <- 0..(pool_size - 1) do
        Supervisor.child_spec(
          {Redix, redis ++ [name: conn(instance, index), sync_connect: false]},
          id: {:redix, index}
        )
      end

    loader = %{
      id: :load_scripts,
      start: {Task, :start_link, [fn -> load_scripts(instance) end]},
      restart: :temporary
    }

    Supervisor.init(connections ++ [loader], strategy: :one_for_one)
  end

  @doc false
  # Redix calls this on every connect.
  @spec password(atom()) :: String.t() | nil
  def password(instance) do
    {url, _namespace, _pool_size} = settings(instance)
    Ankusa.Redis.Options.password(url)
  end

  # `SCRIPT LOAD` once the first connection is up, so the first hooks are
  # `EVALSHA` hits. Best effort: a `NOSCRIPT` loads it through `EVAL` anyway.
  defp load_scripts(instance, attempts \\ 30) do
    results =
      for script <- [@hit_script, @settle_script] do
        Redix.command(conn(instance, 0), ["SCRIPT", "LOAD", script], timeout: 1_000)
      end

    if Enum.any?(results, &match?({:error, _}, &1)) and attempts > 1 do
      Process.sleep(1_000)
      load_scripts(instance, attempts - 1)
    else
      :ok
    end
  end

  defp settings(instance) do
    {_module, opts} = Ankusa.config(instance).fleet.store

    {Keyword.fetch!(opts, :url), Keyword.get(opts, :namespace) || "ankusa:fleet:#{instance}",
     Keyword.get(opts, :pool_size, @default_pool_size)}
  end

  defp conn(instance, index), do: Ankusa.via(instance, {:fleet_redis, index})

  defp pick(instance, pool_size), do: conn(instance, :erlang.phash2(self(), pool_size))

  # ── Ankusa.RateLimiter.Store ────────────────────────────────────────────────

  @impl Ankusa.RateLimiter.Store
  def hit(instance, charges, claim, timeout_ms) do
    {_url, namespace, pool_size} = settings(instance)

    {bucket_keys, bucket_args} =
      charges
      |> Enum.map(fn {kind, tenant, increment, capacity} ->
        {bucket_key(namespace, kind, tenant), [increment, capacity]}
      end)
      |> Enum.unzip()

    {claim_keys, claim_args} = claim_parts(namespace, claim)
    keys = bucket_keys ++ claim_keys

    args =
      Enum.map([length(bucket_keys) | List.flatten(bucket_args)] ++ claim_args, &to_string/1)

    case eval(pick(instance, pool_size), @hit_sha, @hit_script, keys, args, timeout_ms) do
      {:ok, [0, ms]} -> {:rate_limited, ms}
      {:ok, [1, _]} -> :ok
      {:ok, [2, hook_id]} -> {:duplicate, hook_id}
      {:ok, [3, ms]} -> {:pending, ms}
      {:ok, other} -> {:error, {:unexpected_reply, other}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl Ankusa.RateLimiter.Store
  def reset(instance, tenant, timeout_ms) do
    {_url, namespace, pool_size} = settings(instance)
    keys = [bucket_key(namespace, :hooks, tenant), bucket_key(namespace, :bytes, tenant)]

    case Redix.command(pick(instance, pool_size), ["DEL" | keys], timeout: timeout_ms) do
      {:ok, _deleted} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # ── Ankusa.Dedupe.Store ─────────────────────────────────────────────────────

  @impl Ankusa.Dedupe.Store
  def settle(_instance, [], _timeout_ms), do: :ok

  def settle(instance, ops, timeout_ms) do
    {_url, namespace, pool_size} = settings(instance)
    conn = pick(instance, pool_size)
    calls = Enum.map(ops, &settle_call(namespace, &1))

    deadline = now_ms() + timeout_ms

    with {:ok, replies} <- pipeline(conn, calls, @settle_sha, deadline) do
      replies =
        if Enum.any?(replies, &noscript?/1),
          do: pipeline_eval(conn, calls, @settle_script, deadline),
          else: {:ok, replies}

      case replies do
        {:ok, results} -> first_error(results)
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp settle_call(namespace, {:promote, claim, hook_id}) do
    {[claim_key(namespace, claim)],
     ["p", pending_value(claim), hook_id, Integer.to_string(claim.ttl_ms)]}
  end

  defp settle_call(namespace, {:release, claim}) do
    {[claim_key(namespace, claim)], ["r", pending_value(claim), "", "0"]}
  end

  defp pipeline(conn, calls, sha, deadline) do
    commands =
      Enum.map(calls, fn {keys, args} ->
        ["EVALSHA", sha, Integer.to_string(length(keys)) | keys ++ args]
      end)

    Redix.pipeline(conn, commands, timeout: remaining(deadline))
  end

  defp pipeline_eval(conn, calls, script, deadline) do
    commands =
      Enum.map(calls, fn {keys, args} ->
        ["EVAL", script, Integer.to_string(length(keys)) | keys ++ args]
      end)

    Redix.pipeline(conn, commands, timeout: remaining(deadline))
  end

  defp first_error(results) do
    case Enum.find(results, &match?(%Redix.Error{}, &1)) do
      nil -> :ok
      %Redix.Error{} = error -> {:error, error}
    end
  end

  defp noscript?(%Redix.Error{message: "NOSCRIPT" <> _}), do: true
  defp noscript?(_reply), do: false

  # ── helpers ─────────────────────────────────────────────────────────────────

  # `EVALSHA`, and `EVAL` when Redis has not got the script (it restarted, or a
  # `SCRIPT FLUSH`): one budget for both.
  defp eval(conn, sha, script, keys, args, timeout_ms) do
    deadline = now_ms() + timeout_ms
    numkeys = Integer.to_string(length(keys))

    case Redix.command(conn, ["EVALSHA", sha, numkeys | keys ++ args], timeout: timeout_ms) do
      {:error, %Redix.Error{message: "NOSCRIPT" <> _}} ->
        Redix.command(conn, ["EVAL", script, numkeys | keys ++ args],
          timeout: remaining(deadline)
        )

      other ->
        other
    end
  end

  defp remaining(deadline), do: max(1, deadline - now_ms())
  defp now_ms, do: System.monotonic_time(:millisecond)

  defp bucket_key(namespace, :hooks, tenant), do: "#{namespace}:rl:{#{tenant}}:h"
  defp bucket_key(namespace, :bytes, tenant), do: "#{namespace}:rl:{#{tenant}}:b"

  defp claim_key(namespace, %{tenant: tenant, source_id: source_id, key: key}),
    do: "#{namespace}:dd:{#{tenant}}:#{byte_size(source_id)}:#{source_id}:#{key}"

  defp pending_value(%{node: node, hook_id: hook_id}), do: "pending:#{node}:#{hook_id}"

  defp claim_parts(_namespace, nil), do: {[], ["0", "", "0", "0"]}

  defp claim_parts(namespace, claim) do
    {[claim_key(namespace, claim)],
     [
       "1",
       pending_value(claim),
       Integer.to_string(claim.pending_ttl_ms),
       Integer.to_string(@pending_hint_ms)
     ]}
  end
end
