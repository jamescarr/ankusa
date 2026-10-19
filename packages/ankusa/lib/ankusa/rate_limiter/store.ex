defmodule Ankusa.RateLimiter.Store do
  @moduledoc """
  A shared store for the tenant quota and the provider-event claim, behind
  `rate_limits.scope: :fleet` and `dedupe.scope: :fleet` (`fleet.store`).

  The default scope, `:node`, needs none: `Ankusa.Edge.RateLimiter` keeps its
  buckets in this node's ETS, and the queue writer keeps its dedupe index in the
  node's store. A fleet store makes both shared by every edge node of a fleet.
  `Ankusa.RateLimiter.Store.Redis` (the `ankusa_redis` package) is the
  implementation; the same module also implements `Ankusa.Dedupe.Store`, which
  settles the claims made here.

  ## One round trip per hook

  `hit/4` takes the tenant's charges and the dedupe claim together, so a fleet
  that scopes both costs one call per hook, never two. The charges are GCRA, the
  algorithm `Ankusa.Edge.RateLimiter` runs in ETS: a denied charge writes nothing,
  and a hook that is a duplicate or in flight elsewhere spends none of the
  tenant's budget.

  ## The claim is two-phase

  A claim is *pending* until the hook that made it is durable on its node. Only
  `c:Ankusa.Dedupe.Store.settle/3` makes it *committed*, after the synced commit;
  a node that fails to commit releases it. Until then another node that meets the
  key is told `{:pending, retry_after_ms}` (the sender retries), never "duplicate":
  a duplicate answer is a `2xx`, and a `2xx` for a hook nobody stored is a lost
  webhook. A node that dies holding a claim leaves it to expire after
  `fleet.pending_ttl_ms`.

  ## Failure is a value

  A store that cannot answer within `timeout_ms` returns `{:error, reason}`. The
  edge then limits and dedupes from this node's own state (`Ankusa.Edge.Fleet`),
  so an implementation must honour the budget: it is a hard per-call limit, not a
  hint.
  """

  @typedoc """
  One bucket to charge: `kind` (`:hooks` or `:bytes`), the tenant, the GCRA
  `increment` and `capacity`, both in microseconds.
  """
  @type charge ::
          {kind :: :hooks | :bytes, tenant :: String.t(), increment :: pos_integer(),
           capacity :: pos_integer()}

  @typedoc """
  A provider event key to claim: scoped by `tenant` and `source_id`, claimed by
  `hook_id` on `node`, committed for `ttl_ms` once the hook is durable and
  pending for at most `pending_ttl_ms` before that.
  """
  @type claim :: %{
          tenant: String.t(),
          source_id: String.t(),
          key: String.t(),
          hook_id: String.t(),
          node: node(),
          ttl_ms: pos_integer(),
          pending_ttl_ms: pos_integer()
        }

  @type result ::
          :ok
          | {:duplicate, hook_id :: String.t()}
          | {:pending, retry_after_ms :: pos_integer()}
          | {:rate_limited, retry_after_ms :: pos_integer()}
          | {:error, term()}

  @doc """
  Start the store's processes. `opts` is `[instance: atom]` and the instance's
  `fleet.store` options are read from its config. Must not fail, or block, on a
  store that is unreachable: a fleet store outage degrades the edge, it never
  stops it.
  """
  @callback start_link(keyword()) :: Supervisor.on_start()

  @doc """
  Charge every bucket in `charges` and, when `claim` is not `nil`, claim its key
  — in one call that returns within `timeout_ms`. A denied charge returns
  `{:rate_limited, ms}` before the claim and writes nothing; a claim that is
  taken returns `{:duplicate, hook_id}` (committed) or `{:pending, ms}` (in
  flight) and spends no charge. `:ok` means every charge was taken and the claim,
  if any, is now this hook's.
  """
  @callback hit(
              instance :: atom(),
              charges :: [charge()],
              claim :: claim() | nil,
              timeout_ms :: pos_integer()
            ) :: result()

  @doc "Forget `tenant`'s buckets (a limit override changed), best effort."
  @callback reset(instance :: atom(), tenant :: String.t(), timeout_ms :: pos_integer()) ::
              :ok | {:error, term()}
end
