defmodule Ankusa.Dedupe.Store do
  @moduledoc """
  The second phase of a fleet dedupe claim (`dedupe.scope: :fleet`).

  `c:Ankusa.RateLimiter.Store.hit/4` claims a provider event key as *pending*,
  ahead of the queue writer's own check. What happens after the hook's commit
  settles it, here, in batches, off the request path (`Ankusa.Edge.Fleet`):

    * `{:promote, claim, hook_id}` — the hook is durable on its node: the key
      becomes *committed* for the source's dedupe TTL, with `hook_id` as the id
      every later retry is answered with. It applies to a key still pending under
      this claim, or one that expired meanwhile; it never overwrites another
      node's claim.
    * `{:release, claim}` — the commit failed, or the node answered something
      that stored nothing: the key is deleted if, and only if, it is still this
      claim's pending entry, so the provider's retry (here or on another node)
      claims it afresh.

  A settle that is lost (the store is down, the node died) costs a duplicate or
  a delay, never a hook: a pending key expires after `fleet.pending_ttl_ms`, and
  until it does another node answers `409 dedupe_pending`, not a `2xx`. The
  node's own dedupe index, written in the commit's batch, still collapses the
  retry on the node that stored the hook.
  """

  alias Ankusa.RateLimiter.Store

  @type op :: {:promote, Store.claim(), hook_id :: String.t()} | {:release, Store.claim()}

  @doc "Apply `ops` in one pipelined call that returns within `timeout_ms`."
  @callback settle(instance :: atom(), ops :: [op()], timeout_ms :: pos_integer()) ::
              :ok | {:error, term()}
end
