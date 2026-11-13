defmodule Ankusa.Gateway.Authorizer do
  @moduledoc """
  One step of a gateway policy (`gateway.authz.authorizers`).

  `decide/3` answers for one `Ankusa.Gateway.Authz.Request`:

    * `{:ok, :allow, meta}`, `{:ok, :deny, meta}` or `{:ok, :abstain, meta}`;
      `meta` may carry `:ttl_ms` (a freshness hint for a cached allow),
      `:scope` (named in the `403` body) and `:reason`.
    * `{:error, {:unavailable, reason}}` — it could not decide; the step's
      `on_unavailable` says what that means for the policy.

  `kind/1` says where it runs: `:inline` in the request process, `:cached`
  through the gateway's decision cache, `:remote` through the cache and a
  per-authorizer caller (coalescing, a concurrency limit, batching through
  `decide_many/3`, a breaker). A `:cached` or `:remote` authorizer decides
  from `request.principal`, `request.via`, `request.action` and
  `request.resource` only (plus whatever its `cache_scope/2` adds to the cache
  key); `request.conn` is for `:inline` ones (a `:remote` one gets `nil`, and
  reads the caller's credential, when it forwards it, from
  `request.authorization`, its `cache_scope/2` then keying the decision by
  that credential).

  `init!/2` validates the authorizer's options (raising `ArgumentError` naming
  `key`, `"gateway.authz.authorizers.<name>"`) and returns its state.
  `ready?/3`, when exported, holds the gateway's `/ready` at `503` while it is
  `false`.
  """

  @type ctx :: %{instance: atom(), name: String.t(), deadline: integer()}
  @type meta :: %{
          optional(:ttl_ms) => non_neg_integer(),
          optional(:scope) => String.t(),
          optional(:reason) => atom()
        }
  @type result :: {:ok, :allow | :deny | :abstain, meta()} | {:error, {:unavailable, term()}}

  @callback init!(opts :: map(), key :: String.t()) :: state :: map()
  @callback kind(state :: map()) :: :inline | :cached | :remote
  @callback decide(Ankusa.Gateway.Authz.Request.t(), state :: map(), ctx()) :: result()
  @callback decide_many([Ankusa.Gateway.Authz.Request.t()], state :: map(), ctx()) :: [result()]
  @callback cache_scope(Ankusa.Gateway.Authz.Request.t(), state :: map()) :: term()
  @callback child_specs(instance :: atom(), name :: String.t(), state :: map()) :: [
              Supervisor.child_spec()
            ]
  @callback ready?(instance :: atom(), name :: String.t(), state :: map()) :: boolean()
  @optional_callbacks decide_many: 3, cache_scope: 2, child_specs: 3, ready?: 3
end
