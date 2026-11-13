defmodule Ankusa.Gateway.Enricher do
  @moduledoc """
  One step of `gateway.auth.enrichers` (at most 4, in order): after an
  authenticator accepts a credential, enrichers add groups and claims to the
  principal — from an identity provider's API, a user directory, an internal
  service — before the principal is fingerprinted (`subject_key`, what the
  decision caches key on) and cached.

      auth: %{
        authenticators: [%{type: :jwt, issuer: "...", audience: "...", jwks_url: "..."}],
        enrichers: [
          %{type: :okta_groups, domain: "acme.okta.com", api_token: "00..."},
          %{type: :http, url: "https://dir.internal/users/{{ claims.uid }}",
            headers: %{"authorization" => "Bearer ..."}, groups_path: "data.groups"}
        ]
      }

  | Key | Default | |
  | --- | --- | --- |
  | `only` | `[]` (every authenticator) | the names of the authenticators this enricher runs after |
  | `on_unavailable` | `:fail` | `:fail`: `503 auth_unavailable`; `:skip`: continue with the unenriched principal |
  | `ttl_ms` | `60000` | how long the enricher's groups and claims are reused (1000..86400000) |
  | `timeout_ms` | `2000` | the enricher's own budget per call (100..30000) |

  An enricher's result is cached in the `:authn` cache under
  `{:enrich, index, authenticator name, principal.id}`, through one
  `Ankusa.Gateway.Caller` per enricher (`{:enrich, index}`), so a hundred
  requests for one principal make one call and a second token of the same
  subject reuses the first's groups. That entry outlives a token: `ttl_ms`,
  not `auth.cache.ttl_ms`, is how long a group removed at the source can still
  be seen.

  With `on_unavailable: :skip` the principal is used unenriched and cached for
  `auth.cache.negative_ttl_ms` only, so the groups come back as soon as the
  source does.

  An enricher is a module implementing this behaviour, named as the entry's
  `type` (`okta_groups` and `http` ship with the gateway, YAML included).
  """

  alias Ankusa.Gateway.{Cache, Caller, Options}
  alias Ankusa.Principal

  @type ctx :: %{
          instance: atom(),
          index: non_neg_integer(),
          authenticator: String.t(),
          timeout_ms: pos_integer()
        }

  @callback init!(opts :: map(), key :: String.t()) :: state :: map()

  @doc """
  The enriched principal, or `{:error, {:unavailable, reason}}` when its source
  could not answer (the entry's `on_unavailable` then decides).
  """
  @callback enrich(Principal.t(), state :: map(), ctx()) ::
              {:ok, Principal.t()} | {:error, {:unavailable, term()}}

  @doc "Children this enricher needs: its `Ankusa.Gateway.Caller` (see `caller_spec/4`), usually."
  @callback child_specs(instance :: atom(), index :: non_neg_integer(), state :: map()) :: [
              Supervisor.child_spec()
            ]

  @optional_callbacks child_specs: 3

  @common_keys [:only, :on_unavailable, :ttl_ms, :timeout_ms]
  @max_inflight 16
  @queue_max 256
  @breaker %{failures: 5, open_ms: 5_000}

  @doc "The option keys every enricher takes."
  @spec keys() :: [atom()]
  def keys, do: @common_keys

  @doc """
  The common options of `opts` (a map; the enricher checks its own keys),
  validated, with defaults. Raises `ArgumentError` naming `key.<option>`.
  """
  @spec common!(map(), String.t()) :: map()
  def common!(opts, key) do
    only =
      case Map.get(opts, :only, []) do
        list when is_list(list) ->
          Enum.map(list, &Options.name!(&1, "#{key}.only"))

        other ->
          raise ArgumentError,
                "#{key}.only must be a list of authenticator names, got #{inspect(other)}"
      end

    on_unavailable = Map.get(opts, :on_unavailable, :fail)

    unless on_unavailable in [:fail, :skip] do
      raise ArgumentError,
            "#{key}.on_unavailable must be :fail or :skip, got #{inspect(on_unavailable)}"
    end

    %{
      only: only,
      on_unavailable: on_unavailable,
      ttl_ms: Options.int_in!(Map.get(opts, :ttl_ms, 60_000), 1_000..86_400_000, "#{key}.ttl_ms"),
      timeout_ms:
        Options.int_in!(Map.get(opts, :timeout_ms, 2_000), 100..30_000, "#{key}.timeout_ms")
    }
  end

  @doc """
  The `Ankusa.Gateway.Caller` of enricher `index` (a `child_specs/3` returns
  `[caller_spec(instance, index, __MODULE__, state)]`): one process for its
  calls, 16 in flight, 256 queued, behind a breaker.
  """
  @spec caller_spec(atom(), non_neg_integer(), module(), map()) :: Supervisor.child_spec()
  def caller_spec(instance, index, module, state) do
    Caller.child_spec(
      instance: instance,
      name: {:enrich, index},
      label: "enrich:#{index}",
      max_inflight: @max_inflight,
      queue_max: @queue_max,
      breaker: @breaker,
      run: fn item -> run(instance, index, module, state, item) end
    )
  end

  @doc false
  # The caller's run: enrich `{principal, authenticator name}`, remember the
  # groups and claims the enricher contributed under the key the plug reads,
  # and answer them (so the waiters never recompute what was just cached).
  @spec run(atom(), non_neg_integer(), module(), map(), {Principal.t(), String.t()}) ::
          {:ok, map()} | {:error, {:unavailable, term()}}
  def run(instance, index, module, state, {principal, authenticator}) do
    ctx = %{
      instance: instance,
      index: index,
      authenticator: authenticator,
      timeout_ms: state.timeout_ms
    }

    case module.enrich(principal, state, ctx) do
      {:ok, %Principal{} = enriched} ->
        delta = %{groups: enriched.groups, claims: enriched.claims}
        Cache.put(instance, :authn, key(index, authenticator, principal.id), delta, state.ttl_ms)
        {:ok, delta}

      {:error, reason} ->
        {:error, {:unavailable, reason}}
    end
  end

  @doc false
  @spec key(non_neg_integer(), String.t(), String.t()) :: tuple()
  def key(index, authenticator, principal_id), do: {:enrich, index, authenticator, principal_id}
end
