defmodule Ankusa.Routes.Snapshot do
  @moduledoc """
  The compiled, in-memory view of the route table: what the guard reads on
  every request, and what makes a mutation take effect everywhere at once.

  Both stores (`Ankusa.Routes.Store.ETS` and `Ankusa.Routes.Store.Redis`) build
  their snapshots here, so a definition produces the same `patterns` order and
  the same `by_id` map whichever store it lives in.

  It lives in `:persistent_term`, written on boot and after every mutation.
  Reads are lock-free and allocation-free — the guard must never take a
  GenServer call or a network round trip per request — and writes are rare
  (route changes), which is the trade `:persistent_term` wants: a global GC
  per write, in exchange for cheapest-possible reads.

  ## Shape

      %{
        version: pos_integer(),
        patterns: [%{route: Route.t(), segments: [Matcher.segment()]}],
        by_id: %{String.t() => Route.t()},
        ip_rules: %{default: :allow | :deny, rules: [%{action: atom(), cidr: CIDR.t()}]},
        trusted_proxies: [CIDR.t()]
      }

  `patterns` holds **every** route, enabled or not; the guard skips a disabled
  one at match time, so toggling `enabled` is a state change, not a recompile.

  ## Ordering

  The first matching pattern wins, so the order is the priority list, most
  specific first:

    1. descending literal-segment count — `/hooks/stripe` before `/hooks/:id`;
    2. patterns without a wildcard before those with one — `/hooks/a` before
       `/hooks/*`;
    3. route id ascending, so the order is total and never depends on map
       iteration.

  `version` is bumped by every mutation. The decision cache
  (`Ankusa.Routes.Cache`) keys on it, so a change makes every cached decision
  unreachable without a delete pass.
  """

  alias Ankusa.Routes.{Matcher, Route}
  alias CIDR

  @doc """
  Compile a store's state into a snapshot. `state` is the stores'
  `%{instance:, routes:, ip_rules:, version:}` map; `trusted_proxies` is folded
  in from the instance's config, so the guard reads them once per request.
  """
  @spec build(map()) :: map()
  def build(state) do
    %{routes: routes, ip_rules: ip_rules, version: version, instance: instance} = state

    placed =
      Enum.map(routes, fn {_id, %Route{} = route} ->
        %{route: route, segments: Matcher.compile(route)}
      end)

    %{
      version: version,
      patterns: Enum.sort_by(placed, &priority/1),
      by_id: Map.new(routes, fn {id, route} -> {id, route} end),
      ip_rules: ip_rules,
      trusted_proxies: trusted_proxies(instance)
    }
  end

  @doc """
  Rebuild, write, and emit the change telemetry for a mutation. Returns `state`.
  The caller owns `state.version` — the ETS store bumps it locally, the Redis
  store takes it from `INCR` — so no bump happens here.
  """
  @spec publish(map(), {atom(), String.t() | nil}) :: map()
  def publish(state, {action, route_id}) do
    publish(state)

    Ankusa.Telemetry.emit([:routes, :changed], %{}, %{
      instance: state.instance,
      action: action,
      route_id: route_id,
      version: state.version
    })

    state
  end

  @doc "Rebuild and write the snapshot without telemetry (boot, seed, reload)."
  @spec publish(map()) :: :ok
  def publish(state), do: put(state.instance, build(state))

  defp trusted_proxies(instance) do
    Enum.flat_map(Ankusa.config(instance).routes.trusted_proxies, fn cidr ->
      case CIDR.parse(cidr) do
        %CIDR{} = parsed -> [parsed]
        {:error, _} -> []
      end
    end)
  end

  @doc "Write a snapshot where the guard reads it: instance-scoped `:persistent_term`."
  @spec put(atom(), map()) :: :ok
  def put(instance, snapshot), do: :persistent_term.put(key(instance), snapshot)

  @doc "The snapshot for `instance`, or `nil` before the store has written one."
  @spec get(atom()) :: map() | nil
  def get(instance), do: :persistent_term.get(key(instance), nil)

  defp key(instance), do: {Ankusa.Routes, :snapshot, instance}

  # Ascending sort key: negative literal count is "most literals first", a
  # wildcard sorts after everything without one.
  defp priority(%{route: route, segments: segments}) do
    {literal_count, wildcard?} = Matcher.specificity(segments)
    {-literal_count, if(wildcard?, do: 1, else: 0), route.id}
  end
end
