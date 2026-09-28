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
        ip_rules: %{default: :allow | :deny, rules: [%{action: atom(), cidr: CIDR.t()}]}
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

  @doc """
  Compile routes into a snapshot. `routes` is the stores' `%{id => %Route{}}`
  map, `ip_rules` the global rule list already parsed into CIDRs.
  """
  @spec build(%{String.t() => Route.t()}, map(), pos_integer()) :: map()
  def build(routes, ip_rules, version) do
    placed =
      Enum.map(routes, fn {_id, %Route{} = route} ->
        %{route: route, segments: Matcher.compile(route)}
      end)

    %{
      version: version,
      patterns: Enum.sort_by(placed, &priority/1),
      by_id: Map.new(routes, fn {id, route} -> {id, route} end),
      ip_rules: ip_rules
    }
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
