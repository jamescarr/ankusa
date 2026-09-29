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
        ip_rules: %{default: :allow | :deny, rules: [%{action: atom(), cidr: %CIDR{}}]},
        trusted_proxies: [%CIDR{}]
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

  @doc """
  Build the boot-time `routes` and `ip_rules` for a store from `config.routes`,
  reporting the same failures `Ankusa.Routes.validate_config!/1` already raises
  as error tuples a store can `{:stop, reason}` on.

  The check order mirrors `validate_seed!`: a non-list seed, then the cap, then
  per-entry validation, then duplicate ids, then compiled-segment conflicts, then
  the global rules.
  """
  @spec initial_table(map()) ::
          {:ok,
           %{
             routes: %{String.t() => Route.t()},
             ip_rules: %{default: :allow | :deny, rules: [Route.ip_rule()]}
           }}
          | {:error,
             {:invalid_seed, non_neg_integer(), String.t(), String.t()}
             | {:duplicate_seed_ids, String.t(), non_neg_integer()}
             | {:seed_too_large, non_neg_integer(), pos_integer()}
             | {:seed_conflict, non_neg_integer(), non_neg_integer(), String.t(), String.t()}
             | {:seed_not_a_list, term()}
             | {:invalid_ip_rules, String.t()}}
  def initial_table(%{seed: seed, max_routes: max_routes, ip_rules: ip_rules_config}) do
    cond do
      not is_list(seed) ->
        {:error, {:seed_not_a_list, seed}}

      length(seed) > max_routes ->
        {:error, {:seed_too_large, length(seed), max_routes}}

      true ->
        with {:ok, parsed} <- parse_seed(seed),
             :ok <- unique_seed_ids(parsed),
             :ok <- no_seed_conflicts(parsed),
             {:ok, ip_rules} <- parse_global_rules(ip_rules_config) do
          {:ok,
           %{
             routes: Map.new(parsed, fn {route, _index} -> {route.id, route} end),
             ip_rules: ip_rules
           }}
        end
    end
  end

  defp parse_seed(seed) do
    seed
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {attrs, index}, {:ok, acc} ->
      case Route.from_attrs(attrs) do
        {:ok, route} ->
          {:cont, {:ok, [{route, index} | acc]}}

        {:error, {:invalid, field, message}} ->
          {:halt, {:error, {:invalid_seed, index, field, message}}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp unique_seed_ids(parsed) do
    case Enum.find(Enum.group_by(parsed, fn {route, _index} -> route.id end), fn {_id, group} ->
           length(group) > 1
         end) do
      nil ->
        :ok

      {id, [_first, {_route, index} | _rest]} ->
        {:error, {:duplicate_seed_ids, id, index}}
    end
  end

  defp no_seed_conflicts(parsed) do
    parsed
    |> Enum.reduce_while([], fn {route, index}, accepted ->
      new = Matcher.compile(route)

      case Enum.find(accepted, fn {other, _first_index} ->
             other.id != route.id and other.enabled and Matcher.compile(other) == new and
               Enum.any?(other.methods, &(&1 in route.methods))
           end) do
        nil ->
          {:cont, [{route, index} | accepted]}

        {_other, first_index} ->
          {:halt,
           {:error,
            {:seed_conflict, index, first_index, Enum.join(route.methods, "/"), route.path}}}
      end
    end)
    |> case do
      {:error, _} = err -> err
      _accepted -> :ok
    end
  end

  defp parse_global_rules(%{default: default, rules: rules}) do
    case Route.parse_rules(rules) do
      {:ok, parsed} -> {:ok, %{default: default, rules: parsed}}
      {:error, message} -> {:error, {:invalid_ip_rules, message}}
    end
  end

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
