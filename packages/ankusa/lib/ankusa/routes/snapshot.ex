defmodule Ankusa.Routes.Snapshot do
  @moduledoc """
  The compiled, in-memory view of the route table: what the guard reads on
  every request, and what makes a mutation take effect everywhere at once.

  Both stores (`Ankusa.Routes.Store.ETS` and `Ankusa.Routes.Store.Redis`) build
  their snapshots here, so a definition produces the same pattern order and
  the same id lookup whichever store it lives in.

  ## Where it lives

  One named `:protected` ETS table per instance (`ordered_set`,
  `read_concurrency: true`), owned by the routes store process: readers never
  take a GenServer call or a network round trip per request, and a mutation
  writes the rows it changes, not the whole table. (It used to be one
  `:persistent_term` value rebuilt and rewritten per mutation, which costs a
  global GC pass and a full copy each time.) The table dies with its owner;
  the store restarting writes it again.

  Rows, all tagged with a *generation*:

    * `{:meta, %{gen, version, epoch, ip_rules, trusted_proxies, count}}` — the
      one row a reader starts from;
    * `{{:pattern, gen, priority, id}, route, segments}` — every route, enabled
      or not, in priority order (the table's own key order);
    * `{{:id, gen, id}, route}`.

  A reader reads `:meta` once and uses its `gen` for every other row.
  `publish/1` (boot, a Redis reload) writes a whole new generation, then flips
  `:meta` to it, and drops the old generation a second later, so a reader that
  read the old `:meta` just before the flip finishes its walk on rows that are
  still there. `mutate/3` (one route created, replaced or deleted in the ETS
  store) edits the current generation in place and writes a new `:meta`.

  Every route is a pattern row, enabled or not; the guard skips a disabled
  one at match time, so toggling `enabled` is a state change, not a recompile.

  ## Ordering

  The first matching pattern wins, so the order is the priority list, most
  specific first:

    1. descending literal-segment count — `/hooks/stripe` before `/hooks/:id`;
    2. patterns without a wildcard before those with one — `/hooks/a` before
       `/hooks/*`;
    3. route id ascending, so the order is total and never depends on map
       iteration.

  `version` belongs to the store: every mutation bumps it, and a write is only
  applied if its writer validated against the current one (see
  `Ankusa.Routes.Store`). `epoch` belongs to the snapshot: a number drawn afresh
  for every change and never reused, which the decision cache
  (`Ankusa.Routes.Cache`) keys on, so a change makes every cached decision
  unreachable without a delete pass. It is separate from `version` because a
  store's version can start over (a restarted in-memory store, a flushed Redis)
  while a cache entry keyed on it would still be alive.
  """

  alias Ankusa.Net
  alias Ankusa.Routes.{Matcher, Route}

  # How long an old generation outlives the flip to a new one: a request reads
  # its rows in microseconds, so a second is generous.
  @drop_after_ms 1_000

  @type meta :: %{
          gen: pos_integer(),
          version: pos_integer(),
          epoch: pos_integer(),
          ip_rules: %{default: :allow | :deny, rules: [Route.ip_rule()]},
          trusted_proxies: [term()],
          count: non_neg_integer()
        }

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
      epoch: System.unique_integer([:positive, :monotonic]),
      patterns: Enum.sort_by(placed, &priority/1),
      by_id: Map.new(routes, fn {id, route} -> {id, route} end),
      ip_rules: ip_rules,
      trusted_proxies: trusted_proxies(instance)
    }
  end

  @doc """
  Write a whole new generation, emit the change telemetry, and return `state`.
  The caller owns `state.version` — the ETS store bumps it locally, the Redis
  store takes it from `INCR` — so no bump happens here.
  """
  @spec publish(map(), {atom(), String.t() | nil}) :: map()
  def publish(state, {action, route_id}) do
    publish(state)
    changed(state, action, route_id)
    state
  end

  @doc """
  Write a whole new generation without telemetry (boot, seed, reload), from
  the process that owns the table (it is created, owned by the caller, if it
  does not exist yet). The caller must handle `{:drop_generation, gen}` with
  `drop_generation/2`.
  """
  @spec publish(map()) :: :ok
  def publish(state) do
    table = ensure_table(state.instance)
    snapshot = build(state)
    old = current_gen(table)
    gen = System.unique_integer([:positive, :monotonic])

    patterns =
      Enum.map(snapshot.patterns, fn %{route: route, segments: segments} = placed ->
        {pattern_key(gen, placed), route, segments}
      end)

    ids = Enum.map(snapshot.by_id, fn {id, route} -> {{:id, gen, id}, route} end)
    true = :ets.insert(table, patterns ++ ids)

    meta = %{
      gen: gen,
      version: snapshot.version,
      epoch: snapshot.epoch,
      ip_rules: snapshot.ip_rules,
      trusted_proxies: snapshot.trusted_proxies,
      count: map_size(snapshot.by_id)
    }

    true = :ets.insert(table, {:meta, meta})
    if old, do: Process.send_after(self(), {:drop_generation, old}, @drop_after_ms)
    :ok
  end

  @doc """
  Apply one change to the current generation in place — `{:put, route}`,
  `{:delete, id}` or `:ip_rules` — write a fresh `:meta` (new epoch, the
  store's `version`, `ip_rules` and count), and emit the change telemetry.
  Returns `state`. Called by the owning process after it updated `state`.
  """
  @spec mutate(map(), {:put, Route.t()} | {:delete, String.t()} | :ip_rules, {atom(), term()}) ::
          map()
  def mutate(state, change, {action, route_id}) do
    table = table(state.instance)
    [{:meta, meta}] = :ets.lookup(table, :meta)
    apply_change(table, meta.gen, change)

    meta = %{
      meta
      | version: state.version,
        epoch: System.unique_integer([:positive, :monotonic]),
        ip_rules: state.ip_rules,
        count: map_size(state.routes)
    }

    true = :ets.insert(table, {:meta, meta})
    changed(state, action, route_id)
    state
  end

  defp apply_change(table, gen, {:put, %Route{id: id} = route}) do
    placed = %{route: route, segments: Matcher.compile(route)}
    key = pattern_key(gen, placed)
    previous = :ets.lookup(table, {:id, gen, id})

    # The new rows first: a reader walking now sees the old pattern, the new
    # one, or both, never neither.
    true = :ets.insert(table, [{key, route, placed.segments}, {{:id, gen, id}, route}])

    with [{_key, %Route{} = old}] <- previous,
         old_key when old_key != key <- pattern_key(gen, place(old)) do
      :ets.delete(table, old_key)
    end

    :ok
  end

  defp apply_change(table, gen, {:delete, id}) do
    case :ets.lookup(table, {:id, gen, id}) do
      [{_key, %Route{} = old}] ->
        :ets.delete(table, pattern_key(gen, place(old)))
        :ets.delete(table, {:id, gen, id})

      [] ->
        :ok
    end

    :ok
  end

  defp apply_change(_table, _gen, :ip_rules), do: :ok

  defp place(route), do: %{route: route, segments: Matcher.compile(route)}

  @doc "Delete every row of an old generation (the owner's `{:drop_generation, gen}`)."
  @spec drop_generation(atom(), pos_integer()) :: :ok
  def drop_generation(instance, gen) do
    :ets.select_delete(table(instance), [
      {{{:pattern, gen, :_, :_}, :_, :_}, [], [true]},
      {{{:id, gen, :_}, :_}, [], [true]}
    ])

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp changed(state, action, route_id) do
    Ankusa.Telemetry.emit([:routes, :changed], %{}, %{
      instance: state.instance,
      action: action,
      route_id: route_id,
      version: state.version
    })
  end

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

  # Only enabled routes collide (a disabled one captures nothing), the same rule
  # `Ankusa.Routes.create/2` applies at runtime, so a seed the API would accept
  # never fails boot because of the order its entries are listed in. Routes are
  # indexed by compiled pattern, so this is linear in the size of the seed.
  defp no_seed_conflicts(parsed) do
    parsed
    |> Enum.reduce_while(%{}, fn {route, index}, seen ->
      if route.enabled do
        compiled = Matcher.compile(route)

        case Enum.find(Map.get(seen, compiled, []), fn {methods, _first_index} ->
               Enum.any?(methods, &(&1 in route.methods))
             end) do
          nil ->
            {:cont,
             Map.update(seen, compiled, [{route.methods, index}], &[{route.methods, index} | &1])}

          {_methods, first_index} ->
            {:halt,
             {:error,
              {:seed_conflict, index, first_index, Enum.join(route.methods, "/"), route.path}}}
        end
      else
        {:cont, seen}
      end
    end)
    |> case do
      {:error, _} = err -> err
      _seen -> :ok
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
      case Net.parse_cidr(cidr) do
        {:ok, parsed} -> [parsed]
        {:error, _message} -> []
      end
    end)
  end

  # ── the table ─────────────────────────────────────────────────────────────

  @doc "The instance's snapshot table name."
  @spec table(atom()) :: atom()
  def table(instance), do: :"ankusa_routes_#{instance}"

  defp ensure_table(instance) do
    name = table(instance)

    case :ets.whereis(name) do
      :undefined ->
        :ets.new(name, [:named_table, :protected, :ordered_set, read_concurrency: true])

      _tid ->
        name
    end
  end

  defp current_gen(table) do
    case :ets.lookup(table, :meta) do
      [{:meta, %{gen: gen}}] -> gen
      [] -> nil
    end
  end

  # ── reads ─────────────────────────────────────────────────────────────────

  @doc "The current `:meta` row, or `nil` before the store has written one."
  @spec meta(atom()) :: meta() | nil
  def meta(instance) do
    case :ets.lookup(table(instance), :meta) do
      [{:meta, meta}] -> meta
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  @doc "One route of `meta`'s generation."
  @spec fetch(atom(), meta(), String.t()) :: {:ok, Route.t()} | :error
  def fetch(instance, %{gen: gen}, id) do
    case :ets.lookup(table(instance), {:id, gen, id}) do
      [{_key, route}] -> {:ok, route}
      [] -> :error
    end
  rescue
    ArgumentError -> :error
  end

  @doc "Every route of `meta`'s generation, id ascending."
  @spec routes(atom(), meta()) :: [Route.t()]
  def routes(instance, %{gen: gen}) do
    :ets.select(table(instance), [{{{:id, gen, :_}, :"$1"}, [], [:"$1"]}])
  rescue
    ArgumentError -> []
  end

  @doc "Every `{route, segments}` of `meta`'s generation, in priority order."
  @spec patterns(atom(), meta()) :: [{Route.t(), [Matcher.segment()]}]
  def patterns(instance, %{gen: gen}) do
    :ets.select(table(instance), [
      {{{:pattern, gen, :_, :_}, :"$1", :"$2"}, [], [{{:"$1", :"$2"}}]}
    ])
  rescue
    ArgumentError -> []
  end

  @doc "How many routes the current generation holds (0 with none loaded)."
  @spec count(atom()) :: non_neg_integer()
  def count(instance) do
    case meta(instance) do
      nil -> 0
      %{count: count} -> count
    end
  end

  @doc """
  The route matching `method` and `segments`: the first enabled route in
  priority order whose pattern matches and that serves the method. A path
  that matches only for other methods is `{:reject, :method}`, nothing at all
  `{:reject, :no_route}`; both are 404 to the sender. Walks the table's own
  key order and stops at the first hit.
  """
  @spec scan(atom(), meta(), String.t(), [String.t()]) ::
          {:match, String.t()} | {:reject, :no_route | :method}
  def scan(instance, %{gen: gen}, method, segments) do
    table = table(instance)
    walk(table, :ets.next(table, {:pattern, gen, {}, ""}), gen, method, segments, :no_route)
  rescue
    ArgumentError -> {:reject, :no_route}
  end

  defp walk(table, {:pattern, gen, _priority, _id} = key, gen, method, segments, acc) do
    acc =
      case :ets.lookup(table, key) do
        [{_key, route, pattern}] ->
          if Matcher.match?(pattern, segments) do
            cond do
              not route.enabled -> acc
              method in route.methods -> {:ok, route.id}
              true -> :method
            end
          else
            acc
          end

        # Deleted between `next` and `lookup`: skip it.
        [] ->
          acc
      end

    case acc do
      {:ok, id} -> {:match, id}
      acc -> walk(table, :ets.next(table, key), gen, method, segments, acc)
    end
  end

  defp walk(_table, _end_or_other_gen, _gen, _method, _segments, reason), do: {:reject, reason}

  # Ascending sort key: negative literal count is "most literals first", a
  # wildcard sorts after everything without one.
  defp priority(%{route: route, segments: segments}) do
    {literal_count, wildcard?} = Matcher.specificity(segments)
    {-literal_count, if(wildcard?, do: 1, else: 0), route.id}
  end

  defp pattern_key(gen, placed) do
    {neg_literals, wildcard, id} = priority(placed)
    {:pattern, gen, {neg_literals, wildcard}, id}
  end
end
