defmodule Ankusa.Routes.Snapshot do
  @moduledoc """
  The compiled, in-memory view of the route table: what the guard reads on
  every request, and what makes a mutation take effect everywhere at once.

  Both stores (`Ankusa.Routes.Store.ETS` and `Ankusa.Routes.Store.Redis`) build
  their snapshots here, so a definition produces the same pattern order and
  the same id lookup whichever store it lives in.

  ## Where it lives

  One named `:protected` ETS table per instance (`ordered_set`,
  `read_concurrency: true`), written by the routes store process: readers
  never take a GenServer call or a network round trip per request, and a
  mutation writes the rows it changes, not the whole table. (It used to be one
  `:persistent_term` value rebuilt and rewritten per mutation, which costs a
  global GC pass and a full copy each time.) `Ankusa.Routes.TableOwner`
  creates it and lends it to the store, and gets it back if the store dies. A
  restarted store takes the table over with `adopt/1` and resumes from the
  routes in it, rather than starting from its config again.

  Rows, all tagged with a *generation*:

    * `{:meta, %{gen, version, epoch, ip_rules, trusted_proxies, count,
      max_prefix}}` — the one row a reader starts from;
    * `{{:lit, gen, prefix, priority, id}, enabled, methods, segments}` — every
      route, enabled or not, in the *literal-prefix index*: `prefix` is the
      leading run of literal segments of its pattern (at most 8), so
      `/hooks/stripe/:id` lives under `["hooks", "stripe"]` and `/:tenant/hook`
      under `[]`. A bucket is one contiguous run of keys in priority order;
    * `{{:id, gen, id}, route}`;
    * `{{:tenant, gen, tenant_id, id}}` — the `{tenant, id}` index: one key per
      route, so a tenant's routes come back id-ascending from a walk over its
      own keys, not over every tenant's;
    * `{{:conflict, gen, pattern, method}, id}` — the `{compiled pattern,
      method}` index of the *enabled* routes (`pattern` is the segments as one
      binary, `hooks/:/*`), so "does an enabled route already capture this" is
      a lookup, not a scan.

  A reader reads `:meta` once and uses its `gen` for every other row.
  `publish/1` (boot, a Redis reload) writes a whole new generation, then flips
  `:meta` to it, and drops the old generation a second later, so a reader that
  read the old `:meta` just before the flip finishes its walk on rows that are
  still there. `mutate/3` (one route created, replaced or deleted in the ETS
  store) edits the current generation in place and writes a new `:meta`.

  Every route is a prefix-index row, enabled or not; the guard skips a disabled
  one at match time, so toggling `enabled` is a state change, not a recompile.

  ## Matching

  A request's candidates are the buckets of every prefix of its own segments
  (`[]`, `[s0]`, `[s0, s1]`, … up to `meta.max_prefix`, at most 8): a few
  short ordered walks, merged in priority order, so a miss costs the handful of
  patterns that share the request's leading literals, never every pattern in
  the table.

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

  # A bucket's prefix is at most this many segments: a pattern with a longer
  # literal run shares the bucket of its first `@max_prefix`, and a request
  # probes at most `@max_prefix + 1` buckets.
  @max_prefix 8

  # Routes written per `:ets.insert/2` while a generation is published: the
  # row list stays small and the scheduler gets to preempt between batches.
  @batch 2_000

  # What one route costs a node, in bytes: its rows in the snapshot table plus
  # the store's own copy of the definition. `bench/routes_bench.exs` measures
  # it (about 4.9 KB for a route with the default fields at 10^6 routes) and
  # fails if it passes this figure. The boot check of
  # `routes.memory_budget_bytes` multiplies by it.
  @route_bytes_estimate 5_120

  @type meta :: %{
          gen: pos_integer(),
          version: pos_integer(),
          epoch: pos_integer(),
          ip_rules: %{default: :allow | :deny, rules: [Route.ip_rule()]},
          trusted_proxies: [term()],
          count: non_neg_integer(),
          max_prefix: non_neg_integer()
        }

  @doc "What `count` routes are estimated to cost a node, in bytes."
  @spec estimated_bytes(non_neg_integer()) :: non_neg_integer()
  def estimated_bytes(count), do: count * @route_bytes_estimate

  @doc """
  Would `count` routes fit in `budget` bytes (`routes.memory_budget_bytes`)? A
  store asks before it publishes a load, so a table that does not fit is
  refused at boot instead of by the OOM killer.
  """
  @spec check_budget(non_neg_integer(), pos_integer()) ::
          :ok | {:error, {:routes_over_budget, non_neg_integer(), pos_integer()}}
  def check_budget(count, budget) do
    if estimated_bytes(count) > budget,
      do: {:error, {:routes_over_budget, count, budget}},
      else: :ok
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
  the routes store (which takes the table from `Ankusa.Routes.TableOwner`, or
  creates its own when it runs without one). The caller must handle `{:drop_generation, gen}` with
  `drop_generation/2`.
  """
  @spec publish(map()) :: :ok
  def publish(state) do
    table = ensure_table(state.instance)
    old = current_gen(table)
    gen = System.unique_integer([:positive, :monotonic])

    max_prefix =
      state.routes
      |> Stream.map(fn {_id, %Route{} = route} -> route end)
      |> Stream.chunk_every(@batch)
      |> Enum.reduce(0, fn batch, max_prefix ->
        max(max_prefix, insert_batch(table, gen, batch))
      end)

    meta = %{
      gen: gen,
      version: state.version,
      epoch: System.unique_integer([:positive, :monotonic]),
      ip_rules: state.ip_rules,
      trusted_proxies: trusted_proxies(state.instance),
      count: map_size(state.routes),
      max_prefix: max_prefix
    }

    true = :ets.insert(table, {:meta, meta})
    if old, do: Process.send_after(self(), {:drop_generation, old}, @drop_after_ms)
    :ok
  end

  # One batch of a generation's rows; the longest prefix it indexed.
  defp insert_batch(table, gen, routes) do
    {rows, max_prefix} =
      Enum.reduce(routes, {[], 0}, fn route, {rows, max_prefix} ->
        segments = Matcher.compile(route)
        prefix = prefix(segments)
        {rows(gen, route, segments, prefix) ++ rows, max(max_prefix, length(prefix))}
      end)

    true = :ets.insert(table, rows)
    max_prefix
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
    prefix_length = apply_change(table, meta.gen, change)

    meta = %{
      meta
      | version: state.version,
        epoch: System.unique_integer([:positive, :monotonic]),
        ip_rules: state.ip_rules,
        count: map_size(state.routes),
        max_prefix: max(meta.max_prefix, prefix_length)
    }

    true = :ets.insert(table, {:meta, meta})
    changed(state, action, route_id)
    state
  end

  # The length of the prefix the change indexed (0 for one that indexed none).
  defp apply_change(table, gen, {:put, %Route{id: id} = route}) do
    segments = Matcher.compile(route)
    prefix = prefix(segments)
    previous = :ets.lookup(table, {:id, gen, id})

    # The new rows first: a reader walking now sees the old pattern, the new
    # one, or both, never neither.
    true = :ets.insert(table, rows(gen, route, segments, prefix))

    with [{_key, %Route{} = old}] <- previous do
      old_segments = Matcher.compile(old)

      stale =
        keys(gen, old, old_segments, prefix(old_segments)) -- keys(gen, route, segments, prefix)

      Enum.each(stale, &delete_key(table, &1, id))
    end

    length(prefix)
  end

  defp apply_change(table, gen, {:delete, id}) do
    with [{_key, %Route{} = old}] <- :ets.lookup(table, {:id, gen, id}) do
      segments = Matcher.compile(old)
      Enum.each(keys(gen, old, segments, prefix(segments)), &delete_key(table, &1, id))
    end

    0
  end

  defp apply_change(_table, _gen, :ip_rules), do: 0

  # Every row of one route. The `:lit` row carries what a scan needs
  # (`enabled`, `methods`, the compiled pattern) rather than the whole route,
  # so a route costs one copy of its definition (the `:id` row), not two.
  defp rows(gen, %Route{} = route, segments, prefix) do
    [
      {lit_key(gen, route, segments, prefix), route.enabled, route.methods, segments},
      {{:id, gen, route.id}, route},
      {{:tenant, gen, route.tenant_id, route.id}}
      | Enum.map(conflict_keys(gen, route, segments), &{&1, route.id})
    ]
  end

  defp keys(gen, %Route{} = route, segments, prefix) do
    [
      lit_key(gen, route, segments, prefix),
      {:id, gen, route.id},
      {:tenant, gen, route.tenant_id, route.id}
      | conflict_keys(gen, route, segments)
    ]
  end

  # Ascending sort key inside a bucket: negative literal count is "most
  # literals first", a wildcard sorts after everything without one, then the id.
  defp lit_key(gen, %Route{id: id}, segments, prefix) do
    {literal_count, wildcard?} = Matcher.specificity(segments)
    {:lit, gen, prefix, {-literal_count, if(wildcard?, do: 1, else: 0)}, id}
  end

  # Only an enabled route captures anything, so only it is in the conflict index.
  defp conflict_keys(gen, %Route{enabled: true, methods: methods}, segments) do
    pattern = pattern_key(segments)
    Enum.map(methods, &{:conflict, gen, pattern, &1})
  end

  defp conflict_keys(_gen, %Route{}, _segments), do: []

  # The compiled pattern as one small binary: a literal never holds `/`, `:` or
  # `*` (`Matcher.segments/1`), so the join names one pattern and no other, and
  # the key costs a few bytes instead of a copy of the segment list.
  defp pattern_key(segments) do
    Enum.map_join(segments, "/", fn
      {:literal, literal} -> literal
      :param -> ":"
      :wildcard -> "*"
    end)
  end

  # A conflict key only goes with the route that holds it: a table that was
  # loaded with two enabled routes on one pattern (nothing but a hand-edited
  # Redis can) keeps the holder's entry when the other one is deleted.
  defp delete_key(table, {:conflict, _gen, _segments, _method} = key, id) do
    case :ets.lookup(table, key) do
      [{_key, ^id}] -> :ets.delete(table, key)
      _other -> true
    end
  end

  defp delete_key(table, key, _id), do: :ets.delete(table, key)

  # The leading run of literal segments, as strings, capped at `@max_prefix`.
  defp prefix(segments) do
    segments
    |> Enum.take_while(&match?({:literal, _literal}, &1))
    |> Enum.take(@max_prefix)
    |> Enum.map(fn {:literal, literal} -> literal end)
  end

  @doc "Delete every row of an old generation (the owner's `{:drop_generation, gen}`)."
  @spec drop_generation(atom(), pos_integer()) :: :ok
  def drop_generation(instance, gen) do
    :ets.select_delete(table(instance), [
      {{{:lit, gen, :_, :_, :_}, :_, :_, :_}, [], [true]},
      {{{:id, gen, :_}, :_}, [], [true]},
      {{{:tenant, gen, :_, :_}}, [], [true]},
      {{{:conflict, gen, :_, :_}, :_}, [], [true]}
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
  # The instance name is operator config: one atom per instance, never request input.
  # sobelow_skip ["DOS.BinToAtom"]
  def table(instance), do: :"ankusa_routes_#{instance}"

  # The instance's `Ankusa.Routes.TableOwner` creates the table and lends it to
  # the store, so a store restart finds the last published routes still there.
  # A store running without one (on its own, in tests) creates its own.
  defp ensure_table(instance) do
    name = table(instance)

    case :ets.whereis(name) do
      :undefined ->
        :ets.new(name, [:named_table, :protected, :ordered_set, read_concurrency: true])

      _tid ->
        if :ets.info(name, :owner) != self() do
          :ok = Ankusa.Routes.TableOwner.hand_over(instance, self())

          # `give_away` announces itself with a message the owner sent before
          # its reply, so it is already here: take it, so no store has to.
          receive do
            {:"ETS-TRANSFER", _tid, _from, :routes} -> :ok
          after
            0 -> :ok
          end
        end

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

  @doc """
  Take over the table a previous routes store left behind (it survives the
  store through `Ankusa.Routes.TableOwner`): the caller becomes its owner, any
  generation other than the current one is dropped, and the current one comes
  back as a store's state fields. `:none` when no table has been published yet
  (an instance's first boot).
  """
  @spec adopt(atom()) ::
          {:ok, %{routes: %{String.t() => Route.t()}, ip_rules: map(), version: pos_integer()}}
          | :none
  def adopt(instance) do
    case meta(instance) do
      nil ->
        :none

      %{gen: gen} = meta ->
        table = ensure_table(instance)

        # The old owner's `{:drop_generation, old}` timer died with it.
        :ets.select_delete(table, [
          {{{:lit, :"$1", :_, :_, :_}, :_, :_, :_}, [{:"/=", :"$1", gen}], [true]},
          {{{:id, :"$1", :_}, :_}, [{:"/=", :"$1", gen}], [true]},
          {{{:tenant, :"$1", :_, :_}}, [{:"/=", :"$1", gen}], [true]},
          {{{:conflict, :"$1", :_, :_}, :_}, [{:"/=", :"$1", gen}], [true]}
        ])

        {:ok,
         %{
           routes: Map.new(routes(instance, meta), &{&1.id, &1}),
           ip_rules: meta.ip_rules,
           version: meta.version
         }}
    end
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

  @doc """
  The routes `tenant` owns in `meta`'s generation, id ascending, as a lazy
  stream that starts after `after_id` (`nil` for the first). It walks the
  `{tenant, id}` index, so its cost is the page read, not the table.
  """
  @spec tenant_routes(atom(), meta(), String.t(), String.t() | nil) :: Enumerable.t()
  def tenant_routes(instance, %{gen: gen}, tenant, after_id) do
    table = table(instance)

    Stream.unfold({:tenant, gen, tenant, after_id || ""}, fn key ->
      case next_key(table, key) do
        {:tenant, ^gen, ^tenant, id} = next ->
          # A route deleted between `next` and `lookup` is skipped by the `nil`.
          route =
            case :ets.lookup(table, {:id, gen, id}) do
              [{_key, %Route{} = route}] -> route
              [] -> nil
            end

          {route, next}

        _end_of_tenant ->
          nil
      end
    end)
    |> Stream.reject(&is_nil/1)
  end

  @doc """
  Every route of `meta`'s generation, id ascending, as a lazy stream that
  starts after `after_id` (`nil` for the first). It walks the `:id` rows from
  the cursor, so its cost is the page read, not the table.
  """
  @spec routes_after(atom(), meta(), String.t() | nil) :: Enumerable.t()
  def routes_after(instance, %{gen: gen}, after_id) do
    table = table(instance)

    Stream.unfold({:id, gen, after_id || ""}, fn key ->
      case next_key(table, key) do
        {:id, ^gen, id} = next ->
          # A route deleted between `next` and `lookup` is skipped by the `nil`.
          route =
            case :ets.lookup(table, {:id, gen, id}) do
              [{_key, %Route{} = route}] -> route
              [] -> nil
            end

          {route, next}

        _end_of_ids ->
          nil
      end
    end)
    |> Stream.reject(&is_nil/1)
  end

  # A table that is gone (the instance is stopping) ends the walk.
  defp next_key(table, key) do
    :ets.next(table, key)
  rescue
    ArgumentError -> :"$end_of_table"
  end

  @doc """
  The enabled route that already captures `route`'s request class — the same
  compiled pattern and at least one method in common, other than `route`
  itself — or `:none`. One lookup per method of `route`, whatever the table
  holds.
  """
  @spec conflict(atom(), meta(), Route.t()) :: {:ok, Route.t()} | :none
  def conflict(instance, %{gen: gen} = meta, %Route{id: id, methods: methods} = route) do
    table = table(instance)
    pattern = pattern_key(Matcher.compile(route))

    Enum.find_value(methods, :none, fn method ->
      with [{_key, holder_id}] when holder_id != id <-
             :ets.lookup(table, {:conflict, gen, pattern, method}),
           {:ok, holder} <- fetch(instance, meta, holder_id) do
        {:ok, holder}
      else
        _no_holder -> nil
      end
    end)
  rescue
    ArgumentError -> :none
  end

  @doc """
  Every other route whose pattern overlaps `route`'s — some request path both
  capture (`Ankusa.Routes.Matcher.overlap?/2`) — whatever its methods, whether
  it is enabled, and whichever tenant owns it (each returned route carries its
  `tenant_id`, so "does another tenant already hold part of this path space"
  is a filter over the answer). `limit` stops the walk after that many.

  It reads the prefix index along one chain: the buckets of every proper prefix
  of the route's own literal prefix, and the contiguous run of buckets at or
  under it (list order keeps everything that extends a prefix next to it), so
  the cost is a seek per level plus the routes sharing the route's leading
  literals, not the table. A pattern that starts with a parameter or wildcard
  has no literal prefix and can overlap anything, so for it the run is the
  whole index.
  """
  @spec overlaps(atom(), meta(), Route.t(), pos_integer() | :infinity) :: [Route.t()]
  def overlaps(instance, %{gen: gen} = meta, %Route{id: id} = route, limit \\ :infinity) do
    table = table(instance)
    segments = Matcher.compile(route)
    prefix = prefix(segments)
    depth = length(prefix)

    found =
      0..depth
      |> Stream.flat_map(fn level ->
        lit_keys(table, gen, Enum.take(prefix, level), level == depth)
      end)
      |> Stream.filter(fn key ->
        elem(key, 4) != id and overlapping?(table, key, segments)
      end)
      |> Stream.flat_map(fn key ->
        case fetch(instance, meta, elem(key, 4)) do
          {:ok, other} -> [other]
          :error -> []
        end
      end)

    if limit == :infinity, do: Enum.to_list(found), else: Enum.take(found, limit)
  rescue
    ArgumentError -> []
  end

  defp overlapping?(table, key, segments) do
    case :ets.lookup(table, key) do
      [{_key, _enabled, _methods, pattern}] -> Matcher.overlap?(segments, pattern)
      [] -> false
    end
  end

  # The `:lit` keys of the bucket `prefix`, and of every bucket under it when
  # `extending?`, in key order.
  defp lit_keys(table, gen, prefix, extending?) do
    Stream.unfold({:lit, gen, prefix, {}, ""}, fn key ->
      case next_key(table, key) do
        {:lit, ^gen, bucket, _priority, _id} = next ->
          if bucket == prefix or (extending? and :lists.prefix(prefix, bucket)),
            do: {next, next},
            else: nil

        _end_of_run ->
          nil
      end
    end)
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
  `{:reject, :no_route}`; both are 404 to the sender. Walks the buckets of the
  request's own literal prefixes, merged in priority order, and stops at the
  first hit.
  """
  @spec scan(atom(), meta(), String.t(), [String.t()]) ::
          {:match, String.t()} | {:reject, :no_route | :method}
  def scan(instance, %{gen: gen, max_prefix: max_prefix}, method, segments) do
    table = table(instance)

    cursors =
      segments
      |> probe_prefixes(max_prefix, [], [[]])
      |> Enum.flat_map(&cursor(table, gen, &1, {:lit, gen, &1, {}, ""}))

    merge(table, gen, cursors, method, segments, :no_route)
  rescue
    ArgumentError -> {:reject, :no_route}
  end

  # `[]`, `[s0]`, `[s0, s1]`, … up to `max` segments or the request's own end.
  defp probe_prefixes(_segments, 0, _reversed, acc), do: acc
  defp probe_prefixes([], _max, _reversed, acc), do: acc

  defp probe_prefixes([segment | rest], max, reversed, acc) do
    reversed = [segment | reversed]
    probe_prefixes(rest, max - 1, reversed, [Enum.reverse(reversed) | acc])
  end

  # The first row of `prefix`'s bucket after `from`, as a merge cursor, or none.
  defp cursor(table, gen, prefix, from) do
    case :ets.next(table, from) do
      {:lit, ^gen, ^prefix, priority, id} = key -> [{priority, id, key, prefix}]
      _end_of_bucket -> []
    end
  end

  defp merge(_table, _gen, [], _method, _segments, reason), do: {:reject, reason}

  defp merge(table, gen, cursors, method, segments, acc) do
    {{_priority, id, key, prefix}, others} = take_min(cursors)

    acc =
      case :ets.lookup(table, key) do
        [{_key, enabled, methods, pattern}] ->
          if Matcher.match?(pattern, segments) do
            cond do
              not enabled -> acc
              method in methods -> {:ok, id}
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
      acc -> merge(table, gen, cursor(table, gen, prefix, key) ++ others, method, segments, acc)
    end
  end

  # The cursor whose next row sorts first, and the others (at most
  # `@max_prefix` of them).
  defp take_min([first | rest]), do: take_min(rest, first, [])
  defp take_min([], min, others), do: {min, others}

  defp take_min([cursor | rest], min, others) do
    if before?(cursor, min),
      do: take_min(rest, cursor, [min | others]),
      else: take_min(rest, min, [cursor | others])
  end

  defp before?({priority, id, _key, _prefix}, {other_priority, other_id, _other, _prefix2}),
    do: priority < other_priority or (priority == other_priority and id < other_id)
end
