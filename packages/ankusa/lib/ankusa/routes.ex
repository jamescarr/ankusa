defmodule Ankusa.Routes do
  @moduledoc """
  Route management: the allowlist guard's decisions, the CRUD over route
  definitions, and the boot validation for `config.routes`.

  This is the only module the guard and the admin API call. It owns the rules
  the stores can't know about — whether two enabled routes collide, which reason
  a rejection carries, what the dry run reports — and leaves the stores to hold
  definitions and the cache to remember decisions.

  ## Off by default, deny by default

  `enabled?: false` is the historical behaviour: every `POST` is captured. With
  routes on, a request is captured only if it passes the IP rules and matches an
  enabled route; everything else is rejected **before** the WAL is touched, so a
  rejected request never becomes a record, a dispatch, or a delivery.

  ## Decision order

    1. **Global IP rules.** Cheap bitmask scans, no route lookup, so a denied
       sender costs nothing else.
    2. **Route matching**, via `Ankusa.Routes.Cache` (keyed by the snapshot
       version) and, on a miss, the compiled pattern list.
    3. **The route's own IP rules**, if it declares any: they *replace* the
       global list for that route, and deny when none of them matches. A global
       deny has already won by then.

  A route declares rules so a provider can be pinned to its published ranges
  while a global ban list still applies to everything — see
  `docs/configuration.md`.
  """

  alias Ankusa.Net
  alias Ankusa.Routes.{Cache, Matcher, Route, Store}

  @type decision :: {:ok, String.t()} | {:reject, :no_route | :method | :ip_denied}

  @type reason :: :matched | :no_route | :method | :ip_denied

  @default_limit 100
  @max_limit 200

  @doc "Are routes enforced on this instance?"
  @spec enabled?(atom()) :: boolean()
  def enabled?(instance), do: Ankusa.config(instance).routes.enabled

  @doc """
  The compiled route table, or `nil` when no store has published one.

  The guard treats `nil` as a rejection: routes are enabled, so a table that
  isn't loaded means "nothing is allowed", not "everything is".
  """
  @spec snapshot(atom()) :: map() | nil
  def snapshot(instance), do: Store.snapshot(instance)

  @doc """
  Decide a request against the IP rules and the route table; `segments` are
  already-normalized path segments (`Ankusa.Routes.Matcher.normalize/2`).
  """
  @spec authorize(atom(), String.t(), [String.t()], Net.ip()) :: decision()
  def authorize(instance, method, segments, ip) do
    %{decision: decision} = evaluate(instance, method, segments, ip)
    decision
  end

  @doc """
  `authorize/4` for a raw request, normalizing the path first and reporting
  whether route matching came from the cache — how the guard makes a cache hit
  observable in telemetry.
  """
  @spec authorize_path(atom(), String.t(), [String.t()], String.t(), Net.ip()) ::
          {decision(), boolean()}
  def authorize_path(instance, method, path_info, request_path, ip) do
    case Matcher.normalize(path_info, request_path) do
      :error ->
        {{:reject, :no_route}, false}

      {:ok, segments} ->
        %{decision: decision, cached: cached} = evaluate(instance, method, segments, ip)
        {decision, cached}
    end
  end

  @doc """
  Explain what would happen to a request, without touching the decision cache
  and without capturing anything: the answer to "why was my webhook rejected".

  The request is `%{"method" =>, "path" =>, "ip" =>}`. The result names the
  decision, the reason, the route that matched (if any), and the IP rule that
  produced an IP decision, with the scope it came from.
  """
  @spec dry_run(atom(), map()) ::
          {:ok, map()} | {:error, {:invalid, String.t(), String.t()}}
  def dry_run(instance, request) when is_map(request) do
    with {:ok, method} <- method(request),
         {:ok, segments} <- request_path(request),
         {:ok, ip} <- request_ip(request) do
      do_dry_run(instance, method, segments, ip)
    end
  end

  def dry_run(_instance, _request), do: {:error, {:invalid, "request", "must be a JSON object"}}

  # ── listing and CRUD ────────────────────────────────────────────────────────

  @doc """
  List routes, id ascending, with cursor pagination.

  `cursor` is the last id of the previous page (ids are opaque slugs, so
  "greater than" is a total order over them). `limit` is clamped to `1..200` and
  `next_cursor` is only set when there is another page.
  """
  @spec list(atom(), keyword()) ::
          {:ok, %{routes: [Route.t()], next_cursor: String.t() | nil}}
  def list(instance, opts \\ []) do
    enabled = Keyword.get(opts, :enabled)
    cursor = Keyword.get(opts, :cursor)
    limit = clamp_limit(Keyword.get(opts, :limit, @default_limit))

    routes =
      instance
      |> snapshot()
      |> routes_after(cursor)
      |> filter_enabled(enabled)
      |> Enum.sort_by(& &1.id)

    page = Enum.take(routes, limit)

    next_cursor =
      if length(routes) > limit do
        case List.last(page) do
          nil -> nil
          last -> last.id
        end
      end

    {:ok, %{routes: page, next_cursor: next_cursor}}
  end

  @doc "Fetch one route definition."
  @spec get(atom(), String.t()) :: {:ok, Route.t()} | {:error, :not_found}
  def get(instance, id), do: fetch(instance, id)

  @doc """
  Create a route.

  The id must be free, and an enabled route must not share a path with another
  enabled route serving one of the same methods — the two would both match, and
  "which one wins" would be an implementation detail. Disabled routes are free to
  collide: they capture nothing.
  """
  @spec create(atom(), term()) ::
          {:ok, Route.t()}
          | {:error, {:invalid, String.t(), String.t()}}
          | {:error, {:conflict, String.t()}}
          | {:error, :too_many_routes | :store_unavailable}
  def create(instance, attrs) do
    with {:ok, route} <- Route.from_attrs(attrs),
         snapshot = snapshot(instance),
         :ok <- unique_id(snapshot, route),
         :ok <- no_conflict(snapshot, route) do
      put(instance, route, Store.insert(instance, route))
    end
  end

  @doc """
  Replace a route wholesale, addressed by `id` (`PUT` semantics, idempotent).

  A `PUT` for an id that does not exist yet creates it, so the cap applies; one
  for an existing id keeps its `inserted_at`.
  """
  @spec replace(atom(), String.t(), term()) ::
          {:ok, Route.t()}
          | {:error, {:invalid, String.t(), String.t()}}
          | {:error, {:conflict, String.t()}}
          | {:error, :too_many_routes | :store_unavailable}
  def replace(instance, id, attrs) do
    with {:ok, route} <- Route.from_attrs(attrs, id: id),
         snapshot = snapshot(instance),
         :ok <- no_conflict(snapshot, route) do
      case Map.fetch(snapshot.by_id, id) do
        {:ok, existing} ->
          # The stored route is what gets returned: `inserted_at` is the one the
          # route already had, and the caller has to be told that, not the
          # timestamp this call happened to mint.
          stored = %{route | inserted_at: existing.inserted_at}
          put(instance, stored, Store.replace(instance, stored))

        :error ->
          put(instance, route, Store.insert(instance, route))
      end
    end
  end

  @doc """
  Partial update: `enabled`, `methods`, `ip_rules`, and `metadata` only.

  `path` and `id` are immutable here — moving a route changes which requests it
  captures, which is a `PUT` (a full, reviewable definition), not a patch.
  """
  @spec update(atom(), String.t(), term()) ::
          {:ok, Route.t()}
          | {:error, {:invalid, String.t(), String.t()}}
          | {:error, {:conflict, String.t()}}
          | {:error, :not_found | :too_many_routes | :store_unavailable}
  def update(instance, id, patch) do
    with {:ok, existing} <- fetch(instance, id),
         {:ok, patch} <- patch_attrs(patch),
         attrs = Map.merge(attrs_of(existing), patch),
         {:ok, route} <- Route.from_attrs(attrs, id: id),
         route = %{route | inserted_at: existing.inserted_at},
         snapshot = snapshot(instance),
         :ok <- no_conflict(snapshot, route) do
      put(instance, route, Store.replace(instance, route))
    end
  end

  @doc "Delete a route definition."
  @spec delete(atom(), String.t()) :: :ok | {:error, :not_found | :store_unavailable}
  def delete(instance, id), do: Store.delete(instance, id)

  @doc "The global IP rules, parsed."
  @spec ip_rules(atom()) :: %{default: :allow | :deny, rules: [Route.ip_rule()]}
  def ip_rules(instance), do: snapshot(instance).ip_rules

  @doc """
  Replace the global IP rules.

  The list is ordered, first match wins, and `default` decides the case where
  nothing matched — the "obvious" `default: :allow` is only safe when the
  surrounding network is already trusted.
  """
  @spec put_ip_rules(atom(), term()) ::
          {:ok, %{default: :allow | :deny, rules: [Route.ip_rule()]}}
          | {:error, {:invalid, String.t(), String.t()}}
          | {:error, :store_unavailable}
  def put_ip_rules(instance, attrs) do
    with {:ok, attrs} <- ip_rules_attrs(attrs),
         {:ok, rules} <- parse_rules(attrs["rules"] || []),
         {:ok, default} <- default_action(attrs["default"]) do
      ip_rules = %{default: default, rules: rules}

      case Store.put_ip_rules(instance, ip_rules) do
        :ok -> {:ok, ip_rules}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  # ── boot validation ─────────────────────────────────────────────────────────

  @doc """
  Validate `config.routes` at boot, raising `ArgumentError` — the same contract
  as `Ankusa.ClaimCheck.validate_config!/1`, and for the same reason: a
  misconfigured guard rejects real traffic, so it must not boot quietly.

  Configuration that is only meaningful when routes are on (the seed) is only
  checked when `enabled: true`; the rest is always checked, so a typo in a
  config that is about to be switched on is caught by the same run that
  loads it.
  """
  @spec validate_config!(Ankusa.Config.t()) :: :ok
  def validate_config!(%Ankusa.Config{routes: routes}) do
    validate_limits!(routes)
    validate_cache!(routes.cache)
    validate_store!(routes.store)
    validate_proxies!(routes.trusted_proxies)
    validate_global_rules!(routes.ip_rules)
    validate_admin_port!(routes.admin.port)

    if routes.enabled do
      validate_seed!(routes)
    end

    :ok
  end

  # ── decision internals ──────────────────────────────────────────────────────

  # The single implementation: `authorize/4`, `authorize_path/5`, and the dry run
  # all come through here, so they cannot drift apart. It returns the decision,
  # whether route matching was served from the cache, the IP rule that decided an
  # IP question (the dry run's "which rule" answer), and the matched route's id
  # even when its own rules rejected the sender — that is the case an operator
  # most needs named.
  defp evaluate(instance, method, segments, ip) do
    case snapshot(instance) do
      # No route table loaded: routes are on with nothing to match, so nothing
      # is allowed. The guard logs a warning and rejects the same way.
      nil ->
        %{decision: {:reject, :no_route}, cached: false, ip_rule: nil, route_id: nil}

      table ->
        case first_rule(table.ip_rules, ip) do
          {:deny, rule} ->
            # Global deny wins outright, and it is decided before any route
            # lookup: a blocked sender costs one bitmask scan.
            %{
              decision: {:reject, :ip_denied},
              cached: false,
              ip_rule: ip_rule(rule, "global"),
              route_id: nil
            }

          {:allow, _rule} ->
            cache_config = Ankusa.config(instance).routes.cache
            {match, cached} = route_match(instance, table, cache_config, method, segments)

            case match do
              {:reject, reason} ->
                %{decision: {:reject, reason}, cached: cached, ip_rule: nil, route_id: nil}

              {:match, id} ->
                route_ip(Map.fetch!(table.by_id, id), id, cached, ip)
            end
        end
    end
  end

  # A route's own rules *replace* the global allow list for that route (a global
  # deny already won above), so a route that declares rules and matches none of
  # them denies: the point of pinning a provider's ranges is that anything else
  # is refused.
  defp route_ip(route, id, cached, ip) do
    case route_rule(route, ip) do
      {:allow, rule} ->
        %{decision: {:ok, id}, cached: cached, ip_rule: ip_rule(rule, "route"), route_id: id}

      {:deny, rule} ->
        %{
          decision: {:reject, :ip_denied},
          cached: cached,
          ip_rule: ip_rule(rule, "route"),
          route_id: id
        }
    end
  end

  defp route_rule(%Route{ip_rules: []}, _ip), do: {:allow, nil}

  defp route_rule(%Route{ip_rules: rules}, ip) do
    case Enum.find(rules, &Ankusa.Net.CIDR.contains?(&1.cidr, ip)) do
      nil -> {:deny, nil}
      rule -> {rule.action, rule}
    end
  end

  # Route matching only — never IP. The cache key is
  # `{version, method, segments}`, so a mutation makes every entry unreachable
  # instead of having to delete them.
  defp route_match(instance, table, cache_config, method, segments) do
    case Cache.lookup(instance, table.version, method, segments) do
      {:ok, decision} ->
        {decision, true}

      :error ->
        decision = scan(table.patterns, method, segments)

        Cache.store(
          instance,
          table.version,
          method,
          segments,
          decision,
          ttl(cache_config, decision)
        )

        {decision, false}
    end
  end

  # The pattern list is a priority list (see Ankusa.Routes.Snapshot), so the
  # first enabled route matching both method and path wins. A path match for
  # another method is remembered only to report `:method` instead of `:no_route`;
  # both are 404 to the sender, so nothing is confirmed either way.
  defp scan(patterns, method, segments) do
    patterns
    |> Enum.reduce_while(:no_route, fn %{route: route, segments: pattern}, acc ->
      if Matcher.match?(pattern, segments) do
        cond do
          not route.enabled -> {:cont, acc}
          method in route.methods -> {:halt, {:ok, route.id}}
          true -> {:cont, :method}
        end
      else
        {:cont, acc}
      end
    end)
    |> case do
      {:ok, id} -> {:match, id}
      reason -> {:reject, reason}
    end
  end

  defp ttl(%{ttl_ms: ttl_ms}, {:match, _id}), do: ttl_ms
  defp ttl(%{negative_ttl_ms: ttl_ms}, {:reject, _reason}), do: ttl_ms

  defp first_rule(%{default: default, rules: rules}, ip) do
    case Enum.find(rules, &Ankusa.Net.CIDR.contains?(&1.cidr, ip)) do
      nil -> {default, nil}
      rule -> {rule.action, rule}
    end
  end

  # Only a rule that actually matched is reported: a `default` decision has no
  # rule behind it, and inventing one would mislead the dry run.
  defp ip_rule(nil, _scope), do: nil

  defp ip_rule(rule, scope) do
    %{action: rule.action, cidr: Ankusa.Net.CIDR.to_string(rule.cidr), scope: scope}
  end

  defp do_dry_run(instance, method, segments, ip) do
    %{decision: decision, ip_rule: ip_rule, route_id: route_id} =
      evaluate(instance, method, segments, ip)

    {:ok,
     %{
       decision: if(match?(decision), do: :allow, else: :deny),
       reason: reason(decision),
       route_id: route_id,
       ip_rule: ip_rule
     }}
  end

  defp match?({:ok, _id}), do: true
  defp match?({:reject, _reason}), do: false

  defp reason({:ok, _id}), do: :matched
  defp reason({:reject, reason}), do: reason

  # ── request parsing for the dry run ─────────────────────────────────────────

  defp method(%{"method" => method}) when is_binary(method), do: {:ok, method}

  defp method(_request),
    do: {:error, {:invalid, "method", "is required and must be a string"}}

  defp request_path(%{"path" => path}) when is_binary(path) do
    case Matcher.normalize(String.split(path, "/", trim: true), path) do
      {:ok, segments} -> {:ok, segments}
      :error -> {:error, {:invalid, "path", "is not a matchable request path"}}
    end
  end

  defp request_path(_request),
    do: {:error, {:invalid, "path", "is required and must be a string"}}

  defp request_ip(%{"ip" => ip}) do
    case Net.parse(ip) do
      {:ok, parsed} -> {:ok, parsed}
      :error -> {:error, {:invalid, "ip", "must be an IPv4 or IPv6 address"}}
    end
  end

  defp request_ip(_request),
    do: {:error, {:invalid, "ip", "is required and must be a string"}}

  # ── CRUD internals ──────────────────────────────────────────────────────────

  # A route is only ever reported as persisted once the store said so; on a store
  # error the caller gets the error tuple, not a definition that isn't there.
  defp put(_instance, route, :ok), do: {:ok, route}
  defp put(_instance, _route, {:error, reason}), do: {:error, reason}

  defp fetch(instance, id) do
    case Store.get(instance, id) do
      {:ok, route} -> {:ok, route}
      :error -> {:error, :not_found}
    end
  end

  defp routes_after(nil, _cursor), do: []

  defp routes_after(snapshot, cursor) do
    routes = Map.values(snapshot.by_id)

    case cursor do
      nil -> routes
      cursor -> Enum.filter(routes, &(&1.id > cursor))
    end
  end

  defp filter_enabled(routes, nil), do: routes
  defp filter_enabled(routes, enabled), do: Enum.filter(routes, &(&1.enabled == enabled))

  defp clamp_limit(limit) when is_integer(limit), do: limit |> max(1) |> min(@max_limit)
  defp clamp_limit(_limit), do: @default_limit

  defp unique_id(snapshot, route) do
    if Map.has_key?(snapshot.by_id, route.id), do: {:error, {:conflict, route.id}}, else: :ok
  end

  defp no_conflict(snapshot, route) do
    if route.enabled do
      case Enum.find(Map.values(snapshot.by_id), &collides?(&1, route)) do
        nil -> :ok
        other -> {:error, {:conflict, other.id}}
      end
    else
      :ok
    end
  end

  # Two *enabled* routes collide when they capture the same request class: the
  # same normalized pattern and at least one method in common. Disabled routes
  # are exempt — they capture nothing, so a staging definition may sit next to a
  # live one.
  defp collides?(%Route{} = other, %Route{} = route) do
    other.id != route.id and other.enabled and
      Matcher.segments(other.path) == Matcher.segments(route.path) and
      Enum.any?(other.methods, &(&1 in route.methods))
  end

  defp attrs_of(%Route{} = route),
    do: Map.drop(Route.to_json(route), ["inserted_at", "updated_at"])

  defp patch_attrs(patch) when is_list(patch) do
    if Keyword.keyword?(patch) do
      {:ok, Map.new(patch, fn {key, value} -> {to_string(key), value} end)}
    else
      {:error, {:invalid, "route", "must be a map or keyword list"}}
    end
  end

  defp patch_attrs(patch) when is_map(patch) do
    patch = Map.new(patch, fn {key, value} -> {to_string(key), value} end)

    # Anything else in the patch — a typo, a forged `inserted_at` — is caught by
    # `Route.from_attrs/2` on the merged attributes.
    case Enum.find(Map.keys(patch), &(&1 in ["id", "path"])) do
      nil -> {:ok, patch}
      key -> {:error, {:invalid, key, "immutable; use PUT"}}
    end
  end

  defp patch_attrs(_patch), do: {:error, {:invalid, "route", "must be a map or keyword list"}}

  defp ip_rules_attrs(attrs) when is_map(attrs) or is_list(attrs) do
    if is_list(attrs) and not Keyword.keyword?(attrs) do
      {:error, {:invalid, "ip_rules", "must be a map"}}
    else
      {:ok, Map.new(attrs, fn {key, value} -> {to_string(key), value} end)}
    end
  end

  defp ip_rules_attrs(_attrs), do: {:error, {:invalid, "ip_rules", "must be a map"}}

  defp parse_rules(rules) do
    case Route.parse_rules(rules) do
      {:ok, parsed} -> {:ok, parsed}
      {:error, message} -> {:error, {:invalid, "rules", message}}
    end
  end

  defp default_action(nil), do: {:ok, :allow}
  defp default_action(action) when action in [:allow, "allow"], do: {:ok, :allow}
  defp default_action(action) when action in [:deny, "deny"], do: {:ok, :deny}

  defp default_action(action),
    do: {:error, {:invalid, "default", "must be \"allow\" or \"deny\", got #{inspect(action)}"}}

  # ── boot validation internals ───────────────────────────────────────────────

  defp validate_limits!(routes) do
    unless is_integer(routes.max_routes) and routes.max_routes >= 1 do
      raise ArgumentError,
            "routes.max_routes must be a positive integer, got #{inspect(routes.max_routes)}"
    end

    unless is_integer(routes.log_sample) and routes.log_sample >= 0 do
      raise ArgumentError,
            "routes.log_sample must be a non-negative integer, got #{inspect(routes.log_sample)}"
    end

    unless routes.ip_denied_status in [403, 404] do
      raise ArgumentError,
            "routes.ip_denied_status must be 403 or 404, got #{inspect(routes.ip_denied_status)}"
    end
  end

  defp validate_cache!(cache) do
    unless is_map(cache) do
      raise ArgumentError, "routes.cache must be a map, got #{inspect(cache)}"
    end

    Enum.each([:max_size, :ttl_ms, :negative_ttl_ms, :gc_interval_ms], fn key ->
      value = Map.get(cache, key)

      unless is_integer(value) and value > 0 do
        raise ArgumentError,
              "routes.cache.#{key} must be a positive integer, got #{inspect(value)}"
      end
    end)

    if cache.ttl_ms >= cache.gc_interval_ms do
      raise ArgumentError,
            "routes.cache.ttl_ms (#{cache.ttl_ms}) must be under routes.cache.gc_interval_ms " <>
              "(#{cache.gc_interval_ms}): the local cache's garbage collector can drop a " <>
              "generation before an entry's own TTL is up"
    end
  end

  defp validate_store!({module, opts}) when is_atom(module) and (is_list(opts) or is_map(opts)),
    do: :ok

  defp validate_store!(other) do
    raise ArgumentError, "routes.store must be a {module, opts} pair, got #{inspect(other)}"
  end

  defp validate_proxies!(proxies) do
    unless is_list(proxies) do
      raise ArgumentError,
            "routes.trusted_proxies must be a list of CIDRs, got #{inspect(proxies)}"
    end

    Enum.each(proxies, fn cidr ->
      case Ankusa.Net.CIDR.parse(cidr) do
        {:ok, _parsed} -> :ok
        {:error, :invalid_cidr} -> raise ArgumentError, not_a_cidr("trusted_proxies", cidr)
      end
    end)
  end

  defp validate_global_rules!(%{default: default, rules: rules}) do
    unless default in [:allow, :deny] do
      raise ArgumentError,
            "routes.ip_rules.default must be :allow or :deny, got #{inspect(default)}"
    end

    case Route.parse_rules(rules) do
      {:ok, _parsed} ->
        :ok

      {:error, message} ->
        raise ArgumentError, "routes.ip_rules.rules are invalid: #{message}"
    end
  end

  defp validate_admin_port!(port) do
    unless is_integer(port) and port >= 0 and port <= 65_535 do
      raise ArgumentError, "routes.admin.port must be a TCP port, got #{inspect(port)}"
    end
  end

  defp validate_seed!(%{seed: seed, max_routes: max_routes}) do
    unless is_list(seed) do
      raise ArgumentError, "routes.seed must be a list of routes, got #{inspect(seed)}"
    end

    if length(seed) > max_routes do
      raise ArgumentError,
            "routes.seed defines #{length(seed)} routes, more than routes.max_routes " <>
              "(#{max_routes})"
    end

    parsed =
      seed
      |> Enum.with_index()
      |> Enum.map(fn {attrs, index} ->
        case Route.from_attrs(attrs) do
          {:ok, route} -> {route, index}
          {:error, {:invalid, field, message}} -> raise invalid_seed(index, field, message)
        end
      end)

    parsed
    |> Enum.group_by(fn {route, _index} -> route.id end)
    |> Enum.find(fn {_id, group} -> length(group) > 1 end)
    |> case do
      nil ->
        :ok

      {id, [_first, {_route, index} | _rest]} ->
        raise ArgumentError,
              "routes.seed[#{index}] reuses route id #{inspect(id)}; route ids must be unique"
    end

    # The same rule the API enforces, so a seed cannot boot into a state the API
    # would have refused to create.
    Enum.reduce(parsed, [], fn {route, index}, accepted ->
      case Enum.find(accepted, fn {other, _index} -> collides?(other, route) end) do
        nil ->
          [{route, index} | accepted]

        {_other, first_index} ->
          raise ArgumentError,
                "routes.seed[#{index}] conflicts with routes.seed[#{first_index}]: both capture " <>
                  "#{Enum.join(route.methods, "/")} #{route.path}"
      end
    end)

    :ok
  end

  defp invalid_seed(index, field, message) do
    ArgumentError.exception("routes.seed[#{index}] is invalid: #{field} #{message}")
  end

  defp not_a_cidr(key, cidr) do
    "routes.#{key} entries must be CIDRs, got #{inspect(cidr)}"
  end
end
