defmodule Ankusa.Gateway.Authz.Table do
  @moduledoc """
  The `table` authorizer: permissions that live in tables somebody else
  maintains, reduced to rows of `principal`, `action`, `space`, `prefix` and
  `effect`.

  A row matches a request when

    * its `principal` is one of the request's identities (`identities`, three
      templates by default: the caller's id, each group and each tenant);
    * its `action` is the request's action or `*`;
    * its `space` is the request's space or `*`;
    * its `prefix` is the resource's key or one of its ancestor directories
      (`""`, `"a/"`, `"a/b/"`, …; a key has at most 32 segments, so at most 33
      prefixes).

  The request is denied when any matching row denies, allowed when any
  matching row allows (with `effect` absent or `allow`), and abstains when no
  row matches.

  ## Modes

    * `:snapshot` (`kind: :cached`) — an `Ankusa.Gateway.Authz.Table.Snapshot`
      process loads the source into a fresh ETS table every `refresh_ms` (or
      whenever `version/2` says it changed) and publishes it atomically. A
      decision is a bounded number of ETS reads, so a customer table never
      sits in the request path. A snapshot older than `stale_after_ms` fails
      closed (`{:error, {:unavailable, :stale_snapshot}}`) until the next load
      succeeds.
    * `:lookup` (`kind: :remote`) — every miss asks the source
      (`lookup_query`) through the authorizer's caller and decision cache, so
      freshness is bounded by `authz.cache`, not by a refresh. Use this for
      permissions that are not prefix-shaped.

  `cache_scope/2` is `{generation, kind, space, key}`: a new snapshot orphans
  every cached decision, so no decision from before a refresh outlives it.

  ## Options

  | Key | Default | |
  | --- | --- | --- |
  | `mode` | `:snapshot` | `:snapshot` or `:lookup` |
  | `source` | required | `{module, opts}`, an `Ankusa.Gateway.Authz.Table.Source` |
  | `columns` | `%{principal: "principal", action: "action", space: "space", prefix: "prefix", effect: "effect"}` | the source's column names |
  | `identities` | `["user:{{ principal.id }}", "group:{{ principal.groups }}", "tenant:{{ principal.tenants }}"]` | `Ankusa.Gateway.Template` templates, expanded with `render_many/2` |
  | `max_identities` | `256` | a caller with more is unavailable (fail closed) |
  | `ready_timeout_ms` | `60000` | how long a snapshot that never loaded holds `/ready` at `503`; `0` = never |
  | `refresh_ms` | `30000` | a snapshot's reload period |
  | `max_rows` | `1000000` | a larger load is refused and the previous table kept |
  | `max_bytes` | `268435456` | likewise, in ETS bytes |
  | `stale_after_ms` | `300000` | a snapshot older than this is unavailable |

  plus the common `:remote` options (`Ankusa.Gateway.Authz.Remote`), used by
  `mode: :lookup`.

  ## Telemetry

  `Ankusa.Gateway.Authz.Table.Snapshot` emits `[:ankusa, :gateway,
  :table_refresh]` `%{duration, rows, bytes}` `%{instance, authorizer, result:
  :ok | :unchanged | :too_large | :invalid | :error}` per load.
  """

  @behaviour Ankusa.Gateway.Authorizer

  alias Ankusa.Gateway.Authz.{Remote, Request}
  alias Ankusa.Gateway.{Options, Template}

  @action_names ~w(read write delete list share)
  @space ~r/\A[a-z0-9][a-z0-9_-]{0,62}\z/
  @keys [
          :mode,
          :source,
          :columns,
          :identities,
          :max_identities,
          :ready_timeout_ms,
          :refresh_ms,
          :max_rows,
          :max_bytes,
          :stale_after_ms
        ] ++ Remote.keys()

  @column_keys [:principal, :action, :space, :prefix, :effect]
  @default_columns %{
    principal: "principal",
    action: "action",
    space: "space",
    prefix: "prefix",
    effect: "effect"
  }
  @default_identities [
    "user:{{ principal.id }}",
    "group:{{ principal.groups }}",
    "tenant:{{ principal.tenants }}"
  ]
  @template_roots ["principal", "claims"]

  @impl true
  @spec init!(map(), String.t()) :: map()
  def init!(opts, key) do
    given = Options.map!(opts, key)
    Options.unknown_keys!(given, @keys, key)
    mode = mode!(Map.get(given, :mode, :snapshot), key)

    %{
      name: authorizer_name(key),
      mode: mode,
      source: source!(Map.get(given, :source), "#{key}.source", mode),
      columns: columns!(Map.get(given, :columns, %{}), "#{key}.columns"),
      identities:
        identities!(Map.get(given, :identities, @default_identities), "#{key}.identities"),
      max_identities:
        Options.int_in!(Map.get(given, :max_identities, 256), 1..2_000, "#{key}.max_identities"),
      ready_timeout_ms:
        Options.int_in!(
          Map.get(given, :ready_timeout_ms, 60_000),
          0..600_000,
          "#{key}.ready_timeout_ms"
        ),
      refresh_ms:
        Options.int_in!(
          Map.get(given, :refresh_ms, 30_000),
          1_000..86_400_000,
          "#{key}.refresh_ms"
        ),
      max_rows:
        Options.int_in!(Map.get(given, :max_rows, 1_000_000), 1..50_000_000, "#{key}.max_rows"),
      max_bytes:
        Options.int_in!(
          Map.get(given, :max_bytes, 268_435_456),
          1_048_576..17_179_869_184,
          "#{key}.max_bytes"
        ),
      stale_after_ms:
        Options.int_in!(
          Map.get(given, :stale_after_ms, 300_000),
          1_000..86_400_000,
          "#{key}.stale_after_ms"
        ),
      remote: Remote.options!(given, key)
    }
    |> stale!(key)
  end

  defp mode!("snapshot", _key), do: :snapshot
  defp mode!(:snapshot, _key), do: :snapshot
  defp mode!("lookup", _key), do: :lookup
  defp mode!(:lookup, _key), do: :lookup

  defp mode!(other, key),
    do: raise(ArgumentError, "#{key}.mode must be :snapshot or :lookup, got #{inspect(other)}")

  defp source!({module, opts}, key, mode) when is_atom(module) do
    unless Options.implements?(module, init!: 2, load: 4) do
      raise ArgumentError,
            "#{key} must name a module implementing Ankusa.Gateway.Authz.Table.Source, " <>
              "got #{inspect(module)}"
    end

    if mode == :lookup and not Options.implements?(module, lookup: 6) do
      raise ArgumentError, "#{key}: #{inspect(module)} does not implement lookup/6 (mode: lookup)"
    end

    source_opts = Options.map!(opts, key)
    state = module.init!(source_opts, key)
    %{module: module, state: state, opts: source_opts}
  end

  defp source!(other, key, _mode),
    do: raise(ArgumentError, "#{key} must be {module, opts}, got #{inspect(other)}")

  defp columns!(given, key) do
    map = Options.map!(given, key)
    Options.unknown_keys!(map, @column_keys, key)

    Map.new(@column_keys, fn column ->
      {column,
       Options.string!(
         Map.get(map, column, Map.fetch!(@default_columns, column)),
         "#{key}.#{column}"
       )}
    end)
  end

  defp identities!(list, key) when is_list(list) and list != [] do
    list
    |> Enum.with_index()
    |> Enum.map(fn {string, index} ->
      Template.compile!(string, "#{key}[#{index}]", @template_roots)
    end)
  end

  defp identities!(other, key),
    do:
      raise(ArgumentError, "#{key} must be a non-empty list of templates, got #{inspect(other)}")

  defp stale!(state, key) do
    if state.stale_after_ms < state.refresh_ms do
      raise ArgumentError, "#{key}.stale_after_ms must be at least #{key}.refresh_ms"
    end

    state
  end

  defp authorizer_name(key) do
    key |> String.split(".") |> List.last()
  end

  # ── the authorizer ──────────────────────────────────────────────────────────

  @impl true
  @spec kind(map()) :: :cached | :remote
  def kind(%{mode: :lookup}), do: :remote
  def kind(_state), do: :cached

  @impl true
  @spec decide(Request.t(), map(), map()) :: Ankusa.Gateway.Authorizer.result()
  def decide(%Request{} = request, state, ctx) do
    with {:ok, identities} <- identities(request, state) do
      case state.mode do
        :snapshot -> snapshot_decide(identities, request, state)
        :lookup -> lookup_decide(identities, request, state, ctx)
      end
    end
  end

  defp identities(request, state) do
    bindings = Template.bindings(request)

    names =
      for template <- state.identities,
          {:ok, rendered} <- [Template.render_many(template, bindings)],
          name <- rendered,
          uniq: true,
          do: name

    if length(names) > state.max_identities,
      do: {:error, {:unavailable, :too_many_identities}},
      else: {:ok, names}
  end

  # ── snapshot mode ───────────────────────────────────────────────────────────

  defp snapshot_decide(identities, request, state) do
    %{space: space, key: key} = request.resource
    action = to_string(request.action)
    spaces = Enum.uniq([space, "*"])
    actions = Enum.uniq([action, "*"])
    prefixes = prefixes(key)

    case snapshot(identities, {spaces, actions, prefixes}, state, 1) do
      :stale -> {:error, {:unavailable, :stale_snapshot}}
      verdict -> verdict
    end
  end

  defp snapshot(identities, combos, state, attempts) do
    with {:ok, tid, loaded_ms, has_deny?} <- published(state),
         true <- fresh?(loaded_ms, state) do
      try do
        row_verdict(tid, identities, combos, has_deny?)
      rescue
        # The generation was deleted under a paused reader (the snapshot
        # published a newer table and dropped ours). Re-read once: a live
        # table answers, and otherwise the snapshot is unavailable.
        ArgumentError ->
          if attempts > 1, do: :stale, else: snapshot(identities, combos, state, attempts + 1)
      end
    else
      _stale -> :stale
    end
  end

  defp published(%{instance: instance, name: name}) do
    case :ets.lookup(tables_table(instance), name) do
      [{^name, tid, _generation, loaded_ms, has_deny?}] -> {:ok, tid, loaded_ms, has_deny?}
      [] -> :error
    end
  rescue
    ArgumentError -> :error
  end

  defp fresh?(loaded_ms, state),
    do: System.monotonic_time(:millisecond) - loaded_ms <= state.stale_after_ms

  # The four loops are unrolled rather than a list of 2048 keys: a decision
  # allocates nothing but the key tuples, which is what keeps its p99 flat.
  defp row_verdict(tid, identities, {spaces, actions, prefixes}, has_deny?) do
    Enum.reduce_while(identities, {:ok, :abstain, %{}}, fn identity, found ->
      case identity_verdict(tid, identity, spaces, actions, prefixes, has_deny?, found) do
        {:halt, _verdict} = stop -> {:halt, stop}
        found -> {:cont, found}
      end
    end)
    |> case do
      {:halt, verdict} -> verdict
      found -> found
    end
  end

  defp identity_verdict(tid, identity, spaces, actions, prefixes, has_deny?, found) do
    Enum.reduce_while(spaces, found, fn space, found ->
      case space_verdict(tid, identity, space, actions, prefixes, has_deny?, found) do
        {:halt, _verdict} = stop -> {:halt, stop}
        found -> {:cont, found}
      end
    end)
  end

  defp space_verdict(tid, identity, space, actions, prefixes, has_deny?, found) do
    Enum.reduce_while(actions, found, fn action, found ->
      case prefix_verdict(tid, identity, space, action, prefixes, has_deny?, found) do
        {:halt, _verdict} = stop -> {:halt, stop}
        found -> {:cont, found}
      end
    end)
  end

  defp prefix_verdict(tid, identity, space, action, prefixes, has_deny?, found) do
    Enum.reduce_while(prefixes, found, fn prefix, found ->
      case :ets.lookup(tid, {identity, space, action, prefix}) do
        [{_key, :deny}] ->
          {:halt, {:halt, {:ok, :deny, %{reason: :table}}}}

        [{_key, :allow}] when has_deny? ->
          {:cont, {:ok, :allow, %{}}}

        [{_key, :allow}] ->
          {:halt, {:halt, {:ok, :allow, %{}}}}

        [] ->
          {:cont, found}
      end
    end)
  end

  # ── lookup mode ─────────────────────────────────────────────────────────────

  defp lookup_decide(identities, request, state, ctx) do
    %{space: space, key: key} = request.resource
    action = to_string(request.action)

    case state.source.module.lookup(state.source.state, ctx, identities, action, space, key) do
      {:ok, rows} when is_list(rows) -> rows_verdict(rows, state)
      {:ok, other} -> {:error, {:unavailable, {:bad_response, other}}}
      {:error, reason} -> {:error, {:unavailable, reason}}
    end
  end

  defp rows_verdict(rows, state) do
    effects = rows |> Enum.map(&effect(row_field(&1, state.columns.effect))) |> Enum.uniq()

    cond do
      :deny in effects -> {:ok, :deny, %{reason: :table}}
      :allow in effects -> {:ok, :allow, %{}}
      true -> {:ok, :abstain, %{}}
    end
  end

  # ── shared ──────────────────────────────────────────────────────────────────

  @doc false
  # The five columns of a source row, as the configured names.
  @spec row_field(map(), String.t()) :: term()
  def row_field(row, column) when is_map(row),
    do: Map.get(row, column) || Map.get(row, safe_atom(column))

  def row_field(_row, _column), do: nil

  @doc false
  # `:allow` (the default), `:deny`, or `:invalid`.
  @spec effect(term()) :: :allow | :deny | :invalid
  def effect(nil), do: :allow
  def effect("allow"), do: :allow
  def effect(:allow), do: :allow
  def effect("deny"), do: :deny
  def effect(:deny), do: :deny
  def effect(_other), do: :invalid

  @doc false
  # Is `value` an action a row may name (`*` included)?
  @spec action(term()) :: boolean()
  def action(value) when is_binary(value), do: value == "*" or value in @action_names
  def action(_value), do: false

  @doc false
  # Is `value` a space a row may name (`*` included)?
  @spec space(term()) :: boolean()
  def space("*"), do: true

  def space(value) when is_binary(value),
    do: Regex.match?(@space, value) or value == "claims"

  def space(_value), do: false

  @doc false
  # The key itself and every ancestor directory prefix, `""` included.
  @spec prefixes(String.t()) :: [String.t()]
  def prefixes(""), do: [""]

  def prefixes(key) do
    segments = key |> String.trim_trailing("/") |> String.split("/")

    ancestors =
      for n <- 0..(length(segments) - 1) do
        if n == 0, do: "", else: Enum.take(segments, n) |> Enum.join("/") |> Kernel.<>("/")
      end

    Enum.uniq(ancestors ++ [key])
  end

  @doc false
  # The named ETS table a snapshot publishes its generation into; created by
  # `Ankusa.Gateway.Supervisor`.
  # sobelow_skip ["DOS.BinToAtom"]
  @spec tables_table(atom()) :: atom()
  def tables_table(instance), do: :"ankusa_gateway_tables_#{instance}"

  defp safe_atom(column) when is_binary(column) do
    String.to_existing_atom(column)
  rescue
    ArgumentError -> nil
  end

  # ── child specs and readiness ───────────────────────────────────────────────

  @impl true
  @spec cache_scope(Request.t(), map()) :: term()
  def cache_scope(%Request{resource: resource}, %{mode: :lookup}) do
    {resource.kind, resource.space, resource[:tenant], resource.key}
  end

  def cache_scope(%Request{resource: resource}, state) do
    {generation(state), resource.kind, resource.space, resource.key}
  end

  defp generation(%{instance: instance, name: name}) do
    case :ets.lookup(tables_table(instance), name) do
      [{^name, _tid, generation, _loaded_ms, _has_deny?}] -> generation
      [] -> :none
    end
  rescue
    ArgumentError -> :none
  end

  @impl true
  @spec child_specs(atom(), String.t(), map()) :: [Supervisor.child_spec()]
  def child_specs(instance, name, state) do
    source_children =
      if function_exported?(state.source.module, :child_specs, 3),
        do: state.source.module.child_specs(instance, name, state.source.state),
        else: []

    snapshot =
      case state.mode do
        :lookup ->
          []

        :snapshot ->
          snapshot_state = Map.put(state, :instance, instance)
          [{__MODULE__.Snapshot, instance: instance, name: name, state: snapshot_state}]
      end

    source_children ++ snapshot
  end

  @impl true
  @spec ready?(atom(), String.t(), map()) :: boolean()
  def ready?(_instance, _name, %{mode: :lookup}), do: true

  def ready?(instance, name, state) do
    case :ets.lookup(tables_table(instance), {:ready, name}) do
      [{_key, {true, _started_ms}}] -> true
      [{_key, {false, started_ms}}] -> now() - started_ms >= state.ready_timeout_ms
      [] -> false
    end
  rescue
    ArgumentError -> false
  end

  defp now, do: System.monotonic_time(:millisecond)
end
