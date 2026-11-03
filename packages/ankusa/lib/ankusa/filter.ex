defmodule Ankusa.Filter do
  @max_leaves 32
  @max_depth 4
  @max_literal_bytes 256
  @max_values 64
  @max_header_bytes 128
  @max_segments 16

  @moduledoc """
  Per-sink content filters: a data predicate on a sink entry that decides
  whether a hook gets a delivery row for that sink.

      sinks: [
        {Ankusa.Sink.Kafka, topic: "invoices", filter: %{event_type: ["invoice.paid", "invoice.failed"]}},
        {Ankusa.Sink.Http, url: "...", filter: %{"header.x-github-event" => "push"}}
      ]

  The option is read by core, not by the sink module (`normalize_sinks!/2`
  strips it, as `Ankusa.Ordering` strips `ordered`). A sink without a `filter`
  is selected for every hook. A hook no sink selects is acknowledged
  `201 {"filtered": true}` and not stored (`Ankusa.Edge.Ingest`).

  ## Grammar

  Plain data, identical in Elixir, YAML and the admin API's JSON; map keys may
  be atoms or strings.

      filter  := node
      node    := %{all: [node, …]} | %{any: [node, …]} | %{not: node} | %{field => matcher}
      field   := "event_type" | "tenant" | "content_type" | "header.<name>" | "json.<dot.path>"
      matcher := "literal" (equals) | ["a", "b"] (in) | %{op => value}, exactly one op
      op      := eq | neq | in | not_in | prefix | suffix | exists (a boolean)

  A map node has exactly one key; `all` and `any` take non-empty lists. Ceilings,
  each an error when exceeded: #{@max_leaves} conditions per filter, #{@max_depth} levels of
  `all`/`any`/`not`, a literal of #{@max_literal_bytes} bytes, #{@max_values} values in an `in`/`not_in`, a
  header name of #{@max_header_bytes} bytes, a JSON path of 1 to #{@max_segments} non-empty segments.

  ## Values

    * `event_type`: the hook's `Ankusa.Envelope.event_type` (`Ankusa.EventType`).
    * `tenant`: the hook's tenant, `"default"` without one.
    * `content_type`: the media type of the `content-type` header (before the
      first `;`, trimmed, ASCII-lowercased); absent without one.
    * `header.<name>`: the first value of that request header (case-insensitive),
      from the complete header list at ingest and the stored list at a replay.
    * `json.<path>`: the value at the dot path of the JSON body, shared with the
      dedupe key, the event type and the ordering key (one capped decode, see
      `Ankusa.Routing`). A string is itself, an integer its decimal string, a
      boolean `"true"` or `"false"`; anything else (an object, a list, `null`, a
      missing key) is absent.

  Comparisons are byte-exact; `prefix` and `suffix` work on bytes. There is no
  numeric comparison. On an absent value `eq`, `in`, `prefix` and `suffix` are
  false; `neq` and `not_in` are true; `exists: true` is false and `exists: false`
  is true.

  ## Unevaluable

  A filter with a `json.*` condition cannot be judged when the body is over
  `dedupe.max_json_bytes` or is not JSON. A filter never loses a hook: it
  selects the sink, and `[:ankusa, :dispatch, :filter_unevaluable]` is emitted
  once for the sink (`Ankusa.Routing`).
  """

  alias Ankusa.{Envelope, Ordering, Source}

  @enforce_keys [:tree]
  defstruct [:tree, json?: false]

  @typedoc "A header name (lowercased) or a JSON path, or one of the fixed fields."
  @type field ::
          :event_type | :tenant | :content_type | {:header, String.t()} | {:json, [String.t()]}

  @type matcher ::
          {:eq | :neq | :prefix | :suffix, String.t()}
          | {:in | :not_in, [String.t()]}
          | {:exists, boolean()}

  @type tree ::
          {:all, [tree()]} | {:any, [tree()]} | {:not, tree()} | {:leaf, field(), matcher()}

  @type t :: %__MODULE__{tree: tree(), json?: boolean()}

  @typedoc "The body's JSON as `Ankusa.Routing` decoded it, or `:skip` when it was not."
  @type decoded :: {:ok, term()} | {:error, term()} | :skip

  @ops ~w(eq neq in not_in prefix suffix exists)

  # ── the grammar ───────────────────────────────────────────────────────────

  @doc """
  Parse `term` into a filter. `{:error, reason}` reads after the word "filter"
  (`"filter node must have exactly one key"`).
  """
  @spec parse(term()) :: {:ok, t()} | {:error, String.t()}
  def parse(term) do
    tree = node!(term, 0)

    if leaf_count(tree) > @max_leaves,
      do: fail!("cannot have more than #{@max_leaves} conditions")

    {:ok, %__MODULE__{tree: tree, json?: json_tree?(tree)}}
  catch
    {:filter_error, reason} -> {:error, reason}
  end

  @doc "`parse/1`, raising `ArgumentError` with `\"\#{label}: filter …\"`."
  @spec new!(String.t(), term()) :: t()
  def new!(label, term) do
    case parse(term) do
      {:ok, filter} -> filter
      {:error, reason} -> raise ArgumentError, "#{label}: filter #{reason}"
    end
  end

  defp fail!(reason), do: throw({:filter_error, reason})

  defp node!(map, depth) when is_map(map) and not is_struct(map) do
    case Map.to_list(map) do
      [{key, value}] ->
        node_pair!(key_name!(key), value, depth)

      pairs ->
        fail!(
          "node must have exactly one key, got #{length(pairs)}" <>
            if(pairs == [], do: "", else: ": " <> Enum.map_join(pairs, ", ", &key_label/1))
        )
    end
  end

  defp node!(other, _depth), do: fail!("node must be a map, got: #{inspect(other)}")

  defp key_label({key, _value}), do: if(is_binary(key), do: key, else: inspect(key))

  defp key_name!(key) when is_binary(key), do: key
  defp key_name!(key) when is_atom(key) and key not in [nil, true, false], do: Atom.to_string(key)
  defp key_name!(key), do: fail!("node key must be a string, got: #{inspect(key)}")

  defp node_pair!(name, nodes, depth) when name in ["all", "any"] do
    depth = nest!(depth)

    case nodes do
      [_ | _] = list when is_list(list) ->
        {String.to_existing_atom(name), Enum.map(list, &node!(&1, depth))}

      other ->
        fail!("#{name} needs a non-empty list of nodes, got: #{inspect(other)}")
    end
  end

  defp node_pair!("not", node, depth), do: {:not, node!(node, nest!(depth))}

  defp node_pair!(name, matcher, _depth), do: {:leaf, field!(name), matcher!(name, matcher)}

  defp nest!(depth) when depth >= @max_depth,
    do: fail!("cannot nest all, any and not more than #{@max_depth} deep")

  defp nest!(depth), do: depth + 1

  defp field!("event_type"), do: :event_type
  defp field!("tenant"), do: :tenant
  defp field!("content_type"), do: :content_type

  defp field!("header." <> name) do
    cond do
      name == "" ->
        fail!("field \"header.\" needs a header name")

      byte_size(name) > @max_header_bytes ->
        fail!("header name is over #{@max_header_bytes} bytes")

      true ->
        {:header, String.downcase(name, :ascii)}
    end
  end

  defp field!("json." <> path) do
    segments = String.split(path, ".")

    cond do
      length(segments) > @max_segments ->
        fail!("json path #{inspect(path)} has more than #{@max_segments} segments")

      Enum.any?(segments, &(&1 == "")) ->
        fail!("json path #{inspect(path)} needs non-empty segments")

      true ->
        {:json, segments}
    end
  end

  defp field!(other) do
    fail!(
      "field #{inspect(other)} is not one of event_type, tenant, content_type, " <>
        "header.<name>, json.<path> (or all, any, not)"
    )
  end

  defp matcher!(field, value) when is_binary(value), do: {:eq, literal!(field, value)}
  defp matcher!(field, list) when is_list(list), do: {:in, values!(field, "in", list)}

  defp matcher!(field, map) when is_map(map) and not is_struct(map) do
    case Map.to_list(map) do
      [{key, value}] -> op!(field, key_name!(key), value)
      _ -> fail!("#{field} takes exactly one operator of #{Enum.join(@ops, ", ")}")
    end
  end

  defp matcher!(field, other) do
    fail!("#{field} takes a string, a list of strings or one operator, got: #{inspect(other)}")
  end

  defp op!(field, op, value) when op in ["eq", "neq", "prefix", "suffix"],
    do: {String.to_existing_atom(op), literal!(field, value)}

  defp op!(field, op, value) when op in ["in", "not_in"],
    do: {String.to_existing_atom(op), values!(field, op, value)}

  defp op!(_field, "exists", value) when is_boolean(value), do: {:exists, value}

  defp op!(field, "exists", value),
    do: fail!("#{field}: exists takes true or false, got: #{inspect(value)}")

  defp op!(field, op, _value),
    do: fail!("#{field}: unknown operator #{inspect(op)}, use one of #{Enum.join(@ops, ", ")}")

  defp values!(field, op, list) when is_list(list) do
    cond do
      list == [] -> fail!("#{field}: #{op} needs at least one value")
      length(list) > @max_values -> fail!("#{field}: #{op} takes at most #{@max_values} values")
      true -> Enum.map(list, &literal!(field, &1))
    end
  end

  defp values!(field, op, other),
    do: fail!("#{field}: #{op} takes a list of strings, got: #{inspect(other)}")

  defp literal!(_field, value) when is_binary(value) and byte_size(value) <= @max_literal_bytes,
    do: value

  defp literal!(field, value) when is_binary(value),
    do: fail!("#{field}: a value is over #{@max_literal_bytes} bytes")

  defp literal!(field, value),
    do: fail!("#{field}: a value must be a string (quote a number), got: #{inspect(value)}")

  defp leaf_count({:leaf, _field, _matcher}), do: 1
  defp leaf_count({:not, node}), do: leaf_count(node)
  defp leaf_count({_combinator, nodes}), do: nodes |> Enum.map(&leaf_count/1) |> Enum.sum()

  defp json_tree?({:leaf, {:json, _path}, _matcher}), do: true
  defp json_tree?({:leaf, _field, _matcher}), do: false
  defp json_tree?({:not, node}), do: json_tree?(node)
  defp json_tree?({_combinator, nodes}), do: Enum.any?(nodes, &json_tree?/1)

  # ── a source's sink entries ───────────────────────────────────────────────

  @doc """
  Reads `filter` off every sink entry of `source_id` and returns the entries as
  the pipeline hands them to a sink (the option gone) with the per-sink filters,
  or `nil` when no sink has one. Raises `ArgumentError` naming the source and
  the sink for a malformed filter.
  """
  @spec normalize_sinks!(String.t(), [{module(), keyword()}]) ::
          {[{module(), keyword()}], [t() | nil] | nil}
  def normalize_sinks!(source_id, sinks) when is_list(sinks) do
    {sinks, filters} =
      sinks
      |> Enum.with_index()
      |> Enum.map(fn {sink, index} -> normalize_sink!(source_id, index, sink) end)
      |> Enum.unzip()

    {sinks, if(Enum.all?(filters, &is_nil/1), do: nil, else: filters)}
  end

  def normalize_sinks!(_source_id, sinks), do: {sinks, nil}

  defp normalize_sink!(source_id, index, {mod, opts} = sink)
       when is_atom(mod) and is_list(opts) do
    if Keyword.keyword?(opts) and Keyword.has_key?(opts, :filter) do
      stripped = {mod, Keyword.delete(opts, :filter)}

      case Keyword.fetch!(opts, :filter) do
        nil -> {stripped, nil}
        term -> {stripped, new!("source #{inspect(source_id)}: sinks[#{index}]", term)}
      end
    else
      {sink, nil}
    end
  end

  defp normalize_sink!(_source_id, _index, sink), do: {sink, nil}

  # ── what the source and the instance's config say about it ────────────────

  @doc """
  The checks a source needs the instance's config for: an `event_type` condition
  needs a source that reads one, and a `header.<name>` condition must survive
  the source's stored-header policy, because a replay evaluates the filter on
  the stored hook (the same rule as an ordering key, `Ankusa.Ordering.check/2`).
  """
  @spec check(Ankusa.Config.t(), Source.t()) :: :ok | {:error, String.t()}
  def check(_config, %Source{filters: nil}), do: :ok

  def check(%Ankusa.Config{} = config, %Source{} = source) do
    with :ok <- check_event_type(source.id, source.filters, source.event_type != nil) do
      policy = Ordering.stored_policy(config, source)

      source.filters
      |> Enum.with_index()
      |> Enum.find_value(:ok, fn
        {nil, _index} ->
          nil

        {filter, index} ->
          filter
          |> headers()
          |> Enum.find_value(fn name ->
            if name in policy.drop or name in policy.hash do
              {:error,
               "source #{inspect(source.id)}: sinks[#{index}] filters on header #{inspect(name)}, " <>
                 "which this source's store_headers policy drops or hashes; a replay could not " <>
                 "evaluate the filter on the stored hook"}
            end
          end)
      end)
    end
  end

  @doc """
  An `event_type` condition on a source that reads no event type could never
  match: `{:error, message}`. `reads_event_type?` is whether the source has an
  `event_type`.
  """
  @spec check_event_type(String.t(), [t() | nil] | nil, boolean()) ::
          :ok | {:error, String.t()}
  def check_event_type(_source_id, nil, _reads?), do: :ok
  def check_event_type(_source_id, _filters, true), do: :ok

  def check_event_type(source_id, filters, false) do
    filters
    |> Enum.with_index()
    |> Enum.find_value(:ok, fn
      {nil, _index} ->
        nil

      {filter, index} ->
        if event_type?(filter.tree) do
          {:error,
           "source #{inspect(source_id)}: sinks[#{index}] filters on event_type, but the " <>
             "source has no event_type to read it from"}
        end
    end)
  end

  @doc "Boot check: `check/2` for every statically configured source. Raises `ArgumentError`."
  @spec validate_config!(Ankusa.Config.t()) :: :ok
  def validate_config!(%Ankusa.Config{} = config) do
    config
    |> Ankusa.Queue.static_sources()
    |> Enum.each(fn {_id, source} ->
      with {:error, message} <- check(config, source), do: raise(ArgumentError, message)
    end)
  end

  defp headers(%__MODULE__{tree: tree}), do: tree |> leaves() |> Enum.flat_map(&header_of/1)

  defp header_of({:leaf, {:header, name}, _matcher}), do: [name]
  defp header_of(_leaf), do: []

  defp event_type?(tree), do: tree |> leaves() |> Enum.any?(&match?({:leaf, :event_type, _}, &1))

  defp leaves({:leaf, _field, _matcher} = leaf), do: [leaf]
  defp leaves({:not, node}), do: leaves(node)
  defp leaves({_combinator, nodes}), do: Enum.flat_map(nodes, &leaves/1)

  # ── evaluation ────────────────────────────────────────────────────────────

  @doc "True when judging any of `filters` (a source's list, one filter, or `nil`) needs the decoded body."
  @spec needs_json?(t() | [t() | nil] | nil) :: boolean()
  def needs_json?(nil), do: false
  def needs_json?(%__MODULE__{json?: json?}), do: json?
  def needs_json?(filters) when is_list(filters), do: Enum.any?(filters, &needs_json?/1)

  @doc """
  Judge `filter` for `env`: `:match`, `:no_match`, or `:unevaluable` (it reads
  the JSON body and `decoded` is not a decoded body).
  """
  @spec evaluate(t(), Envelope.t(), decoded()) :: :match | :no_match | :unevaluable
  def evaluate(%__MODULE__{json?: true}, _env, decoded) when not is_tuple(decoded),
    do: :unevaluable

  def evaluate(%__MODULE__{json?: true}, _env, {:error, _reason}), do: :unevaluable

  def evaluate(%__MODULE__{tree: tree}, %Envelope{} = env, decoded),
    do: if(holds?(tree, env, decoded), do: :match, else: :no_match)

  @doc """
  The indexes of the sinks `filters` (a source's `filters`, aligned with its
  sinks) select for `env`; `:all` for `nil`. An unevaluable filter selects.
  """
  @spec select([t() | nil] | nil, Envelope.t(), decoded()) :: :all | [non_neg_integer()]
  def select(nil, _env, _decoded), do: :all

  def select(filters, %Envelope{} = env, decoded) when is_list(filters) do
    filters
    |> Enum.with_index()
    |> Enum.flat_map(fn
      {nil, index} -> [index]
      {filter, index} -> if evaluate(filter, env, decoded) == :no_match, do: [], else: [index]
    end)
  end

  @doc "The indexes of the filters that `decoded` leaves unevaluable."
  @spec unevaluable([t() | nil] | nil, decoded()) :: [non_neg_integer()]
  def unevaluable(nil, _decoded), do: []
  def unevaluable(_filters, {:ok, _body}), do: []

  def unevaluable(filters, _decoded) when is_list(filters) do
    for {%__MODULE__{json?: true}, index} <- Enum.with_index(filters), do: index
  end

  defp holds?({:all, nodes}, env, decoded), do: Enum.all?(nodes, &holds?(&1, env, decoded))
  defp holds?({:any, nodes}, env, decoded), do: Enum.any?(nodes, &holds?(&1, env, decoded))
  defp holds?({:not, node}, env, decoded), do: not holds?(node, env, decoded)

  defp holds?({:leaf, field, matcher}, env, decoded),
    do: matches?(matcher, value(field, env, decoded))

  defp matches?({:eq, literal}, value), do: value == literal
  defp matches?({:neq, literal}, value), do: value != literal
  defp matches?({:in, literals}, value), do: value != nil and value in literals
  defp matches?({:not_in, literals}, value), do: value == nil or value not in literals

  defp matches?({:prefix, literal}, value),
    do: value != nil and String.starts_with?(value, literal)

  defp matches?({:suffix, literal}, value), do: value != nil and String.ends_with?(value, literal)
  defp matches?({:exists, present?}, value), do: value != nil == present?

  defp value(:event_type, env, _decoded), do: env.event_type
  defp value(:tenant, env, _decoded), do: env.tenant_id || "default"
  defp value(:content_type, env, _decoded), do: media_type(env.content_type)
  defp value({:header, name}, env, _decoded), do: Envelope.header_downcased(env, name)
  defp value({:json, path}, _env, {:ok, body}), do: json_value(body, path)

  defp media_type(nil), do: nil

  defp media_type(content_type) when is_binary(content_type) do
    [media | _parameters] = :binary.split(content_type, ";")
    media |> String.trim() |> String.downcase(:ascii)
  end

  defp json_value(value, []) when is_binary(value), do: value
  defp json_value(value, []) when is_integer(value), do: Integer.to_string(value)
  defp json_value(true, []), do: "true"
  defp json_value(false, []), do: "false"

  defp json_value(value, [segment | rest]) when is_map(value) do
    case Map.fetch(value, segment) do
      {:ok, next} -> json_value(next, rest)
      :error -> nil
    end
  end

  defp json_value(_value, _path), do: nil

  # ── describing ────────────────────────────────────────────────────────────

  @doc """
  One line, for the AsyncAPI channel description:
  `all(event_type in [invoice.paid, invoice.failed], header.x-github-event = push)`.
  """
  @spec describe(t()) :: String.t()
  def describe(%__MODULE__{tree: tree}), do: describe_node(tree)

  defp describe_node({:all, nodes}),
    do: "all(" <> Enum.map_join(nodes, ", ", &describe_node/1) <> ")"

  defp describe_node({:any, nodes}),
    do: "any(" <> Enum.map_join(nodes, ", ", &describe_node/1) <> ")"

  defp describe_node({:not, node}), do: "not(" <> describe_node(node) <> ")"

  defp describe_node({:leaf, field, matcher}),
    do: field_name(field) <> " " <> describe_matcher(matcher)

  defp describe_matcher({:eq, literal}), do: "= " <> literal
  defp describe_matcher({:neq, literal}), do: "!= " <> literal
  defp describe_matcher({:prefix, literal}), do: "starts with " <> literal
  defp describe_matcher({:suffix, literal}), do: "ends with " <> literal
  defp describe_matcher({:in, literals}), do: "in [" <> Enum.join(literals, ", ") <> "]"
  defp describe_matcher({:not_in, literals}), do: "not in [" <> Enum.join(literals, ", ") <> "]"
  defp describe_matcher({:exists, true}), do: "exists"
  defp describe_matcher({:exists, false}), do: "is absent"

  defp field_name(:event_type), do: "event_type"
  defp field_name(:tenant), do: "tenant"
  defp field_name(:content_type), do: "content_type"
  defp field_name({:header, name}), do: "header." <> name
  defp field_name({:json, path}), do: "json." <> Enum.join(path, ".")
end
