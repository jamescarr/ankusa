defmodule Ankusa.Ordering do
  @moduledoc """
  Opt-in ordering lanes (ADR 0004): what a source's sink entry may say about
  order, and how a hook's lane is resolved at ingest.

      sinks: [{Ankusa.Sink.Http, url: "...", ordered: true, ordering_key: {:header, "x-entity-id"}}]

  Two options on a sink entry, read by core, not by the sink module:

    * `ordered: true` (default `false`): the sink's hooks of one lane are
      delivered one at a time, each after the previous returned `:ok`, in the
      order they were committed on this node. A retry blocks its lane and
      nothing else.
    * `ordering_key`: `{:header, name}` (a request header, case-insensitive),
      `{:json, "data.object.customer"}` (a dot path into the JSON body, as
      `Ankusa.Dedupe` reads one) or `{mod, fun, args}` (called as
      `apply(mod, fun, [envelope | args])`, returning a binary). Without a key
      every hook of the sink is in one lane. `ordering_key` without `ordered: true`
      keeps its meaning on `Ankusa.Sink.GooglePubSub` (a string or a 1-arity fun,
      handed to the sink) and is an error on every other sink.

  A lane is `{tenant, source, sink index, key}`. It is resolved **at ingest**, from
  the complete header list and the body, because the lane a row joins is fixed
  before it is committed; every other path that makes delivery rows (an archive
  replay, a released quarantine hook) resolves it from the stored envelope by the
  same function, so a `{:header, name}` key must survive the source's
  `store_headers` policy (`check/2`).

  A key longer than 128 bytes (the SQS group id limit) becomes `"sha256:" <> hex`.
  A missing key (no such header, a body that is not JSON, over 1 MiB, a missing or
  non-string path value, an MFA that raised or returned a non-binary) puts the hook
  in the sink's shared keyless lane, counted in
  `ankusa_dispatch_lane_key_missing_total{sink}`: an ordered sink never silently
  falls back to unordered.
  """

  alias Ankusa.{Dedupe, Envelope, Source}
  alias Ankusa.Edge.StoredHeaders

  @json_key_max_bytes 1_048_576
  @max_key_bytes 128

  @typedoc "How one sink's lane key is read."
  @type key_spec :: {:header, String.t()} | {:json, [String.t()]} | {module(), atom(), list()}

  @typedoc "One sink's ordering: `nil` (unordered) or `{:ordered, key spec or nil}`."
  @type sink_ordering :: nil | {:ordered, key_spec() | nil}

  @typedoc "One sink's lane of a hook: the lane id and the key stamped into the row."
  @type lane :: {<<_::128>>, binary()}

  # ── the option grammar ────────────────────────────────────────────────────

  @doc """
  Reads `ordered` and `ordering_key` off every sink entry of `source_id`, and
  returns the entries as the pipeline hands them to a sink (both options gone,
  except the legacy `ordering_key` of an unordered `Sink.GooglePubSub`) with the
  per-sink ordering, or `nil` when no sink is ordered. Raises `ArgumentError`
  naming the source for every form ADR 0004 section 1 rejects.
  """
  @spec normalize_sinks!(String.t(), [{module(), keyword()}]) ::
          {[{module(), keyword()}], [sink_ordering()] | nil}
  def normalize_sinks!(source_id, sinks) when is_list(sinks) do
    {sinks, ordering} =
      sinks
      |> Enum.with_index()
      |> Enum.map(fn {sink, index} -> normalize_sink!(source_id, index, sink) end)
      |> Enum.unzip()

    {sinks, if(Enum.all?(ordering, &is_nil/1), do: nil, else: ordering)}
  end

  def normalize_sinks!(_source_id, sinks), do: {sinks, nil}

  defp normalize_sink!(source_id, index, {mod, opts} = sink)
       when is_atom(mod) and is_list(opts) do
    label = "source #{inspect(source_id)}: sinks[#{index}]"

    if Keyword.keyword?(opts) do
      ordered = Keyword.get(opts, :ordered, false)

      unless is_boolean(ordered) do
        raise ArgumentError, "#{label}: ordered must be a boolean, got #{inspect(ordered)}"
      end

      if ordered,
        do: ordered_sink!(label, mod, opts),
        else: unordered_sink!(label, mod, opts)
    else
      {sink, nil}
    end
  end

  defp normalize_sink!(_source_id, _index, sink), do: {sink, nil}

  defp ordered_sink!(label, mod, opts) do
    key = key_spec!(label, Keyword.get(opts, :ordering_key))

    if mod == Ankusa.Sink.SQS and not fifo?(Keyword.get(opts, :queue_url)) do
      raise ArgumentError,
            "#{label}: ordered: true needs a FIFO queue (queue_url ending in .fifo): " <>
              "a standard queue cannot show its consumer an order"
    end

    {{mod, Keyword.drop(opts, [:ordered, :ordering_key])}, {:ordered, key}}
  end

  defp unordered_sink!(label, mod, opts) do
    opts = Keyword.delete(opts, :ordered)

    if Keyword.has_key?(opts, :ordering_key) and mod != Ankusa.Sink.GooglePubSub do
      raise ArgumentError,
            "#{label}: ordering_key needs ordered: true (only Ankusa.Sink.GooglePubSub " <>
              "takes an ordering_key of its own)"
    end

    {{mod, opts}, nil}
  end

  defp key_spec!(_label, nil), do: nil

  defp key_spec!(_label, {:header, name}) when is_binary(name) and name != "",
    do: {:header, String.downcase(name, :ascii)}

  defp key_spec!(label, {:json, path}) when is_binary(path) do
    segments = String.split(path, ".")

    if Enum.all?(segments, &(&1 != "")),
      do: {:json, segments},
      else:
        raise(
          ArgumentError,
          "#{label}: ordering_key json path segments must be non-empty, got: #{inspect(path)}"
        )
  end

  defp key_spec!(label, {:json, [_ | _] = segments} = spec) do
    if Enum.all?(segments, &(is_binary(&1) and &1 != "")),
      do: spec,
      else:
        raise(
          ArgumentError,
          "#{label}: ordering_key json path segments must be non-empty strings, got: #{inspect(segments)}"
        )
  end

  defp key_spec!(_label, {mod, fun, args} = spec)
       when is_atom(mod) and is_atom(fun) and is_list(args),
       do: spec

  defp key_spec!(label, other) do
    raise ArgumentError,
          "#{label}: with ordered: true, ordering_key must be {:header, name}, " <>
            "{:json, \"a.b\"} or {mod, fun, args}, got: #{inspect(other)}"
  end

  defp fifo?(queue_url) when is_binary(queue_url),
    do: queue_url |> String.trim_trailing("/") |> String.ends_with?(".fifo")

  defp fifo?(_queue_url), do: false

  # ── what the instance's config says about it ──────────────────────────────

  @doc """
  The checks a source needs the instance's config for: lanes need the
  store-backed queue (`wal.type: none` delivers inside the request, with no lane),
  and a `{:header, name}` key must survive the source's stored-header policy,
  because a replay resolves the key from the stored hook.
  """
  @spec check(Ankusa.Config.t(), Source.t()) :: :ok | {:error, String.t()}
  def check(_config, %Source{ordering: nil}), do: :ok

  def check(%Ankusa.Config{wal: :none}, %Source{id: id}) do
    {:error,
     "source #{inspect(id)}: ordered: true needs the store-backed queue (wal.type disk or " <>
       "dual); wal.type none delivers inside the request and keeps no lane"}
  end

  def check(%Ankusa.Config{} = config, %Source{} = source) do
    policy = stored_policy(config, source)

    source.ordering
    |> Enum.with_index()
    |> Enum.find_value(:ok, fn
      {{:ordered, {:header, name}}, index} ->
        if name in policy.drop or name in policy.hash do
          {:error,
           "source #{inspect(source.id)}: sinks[#{index}] orders by header #{inspect(name)}, " <>
             "which this source's store_headers policy drops or hashes; a replay could not " <>
             "resolve the lane from the stored hook"}
        end

      _other ->
        nil
    end)
  end

  defp stored_policy(config, %Source{store_headers: override, verifier: verifier}) do
    policy = StoredHeaders.resolve(override, config.store_headers)

    case verifier do
      {Ankusa.Verifier.Token, opts} ->
        %{policy | drop: Ankusa.Verifier.Token.credential_headers(opts) ++ policy.drop}

      _ ->
        policy
    end
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

  # ── resolving a hook's lanes ──────────────────────────────────────────────

  @doc """
  The provider event key and the lanes of `env` for `source`, resolved from the
  complete header list and the body: `{dedupe_key, lanes}`. `lanes` is `nil` for
  a source with no ordered sink, else a list aligned with the source's sinks, each
  `nil` (unordered) or `{lane, key}`. One JSON decode serves a `dedupe: json` and
  an `ordering_key: {:json, _}` on the same source.
  """
  @spec resolve(atom(), Source.t(), Envelope.t()) :: {String.t() | nil, [lane() | nil] | nil}
  def resolve(instance, %Source{ordering: nil, dedupe: dedupe}, env),
    do: {Dedupe.key(dedupe, env, :skip, Dedupe.max_json_bytes(instance)), nil}

  def resolve(instance, %Source{ordering: ordering, dedupe: dedupe} = source, env) do
    decoded = decode_for(ordering, env)
    key = Dedupe.key(dedupe, env, decoded, Dedupe.max_json_bytes(instance))
    {key, lanes(instance, source, env, decoded)}
  end

  @doc "The lanes of `env` for `source` alone (see `resolve/3`)."
  @spec lanes(atom(), Source.t(), Envelope.t()) :: [lane() | nil] | nil
  def lanes(_instance, %Source{ordering: nil}, _env), do: nil

  def lanes(instance, %Source{ordering: ordering} = source, env),
    do: lanes(instance, source, env, decode_for(ordering, env))

  defp lanes(instance, %Source{id: id, sinks: sinks, ordering: ordering}, env, decoded) do
    sinks
    |> Enum.zip(ordering)
    |> Enum.with_index()
    |> Enum.map(fn
      {{_sink, nil}, _index} ->
        nil

      {{{mod, _opts}, {:ordered, spec}}, index} ->
        key = key(spec, env, decoded)

        if spec != nil and key == nil,
          do:
            Ankusa.Telemetry.emit([:dispatch, :lane_key_missing], %{}, %{
              instance: instance,
              sink: mod
            })

        {lane_id(index, id, key), key || ""}
    end)
  end

  # The body is decoded at most once, and only for a `{:json, _}` key.
  defp decode_for(ordering, env) do
    if Enum.any?(ordering, &match?({:ordered, {:json, _}}, &1)) and
         byte_size(env.body) <= @json_key_max_bytes,
       do: JSON.decode(env.body),
       else: :skip
  end

  defp key(nil, _env, _decoded), do: nil

  defp key({:header, name}, env, _decoded) do
    case Envelope.header(env, name) do
      value when is_binary(value) and value != "" -> clamp(value)
      _ -> nil
    end
  end

  defp key({:json, path}, _env, {:ok, decoded}) do
    case Dedupe.walk(decoded, path) do
      value when is_binary(value) and value != "" -> clamp(value)
      _ -> nil
    end
  end

  # A body past the cap, or one that is not JSON: no key.
  defp key({:json, _path}, _env, _decoded), do: nil

  defp key({mod, fun, args}, env, _decoded) do
    case apply(mod, fun, [env | args]) do
      value when is_binary(value) and value != "" -> clamp(value)
      _ -> nil
    end
  rescue
    _ -> nil
  catch
    _kind, _value -> nil
  end

  defp clamp(key) when byte_size(key) > @max_key_bytes,
    do: "sha256:" <> Base.encode16(:crypto.hash(:sha256, key), case: :lower)

  defp clamp(key), do: key

  @doc """
  The 16-byte lane id of `key` (`nil`: the keyless lane) in sink `sink` of
  `source_id`: hashed so the index key has a fixed width and no delimiter can be
  forged. The tenant is not in it: it is the ordinal in front of it in every lane
  key. A collision would only merge two lanes (more ordering, never less).
  """
  @spec lane_id(non_neg_integer(), String.t(), binary() | nil) :: <<_::128>>
  def lane_id(sink, source_id, key) do
    {flag, key} = if key == nil, do: {1, ""}, else: {0, key}

    :crypto.hash(:sha256, <<sink::16, source_id::binary, 0, flag::8, key::binary>>)
    |> binary_part(0, 16)
  end

  @doc """
  The key a sink receives as `ctx.ordering_key` for a row stamped with `key`
  (`""`: the keyless lane): the key itself, or `"tenant/source"`, the default
  group of the queue adapters, for the keyless lane.
  """
  @spec sink_key(binary(), String.t() | nil, String.t()) :: String.t()
  def sink_key("", tenant_id, source_id), do: "#{tenant_id}/#{source_id}"
  def sink_key(key, _tenant_id, _source_id), do: key
end
