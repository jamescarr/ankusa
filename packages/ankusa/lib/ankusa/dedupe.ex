defmodule Ankusa.Dedupe do
  @default_ttl_ms 259_200_000

  @moduledoc """
  Per-source provider event keys.

  Providers retry a webhook until they see a `2xx`, so a retry can arrive on
  any node of a fleet. Every provider stamps its deliveries with an event id —
  a delivery header (GitHub's `x-github-delivery`, Svix's `svix-id`) or a JSON
  field in the body (Stripe's `"id"`). `key/2` extracts that stamp from an
  envelope, and the queue writer collapses envelopes that share one within the
  TTL window.

  The TTL bounds the memory a provider retry storm can occupy: after it
  expires, the same event commits again (and consumers dedupe on the
  `idempotency_key` the message carries). #{@default_ttl_ms} ms (72 h) covers
  Stripe's 3-day and Shopify's 48-hour retry schedules.

  The window is per tenant and source: the same key under two tenants (one
  `tenant_path` source serves many) is two events.

  ## Sources

    * `{:header, name}` — the value of a request header (case-insensitive).
    * `{:json, ["a", "b"]}` — walk `a` → `b` through the decoded JSON body;
      only map keys are followed. A string value is the key; an integer value
      becomes its decimal string. Anything else yields `nil`.

  ## Salesforce

  The `:salesforce` preset is `{:json, ["id"]}`, and that is a convention, not a
  Salesforce-documented delivery id. Salesforce stamps no delivery-id header on
  any outbound mechanism. Outbound Messages are SOAP/XML (their per-notification
  `Notification/Id` is not reachable by a JSON path), and Flow/Apex HTTP callouts
  and Data 360 webhook targets send a body the org builds. The preset therefore
  reads the top-level `"id"` of a JSON body, which the callout has to set to a
  value that is the same on every retry of one event (the record id plus a
  change stamp, a platform event's `EventUuid`). A body with no `"id"` is never
  deduplicated: pick `header:` or `json:` for a body shaped otherwise.
  """

  require Logger

  @enforce_keys [:from, :ttl_ms]
  defstruct [:from, :ttl_ms]

  @type t :: %__MODULE__{
          from: {:header, String.t()} | {:json, [String.t()]},
          ttl_ms: pos_integer()
        }

  # The provider stamps are stable, public names: keep them in a map so a
  # config typo fails at boot instead of silently never deduping.
  @presets %{
    github: {:header, "x-github-delivery"},
    standard_webhooks: {:header, "webhook-id"},
    svix: {:header, "svix-id"},
    shopify: {:header, "x-shopify-webhook-id"},
    stripe: {:json, ["id"]},
    salesforce: {:json, ["id"]}
  }

  @max_key_bytes 200

  @doc """
  Build a dedupe spec from config. Accepts:

    * `nil` — no dedupe.
    * a preset atom (`#{Enum.join(Map.keys(@presets), "`, `")}`).
    * a keyword list / map with exactly one of `:preset` (atom), `:header`
      (string, stored lowercased) or `:json` (a dot path: every segment
      non-empty), plus an optional `:ttl_ms` (positive integer).

  Anything else raises `ArgumentError` with a `"dedupe: "` message.
  """
  @spec new!(nil | atom() | keyword() | map()) :: t() | nil
  def new!(nil), do: nil
  def new!(preset) when is_atom(preset), do: new!(%{preset: preset})

  def new!(opts) when is_list(opts) or is_map(opts) do
    opts = Map.new(opts)
    ttl_ms = Map.get(opts, :ttl_ms, @default_ttl_ms)

    unless is_integer(ttl_ms) and ttl_ms > 0 do
      raise ArgumentError, "dedupe: ttl_ms must be a positive integer, got: #{inspect(ttl_ms)}"
    end

    from =
      case {Map.get(opts, :preset), Map.get(opts, :header), Map.get(opts, :json)} do
        {nil, nil, nil} ->
          raise ArgumentError,
                "dedupe: set exactly one of preset, header, json (got none)"

        {preset, nil, nil} when is_atom(preset) ->
          case Map.fetch(@presets, preset) do
            {:ok, from} -> from
            :error -> raise ArgumentError, "dedupe: unknown preset #{inspect(preset)}"
          end

        {nil, header, nil} when is_binary(header) and header != "" ->
          {:header, String.downcase(header)}

        {nil, nil, json} when is_binary(json) ->
          segments = String.split(json, ".")
          if Enum.all?(segments, &(&1 != "")), do: {:json, segments}, else: raise_json!(json)

        _other ->
          raise ArgumentError, "dedupe: set exactly one of preset, header, json"
      end

    %__MODULE__{from: from, ttl_ms: ttl_ms}
  end

  def new!(other) do
    raise ArgumentError, "dedupe: expected a preset atom or a map, got: #{inspect(other)}"
  end

  defp raise_json!(json) do
    raise ArgumentError, "dedupe: json path segments must be non-empty, got: #{inspect(json)}"
  end

  @doc """
  Extract the provider event key from an envelope, or `nil` when the stamp is
  missing or the body does not decode.

  A key longer than #{@max_key_bytes} bytes is replaced by its lowercase hex
  SHA-256, prefixed `"sha256:"`.

  `decoded` is the body's JSON when the caller has decoded it already
  (`{:ok, term}` or `{:error, reason}`: `Ankusa.Ordering` shares one decode between
  an ordering key and a `dedupe: json`); `:skip` decodes it here.
  """
  @spec key(t() | nil, Ankusa.Envelope.t(), {:ok, term()} | {:error, term()} | :skip) ::
          String.t() | nil
  def key(dedupe, env, decoded \\ :skip)
  def key(nil, _env, _decoded), do: nil

  def key(%__MODULE__{from: {:header, name}}, env, _decoded) do
    case Ankusa.Envelope.header(env, name) do
      nil -> nil
      "" -> nil
      value -> clamp(value)
    end
  end

  def key(%__MODULE__{from: {:json, path}}, env, :skip),
    do: key_from_json(path, JSON.decode(env.body))

  def key(%__MODULE__{from: {:json, path}}, _env, decoded), do: key_from_json(path, decoded)

  defp key_from_json(path, {:ok, decoded}) do
    case walk(decoded, path) do
      value when is_binary(value) and value != "" -> clamp(value)
      _ -> nil
    end
  end

  defp key_from_json(_path, {:error, _reason}), do: nil

  @doc false
  # The value at `path` (map keys only): a string, an integer as its decimal
  # string, or `nil`.
  @spec walk(term(), [String.t()]) :: String.t() | nil
  def walk(value, []) do
    case value do
      v when is_binary(v) -> v
      v when is_integer(v) -> Integer.to_string(v)
      _ -> nil
    end
  end

  def walk(value, [segment | rest]) when is_map(value) do
    case Map.fetch(value, segment) do
      {:ok, next} -> walk(next, rest)
      :error -> nil
    end
  end

  def walk(_value, [_segment | _rest]), do: nil

  defp clamp(key) do
    if byte_size(key) > @max_key_bytes do
      "sha256:" <> Base.encode16(:crypto.hash(:sha256, key), case: :lower)
    else
      key
    end
  end

  @doc """
  The ids, sorted, of the sources `config.source_store` declares (its `:sources`
  option, as `Ankusa.SourceStore.Static` reads it) that set no `dedupe`.

  Such a source stores a provider retry as a new hook with a new
  `idempotency_key`. A walk over the declared specs, no store read: the gauge
  `ankusa_sources_without_dedupe` reports its length. Sources a writable store
  holds are not counted: they are not known at boot.
  """
  @spec undeduped_sources(Ankusa.Config.t()) :: [String.t()]
  def undeduped_sources(%Ankusa.Config{source_store: {_mod, opts}}) do
    opts
    |> Keyword.get(:sources, %{})
    |> Enum.filter(fn {_id, spec} -> declared_dedupe(spec) == nil end)
    |> Enum.map(fn {id, _spec} -> id end)
    |> Enum.sort()
  end

  defp declared_dedupe(%Ankusa.Source{dedupe: dedupe}), do: dedupe
  defp declared_dedupe(spec), do: spec[:dedupe]

  @doc """
  Log one boot warning per source `undeduped_sources/1` names. Dedupe is opt-in,
  and a source without it is the one setting where a provider's retry is
  indistinguishable from a new event, so boot says so.
  """
  @spec warn_undeduped(Ankusa.Config.t()) :: :ok
  def warn_undeduped(%Ankusa.Config{} = config) do
    for id <- undeduped_sources(config) do
      Logger.warning(
        "[ankusa] source #{id} has no dedupe: a provider retry is stored as a new hook " <>
          "with a new idempotency_key. Set dedupe: to the provider's event id (a preset, " <>
          "a header or a json path); ankusa_sources_without_dedupe counts these sources."
      )
    end

    :ok
  end
end
