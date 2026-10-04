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
  """

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
    stripe: {:json, ["id"]}
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
  """
  @spec key(t() | nil, Ankusa.Envelope.t()) :: String.t() | nil
  def key(nil, _env), do: nil

  def key(%__MODULE__{from: {:header, name}}, env) do
    case Ankusa.Envelope.header(env, name) do
      nil -> nil
      "" -> nil
      value -> clamp(value)
    end
  end

  def key(%__MODULE__{from: {:json, path}}, env) do
    case JSON.decode(env.body) do
      {:ok, decoded} ->
        case walk(decoded, path) do
          value when is_binary(value) and value != "" -> clamp(value)
          _ -> nil
        end

      {:error, _reason} ->
        nil
    end
  end

  defp walk(value, []) do
    case value do
      v when is_binary(v) -> v
      v when is_integer(v) -> Integer.to_string(v)
      _ -> nil
    end
  end

  defp walk(value, [segment | rest]) when is_map(value) do
    case Map.fetch(value, segment) do
      {:ok, next} -> walk(next, rest)
      :error -> nil
    end
  end

  defp walk(_value, [_segment | _rest]), do: nil

  defp clamp(key) do
    if byte_size(key) > @max_key_bytes do
      "sha256:" <> Base.encode16(:crypto.hash(:sha256, key), case: :lower)
    else
      key
    end
  end
end
