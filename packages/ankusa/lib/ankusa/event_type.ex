defmodule Ankusa.EventType do
  @max_value_bytes 128
  @max_segments 16

  @moduledoc """
  Per-source provider event names.

  A provider names what happened (`push`, `invoice.paid`) in a request header
  (GitHub's `x-github-event`, Shopify's `x-shopify-topic`) or a JSON field of the
  body (Stripe's `"type"`). `extract/3` reads that name at ingest, from the
  complete header list or the body the request already decoded for
  `Ankusa.Dedupe` and `Ankusa.Ordering` (`Ankusa.Routing`), and the hook is
  stored with it (`Ankusa.Envelope.event_type`) and carried to every consumer.

  ## Sources

    * `{:header, name}`: the value of a request header (case-insensitive).
    * `{:json, ["a", "b"]}`: walk `a` → `b` through the decoded JSON body; only
      map keys are followed, and only a non-empty string is a name (an integer, a
      map, a list or a missing key is no event type). A body past
      `dedupe.max_json_bytes` (1 MiB by default) is not decoded: no event type.

  An event type is a name, never hashed: a value over #{@max_value_bytes} bytes is no event
  type. A source with an `event_type` and a hook that carries none is counted in
  `ankusa_ingest_event_type_missing_total` (`Ankusa.Routing`).
  """

  alias Ankusa.Envelope

  @enforce_keys [:from]
  defstruct [:from]

  @type from :: {:header, String.t()} | {:json, [String.t()]}
  @type t :: %__MODULE__{from: from()}

  @typedoc "The body's JSON as `Ankusa.Routing` decoded it, or `:skip` when it was not."
  @type decoded :: {:ok, term()} | {:error, term()} | :skip

  @presets %{
    github: {:header, "x-github-event"},
    shopify: {:header, "x-shopify-topic"},
    stripe: {:json, ["type"]},
    standard_webhooks: {:json, ["type"]},
    svix: {:json, ["type"]},
    slack: {:json, ["event", "type"]}
  }

  @doc "The provider presets, name to source."
  @spec presets() :: %{atom() => from()}
  def presets, do: @presets

  @doc """
  Build an event type spec for `source_id` from config. Accepts:

    * `nil`: no event type.
    * a preset atom (`#{Enum.map_join(Map.keys(@presets), "`, `", &inspect/1)}`).
    * a map or keyword list with exactly one of `:preset` (atom), `:header`
      (non-empty string of at most #{@max_value_bytes} bytes, stored ASCII-lowercased) or
      `:json` (a dot path of 1 to #{@max_segments} non-empty segments).

  Anything else raises `ArgumentError` naming the source.
  """
  @spec new!(nil | atom() | keyword() | map(), String.t() | nil) :: t() | nil
  def new!(term, source_id \\ nil)
  def new!(nil, _source_id), do: nil

  def new!(preset, source_id) when is_atom(preset),
    do: new!(%{preset: preset}, source_id)

  def new!(opts, source_id) when is_list(opts) or is_map(opts) do
    label = "source #{inspect(source_id)}: event_type"

    unless is_map(opts) or Keyword.keyword?(opts) do
      raise ArgumentError, "#{label}: expected a preset atom or a map, got: #{inspect(opts)}"
    end

    opts = Map.new(opts)

    from =
      case {Map.get(opts, :preset), Map.get(opts, :header), Map.get(opts, :json)} do
        {nil, nil, nil} ->
          raise ArgumentError, "#{label}: set exactly one of preset, header, json (got none)"

        {preset, nil, nil} when is_atom(preset) ->
          case Map.fetch(@presets, preset) do
            {:ok, from} -> from
            :error -> raise ArgumentError, "#{label}: unknown preset #{inspect(preset)}"
          end

        {nil, header, nil} ->
          {:header, header!(label, header)}

        {nil, nil, json} ->
          {:json, json!(label, json)}

        _other ->
          raise ArgumentError, "#{label}: set exactly one of preset, header, json"
      end

    %__MODULE__{from: from}
  end

  def new!(other, source_id) do
    raise ArgumentError,
          "source #{inspect(source_id)}: event_type: expected a preset atom or a map, " <>
            "got: #{inspect(other)}"
  end

  defp header!(_label, header)
       when is_binary(header) and header != "" and byte_size(header) <= @max_value_bytes,
       do: String.downcase(header, :ascii)

  defp header!(label, header) do
    raise ArgumentError,
          "#{label}: header must be a non-empty string of at most #{@max_value_bytes} bytes, " <>
            "got: #{inspect(header)}"
  end

  defp json!(label, json) when is_binary(json) do
    segments = String.split(json, ".")

    if Enum.all?(segments, &(&1 != "")) and length(segments) <= @max_segments,
      do: segments,
      else:
        raise(
          ArgumentError,
          "#{label}: json path must be 1 to #{@max_segments} non-empty dot-separated " <>
            "segments, got: #{inspect(json)}"
        )
  end

  defp json!(label, json) do
    raise ArgumentError, "#{label}: json must be a dot path string, got: #{inspect(json)}"
  end

  @doc "True when reading the event type needs the decoded body."
  @spec needs_json?(t() | nil) :: boolean()
  def needs_json?(%__MODULE__{from: {:json, _path}}), do: true
  def needs_json?(_other), do: false

  @doc """
  The event type of `env` for `spec`, or `nil`: no spec, no such header, no
  string at the path, a body that did not decode (`decoded` is `:skip` for one
  past the cap), or a value over #{@max_value_bytes} bytes.

  `decoded` is the body's JSON when the caller has decoded it already.
  """
  @spec extract(t() | nil, Envelope.t(), decoded()) :: String.t() | nil
  def extract(nil, _env, _decoded), do: nil

  def extract(%__MODULE__{from: {:header, name}}, env, _decoded),
    do: name_or_nil(Envelope.header_downcased(env, name))

  def extract(%__MODULE__{from: {:json, path}}, _env, {:ok, decoded}),
    do: name_or_nil(walk(decoded, path))

  def extract(%__MODULE__{from: {:json, _path}}, _env, _decoded), do: nil

  defp name_or_nil(value)
       when is_binary(value) and value != "" and byte_size(value) <= @max_value_bytes,
       do: value

  defp name_or_nil(_value), do: nil

  # Map keys only; whatever sits at the end of the path is judged by
  # `name_or_nil/1`, so an integer is no name (`Ankusa.Dedupe.walk/2` would
  # render it as a key).
  defp walk(value, []), do: value

  defp walk(value, [segment | rest]) when is_map(value) do
    case Map.fetch(value, segment) do
      {:ok, next} -> walk(next, rest)
      :error -> nil
    end
  end

  defp walk(_value, [_segment | _rest]), do: nil
end
