defmodule Ankusa.Routing do
  @moduledoc """
  What a hook's content decides, resolved once at ingest: the provider event
  key (`Ankusa.Dedupe`), the event type (`Ankusa.EventType`) and the ordering
  lanes (`Ankusa.Ordering`).

  All three read the complete request header list, and the three of them share
  one JSON decode of the body: it happens at most once per hook, only when a
  source setting needs it, and only for a body of at most
  `dedupe.max_json_bytes` (1 MiB by default). A larger body is never decoded on
  the request process.

  Every other path that makes delivery rows from a stored hook (a released
  quarantine hook, an archive replay, boot recovery of the intake log) resolves
  from the stored envelope by the same functions.

  A source with an `event_type` whose hook carries none emits
  `[:ankusa, :ingest, :event_type_missing]` (`:instance`, `:source_id`,
  `:tenant_id`), counted in `ankusa_ingest_event_type_missing_total`.
  """

  alias Ankusa.{Dedupe, Envelope, EventType, Ordering, Source}

  # `selected` is the indexes of the sinks that get a delivery row, or `:all`.
  defstruct dedupe_key: nil, event_type: nil, lanes: nil, selected: :all

  @type t :: %__MODULE__{
          dedupe_key: String.t() | nil,
          event_type: String.t() | nil,
          lanes: [Ordering.lane() | nil] | nil,
          selected: :all | [non_neg_integer()]
        }

  @doc """
  Resolve `env` for `source`. `dedupe: true` extracts the provider event key;
  `dedupe: false` leaves `dedupe_key` `nil`, for a hook that must not claim one
  (a flag-accepted forgery, an archived hook that is replayed).
  """
  @spec resolve(atom(), Source.t(), Envelope.t(), dedupe: boolean()) :: t()
  def resolve(instance, %Source{} = source, %Envelope{} = env, opts) do
    dedupe? = Keyword.fetch!(opts, :dedupe)
    max_json_bytes = Dedupe.max_json_bytes(instance)
    decoded = decode(source, env, max_json_bytes, dedupe?)
    event_type = EventType.extract(source.event_type, env, decoded)

    if source.event_type != nil and event_type == nil do
      Ankusa.Telemetry.emit([:ingest, :event_type_missing], %{}, %{
        instance: instance,
        source_id: env.source_id,
        tenant_id: env.tenant_id
      })
    end

    %__MODULE__{
      dedupe_key: if(dedupe?, do: Dedupe.key(source.dedupe, env, decoded, max_json_bytes)),
      event_type: event_type,
      lanes: Ordering.lanes(instance, source, env, decoded),
      selected: :all
    }
  end

  @doc """
  The body of `env` decoded as JSON for everything `source` reads from it, or
  `:skip`: nothing reads it, or it is over `max_json_bytes`. The decode is
  `{:ok, term}` or `{:error, reason}`, as `Ankusa.Dedupe.key/4`,
  `Ankusa.EventType.extract/3` and `Ankusa.Ordering.lanes/4` take it.
  """
  @spec decode(Source.t(), Envelope.t(), pos_integer()) ::
          {:ok, term()} | {:error, term()} | :skip
  def decode(%Source{} = source, %Envelope{} = env, max_json_bytes),
    do: decode(source, env, max_json_bytes, true)

  defp decode(%Source{} = source, %Envelope{body: body}, max_json_bytes, dedupe?) do
    if needs_json?(source, dedupe?) and byte_size(body) <= max_json_bytes,
      do: JSON.decode(body),
      else: :skip
  end

  defp needs_json?(%Source{dedupe: dedupe, ordering: ordering, event_type: event_type}, dedupe?) do
    (dedupe? and match?(%Dedupe{from: {:json, _}}, dedupe)) or
      EventType.needs_json?(event_type) or
      (ordering != nil and Enum.any?(ordering, &match?({:ordered, {:json, _}}, &1)))
  end
end
