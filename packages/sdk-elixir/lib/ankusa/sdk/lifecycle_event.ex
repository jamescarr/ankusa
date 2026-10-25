defmodule Ankusa.SDK.LifecycleEvent do
  @moduledoc """
  A lifecycle event: the CloudEvents 1.0 (structured mode) event an Ankusa
  instance publishes when a source or a route is created, updated or deleted,
  when a tenant is erased, or when the instance is restored from a backup.

  It arrives as the body of a queue message from the instance's lifecycle sinks
  (content type `application/cloudevents+json`, `source_id` `ankusa:lifecycle`):
  decode the message with `Ankusa.SDK.Message.decode/1`, take `hook.body` from
  `Ankusa.SDK.Message.to_hook/2`, and hand it to `decode/1`.

      {:ok, event} = Ankusa.SDK.LifecycleEvent.decode(hook.body)
      event.type     #=> "io.ankusa.source.created"
      event.subject  #=> "acme.stripe"
      event.data     #=> the entity as the admin API shows it, secrets redacted

  Delivery is at least once and in commit order: `id` is derived from the
  change, so the same event always has the same id, and `revision` (per
  `subject`) lets a consumer ignore a stale update. `sequence` is the
  instance's audit sequence number as the decimal string it is on the wire.

  `decode/1` ignores keys it does not know and event types it does not know, so
  a consumer keeps working when a newer node announces more. Anything else wrong
  is an `Ankusa.SDK.InvalidMessageError` (never retryable) whose `code` is
  `"invalid_json"`, `"not_an_object"`, `"unsupported_version"` (the event's
  `specversion` is not `"1.0"`) or `"invalid_field"` with the offending `field`.
  """

  alias Ankusa.SDK.InvalidMessageError

  @type t :: %__MODULE__{
          specversion: String.t(),
          id: String.t(),
          source: String.t(),
          type: String.t(),
          subject: String.t(),
          time: String.t(),
          datacontenttype: String.t() | nil,
          data: map() | nil,
          revision: pos_integer() | nil,
          actor: String.t() | nil,
          instance: String.t() | nil,
          sequence: String.t() | nil,
          replayid: String.t() | nil,
          traceparent: String.t() | nil,
          tracestate: String.t() | nil
        }

  defstruct [
    :specversion,
    :id,
    :source,
    :type,
    :subject,
    :time,
    :datacontenttype,
    :data,
    :revision,
    :actor,
    :instance,
    :sequence,
    :replayid,
    :traceparent,
    :tracestate
  ]

  @required ~w(id source type subject time)
  @sequence ~r/\A[0-9]+\z/

  @doc """
  Decode one lifecycle event (the JSON of the CloudEvent).

  The checks run in a fixed order and the first failure wins.
  """
  @spec decode(binary()) :: {:ok, t()} | {:error, InvalidMessageError.t()}
  def decode(data) when is_binary(data) do
    with {:ok, value} <- decode_json(data),
         {:ok, raw} <- object(value),
         :ok <- specversion(raw),
         {:ok, [id, source, type, subject, time]} <- required(raw),
         {:ok, datacontenttype} <- optional_string(raw, "datacontenttype"),
         {:ok, body} <- data(raw),
         {:ok, revision} <- revision(raw),
         {:ok, actor} <- optional_string(raw, "actor"),
         {:ok, instance} <- optional_string(raw, "instance"),
         {:ok, sequence} <- sequence(raw),
         {:ok, replayid} <- optional_string(raw, "replayid"),
         {:ok, traceparent} <- optional_string(raw, "traceparent"),
         {:ok, tracestate} <- optional_string(raw, "tracestate") do
      {:ok,
       %__MODULE__{
         specversion: "1.0",
         id: id,
         source: source,
         type: type,
         subject: subject,
         time: time,
         datacontenttype: datacontenttype,
         data: body,
         revision: revision,
         actor: actor,
         instance: instance,
         sequence: sequence,
         replayid: replayid,
         traceparent: traceparent,
         tracestate: tracestate
       }}
    end
  end

  defp decode_json(data) do
    case JSON.decode(data) do
      {:ok, value} -> {:ok, value}
      {:error, _reason} -> {:error, invalid(:invalid_json)}
    end
  end

  defp object(value) when is_map(value), do: {:ok, value}
  defp object(_value), do: {:error, invalid(:not_an_object)}

  defp specversion(%{"specversion" => "1.0"}), do: :ok
  defp specversion(_raw), do: {:error, invalid(:unsupported_version)}

  defp required(raw) do
    Enum.reduce_while(@required, {:ok, []}, fn key, {:ok, acc} ->
      case raw do
        %{^key => value} when is_binary(value) and value != "" -> {:cont, {:ok, acc ++ [value]}}
        _other -> {:halt, {:error, invalid_field(key)}}
      end
    end)
  end

  defp optional_string(raw, key) do
    case Map.fetch(raw, key) do
      :error -> {:ok, nil}
      {:ok, value} when is_binary(value) or is_nil(value) -> {:ok, value}
      {:ok, _other} -> {:error, invalid_field(key)}
    end
  end

  defp data(raw) do
    case Map.fetch(raw, "data") do
      :error -> {:ok, nil}
      {:ok, value} when is_map(value) or is_nil(value) -> {:ok, value}
      {:ok, _other} -> {:error, invalid_field("data")}
    end
  end

  defp revision(raw) do
    case Map.fetch(raw, "revision") do
      :error -> {:ok, nil}
      {:ok, nil} -> {:ok, nil}
      {:ok, value} when is_integer(value) and value >= 1 -> {:ok, value}
      {:ok, _other} -> {:error, invalid_field("revision")}
    end
  end

  defp sequence(raw) do
    case Map.fetch(raw, "sequence") do
      :error ->
        {:ok, nil}

      {:ok, nil} ->
        {:ok, nil}

      {:ok, value} when is_binary(value) ->
        if Regex.match?(@sequence, value),
          do: {:ok, value},
          else: {:error, invalid_field("sequence")}

      {:ok, _other} ->
        {:error, invalid_field("sequence")}
    end
  end

  defp invalid_field(key), do: invalid({:invalid_field, key})

  defp invalid(reason) do
    {code, field} = classify(reason)

    %InvalidMessageError{
      message: "invalid lifecycle event: #{inspect(reason)}",
      reason: reason,
      code: code,
      field: field
    }
  end

  defp classify(:invalid_json), do: {"invalid_json", nil}
  defp classify(:not_an_object), do: {"not_an_object", nil}
  defp classify(:unsupported_version), do: {"unsupported_version", nil}
  defp classify({:invalid_field, key}), do: {"invalid_field", key}
end
