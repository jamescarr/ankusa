defmodule Ankusa.Storage do
  @moduledoc """
  Read side of long-term segment storage. `fetch/2` resolves an event id to its
  segment, range-reads exactly its frame from the blob store, and unframes it
  back into the original `Ankusa.Envelope`.

  A segment's catalogue row lives in the node's `Ankusa.Store`, with the range of
  event ids it holds; its index object (`seg/<first>-<last>.idx` in the blob
  store) maps each id to `{offset, length, seq}`. Event ids are time-ordered, so
  the id range narrows a lookup to a segment or two. Events archived by a 0.3
  node resolve through the locations imported from its index log.
  """

  alias Ankusa.{Config, Envelope}
  alias Ankusa.Queue.Archive

  @spec fetch(atom(), String.t()) :: {:ok, Envelope.t()} | :error
  def fetch(instance, event_id) do
    %Config{storage: %{codec: {codec, _}}} = Ankusa.config(instance)

    with {:ok, {segment_key, offset, length, seq}} <- locate(instance, event_id),
         {:ok, frame} <-
           Ankusa.BlobStore.get_range(instance, :segments, segment_key, offset, length),
         {:ok, payload} <- codec.decode_record(frame) do
      {:ok, %{Envelope.from_binary(payload) | seq: seq}}
    else
      _ -> :error
    end
  end

  defp locate(instance, event_id) do
    case Archive.legacy_location(instance, event_id) do
      {:ok, location} ->
        {:ok, location}

      :error ->
        with {:ok, rows} <- Archive.segments_containing(instance, event_id) do
          Enum.find_value(rows, :error, &locate_in(instance, &1, event_id))
        end
    end
  end

  defp locate_in(instance, %{key: key, idx_key: idx_key}, event_id) do
    with {:ok, bin} <- Ankusa.BlobStore.get(instance, :segments, idx_key),
         {:ok, {offset, length, seq}} <- Map.fetch(:erlang.binary_to_term(bin), event_id) do
      {:ok, {key, offset, length, seq}}
    else
      _ -> nil
    end
  end
end
