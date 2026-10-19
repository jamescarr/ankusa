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

  A hook of a tenant that was erased (`Ankusa.Tenant`) after the hook was
  received is `{:error, :gone}` (`410` where it is served): the segment still
  holds its frame until the bucket's lifecycle expires it, and the tombstone is
  what keeps it from being read in the meantime.
  """

  alias Ankusa.{Config, Envelope, Tenant}
  alias Ankusa.Queue.Archive

  @spec fetch(atom(), String.t()) :: {:ok, Envelope.t()} | :error | {:error, :gone}
  def fetch(instance, event_id) do
    %Config{storage: %{codec: {codec, _}}} = Ankusa.config(instance)

    with {:ok, {segment_key, offset, length, seq}} <- locate(instance, event_id),
         {:ok, frame} <-
           Ankusa.BlobStore.get_range(instance, :segments, segment_key, offset, length),
         {:ok, payload} <- codec.decode_record(frame),
         {:ok, env} <- Envelope.from_binary(payload) do
      served(instance, %{env | seq: seq})
    else
      _ -> :error
    end
  end

  # A tombstone that cannot be read is not "not erased": the answer is `:error`.
  defp served(instance, env) do
    case Tenant.erased_at(instance, env.tenant_id || "default") do
      {:ok, at} when env.received_at <= at -> {:error, :gone}
      {:error, _reason} -> :error
      _not_erased_or_later -> {:ok, env}
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

  @doc """
  The index object of a segment (`%{event_id => {offset, length, seq}}`), as the
  catalogue row `row` names it: one object read, no frame. `:error` when the
  object is missing or unreadable.
  """
  @spec segment_index(atom(), map()) :: {:ok, map()} | :error
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  def segment_index(instance, %{idx_key: idx_key}) do
    case Ankusa.BlobStore.get(instance, :segments, idx_key) do
      {:ok, bin} -> {:ok, :erlang.binary_to_term(bin)}
      _ -> :error
    end
  end

  defp locate_in(instance, %{key: key} = row, event_id) do
    with {:ok, index} <- segment_index(instance, row),
         {:ok, {offset, length, seq}} <- Map.fetch(index, event_id) do
      {:ok, {key, offset, length, seq}}
    else
      _ -> nil
    end
  end
end
