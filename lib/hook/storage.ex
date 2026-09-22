defmodule Hook.Storage do
  @moduledoc """
  Read side of long-term segment storage. `fetch/2` resolves an event id through
  the durable `Hook.Storage.Index`, range-reads exactly its frame from the blob
  store, and unframes it back into the original `Hook.Envelope`.
  """

  alias Hook.{Config, Envelope}
  alias Hook.Storage.Index

  @spec fetch(atom(), String.t()) :: {:ok, Envelope.t()} | :error
  def fetch(instance, event_id) do
    %Config{storage: %{codec: {codec, _}}} = config = Hook.config(instance)

    with {:ok, row} <- Index.lookup(config, event_id),
         {:ok, frame} <-
           Hook.BlobStore.get_range(instance, row.segment_key, row.offset, row.length),
         {:ok, payload} <- codec.decode_record(frame) do
      {:ok, Envelope.from_binary(payload)}
    else
      _ -> :error
    end
  end
end
