defmodule Ankusa.Dispatch.Receiver do
  @moduledoc """
  The idempotent receiver: the stage in front of dispatch that decides whether a
  record is the first copy of its event.

  One receiver per partition, and one consumer per partition. Every copy of an
  event has to land in the same partition, or two consumers would each see only
  part of the copies and both would deliver. The partition is
  `hash(tenant_id, source_id)` — the dedup *scope*, not the key — so a partition
  holds whole scopes, and whichever consumer owns a partition sees every copy of
  every event in it, whatever keys those events have.

  Delivery is the default: a record is dropped only when the store holds a
  strictly earlier copy of the same event inside the expiry window. The rule
  itself is `Ankusa.DedupStore.decide/4`; what this module adds is the scope, the
  key, and the partition.
  """

  alias Ankusa.{DedupStore, Envelope, Source}

  defstruct [:partition, :store, :ttl_ms]

  @type t :: %__MODULE__{
          partition: non_neg_integer(),
          store: DedupStore.store(),
          ttl_ms: pos_integer()
        }

  @doc """
  Open the receiver for one partition.

  The store is opened here, so its lifetime is the receiver's: an in-process
  ledger dies with the process that consumes the partition.
  """
  @spec new(non_neg_integer(), keyword()) :: t()
  def new(partition, opts) do
    %__MODULE__{
      partition: partition,
      store: DedupStore.open(Keyword.fetch!(opts, :store)),
      ttl_ms: Keyword.fetch!(opts, :ttl_ms)
    }
  end

  @doc """
  Which partition an event belongs to.

  Hashes the scope rather than the key so that every copy of an event — all of
  which share `(tenant_id, source_id)` with their siblings — is placed together,
  and so that a partition never holds half of an event's copies.
  """
  @spec partition(DedupStore.scope(), pos_integer()) :: non_neg_integer()
  def partition({tenant_id, source_id}, partitions) do
    :erlang.phash2({tenant_id, source_id}, partitions)
  end

  @doc "The dedup scope of an envelope."
  @spec scope(Envelope.t()) :: DedupStore.scope()
  def scope(%Envelope{tenant_id: tenant_id, source_id: source_id}), do: {tenant_id, source_id}

  @doc """
  Has this event already been delivered?

  Records the copy either way: the first copy to arrive is the one remembered as
  the event's first commit, which is what later copies are compared against.
  """
  @spec duplicate?(t(), Source.t(), Envelope.t()) :: boolean()
  def duplicate?(%__MODULE__{} = receiver, %Source{} = source, %Envelope{} = env) do
    case key(source, env) do
      nil ->
        false

      key ->
        case DedupStore.record(
               receiver.store,
               scope(env),
               key,
               env.seq,
               env.committed_at,
               receiver.ttl_ms
             ) do
          :deliver -> false
          :drop -> true
        end
    end
  end

  @doc """
  The event's dedup key, or `nil` when this source does not dedup.

  `dedup: :none` delivers every copy. Otherwise the source's `DedupKey` module
  extracts the provider's event id from the record; a record the module cannot
  key (no rule matched) is delivered too — boot has already warned about a
  source configured that way.
  """
  @spec key(Source.t(), Envelope.t()) :: String.t() | nil
  def key(%Source{dedup: :none}, _env), do: nil

  def key(%Source{dedup_key: {mod, opts}}, env) do
    case mod.extract(env, opts) do
      {:ok, key} when is_binary(key) -> key
      _ -> nil
    end
  end

  def key(%Source{}, _env), do: nil
end
