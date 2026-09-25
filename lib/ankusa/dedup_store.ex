defmodule Ankusa.DedupStore do
  @moduledoc """
  Where the idempotent receiver remembers the events it has already delivered.

  Dedup lives in front of dispatch, not on the ack path: the WAL is a durable,
  ordered, append-only log with no uniqueness constraint, the edge acks every
  copy it committed, and this is what stops the second copy of a provider's
  retry from reaching a sink.

  ## What a store holds

  `dedup_key -> {first_seq, committed_at}` — the *first* sequence this event was
  committed at, and when that record was committed. Not a seen flag, because a
  flag cannot tell a duplicate from a re-read:

  * A copy is dropped only when a **strictly earlier** copy was already
    delivered: `first_seq < seq`.
  * A re-read of the same record after a crash has `first_seq == seq` and is
    **delivered**, not dropped. The dispatcher resumes from its durable cursor,
    so a crash re-reads records; dropping those would lose them.
  * `first_seq` is kept as the minimum and `committed_at` as the maximum, so the
    decision does not depend on the order copies arrive in beyond the
    `first_seq < seq` comparison itself.

  ## Expiry is measured in the records' own timeline

  A key ages out when the copies behind it are older than `ttl_ms` *measured
  between the two records' commit timestamps* — never against the wall clock at
  read time. A dispatcher that has fallen hours behind therefore still dedupes
  correctly: it compares record to record, not record to now.

  A key that is never seen again stays in the store; an implementation may sweep
  those, and a sweep may only drop entries the rule above would ignore anyway.
  That is why a sweep is allowed to use the wall clock while a *decision* is
  not: dropping too much can re-deliver a record (at-least-once, which the
  pipeline already guarantees), and can never drop one.

  ## Implementations

  * `Ankusa.DedupStore.ETS` — the in-process default, one table per receiver.
  * `Ankusa.DedupStore.Ra` — the ledger in replicated state, so two dispatchers
    that fail over to each other share what they have seen.

  The decision itself is `decide/4`, shared by every implementation so the rule
  is written once.
  """

  @typedoc "A store handle. Opaque to callers; each implementation defines its own."
  @type store :: term()

  @typedoc "What a key is known by: the dedup scope, without the key itself."
  @type scope :: {tenant_id :: String.t(), source_id :: String.t()}

  @typedoc "The first commit of an event, and when that commit happened."
  @type entry :: %{first_seq: pos_integer(), committed_at: integer()}

  @doc """
  Record this copy of an event and answer whether it should be delivered.

  One call rather than a fetch and a put, so a store can make the decision
  atomically: a Raft-backed store must not need two round trips per record.
  """
  @callback record(
              store(),
              scope(),
              key :: String.t(),
              seq :: pos_integer(),
              committed_at :: integer(),
              ttl_ms :: pos_integer()
            ) :: :deliver | :drop

  @doc """
  Record a copy in `store`, dispatching to its implementation.

  A store is a handle (`%Ankusa.DedupStore.ETS{}`), not a `{module, opts}` pair:
  the store is opened once by the receiver that owns it, so the call only has to
  find the implementation behind the handle.
  """
  @spec record(store(), scope(), String.t(), pos_integer(), integer(), pos_integer()) ::
          :deliver | :drop
  def record(%mod{} = store, scope, key, seq, committed_at, ttl_ms) do
    mod.record(store, scope, key, seq, committed_at, ttl_ms)
  end

  @doc """
  The rule every store applies, in one place.

  Returns the decision and the entry to keep. `stored` is `:error` when the key
  is not in the store.
  """
  @spec decide(entry() | :error, pos_integer(), integer(), pos_integer()) ::
          {:deliver | :drop, entry()}
  def decide(:error, seq, committed_at, _ttl_ms) do
    {:deliver, %{first_seq: seq, committed_at: committed_at}}
  end

  def decide(%{first_seq: first, committed_at: at}, seq, committed_at, ttl_ms) do
    cond do
      # The copies behind this key are older than the window, measured between
      # them and this record — so this record is not their duplicate.
      at + ttl_ms < committed_at ->
        {:deliver, %{first_seq: seq, committed_at: committed_at}}

      # A strictly earlier copy has already gone through.
      first < seq ->
        {:drop, %{first_seq: first, committed_at: max(at, committed_at)}}

      # This *is* the earliest copy we have seen: a re-read of the record that
      # went through (`first == seq`), or one arriving for the first time after
      # a rewind. Deliver it, and remember it as the earliest.
      true ->
        {:deliver, %{first_seq: min(first, seq), committed_at: max(at, committed_at)}}
    end
  end

  @doc """
  Open the store named by `config.dispatch.dedup_store`.

  Called by each receiver as it starts, so a store's lifetime follows the
  process that consumes the partition.
  """
  @spec open({module(), keyword()}) :: store()
  def open({mod, opts}), do: mod.new(opts)
end

defmodule Ankusa.DedupStore.ETS do
  @moduledoc """
  In-process dedup: one ETS table, owned by whichever process opened it.

  That owner is the receiver for a partition, so the table dies with it and a
  restarted dispatcher starts with an empty ledger. Losing it can only cause a
  re-delivery — never a lost record — which is why the in-process default is
  safe: at-least-once is the pipeline's promise and the sink is idempotent per
  record id.

  A key that is never seen again would otherwise sit in the table forever, so
  the table is swept once it has grown by `:sweep_delta` entries since the last
  sweep. The sweep drops only entries whose commit time is already outside the
  window, which the decision rule would ignore anyway: it can re-deliver, never
  mis-drop.
  """

  @behaviour Ankusa.DedupStore

  alias Ankusa.DedupStore

  defstruct [:table, :sweep_delta]

  @type t :: %__MODULE__{table: :ets.tid(), sweep_delta: pos_integer()}

  @sweep_delta 10_000

  @doc """
  `:sweep_delta` (default #{@sweep_delta}) is how many entries may accumulate
  before expired ones are swept.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    table = :ets.new(:ankusa_dedup, [:set, :private])
    %__MODULE__{table: table, sweep_delta: Keyword.get(opts, :sweep_delta, @sweep_delta)}
  end

  @impl true
  def record(%__MODULE__{} = store, scope, key, seq, committed_at, ttl_ms) do
    %{table: table} = store

    stored =
      case :ets.lookup(table, {scope, key}) do
        [{_, entry}] -> entry
        [] -> :error
      end

    {decision, entry} = DedupStore.decide(stored, seq, committed_at, ttl_ms)
    :ets.insert(table, {{scope, key}, entry})
    maybe_sweep(store, committed_at, ttl_ms)

    decision
  end

  # `:ets.info/2` is O(1), so this costs one call per record and the sweep
  # itself is proportional to the growth since the last one. The high-water
  # marker lives in the process dictionary keyed by the table, so two stores in
  # one process (two partitions in one pipeline) do not share a counter.
  defp maybe_sweep(%{table: table, sweep_delta: delta}, committed_at, ttl_ms) do
    size = :ets.info(table, :size)

    if size - Process.get({__MODULE__, table}, 0) >= delta do
      Process.put({__MODULE__, table}, size)

      :ets.select_delete(table, [
        {{{:"$1", :"$2"}, :"$3"},
         [{:is_map, :"$3"}, {:<, {:map_get, :committed_at, :"$3"}, committed_at - ttl_ms}],
         [true]}
      ])
    end
  end
end
