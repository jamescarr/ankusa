defmodule Ankusa.DedupStore.Ra do
  @moduledoc """
  `Ankusa.DedupStore` in replicated state: the receiver's ledger kept by a
  `Ankusa.WAL.Ra` cluster instead of in the dispatcher's own memory.

  ## When this, and not the default

  `Ankusa.DedupStore.ETS` is per-dispatcher: when a partition moves — a
  failover, a rolling restart, a machine that came back — the new owner starts
  with an empty ledger, and so delivers copies the previous owner had already
  delivered. That is at-least-once, never a loss, and for a single-node
  deployment it is the right trade: an ETS lookup is free next to a consensus
  round.

  This store is for the case that trade does not cover: a fleet where the same
  provider retry can meet two different dispatchers. The ledger then lives in
  the Raft state of the cluster that already holds the WAL, so whichever node is
  dispatching reads what the previous owner had already seen, and "one delivery
  per event" survives the handover.

      wal: {Ankusa.WAL.Ra, members: [{:"ankusa_wal_default", :"ankusa@wal-0"}, ...]},
      dispatch: %{
        dedup_store: {Ankusa.DedupStore.Ra, members: [{:"ankusa_wal_default", :"ankusa@wal-0"}, ...]}
      }

  `:members` must be the **WAL cluster's** members: the command is applied by
  `Ankusa.WAL.Ra.Machine`, so the ledger is that cluster's state and needs no
  cluster of its own. A deployment on `WAL.DiskLog` has no such cluster and
  cannot use this store. `:timeout_ms` (default `#{5_000}`) bounds how long one
  decision may retry before the store gives up on the cluster.

  ## Cost

  One consensus round trip per key-bearing record — per record, not per batch,
  because a ledger decision is the *input* to the next decision, so the
  commands cannot be reordered. A dispatcher on this store is therefore
  throughput-bound by its cluster's round-trip latency. That is the price of the
  ledger surviving the dispatcher; anything that can live with per-dispatcher
  memory should use the default.

  ## A cluster that cannot answer says so

  This is the one store that can be *unavailable*: an in-process ledger always
  answers. When the cluster cannot be reached inside `:timeout_ms`, `record/6`
  returns `{:error, reason}` and dispatch leaves the record undecided — it stops
  reading there and retries from that seq — rather than picking an answer.
  Delivering without recording would be worse than a delay: the ledger would
  have no entry for that copy, so the *next* copy of the same event would look
  like a first one, and the guarantee would be gone for that event rather than
  late for one record. What a stalled dispatcher costs is throughput; what a
  guessed answer costs is correctness.
  """

  @behaviour Ankusa.DedupStore

  alias Ankusa.WAL.Ra

  defstruct [:members, :timeout_ms]

  @type t :: %__MODULE__{members: [{atom(), node()}], timeout_ms: pos_integer()}

  @default_timeout_ms 5_000

  @doc """
  `:members` (required) is the WAL cluster's member list; `:timeout_ms` bounds
  one decision's retrying.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      members: validate_members!(Keyword.get(opts, :members, [])),
      timeout_ms: Keyword.get(opts, :timeout_ms, @default_timeout_ms)
    }
  end

  @impl Ankusa.DedupStore
  def record(%__MODULE__{} = store, {tenant, source}, key, seq, committed_at, ttl_ms) do
    command = {:dedup_record, tenant, source, key, seq, committed_at, ttl_ms}

    case Ra.remote_command(store.members, command, timeout: store.timeout_ms) do
      {:ok, :deliver} ->
        :deliver

      {:ok, :drop} ->
        :drop

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, {:unexpected_reply, other}}
    end
  end

  defp validate_members!([]) do
    raise ArgumentError,
          "Ankusa.DedupStore.Ra needs :members — the non-empty {cluster, node} list " <>
            "of the Ankusa.WAL.Ra cluster that holds the ledger"
  end

  defp validate_members!(members) when is_list(members) do
    Enum.each(members, fn
      {cluster, node} when is_atom(cluster) and is_atom(node) ->
        :ok

      other ->
        raise ArgumentError,
              "Ankusa.DedupStore.Ra :members must be {cluster, node} tuples, got #{inspect(other)}"
    end)

    members
  end

  defp validate_members!(other) do
    raise ArgumentError,
          "Ankusa.DedupStore.Ra :members must be a list of {cluster, node} tuples, " <>
            "got #{inspect(other)}"
  end
end
