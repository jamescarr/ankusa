defmodule Ankusa.Edge.Batcher do
  @moduledoc """
  The admission gate of one batcher partition, and the caller's way into the
  queue writer. A request calls `commit/4` **from its own process**: the call
  charges its record against the partition's bound, hands the hook to the queue
  writer (`Ankusa.Queue.enqueue/3`) and blocks on the reply. The writer merges
  the calls of every partition, whatever process made them, into one group
  commit (one fsync), and every blocked caller is replied to *after* the
  commit is durable — that is what makes the ack honest.

  There is no process between the request and the writer: the hops a hook
  makes are the request process, the writer and the syncer (`Ankusa.Queue.Syncer`).
  The partition's process only owns the counters and registers them; the work a
  batcher process used to do per hook (buffer the record, start a supervised
  task, answer the caller from the task's result) cost about as much CPU as the
  writer's own work at 10 KB bodies.

  The bound counts the records the partition holds: from the moment `commit/4`
  admits one until it returns. When the count reaches `batcher.max_queue`, or
  the bodies held would pass `batcher.max_queue_bytes`, the gate sheds load: the
  caller gets `{:error, :overload}`, which the edge turns into a `503` with
  `Retry-After`. Never ack what you haven't saved. A record bigger than the byte
  bound on its own is still admitted into an empty partition: `max_body_bytes`
  is the per-record limit, this one bounds how much a partition holds.

  The bound is the partition's, so on its own one tenant could fill it and every
  other tenant would be shed with it. A tenant whose limit sets `max_queue` or
  `max_queue_bytes` (`Ankusa.Edge.RateLimiter`) is counted by `commit/4`
  across this node's partitions before the call reaches a partition, and is
  refused `{:error, {:tenant_overload, retry_after_ms}}` (a `429`) past its
  share: the partition is never asked, and the global `503` is left to the
  tenants still within theirs.

  ## Deadlines

  Every record carries a deadline: the latest moment its batch may *start*
  committing. The writer refuses a call whose deadline passed while it queued
  behind other work (`Ankusa.Queue.enqueue/3`), and the caller is answered
  `{:error, :store_unavailable}`: nothing is written and no seq is consumed. A
  batch the writer has started is never abandoned: its callers wait for the
  commit's outcome, however long the disk takes. A stall therefore never turns
  into a `503` for a hook that is then committed. The one way a `503` can still
  cover a durable hook is a process dying while the writer is mid-commit (the
  writer crashing after the sync but before it replies, or the caller killed:
  its own provider connection is gone then); the provider's retry stores the
  hook a second time.
  """

  use GenServer

  require Logger

  alias Ankusa.Edge.RateLimiter
  alias Ankusa.Queue

  @count 1
  @bytes 2

  # ── api ───────────────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    partition = Keyword.fetch!(opts, :partition)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, {:batcher, partition}))
  end

  def child_spec(opts) do
    partition = Keyword.fetch!(opts, :partition)

    %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance), partition},
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  # The latest a record may wait before its batch starts committing (see
  # "Deadlines" in the moduledoc). `Ankusa.Listener.ingress_shutdown_timeout_ms/1`
  # and `Ankusa.Config` size the shutdown and the intake attempt from it.
  @record_deadline_ms 15_000

  @doc "The default record deadline in milliseconds: the latest a record may wait before its batch starts committing."
  @spec record_deadline_ms() :: pos_integer()
  def record_deadline_ms, do: @record_deadline_ms

  @doc """
  How many records the partition holds. A drain waits for every partition to
  report `0` (`Ankusa.Instance.Drain`): each of those records belongs to a
  request that has not been answered yet. Exits `:noproc` when the partition is
  not running.
  """
  @spec queued(atom(), non_neg_integer()) :: non_neg_integer()
  def queued(instance, partition) do
    {counters, _max_queue, _max_bytes} = gate!(instance, partition)
    :atomics.get(counters, @count)
  end

  @doc """
  Submit a record and block until it is durably committed (or shed).

  Returns `{:committed, env}` | `{:duplicate, env}` | `{:error, :overload}` |
  `{:error, {:tenant_overload, retry_after_ms}}` |
  `{:error, :tenant_limit}` (the hook's tenant would pass `tenants.max`) |
  `{:error, :tenant_erasing}` (an erase of the hook's tenant is running) |
  `{:error, :dedupe_quota}` (its dedupe key would pass `dedupe.max_entries` or
  `dedupe.max_entries_per_tenant`) |
  `{:error, :store_unavailable}`; under `wal: :dual` also
  `{:error, :intake_unavailable}` (the intake breaker is open or its pending set is
  full: nothing was stored), `{:error, :intake_pending}` (a retry that met an
  original not yet confirmed in the log) and `{:error, {:half, reason, id}}` (the
  hook persisted in only one place and is carried to completion by
  `Ankusa.Intake.Reconciler`). The queue itself is never allowed to crash the
  call: a failed commit (or a dead writer process) is reported as
  `:store_unavailable`, which the edge maps to `503`.

  The record's tenant share and its place in the partition are counted before
  the call and given back when it returns or exits, whatever the outcome, so a
  caller that crashes mid-commit leaks nothing. (A caller killed with `:kill`
  runs no cleanup and keeps its place until the partition restarts; nothing on
  the request path sends one.)

  `timeout` (milliseconds, or `:infinity`) bounds how long the record may wait
  before its batch *starts* committing. Once it has started the call waits for
  the outcome, so a slow disk is never answered `:store_unavailable` for a hook
  it then commits (see the moduledoc for the crash windows that still can).
  """
  @spec commit(atom(), non_neg_integer(), Queue.entry(), timeout()) ::
          {:committed, Ankusa.Envelope.t()}
          | {:duplicate, Ankusa.Envelope.t()}
          | {:error,
             :overload | :tenant_limit | :tenant_erasing | :dedupe_quota | :store_unavailable}
          | {:error, :intake_unavailable | :intake_pending}
          | {:error, {:half, :intake_unavailable | :store_unavailable, String.t()}}
          | {:error, {:tenant_overload, pos_integer()}}
  def commit(instance, partition, record, timeout \\ @record_deadline_ms) do
    case acquire(instance, record) do
      {:ok, lease} ->
        try do
          admit(instance, partition, record, timeout)
        after
          RateLimiter.release_queue(instance, lease)
        end

      {:error, {:tenant_overload, _retry_after_ms}} = refused ->
        refused
    end
  end

  # A record with no tenant (the tests' bare entries) has no share to count.
  defp acquire(instance, record) do
    case record do
      %{envelope: %{tenant_id: tenant_id}} when is_binary(tenant_id) ->
        RateLimiter.acquire_queue(instance, tenant_id, record_bytes(record))

      _ ->
        {:ok, nil}
    end
  end

  # The partition's place is taken before the call and given back after it, on
  # every path: a counter that is not released would shed hooks for good.
  defp admit(instance, partition, record, timeout) do
    {counters, max_queue, max_bytes} = gate!(instance, partition)
    size = record_bytes(record)
    count = :atomics.add_get(counters, @count, 1)
    bytes = :atomics.add_get(counters, @bytes, size)

    if count > max_queue or (count > 1 and bytes > max_bytes) do
      :atomics.sub(counters, @count, 1)
      :atomics.sub(counters, @bytes, size)

      Ankusa.Telemetry.emit([:load_shed], %{queue: count - 1, bytes: bytes - size}, %{
        instance: instance
      })

      {:error, :overload}
    else
      try do
        enqueue(instance, record, deadline(timeout))
      after
        :atomics.sub(counters, @count, 1)
        :atomics.sub(counters, @bytes, size)
      end
    end
  end

  # The writer's answer to one hook. A store that is down, a writer that is gone
  # and a batch the writer refused (its deadline passed, or its caller was
  # already dead) are all `:store_unavailable`, nothing acked: the failures that
  # matter are logged where they happen (`Ankusa.Queue.Writer`,
  # `Ankusa.Queue.Syncer`), once per group, not once per request.
  defp enqueue(instance, record, deadline) do
    case Queue.enqueue(instance, [record], deadline) do
      {:ok, [reply]} -> reply
      {:error, _reason} -> {:error, :store_unavailable}
    end
  rescue
    error ->
      Logger.error("[ankusa] queue enqueue raised: #{Exception.format_banner(:error, error)}")
      {:error, :store_unavailable}
  catch
    :exit, _reason -> {:error, :store_unavailable}
  end

  defp gate!(instance, partition) do
    case Registry.lookup(Ankusa.Registry, {instance, {:batcher_queue, partition}}) do
      [{_pid, gate}] -> gate
      [] -> exit({:noproc, {__MODULE__, :gate, [instance, partition]}})
    end
  end

  defp record_bytes(%{envelope: %{body: body}}) when is_binary(body), do: byte_size(body)
  defp record_bytes(_record), do: 0

  # `:infinity` stays an atom all the way to the writer. Atoms sort above every
  # integer, so it is never `<= now` and never the minimum of a group that holds
  # a real deadline.
  defp deadline(:infinity), do: :infinity
  defp deadline(timeout_ms), do: System.monotonic_time(:millisecond) + timeout_ms

  # ── server ────────────────────────────────────────────────────────────────

  # The process exists to own the registration: the counters and the bounds are
  # the Registry value of `{instance, {:batcher_queue, partition}}`, so a request
  # reads them with one ETS lookup and no message. A restart starts at zero;
  # callers that charged the old counters give back to them, not to the new ones.
  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    partition = Keyword.fetch!(opts, :partition)
    b = Ankusa.config(instance).batcher
    counters = :atomics.new(2, signed: true)

    {:ok, _} =
      Registry.register(
        Ankusa.Registry,
        {instance, {:batcher_queue, partition}},
        {counters, b.max_queue, b.max_queue_bytes}
      )

    {:ok, %{instance: instance, partition: partition}}
  end
end
