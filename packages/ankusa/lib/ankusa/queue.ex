defmodule Ankusa.Queue do
  @moduledoc """
  The hook queue: the public face of the store's hook and delivery machinery.

  Every hook is written once, with a strictly increasing `seq` assigned by
  `Ankusa.Queue.Writer`, and carries one delivery row per sink of its source at
  ack time. Rows advance through the `:index` column family (due → inflight →
  delivered/dead) and are owned by `Ankusa.Dispatch.Pipeline`; the DLQ is the
  set of dead rows. A hook is deleted once its last obligation — every
  delivery row and, while `:storage` runs, the archive — is cleared, whatever
  roles this node runs.

  The seq is set on the committed envelope returned by `enqueue/2`; on disk it
  comes from the key, so an envelope read back is always stamped from its key.
  """

  alias Ankusa.Envelope
  alias Ankusa.Queue.Deliveries
  alias Ankusa.Queue.Writer
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @type entry :: %{
          required(:envelope) => Envelope.t(),
          required(:sinks) => [{module(), keyword()}],
          optional(:dedupe_ttl_ms) => pos_integer() | nil,
          # set on every delivery row as `replay:` (a released quarantine hook)
          optional(:replay_id) => String.t(),
          # one per sink: `nil` (unordered) or `{lane, key}` (`Ankusa.Ordering`, ADR
          # 0004); the delivery row joins that lane instead of becoming due at once
          optional(:lanes) => [Ankusa.Ordering.lane() | nil] | nil
        }

  @doc """
  Commits `entries` durably to the store (one synced batch), assigning each a
  fresh seq, and returns the committed envelopes. The batch is atomic: it is
  either fully durable or nothing was acked.

  `deadline` is a `System.monotonic_time(:millisecond)` value. The writer
  refuses with `{:error, :deadline_exceeded}` a batch it could not *start*
  before then, and with `{:error, :caller_gone}` one whose caller died while it
  waited; in both nothing is written and no seq is consumed. Once the writer has
  started the batch the call waits for the commit's outcome however long it
  takes, so a returned `{:error, _}` other than those two may mean the commit
  failed, and `{:ok, _}` means it is durable. The call is never abandoned
  half-way: an abandoned call would commit anyway, unacknowledged.

  An entry's reply is `{:error, :tenant_limit}` instead when its tenant would
  be one past `tenants.max`, or `{:error, :tenant_erasing}` when an erase of
  its tenant is running (`Ankusa.Tenant`): that hook is not written. It is
  `{:error, :dedupe_quota}` when its dedupe key would be a new live key past
  `dedupe.max_entries` or `dedupe.max_entries_per_tenant`: the hook is not
  written either, and never stored without its key.

  ## Group commit

  The work that needs no seq runs here, in the caller (the batcher's commit
  task), not in the writer: the hook's binary, one encoded delivery row per sink
  and one `multi_get` of the dedupe keys (a *prefetch*). The writer merges every
  call it has queued, across partitions, into one group with one synced write,
  and re-validates the prefetch before it assigns seqs (`Ankusa.Queue.Writer`),
  so two partitions that read the same key as absent still commit one hook.
  """
  @spec enqueue(atom(), [entry()], integer() | :infinity) ::
          {:ok,
           [
             {:committed, Envelope.t()}
             | {:duplicate, Envelope.t()}
             | {:error, :tenant_limit | :tenant_erasing | :dedupe_quota}
             | {:error, :intake_unavailable | :intake_pending | :store_unavailable}
             | {:error, {:half, :intake_unavailable | :store_unavailable, String.t()}}
           ]}
          | {:error, term()}
  def enqueue(instance, entries, deadline \\ :infinity) do
    with {:ok, prepared} <- prepare(instance, entries, true) do
      GenServer.call(
        Ankusa.via(instance, :queue_writer),
        {:enqueue, prepared, deadline, []},
        :infinity
      )
    end
  end

  @doc """
  Commit hooks released from the quarantine pen: `enqueue/3` semantics (dedupe,
  archive obligation, fresh seqs) plus `replay: replay_id` on every delivery
  row, with `extra_ops` (the pen deletes and the job's cursor) in the same
  synced batch — so a crash cannot leave a hook both committed and still held,
  or held and gone. `extra_ops` commit even when every entry is a duplicate.
  A tenant past `tenants.max` (or one being erased) is the whole call's
  `{:error, :tenant_limit}` (`:tenant_erasing`):
  nothing commits, the pen keeps its entries.
  Never deadline-bound.
  """
  @spec release(atom(), [entry()], [Ankusa.Store.op()]) ::
          {:ok, [{:committed, Envelope.t()} | {:duplicate, Envelope.t()}]} | {:error, term()}
  def release(instance, entries, extra_ops) do
    {:ok, prepared} = prepare(instance, entries, false)

    GenServer.call(
      Ankusa.via(instance, :queue_writer),
      {:enqueue, prepared, :infinity, extra_ops},
      :infinity
    )
  end

  # What the writer is handed: the items (`{envelope, binary, rows, dedupe ttl,
  # replay id}`, `rows` being `{encoded delivery row, lane or nil}` of each sink,
  # stamped with
  # the time the hook was received), their count and body bytes, and, with
  # `prefetch?`, the stored value of every dedupe key read at `read_at` (a
  # `System.monotonic_time(:millisecond)` taken before the read, which is what
  # tells the writer whether a write it made since could be missing from it).
  defp prepare(instance, entries, prefetch?) do
    read_at = System.monotonic_time(:millisecond)
    at = System.system_time(:millisecond)

    items =
      Enum.map(entries, fn %{envelope: env, sinks: sinks} = entry ->
        bin = Envelope.to_binary(%{env | seq: nil})
        replay_id = Map.get(entry, :replay_id)
        size = byte_size(bin)
        lanes = Map.get(entry, :lanes) || List.duplicate(nil, length(sinks))

        rows =
          sinks
          |> Enum.zip(lanes)
          |> Enum.map(fn {{mod, _opts}, lane} ->
            row = Deliveries.new_row(mod, at, size, env.source_id, replay: replay_id, lane: lane)
            {Deliveries.encode_row(row), lane && elem(lane, 0)}
          end)

        {env, bin, rows, Map.get(entry, :dedupe_ttl_ms), replay_id}
      end)

    with {:ok, prefetch} <- if(prefetch?, do: prefetch(instance, items), else: {:ok, %{}}) do
      {:ok,
       %{
         items: items,
         count: length(items),
         bytes:
           Enum.reduce(items, 0, fn {_env, bin, _rows, _ttl, _r}, n -> n + byte_size(bin) end),
         prefetch: prefetch,
         read_at: read_at
       }}
    end
  end

  defp prefetch(instance, items) do
    keys =
      for {env, _bin, _rows, ttl, _replay_id} <- items,
          key = Writer.dedupe_key(env, ttl),
          do: key

    case Enum.uniq(keys) do
      [] ->
        {:ok, %{}}

      keys ->
        with {:ok, results} <- Store.multi_get(instance, :index, keys),
             do: {:ok, keys |> Enum.zip(results) |> Map.new()}
    end
  end

  @type redrive_entry :: %{
          # per entry of `sinks`: `nil` or `{lane, key}` (ADR 0004); absent is unordered
          optional(:lanes) => [Ankusa.Ordering.lane() | nil] | nil,
          bin: binary(),
          size: non_neg_integer(),
          sinks: [{non_neg_integer(), module()}],
          replay_id: String.t(),
          source_id: String.t(),
          tenant_id: String.t() | nil
        }

  @doc """
  Re-commit archived hooks as new queue entries, for a `kind: :archive` replay
  job. Each entry gets a fresh seq, its hook payload, and one pending delivery
  row per bound sink carrying `replay: replay_id`. No archive obligation (the
  hook is already archived) and no ingest dedupe.

  `extra_ops` ride in the same synced batch as the hooks — the job's cursor
  update — so the cursor and the moved hooks commit atomically. Returns the
  number of entries committed. The call is `:infinity` for the same reason as
  `enqueue/3`: the writer never abandons a started batch.
  """
  @spec redrive(atom(), [redrive_entry()], [Ankusa.Store.op()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def redrive(instance, entries, extra_ops) do
    GenServer.call(
      Ankusa.via(instance, :queue_writer),
      {:redrive, entries, extra_ops},
      :infinity
    )
  end

  @doc """
  Committed envelopes with seq > `after_seq`, ascending, `seq` set from the
  key.
  """
  @spec hooks(atom(), non_neg_integer(), pos_integer()) ::
          {:ok, [Envelope.t()]} | {:error, term()}
  def hooks(instance, after_seq, limit) do
    lower = Keys.hook(after_seq + 1)
    upper = Keys.family(:hooks).hi

    case Store.fold(
           instance,
           :hooks,
           {lower, upper},
           {0, []},
           fn key, bin, {n, acc} ->
             if n < limit do
               stamp(key, bin, n, acc)
             else
               {:halt, {n, acc}}
             end
           end
         ) do
      {:ok, {_n, list}} -> {:ok, Enum.reverse(list)}
      {:error, reason} -> {:error, reason}
    end
  end

  # A hook that does not decode (corrupt, or an envelope version this node
  # does not know) stops the read with a named error, not a raise.
  defp stamp(<<seq::64>>, bin, n, acc) do
    case Envelope.from_binary(bin) do
      {:ok, env} ->
        {:cont, {n + 1, [%{env | seq: seq} | acc]}}

      {:error, reason} ->
        {:error, {:corrupt_value, :hooks, "hook #{seq} does not decode: #{reason}"}}
    end
  end

  @doc """
  Store statistics: `next_seq` (exact), `hooks` and `deliveries` (RocksDB key
  estimates), and `disk_bytes` (SST + blob file sizes across column families).
  """
  @spec stats(atom()) :: {:ok, map()} | {:error, term()}
  def stats(instance) do
    with {:ok, next_seq} <- next_seq(instance),
         {:ok, hooks} <- key_estimate(instance, :hooks),
         {:ok, deliveries} <- key_estimate(instance, :deliveries),
         {:ok, disk_bytes} <- disk_bytes(instance) do
      {:ok,
       %{
         next_seq: next_seq,
         hooks: hooks,
         deliveries: deliveries,
         disk_bytes: disk_bytes
       }}
    end
  end

  # The store keeps end-of-range sentinel keys in every scanned family; they are
  # not hooks or deliveries, so an empty store reports 0, not 2.
  defp key_estimate(instance, cf) do
    sentinels = Enum.count(Keys.sentinels(), fn {sentinel_cf, _key} -> sentinel_cf == cf end)

    with {:ok, n} <- Store.property(instance, cf, "rocksdb.estimate-num-keys") do
      {:ok, max(n - sentinels, 0)}
    end
  end

  @doc """
  The dead-lettered deliveries (the DLQ), newest first. `total` counts the
  matching dead keys that could be read back as a row and a hook; `entries`
  holds the first `:limit`, each `%{envelope, reason, at}` with `reason` the
  failure as text. A dead key whose row or hook is gone (corruption) is logged
  and not listed; one beyond the returned page is still counted, so `total` is
  exact unless corruption hides past the page. Options: `:source_id`, `:since`
  (a unix-ms lower bound on the dead-letter time), `:tenants` (a list of tenant
  ids: only dead letters of those tenants' hooks, read from each tenant's range
  of the by-tenant dead index with no hook read; `nil`, the default, is
  every tenant), `:limit`.
  """
  @spec dead(atom(), keyword()) ::
          {:ok, %{total: non_neg_integer(), entries: [map()]}} | {:error, term()}
  def dead(instance, opts), do: Deliveries.dead(instance, opts)

  @doc """
  The undelivered rows of one source in this node's queue: `pending` waiting
  in the due index (due now or after a backoff) and `inflight` claimed by
  dispatch. Walks both indexes (O(backlog)); rows written before delivery rows
  recorded their source are matched by reading their hook. `tenant:` names the
  one tenant a source owned by a tenant files its hooks under, and limits the
  walk to that tenant's rows.
  """
  @spec pending_for_source(atom(), String.t(), keyword()) ::
          {:ok, %{pending: non_neg_integer(), inflight: non_neg_integer()}} | {:error, term()}
  def pending_for_source(instance, source_id, opts \\ []),
    do: Deliveries.pending_for_source(instance, source_id, opts)

  @doc """
  `%{pending, scheduled, inflight, bytes}` of one tenant, from its counters:
  O(1). `pending` counts first-attempt rows waiting and `scheduled` retry rows
  waiting (including a retry whose backoff just ended); `bytes` is the stored
  size of every delivery row the tenant holds (a hook with *n* sinks counts *n*
  times).
  """
  @spec pending_for_tenant(atom(), String.t()) ::
          {:ok,
           %{pending: integer(), scheduled: integer(), inflight: integer(), bytes: integer()}}
          | {:error, term()}
  def pending_for_tenant(instance, tenant), do: Deliveries.pending_for_tenant(instance, tenant)

  @doc "`%{dead: n}`: the dead rows of one tenant, from its counter: O(1)."
  @spec dead_for_tenant(atom(), String.t()) :: {:ok, %{dead: integer()}} | {:error, term()}
  def dead_for_tenant(instance, tenant), do: Deliveries.dead_for_tenant(instance, tenant)

  @doc """
  Removes `tenant`'s `:pending` (every due row), `:inflight` (claimed rows,
  for a tenant dispatch has stopped claiming: see `Ankusa.Tenant`) and/or
  `:dead` rows (`kinds` a subset of `[:pending, :inflight, :dead]`) in synced
  chunks of 500: idempotent, so a run that stopped part-way resumes by being
  called again. Without `:inflight` it never touches claimed rows. Returns the
  rows removed per kind.
  """
  @spec purge_tenant(atom(), String.t(), [:pending | :inflight | :dead]) ::
          {:ok,
           %{
             required(:pending) => non_neg_integer(),
             required(:dead) => non_neg_integer(),
             optional(:inflight) => non_neg_integer()
           }}
          | {:error, term()}
  def purge_tenant(instance, tenant, kinds), do: Deliveries.purge_tenant(instance, tenant, kinds)

  defp next_seq(instance) do
    case Store.get(instance, :default, Keys.meta("next_seq")) do
      {:ok, <<n::64>>} -> {:ok, n}
      :not_found -> {:ok, 1}
      {:error, reason} -> {:error, reason}
    end
  end

  defp disk_bytes(instance) do
    cfs = [
      :default,
      :hooks,
      :deliveries,
      :index,
      :archive,
      :quarantine,
      :audit,
      :tenants,
      :counters
    ]

    Enum.reduce_while(cfs, {:ok, 0}, fn cf, {:ok, acc} ->
      case Store.property(instance, cf, "rocksdb.total-sst-files-size") do
        {:ok, sst} ->
          blob =
            case Store.property(instance, cf, "rocksdb.total-blob-file-size") do
              {:ok, n} -> n
              {:error, _} -> 0
            end

          {:cont, {:ok, acc + sst + blob}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  @doc """
  The configured queue mode's name, for boot banners and `check-config`:
  `"disk"` for the store-backed queue, `"dual"` for the store plus the intake
  log (`wal: :dual`), `"none"` under `wal: :none`.
  """
  @spec label(:disk | :dual | :none) :: String.t()
  def label(:none), do: "none"
  def label(:dual), do: "dual"
  def label(:disk), do: "disk"

  # ── boot validation ─────────────────────────────────────────────────────

  @doc """
  Reject a configuration that would ack without durability.

  Under `wal: :none` the provider's `2xx` means "a sink confirmed", so every
  statically configured source must have at least one sink whose `:ok` means the
  hook is durably accepted by something that outlives this node — see
  `Ankusa.Sink.durable?/2`. Raises `ArgumentError` naming the first source that
  cannot make that promise.

  This covers the sources in `config.source_store`. A source written at runtime
  (the admin API, `Ankusa.SourceStore.put/5`) is refused by the same rule when
  it is written, by every writable source store; see `check_durable/2`.
  """
  @spec validate_config!(Ankusa.Config.t()) :: :ok
  def validate_config!(%Ankusa.Config{} = config) do
    config
    |> static_sources()
    |> Enum.each(fn {_id, source} ->
      with {:error, message} <- check_durable(config, source), do: raise(ArgumentError, message)
    end)
  end

  @doc """
  `:ok` unless `config` runs `wal: :none` and none of `source`'s sinks is
  durable (`Ankusa.Sink.durable?/2`) — such a source would answer the provider
  `2xx` with no durable copy anywhere. Any other WAL mode always passes.
  """
  @spec check_durable(Ankusa.Config.t(), Ankusa.Source.t()) :: :ok | {:error, String.t()}
  def check_durable(%Ankusa.Config{wal: :none}, %Ankusa.Source{id: id, sinks: sinks}) do
    if Enum.any?(sinks, fn {mod, opts} -> Ankusa.Sink.durable?(mod, opts) end) do
      :ok
    else
      {:error,
       "source #{inspect(id)}: wal: :none acks the provider on a sink's confirm, " <>
         "but none of its sinks is durable. Configure a durable sink, or use wal.type: disk."}
    end
  end

  def check_durable(%Ankusa.Config{}, %Ankusa.Source{}), do: :ok

  @doc false
  # The sources declared in `config.source_store`'s `:sources`, built. Shared
  # with `Ankusa.Verifier.validate_config!/1`.
  @spec static_sources(Ankusa.Config.t()) :: [{String.t(), Ankusa.Source.t()}]
  def static_sources(config) do
    {_mod, opts} = config.source_store

    opts
    |> Keyword.get(:sources, %{})
    |> Enum.map(fn
      {id, %Ankusa.Source{} = source} -> {id, source}
      {id, source_opts} -> {id, Ankusa.Source.new(id, source_opts)}
    end)
  end
end
