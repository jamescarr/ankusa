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
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @type entry :: %{
          required(:envelope) => Envelope.t(),
          required(:sinks) => [{module(), keyword()}],
          optional(:dedupe_ttl_ms) => pos_integer() | nil,
          # set on every delivery row as `replay:` (a released quarantine hook)
          optional(:replay_id) => String.t()
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
  """
  @spec enqueue(atom(), [entry()], integer() | :infinity) ::
          {:ok, [{:committed, Envelope.t()} | {:duplicate, Envelope.t()}]} | {:error, term()}
  def enqueue(instance, entries, deadline \\ :infinity) do
    GenServer.call(
      Ankusa.via(instance, :queue_writer),
      {:enqueue, items(entries), deadline, []},
      :infinity
    )
  end

  @doc """
  Commit hooks released from the quarantine pen: `enqueue/3` semantics (dedupe,
  archive obligation, fresh seqs) plus `replay: replay_id` on every delivery
  row, with `extra_ops` (the pen deletes and the job's cursor) in the same
  synced batch — so a crash cannot leave a hook both committed and still held,
  or held and gone. `extra_ops` commit even when every entry is a duplicate.
  Never deadline-bound.
  """
  @spec release(atom(), [entry()], [Ankusa.Store.op()]) ::
          {:ok, [{:committed, Envelope.t()} | {:duplicate, Envelope.t()}]} | {:error, term()}
  def release(instance, entries, extra_ops) do
    GenServer.call(
      Ankusa.via(instance, :queue_writer),
      {:enqueue, items(entries), :infinity, extra_ops},
      :infinity
    )
  end

  defp items(entries) do
    Enum.map(entries, fn %{envelope: env, sinks: sinks} = entry ->
      {env, Envelope.to_binary(%{env | seq: nil}), Enum.map(sinks, &elem(&1, 0)),
       Map.get(entry, :dedupe_ttl_ms), Map.get(entry, :replay_id)}
    end)
  end

  @type redrive_entry :: %{
          bin: binary(),
          size: non_neg_integer(),
          sinks: [{non_neg_integer(), module()}],
          replay_id: String.t()
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
               {:cont, {n + 1, [stamp(key, bin) | acc]}}
             else
               {:halt, {n, acc}}
             end
           end
         ) do
      {:ok, {_n, list}} -> {:ok, Enum.reverse(list)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp stamp(<<seq::64>>, bin), do: %{Envelope.from_binary(bin) | seq: seq}

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
  (a unix-ms lower bound on the dead-letter time), `:limit`.
  """
  @spec dead(atom(), keyword()) ::
          {:ok, %{total: non_neg_integer(), entries: [map()]}} | {:error, term()}
  def dead(instance, opts), do: Deliveries.dead(instance, opts)

  defp next_seq(instance) do
    case Store.get(instance, :default, Keys.meta("next_seq")) do
      {:ok, <<n::64>>} -> {:ok, n}
      :not_found -> {:ok, 1}
      {:error, reason} -> {:error, reason}
    end
  end

  defp disk_bytes(instance) do
    cfs = [:default, :hooks, :deliveries, :index, :archive, :quarantine]

    Enum.reduce_while(cfs, {:ok, 0}, fn cf, {:ok, acc} ->
      with {:ok, sst} <- Store.property(instance, cf, "rocksdb.total-sst-files-size") do
        blob =
          case Store.property(instance, cf, "rocksdb.total-blob-file-size") do
            {:ok, n} -> n
            {:error, _} -> 0
          end

        {:cont, {:ok, acc + sst + blob}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @doc """
  The configured queue mode's name, for boot banners and `check-config`:
  `"disk"` for the store-backed queue, `"none"` under `wal: :none`.
  """
  @spec label(:disk | :none) :: String.t()
  def label(:none), do: "none"
  def label(:disk), do: "disk"

  # ── boot validation ─────────────────────────────────────────────────────

  @doc """
  Reject a configuration that would ack without durability.

  Under `wal: :none` the provider's `2xx` means "a sink confirmed", so every
  statically configured source must have at least one sink whose `:ok` means the
  hook is durably accepted by something that outlives this node — see
  `Ankusa.Sink.durable?/2`. Raises `ArgumentError` naming the first source that
  cannot make that promise.

  Only sources in `config.source_store` are checked. A source created at
  runtime through the admin API is not: the store's decoder has no instance
  config, so `Ankusa.SourceStore.put/5` is the place runtime enforcement would
  have to live. Use `wal.type: disk` if sources are edited at runtime and a
  log-only sink cannot be ruled out.
  """
  @spec validate_config!(Ankusa.Config.t()) :: :ok
  def validate_config!(%Ankusa.Config{wal: :none} = config) do
    Enum.each(static_sources(config), fn {id, %Ankusa.Source{sinks: sinks}} ->
      unless Enum.any?(sinks, fn {mod, opts} -> Ankusa.Sink.durable?(mod, opts) end) do
        raise ArgumentError,
              "source #{inspect(id)}: wal: :none acks the provider on a sink's confirm, " <>
                "but none of its sinks is durable. Configure a durable sink, or use wal.type: disk."
      end
    end)
  end

  def validate_config!(%Ankusa.Config{}), do: :ok

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
