defmodule Ankusa.Edge.Quarantine do
  @moduledoc """
  A durable holding pen for envelopes that failed verification under a
  `:quarantine` policy. A bad secret rotation should never silently eat real
  events, and the pen is how they come back: a `kind: :quarantine` replay job
  (`Ankusa.Replay`) re-verifies each held hook against its source's *current*
  verifier and commits the ones that now pass; `purge/3` drops the rest.

  A flood of forged requests must not fill the disk, or starve another
  source's real failures of room, so the pen is bounded three times:

    * one token bucket per source (`config.quarantine`) — `burst` tokens,
      refilled `rate` per second. Over it, `put/3` answers
      `{:rate_limited, retry_after_ms}` (`429 quarantine_rate_limited`).
    * a cap on the pen's total bytes, `max_bytes`. A write that would cross
      it answers `:full` (`503 quarantine_full`). A full pen refuses new
      hooks; it never evicts one it already acked.
    * at most 1 000 writes waiting for the pen's disk write at once (`pending`,
      the `ankusa_quarantine_pending` gauge). Past it `put/3` answers
      `{:error, :overload}` (`503` with `Retry-After: 1`) and spends nothing.

  ## Who does what

  The decisions are made in the calling request process, not in a queue: a
  source's bucket is one ETS row updated with a compare-and-swap (GCRA, as
  `Ankusa.Edge.RateLimiter`'s buckets are), and the byte and pending counts are
  `:ets.update_counter/3`, so a flood on one source never makes another
  source's `hit/3` or `put/3` wait for it. Only the disk write is shared: the
  caller hands its two rows to the pen process, which commits every write that
  has queued up meanwhile in **one** synced `Ankusa.Store.write/3` (a group
  commit), so a flood costs one fsync per group instead of one per request and
  holds one dirty-IO scheduler however many callers wait. The pen process
  otherwise keeps the node-start recovery of the byte count, the bucket sweep,
  the retention sweep and `purge/3`.

  Each record is two keys in this node's `Ankusa.Store`, committed in one synced
  batch: a small summary (id, source, tenant, time, reason, size) that
  `recent/2` and `page/5` list, and the whole envelope, which `envelope/2`
  reads back. `put/3` answers `:ok` only once both are on disk.
  """

  use GenServer

  require Logger

  alias Ankusa.{Audit, Config, Envelope, Store}
  alias Ankusa.Audit.Context
  alias Ankusa.Edge.RateLimiter
  alias Ankusa.Queue.Tenants
  alias Ankusa.Store.Keys
  alias Ankusa.Tenant.Gate

  # Idle buckets are dropped this often (a missing bucket is a full one).
  @sweep_interval_ms 60_000
  @max_ms 0xFFFF_FFFF_FFFF_FFFF
  @purge_timeout_ms 60_000
  @retention_interval_ms 3_600_000
  @retention_chunk 1_000
  @retention_chunks 100

  # How long a caller waits for the pen's group commit before it answers 503. A
  # write still queued then may land afterwards, so the caller keeps its token
  # and its bytes counted.
  @write_timeout_ms 5_000
  # Writes waiting for the pen (queued or in the group being committed).
  @max_pending 1_000
  # The most writes one group commit takes.
  @write_group 256

  @typedoc """
  `:source_id`/`:id` match the summary; `:since`/`:until` (ms, inclusive) bound
  `received_at`; `:tenants`, a list of tenant ids (`nil`: every tenant), keeps
  the entries of those tenants.
  """
  @type filter :: %{
          optional(:source_id) => String.t() | nil,
          optional(:id) => String.t() | nil,
          optional(:since) => non_neg_integer() | nil,
          optional(:until) => non_neg_integer() | nil,
          optional(:tenants) => [String.t()] | nil
        }

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :quarantine))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc "Check `config.quarantine`; raises `ArgumentError` naming the bad key."
  @spec validate_config!(Config.t()) :: :ok
  def validate_config!(%Config{quarantine: %{burst: burst, rate: rate, max_bytes: max_bytes}}) do
    unless is_integer(burst) and burst >= 1 do
      raise ArgumentError, "quarantine.burst must be a positive integer, got #{inspect(burst)}"
    end

    unless is_number(rate) and rate > 0 do
      raise ArgumentError, "quarantine.rate must be a positive number, got #{inspect(rate)}"
    end

    unless is_integer(max_bytes) and max_bytes >= 1 do
      raise ArgumentError,
            "quarantine.max_bytes must be a positive integer, got #{inspect(max_bytes)}"
    end

    :ok
  end

  @doc """
  Durably record a quarantined envelope. Returns `:ok` once it is on disk;
  `{:rate_limited, retry_after_ms}` when the source's bucket is empty; `:full`
  when the entry would take the pen past `max_bytes` (no token is spent);
  `{:error, :overload}` when #{1_000} writes already wait for the pen (nothing is
  spent); `{:error, :store_unavailable}` when the store could not take the
  write (no token is spent) or the pen did not answer; `{:error,
  :tenant_erasing}` when an erase of the envelope's tenant is running
  (`Ankusa.Tenant`).
  """
  @spec put(atom(), Envelope.t(), term()) ::
          :ok
          | {:rate_limited, pos_integer()}
          | :full
          | {:error, :store_unavailable | :tenant_erasing | :overload}
  def put(instance, env, reason) do
    tenant = Tenants.normalize(env.tenant_id)

    if Gate.closed?(instance, tenant),
      do: {:error, :tenant_erasing},
      else: charged_put(instance, env, reason, tenant)
  rescue
    # The pen's tables went with the pen: it is restarting.
    ArgumentError -> {:error, :store_unavailable}
  catch
    :exit, _reason -> {:error, :store_unavailable}
  end

  # The source's token first (a flood on a drained bucket costs one ETS read),
  # then the bytes and the write. Whatever refuses the hook after that gives
  # the token back.
  defp charged_put(instance, env, reason, tenant) do
    limits = Ankusa.config(instance).quarantine
    source_id = env.source_id

    case charge(instance, source_id, limits) do
      {:error, {:rate_limited, retry_after_ms}} ->
        refused(:rate_limited, instance, source_id, tenant)
        {:rate_limited, retry_after_ms}

      {:ok, increment} ->
        case reserve_and_write(instance, env, reason, limits.max_bytes) do
          :ok ->
            :ok

          :full ->
            refund(instance, source_id, increment)
            refused(:full, instance, source_id, tenant)
            :full

          {:error, _reason} = error ->
            refund(instance, source_id, increment)
            error
        end
    end
  end

  @doc """
  Spend one token of `source_id`'s bucket without writing anything: the charge
  for a hook a source accepts *flagged* (`on_verify_failure: :accept_flag`),
  which is committed like any hook but must not be free to forge. Same bucket,
  same arithmetic as `put/3`; `tenant_id` only labels the refusal's telemetry.
  `{:rate_limited, retry_after_ms}` when it is empty; `{:error, :store_unavailable}`
  when the pen's tables are gone (it is restarting).
  """
  @spec hit(atom(), String.t(), String.t() | nil) ::
          :ok | {:rate_limited, pos_integer()} | {:error, :store_unavailable}
  def hit(instance, source_id, tenant_id) do
    case charge(instance, source_id, Ankusa.config(instance).quarantine) do
      {:ok, _increment} ->
        :ok

      {:error, {:rate_limited, retry_after_ms}} ->
        refused(:rate_limited, instance, source_id, Tenants.normalize(tenant_id))
        {:rate_limited, retry_after_ms}
    end
  rescue
    ArgumentError -> {:error, :store_unavailable}
  end

  @doc """
  Tell the pen `bytes` left it outside `purge/3` — a release commit deleted
  their entries. A no-op when the pen is not running on this node.
  """
  @spec released(atom(), non_neg_integer()) :: :ok
  def released(_instance, 0), do: :ok

  def released(instance, bytes) when is_integer(bytes) and bytes > 0,
    do: give_back(counters_table(instance), :bytes, bytes)

  @doc """
  Delete up to `limit` entries matching `filter`, oldest first, in one synced
  batch together with the purge's audit row (`quarantine.purge`, with the
  filter, the count and the bytes as its `detail`). Returns how many went and
  the bytes they held. A pen that is not running (its domain restarting) is
  `{:error, :store_unavailable}`. Options: `audit:`, the
  `Ankusa.Audit.Context` of the change.
  """
  @spec purge(atom(), filter(), pos_integer(), keyword()) ::
          {:ok, %{deleted: non_neg_integer(), bytes: non_neg_integer()}}
          | {:error, :store_unavailable}
  def purge(instance, filter, limit, opts \\ [])
      when is_map(filter) and is_integer(limit) and limit > 0 do
    GenServer.call(
      Ankusa.via(instance, :quarantine),
      {:purge, filter, limit, Context.from_opts(opts)},
      @purge_timeout_ms
    )
  catch
    :exit, _reason -> {:error, :store_unavailable}
  end

  @doc """
  Delete the entries held longer than `quarantine.retention_days`, oldest first,
  in synced batches of at most #{1_000}, each with its audit row
  (`quarantine.retention`). An entry a live `kind: :quarantine` replay job can
  still reach (received at or after the earliest `since` of one) stays; so does
  everything while the replayer cannot say. Runs hourly, in a process of its own;
  callable on its own. `more` says a pass's budget, not the end of the pen,
  stopped it. Emits `[:ankusa, :quarantine, :pruned]` (`count`, `bytes`) per batch.
  """
  @spec prune(atom()) ::
          {:ok, %{deleted: non_neg_integer(), bytes: non_neg_integer(), more: boolean()}}
          | {:error, term()}
  def prune(instance) do
    days = Ankusa.config(instance).quarantine.retention_days
    cutoff = System.system_time(:millisecond) - days * 86_400_000

    with {:ok, floor} <- Ankusa.Replay.live_floor(instance, :quarantine, :since) do
      prune_chunks(instance, range(nil, min(cutoff, floor) - 1), @retention_chunks, %{
        deleted: 0,
        bytes: 0,
        more: false
      })
    end
  end

  @doc """
  What the pen holds: `bytes` (the count the `max_bytes` cap is enforced
  against), `pending` (writes waiting for the pen's disk write) and `entries`.
  The entry count is a scan of the pen's summary keys, run in the caller, so it
  never queues a write behind it. A pen that is not running is
  `{:error, :store_unavailable}`.
  """
  @spec held(atom()) ::
          {:ok,
           %{bytes: non_neg_integer(), entries: non_neg_integer(), pending: non_neg_integer()}}
          | {:error, :store_unavailable}
  def held(instance) do
    %{bytes: bytes, pending: pending} = GenServer.call(Ankusa.via(instance, :quarantine), :held)
    %{lo: lo, hi: hi} = Keys.family(:quarantine)

    case Store.fold(instance, :quarantine, {lo, hi}, 0, fn _key, _value, n -> {:cont, n + 1} end) do
      {:ok, entries} -> {:ok, %{bytes: bytes, entries: entries, pending: pending}}
      {:error, _reason} -> {:error, :store_unavailable}
    end
  catch
    :exit, _reason -> {:error, :store_unavailable}
  end

  @doc """
  The most recent quarantined entries matching `filter` (`:source_id`, `:id`,
  `:tenants`), newest first, at most `limit`. Reads the store directly, so it
  never queues behind a write.
  """
  @spec recent(atom(), non_neg_integer(), filter()) :: {:ok, [map()]} | {:error, term()}
  def recent(instance, limit, filter \\ %{})

  def recent(_instance, 0, _filter), do: {:ok, []}

  def recent(instance, limit, filter) when is_integer(limit) and limit > 0 do
    %{lo: lo, hi: hi} = Keys.family(:quarantine)

    result =
      Store.fold(
        instance,
        :quarantine,
        {lo, hi},
        {0, []},
        fn _key, value, {n, acc} = state ->
          # A summary that does not decode is not listed, as in `page/5`.
          with {:ok, summary} <- decode_summary(value),
               true <- matches?(filter, summary) do
            acc = [summary | acc]
            if n + 1 >= limit, do: {:halt, {n + 1, acc}}, else: {:cont, {n + 1, acc}}
          else
            _ -> {:cont, state}
          end
        end,
        reverse: true
      )

    case result do
      {:ok, {_n, entries}} -> {:ok, Enum.reverse(entries)}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  The summary-key range holding entries received in `since..until` (ms,
  inclusive; `nil` is unbounded). A bound outside what the key's 64 bits hold
  is clamped, never wrapped: a negative `until` is an empty range, not the
  whole pen.
  """
  @spec range(integer() | nil, integer() | nil) :: {binary(), binary()}
  def range(since, until) do
    %{hi: hi} = Keys.family(:quarantine)
    lower = <<?s, (since || 0) |> max(0) |> min(@max_ms)::64>>
    upper = if until == nil or until >= @max_ms, do: hi, else: <<?s, max(until + 1, 0)::64>>
    {lower, upper}
  end

  @doc """
  A paged scan over the pen's summaries in `{lower, upper}` (half-open), for
  the replay engine. Keeps entries matching `filter` (`:source_id`, `:id`; the
  range carries time). Stops after `limit` hits or `max_scan` keys examined; a
  summary that does not decode is scanned and skipped. Returns the hits
  ascending as `{summary_key, summary}`, the last key seen (`nil` when the
  range held nothing), whether the range was exhausted, and the keys scanned.
  """
  @spec page(atom(), {binary(), binary()}, filter(), pos_integer(), pos_integer()) ::
          {:ok, [{binary(), map()}], binary() | nil, boolean(), non_neg_integer()}
          | {:error, term()}
  def page(instance, {lower, upper}, filter, limit, max_scan) do
    result =
      Store.fold(instance, :quarantine, {lower, upper}, {0, 0, [], nil}, fn key,
                                                                            value,
                                                                            {scanned, hit_n, hits,
                                                                             last} ->
        # The scan budget is enforced before the filter: a filter that matches
        # little must not make one tick fold the whole range.
        if scanned >= max_scan do
          {:halt, {scanned, hit_n, hits, last}}
        else
          case decode_summary(value) do
            {:ok, summary} ->
              cond do
                not matches?(filter, summary) ->
                  {:cont, {scanned + 1, hit_n, hits, key}}

                hit_n >= limit ->
                  {:halt, {scanned, hit_n, hits, last}}

                true ->
                  {:cont, {scanned + 1, hit_n + 1, [{key, summary} | hits], key}}
              end

            :error ->
              {:cont, {scanned + 1, hit_n, hits, key}}
          end
        end
      end)

    case result do
      {:ok, {scanned, hit_n, hits, last}} ->
        # Only running off the range means every entry in it was examined.
        exhausted? = hit_n < limit and scanned < max_scan
        {:ok, Enum.reverse(hits), last, exhausted?, scanned}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  The held envelope for a `page/5` hit. An entry written before the pen kept
  whole envelopes (by 0.4, or imported from a 0.3 data dir) holds only headers
  and body; it comes back as a `POST /` from the summary's source with no
  tenant (the caller supplies the source's own).
  """
  @spec envelope(atom(), {binary(), map()}) ::
          {:ok, Envelope.t()} | :not_found | {:error, :undecodable | term()}
  def envelope(instance, {<<?s, received_at::64, id::binary>>, summary}) do
    case Store.get(instance, :quarantine, Keys.quarantine_body(received_at, id)) do
      {:ok, bin} -> decode_held(bin, received_at, id, summary)
      :not_found -> :not_found
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "The store ops that delete the entry under `summary_key`: both of its keys."
  @spec delete_ops(binary()) :: [Store.op()]
  def delete_ops(<<?s, received_at::64, id::binary>> = summary_key) do
    [
      {:delete, :quarantine, summary_key},
      {:delete, :quarantine, Keys.quarantine_body(received_at, id)}
    ]
  end

  @doc false
  # The summary as stored, with `:size` — the bytes the entry's two keys hold,
  # to within the few that `:size` itself adds. `Ankusa.Store.Migrate` uses it
  # so imported entries count against the cap like written ones.
  @spec encode_summary(map(), binary()) :: binary()
  def encode_summary(summary, held_bin), do: summary |> sized(held_bin) |> elem(0)

  defp sized(summary, held_bin) do
    size = byte_size(:erlang.term_to_binary(summary)) + byte_size(held_bin)
    {:erlang.term_to_binary(Map.put(summary, :size, size)), size}
  end

  # ── request-side admission ────────────────────────────────────────────────

  defp charge(instance, source_id, %{burst: burst, rate: rate}) do
    {increment, capacity} =
      RateLimiter.gcra(System.convert_time_unit(1, :second, :native), rate, burst, 1)

    case RateLimiter.hit_bucket(buckets_table(instance), source_id, increment, capacity) do
      :ok -> {:ok, increment}
      {:error, {:rate_limited, _retry_after_ms}} = denied -> denied
    end
  end

  # Moves the row's arrival time back by what `charge/3` moved it forward: a
  # refusal after the token was taken must not have cost the source anything.
  # A row the sweep deleted meanwhile was a full bucket already.
  defp refund(instance, source_id, increment) do
    :ets.update_counter(buckets_table(instance), source_id, {2, -increment})
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp refused(kind, instance, source_id, tenant_id) do
    Ankusa.Telemetry.emit([:quarantine, kind], %{}, %{
      instance: instance,
      source_id: source_id,
      tenant_id: tenant_id
    })
  end

  # Charge first, compare, undo on refusal: two racers at the edge of a cap see
  # different counts, so the pen never passes `max_bytes` or `@max_pending`.
  defp reserve_and_write(instance, env, reason, max_bytes) do
    held = Envelope.to_binary(%{env | seq: nil})

    {summary, size} =
      sized(
        %{
          id: env.id,
          source_id: env.source_id,
          tenant_id: env.tenant_id,
          received_at: env.received_at,
          reason: reason
        },
        held
      )

    counters = counters_table(instance)

    cond do
      :ets.update_counter(counters, :bytes, {2, size}) > max_bytes ->
        give_back(counters, :bytes, size)
        :full

      :ets.update_counter(counters, :pending, {2, 1}) > @max_pending ->
        give_back(counters, :pending, 1)
        give_back(counters, :bytes, size)
        {:error, :overload}

      true ->
        ops = [
          {:put, :quarantine, Keys.quarantine_summary(env.received_at, env.id), summary},
          {:put, :quarantine, Keys.quarantine_body(env.received_at, env.id), held}
        ]

        # The pen takes this write off `pending` once it has answered it, so a
        # caller that gave up (the exit goes to `put/3`) still counts while its
        # message is queued.
        case GenServer.call(Ankusa.via(instance, :quarantine), {:write, ops}, @write_timeout_ms) do
          :ok ->
            :ok

          {:error, _reason} ->
            give_back(counters, :bytes, size)
            {:error, :store_unavailable}
        end
    end
  end

  # ── server ────────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)

    # The cap is only as good as the count: refuse to start rather than guess
    # an empty pen.
    case held_bytes(instance) do
      {:ok, bytes} ->
        new_table(buckets_table(instance))
        counters = new_table(counters_table(instance))
        :ets.insert(counters, [{:bytes, bytes}, {:pending, 0}])

        schedule_sweep()
        schedule_retention(@retention_interval_ms)
        {:ok, %{instance: instance, pruning: nil}}

      {:error, reason} ->
        {:stop, {:quarantine_init_failed, reason}}
    end
  end

  # Group commit: this write and every other one already queued (up to
  # `@write_group`) go to the store in one synced batch, and each caller gets
  # that batch's answer. The rows of different hooks never share a key.
  @impl true
  def handle_call({:write, ops}, from, state) do
    {froms, groups, n} = drain_writes([from], [ops], 1)

    reply =
      case Store.write(state.instance, Enum.concat(groups), sync: true) do
        :ok ->
          :ok

        {:error, error} ->
          Logger.error("[ankusa] quarantine write failed: #{inspect(error)}")
          {:error, error}
      end

    Enum.each(froms, &GenServer.reply(&1, reply))
    give_back(counters_table(state.instance), :pending, n)
    {:noreply, state}
  end

  def handle_call(:held, _from, state) do
    counters = counters_table(state.instance)

    {:reply,
     %{
       bytes: :ets.lookup_element(counters, :bytes, 2),
       pending: :ets.lookup_element(counters, :pending, 2)
     }, state}
  end

  def handle_call({:purge, filter, limit, context}, _from, state) do
    {since, until} = {Map.get(filter, :since), Map.get(filter, :until)}

    with {:ok, hits} <- purge_hits(state.instance, range(since, until), filter, limit),
         ops = Enum.flat_map(hits, fn {key, _size} -> delete_ops(key) end),
         bytes = hits |> Enum.map(&elem(&1, 1)) |> Enum.sum(),
         :ok <-
           Audit.commit(state.instance, context, purge_attrs(filter, limit, hits, bytes), ops) do
      give_back(counters_table(state.instance), :bytes, bytes)
      {:reply, {:ok, %{deleted: length(hits), bytes: bytes}}, state}
    else
      {:error, error} ->
        Logger.error("[ankusa] quarantine purge failed: #{inspect(error)}")
        {:reply, {:error, :store_unavailable}, state}
    end
  end

  defp drain_writes(froms, groups, n) when n >= @write_group, do: {froms, groups, n}

  defp drain_writes(froms, groups, n) do
    receive do
      {:"$gen_call", from, {:write, ops}} -> drain_writes([from | froms], [ops | groups], n + 1)
    after
      0 -> {froms, groups, n}
    end
  end

  # What a purge says about itself: the filter and what it took, never a body.
  defp purge_attrs(filter, limit, hits, bytes) do
    filter_detail =
      for {key, value} when not is_nil(value) <- filter, into: %{}, do: {to_string(key), value}

    %{
      action: "quarantine.purge",
      resource: "quarantine",
      detail: %{
        "filter" => filter_detail,
        "limit" => limit,
        "deleted" => length(hits),
        "bytes" => bytes
      }
    }
  end

  @impl true
  def handle_info(:sweep, state) do
    # A row whose arrival time has passed is a full bucket: dropping it admits
    # exactly what keeping it would.
    :ets.select_delete(buckets_table(state.instance), [
      {{:_, :"$1"}, [{:"=<", :"$1", System.monotonic_time()}], [true]}
    ])

    schedule_sweep()
    {:noreply, state}
  end

  # The retention sweep runs in a process of its own (`prune/1`): it asks the
  # replayer for its jobs, which the pen must never wait on.
  def handle_info(:retention, %{pruning: nil} = state) do
    instance = state.instance

    {_pid, ref} =
      spawn_monitor(fn ->
        case prune(instance) do
          {:ok, %{more: true}} ->
            exit({:shutdown, :more})

          {:ok, _done} ->
            exit(:normal)

          {:error, reason} ->
            Logger.warning("[ankusa] quarantine retention failed, retrying: #{inspect(reason)}")
            exit(:normal)
        end
      end)

    {:noreply, %{state | pruning: ref}}
  end

  def handle_info(:retention, state) do
    schedule_retention(@retention_interval_ms)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{pruning: ref} = state) do
    schedule_retention(if reason == {:shutdown, :more}, do: 1_000, else: @retention_interval_ms)
    {:noreply, %{state | pruning: nil}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # ── helpers ───────────────────────────────────────────────────────────────

  defp held_bytes(instance) do
    %{lo: lo, hi: hi} = Keys.family(:quarantine)

    Store.fold(instance, :quarantine, {lo, hi}, 0, fn _key, value, bytes ->
      {:cont, bytes + summary_size(decode_summary(value))}
    end)
  end

  # An undecodable summary matches only a purge with no source/id filter, and
  # counts 0 bytes, as it did against the cap.
  defp purge_hits(instance, range, filter, limit) do
    result =
      Store.fold(instance, :quarantine, range, {0, []}, fn key, value, {n, hits} ->
        decoded = decode_summary(value)

        hit? =
          case decoded do
            {:ok, summary} -> matches?(filter, summary)
            :error -> unfiltered?(filter)
          end

        cond do
          not hit? -> {:cont, {n, hits}}
          n + 1 >= limit -> {:halt, {n + 1, [{key, summary_size(decoded)} | hits]}}
          true -> {:cont, {n + 1, [{key, summary_size(decoded)} | hits]}}
        end
      end)

    case result do
      {:ok, {_n, hits}} -> {:ok, Enum.reverse(hits)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp matches?(filter, summary) do
    keep?(filter, :source_id, Map.get(summary, :source_id)) and
      keep?(filter, :id, Map.get(summary, :id)) and tenant_ok?(filter, summary)
  end

  # An entry written before the pen kept the tenant (0.4), or imported from a
  # 0.3 data dir, has none: it belongs to `"default"`, as in
  # `Envelope.idempotency_key/1`.
  defp tenant_ok?(%{tenants: tenants}, summary) when is_list(tenants),
    do: (Map.get(summary, :tenant_id) || "default") in tenants

  defp tenant_ok?(_filter, _summary), do: true

  defp unfiltered?(filter),
    do:
      Map.get(filter, :source_id) == nil and Map.get(filter, :id) == nil and
        Map.get(filter, :tenants) == nil

  defp keep?(filter, key, value) do
    case Map.get(filter, key) do
      nil -> true
      wanted -> wanted == value
    end
  end

  defp summary_size({:ok, %{size: size}}) when is_integer(size) and size >= 0, do: size
  defp summary_size(_decoded), do: 0

  # Trusted, internally written data (the reason may be any term), but a
  # summary a scan cannot read must not wedge it.
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp decode_summary(value) do
    case :erlang.binary_to_term(value) do
      %{} = summary -> {:ok, summary}
      _ -> :error
    end
  rescue
    ArgumentError -> :error
  end

  defp decode_held(<<"AE", _::binary>> = bin, _received_at, _id, _summary),
    do: Envelope.from_binary(bin)

  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp decode_held(bin, received_at, id, summary) do
    case :erlang.binary_to_term(bin) do
      %{method: _} = map ->
        {:ok, struct(Envelope, map)}

      %{headers: headers, body: body} when is_list(headers) and is_binary(body) ->
        env = %Envelope{
          id: id,
          source_id: Map.get(summary, :source_id),
          tenant_id: nil,
          received_at: received_at,
          method: "POST",
          path: "/",
          headers: headers,
          body: body,
          size: byte_size(body)
        }

        {:ok, %{env | content_type: Envelope.header(env, "content-type")}}

      _ ->
        {:error, :undecodable}
    end
  rescue
    ArgumentError -> {:error, :undecodable}
  end

  # Bucket rows `{source_id, tat}` and the counters `{:bytes, n}`, `{:pending,
  # n}`: both written by request processes, owned (and recreated at a restart)
  # by the pen.
  # The instance name is operator config: one atom per instance, never request input.
  # sobelow_skip ["DOS.BinToAtom"]
  defp buckets_table(instance), do: :"ankusa_quarantine_buckets_#{instance}"

  # The instance name is operator config: one atom per instance, never request input.
  # sobelow_skip ["DOS.BinToAtom"]
  defp counters_table(instance), do: :"ankusa_quarantine_counters_#{instance}"

  defp new_table(name),
    do:
      :ets.new(name, [
        :set,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])

  # Counters never go below zero, and a pen that is not running has nothing to
  # give back to.
  defp give_back(table, key, amount) do
    :ets.update_counter(table, key, {2, -amount, 0, 0})
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp schedule_sweep, do: Process.send_after(self(), :sweep, @sweep_interval_ms)
  defp schedule_retention(ms), do: Process.send_after(self(), :retention, ms)

  defp prune_chunks(_instance, _range, 0, acc), do: {:ok, %{acc | more: true}}

  defp prune_chunks(instance, range, left, acc) do
    with {:ok, hits} <- purge_hits(instance, range, %{}, @retention_chunk) do
      case hits do
        [] ->
          {:ok, acc}

        hits ->
          pruned = prune_hits(instance, hits)

          with {:ok, bytes} <- pruned do
            acc = %{acc | deleted: acc.deleted + length(hits), bytes: acc.bytes + bytes}

            if length(hits) < @retention_chunk,
              do: {:ok, acc},
              else: prune_chunks(instance, range, left - 1, acc)
          end
      end
    end
  end

  # One chunk, in one synced batch with its audit row: `{:ok, bytes}`.
  defp prune_hits(instance, hits) do
    ops = Enum.flat_map(hits, fn {key, _size} -> delete_ops(key) end)
    bytes = hits |> Enum.map(&elem(&1, 1)) |> Enum.sum()
    <<?s, oldest::64, _::binary>> = hits |> hd() |> elem(0)
    <<?s, newest::64, _::binary>> = hits |> List.last() |> elem(0)

    attrs = %{
      action: "quarantine.retention",
      resource: "quarantine",
      detail: %{
        "deleted" => length(hits),
        "bytes" => bytes,
        "oldest_received_at" => oldest,
        "newest_received_at" => newest
      }
    }

    with :ok <- Audit.commit(instance, Context.system(), attrs, ops) do
      released(instance, bytes)

      Ankusa.Telemetry.emit([:quarantine, :pruned], %{count: length(hits), bytes: bytes}, %{
        instance: instance
      })

      {:ok, bytes}
    end
  end
end
