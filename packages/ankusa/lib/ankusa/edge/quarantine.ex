defmodule Ankusa.Edge.Quarantine do
  @moduledoc """
  A durable holding pen for envelopes that failed verification under a
  `:quarantine` policy. A bad secret rotation should never silently eat real
  events, and the pen is how they come back: a `kind: :quarantine` replay job
  (`Ankusa.Replay`) re-verifies each held hook against its source's *current*
  verifier and commits the ones that now pass; `purge/3` drops the rest.

  A flood of forged requests must not fill the disk, or starve another
  source's real failures of room, so the pen is bounded twice
  (`config.quarantine`):

    * one token bucket per source — `burst` tokens, refilled `rate` per
      second. Over it, `put/3` answers `{:rate_limited, retry_after_ms}`
      (`429 quarantine_rate_limited`).
    * a cap on the pen's total bytes, `max_bytes`. A write that would cross
      it answers `:full` (`503 quarantine_full`). A full pen refuses new
      hooks; it never evicts one it already acked.

  Each record is two keys in this node's `Ankusa.Store`, committed in one synced
  batch: a small summary (id, source, tenant, time, reason, size) that
  `recent/2` and `page/5` list, and the whole envelope, which `envelope/2`
  reads back. `put/3` answers `:ok` only once both are on disk.
  """

  use GenServer

  require Logger

  alias Ankusa.{Config, Envelope, Store}
  alias Ankusa.Store.Keys

  # Idle buckets are dropped this often (a missing bucket is a full one).
  @sweep_interval_ms 60_000
  @max_ms 0xFFFF_FFFF_FFFF_FFFF
  @purge_timeout_ms 60_000

  @typedoc "`:source_id`/`:id` match the summary; `:since`/`:until` (ms, inclusive) bound `received_at`."
  @type filter :: %{
          optional(:source_id) => String.t() | nil,
          optional(:id) => String.t() | nil,
          optional(:since) => non_neg_integer() | nil,
          optional(:until) => non_neg_integer() | nil
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
  when the entry would take the pen past `max_bytes` (no token is spent); or
  `{:error, :store_unavailable}` when the store could not take the write (no
  token is spent).
  """
  @spec put(atom(), Envelope.t(), term()) ::
          :ok | {:rate_limited, pos_integer()} | :full | {:error, :store_unavailable}
  def put(instance, env, reason) do
    GenServer.call(Ankusa.via(instance, :quarantine), {:put, env, reason})
  end

  @doc """
  Tell the pen `bytes` left it outside `purge/3` — a release commit deleted
  their entries. A no-op when the pen is not running on this node.
  """
  @spec released(atom(), non_neg_integer()) :: :ok
  def released(_instance, 0), do: :ok

  def released(instance, bytes) when is_integer(bytes) and bytes > 0 do
    case Ankusa.whereis(instance, :quarantine) do
      nil -> :ok
      pid -> GenServer.cast(pid, {:released, bytes})
    end
  end

  @doc """
  Delete up to `limit` entries matching `filter`, oldest first, in one synced
  batch. Returns how many went and the bytes they held. A pen that is not
  running (its domain restarting) is `{:error, :store_unavailable}`.
  """
  @spec purge(atom(), filter(), pos_integer()) ::
          {:ok, %{deleted: non_neg_integer(), bytes: non_neg_integer()}}
          | {:error, :store_unavailable}
  def purge(instance, filter, limit) when is_map(filter) and is_integer(limit) and limit > 0 do
    GenServer.call(Ankusa.via(instance, :quarantine), {:purge, filter, limit}, @purge_timeout_ms)
  catch
    :exit, _reason -> {:error, :store_unavailable}
  end

  @doc """
  The most recent quarantined entries, newest first, at most `limit`. Reads the
  store directly, so it never queues behind a write.
  """
  @spec recent(atom(), non_neg_integer()) :: {:ok, [map()]} | {:error, term()}
  def recent(_instance, 0), do: {:ok, []}

  def recent(instance, limit) when is_integer(limit) and limit > 0 do
    %{lo: lo, hi: hi} = Keys.family(:quarantine)

    result =
      Store.fold(
        instance,
        :quarantine,
        {lo, hi},
        {0, []},
        fn _key, value, {n, acc} ->
          # Trusted, internally written data; the reason may be any term.
          acc = [:erlang.binary_to_term(value) | acc]
          if n + 1 >= limit, do: {:halt, {n + 1, acc}}, else: {:cont, {n + 1, acc}}
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

  # ── server ────────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    %{burst: burst, rate: rate, max_bytes: max_bytes} = config.quarantine

    # The cap is only as good as the count: refuse to start rather than guess
    # an empty pen.
    case held_bytes(config.instance) do
      {:ok, bytes} ->
        schedule_sweep()

        {:ok,
         %{
           instance: config.instance,
           burst: burst * 1.0,
           rate: rate * 1.0,
           max_bytes: max_bytes,
           bytes: bytes,
           buckets: %{}
         }}

      {:error, reason} ->
        {:stop, {:quarantine_init_failed, reason}}
    end
  end

  @impl true
  def handle_call({:put, env, reason}, _from, state) do
    source_id = env.source_id
    bucket = refill(Map.get(state.buckets, source_id), state, mono_ms())

    if bucket.tokens < 1.0 do
      Ankusa.Telemetry.emit([:quarantine, :rate_limited], %{}, %{
        instance: state.instance,
        source_id: source_id
      })

      retry_after_ms = max(ceil((1.0 - bucket.tokens) * 1000 / state.rate), 1)
      {:reply, {:rate_limited, retry_after_ms}, put_bucket(state, source_id, bucket)}
    else
      held = Envelope.to_binary(%{env | seq: nil})

      {summary, size} =
        sized(
          %{
            id: env.id,
            source_id: source_id,
            tenant_id: env.tenant_id,
            received_at: env.received_at,
            reason: reason
          },
          held
        )

      if state.bytes + size > state.max_bytes do
        Ankusa.Telemetry.emit([:quarantine, :full], %{}, %{
          instance: state.instance,
          source_id: source_id
        })

        {:reply, :full, put_bucket(state, source_id, bucket)}
      else
        case write(state.instance, env, summary, held) do
          :ok ->
            state = put_bucket(state, source_id, %{bucket | tokens: bucket.tokens - 1.0})
            {:reply, :ok, %{state | bytes: state.bytes + size}}

          {:error, error} ->
            Logger.error("[ankusa] quarantine write failed: #{inspect(error)}")
            {:reply, {:error, :store_unavailable}, put_bucket(state, source_id, bucket)}
        end
      end
    end
  end

  def handle_call({:purge, filter, limit}, _from, state) do
    {since, until} = {Map.get(filter, :since), Map.get(filter, :until)}

    with {:ok, hits} <- purge_hits(state.instance, range(since, until), filter, limit),
         ops = Enum.flat_map(hits, fn {key, _size} -> delete_ops(key) end),
         :ok <- Store.write(state.instance, ops, sync: true) do
      bytes = hits |> Enum.map(&elem(&1, 1)) |> Enum.sum()

      {:reply, {:ok, %{deleted: length(hits), bytes: bytes}},
       %{state | bytes: max(state.bytes - bytes, 0)}}
    else
      {:error, error} ->
        Logger.error("[ankusa] quarantine purge failed: #{inspect(error)}")
        {:reply, {:error, :store_unavailable}, state}
    end
  end

  @impl true
  def handle_cast({:released, bytes}, state) do
    {:noreply, %{state | bytes: max(state.bytes - bytes, 0)}}
  end

  @impl true
  def handle_info(:sweep, state) do
    now = mono_ms()

    buckets =
      state.buckets
      |> Enum.map(fn {source_id, bucket} -> {source_id, refill(bucket, state, now)} end)
      |> Enum.filter(fn {_source_id, bucket} -> bucket.tokens < state.burst end)
      |> Map.new()

    schedule_sweep()
    {:noreply, %{state | buckets: buckets}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # ── helpers ───────────────────────────────────────────────────────────────

  defp write(instance, env, summary, held) do
    Store.write(
      instance,
      [
        {:put, :quarantine, Keys.quarantine_summary(env.received_at, env.id), summary},
        {:put, :quarantine, Keys.quarantine_body(env.received_at, env.id), held}
      ],
      sync: true
    )
  end

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
            :error -> Map.get(filter, :source_id) == nil and Map.get(filter, :id) == nil
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
      keep?(filter, :id, Map.get(summary, :id))
  end

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
  defp decode_summary(value) do
    case :erlang.binary_to_term(value) do
      %{} = summary -> {:ok, summary}
      _ -> :error
    end
  rescue
    ArgumentError -> :error
  end

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

  defp refill(nil, state, now), do: %{tokens: state.burst, last: now}

  defp refill(bucket, state, now) do
    elapsed = (now - bucket.last) / 1000.0
    %{tokens: min(state.burst, bucket.tokens + elapsed * state.rate), last: now}
  end

  defp put_bucket(state, source_id, bucket),
    do: %{state | buckets: Map.put(state.buckets, source_id, bucket)}

  defp schedule_sweep, do: Process.send_after(self(), :sweep, @sweep_interval_ms)

  defp mono_ms, do: System.monotonic_time(:millisecond)
end
