defmodule Ankusa.ClaimCheck.Stores do
  @moduledoc """
  The object stores claim packs live in — `claim_check.blob_stores`, else
  `[storage.blob_store]` — and which of them a pack is written to and read
  from. Stores are named by their index in that list. Claim keys are never
  prefixed, so one gateway reads every node's claims.

  ## Placement

  `claim_check.placement` picks a new pack's *home* store:

    * `:ordered` — the first store; the others take packs only while it is
      down.
    * `:hash` — the pack id's 64 random bits modulo the number of stores, so
      packs spread evenly over every store; the next stores in list order
      take over while a pack's home is down.

  A write tries the home store, then each next one in list order, wrapping
  around, and stops at the first that takes the pack. The store a pack landed
  in is not recorded anywhere: a ref names only a tenant and a claim id.

  ## Breaker

  A store whose write fails is skipped by later writes for
  `claim_check.store_cooldown_ms`, then tried again; a success closes it. When
  every store is down, every store is tried anyway — a write is never refused
  untried. The cooldown is fixed: the stores back each other up, and the
  dispatch retry policy backs the whole write. Each open and close emits
  `[:ankusa, :claim_check, :store_breaker]`. Breakers live in an ETS table
  this process owns; a restart closes them all.

  ## Reads

  A read searches: every store, in the pack's write order, at the configured
  `claim_check.key_layout`, then every store again at the other layout — so a
  pack a failover put elsewhere, or one written before `key_layout` or
  `placement` changed, is still found. Reads never consult the breaker. A miss
  is reported as `:not_found` only when every store answered; if any store
  could not, the read is `{:unavailable, reason}` (or `:forbidden`), the first
  such error, so a claim is never declared gone while it may be in a store that
  did not answer.

  Adding a store moves `:hash` homes (the search still finds older packs).
  Removing a store makes the packs in it unreadable: remove one only after
  its packs have aged out of retention.
  """

  use GenServer

  require Logger

  alias Ankusa.{Config, Telemetry, ULID}

  @typedoc "A claim store: its index in `claim_check.blob_stores`, module and options."
  @type store :: {index :: non_neg_integer(), module(), keyword()}

  @type read_error :: :not_found | :forbidden | {:unavailable, term()}

  # ── process ─────────────────────────────────────────────────────────────────

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, instance, name: Ankusa.via(instance, :claim_stores))
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @impl true
  def init(instance) do
    # `:public`: every writer (dispatch tasks, direct-mode edge requests)
    # opens and closes breakers itself; this process only owns the table.
    :ets.new(table(instance), [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, %{instance: instance}}
  end

  # ── stores ──────────────────────────────────────────────────────────────────

  @doc "The claim stores, indexed in config order."
  @spec list(Config.t()) :: [store()]
  def list(%Config{claim_check: cc, storage: storage}) do
    (cc.blob_stores || [storage.blob_store])
    |> Enum.with_index(fn {mod, opts}, index -> {index, mod, opts} end)
  end

  @doc "The index of the store a new pack `pack_id` goes to first (see Placement)."
  @spec home(Config.t(), String.t()) :: non_neg_integer()
  def home(%Config{claim_check: %{placement: :ordered}}, _pack_id), do: 0

  def home(%Config{claim_check: %{placement: :hash}} = config, pack_id) do
    {:ok, <<_ms::48, entropy::64, _position::16>>} = ULID.decode(pack_id)
    rem(entropy, length(list(config)))
  end

  # ── writes ──────────────────────────────────────────────────────────────────

  @doc """
  Write a pack's bytes under `key`: the home store first, then the next ones,
  skipping stores whose breaker is open (unless all are). Returns the index of
  the store that took it. When every attempt fails: `{:unavailable, reason}`
  after a single attempt, else `{:unavailable, [{index, reason}, ...]}` in
  attempt order.
  """
  @spec write(atom(), Config.t(), String.t(), String.t(), iodata()) ::
          {:ok, non_neg_integer()} | {:error, {:unavailable, term()}}
  def write(instance, %Config{} = config, pack_id, key, data) do
    order = order(config, pack_id)

    candidates =
      case Enum.reject(order, fn {index, _mod, _opts} -> down?(instance, index) end) do
        [] -> order
        up -> up
      end

    candidates
    |> Enum.reduce_while([], fn {index, mod, opts}, failures ->
      case attempt(mod, instance, key, data, opts) do
        :ok ->
          mark_up(instance, index)
          {:halt, {:ok, index}}

        {:error, reason} ->
          mark_down(instance, config, index, reason)
          {:cont, [{index, reason} | failures]}
      end
    end)
    |> case do
      {:ok, index} -> {:ok, index}
      [{_index, reason}] -> {:error, {:unavailable, reason}}
      failures -> {:error, {:unavailable, Enum.reverse(failures)}}
    end
  end

  # A blob store is external code, and so is a `:token_provider` it calls (a
  # `GenServer.call` into Goth or the like): a raise, exit, throw or a stray
  # return in its write is an unavailable store, not a crashed caller. Under
  # `wal: :none` the caller is the request.
  defp attempt(mod, instance, key, data, opts) do
    case mod.put(instance, key, data, opts) do
      :ok -> :ok
      {:error, _reason} = error -> error
      other -> {:error, {:bad_return, other}}
    end
  rescue
    error -> {:error, error}
  catch
    :exit, reason -> {:error, {:exit, reason}}
    :throw, value -> {:error, {:throw, value}}
  end

  # ── reads ───────────────────────────────────────────────────────────────────

  @doc """
  Find pack `pack_id` under the first of `keys` any store has, and read
  `length` bytes of it at `offset`: each key in turn, across every store in
  the pack's write order (see Reads). Returns the bytes with the store index
  and key they came from.
  """
  @spec read_range(atom(), Config.t(), String.t(), [String.t()], non_neg_integer(), pos_integer()) ::
          {:ok, binary(), non_neg_integer(), String.t()} | {:error, read_error()}
  def read_range(instance, %Config{} = config, pack_id, keys, offset, length) do
    order = order(config, pack_id)

    # Layout-major: every store at the configured layout before any at the
    # other one.
    for(key <- keys, {index, mod, opts} <- order, do: {key, index, mod, opts})
    |> Enum.reduce_while(nil, fn {key, index, mod, opts}, error ->
      case get(mod, instance, key, offset, length, opts) do
        {:ok, bin} -> {:halt, {:found, bin, index, key}}
        {:error, :not_found} -> {:cont, error}
        {:error, reason} -> {:cont, error || reason}
      end
    end)
    |> case do
      {:found, bin, index, key} -> {:ok, bin, index, key}
      nil -> {:error, :not_found}
      error -> {:error, error}
    end
  end

  @doc """
  `length` bytes at `offset` of `key` in store `index`: one ranged read. A
  store index past the end of the list (one removed since the claim was
  located) is `:not_found`.
  """
  @spec get_range(
          atom(),
          Config.t(),
          non_neg_integer(),
          String.t(),
          non_neg_integer(),
          pos_integer()
        ) ::
          {:ok, binary()} | {:error, read_error()}
  def get_range(instance, %Config{} = config, index, key, offset, length) do
    case Enum.at(list(config), index) do
      {^index, mod, opts} -> get(mod, instance, key, offset, length, opts)
      nil -> {:error, :not_found}
    end
  end

  # A read that starts past the end of an object comes back empty (LocalFS
  # answers :eof; S3 and GCS answer 416): the object is there. One that runs
  # past the end comes back short; the caller decides what a short read means.
  # A 403 is the store refusing this node's credential (an S3 credential
  # without `s3:ListBucket` answers 403 for a missing key, too).
  defp get(mod, instance, key, offset, length, opts) do
    case mod.get_range(instance, key, offset, length, opts) do
      {:ok, bin} -> {:ok, bin}
      {:error, :not_found} -> {:error, :not_found}
      {:error, :eof} -> {:ok, <<>>}
      {:error, {:status, 416, _body}} -> {:ok, <<>>}
      {:error, {:status, 403, _body}} -> {:error, :forbidden}
      {:error, reason} -> {:error, {:unavailable, reason}}
    end
  end

  # ── breaker ─────────────────────────────────────────────────────────────────

  # The stores in the order a pack's writes try them, and its reads search
  # them: its home, then each next one, wrapping around.
  defp order(config, pack_id) do
    stores = list(config)
    home = home(config, pack_id)
    Enum.drop(stores, home) ++ Enum.take(stores, home)
  end

  # No table (an instance whose tree isn't running, e.g. a test that only
  # called `Ankusa.put_config/1`): no breakers, every store is up.
  defp down?(instance, index) do
    case :ets.lookup(table(instance), index) do
      [{^index, until}] -> until > now_ms()
      [] -> false
    end
  rescue
    ArgumentError -> false
  end

  # `insert_new` decides the transition, so concurrent failures of one store
  # emit one `:open`; a store already open (or past its cooldown and failing
  # again) only has its cooldown pushed out.
  defp mark_down(instance, config, index, reason) do
    cooldown = config.claim_check.store_cooldown_ms
    entry = {index, now_ms() + cooldown}

    if :ets.insert_new(table(instance), entry) do
      Logger.warning(
        "[ankusa] claim store #{index} failed a write " <>
          "(#{inspect(reason, limit: 10, printable_limit: 256)}); " <>
          "writes skip it for #{cooldown} ms"
      )

      breaker_event(instance, index, :open, reason)
    else
      :ets.insert(table(instance), entry)
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  # `take` is atomic too: one close per open.
  defp mark_up(instance, index) do
    case :ets.take(table(instance), index) do
      [] -> :ok
      [_entry] -> breaker_event(instance, index, :closed, nil)
    end
  rescue
    ArgumentError -> :ok
  end

  defp breaker_event(instance, index, state, reason) do
    Telemetry.emit(
      [:claim_check, :store_breaker],
      %{},
      %{instance: instance, store: index, state: state, reason: reason}
    )
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp table(instance), do: :"ankusa_claim_stores_#{instance}"
end
