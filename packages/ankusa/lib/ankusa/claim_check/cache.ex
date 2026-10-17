defmodule Ankusa.ClaimCheck.Cache do
  @moduledoc """
  The claim gateway's memory of what it has read: pack indexes and whole
  claim bodies, so a repeat read never reaches the object store.

  ## What is cached

    * a pack — the store and key it was found under, and its first bytes
      (`Ankusa.ClaimCheck.Pack`'s index), with how many bytes were asked for
      (`read`), so a short prefix (a small pack) is told apart from one that
      was never asked for that far. Every later claim of the pack is located
      without a store read. Kept 10 minutes.
    * a claim's whole bytes, when they are at most
      `claim_check.read_cache_max_entry_bytes`; larger claims are served and
      never inserted, and ranged reads are never cached. Kept 2 minutes.

  Packs and claims are written once and never rewritten, so an entry is never
  wrong while its pack exists. The only staleness is a pack swept by
  retention: a cached index answers `HEAD` `200` for up to 10 minutes after
  the sweep, a cached claim is served for up to 2, and a `GET` whose bytes
  read answers `:not_found` forgets the pack (`Ankusa.ClaimCheck.read_bytes/5`).

  ## Memory

  `claim_check.read_cache_bytes` bounds what is held, counted as each entry's
  bytes plus a fixed per-entry overhead. The entries live in two generations,
  each an ETS table: inserts go to the newer one, and when it holds half the
  budget it becomes the older one and the previous older one is dropped whole.
  A hit in the older generation is copied into the newer one (and counted
  again), so what keeps being read keeps living. Every 10 minutes the
  generations turn over regardless, so an idle cache empties within 20.

  An insert reserves its bytes before it lands, and one that finds the newer
  generation already full is dropped (the next read goes to the store), so a
  generation never holds more than half the budget plus the one entry that
  filled it. That insert asks this process for the turnover; inserts dropped
  until it is handled are what keeps the bound while it is pending.

  `Ankusa.Routes.Cache` uses Nebulex; this cache can't. Nebulex bounds memory
  by `:ets.info(tab, :memory)`, which counts a binary longer than 64 bytes as
  the few words of its reference, never its bytes (a thousand cached 50 KiB
  claims measure about 140 KB), and every claim and pack index here is such a
  binary.

  Instance-scoped: this process registers under
  `Ankusa.via(instance, :claim_cache)` and owns the tables. When it isn't
  running (no `:claim_check` role, or `read_cache_bytes: 0`) every lookup
  misses and every insert is dropped.
  """

  use GenServer

  @index_ttl :timer.minutes(10)
  @body_ttl :timer.minutes(2)
  @turnover_ms :timer.minutes(10)
  # The ETS tuple, the key's strings, and the binary's own header, roughly.
  @entry_overhead 256

  @typedoc """
  A located pack: the claim store it was found in, its key there, its first
  bytes, and how many bytes were asked for (`prefix` is shorter only when the
  object is).
  """
  @type pack :: %{
          store: non_neg_integer(),
          key: String.t(),
          prefix: binary(),
          read: pos_integer()
        }

  # ── process ─────────────────────────────────────────────────────────────────

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, instance, name: Ankusa.via(instance, :claim_cache))
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @impl true
  def init(instance) do
    half = div(Ankusa.config(instance).claim_check.read_cache_bytes, 2)
    # `:protected`: only this process turns generations over; every reader
    # looks the current pair up here.
    :ets.new(table(instance), [:named_table, :protected, :set, read_concurrency: true])
    :ets.insert(table(instance), {:gens, generation(), nil, half})
    Process.send_after(self(), :turnover, @turnover_ms)
    {:ok, %{instance: instance}}
  end

  @impl true
  def handle_cast({:turn_over, full}, state) do
    case gens(state.instance) do
      {^full, _older, _half} -> turn_over(state.instance)
      _already -> :ok
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:turnover, state) do
    turn_over(state.instance)
    Process.send_after(self(), :turnover, @turnover_ms)
    {:noreply, state}
  end

  defp turn_over(instance) do
    {newer, older, half} = gens(instance)
    :ets.insert(table(instance), {:gens, generation(), newer, half})
    if older, do: :ets.delete(elem(older, 0))
    :ok
  end

  # A generation: its table, and the bytes inserted into it. `:public`: every
  # request process inserts itself.
  defp generation do
    table =
      :ets.new(:ankusa_claim_cache_gen, [
        :public,
        :set,
        read_concurrency: true,
        write_concurrency: true
      ])

    {table, :atomics.new(1, signed: false)}
  end

  # ── packs ───────────────────────────────────────────────────────────────────

  @doc "The cached pack `pack_id` of `tenant_id`, if any."
  @spec pack(atom(), String.t(), String.t()) :: {:ok, pack()} | :error
  def pack(instance, tenant_id, pack_id), do: get(instance, {:pack, tenant_id, pack_id})

  @doc "Cache a located pack, or do nothing if the cache isn't running."
  @spec put_pack(atom(), String.t(), String.t(), pack()) :: :ok
  def put_pack(instance, tenant_id, pack_id, pack) do
    pack = %{pack | prefix: own(pack.prefix)}
    weight = byte_size(pack.prefix) + byte_size(pack.key)
    put(instance, {:pack, tenant_id, pack_id}, pack, weight, @index_ttl)
  end

  @doc "Forget a pack (one that turned out to be gone), if it was cached."
  @spec forget_pack(atom(), String.t(), String.t()) :: :ok
  def forget_pack(instance, tenant_id, pack_id) do
    {{newer, _bytes}, older, _half} = gens(instance)
    key = {:pack, tenant_id, pack_id}
    :ets.delete(newer, key)
    if older, do: :ets.delete(elem(older, 0), key)
    :ok
  rescue
    # not running, or a generation dropped under us
    ArgumentError -> :ok
  end

  # ── bodies ──────────────────────────────────────────────────────────────────

  @doc "The cached whole bytes of the claim at `index` of pack `pack_id`, if any."
  @spec body(atom(), String.t(), String.t(), non_neg_integer()) :: {:ok, binary()} | :error
  def body(instance, tenant_id, pack_id, index),
    do: get(instance, {:body, tenant_id, pack_id, index})

  @doc """
  Cache a claim's whole bytes, or do nothing if the cache isn't running. The
  caller keeps them under `claim_check.read_cache_max_entry_bytes`.
  """
  @spec put_body(atom(), String.t(), String.t(), non_neg_integer(), binary()) :: :ok
  def put_body(instance, tenant_id, pack_id, index, bin) do
    bin = own(bin)
    put(instance, {:body, tenant_id, pack_id, index}, bin, byte_size(bin), @body_ttl)
  end

  @doc false
  # Bytes held across both generations, as the budget counts them (each
  # entry's bytes plus the overhead); 0 when the cache isn't running.
  @spec bytes(atom()) :: non_neg_integer()
  def bytes(instance) do
    {{_newer, newer_bytes}, older, _half} = gens(instance)
    :atomics.get(newer_bytes, 1) + if(older, do: :atomics.get(elem(older, 1), 1), else: 0)
  rescue
    ArgumentError -> 0
  end

  # A body from an HTTP adapter can be a slice of a larger buffer, and ETS
  # would keep the whole buffer alive: store, and count, only the bytes.
  defp own(bin) do
    if :binary.referenced_byte_size(bin) > byte_size(bin), do: :binary.copy(bin), else: bin
  end

  # ── tables ──────────────────────────────────────────────────────────────────

  # Entries are `{key, value, expires_at_ms, weight}`. An expired entry is a
  # miss; it goes when its generation does.
  defp get(instance, key) do
    {{newer, _bytes} = gen, older, half} = gens(instance)
    now = now_ms()

    case :ets.lookup(newer, key) do
      [{^key, value, expires, _weight}] when expires > now ->
        {:ok, value}

      [_expired] ->
        :error

      [] when older != nil ->
        case :ets.lookup(elem(older, 0), key) do
          [{^key, value, expires, _weight} = entry] when expires > now ->
            insert(instance, gen, entry, half)
            {:ok, value}

          _miss ->
            :error
        end

      [] ->
        :error
    end
  rescue
    ArgumentError -> :error
  end

  defp put(instance, key, value, weight, ttl) do
    {gen, _older, half} = gens(instance)
    insert(instance, gen, {key, value, now_ms() + ttl, weight + @entry_overhead}, half)
  rescue
    ArgumentError -> :ok
  end

  # The bytes are reserved first: a generation that already holds half the
  # budget takes nothing more (and gives the reservation back, so the count
  # is what the table holds), and only the insert that filled it asks for
  # the turnover, so one is asked for per generation.
  defp insert(instance, {table, bytes} = gen, {_key, _value, _expires, weight} = entry, half) do
    total = :atomics.add_get(bytes, 1, weight)

    cond do
      total - weight >= half ->
        :atomics.sub(bytes, 1, weight)

      total >= half ->
        :ets.insert(table, entry)
        GenServer.cast(Ankusa.via(instance, :claim_cache), {:turn_over, gen})

      true ->
        :ets.insert(table, entry)
    end

    :ok
  end

  # Raises `ArgumentError` when the cache isn't running.
  defp gens(instance) do
    [{:gens, newer, older, half}] = :ets.lookup(table(instance), :gens)
    {newer, older, half}
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp table(instance), do: :"ankusa_claim_cache_#{instance}"
end
