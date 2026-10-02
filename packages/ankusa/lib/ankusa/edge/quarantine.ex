defmodule Ankusa.Edge.Quarantine do
  @moduledoc """
  A rate-limited durable holding pen for envelopes that failed verification under
  a `:quarantine` policy. A bad secret rotation should never silently eat real
  events — but a flood of forged requests should never be able to fill the disk
  either, so writes are token-bucket rate limited.

  Each record is two keys in this node's `Ankusa.Store`, committed in one synced
  batch: a small summary (id, source, time, reason) that `recent/2` lists, and
  the headers and body, which only an operator who goes to the store for them
  reads back. `put/3` answers `:ok` only once both are on disk.
  """

  use GenServer

  require Logger

  alias Ankusa.{Envelope, Store}
  alias Ankusa.Store.Keys

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :quarantine))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc """
  Durably record a quarantined envelope. Returns `:ok`, `:rate_limited`, or
  `{:error, :store_unavailable}` when the store could not take the write (no
  token is spent).
  """
  @spec put(atom(), Envelope.t(), term()) :: :ok | :rate_limited | {:error, :store_unavailable}
  def put(instance, env, reason) do
    GenServer.call(Ankusa.via(instance, :quarantine), {:put, env, reason})
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

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)

    # token bucket: `burst` tokens, refilled `rate` per second
    {:ok,
     %{
       instance: config.instance,
       tokens: 100.0,
       burst: 100.0,
       rate: 20.0,
       last: mono_ms()
     }}
  end

  @impl true
  def handle_call({:put, env, reason}, _from, state) do
    state = refill(state)

    if state.tokens >= 1.0 do
      case write(state.instance, env, reason) do
        :ok ->
          {:reply, :ok, %{state | tokens: state.tokens - 1.0}}

        {:error, error} ->
          Logger.error("[ankusa] quarantine write failed: #{inspect(error)}")
          {:reply, {:error, :store_unavailable}, state}
      end
    else
      Ankusa.Telemetry.emit([:quarantine, :rate_limited], %{}, %{
        instance: state.instance,
        source_id: env.source_id
      })

      {:reply, :rate_limited, state}
    end
  end

  defp write(instance, env, reason) do
    summary = %{
      id: env.id,
      source_id: env.source_id,
      received_at: env.received_at,
      reason: reason
    }

    held = %{headers: env.headers, body: env.body}

    Store.write(
      instance,
      [
        {:put, :quarantine, Keys.quarantine_summary(env.received_at, env.id),
         :erlang.term_to_binary(summary)},
        {:put, :quarantine, Keys.quarantine_body(env.received_at, env.id),
         :erlang.term_to_binary(held)}
      ],
      sync: true
    )
  end

  defp refill(state) do
    now = mono_ms()
    elapsed = (now - state.last) / 1000.0
    tokens = min(state.burst, state.tokens + elapsed * state.rate)
    %{state | tokens: tokens, last: now}
  end

  defp mono_ms, do: System.monotonic_time(:millisecond)
end
