defmodule Ankusa.Edge.Quarantine do
  @moduledoc """
  A rate-limited durable holding pen for envelopes that failed verification under
  a `:quarantine` policy. A bad secret rotation should never silently eat real
  events — but a flood of forged requests should never be able to fill the disk
  either, so writes are token-bucket rate limited.

  Records are appended to `quarantine/quarantine.log` (length-prefixed terms) and
  the most recent are kept in memory for the dashboard.
  """

  use GenServer

  alias Ankusa.{Config, Envelope}

  @keep_recent 200

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :quarantine))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc "Durably record a quarantined envelope. Returns `:ok` or `:rate_limited`."
  @spec put(atom(), Envelope.t(), term()) :: :ok | :rate_limited
  def put(instance, env, reason) do
    GenServer.call(Ankusa.via(instance, :quarantine), {:put, env, reason})
  end

  @doc "List recent quarantined entries (newest first)."
  @spec recent(atom()) :: [map()]
  def recent(instance), do: GenServer.call(Ankusa.via(instance, :quarantine), :recent)

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    path = Config.path(config, "quarantine/quarantine.log")
    File.mkdir_p!(Path.dirname(path))
    {:ok, fd} = :file.open(path, [:append, :raw, :binary])

    # token bucket: `burst` tokens, refilled `rate` per second
    {:ok,
     %{
       instance: config.instance,
       fd: fd,
       recent: [],
       tokens: 100.0,
       burst: 100.0,
       rate: 20.0,
       last: mono_ms()
     }}
  end

  @impl true
  def terminate(_reason, %{fd: fd}), do: :file.close(fd)

  @impl true
  def handle_call({:put, env, reason}, _from, state) do
    state = refill(state)

    if state.tokens >= 1.0 do
      record = %{
        id: env.id,
        source_id: env.source_id,
        received_at: env.received_at,
        reason: reason,
        headers: env.headers,
        body: env.body
      }

      :ok = :file.write(state.fd, framed(record))
      :ok = :file.datasync(state.fd)

      summary = Map.drop(record, [:body, :headers])
      recent = Enum.take([summary | state.recent], @keep_recent)
      {:reply, :ok, %{state | tokens: state.tokens - 1.0, recent: recent}}
    else
      Ankusa.Telemetry.emit([:quarantine, :rate_limited], %{}, %{
        instance: state.instance,
        source_id: env.source_id
      })

      {:reply, :rate_limited, state}
    end
  end

  def handle_call(:recent, _from, state), do: {:reply, state.recent, state}

  defp refill(state) do
    now = mono_ms()
    elapsed = (now - state.last) / 1000.0
    tokens = min(state.burst, state.tokens + elapsed * state.rate)
    %{state | tokens: tokens, last: now}
  end

  defp framed(term) do
    bin = :erlang.term_to_binary(term)
    <<byte_size(bin)::32, bin::binary>>
  end

  defp mono_ms, do: System.monotonic_time(:millisecond)
end
