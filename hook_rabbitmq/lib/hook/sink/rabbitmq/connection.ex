defmodule Hook.Sink.RabbitMQ.Connection do
  @moduledoc """
  One AMQP connection + confirm-mode channel per instance. Declares the
  configured exchange once at connect time (idempotent) — never a queue;
  queue ownership belongs to consumers, not this sink.

  Publishes are confirmed: every `publish/5` blocks for the broker's ack
  before returning `:ok`, so a return value the dispatch pipeline trusts as
  "delivered" really was persisted by RabbitMQ, not just handed to a socket.

  Connection loss is not treated as a crash — this GenServer stays up and
  retries on a timer, replying `{:error, :not_connected}` to publishes in the
  meantime. That error already flows into the source's `Hook.RetryPolicy`
  exactly like any other sink failure, so there is no separate reconnect
  policy to get wrong.
  """

  use GenServer
  require Logger

  @default_retry_ms 5_000
  @default_confirm_timeout_ms 5_000

  # ── public API ──────────────────────────────────────────────────────────

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance)},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent
    }
  end

  @spec publish(GenServer.server(), String.t(), binary(), keyword()) ::
          :ok | {:error, term()}
  def publish(server, routing_key, payload, headers \\ []) do
    GenServer.call(server, {:publish, routing_key, payload, headers}, 15_000)
  end

  # ── GenServer ───────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    state = %{
      instance: Keyword.fetch!(opts, :instance),
      url: Keyword.get(opts, :url, "amqp://guest:guest@localhost:5672"),
      exchange: Keyword.fetch!(opts, :exchange),
      exchange_type: Keyword.get(opts, :exchange_type, :topic),
      retry_ms: Keyword.get(opts, :retry_ms, @default_retry_ms),
      confirm_timeout_ms: Keyword.get(opts, :confirm_timeout_ms, @default_confirm_timeout_ms),
      conn: nil,
      chan: nil
    }
    {:ok, try_connect(state)}
  end

  defp try_connect(state) do
    case connect(state) do
      {:ok, conn, chan} ->
        Process.monitor(conn.pid)
        Logger.info("[hook_rabbitmq] connected, exchange=#{state.exchange}")
        %{state | conn: conn, chan: chan}

      {:error, reason} ->
        Logger.warning("[hook_rabbitmq] connect failed: #{inspect(reason)}, retrying")
        Process.send_after(self(), :connect, state.retry_ms)
        state
    end
  end

  @impl true
  def handle_info(:connect, state), do: {:noreply, try_connect(state)}

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, %{conn: %{pid: pid}} = state) do
    Logger.warning("[hook_rabbitmq] connection lost: #{inspect(reason)}, reconnecting")
    Process.send_after(self(), :connect, state.retry_ms)
    {:noreply, %{state | conn: nil, chan: nil}}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state), do: {:noreply, state}

  @impl true
  def handle_call({:publish, _routing_key, _payload, _headers}, _from, %{chan: nil} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call({:publish, routing_key, payload, headers}, _from, state) do
    result =
      try do
        :ok =
          AMQP.Basic.publish(state.chan, state.exchange, routing_key, payload,
            persistent: true,
            content_type: "application/json",
            headers: headers
          )

        case AMQP.Confirm.wait_for_confirms(state.chan, state.confirm_timeout_ms) do
          true -> :ok
          false -> {:error, :nacked}
          :timeout -> {:error, :confirm_timeout}
        end
      catch
        :exit, reason -> {:error, {:publish_failed, reason}}
      end

    {:reply, result, state}
  end

  # ── internals ───────────────────────────────────────────────────────────

  defp connect(state) do
    with {:ok, conn} <- AMQP.Connection.open(state.url),
         {:ok, chan} <- AMQP.Channel.open(conn),
         :ok <- AMQP.Confirm.select(chan),
         :ok <- AMQP.Exchange.declare(chan, state.exchange, state.exchange_type, durable: true) do
      {:ok, conn, chan}
    end
  end
end
