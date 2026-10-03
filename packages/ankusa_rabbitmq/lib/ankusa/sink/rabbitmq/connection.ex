defmodule Ankusa.Sink.RabbitMQ.Connection do
  @moduledoc """
  One AMQP connection + confirm-mode channel per instance. Declares the
  configured exchange whenever it opens the channel (idempotent) — never a
  queue; queue ownership belongs to consumers, not this sink.

  Every publish is `mandatory` and waits for the broker's confirm of that
  publish, so `publish/4` answers `:ok` only when at least one queue bound to
  the exchange accepted the message. A message the exchange routes to no queue
  is `{:error, {:unroutable, routing_key}}`, one the broker refuses (a queue's
  `reject-publish` overflow) is `{:error, :nacked}`, and no confirm within
  `:confirm_timeout_ms` milliseconds is `{:error, :confirm_timeout}`.

  The channel is monitored. When the broker closes it (a publish to a deleted
  exchange is a 404) the publish waiting on it answers
  `{:error, {:channel_closed, reason}}` and the channel is reopened at once on
  the same connection, re-declaring the exchange.

  Connection loss is not treated as a crash — this GenServer stays up and
  retries on a timer, replying `{:error, :not_connected}` to publishes in the
  meantime. Every one of these errors flows into the source's
  `Ankusa.RetryPolicy` exactly like any other sink failure, so there is no
  separate reconnect policy to get wrong.
  """

  use GenServer
  require Logger
  require Record

  @default_retry_ms 5_000
  @default_confirm_timeout_ms 5_000

  # The messages the channel process sends its registered return and confirm
  # handlers (`:amqp_channel.register_return_handler/2`,
  # `register_confirm_handler/2`).
  @framing "rabbit_common/include/rabbit_framing.hrl"
  Record.defrecordp(:basic_ack, :"basic.ack", Record.extract(:"basic.ack", from_lib: @framing))
  Record.defrecordp(:basic_nack, :"basic.nack", Record.extract(:"basic.nack", from_lib: @framing))

  Record.defrecordp(
    :basic_return,
    :"basic.return",
    Record.extract(:"basic.return", from_lib: @framing)
  )

  Record.defrecordp(
    :amqp_msg,
    :amqp_msg,
    Record.extract(:amqp_msg, from_lib: "amqp_client/include/amqp_client.hrl")
  )

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
  # `props` is `[message_id: String.t() | nil, headers: [{String.t(), atom(), term()}]`.
  def publish(server, routing_key, payload, props \\ []) do
    GenServer.call(server, {:publish, routing_key, payload, props}, 15_000)
  end

  @doc """
  The AMQP URL this connection uses when `:url` is not configured.
  """
  @spec default_url() :: String.t()
  def default_url, do: "amqp://guest:guest@localhost:5672"

  # ── GenServer ───────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    state = %{
      instance: Keyword.fetch!(opts, :instance),
      url: Keyword.get(opts, :url, default_url()),
      exchange: Keyword.fetch!(opts, :exchange),
      exchange_type: Keyword.get(opts, :exchange_type, :topic),
      retry_ms: Keyword.get(opts, :retry_ms, @default_retry_ms),
      confirm_timeout_ms: Keyword.get(opts, :confirm_timeout_ms, @default_confirm_timeout_ms),
      conn: nil,
      conn_ref: nil,
      chan: nil,
      chan_ref: nil
    }

    {:ok, connect(state)}
  end

  @impl true
  def handle_info(:connect, state), do: {:noreply, connect(state)}

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{conn_ref: ref} = state) do
    Logger.warning("[ankusa_rabbitmq] connection lost: #{inspect(reason)}, reconnecting")
    if state.chan_ref, do: Process.demonitor(state.chan_ref, [:flush])
    Process.send_after(self(), :connect, state.retry_ms)
    {:noreply, %{state | conn: nil, conn_ref: nil, chan: nil, chan_ref: nil}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{chan_ref: ref} = state) do
    {:noreply, channel_lost(state, reason)}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state), do: {:noreply, state}

  # Confirms and returns for a publish that already answered :confirm_timeout.
  def handle_info(basic_ack(), state), do: {:noreply, state}
  def handle_info(basic_nack(), state), do: {:noreply, state}
  def handle_info({basic_return(), amqp_msg()}, state), do: {:noreply, state}

  @impl true
  def handle_call({:publish, _routing_key, _payload, _props}, _from, %{chan: nil} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call({:publish, routing_key, payload, props}, _from, state) do
    {result, state} = publish_and_confirm(state, routing_key, payload, props)
    {:reply, result, state}
  end

  # ── connection lifecycle ────────────────────────────────────────────────

  # Fills in whatever is missing — the connection, then the channel — and
  # leaves a live pair alone. A failed step retries after :retry_ms and keeps
  # what it already has, so a channel that won't open never leaks connections.
  defp connect(%{conn: nil} = state) do
    case AMQP.Connection.open(state.url) do
      {:ok, conn} ->
        connect(%{state | conn: conn, conn_ref: Process.monitor(conn.pid)})

      {:error, reason} ->
        Logger.warning("[ankusa_rabbitmq] connect failed: #{inspect(reason)}, retrying")
        Process.send_after(self(), :connect, state.retry_ms)
        state
    end
  end

  defp connect(%{chan: nil} = state) do
    case open_channel(state) do
      {:ok, chan} ->
        Logger.info("[ankusa_rabbitmq] connected, exchange=#{state.exchange}")
        %{state | chan: chan, chan_ref: Process.monitor(chan.pid)}

      {:error, reason} ->
        Logger.warning(
          "[ankusa_rabbitmq] channel setup failed, exchange=#{state.exchange}: " <>
            "#{inspect(reason)}, retrying"
        )

        Process.send_after(self(), :connect, state.retry_ms)
        state
    end
  end

  defp connect(state), do: state

  # A failure after the channel opened closes it (a 406 on declare has the
  # broker close it and the declare call exit), so retries never leak channels.
  defp open_channel(state) do
    case safely(fn -> AMQP.Channel.open(state.conn) end) do
      {:ok, chan} ->
        case safely(fn -> setup_channel(chan, state) end) do
          :ok ->
            {:ok, chan}

          {:error, reason} ->
            close_channel(chan)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # The handler registrations are casts, so they reach the channel before any
  # later publish call from this process. They are the raw amqp_client ones:
  # the channel process itself sends returns and confirms here, in the order
  # the broker sent them. (`AMQP.Basic.return/2` and
  # `AMQP.Confirm.register_handler/2` route both through a separate process.)
  defp setup_channel(chan, state) do
    with :ok <- AMQP.Confirm.select(chan),
         :ok <-
           AMQP.Exchange.declare(chan, state.exchange, state.exchange_type, durable: true) do
      :ok = :amqp_channel.register_return_handler(chan.pid, self())
      :ok = :amqp_channel.register_confirm_handler(chan.pid, self())
    end
  end

  defp safely(fun) do
    fun.()
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  # The channel may already be dead (a broker close); never Process.exit/2 it:
  # amqp_client turns a killed channel into a closed connection.
  defp close_channel(chan) do
    AMQP.Channel.close(chan)
  catch
    :exit, _reason -> :ok
  end

  # The connection stays up when the broker closes only the channel, so reopen
  # on it now. If the connection is dying too, the reopen fails and retries,
  # and the connection's :DOWN resets everything.
  defp channel_lost(state, reason) do
    Logger.warning("[ankusa_rabbitmq] channel closed: #{inspect(reason)}, reopening")
    send(self(), :connect)
    %{state | chan: nil, chan_ref: nil}
  end

  # ── publish ─────────────────────────────────────────────────────────────

  defp publish_and_confirm(state, routing_key, payload, props) do
    deadline = System.monotonic_time(:millisecond) + state.confirm_timeout_ms

    case send_publish(state, routing_key, payload, props) do
      {:ok, seqno} -> await_confirm(state, seqno, routing_key, payload, deadline, false)
      {:error, _reason} = error -> {error, state}
    end
  end

  # This process is the channel's only publisher, so the seqno read first is the
  # delivery tag the broker will confirm. A blocked publish does not consume one.
  defp send_publish(state, routing_key, payload, props) do
    seqno = AMQP.Confirm.next_publish_seqno(state.chan)

    opts = [
      mandatory: true,
      persistent: true,
      content_type: "application/json",
      headers: Keyword.get(props, :headers, [])
    ]

    # The hook's `id` is the broker's `message_id`, so consumers dedupe on it.
    opts =
      case Keyword.get(props, :message_id) do
        nil -> opts
        id when is_binary(id) -> Keyword.put(opts, :message_id, id)
      end

    case AMQP.Basic.publish(state.chan, state.exchange, routing_key, payload, opts) do
      :ok -> {:ok, seqno}
      {:error, reason} -> {:error, {:publish_failed, reason}}
    end
  catch
    :exit, reason -> {:error, {:publish_failed, reason}}
  end

  # RabbitMQ sends basic.return before basic.ack for a mandatory message no queue
  # took, and the channel process sends both straight here, so a return seen
  # before this publish's ack means it was unroutable. A return is this
  # publish's when its payload is these bytes; any other confirm or return is
  # a timed-out publish's and stays for handle_info to drop.
  defp await_confirm(state, seqno, routing_key, payload, deadline, returned?) do
    chan_ref = state.chan_ref
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {basic_return(), amqp_msg(payload: ^payload)} ->
        await_confirm(state, seqno, routing_key, payload, deadline, true)

      basic_ack(delivery_tag: tag, multiple: multiple)
      when tag == seqno or (multiple and tag > seqno) ->
        if returned?, do: {{:error, {:unroutable, routing_key}}, state}, else: {:ok, state}

      basic_nack(delivery_tag: tag, multiple: multiple)
      when tag == seqno or (multiple and tag > seqno) ->
        {{:error, :nacked}, state}

      {:DOWN, ^chan_ref, :process, _pid, reason} ->
        {{:error, {:channel_closed, reason}}, channel_lost(state, reason)}
    after
      timeout -> {{:error, :confirm_timeout}, state}
    end
  end
end
