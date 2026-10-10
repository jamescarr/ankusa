defmodule Ankusa.Sink.RabbitMQ.Connection do
  @moduledoc """
  One AMQP connection + confirm-mode channel per `{instance, url, exchange}`.
  Declares the configured exchange whenever it opens the channel (idempotent) —
  never a queue; queue ownership belongs to consumers, not this sink.

  Every publish is `mandatory` and is answered on the broker's confirm of that
  publish, so `publish/4` answers `:ok` only when at least one queue bound to
  the exchange accepted the message. A message the exchange routes to no queue
  is `{:error, {:unroutable, routing_key}}`, one the broker refuses (a queue's
  `reject-publish` overflow) is `{:error, :nacked}`, and no confirm within
  `:confirm_timeout_ms` milliseconds is `{:error, :confirm_timeout}`.

  ## Confirms are asynchronous

  A publish is sent and its caller parked under the publish's sequence number;
  the confirm, whenever it arrives, answers it. Many publishes are in flight on
  the channel at once (up to `:max_inflight`, default 256; past that a publish
  is `{:error, :busy}`), so one slow confirm never serializes the others.

  Every call carries its caller's deadline. A publish still waiting in this
  process's mailbox when its caller gave up (`publish/4` waits 15 s) is
  answered `{:error, :expired}` and never sent: an abandoned call is not
  published behind the caller's back.

  The channel is monitored. When the broker closes it (a publish to a deleted
  exchange is a 404) every publish waiting on it answers
  `{:error, {:channel_closed, reason}}` and the channel is reopened at once on
  the same connection, re-declaring the exchange.

  The connection dials after `start_link/1` returns, and connection loss is
  not treated as a crash — this GenServer stays up and retries on a timer,
  replying `{:error, :not_connected}` to publishes in the meantime (and to any
  that were waiting when it dropped). Every one of these errors flows into the
  source's `Ankusa.RetryPolicy` exactly like any other sink failure, so there
  is no separate reconnect policy to get wrong.
  """

  use GenServer
  require Logger
  require Record

  @default_retry_ms 5_000
  @default_confirm_timeout_ms 5_000
  @default_max_inflight 256
  @call_timeout_ms 15_000

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
    deadline = System.monotonic_time(:millisecond) + @call_timeout_ms

    GenServer.call(
      server,
      {:publish, routing_key, payload, props, deadline},
      @call_timeout_ms
    )
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
      max_inflight: Keyword.get(opts, :max_inflight, @default_max_inflight),
      conn: nil,
      conn_ref: nil,
      chan: nil,
      chan_ref: nil,
      # seqno => %{from, routing_key, payload, returned?, timer}
      pending: %{}
    }

    {:ok, state, {:continue, :connect}}
  end

  # A crash report prints the state: the URL may carry a password, and pending
  # publishes carry hook bodies.
  @impl true
  def format_status(%{state: %{url: url, pending: pending} = state} = status) do
    %{status | state: %{state | url: redact_url(url), pending: map_size(pending)}}
  end

  def format_status(status), do: status

  defp redact_url(url) when is_binary(url) do
    url |> URI.parse() |> Map.put(:userinfo, nil) |> URI.to_string()
  rescue
    _ -> "[redacted]"
  end

  defp redact_url(_url), do: "[redacted]"

  @impl true
  def handle_continue(:connect, state), do: {:noreply, connect(state)}

  @impl true
  def handle_info(:connect, state), do: {:noreply, connect(state)}

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{conn_ref: ref} = state) do
    Logger.warning("[ankusa_rabbitmq] connection lost: #{inspect(reason)}, reconnecting")
    if state.chan_ref, do: Process.demonitor(state.chan_ref, [:flush])
    Process.send_after(self(), :connect, state.retry_ms)
    state = fail_pending(state, {:error, :not_connected})
    {:noreply, %{state | conn: nil, conn_ref: nil, chan: nil, chan_ref: nil}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{chan_ref: ref} = state) do
    {:noreply, channel_lost(state, reason)}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state), do: {:noreply, state}

  def handle_info(basic_ack(delivery_tag: tag, multiple: multiple), state) do
    {:noreply, settle(state, tag, multiple, &ack_result/1)}
  end

  def handle_info(basic_nack(delivery_tag: tag, multiple: multiple), state) do
    {:noreply, settle(state, tag, multiple, fn _entry -> {:error, :nacked} end)}
  end

  # RabbitMQ sends basic.return before the basic.ack of a mandatory message no
  # queue took, and the channel process sends both straight here, in order. A
  # return names the bytes, not the sequence number: it is the oldest pending
  # publish of exactly those bytes that has not been returned yet.
  def handle_info({basic_return(), amqp_msg(payload: payload)}, state) do
    candidate =
      state.pending
      |> Enum.filter(fn {_seqno, entry} -> entry.payload == payload and not entry.returned? end)
      |> Enum.min_by(fn {seqno, _entry} -> seqno end, fn -> nil end)

    case candidate do
      nil ->
        {:noreply, state}

      {seqno, entry} ->
        {:noreply, %{state | pending: Map.put(state.pending, seqno, %{entry | returned?: true})}}
    end
  end

  def handle_info({:confirm_timeout, seqno}, state) do
    case Map.pop(state.pending, seqno) do
      {nil, _pending} ->
        {:noreply, state}

      {entry, pending} ->
        GenServer.reply(entry.from, {:error, :confirm_timeout})
        {:noreply, %{state | pending: pending}}
    end
  end

  @impl true
  def handle_call({:publish, routing_key, payload, props, deadline}, from, state) do
    cond do
      System.monotonic_time(:millisecond) > deadline ->
        {:reply, {:error, :expired}, state}

      state.chan == nil ->
        {:reply, {:error, :not_connected}, state}

      map_size(state.pending) >= state.max_inflight ->
        {:reply, {:error, :busy}, state}

      true ->
        case send_publish(state, routing_key, payload, props) do
          {:ok, seqno} ->
            timer =
              Process.send_after(self(), {:confirm_timeout, seqno}, state.confirm_timeout_ms)

            entry = %{
              from: from,
              routing_key: routing_key,
              payload: payload,
              returned?: false,
              timer: timer
            }

            {:noreply, %{state | pending: Map.put(state.pending, seqno, entry)}}

          {:error, _reason} = error ->
            {:reply, error, state}
        end
    end
  end

  # ── confirms ────────────────────────────────────────────────────────────

  defp ack_result(%{returned?: true, routing_key: routing_key}),
    do: {:error, {:unroutable, routing_key}}

  defp ack_result(_entry), do: :ok

  # Answer the publish `tag` names — or, with `multiple`, every one up to it.
  defp settle(state, tag, multiple, result) do
    {settled, pending} =
      if multiple do
        Map.split_with(state.pending, fn {seqno, _entry} -> seqno <= tag end)
      else
        case Map.pop(state.pending, tag) do
          {nil, pending} -> {%{}, pending}
          {entry, pending} -> {%{tag => entry}, pending}
        end
      end

    Enum.each(settled, fn {_seqno, entry} ->
      Process.cancel_timer(entry.timer)
      GenServer.reply(entry.from, result.(entry))
    end)

    %{state | pending: pending}
  end

  defp fail_pending(state, reply) do
    Enum.each(state.pending, fn {_seqno, entry} ->
      Process.cancel_timer(entry.timer)
      GenServer.reply(entry.from, reply)
    end)

    %{state | pending: %{}}
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
  # on it now. Every publish waiting on the old channel is answered: a new
  # channel numbers its publishes from 1 again, so none of them can be
  # confirmed any more. If the connection is dying too, the reopen fails and
  # retries, and the connection's :DOWN resets everything.
  defp channel_lost(state, reason) do
    Logger.warning("[ankusa_rabbitmq] channel closed: #{inspect(reason)}, reopening")
    state = fail_pending(state, {:error, {:channel_closed, reason}})
    send(self(), :connect)
    %{state | chan: nil, chan_ref: nil}
  end

  # ── publish ─────────────────────────────────────────────────────────────

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

    # The hook's `id` is the broker's `message_id`, a handle for broker-level
    # tooling; the key to dedupe on is the `ankusa_idempotency_key` header.
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
end
