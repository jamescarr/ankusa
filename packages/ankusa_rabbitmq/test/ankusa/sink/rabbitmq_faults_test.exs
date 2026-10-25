defmodule Ankusa.Sink.RabbitMQFaultsTest do
  @moduledoc """
  Broker faults (DEL-7): `Ankusa.Sink.RabbitMQ` publishes through toxiproxy
  (`docker compose up -d --wait` in this directory starts both), and the broker
  is killed, slowed and cut under it. The test's own connections go straight to
  the broker (`AMQP_URL`, default `amqp://guest:guest@localhost:5673`).

  The contract under test: `deliver/3` answers `:ok` only for a message the
  broker retained, and every other answer is an error dispatch retries.
  `TOXIPROXY_URL` (default `http://127.0.0.1:8475`) and `AMQP_PROXY_URL`
  (default `amqp://guest:guest@127.0.0.1:5674`) point elsewhere.
  """

  use ExUnit.Case, async: false

  alias Ankusa.{BrokerFaults, Toxiproxy}
  alias Ankusa.Sink.RabbitMQ

  @moduletag capture_log: true
  @moduletag timeout: 300_000

  @amqp_url System.get_env("AMQP_URL", "amqp://guest:guest@localhost:5673")
  @proxy_url System.get_env("AMQP_PROXY_URL", "amqp://guest:guest@127.0.0.1:5674")
  @api System.get_env("TOXIPROXY_URL", "http://127.0.0.1:8475")

  import Ankusa.BrokerFaults, only: [envelope: 0, ctx: 1]

  setup_all do
    Toxiproxy.populate!(@api, [
      %{name: "rabbitmq", listen: "0.0.0.0:5674", upstream: "rabbitmq:5672"}
    ])

    :ok
  end

  setup do
    Toxiproxy.reset!(@api)
    on_exit(fn -> Toxiproxy.reset!(@api) end)

    exchange = "ankusa.faults.#{System.unique_integer([:positive])}"
    queue = "ankusa.faults.#{System.unique_integer([:positive])}"
    declare_queue!(exchange, queue)

    %{
      instance: :"rmq_faults_#{System.unique_integer([:positive])}",
      exchange: exchange,
      queue: queue
    }
  end

  # A durable queue bound to the exchange, so a message the broker retained
  # survives a kill. Idempotent: run again once the broker is back.
  defp declare_queue!(exchange, queue) do
    with_channel(fn chan ->
      :ok = AMQP.Exchange.declare(chan, exchange, :topic, durable: true)
      {:ok, _} = AMQP.Queue.declare(chan, queue, durable: true)
      :ok = AMQP.Queue.bind(chan, queue, exchange, routing_key: "#")
    end)
  end

  # The test's own connection, direct to the broker and unlinked: it must not
  # take the test down when the broker is killed under it.
  defp with_channel(fun) do
    {:ok, conn} = AMQP.Connection.open(@amqp_url)
    Process.unlink(conn.pid)
    {:ok, chan} = AMQP.Channel.open(conn)

    try do
      fun.(chan)
    after
      safely_close(conn)
    end
  end

  defp safely_close(conn) do
    AMQP.Connection.close(conn)
  catch
    :exit, _ -> :ok
  end

  # Every message in the queue, as `{message_id, headers}`, consumed.
  defp drain(queue) do
    with_channel(fn chan -> drain(chan, queue, []) end)
  end

  defp drain(chan, queue, acc) do
    case AMQP.Basic.get(chan, queue, no_ack: true) do
      {:ok, payload, meta} -> drain(chan, queue, [{meta.message_id, JSON.decode!(payload)} | acc])
      {:empty, _} -> Enum.reverse(acc)
    end
  end

  defp ids(queue), do: queue |> drain() |> Enum.map(&elem(&1, 0))

  # The queue holds `id` within a few seconds (a confirm that was lost does not
  # mean the message was not routed).
  defp retained?(queue, id) do
    BrokerFaults.eventually(
      fn -> id in ids(queue) end,
      10_000,
      "the broker to hold #{id}"
    )
  end

  defp opts(exchange, extra), do: [exchange: exchange, url: @proxy_url] ++ extra

  defp deliver_when_connected(env, ctx, opts) do
    BrokerFaults.eventually(
      fn ->
        case RabbitMQ.deliver(env, ctx, opts) do
          {:error, :not_connected} -> nil
          other -> {:answered, other}
        end
      end,
      15_000,
      "the sink to connect"
    )
    |> elem(1)
  end

  defp connection(inst, exchange),
    do: Ankusa.whereis(inst, RabbitMQ.connection_key(@proxy_url, exchange))

  test "a broker killed with max_inflight publishes awaiting their confirms answers every one, and :ok only for what it kept",
       %{instance: inst, exchange: exchange, queue: queue} do
    opts = opts(exchange, max_inflight: 4, confirm_timeout_ms: 60_000, retry_ms: 200)
    warm = envelope()
    assert :ok = deliver_when_connected(warm, ctx(inst), opts)
    conn = connection(inst, exchange)

    # The broker gets every publish; the confirms are held in the proxy.
    Toxiproxy.toxic!(@api, "rabbitmq", "latency", "downstream", %{latency: 60_000})

    envs = for _ <- 1..4, do: envelope()

    tasks =
      for env <- envs,
          do: Task.async(fn -> Ankusa.Sink.safe_deliver(RabbitMQ, env, ctx(inst), opts) end)

    BrokerFaults.eventually(
      fn -> map_size(:sys.get_state(conn).pending) == 4 end,
      10_000,
      "four publishes waiting on their confirms"
    )

    # The channel is full: a fifth publish is refused, not queued behind them.
    assert {:error, :busy} = RabbitMQ.deliver(envelope(), ctx(inst), opts)

    # The broker dies with the confirms outstanding; removing the toxic lets the
    # proxy tell the sink so (a proxy holding data holds the close as well).
    BrokerFaults.compose!(["kill", "rabbitmq"])
    Toxiproxy.reset!(@api)
    results = Task.await_many(tasks, 60_000)
    BrokerFaults.compose!(["up", "-d", "--wait", "rabbitmq"])
    declare_queue!(exchange, queue)

    # What the broker confirmed it kept, it kept across the kill. Whatever it did
    # not confirm is an error dispatch retries (never an `:ok`).
    held = ids(queue)
    assert warm.id in held

    failed =
      for {env, result} <- Enum.zip(envs, results), result != :ok do
        assert {:error, reason} = result
        assert BrokerFaults.transient?(reason), "not retryable: #{inspect(reason)}"
        env
      end

    for {env, :ok} <- Enum.zip(envs, results), do: assert(env.id in held)

    # The retries land. One may duplicate a message the kill caught half-way:
    # at-least-once, which consumers dedupe on the idempotency key.
    for env <- failed do
      BrokerFaults.deliver_until_ok!(fn -> RabbitMQ.deliver(env, ctx(inst), opts) end)
    end

    kept = Enum.map(failed, & &1.id)
    assert MapSet.subset?(MapSet.new(kept), MapSet.new(ids(queue)))
  end

  test "a confirm that outlasts confirm_timeout_ms is an error, and the message is still the broker's",
       %{instance: inst, exchange: exchange, queue: queue} do
    opts = opts(exchange, confirm_timeout_ms: 300)
    assert :ok = deliver_when_connected(envelope(), ctx(inst), opts)
    assert length(drain(queue)) == 1

    Toxiproxy.toxic!(@api, "rabbitmq", "latency", "downstream", %{latency: 2_000})

    env = envelope()
    assert {:error, :confirm_timeout} = RabbitMQ.deliver(env, ctx(inst), opts)

    # The caller gave up, the broker did not: an error never means "not stored".
    assert retained?(queue, env.id)

    Toxiproxy.reset!(@api)
    assert :ok = deliver_when_connected(env, ctx(inst), opts)
    assert [{id, _}] = drain(queue)
    assert id == env.id
  end

  test "a confirm lost with the connection is an error, and the retry is answered :ok only once stored",
       %{instance: inst, exchange: exchange, queue: queue} do
    opts = opts(exchange, retry_ms: 200)
    assert :ok = deliver_when_connected(envelope(), ctx(inst), opts)
    assert length(drain(queue)) == 1

    # The broker receives the publish; its confirm is dropped, and the proxy cuts
    # the connection after 500 ms.
    Toxiproxy.toxic!(@api, "rabbitmq", "timeout", "downstream", %{timeout: 500})

    env = envelope()
    assert {:error, reason} = RabbitMQ.deliver(env, ctx(inst), opts)
    assert BrokerFaults.transient?(reason), "not retryable: #{inspect(reason)}"
    assert retained?(queue, env.id)

    Toxiproxy.reset!(@api)

    BrokerFaults.deliver_until_ok!(fn -> RabbitMQ.deliver(env, ctx(inst), opts) end)
    assert [{id, _}] = drain(queue)
    assert id == env.id
  end

  test "latency above the attempt deadline kills the attempt, keeps the row, and delivers once it clears",
       %{exchange: exchange, queue: queue} do
    sink = {RabbitMQ, opts(exchange, retry_ms: 200)}
    inst = BrokerFaults.start_dispatch!(sink, 1_000)

    warm = BrokerFaults.enqueue!(inst, envelope())
    BrokerFaults.await_delivered!(inst, 30_000)
    assert [{warm_id, _}] = drain(queue)
    assert warm_id == warm.id

    Toxiproxy.toxic!(@api, "rabbitmq", "latency", "downstream", %{latency: 3_000})

    env = BrokerFaults.enqueue!(inst, envelope())
    row = BrokerFaults.await_attempt_timeout!(inst, 15_000)
    assert row.row.attempts >= 1

    Toxiproxy.reset!(@api)
    BrokerFaults.await_delivered!(inst, 30_000)
    assert env.id in ids(queue)
  end
end
