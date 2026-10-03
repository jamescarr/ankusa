defmodule Ankusa.Sink.RabbitMQTest do
  @moduledoc """
  Requires a live RabbitMQ: `docker compose up -d --wait` in this directory.
  """

  use ExUnit.Case, async: false

  alias Ankusa.{Envelope, UUIDv7}
  alias Ankusa.Sink.RabbitMQ

  @amqp_url "amqp://guest:guest@localhost:5673"

  setup do
    instance = :"rmq_#{System.unique_integer([:positive])}"
    exchange = "ankusa.test.#{System.unique_integer([:positive])}"

    {:ok, conn} = AMQP.Connection.open(@amqp_url)
    {:ok, chan} = AMQP.Channel.open(conn)
    :ok = AMQP.Exchange.declare(chan, exchange, :topic, durable: true)
    {:ok, %{queue: queue}} = AMQP.Queue.declare(chan, "", exclusive: true)
    :ok = AMQP.Queue.bind(chan, queue, exchange, routing_key: "#")

    on_exit(fn -> AMQP.Connection.close(conn) end)

    %{instance: instance, exchange: exchange, chan: chan, queue: queue}
  end

  defp envelope(overrides \\ %{}) do
    base = %Envelope{
      id: UUIDv7.generate(),
      source_id: "src",
      tenant_id: "t1",
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/hooks/src",
      headers: [],
      content_type: "application/json",
      body: ~s({"hello":"world"}),
      size: 18
    }

    struct(base, overrides)
  end

  defp ctx(instance), do: %{instance: instance, source_id: "src", tenant_id: "t1", attempt: 1}

  defp get_message(chan, queue, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    poll(chan, queue, deadline)
  end

  defp poll(chan, queue, deadline) do
    case AMQP.Basic.get(chan, queue, no_ack: true) do
      {:ok, payload, meta} ->
        {payload, meta}

      {:empty, _} ->
        if System.monotonic_time(:millisecond) > deadline do
          flunk("no message received on #{queue} within timeout")
        else
          Process.sleep(50)
          poll(chan, queue, deadline)
        end
    end
  end

  defp deliver_when_connected(env, ctx, opts) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    retry_deliver(env, ctx, opts, deadline)
  end

  defp retry_deliver(env, ctx, opts, deadline) do
    case RabbitMQ.deliver(env, ctx, opts) do
      {:error, :not_connected} ->
        if System.monotonic_time(:millisecond) > deadline do
          flunk("sink did not reconnect within 5 s")
        else
          Process.sleep(50)
          retry_deliver(env, ctx, opts, deadline)
        end

      other ->
        other
    end
  end

  test "inline payload publishes a small message the consumer can decode", %{
    instance: inst,
    exchange: exch,
    chan: chan,
    queue: queue
  } do
    env = envelope()
    assert :ok = RabbitMQ.deliver(env, ctx(inst), exchange: exch, url: @amqp_url)

    {payload, meta} = get_message(chan, queue)
    assert meta.routing_key == "ankusa.src"

    decoded = JSON.decode!(payload)
    assert decoded["v"] == 1
    assert decoded["id"] == env.id
    assert decoded["source_id"] == "src"
    assert decoded["tenant_id"] == "t1"
    assert Base.decode64!(decoded["body_base64"]) == env.body
    refute Map.has_key?(decoded, "claim")
  end

  test "a fat payload is checked in through ClaimCheck and the message carries a ticket", %{
    instance: inst,
    exchange: exch,
    chan: chan,
    queue: queue
  } do
    body = :crypto.strong_rand_bytes(20_000)
    env = envelope(%{body: body, size: byte_size(body), content_type: "application/octet-stream"})

    claim_dir =
      Path.join(
        System.tmp_dir!(),
        "ankusa_rmq_claim_#{System.unique_integer([:positive])}_#{System.os_time(:microsecond)}"
      )

    on_exit(fn -> File.rm_rf(claim_dir) end)
    Ankusa.put_config(Ankusa.Config.new(instance: inst, data_dir: claim_dir))

    opts = [exchange: exch, url: @amqp_url, inline_max_bytes: 1_000]

    assert :ok = RabbitMQ.deliver(env, ctx(inst), opts)

    {payload, _meta} = get_message(chan, queue)
    decoded = JSON.decode!(payload)
    assert decoded["v"] == 1
    refute Map.has_key?(decoded, "body_base64")
    assert "urn:ankusa:claim:v1:" <> _ = decoded["claim"]
    assert {:ok, ^body} = Ankusa.ClaimCheck.redeem(inst, decoded["claim"], decoded["sha256"])
  end

  test "routing_key accepts a static string or a 1-arity function", %{
    instance: inst,
    exchange: exch,
    chan: chan,
    queue: queue
  } do
    env = envelope()

    assert :ok =
             RabbitMQ.deliver(env, ctx(inst),
               exchange: exch,
               url: @amqp_url,
               routing_key: "custom.key"
             )

    {_payload, meta} = get_message(chan, queue)
    assert meta.routing_key == "custom.key"

    assert :ok =
             RabbitMQ.deliver(env, ctx(inst),
               exchange: exch,
               url: @amqp_url,
               routing_key: fn e -> "dyn.#{e.tenant_id}.#{e.source_id}" end
             )

    {_payload2, meta2} = get_message(chan, queue)
    assert meta2.routing_key == "dyn.t1.src"
  end

  test "a publish no queue is bound to receive is {:error, {:unroutable, key}}, not :ok", %{
    instance: inst,
    chan: chan
  } do
    exch = "ankusa.test.unbound.#{System.unique_integer([:positive])}"
    opts = [exchange: exch, url: @amqp_url]

    assert {:error, {:unroutable, "ankusa.src"}} = RabbitMQ.deliver(envelope(), ctx(inst), opts)

    {:ok, %{queue: queue}} = AMQP.Queue.declare(chan, "", exclusive: true)
    :ok = AMQP.Queue.bind(chan, queue, exch, routing_key: "#")

    env = envelope()
    assert :ok = RabbitMQ.deliver(env, ctx(inst), opts)
    {payload, _meta} = get_message(chan, queue)
    assert JSON.decode!(payload)["id"] == env.id
  end

  test "a channel the broker closes is reopened and re-declares the exchange", %{
    instance: inst,
    exchange: exch,
    chan: chan,
    queue: queue
  } do
    opts = [exchange: exch, url: @amqp_url, retry_ms: 200]

    assert :ok = RabbitMQ.deliver(envelope(), ctx(inst), opts)
    get_message(chan, queue)

    :ok = AMQP.Exchange.delete(chan, exch)

    assert {:error, {:channel_closed, _}} = RabbitMQ.deliver(envelope(), ctx(inst), opts)

    assert {:error, {:unroutable, "ankusa.src"}} =
             deliver_when_connected(envelope(), ctx(inst), opts)

    :ok = AMQP.Queue.bind(chan, queue, exch, routing_key: "#")

    env = envelope()
    assert :ok = RabbitMQ.deliver(env, ctx(inst), opts)
    {payload, _meta} = get_message(chan, queue)
    assert JSON.decode!(payload)["id"] == env.id
  end

  test "a publish a queue refuses is {:error, :nacked}", %{instance: inst, chan: chan} do
    exch = "ankusa.test.full.#{System.unique_integer([:positive])}"
    :ok = AMQP.Exchange.declare(chan, exch, :topic, durable: true)

    {:ok, %{queue: queue}} =
      AMQP.Queue.declare(chan, "",
        exclusive: true,
        arguments: [{"x-max-length", 0}, {"x-overflow", "reject-publish"}]
      )

    :ok = AMQP.Queue.bind(chan, queue, exch, routing_key: "#")

    assert {:error, :nacked} =
             RabbitMQ.deliver(envelope(), ctx(inst), exchange: exch, url: @amqp_url)
  end

  test "an unreachable broker fails fast with :not_connected instead of hanging" do
    inst = :"rmq_bad_#{System.unique_integer([:positive])}"
    env = envelope()

    assert {:error, :not_connected} =
             RabbitMQ.deliver(env, ctx(inst),
               exchange: "whatever",
               url: "amqp://guest:guest@127.0.0.1:1"
             )
  end
end
