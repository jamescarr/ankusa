defmodule Ankusa.Sink.RedisFaultsTest do
  @moduledoc """
  Broker faults (DEL-7): `Ankusa.Sink.Redis` publishes through toxiproxy
  (`docker compose up -d --wait` in this directory starts both), and the server
  is killed, slowed and cut under it. Pub/sub keeps nothing, so "the broker
  retained it" is "a subscriber, connected to the server directly (`REDIS_URL`,
  default `redis://localhost:6399`), received it".

  The contract under test: `deliver/3` answers `:ok` only for a message a
  subscriber received, and every other answer is an error dispatch retries.
  `TOXIPROXY_URL` (default `http://127.0.0.1:8478`) and `REDIS_PROXY_URL`
  (default `redis://127.0.0.1:6400`) point elsewhere.
  """

  use ExUnit.Case, async: false

  import Ankusa.BrokerFaults, only: [envelope: 0, ctx: 1]

  alias Ankusa.{BrokerFaults, Toxiproxy}
  alias Ankusa.Sink.Redis

  @moduletag capture_log: true
  @moduletag timeout: 300_000

  @url System.get_env("REDIS_URL", "redis://localhost:6399")
  @proxy_url System.get_env("REDIS_PROXY_URL", "redis://127.0.0.1:6400")
  @api System.get_env("TOXIPROXY_URL", "http://127.0.0.1:8478")

  setup_all do
    Toxiproxy.populate!(@api, [%{name: "redis", listen: "0.0.0.0:6400", upstream: "redis:6379"}])
    :ok
  end

  setup do
    Toxiproxy.reset!(@api)
    on_exit(fn -> Toxiproxy.reset!(@api) end)

    suffix = System.unique_integer([:positive])
    channel = "ankusa.faults.#{suffix}.hooks"

    %{instance: :"redis_faults_#{suffix}", channel: channel, subscriber: subscribe!(channel)}
  end

  # A subscriber connected to the server directly, unlinked (a killed server
  # must not take the test down), that has been confirmed by the server: a
  # publish made after this returns is counted by `PUBLISH`.
  defp subscribe!(channel) do
    {:ok, ps} = Redix.PubSub.start_link(@url, sync_connect: true)
    Process.unlink(ps)
    {:ok, ref} = Redix.PubSub.subscribe(ps, channel, self())
    assert_receive {:redix_pubsub, ^ps, ^ref, :subscribed, _}, 5_000
    {ps, ref}
  end

  # The ids of every message the subscriber has received so far.
  defp received({ps, ref}), do: received(ps, ref, [])

  defp received(ps, ref, acc) do
    receive do
      {:redix_pubsub, ^ps, ^ref, :message, %{payload: payload}} ->
        received(ps, ref, [JSON.decode!(payload)["id"] | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # The subscriber has received `id` within a few seconds, whatever
  # `deliver/3` answered. Reading consumes what the subscriber has received.
  defp heard?(subscriber, id) do
    BrokerFaults.eventually(
      fn -> id in received(subscriber) end,
      10_000,
      "a subscriber to receive #{id}"
    )
  end

  defp opts(channel, extra \\ []), do: [url: @proxy_url, channel: channel] ++ extra

  defp deliver_until_ok!(env, ctx, opts),
    do: BrokerFaults.deliver_until_ok!(fn -> Redis.deliver(env, ctx, opts) end)

  test "a server killed mid-PUBLISH fails the delivery retryably, and the retry reaches a subscriber",
       %{instance: inst, channel: channel, subscriber: subscriber} do
    opts = opts(channel, publish_timeout_ms: 15_000)
    warm = envelope()
    deliver_until_ok!(warm, ctx(inst), opts)
    assert heard?(subscriber, warm.id)

    # The PUBLISH is held in the proxy when the server dies.
    Toxiproxy.toxic!(@api, "redis", "latency", "upstream", %{latency: 60_000})

    env = envelope()
    task = Task.async(fn -> Redis.deliver(env, ctx(inst), opts) end)
    Process.sleep(300)
    BrokerFaults.kill_and_restart!("redis")

    assert {:error, reason} = Task.await(task, 60_000)
    assert BrokerFaults.transient?(reason), "not retryable: #{inspect(reason)}"

    # A fresh subscriber: the killed server took the old one's connection.
    Toxiproxy.reset!(@api)
    subscriber = subscribe!(channel)
    assert env.id not in received(subscriber)

    deliver_until_ok!(env, ctx(inst), opts)
    assert heard?(subscriber, env.id)
  end

  test "publish_timeout_ms expiring mid-flight is an error, and the message is still the subscriber's",
       %{instance: inst, channel: channel, subscriber: subscriber} do
    opts = opts(channel, publish_timeout_ms: 500)
    warm = envelope()
    deliver_until_ok!(warm, ctx(inst), opts)
    assert heard?(subscriber, warm.id)

    Toxiproxy.toxic!(@api, "redis", "latency", "downstream", %{latency: 2_000})

    env = envelope()
    assert {:error, :publish_timeout} = Redis.deliver(env, ctx(inst), opts)

    # The caller gave up, the server did not: an error never means "not published".
    assert heard?(subscriber, env.id)

    Toxiproxy.reset!(@api)
    deliver_until_ok!(env, ctx(inst), opts)
    assert heard?(subscriber, env.id)
  end

  test "a PUBLISH reply lost with the connection is an error, and the retry is answered :ok only once heard",
       %{instance: inst, channel: channel, subscriber: subscriber} do
    opts = opts(channel, publish_timeout_ms: 2_000)
    warm = envelope()
    deliver_until_ok!(warm, ctx(inst), opts)
    assert heard?(subscriber, warm.id)

    # The server publishes; its reply is dropped and the proxy cuts the
    # connection after 500 ms.
    Toxiproxy.toxic!(@api, "redis", "timeout", "downstream", %{timeout: 500})

    env = envelope()
    assert {:error, reason} = Redis.deliver(env, ctx(inst), opts)
    assert BrokerFaults.transient?(reason), "not retryable: #{inspect(reason)}"
    assert heard?(subscriber, env.id)

    Toxiproxy.reset!(@api)
    deliver_until_ok!(env, ctx(inst), opts)
    assert heard?(subscriber, env.id)
  end

  test "latency above the attempt deadline kills the attempt, keeps the row, and delivers once it clears",
       %{channel: channel, subscriber: subscriber} do
    inst = BrokerFaults.start_dispatch!({Redis, opts(channel)}, 1_000)

    warm = BrokerFaults.enqueue!(inst, envelope())
    BrokerFaults.await_delivered!(inst, 30_000)
    assert heard?(subscriber, warm.id)

    Toxiproxy.toxic!(@api, "redis", "latency", "downstream", %{latency: 3_000})

    env = BrokerFaults.enqueue!(inst, envelope())
    row = BrokerFaults.await_attempt_timeout!(inst, 15_000)
    assert row.row.attempts >= 1

    Toxiproxy.reset!(@api)
    BrokerFaults.await_delivered!(inst, 30_000)
    assert heard?(subscriber, env.id)
  end
end
