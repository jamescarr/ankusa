defmodule Ankusa.Sink.KafkaFaultsTest do
  @moduledoc """
  Broker faults (DEL-7): `Ankusa.Sink.Kafka` produces through toxiproxy
  (`docker compose up -d --wait` in this directory starts both; the proxy fronts
  Redpanda's `proxied` listener, which advertises the proxy's own address), and
  the broker is killed, slowed and cut under it. The test's own reads and topic
  administration go straight to the broker (`KAFKA_BROKERS`, default
  `localhost:19092`).

  The contract under test: `deliver/3` answers `:ok` only for a record the
  broker retained, and every other answer is an error dispatch retries.
  `TOXIPROXY_URL` (default `http://127.0.0.1:8476`) and `KAFKA_PROXY_BROKERS`
  (default `127.0.0.1:19093`) point elsewhere.
  """

  use ExUnit.Case, async: false

  import Ankusa.BrokerFaults, only: [envelope: 0, ctx: 1]

  require Record

  alias Ankusa.{BrokerFaults, Toxiproxy}
  alias Ankusa.Sink.Kafka

  Record.defrecordp(
    :kafka_message,
    Record.extract(:kafka_message, from_lib: "kafka_protocol/include/kpro_public.hrl")
  )

  @moduletag capture_log: true
  @moduletag timeout: 300_000

  @brokers System.get_env("KAFKA_BROKERS", "localhost:19092") |> String.split(",")
  @hosts Enum.map(@brokers, fn b ->
           [h, p] = String.split(b, ":")
           {String.to_charlist(h), String.to_integer(p)}
         end)
  @proxy_brokers System.get_env("KAFKA_PROXY_BROKERS", "127.0.0.1:19093") |> String.split(",")
  @api System.get_env("TOXIPROXY_URL", "http://127.0.0.1:8476")

  setup_all do
    Toxiproxy.populate!(@api, [
      %{name: "redpanda", listen: "0.0.0.0:19093", upstream: "redpanda:9093"}
    ])

    :ok
  end

  setup do
    Toxiproxy.reset!(@api)
    on_exit(fn -> Toxiproxy.reset!(@api) end)

    topic = "ankusa.faults.#{System.unique_integer([:positive])}"

    :ok =
      :brod.create_topics(
        @hosts,
        [
          %{
            name: topic,
            num_partitions: 1,
            replication_factor: 1,
            assignments: [],
            configs: []
          }
        ],
        %{timeout: 5_000}
      )

    on_exit(fn -> :brod.delete_topics(@hosts, [topic], 5_000) end)

    %{instance: :"kafka_faults_#{System.unique_integer([:positive])}", topic: topic}
  end

  defp opts(topic, extra \\ []), do: [brokers: @proxy_brokers, topic: topic] ++ extra

  # The ids of every record the topic holds, read from the broker directly.
  # Fails the test when the broker cannot be read within `timeout_ms`.
  defp ids(topic, timeout_ms \\ 30_000) do
    BrokerFaults.eventually(
      fn ->
        case fetch(topic) do
          {:ok, messages} ->
            Enum.map(messages, fn message ->
              message
              |> kafka_message()
              |> Map.new()
              |> Map.fetch!(:value)
              |> JSON.decode!()
              |> Map.fetch!("id")
            end)

          :error ->
            nil
        end
      end,
      timeout_ms,
      "the topic to be readable"
    )
  end

  defp fetch(topic) do
    case :brod.fetch(@hosts, topic, 0, 0) do
      {:ok, {_high_watermark, messages}} -> {:ok, messages}
      _other -> :error
    end
  catch
    _kind, _reason -> :error
  end

  # The broker holds `id` within a few seconds, whatever `deliver/3` answered.
  defp retained?(topic, id) do
    BrokerFaults.eventually(
      fn -> id in ids(topic) end,
      15_000,
      "the broker to hold #{id}"
    )
  end

  defp deliver_until_ok!(env, ctx, opts),
    do: BrokerFaults.deliver_until_ok!(fn -> Kafka.deliver(env, ctx, opts) end)

  test "a broker killed mid-produce fails the delivery retryably, and the retry lands",
       %{instance: inst, topic: topic} do
    opts = opts(topic, produce_timeout_ms: 2_000)
    warm = envelope()
    deliver_until_ok!(warm, ctx(inst), opts)

    # The broker gets the record; its acknowledgement never gets back.
    Toxiproxy.toxic!(@api, "redpanda", "latency", "downstream", %{latency: 60_000})

    env = envelope()
    task = Task.async(fn -> Kafka.deliver(env, ctx(inst), opts) end)
    Process.sleep(500)
    BrokerFaults.kill_and_restart!("redpanda")

    assert {:error, reason} = Task.await(task, 60_000)
    assert BrokerFaults.transient?(reason), "not retryable: #{inspect(reason)}"

    Toxiproxy.reset!(@api)
    deliver_until_ok!(env, ctx(inst), opts)

    # A record the broker confirmed before the kill survived it; the retry may
    # duplicate one the kill caught half-way (consumers dedupe on the key).
    held = ids(topic)
    assert warm.id in held
    assert env.id in held
  end

  test "produce_timeout_ms expiring mid-flight is an error, and the record is still the broker's",
       %{instance: inst, topic: topic} do
    opts = opts(topic, produce_timeout_ms: 500)
    deliver_until_ok!(envelope(), ctx(inst), opts)

    Toxiproxy.toxic!(@api, "redpanda", "latency", "downstream", %{latency: 3_000})

    env = envelope()
    assert {:error, reason} = Kafka.deliver(env, ctx(inst), opts)
    assert BrokerFaults.transient?(reason), "not retryable: #{inspect(reason)}"

    # The caller gave up, the broker did not: an error never means "not stored".
    assert retained?(topic, env.id)

    Toxiproxy.reset!(@api)
    deliver_until_ok!(env, ctx(inst), opts)
    assert Enum.count(ids(topic), &(&1 == env.id)) >= 2
  end

  test "a produce acknowledgement lost with the connection is an error, and the record is the broker's",
       %{instance: inst, topic: topic} do
    opts = opts(topic, produce_timeout_ms: 3_000)
    deliver_until_ok!(envelope(), ctx(inst), opts)

    # The broker receives the record; its acknowledgement is dropped and the
    # proxy cuts the connection after 500 ms.
    Toxiproxy.toxic!(@api, "redpanda", "timeout", "downstream", %{timeout: 500})

    env = envelope()
    assert {:error, reason} = Kafka.deliver(env, ctx(inst), opts)
    assert BrokerFaults.transient?(reason), "not retryable: #{inspect(reason)}"
    assert retained?(topic, env.id)

    Toxiproxy.reset!(@api)
    deliver_until_ok!(env, ctx(inst), opts)
    assert Enum.count(ids(topic), &(&1 == env.id)) >= 2
  end

  test "latency above the attempt deadline kills the attempt, keeps the row, and delivers once it clears",
       %{topic: topic} do
    inst = BrokerFaults.start_dispatch!({Kafka, opts(topic)}, 1_000)

    warm = BrokerFaults.enqueue!(inst, envelope())
    BrokerFaults.await_delivered!(inst, 60_000)
    assert warm.id in ids(topic)

    Toxiproxy.toxic!(@api, "redpanda", "latency", "downstream", %{latency: 3_000})

    env = BrokerFaults.enqueue!(inst, envelope())
    row = BrokerFaults.await_attempt_timeout!(inst, 15_000)
    assert row.row.attempts >= 1

    Toxiproxy.reset!(@api)
    BrokerFaults.await_delivered!(inst, 60_000)
    assert env.id in ids(topic)
  end
end
