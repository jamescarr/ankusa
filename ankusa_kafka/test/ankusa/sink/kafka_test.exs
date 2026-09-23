defmodule Ankusa.Sink.KafkaTest do
  @moduledoc """
  Requires a live Redpanda: `docker compose up -d --wait` in this directory.
  `KAFKA_BROKERS` (default `localhost:19092`) points elsewhere.
  """

  use ExUnit.Case, async: false

  require Record

  alias Ankusa.{ClaimCheck, Envelope, UUIDv7}
  alias Ankusa.Sink.Kafka

  Record.defrecordp(
    :kafka_message,
    Record.extract(:kafka_message, from_lib: "kafka_protocol/include/kpro_public.hrl")
  )

  @brokers System.get_env("KAFKA_BROKERS", "localhost:19092") |> String.split(",")
  @hosts Enum.map(@brokers, fn b ->
           [h, p] = String.split(b, ":")
           {String.to_charlist(h), String.to_integer(p)}
         end)

  setup do
    instance = :"kafka_#{System.unique_integer([:positive])}"
    topic = "ankusa.test.#{System.unique_integer([:positive])}"

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

    %{instance: instance, topic: topic}
  end

  defp envelope(overrides \\ %{}) do
    struct(
      %Envelope{
        id: UUIDv7.generate(),
        source_id: "src",
        tenant_id: "t1",
        received_at: 1_737_500_000_000,
        method: "POST",
        path: "/hooks/src",
        headers: [],
        content_type: "application/json",
        body: ~s({"hello":"world"}),
        size: 17
      },
      overrides
    )
  end

  defp ctx(instance), do: %{instance: instance, source_id: "src", tenant_id: "t1", attempt: 1}

  defp opts(topic, extra \\ []), do: [brokers: @brokers, topic: topic] ++ extra

  defp fetch_all(topic) do
    {:ok, {_hw, messages}} = :brod.fetch(@hosts, topic, 0, 0)
    Enum.map(messages, &Map.new(kafka_message(&1)))
  end

  test "an inline record carries the message, key, headers, and event timestamp", %{
    instance: inst,
    topic: topic
  } do
    env = envelope()
    assert :ok = Kafka.deliver(env, ctx(inst), opts(topic))

    assert [msg] = fetch_all(topic)
    assert msg.key == "t1/src"
    assert msg.ts == env.received_at

    assert Map.new(msg.headers) == %{
             "ankusa_id" => env.id,
             "ankusa_source_id" => "src",
             "ankusa_tenant_id" => "t1",
             "ankusa_message_version" => "1",
             "content_type" => "application/json"
           }

    decoded = JSON.decode!(msg.value)
    assert decoded["v"] == 1
    assert decoded["id"] == env.id
    assert Base.decode64!(decoded["body_base64"]) == env.body
  end

  test "a fat record carries a claim that redeems to the original body", %{
    instance: inst,
    topic: topic
  } do
    dir = Path.join(System.tmp_dir!(), "ankusa_#{inst}")
    on_exit(fn -> File.rm_rf(dir) end)
    Ankusa.put_config(Ankusa.Config.new(instance: inst, data_dir: dir))

    body = :crypto.strong_rand_bytes(20_000)
    env = envelope(%{body: body, size: byte_size(body)})

    assert :ok = Kafka.deliver(env, ctx(inst), opts(topic, inline_max_bytes: 1_000))

    assert [msg] = fetch_all(topic)
    decoded = JSON.decode!(msg.value)
    refute Map.has_key?(decoded, "body_base64")

    assert {:ok, ticket} = ClaimCheck.Ticket.from_map(decoded["claim"])
    assert {:ok, ^body} = ClaimCheck.redeem(inst, ticket)
  end

  test "key accepts a static string or a 1-arity function", %{instance: inst, topic: topic} do
    assert :ok = Kafka.deliver(envelope(), ctx(inst), opts(topic, key: "fixed"))

    assert :ok =
             Kafka.deliver(
               envelope(%{source_id: "other"}),
               ctx(inst),
               opts(topic, key: &"dyn.#{&1.source_id}")
             )

    assert Enum.map(fetch_all(topic), & &1.key) == ["fixed", "dyn.other"]
  end

  test "an unknown topic is an error and is not auto-created", %{instance: inst} do
    topic = "ankusa.missing.#{System.unique_integer([:positive])}"

    assert {:error, _} = Kafka.deliver(envelope(), ctx(inst), opts(topic))

    {:ok, %{topics: topics}} = :brod.get_metadata(@hosts, :all)
    refute Enum.any?(topics, &(&1.name == topic))
  end

  test "an unreachable broker fails within produce_timeout_ms instead of hanging" do
    inst = :"kafka_down_#{System.unique_integer([:positive])}"
    opts = [brokers: ["127.0.0.1:1"], topic: "whatever", produce_timeout_ms: 1_000]

    {elapsed_us, result} = :timer.tc(fn -> Kafka.deliver(envelope(), ctx(inst), opts) end)

    assert {:error, _} = result
    assert elapsed_us < 2_000_000
  end
end
