defmodule Ankusa.Sink.KafkaTest do
  @moduledoc """
  Requires a live Redpanda: `docker compose up -d --wait` in this directory.
  """

  use ExUnit.Case, async: false

  alias Ankusa.{Envelope, UUIDv7}
  alias Ankusa.Sink.Kafka

  @brokers ["localhost:19092"]
  @topic "ankusa.test"

  setup do
    instance = :"kafka_#{System.unique_integer([:positive])}"
    client_id = :"test_consumer_#{System.unique_integer([:positive])}"

    # Start a consumer client for verification
    {:ok, _pid} =
      :brod_client.start_link(
        [{~c"localhost", 19092}],
        client_id,
        []
      )

    on_exit(fn ->
      :brod_client.stop(client_id)
    end)

    %{instance: instance, consumer_client: client_id}
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

  defp fetch_message(client_id, topic, partition, timeout \\ 5_000) do
    # Get the latest offset
    {:ok, latest_offset} = :brod.resolve_offset(client_id, to_charlist(topic), partition, :latest)

    # Fetch from one before the latest (the message we just produced)
    fetch_offset = max(0, latest_offset - 1)

    case :brod.fetch(client_id, to_charlist(topic), partition, fetch_offset,
           max_wait_time: timeout,
           max_bytes: 1_048_576
         ) do
      {:ok, {_high_water_mark, messages}} when length(messages) > 0 ->
        # Get the last message (most recent)
        kafka_message(offset: _, key: key, value: value, headers: headers) = List.last(messages)
        {:ok, key, value, headers}

      {:ok, {_high_water_mark, []}} ->
        :empty

      error ->
        error
    end
  end

  test "inline payload publishes a small message to Kafka", %{
    instance: inst,
    consumer_client: client
  } do
    env = envelope()

    assert :ok =
             Kafka.deliver(env, ctx(inst),
               brokers: @brokers,
               topic: @topic
             )

    # Partition 0 should have the message (hash of "t1/src" key)
    assert {:ok, key, value, headers} = fetch_message(client, @topic, 0)

    assert key == "t1/src"

    decoded = JSON.decode!(value)
    assert decoded["v"] == 1
    assert decoded["id"] == env.id
    assert decoded["source_id"] == "src"
    assert decoded["tenant_id"] == "t1"
    assert Base.decode64!(decoded["body_base64"]) == env.body
    refute Map.has_key?(decoded, "claim")

    # Check headers
    headers_map = Map.new(headers)
    assert headers_map["ankusa_id"] == env.id
    assert headers_map["ankusa_source_id"] == "src"
    assert headers_map["ankusa_tenant_id"] == "t1"
    assert headers_map["ankusa_message_version"] == "1"
    assert headers_map["content_type"] == "application/json"
  end

  test "a fat payload is checked in through ClaimCheck", %{
    instance: inst,
    consumer_client: client
  } do
    body = :crypto.strong_rand_bytes(20_000)
    env = envelope(%{body: body, size: byte_size(body), content_type: "application/octet-stream"})

    claim_dir =
      Path.join(System.tmp_dir!(), "ankusa_kafka_claim_#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf(claim_dir) end)
    Ankusa.put_config(Ankusa.Config.new(instance: inst, data_dir: claim_dir))

    opts = [brokers: @brokers, topic: @topic, inline_max_bytes: 1_000]

    assert :ok = Kafka.deliver(env, ctx(inst), opts)

    assert {:ok, _key, value, _headers} = fetch_message(client, @topic, 0)
    decoded = JSON.decode!(value)

    assert decoded["v"] == 1
    refute Map.has_key?(decoded, "body_base64")
    assert %{"claim" => claim_map} = decoded
    assert claim_map["tenant_id"] == "t1"
    assert claim_map["id"] == env.id
    assert claim_map["size"] == 20_000

    assert {:ok, ticket} = Ankusa.ClaimCheck.Ticket.from_map(claim_map)
    assert {:ok, ^body} = Ankusa.ClaimCheck.redeem(inst, ticket)
  end

  test "key function controls partitioning", %{
    instance: inst,
    consumer_client: client
  } do
    env1 = envelope(%{source_id: "source_a"})
    env2 = envelope(%{source_id: "source_b"})

    opts = [brokers: @brokers, topic: @topic, key: fn env -> env.source_id end]

    assert :ok = Kafka.deliver(env1, ctx(inst), opts)
    assert :ok = Kafka.deliver(env2, ctx(inst), opts)

    # Both should have their source_id as the key
    # Check all partitions to find the messages
    messages =
      for partition <- 0..2 do
        case fetch_message(client, @topic, partition, 1_000) do
          {:ok, key, value, _headers} -> {key, JSON.decode!(value)["source_id"]}
          _ -> nil
        end
      end
      |> Enum.reject(&is_nil/1)

    assert {"source_a", "source_a"} in messages
    assert {"source_b", "source_b"} in messages
  end

  test "unknown topic fails with clear error" do
    env = envelope()
    inst = :"isolated_#{System.unique_integer([:positive])}"

    assert {:error, reason} =
             Kafka.deliver(env, ctx(inst),
               brokers: @brokers,
               topic: "nonexistent.topic"
             )

    # brod returns :unknown_topic_or_partition for unknown topics
    assert reason in [:unknown_topic_or_partition, {:producer_not_found, _}]
  end
end
