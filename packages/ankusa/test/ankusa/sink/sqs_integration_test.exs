defmodule Ankusa.Sink.SQSIntegrationTest do
  @moduledoc """
  `Sink.SQS` against the floci emulator's SQS. Requires
  `docker compose -f docker-compose.integration.yml up -d floci`; run with
  `mix test --include integration`.

  floci does not check SigV4 (the signature is pinned by `Ankusa.Sink.SQSTest`),
  so this proves the request plumbing and SQS's side of the contract: the
  message stored, its attributes, the FIFO group and the deduplication window.
  Messages are read back through floci's non-destructive peek
  (`GET /_aws/sqs/messages`).
  """

  use ExUnit.Case, async: false
  @moduletag :integration

  alias Ankusa.{Envelope, UUIDv7}
  alias Ankusa.Sink.SQS

  @endpoint "http://localhost:4566"

  setup do
    n = System.unique_integer([:positive])

    fifo = create_queue("ankusa-it-#{n}.fifo", %{"FifoQueue" => "true"})
    standard = create_queue("ankusa-it-#{n}", %{})

    %{fifo: fifo, standard: standard}
  end

  defp create_queue(name, attributes) do
    %{status: 200, body: body} =
      Req.post!(@endpoint <> "/",
        headers: [
          {"x-amz-target", "AmazonSQS.CreateQueue"},
          {"content-type", "application/x-amz-json-1.0"}
        ],
        body: JSON.encode!(%{"QueueName" => name, "Attributes" => attributes}),
        decode_body: false
      )

    JSON.decode!(body)["QueueUrl"]
  end

  defp peek(queue_url) do
    %{status: 200, body: body} =
      Req.get!(@endpoint <> "/_aws/sqs/messages",
        params: [QueueUrl: queue_url],
        decode_body: false
      )

    JSON.decode!(body)["messages"]
  end

  defp opts(queue_url) do
    [
      queue_url: queue_url,
      region: "us-east-1",
      endpoint: @endpoint,
      access_key_id: "test",
      secret_access_key: "test"
    ]
  end

  defp envelope do
    %Envelope{
      id: UUIDv7.generate(),
      source_id: "stripe",
      tenant_id: "acme",
      received_at: 1_737_500_000_000,
      method: "POST",
      path: "/hooks/stripe",
      headers: [],
      content_type: "application/json",
      body: ~s({"id":"evt_1"}),
      size: 14
    }
  end

  test "a delivery is stored with the Message body, the attributes and the FIFO group", %{
    fifo: fifo
  } do
    env = envelope()

    assert :ok = SQS.deliver(env, %{attempt: 1}, opts(fifo))

    assert [message] = peek(fifo)

    body = JSON.decode!(message["Body"])
    assert body["id"] == env.id
    assert body["body_base64"] == Base.encode64(env.body)

    attributes = message["MessageAttributes"]
    assert attributes["ankusa_id"]["StringValue"] == env.id
    assert attributes["ankusa_source_id"]["StringValue"] == "stripe"
    assert attributes["ankusa_idempotency_key"]["StringValue"] == env.id

    assert message["Attributes"]["MessageGroupId"] == "acme/stripe"
    assert message["Attributes"]["MessageDeduplicationId"] == env.id
  end

  test "a FIFO queue collapses a retried delivery, and stores a replay", %{fifo: fifo} do
    env = envelope()

    assert :ok = SQS.deliver(env, %{attempt: 1}, opts(fifo))
    assert :ok = SQS.deliver(env, %{attempt: 2}, opts(fifo))
    assert [_one] = peek(fifo)

    assert :ok = SQS.deliver(env, %{attempt: 1, replay_id: "r1"}, opts(fifo))
    assert [_, replay] = peek(fifo)
    assert replay["MessageAttributes"]["ankusa_replay_id"]["StringValue"] == "r1"
  end

  test "a queue that was never created is an SQS error, retried rather than dropped", %{
    fifo: fifo
  } do
    missing = String.replace(fifo, "ankusa-it-", "ankusa-it-never-")

    assert {:error, {:sqs, 400, code, _message}} =
             SQS.deliver(envelope(), %{attempt: 1}, opts(missing))

    assert is_binary(code)
  end

  describe "batched (a delivery from dispatch, through an instance's batcher)" do
    setup do
      instance = :"sqs_it_batch_#{System.unique_integer([:positive])}"
      start_supervised!({Ankusa.Sink.Batcher.Workers, instance: instance})

      handler = "sqs-it-batch-#{instance}"
      test = self()

      :telemetry.attach(
        handler,
        [:ankusa, :sink_batch, :flush],
        fn _event, measurements, %{instance: ^instance}, _ ->
          send(test, {:flush, measurements})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      %{ctx: %{instance: instance, batch: true, attempt: 1}}
    end

    defp envelope(id), do: %{envelope() | id: id}

    test "sqs batch partial failure: SQS refuses one entry of ten, exactly that one is permanent",
         %{fifo: fifo, ctx: ctx} do
      # Ten hooks, each its own group; the emulator, like SQS, refuses the entry
      # with no MessageGroupId, and says so for that entry alone.
      opts =
        opts(fifo) ++
          [
            batch_linger_ms: 2_000,
            message_group_id: fn env -> if env.id == "bad-hook", do: "", else: "g-" <> env.id end
          ]

      envs = for n <- 1..9, do: envelope("good-#{n}")
      envs = envs ++ [envelope("bad-hook")]

      results =
        envs
        |> Enum.map(fn env -> Task.async(fn -> {env.id, SQS.deliver(env, ctx, opts)} end) end)
        |> Map.new(&Task.await/1)

      # one SendMessageBatch of ten
      assert_receive {:flush, %{entries: 10, requests: 1}}, 5_000

      assert {:error, {:permanent, {:sqs, 400, code, _message}}} = results["bad-hook"]
      assert code in ["MissingParameter", "InvalidParameterValue"]

      for n <- 1..9, do: assert(results["good-#{n}"] == :ok)

      stored =
        fifo |> peek() |> Enum.map(&(JSON.decode!(&1["Body"])["id"])) |> Enum.sort()

      assert stored == for(n <- 1..9, do: "good-#{n}") |> Enum.sort()
    end

    test "sqs batch order: one FIFO group arrives in the order its messages were admitted",
         %{fifo: fifo, ctx: ctx} do
      opts = opts(fifo) ++ [batch_linger_ms: 10]
      ids = for n <- 1..25, do: "m#{String.pad_leading(Integer.to_string(n), 2, "0")}"

      ids
      |> Enum.map(fn id ->
        task = Task.async(fn -> SQS.deliver(envelope(id), ctx, opts) end)
        Process.sleep(3)
        task
      end)
      |> Enum.each(fn task -> assert Task.await(task) == :ok end)

      assert fifo |> peek() |> Enum.map(&(JSON.decode!(&1["Body"])["id"])) == ids
    end

    test "a standard queue takes a batch with no FIFO fields", %{standard: standard, ctx: ctx} do
      opts = opts(standard) ++ [batch_linger_ms: 2_000, batch_max_entries: 5]
      envs = for n <- 1..5, do: envelope("s-#{n}")

      assert envs
             |> Enum.map(fn env -> Task.async(fn -> SQS.deliver(env, ctx, opts) end) end)
             |> Enum.map(&Task.await/1) == List.duplicate(:ok, 5)

      assert_receive {:flush, %{entries: 5, requests: 1}}, 5_000
      messages = peek(standard)
      assert length(messages) == 5
      refute Enum.any?(messages, &Map.has_key?(&1["Attributes"], "MessageGroupId"))
    end
  end

  test "a standard queue takes a delivery with no FIFO fields", %{standard: standard} do
    assert :ok = SQS.deliver(envelope(), %{attempt: 1}, opts(standard))

    assert [message] = peek(standard)
    refute Map.has_key?(message["Attributes"], "MessageGroupId")
  end
end
