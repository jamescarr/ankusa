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

  test "a standard queue takes a delivery with no FIFO fields", %{standard: standard} do
    assert :ok = SQS.deliver(envelope(), %{attempt: 1}, opts(standard))

    assert [message] = peek(standard)
    refute Map.has_key?(message["Attributes"], "MessageGroupId")
  end
end
