defmodule Ankusa.Sink.GooglePubSubIntegrationTest do
  @moduledoc """
  `Sink.GooglePubSub` against the floci-gcp emulator's Pub/Sub. Requires
  `docker compose -f docker-compose.integration.yml up -d floci-gcp`; run with
  `mix test --include integration`.

  floci-gcp does not check tokens, so the sink runs without a `:token_provider`.
  This proves the request plumbing and Pub/Sub's side of the contract: the
  message published to a topic arrives on its subscription, with its data and
  attributes intact. Messages are read back with `subscriptions.pull`.
  """

  use ExUnit.Case, async: false
  @moduletag :integration

  alias Ankusa.{Envelope, Sink, UUIDv7}
  alias Ankusa.Sink.GooglePubSub

  @endpoint "http://localhost:4588"
  @project "floci-local"

  setup do
    n = System.unique_integer([:positive])
    topic = "ankusa-it-#{n}"
    subscription = "ankusa-it-#{n}"

    %{status: 200} = put("/topics/#{topic}", nil)

    %{status: 200} =
      put("/subscriptions/#{subscription}", %{
        "topic" => "projects/#{@project}/topics/#{topic}",
        "enableMessageOrdering" => true
      })

    %{topic: topic, subscription: subscription}
  end

  defp put(path, body) do
    options = [decode_body: false] ++ if(body, do: [json: body], else: [])
    Req.put!(@endpoint <> "/v1/projects/#{@project}" <> path, options)
  end

  # Pull is eventually consistent against the emulator: retry while the
  # subscription has nothing for us yet.
  defp pull(subscription, attempts \\ 20) do
    %{status: 200, body: body} =
      Req.post!(
        @endpoint <> "/v1/projects/#{@project}/subscriptions/#{subscription}:pull",
        json: %{"maxMessages" => 10},
        decode_body: false
      )

    case JSON.decode!(body) do
      %{"receivedMessages" => [_ | _] = received} ->
        Enum.map(received, & &1["message"])

      _ when attempts > 1 ->
        Process.sleep(100)
        pull(subscription, attempts - 1)

      _ ->
        []
    end
  end

  defp opts(topic, extra \\ []) do
    [project: @project, topic: topic, endpoint: @endpoint] ++ extra
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

  test "a delivery arrives on the subscription with the Message data and the attributes", %{
    topic: topic,
    subscription: subscription
  } do
    env = envelope()

    assert :ok = GooglePubSub.deliver(env, %{attempt: 1}, opts(topic))

    assert [message] = pull(subscription)

    data = message["data"] |> Base.decode64!() |> JSON.decode!()
    assert data["id"] == env.id
    assert data["source_id"] == "stripe"
    assert data["body_base64"] == Base.encode64(env.body)

    assert message["attributes"]["ankusa_id"] == env.id
    assert message["attributes"]["ankusa_source_id"] == "stripe"
    assert message["attributes"]["ankusa_idempotency_key"] == env.id
  end

  test "an ordering key arrives with the message", %{topic: topic, subscription: subscription} do
    assert :ok = GooglePubSub.deliver(envelope(), %{attempt: 1}, opts(topic, ordering_key: "k"))

    assert [message] = pull(subscription)
    assert message["orderingKey"] == "k"
  end

  test "a topic that was never created is a transient error, retried rather than dropped", %{
    topic: topic
  } do
    assert {:error, reason} =
             GooglePubSub.deliver(envelope(), %{attempt: 1}, opts(topic <> "-never"))

    assert {:transient, _} = Sink.classify(reason)
  end
end
