defmodule Ankusa.Sink.GooglePubSubTest do
  @moduledoc """
  `Sink.GooglePubSub` is one `topics.publish` `POST` per delivery. The transport
  is stubbed with `Req.Test`, so these assert what goes on the wire — the path,
  the bearer token, the JSON message — and how each reply maps to an error class.
  """

  use ExUnit.Case, async: true

  alias Ankusa.{Envelope, Sink, UUIDv7}
  alias Ankusa.Sink.{GooglePubSub, Message}

  @always ~w(ankusa_id ankusa_source_id ankusa_tenant_id ankusa_message_version
             ankusa_idempotency_key content_type)

  def token(token), do: {:ok, token}
  def no_token, do: :error

  setup do
    {:ok, capture} = Agent.start_link(fn -> [] end)

    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      Agent.update(capture, &[{conn.method, conn.request_path, conn.req_headers, body} | &1])

      cond do
        String.contains?(conn.request_path, "/topics/missing:") ->
          error(conn, 404, "NOT_FOUND", "Resource not found (resource=missing).")

        String.contains?(conn.request_path, "/topics/bad:") ->
          error(conn, 400, "INVALID_ARGUMENT", "Invalid message.")

        String.contains?(conn.request_path, "/topics/denied:") ->
          error(conn, 403, "PERMISSION_DENIED", "The caller does not have permission.")

        String.contains?(conn.request_path, "/topics/unauth:") ->
          error(conn, 401, "UNAUTHENTICATED", "Request had invalid credentials.")

        String.contains?(conn.request_path, "/topics/exhausted:") ->
          error(conn, 429, "RESOURCE_EXHAUSTED", "Quota exceeded.")

        String.contains?(conn.request_path, "/topics/unavailable:") ->
          error(conn, 503, "UNAVAILABLE", "The service is unavailable.")

        String.contains?(conn.request_path, "/topics/overloaded:") ->
          Plug.Conn.send_resp(conn, 503, "slow down")

        String.contains?(conn.request_path, "/topics/internal:") ->
          error(conn, 500, "INTERNAL", "Internal error.")

        String.contains?(conn.request_path, "/topics/boom:") ->
          Plug.Conn.send_resp(conn, 500, "kaboom")

        true ->
          Plug.Conn.send_resp(conn, 200, ~s({"messageIds":["1"]}))
      end
    end)

    %{capture: capture}
  end

  defp error(conn, code, status, message) do
    body = JSON.encode!(%{"error" => %{"code" => code, "message" => message, "status" => status}})
    Plug.Conn.send_resp(conn, code, body)
  end

  defp envelope(overrides \\ %{}) do
    struct(
      %Envelope{
        id: UUIDv7.generate(),
        source_id: "stripe",
        tenant_id: "acme",
        received_at: 1_737_500_000_000,
        method: "POST",
        path: "/hooks/stripe",
        headers: [],
        content_type: "application/json",
        body: ~s({"hello":"world"}),
        size: 17
      },
      overrides
    )
  end

  defp opts(extra \\ []) do
    Keyword.merge(
      [
        project: "p",
        topic: "hooks",
        token_provider: {__MODULE__, :token, ["tok"]},
        req_options: [plug: {Req.Test, __MODULE__}]
      ],
      extra
    )
  end

  defp sent(capture) do
    assert [{method, path, headers, body}] = Agent.get(capture, & &1)
    {method, path, Map.new(headers), JSON.decode!(body)}
  end

  test "a publish: POST to the topic, bearer token, one message carrying the Message data",
       %{capture: capture} do
    env = envelope()

    assert :ok = GooglePubSub.deliver(env, %{attempt: 1}, opts())

    {method, path, h, request} = sent(capture)

    assert {method, path} == {"POST", "/v1/projects/p/topics/hooks:publish"}
    assert h["authorization"] == "Bearer tok"
    assert h["content-type"] == "application/json"

    assert [message] = request["messages"]
    {:ok, payload} = Message.encode(env, %{attempt: 1}, Message.default_inline_max_bytes())
    assert Base.decode64!(message["data"]) == payload

    attributes = message["attributes"]
    assert Enum.sort(Map.keys(attributes)) == Enum.sort(@always)
    assert attributes["ankusa_id"] == env.id
    assert attributes["ankusa_tenant_id"] == "acme"
    assert attributes["ankusa_idempotency_key"] == env.id

    refute Map.has_key?(message, "orderingKey")
  end

  test "ordering_key: a static string, or a function of the envelope", %{capture: capture} do
    assert :ok = GooglePubSub.deliver(envelope(), %{attempt: 1}, opts(ordering_key: "k"))
    {_, _, _, request} = sent(capture)
    assert [%{"orderingKey" => "k"}] = request["messages"]

    Agent.update(capture, fn _ -> [] end)

    fun = &"#{&1.source_id}-x"

    assert :ok = GooglePubSub.deliver(envelope(), %{attempt: 1}, opts(ordering_key: fun))
    {_, _, _, request} = sent(capture)
    assert [%{"orderingKey" => "stripe-x"}] = request["messages"]
  end

  test "no tenant leaves out ankusa_tenant_id; a replay carries ankusa_replay_id",
       %{capture: capture} do
    assert :ok =
             GooglePubSub.deliver(
               envelope(%{tenant_id: nil}),
               %{attempt: 1, replay_id: "r1"},
               opts()
             )

    {_, _, _, request} = sent(capture)
    assert [%{"attributes" => attributes}] = request["messages"]
    refute Map.has_key?(attributes, "ankusa_tenant_id")
    assert attributes["ankusa_replay_id"] == "r1"
  end

  test "a token provider that answers :error is :no_credentials, and nothing is sent",
       %{capture: capture} do
    assert {:error, :no_credentials} =
             GooglePubSub.deliver(
               envelope(),
               %{attempt: 1},
               opts(token_provider: {__MODULE__, :no_token, []})
             )

    assert Agent.get(capture, & &1) == []
  end

  test "no token provider sends no authorization header", %{capture: capture} do
    assert :ok =
             GooglePubSub.deliver(
               envelope(),
               %{attempt: 1},
               Keyword.delete(opts(), :token_provider)
             )

    {_, _, h, _} = sent(capture)
    refute Map.has_key?(h, "authorization")
  end

  test "project and topic are percent-encoded into the path", %{capture: capture} do
    assert :ok =
             GooglePubSub.deliver(
               envelope(),
               %{attempt: 1},
               opts(project: "p/../x", topic: "a b")
             )

    {_, path, _, _} = sent(capture)
    assert path == "/v1/projects/p%2F..%2Fx/topics/a%20b:publish"
  end

  test "Pub/Sub errors keep their status; a refused message is permanent, a refused credential or topic a config error" do
    assert {:error,
            {:permanent,
             {:config, {:pubsub, 404, "NOT_FOUND", "Resource not found (resource=missing)."}}} =
              r} =
             GooglePubSub.deliver(envelope(), %{attempt: 1}, opts(topic: "missing"))

    assert {:permanent, {:config, _}} = Sink.classify(r)

    assert {:error, {:permanent, {:config, {:pubsub, 403, "PERMISSION_DENIED", _}}}} =
             GooglePubSub.deliver(envelope(), %{attempt: 1}, opts(topic: "denied"))

    assert {:error, {:permanent, {:config, {:pubsub, 401, "UNAUTHENTICATED", _}}}} =
             GooglePubSub.deliver(envelope(), %{attempt: 1}, opts(topic: "unauth"))

    assert {:error, {:permanent, {:pubsub, 400, "INVALID_ARGUMENT", _}}} =
             GooglePubSub.deliver(envelope(), %{attempt: 1}, opts(topic: "bad"))

    # a 5xx that is not a throttle (a 500 `INTERNAL`, or a proxy's) is transient
    assert {:error, {:status, 500, "kaboom"}} =
             GooglePubSub.deliver(envelope(), %{attempt: 1}, opts(topic: "boom"))

    assert {:error, {:pubsub, 500, "INTERNAL", _}} =
             GooglePubSub.deliver(envelope(), %{attempt: 1}, opts(topic: "internal"))
  end

  test "RESOURCE_EXHAUSTED and 429/503 are a jittered retry-after, never a permanent error" do
    assert {:error, {:retry_after, ms, {:pubsub, 429, "RESOURCE_EXHAUSTED", _}} = failure} =
             GooglePubSub.deliver(envelope(), %{attempt: 1}, opts(topic: "exhausted"))

    # full jitter: 1..500 ms on the first attempt
    assert ms in 1..500
    assert {:retry_after, ^ms, _} = Sink.classify(failure)

    assert {:error, {:retry_after, _, {:pubsub, 503, "UNAVAILABLE", _}}} =
             GooglePubSub.deliver(envelope(), %{attempt: 1}, opts(topic: "unavailable"))

    assert {:error, {:retry_after, _, {:status, 503, "slow down"}}} =
             GooglePubSub.deliver(envelope(), %{attempt: 1}, opts(topic: "overloaded"))

    draws =
      for _ <- 1..200 do
        {:error, {:retry_after, ms, _}} =
          GooglePubSub.deliver(envelope(), %{attempt: 7}, opts(topic: "exhausted"))

        ms
      end

    assert Enum.all?(draws, &(&1 in 1..30_000))
    assert Enum.max(draws) > 500
    assert length(Enum.uniq(draws)) > 20
  end

  test "a 200 that does not name exactly one message id is an error" do
    Req.Test.stub(__MODULE__, fn conn -> Plug.Conn.send_resp(conn, 200, "{}") end)

    assert {:error, {:unexpected_reply, "{}"}} =
             GooglePubSub.deliver(envelope(), %{attempt: 1}, opts())
  end

  test "a message over max_message_bytes is permanent and never sent", %{capture: capture} do
    assert {:error, {:permanent, {:message_too_large, size, 100}}} =
             GooglePubSub.deliver(envelope(), %{attempt: 1}, opts(max_message_bytes: 100))

    assert size > 100
    assert Agent.get(capture, & &1) == []
  end

  test "describe/2: the topic as the channel, the ordering key as a message binding" do
    assert %Sink.Description{
             protocol: "googlepubsub",
             host: "pubsub.googleapis.com",
             address: "projects/p/topics/hooks",
             channel_bindings: %{"googlepubsub" => %{"bindingVersion" => "0.2.0"}},
             message_bindings: %{},
             ankusa_headers: false
           } = GooglePubSub.describe(%{source_id: "stripe", tenant_id: "acme"}, opts())

    assert %Sink.Description{
             host: "localhost:4588",
             message_bindings: %{
               "googlepubsub" => %{"orderingKey" => "k", "bindingVersion" => "0.2.0"}
             }
           } =
             GooglePubSub.describe(
               %{source_id: "stripe", tenant_id: "acme"},
               opts(endpoint: "http://localhost:4588/", ordering_key: "k")
             )
  end
end
