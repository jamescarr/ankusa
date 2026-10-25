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

  test "an ordered sink's orderingKey is the lane's key, over any ordering_key of the sink",
       %{capture: capture} do
    ctx = %{attempt: 1, ordering_key: "cus_1", lane: "abc"}

    assert :ok = GooglePubSub.deliver(envelope(), ctx, opts(ordering_key: "legacy"))

    {_, _, _, request} = sent(capture)
    assert [%{"orderingKey" => "cus_1", "attributes" => attributes}] = request["messages"]
    assert attributes["ankusa_ordering_key"] == "cus_1"
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
             ankusa_headers: true
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

  describe "batched (a delivery from dispatch: ctx batch: true, an instance's batchers)" do
    setup do
      instance = :"pubsub_batch_#{System.unique_integer([:positive])}"
      start_supervised!({Ankusa.Sink.Batcher.Workers, instance: instance})
      %{instance: instance}
    end

    defp ctx(instance, extra \\ %{}),
      do: Map.merge(%{instance: instance, batch: true, attempt: 1}, extra)

    # A poison message is one whose ordering key is POISON; the topic refuses a
    # whole request that carries one, as Pub/Sub does for an invalid message.
    defp poison_key(env), do: if(String.starts_with?(env.id, "poison"), do: "POISON")

    defp batch_opts(extra \\ []) do
      opts(Keyword.merge([batch_linger_ms: 200, ordering_key: &poison_key/1], extra))
    end

    defp stub_publish(capture, fun) do
      Req.Test.stub(__MODULE__, fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        request = JSON.decode!(body)
        Agent.update(capture, &[{conn.request_path, Map.new(conn.req_headers), request} | &1])
        fun.(conn, request)
      end)
    end

    defp accept(conn, request) do
      ids = request["messages"] |> Enum.with_index(1) |> Enum.map(&Integer.to_string(elem(&1, 1)))
      Plug.Conn.send_resp(conn, 200, JSON.encode!(%{"messageIds" => ids}))
    end

    # The topic refuses a request holding a poison message, accepts any other.
    defp refuse_poison(conn, request) do
      if Enum.any?(request["messages"], &(&1["orderingKey"] == "POISON")),
        do: error(conn, 400, "INVALID_ARGUMENT", "Invalid message."),
        else: accept(conn, request)
    end

    defp deliver_all(envs, ctx, opts) do
      envs
      |> Enum.map(fn env ->
        Task.async(fn -> {env.id, GooglePubSub.deliver(env, ctx, opts)} end)
      end)
      |> Map.new(&Task.await(&1, 15_000))
    end

    test "deliveries are one publish with one message each, the way a single publish builds it",
         %{capture: capture, instance: instance} do
      stub_publish(capture, &accept/2)
      envs = for _ <- 1..50, do: envelope()

      results = deliver_all(envs, ctx(instance), batch_opts(batch_max_entries: 50))

      assert Map.values(results) == List.duplicate(:ok, 50)
      assert [{path, headers, request}] = Agent.get(capture, & &1)
      assert path == "/v1/projects/p/topics/hooks:publish"
      assert headers["authorization"] == "Bearer tok"
      assert length(request["messages"]) == 50

      ids = for m <- request["messages"], do: m["attributes"]["ankusa_id"]
      assert Enum.sort(ids) == envs |> Enum.map(& &1.id) |> Enum.sort()
      refute Enum.any?(request["messages"], &Map.has_key?(&1, "orderingKey"))
    end

    test "one invalid message among 999 good ones: 999 delivered, one dead, a bounded number of requests",
         %{capture: capture, instance: instance} do
      stub_publish(capture, &refuse_poison/2)

      envs = for(n <- 1..999, do: envelope(%{id: "good-#{n}"})) ++ [envelope(%{id: "poison-1"})]

      # the batch is full at 1 000, so it leaves at once, not after the linger
      results = deliver_all(envs, ctx(instance), batch_opts(batch_linger_ms: 30_000))

      assert results["poison-1"] ==
               {:error, {:permanent, {:pubsub, 400, "INVALID_ARGUMENT", "Invalid message."}}}

      delivered = results |> Map.delete("poison-1") |> Map.values()
      assert delivered == List.duplicate(:ok, 999)

      # 1 request for the batch, then `2 * ceil(log2 1000)` to isolate the one
      requests = length(Agent.get(capture, & &1))
      assert requests <= 1 + 2 * 10
      assert requests > 1
    end

    test "a refused batch is bisected in order: the good halves are published, in order",
         %{capture: capture, instance: instance} do
      stub_publish(capture, &refuse_poison/2)

      envs =
        for n <- 1..8, do: envelope(%{id: if(n == 5, do: "poison-5", else: "good-#{n}")})

      results =
        deliver_all(envs, ctx(instance), batch_opts(batch_max_entries: 8, batch_linger_ms: 5_000))

      assert {:error, {:permanent, _}} = results["poison-5"]
      assert results |> Map.delete("poison-5") |> Map.values() == List.duplicate(:ok, 7)

      requests = Agent.get(capture, & &1) |> Enum.reverse()
      ids = fn request -> Enum.map(request["messages"], & &1["attributes"]["ankusa_id"]) end

      # the first request holds the whole batch, in the order the deliveries were
      # admitted (concurrent tasks: not the order they were started in)
      assert [{_, _, whole} | _] = requests
      admitted = ids.(whole)
      assert Enum.sort(admitted) == envs |> Enum.map(& &1.id) |> Enum.sort()

      published =
        requests
        |> Enum.reject(fn {_, _, request} ->
          Enum.any?(request["messages"], &(&1["orderingKey"] == "POISON"))
        end)
        |> Enum.flat_map(fn {_, _, request} -> ids.(request) end)

      # halves are published left then right: the accepted messages keep the order
      # they were admitted in
      assert published == admitted -- ["poison-5"]
    end

    test "bisection is bounded: a batch of nothing but bad messages does not cost n requests",
         %{capture: capture, instance: instance} do
      stub_publish(capture, &refuse_poison/2)
      envs = for n <- 1..64, do: envelope(%{id: "poison-#{n}"})

      results =
        deliver_all(
          envs,
          ctx(instance),
          batch_opts(batch_max_entries: 64, batch_linger_ms: 5_000)
        )

      # `2 * (ceil(log2 64) + 1)` splits at most, two requests each, plus the first
      assert length(Agent.get(capture, & &1)) <= 1 + 2 * 2 * 7

      assert Enum.all?(Map.values(results), fn
               {:error, {:permanent, {:pubsub, 400, "INVALID_ARGUMENT", _}}} -> true
               {:error, {:batch_rejected, {:pubsub, 400, "INVALID_ARGUMENT", _}}} -> true
               _ -> false
             end)

      # the ones left over are transient: they retry, in other batches
      assert Enum.any?(Map.values(results), &match?({:error, {:batch_rejected, _}}, &1))

      assert {:transient, _} =
               Sink.classify(
                 results
                 |> Map.values()
                 |> Enum.find(&match?({:error, {:batch_rejected, _}}, &1))
                 |> elem(1)
               )
    end

    test "a request that fails as a whole fails every message the way a single publish would",
         %{capture: capture, instance: instance} do
      envs = fn -> for _ <- 1..3, do: envelope() end

      stub_publish(capture, fn conn, _ -> Plug.Conn.send_resp(conn, 500, "kaboom") end)
      results = envs.() |> deliver_all(ctx(instance), batch_opts()) |> Map.values()
      assert results == List.duplicate({:error, {:status, 500, "kaboom"}}, 3)
      # not bisected: a transient error is not about one message
      assert length(Agent.get(capture, & &1)) == 1

      Agent.update(capture, fn _ -> [] end)
      stub_publish(capture, fn conn, _ -> error(conn, 404, "NOT_FOUND", "no topic") end)
      results = envs.() |> deliver_all(ctx(instance), batch_opts()) |> Map.values()

      assert results ==
               List.duplicate(
                 {:error, {:permanent, {:config, {:pubsub, 404, "NOT_FOUND", "no topic"}}}},
                 3
               )

      # not bisected either: no message caused it
      assert length(Agent.get(capture, & &1)) == 1
    end

    test "RESOURCE_EXHAUSTED and a 429/503 are retry_after, each message with its own jitter",
         %{capture: capture, instance: instance} do
      stub_publish(capture, fn conn, _ ->
        error(conn, 429, "RESOURCE_EXHAUSTED", "Quota exceeded")
      end)

      results =
        for(_ <- 1..3, do: envelope())
        |> deliver_all(ctx(instance, %{attempt: 2}), batch_opts())
        |> Map.values()

      assert Enum.all?(
               results,
               &match?({:error, {:retry_after, _, {:pubsub, 429, "RESOURCE_EXHAUSTED", _}}}, &1)
             )

      stub_publish(capture, fn conn, _ -> Plug.Conn.send_resp(conn, 503, "busy") end)

      assert {:error, {:retry_after, _, {:status, 503, "busy"}}} =
               GooglePubSub.deliver(envelope(), ctx(instance), batch_opts())
    end

    test "a reply that does not name one id per message fails every message",
         %{capture: capture, instance: instance} do
      stub_publish(capture, fn conn, _ ->
        Plug.Conn.send_resp(conn, 200, ~s({"messageIds":["1"]}))
      end)

      results =
        for(_ <- 1..3, do: envelope())
        |> deliver_all(ctx(instance), batch_opts())
        |> Map.values()

      assert Enum.all?(results, &match?({:error, {:unexpected_reply, _}}, &1))
    end

    test "a token provider that answers :error fails the batch without a request",
         %{capture: capture, instance: instance} do
      stub_publish(capture, &accept/2)
      opts = batch_opts(token_provider: {__MODULE__, :no_token, []})

      assert {:error, :no_credentials} = GooglePubSub.deliver(envelope(), ctx(instance), opts)
      assert Agent.get(capture, & &1) == []
    end

    test "an ordering key has one request in flight; its messages keep their order",
         %{capture: capture, instance: instance} do
      test = self()

      stub_publish(capture, fn conn, request ->
        send(test, {:request, Enum.map(request["messages"], & &1["attributes"]["ankusa_id"])})
        accept(conn, request)
      end)

      opts = opts(ordering_key: "k", batch_linger_ms: 20, batch_max_inflight: 4)

      for n <- 1..6 do
        Task.async(fn ->
          GooglePubSub.deliver(envelope(%{id: "m#{n}"}), ctx(instance), opts)
        end)
        |> tap(fn _ -> Process.sleep(5) end)
      end
      |> Enum.each(&Task.await/1)

      order =
        Stream.repeatedly(fn ->
          receive do
            {:request, ids} -> ids
          after
            300 -> nil
          end
        end)
        |> Enum.take_while(& &1)
        |> List.flatten()

      assert order == ["m1", "m2", "m3", "m4", "m5", "m6"]
    end

    test "batch: false (or no batch ctx) publishes one message per request", %{
      capture: capture,
      instance: instance
    } do
      assert :ok = GooglePubSub.deliver(envelope(), ctx(instance), opts(batch: false))
      assert {_, _, _, %{"messages" => [_]}} = sent(capture)

      Agent.update(capture, fn _ -> [] end)
      assert :ok = GooglePubSub.deliver(envelope(), %{instance: instance, attempt: 1}, opts())
      assert {_, _, _, %{"messages" => [_]}} = sent(capture)
    end

    test "batched?/1 follows the batch option" do
      assert GooglePubSub.batched?(topic: "t")
      refute GooglePubSub.batched?(topic: "t", batch: false)
      assert Sink.batched?(GooglePubSub, topic: "t")
    end
  end
end
