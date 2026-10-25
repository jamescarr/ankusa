defmodule Ankusa.Sink.SQSTest do
  @moduledoc """
  `Sink.SQS` is one signed `SendMessage` per delivery. The transport is stubbed
  with `Req.Test`, so these assert what goes on the wire — the JSON request, the
  headers it is signed over, the signature itself — and how each reply maps to
  an error class.
  """

  use ExUnit.Case, async: true

  alias Ankusa.{Envelope, Sink, UUIDv7}
  alias Ankusa.Sink.{Message, SQS}

  @always ~w(ankusa_id ankusa_source_id ankusa_tenant_id ankusa_message_version
             ankusa_idempotency_key content_type)

  setup do
    {:ok, capture} = Agent.start_link(fn -> [] end)

    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      Agent.update(
        capture,
        &[{conn.method, conn.host, conn.request_path, conn.req_headers, body} | &1]
      )

      request = JSON.decode!(body)

      cond do
        String.contains?(request["QueueUrl"], "missing") ->
          error(conn, 400, "QueueDoesNotExist", "The specified queue does not exist.")

        String.contains?(request["QueueUrl"], "denied") ->
          error(conn, 403, "AccessDenied", "User is not authorized.")

        String.contains?(request["QueueUrl"], "badkey") ->
          error(conn, 400, "InvalidClientTokenId", "The security token is invalid.")

        String.contains?(request["QueueUrl"], "throttled") ->
          error(conn, 400, "ThrottlingException", "Rate exceeded")

        String.contains?(request["QueueUrl"], "unavailable") ->
          error(conn, 503, "ServiceUnavailable", "Try again")

        String.contains?(request["QueueUrl"], "toomany") ->
          error(conn, 429, "SomethingNew", "Too many requests")

        String.contains?(request["QueueUrl"], "overloaded") ->
          Plug.Conn.send_resp(conn, 503, "slow down")

        String.contains?(request["QueueUrl"], "bad") ->
          error(conn, 400, "InvalidMessageContents", "Invalid characters found.")

        String.contains?(request["QueueUrl"], "boom") ->
          Plug.Conn.send_resp(conn, 500, "kaboom")

        true ->
          md5 = Base.encode16(:crypto.hash(:md5, request["MessageBody"]), case: :lower)
          ok(conn, md5)
      end
    end)

    %{capture: capture}
  end

  defp ok(conn, md5) do
    Plug.Conn.send_resp(
      conn,
      200,
      JSON.encode!(%{"MessageId" => "m1", "MD5OfMessageBody" => md5})
    )
  end

  defp error(conn, status, code, message) do
    body = JSON.encode!(%{"__type" => "com.amazonaws.sqs##{code}", "message" => message})
    Plug.Conn.send_resp(conn, status, body)
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

  defp opts(queue \\ "hooks.fifo", extra \\ []) do
    [
      queue_url: "http://sqs.test/000000000000/" <> queue,
      region: "us-east-1",
      access_key_id: "test",
      secret_access_key: "test",
      req_options: [plug: {Req.Test, __MODULE__}]
    ] ++ extra
  end

  defp sent(capture) do
    assert [{method, host, path, headers, body}] = Agent.get(capture, & &1)
    {method, host, path, Map.new(headers), body, JSON.decode!(body)}
  end

  test "a FIFO send: the JSON protocol, signed for sqs, with group, dedup id and attributes",
       %{capture: capture} do
    env = envelope()

    assert :ok = SQS.deliver(env, %{attempt: 1}, opts())

    {method, host, path, h, _body, request} = sent(capture)

    assert {method, host, path} == {"POST", "sqs.test", "/"}
    assert h["x-amz-target"] == "AmazonSQS.SendMessage"
    assert h["content-type"] == "application/x-amz-json-1.0"
    assert h["authorization"] =~ ~r|Credential=test/\d{8}/us-east-1/sqs/aws4_request|

    assert h["authorization"] =~
             "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date;x-amz-target,"

    {:ok, payload} = Message.encode(env, %{attempt: 1}, Message.default_inline_max_bytes())

    assert request["QueueUrl"] == "http://sqs.test/000000000000/hooks.fifo"
    assert request["MessageBody"] == payload
    assert request["MessageGroupId"] == "acme/stripe"
    assert request["MessageDeduplicationId"] == env.id

    attributes = request["MessageAttributes"]
    assert Enum.sort(Map.keys(attributes)) == Enum.sort(@always)
    assert attributes["ankusa_id"] == %{"DataType" => "String", "StringValue" => env.id}
    assert attributes["ankusa_tenant_id"]["StringValue"] == "acme"
    assert attributes["ankusa_idempotency_key"]["StringValue"] == env.id
  end

  test "a replay gets its own dedup id and carries ankusa_replay_id", %{capture: capture} do
    env = envelope(%{dedupe_key: "evt_1"})

    assert :ok = SQS.deliver(env, %{attempt: 1, replay_id: "r1"}, opts())

    {_, _, _, _, _, request} = sent(capture)
    assert request["MessageDeduplicationId"] == env.id <> ":replay:r1"
    assert request["MessageAttributes"]["ankusa_replay_id"]["StringValue"] == "r1"
    assert request["MessageAttributes"]["ankusa_dedupe_key"]["StringValue"] == "evt_1"
  end

  test "a standard queue sends no dedup id, and a group id only when configured", %{
    capture: capture
  } do
    assert :ok = SQS.deliver(envelope(), %{attempt: 1}, opts("hooks"))
    {_, _, _, _, _, request} = sent(capture)
    refute Map.has_key?(request, "MessageDeduplicationId")
    refute Map.has_key?(request, "MessageGroupId")

    Agent.update(capture, fn _ -> [] end)

    assert :ok = SQS.deliver(envelope(), %{attempt: 1}, opts("hooks", message_group_id: "g"))
    {_, _, _, _, _, request} = sent(capture)
    refute Map.has_key?(request, "MessageDeduplicationId")
    assert request["MessageGroupId"] == "g"
  end

  test "no tenant: no ankusa_tenant_id attribute (SQS refuses an empty one), group /source", %{
    capture: capture
  } do
    assert :ok = SQS.deliver(envelope(%{tenant_id: nil}), %{attempt: 1}, opts())

    {_, _, _, _, _, request} = sent(capture)
    refute Map.has_key?(request["MessageAttributes"], "ankusa_tenant_id")
    assert request["MessageGroupId"] == "/stripe"
  end

  test "the signature reproduces from what was sent: service sqs, these headers, these bytes",
       %{capture: capture} do
    assert :ok = SQS.deliver(envelope(), %{attempt: 1}, opts())

    {_, _, _, h, body, _} = sent(capture)

    resigned =
      :aws_signature.sign_v4(
        "test",
        "test",
        "us-east-1",
        "sqs",
        parse_amz_date(h["x-amz-date"]),
        "POST",
        "http://sqs.test/",
        [
          {"host", h["host"]},
          {"content-type", h["content-type"]},
          {"x-amz-target", h["x-amz-target"]}
        ],
        body,
        []
      )

    assert resigned |> Map.new() |> Map.fetch!("Authorization") == h["authorization"]
  end

  test "a session token is sent and signed", %{capture: capture} do
    assert :ok = SQS.deliver(envelope(), %{attempt: 1}, opts("hooks.fifo", session_token: "tok"))

    {_, _, _, h, _, _} = sent(capture)
    assert h["x-amz-security-token"] == "tok"
    assert h["authorization"] =~ ~r/SignedHeaders=[^,]*x-amz-security-token/
  end

  test "SQS errors keep their code; a refused message is permanent, a refused credential or queue a config error" do
    assert {:error,
            {:permanent,
             {:config, {:sqs, 400, "QueueDoesNotExist", "The specified queue does not exist."}}} =
              r} =
             SQS.deliver(envelope(), %{attempt: 1}, opts("missing.fifo"))

    assert {:permanent, {:config, _}} = Sink.classify(r)

    assert {:error, {:permanent, {:config, {:sqs, 403, "AccessDenied", _}}}} =
             SQS.deliver(envelope(), %{attempt: 1}, opts("denied.fifo"))

    assert {:error, {:permanent, {:config, {:sqs, 400, "InvalidClientTokenId", _}}}} =
             SQS.deliver(envelope(), %{attempt: 1}, opts("badkey.fifo"))

    assert {:error, {:permanent, {:sqs, 400, "InvalidMessageContents", _}}} =
             SQS.deliver(envelope(), %{attempt: 1}, opts("bad.fifo"))

    assert {:error, {:status, 500, "kaboom"}} =
             SQS.deliver(envelope(), %{attempt: 1}, opts("boom.fifo"))
  end

  test "throttling and 429/503 are a jittered retry-after, never a permanent error" do
    for queue <- ["throttled.fifo", "unavailable.fifo", "toomany.fifo", "overloaded.fifo"] do
      assert {:error, {:retry_after, ms, reason} = failure} =
               SQS.deliver(envelope(), %{attempt: 1}, opts(queue))

      # full jitter: 1..500 ms on the first attempt
      assert ms in 1..500
      assert {:retry_after, ^ms, ^reason} = Sink.classify(failure)
    end

    assert {:error, {:retry_after, _ms, {:sqs, 400, "ThrottlingException", _}}} =
             SQS.deliver(envelope(), %{attempt: 1}, opts("throttled.fifo"))

    assert {:error, {:retry_after, _ms, {:status, 503, "slow down"}}} =
             SQS.deliver(envelope(), %{attempt: 1}, opts("overloaded.fifo"))

    # the ceiling doubles per attempt, to 30 s, so a long throttle spreads out
    draws =
      for _ <- 1..200 do
        {:error, {:retry_after, ms, _}} =
          SQS.deliver(envelope(), %{attempt: 7}, opts("throttled.fifo"))

        ms
      end

    assert Enum.all?(draws, &(&1 in 1..30_000))
    assert Enum.max(draws) > 500
    assert length(Enum.uniq(draws)) > 20
  end

  test "a 200 whose MD5OfMessageBody doesn't match the body sent is an error" do
    Req.Test.stub(__MODULE__, fn conn -> ok(conn, String.duplicate("0", 32)) end)

    assert {:error, {:md5_mismatch, _expected, "00000000000000000000000000000000"}} =
             SQS.deliver(envelope(), %{attempt: 1}, opts())
  end

  test "a message over max_message_bytes is permanent and never sent", %{capture: capture} do
    assert {:error, {:permanent, {:message_too_large, size, 100}}} =
             SQS.deliver(envelope(), %{attempt: 1}, opts("hooks.fifo", max_message_bytes: 100))

    assert size > 100
    assert Agent.get(capture, & &1) == []
  end

  test "a 1 MiB forwarded header is dropped and counted: the message stays under SQS's limit",
       %{capture: capture} do
    handler = "sqs-headers-dropped-#{System.unique_integer([:positive])}"
    test = self()

    :telemetry.attach(
      handler,
      [:ankusa, :sink, :headers_dropped],
      &__MODULE__.report/4,
      test
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    env =
      envelope(%{
        headers: [{"x-ok", "yes"}, {"x-huge", String.duplicate("h", 1_048_576)}]
      })

    ctx = %{attempt: 1, instance: :sqs_header_cap}
    assert :ok = SQS.deliver(env, ctx, opts())

    {_method, _host, _path, _h, body, request} = sent(capture)
    message = JSON.decode!(request["MessageBody"])

    assert message["headers"] == %{"x-ok" => "yes"}
    assert byte_size(body) < 1_048_576

    assert_receive {:dropped, %{count: 1}, %{instance: :sqs_header_cap, reason: :bytes}}
  end

  @doc false
  def report(_event, measurements, meta, pid), do: send(pid, {:dropped, measurements, meta})

  test "describe/2: the queue as the channel, with the sqs binding" do
    assert %Sink.Description{
             protocol: "sqs",
             host: "sqs.test",
             address: "hooks.fifo",
             channel_bindings: %{
               "sqs" => %{
                 "queue" => %{"name" => "hooks.fifo", "fifoQueue" => true},
                 "bindingVersion" => "0.3.0"
               }
             },
             message_bindings: %{},
             ankusa_headers: false
           } = SQS.describe(%{source_id: "stripe", tenant_id: "acme"}, opts())
  end

  describe "batched (a delivery from dispatch: ctx batch: true, an instance's batchers)" do
    setup do
      instance = :"sqs_batch_#{System.unique_integer([:positive])}"
      start_supervised!({Ankusa.Sink.Batcher.Workers, instance: instance})
      %{instance: instance}
    end

    # `Req.Test` stubs by name; each test's requests are its own, because each
    # test has its own queue (a batcher per destination).
    defp batch_opts(extra \\ []),
      do: opts("hooks.fifo", Keyword.merge([batch_linger_ms: 200], extra))

    defp ctx(instance, extra \\ %{}),
      do: Map.merge(%{instance: instance, batch: true, attempt: 1}, extra)

    # What SQS answers: each entry Successful with the MD5 of its body, except
    # those in `failures` (index => {code, sender_fault?}).
    defp batch_reply(conn, request, failures \\ %{}) do
      {successful, failed} =
        Enum.reduce(request["Entries"], {[], []}, fn %{"Id" => id} = entry, {ok, bad} ->
          case failures do
            %{^id => {code, sender_fault?}} ->
              {ok,
               [
                 %{
                   "Id" => id,
                   "Code" => code,
                   "Message" => "no #{id}",
                   "SenderFault" => sender_fault?
                 }
                 | bad
               ]}

            _ ->
              md5 = Base.encode16(:crypto.hash(:md5, entry["MessageBody"]), case: :lower)
              {[%{"Id" => id, "MessageId" => "m#{id}", "MD5OfMessageBody" => md5} | ok], bad}
          end
        end)

      Plug.Conn.send_resp(
        conn,
        200,
        JSON.encode!(%{"Successful" => successful, "Failed" => failed})
      )
    end

    defp stub_batches(capture, fun) do
      Req.Test.stub(__MODULE__, fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        request = JSON.decode!(body)
        Agent.update(capture, &[{Map.new(conn.req_headers), request} | &1])
        fun.(conn, request)
      end)
    end

    defp deliver_all(envs, ctx, opts) do
      envs
      |> Enum.map(fn env -> Task.async(fn -> {env.id, SQS.deliver(env, ctx, opts)} end) end)
      |> Map.new(&Task.await/1)
    end

    test "ten deliveries are one SendMessageBatch, each entry what a SendMessage carries",
         %{capture: capture, instance: instance} do
      stub_batches(capture, &batch_reply/2)
      envs = for _ <- 1..10, do: envelope()

      results = deliver_all(envs, ctx(instance), batch_opts())

      assert Map.values(results) == List.duplicate(:ok, 10)
      assert [{headers, request}] = Agent.get(capture, & &1)
      assert headers["x-amz-target"] == "AmazonSQS.SendMessageBatch"
      assert request["QueueUrl"] == "http://sqs.test/000000000000/hooks.fifo"
      assert length(request["Entries"]) == 10

      # the Id is a position, never the hook's id (a `:` is not allowed in one)
      assert request["Entries"] |> Enum.map(& &1["Id"]) |> Enum.sort() ==
               Enum.map(0..9, &Integer.to_string/1) |> Enum.sort()

      assert request["Entries"] |> Enum.map(& &1["MessageDeduplicationId"]) |> Enum.sort() ==
               envs |> Enum.map(& &1.id) |> Enum.sort()

      for entry <- request["Entries"] do
        assert entry["MessageGroupId"] == "acme/stripe"

        assert entry["MessageAttributes"]["ankusa_id"]["StringValue"] ==
                 entry["MessageDeduplicationId"]
      end

      assert headers["authorization"] =~ ~r|Credential=test/\d{8}/us-east-1/sqs/aws4_request|
    end

    test "a hook twice in one batch (its replay) has two distinct entries and dedup ids",
         %{capture: capture, instance: instance} do
      stub_batches(capture, &batch_reply/2)
      env = envelope(%{id: "default:demo:retry-1"})

      results =
        [
          Task.async(fn -> SQS.deliver(env, ctx(instance), batch_opts()) end),
          Task.async(fn -> SQS.deliver(env, ctx(instance, %{replay_id: "r1"}), batch_opts()) end)
        ]
        |> Enum.map(&Task.await/1)

      assert results == [:ok, :ok]
      assert [{_, %{"Entries" => entries}}] = Agent.get(capture, & &1)
      assert [_, _] = entries
      assert Enum.map(entries, & &1["Id"]) |> Enum.sort() == ["0", "1"]

      assert Enum.map(entries, & &1["MessageDeduplicationId"]) |> Enum.sort() ==
               ["default:demo:retry-1", "default:demo:retry-1:replay:r1"]
    end

    test "a partial failure is mapped to exactly the failed entries, each classified",
         %{capture: capture, instance: instance} do
      stub_batches(capture, fn conn, request ->
        # fail the entries of hooks "bad" (refused) and "slow" (throttled)
        failures =
          for %{"Id" => id, "MessageDeduplicationId" => dedup} <- request["Entries"],
              dedup in ["bad", "slow", "down"],
              into: %{} do
            case dedup do
              "bad" -> {id, {"InvalidParameterValue", true}}
              "slow" -> {id, {"ThrottlingException", true}}
              "down" -> {id, {"InternalError", false}}
            end
          end

        batch_reply(conn, request, failures)
      end)

      envs = [
        envelope(%{id: "good-1"}),
        envelope(%{id: "bad"}),
        envelope(%{id: "slow"}),
        envelope(%{id: "down"}),
        envelope(%{id: "good-2"})
      ]

      results = deliver_all(envs, ctx(instance, %{attempt: 2}), batch_opts())

      assert [{_, _}] =
               Agent.get(capture, &Enum.map(&1, fn {h, r} -> {h, length(r["Entries"])} end))

      assert results["good-1"] == :ok
      assert results["good-2"] == :ok

      # refused: dead-lettered at once, nothing else
      assert {:error, {:permanent, {:sqs, 400, "InvalidParameterValue", "no" <> _}}} =
               results["bad"]

      # throttled: a jittered pause, then the retry policy
      assert {:error, {:retry_after, ms, {:sqs, 400, "ThrottlingException", _}}} = results["slow"]
      assert ms in 1..30_000
      assert {:retry_after, ^ms, _} = Sink.classify(elem(results["slow"], 1))

      # anything else: transient
      assert {:error, {:sqs, 500, "InternalError", _} = r} = results["down"]
      assert {:transient, _} = Sink.classify(r)
    end

    test "an entry the reply does not mention, or with the wrong MD5, fails alone",
         %{capture: capture, instance: instance} do
      stub_batches(capture, fn conn, request ->
        [first, second, _third] = request["Entries"]
        md5 = &Base.encode16(:crypto.hash(:md5, &1["MessageBody"]), case: :lower)

        # the third entry is in neither list; the second has another body's MD5
        reply = %{
          "Successful" => [
            %{"Id" => first["Id"], "MessageId" => "m", "MD5OfMessageBody" => md5.(first)},
            %{
              "Id" => second["Id"],
              "MessageId" => "m",
              "MD5OfMessageBody" => String.duplicate("0", 32)
            }
          ],
          "Failed" => []
        }

        Plug.Conn.send_resp(conn, 200, JSON.encode!(reply))
      end)

      envs = for _ <- 1..3, do: envelope()
      results = envs |> deliver_all(ctx(instance), batch_opts()) |> Map.values()

      assert Enum.count(results, &(&1 == :ok)) == 1
      assert Enum.count(results, &match?({:error, {:md5_mismatch, _, _}}, &1)) == 1
      assert Enum.count(results, &match?({:error, {:unexpected_reply, _}}, &1)) == 1
    end

    test "a request that fails as a whole fails every entry the way a single send would",
         %{capture: capture, instance: instance} do
      envs = fn -> for _ <- 1..3, do: envelope() end

      stub_batches(capture, fn conn, _ -> Plug.Conn.send_resp(conn, 503, "slow down") end)

      results = envs.() |> deliver_all(ctx(instance), batch_opts()) |> Map.values()
      assert [{:error, {:retry_after, _, {:status, 503, "slow down"}}} | _] = results
      assert length(results) == 3

      stub_batches(capture, fn conn, _ -> Plug.Conn.send_resp(conn, 500, "kaboom") end)
      results = envs.() |> deliver_all(ctx(instance), batch_opts()) |> Map.values()
      assert results == List.duplicate({:error, {:status, 500, "kaboom"}}, 3)

      stub_batches(capture, fn conn, _ ->
        error(conn, 400, "QueueDoesNotExist", "The specified queue does not exist.")
      end)

      results = envs.() |> deliver_all(ctx(instance), batch_opts()) |> Map.values()

      assert results ==
               List.duplicate(
                 {:error,
                  {:permanent,
                   {:config,
                    {:sqs, 400, "QueueDoesNotExist", "The specified queue does not exist."}}}},
                 3
               )
    end

    test "throttling an entire request is a retry_after, with its own jitter per entry",
         %{capture: capture, instance: instance} do
      stub_batches(capture, fn conn, _ ->
        error(conn, 400, "ThrottlingException", "Rate exceeded")
      end)

      results =
        for(_ <- 1..4, do: envelope())
        |> deliver_all(ctx(instance, %{attempt: 3}), batch_opts())
        |> Map.values()

      assert [{:error, {:retry_after, _, {:sqs, 400, "ThrottlingException", _}}} | _] = results
      assert Enum.all?(results, &match?({:error, {:retry_after, _, _}}, &1))
    end

    test "no credentials fails the batch without a request", %{
      capture: capture,
      instance: instance
    } do
      # the chain's last source (IMDS) goes through the same stub: nothing there
      Req.Test.stub(__MODULE__, fn conn -> Plug.Conn.send_resp(conn, 404, "") end)
      opts = Keyword.drop(batch_opts(), [:access_key_id, :secret_access_key])

      assert {:error, :no_credentials} =
               SQS.deliver(envelope(), ctx(instance), opts ++ [env: %{}])

      assert Agent.get(capture, & &1) == []
    end

    test "batch: false (or no batch ctx) sends one SendMessage each", %{
      capture: capture,
      instance: instance
    } do
      assert :ok = SQS.deliver(envelope(), ctx(instance), batch_opts(batch: false))

      assert {_, _, _, headers, _, _} = sent(capture)
      assert headers["x-amz-target"] == "AmazonSQS.SendMessage"

      Agent.update(capture, fn _ -> [] end)
      assert :ok = SQS.deliver(envelope(), %{instance: instance, attempt: 1}, batch_opts())
      assert {_, _, _, headers, _, _} = sent(capture)
      assert headers["x-amz-target"] == "AmazonSQS.SendMessage"
    end

    test "an ordered FIFO group is one request in flight, entries in admission order",
         %{capture: capture, instance: instance} do
      test = self()

      stub_batches(capture, fn conn, request ->
        send(test, {:request, Enum.map(request["Entries"], & &1["MessageDeduplicationId"])})
        batch_reply(conn, request)
      end)

      opts = batch_opts(batch_linger_ms: 20, batch_max_inflight: 4)

      for n <- 1..6 do
        Task.async(fn -> SQS.deliver(envelope(%{id: "m#{n}"}), ctx(instance), opts) end)
        |> tap(fn _ -> Process.sleep(5) end)
      end
      |> Enum.each(&Task.await/1)

      # one group (`acme/stripe`), so whatever the batching, the receiver's
      # order is the admission order
      sent_order =
        Stream.repeatedly(fn ->
          receive do
            {:request, ids} -> ids
          after
            300 -> nil
          end
        end)
        |> Enum.take_while(& &1)
        |> List.flatten()

      assert sent_order == ["m1", "m2", "m3", "m4", "m5", "m6"]
    end

    test "batched?/1 follows the batch option" do
      assert SQS.batched?(queue_url: "x")
      refute SQS.batched?(queue_url: "x", batch: false)
      assert Sink.batched?(SQS, queue_url: "x")
      refute Sink.batched?(Sink.Log, [])
    end

    test "a batch option out of range fails validate_opts!/1 and dead-letters a delivery as a config error",
         %{instance: instance} do
      assert :ok = SQS.validate_opts!(batch_opts(batch_max_entries: 10))

      assert_raise ArgumentError, ~r/:batch_max_entries must be an integer from 1 to 10/, fn ->
        SQS.validate_opts!(batch_opts(batch_max_entries: 11))
      end

      assert_raise ArgumentError, ~r/:batch must be true or false/, fn ->
        SQS.validate_opts!(batch_opts(batch: "yes"))
      end

      assert {:error, {:permanent, {:config, {:batch_options, message}}}} =
               SQS.deliver(envelope(), ctx(instance), batch_opts(batch_max_entries: 11))

      assert message =~ ":batch_max_entries"
    end
  end

  # "20260524T000000Z" -> {{2026, 5, 24}, {0, 0, 0}}
  defp parse_amz_date(
         <<y::binary-4, mo::binary-2, d::binary-2, "T", h::binary-2, mi::binary-2, s::binary-2,
           "Z">>
       ) do
    {{to_int(y), to_int(mo), to_int(d)}, {to_int(h), to_int(mi), to_int(s)}}
  end

  defp to_int(bin), do: String.to_integer(bin)
end
