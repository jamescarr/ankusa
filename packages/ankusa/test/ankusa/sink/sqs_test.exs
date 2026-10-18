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

  test "SQS errors keep their code; only a refused message is permanent" do
    assert {:error, {:sqs, 400, "QueueDoesNotExist", "The specified queue does not exist."} = r} =
             SQS.deliver(envelope(), %{attempt: 1}, opts("missing.fifo"))

    assert {:transient, _} = Sink.classify(r)

    assert {:error, {:permanent, {:sqs, 400, "InvalidMessageContents", _}}} =
             SQS.deliver(envelope(), %{attempt: 1}, opts("bad.fifo"))

    assert {:error, {:status, 500, "kaboom"}} =
             SQS.deliver(envelope(), %{attempt: 1}, opts("boom.fifo"))
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

  # "20260524T000000Z" -> {{2026, 5, 24}, {0, 0, 0}}
  defp parse_amz_date(
         <<y::binary-4, mo::binary-2, d::binary-2, "T", h::binary-2, mi::binary-2, s::binary-2,
           "Z">>
       ) do
    {{to_int(y), to_int(mo), to_int(d)}, {to_int(h), to_int(mi), to_int(s)}}
  end

  defp to_int(bin), do: String.to_integer(bin)
end
