defmodule Ankusa.SDK.ReceiverTestHandler do
  @moduledoc false

  @behaviour Ankusa.SDK.Handler

  @impl Ankusa.SDK.Handler
  def handle_hook(%Ankusa.SDK.Hook{} = hook, test_pid) do
    send(test_pid, {:hook, hook})

    if String.contains?(hook.body, "fail"), do: {:error, :boom}, else: :ok
  end
end

defmodule Ankusa.SDK.ReceiverTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test, only: [conn: 3]

  alias Ankusa.SDK.{Hook, Receiver}

  @body ~s({"id":"evt_1"})

  defp receiver_opts(opts \\ []) do
    Receiver.init(Keyword.merge([handler: {Ankusa.SDK.ReceiverTestHandler, self()}], opts))
  end

  defp post(path, body, headers \\ []) do
    headers = Enum.map(headers, fn {name, value} -> {to_string(name), value} end)

    conn(:post, path, body)
    |> then(fn conn ->
      Enum.reduce(headers, conn, fn {name, value}, conn -> put_req_header(conn, name, value) end)
    end)
  end

  test "a valid delivery reaches the handler and is answered 202" do
    conn =
      post("/deliveries", @body,
        "x-ankusa-id": "01a0",
        "x-ankusa-source": "demo",
        "x-ankusa-tenant": "acme",
        "content-type": "application/json"
      )
      |> Receiver.call(receiver_opts(path: "/deliveries"))

    assert conn.status == 202
    assert conn.resp_body == ""

    assert_received {:hook, hook}

    assert hook.id == "01a0"
    assert hook.source_id == "demo"
    assert hook.tenant_id == "acme"
    assert hook.content_type == "application/json"
    assert hook.body == @body
    assert hook.received_at == nil
    assert hook.size == byte_size(@body)
    assert hook.dedupe_key == nil
    assert hook.replay_id == nil
    assert hook.idempotency_key == nil
    assert hook.headers["x-ankusa-id"] == "01a0"
    assert hook.headers["x-ankusa-source"] == "demo"
  end

  test "a delivery carries the dedupe key, replay id, and forwarded headers" do
    conn =
      post("/deliveries", @body,
        "x-ankusa-id": "01a0",
        "x-ankusa-source": "demo",
        "x-ankusa-dedupe-key": "evt_9",
        "x-ankusa-replay-id": "rid-1",
        "x-ankusa-idempotency-key": "default:demo:evt_9",
        "x-github-event": "push"
      )
      |> Receiver.call(receiver_opts())

    assert conn.status == 202
    assert_received {:hook, %Hook{} = hook}

    assert hook.dedupe_key == "evt_9"
    assert hook.replay_id == "rid-1"
    assert hook.idempotency_key == "default:demo:evt_9"
    assert hook.headers["x-github-event"] == "push"
  end

  test "a delivery without a tenant has a nil tenant_id" do
    conn =
      post("/deliveries", @body, "x-ankusa-id": "01a0", "x-ankusa-source": "demo")
      |> Receiver.call(receiver_opts())

    assert conn.status == 202
    assert_received {:hook, %Hook{tenant_id: nil, content_type: nil, source_id: "demo"}}
  end

  test "a handler that fails is answered 503 so the dispatcher retries" do
    log =
      capture_log(fn ->
        conn =
          post("/deliveries", "please fail", "x-ankusa-id": "01a0")
          |> Receiver.call(receiver_opts())

        assert conn.status == 503
        assert conn.resp_body == ""
      end)

    assert log =~ "ankusa hook 01a0 not handled: :boom"
  end

  test "a request without x-ankusa-id is answered 400 with a JSON body" do
    conn = post("/deliveries", @body) |> Receiver.call(receiver_opts())

    assert conn.status == 400
    assert Plug.Conn.get_resp_header(conn, "content-type") == ["application/json; charset=utf-8"]
    assert JSON.decode!(conn.resp_body) == %{"error" => "missing x-ankusa-id"}
    refute_received {:hook, _hook}
  end

  test "an empty x-ankusa-id is a missing id" do
    conn =
      post("/deliveries", @body, "x-ankusa-id": "") |> Receiver.call(receiver_opts())

    assert conn.status == 400
    assert JSON.decode!(conn.resp_body) == %{"error" => "missing x-ankusa-id"}
  end

  test "a body over max_body_bytes is answered 413 without reaching the handler" do
    conn =
      post("/deliveries", String.duplicate("a", 128), "x-ankusa-id": "01a0")
      |> Receiver.call(receiver_opts(max_body_bytes: 64))

    assert conn.status == 413
    assert JSON.decode!(conn.resp_body) == %{"error" => "body too large"}
    refute_received {:hook, _hook}
  end

  test "a body exactly at max_body_bytes is accepted" do
    body = String.duplicate("a", 64)

    conn =
      post("/deliveries", body, "x-ankusa-id": "01a0")
      |> Receiver.call(receiver_opts(max_body_bytes: 64))

    assert conn.status == 202
    assert_received {:hook, %Hook{body: ^body, size: 64}}
  end

  test "a request whose body was already parsed raises" do
    conn =
      post("/deliveries", @body, "x-ankusa-id": "01a0", "content-type": "application/json")
      |> Plug.Parsers.call(Plug.Parsers.init(parsers: [:json], json_decoder: JSON))

    assert_raise ArgumentError,
                 "Ankusa.SDK.Receiver needs the raw request body; mount it before Plug.Parsers",
                 fn -> Receiver.call(conn, receiver_opts()) end
  end

  test "a request to another path is passed through untouched" do
    conn =
      post("/other", @body, "x-ankusa-id": "01a0")
      |> Receiver.call(receiver_opts(path: "/deliveries"))

    assert conn.state == :unset
    assert conn.status == nil
    refute_received {:hook, _hook}
  end

  test "init/1 refuses unknown options and a missing handler" do
    assert_raise ArgumentError, ~r/unknown option :nope/, fn ->
      Receiver.init(handler: Ankusa.SDK.ReceiverTestHandler, nope: true)
    end

    assert_raise ArgumentError, ~r/the :handler option is required/, fn ->
      Receiver.init(path: "/deliveries")
    end

    assert_raise ArgumentError, ~r/:handler must be a module or \{module, arg\}/, fn ->
      Receiver.init(handler: "MyApp.Hooks")
    end

    assert_raise ArgumentError, ~r/:max_body_bytes must be a positive integer/, fn ->
      Receiver.init(handler: Ankusa.SDK.ReceiverTestHandler, max_body_bytes: 0)
    end

    assert_raise ArgumentError, ~r/a whsec_ secret must be base64/, fn ->
      Receiver.init(handler: Ankusa.SDK.ReceiverTestHandler, secret: "whsec_!!!")
    end
  end

  describe "with :secret" do
    @secret "whsec_" <> Base.encode64("receiver-test-key")

    defp signed(body, opts \\ []) do
      id = "msg_1"
      timestamp = Integer.to_string(Keyword.get(opts, :timestamp, System.system_time(:second)))
      mac = :crypto.mac(:hmac, :sha256, "receiver-test-key", "#{id}.#{timestamp}.#{body}")

      [
        "x-ankusa-id": "01a0",
        "webhook-id": id,
        "webhook-timestamp": timestamp,
        "webhook-signature": "v1,bm90LXRoaXMtb25l v1," <> Base.encode64(mac)
      ]
    end

    test "a delivery signed with the secret reaches the handler" do
      conn = post("/", @body, signed(@body)) |> Receiver.call(receiver_opts(secret: @secret))

      assert conn.status == 202
      assert_received {:hook, %Hook{body: @body}}
    end

    test "a tampered body is answered 401 and never reaches the handler" do
      conn =
        post("/", @body <> " ", signed(@body)) |> Receiver.call(receiver_opts(secret: @secret))

      assert conn.status == 401

      assert JSON.decode!(conn.resp_body) == %{
               "error" => "invalid_signature",
               "code" => "no_matching_signature"
             }

      refute_received {:hook, _hook}
    end

    test "a stale timestamp is refused even when the signature matches" do
      headers = signed(@body, timestamp: System.system_time(:second) - 301)
      conn = post("/", @body, headers) |> Receiver.call(receiver_opts(secret: @secret))

      assert conn.status == 401
      assert JSON.decode!(conn.resp_body)["code"] == "timestamp_out_of_tolerance"
    end

    test "an unsigned delivery is refused" do
      conn =
        post("/", @body, "x-ankusa-id": "01a0") |> Receiver.call(receiver_opts(secret: @secret))

      assert conn.status == 401
      assert JSON.decode!(conn.resp_body)["code"] == "missing_header"
    end

    test "any secret of a rotation verifies" do
      conn =
        post("/", @body, signed(@body))
        |> Receiver.call(receiver_opts(secret: ["whsec_" <> Base.encode64("old"), @secret]))

      assert conn.status == 202
    end
  end
end
