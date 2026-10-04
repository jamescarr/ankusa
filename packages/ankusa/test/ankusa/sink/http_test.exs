defmodule Ankusa.Sink.HttpTest do
  @moduledoc """
  `Sink.Http` forwards the envelope body to an HTTP endpoint. The transport is
  stubbed with `Req.Test`, so these assert what actually goes on the wire rather
  than that a request was made.
  """

  use ExUnit.Case, async: true

  alias Ankusa.{Envelope, Sink, UUIDv7}

  setup do
    {:ok, capture} = Agent.start_link(fn -> [] end)

    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      Agent.update(capture, &[{conn.method, conn.request_path, conn.req_headers, body} | &1])

      case conn.request_path do
        "/boom" -> Plug.Conn.send_resp(conn, 503, "nope")
        _ -> Plug.Conn.send_resp(conn, 200, "ok")
      end
    end)

    %{capture: capture}
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

  defp opts(path, extra \\ []) do
    [url: "http://sink.test" <> path, req_options: [plug: {Req.Test, __MODULE__}]] ++ extra
  end

  test "forwards the body verbatim, with the identity headers and content type", %{
    capture: capture
  } do
    env = envelope()

    assert :ok = Sink.Http.deliver(env, %{attempt: 1}, opts("/hooks"))

    assert [{"POST", "/hooks", headers, body}] = Agent.get(capture, & &1)
    h = Map.new(headers)

    assert body == env.body
    assert h["x-ankusa-id"] == env.id
    assert h["x-ankusa-source"] == "stripe"
    assert h["x-ankusa-tenant"] == "acme"
    assert h["content-type"] == "application/json"
  end

  test "falls back to application/octet-stream when the envelope has no content type", %{
    capture: capture
  } do
    assert :ok = Sink.Http.deliver(envelope(%{content_type: nil}), %{attempt: 1}, opts("/hooks"))

    assert [{"POST", "/hooks", headers, _body}] = Agent.get(capture, & &1)
    assert Map.new(headers)["content-type"] == "application/octet-stream"
  end

  test "method and extra headers are configurable", %{capture: capture} do
    assert :ok =
             Sink.Http.deliver(
               envelope(),
               %{attempt: 1},
               opts("/h", method: :put, headers: [{"x-trace", "abc"}])
             )

    assert [{"PUT", "/h", headers, _body}] = Agent.get(capture, & &1)
    assert Map.new(headers)["x-trace"] == "abc"
  end

  test "a non-2xx is an error the source's retry policy acts on", %{capture: capture} do
    assert {:error, {:status, 503}} = Sink.Http.deliver(envelope(), %{attempt: 1}, opts("/boom"))
    assert [_seen] = Agent.get(capture, & &1)
  end

  test "a redirect is reported, not followed", %{capture: capture} do
    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      Agent.update(capture, &[{conn.method, conn.request_path, conn.req_headers, body} | &1])

      conn
      |> Plug.Conn.put_resp_header("location", "/login")
      |> Plug.Conn.send_resp(302, "")
    end)

    # Following it would re-send the hook as a GET and return that response,
    # so the source would see a successful delivery of a hook nobody accepted.
    assert {:error, {:status, 302}} = Sink.Http.deliver(envelope(), %{attempt: 1}, opts("/hooks"))

    assert [{"POST", "/hooks", _headers, body}] = Agent.get(capture, & &1)
    assert body == envelope().body
  end

  test "transport options are an allowlist — ones that change the request raise" do
    # A caller reaching for these would quietly defeat the adapter: `params:`
    # rewrites a signed URL, `auth:` adds a second credential.
    assert_raise ArgumentError, ~r/unsupported :req_options key :params/, fn ->
      Sink.Http.deliver(envelope(), %{attempt: 1},
        url: "http://sink.test/hooks",
        req_options: [params: [a: 1], plug: {Req.Test, __MODULE__}]
      )
    end
  end

  # ── G5 forwarded headers ──────────────────────────────────────────────────

  test "provider headers are forwarded per the source option, minus the denylist", %{
    capture: capture
  } do
    env =
      envelope(%{
        dedupe_key: "evt_1",
        headers: [
          {"X-GitHub-Event", "push"},
          {"Authorization", "Bearer secret"},
          {"X-Ankusa-Whatever", "no"},
          {"Content-Length", "17"}
        ]
      })

    assert :ok =
             Sink.Http.deliver(
               env,
               %{attempt: 1, replay_id: "rid", forward_headers: :default},
               opts("/hooks")
             )

    assert [{"POST", "/hooks", headers, _body}] = Agent.get(capture, & &1)
    h = Map.new(headers)
    assert h["x-github-event"] == "push"
    assert h["x-ankusa-dedupe-key"] == "evt_1"
    assert h["x-ankusa-idempotency-key"] == "acme:stripe:evt_1"
    assert h["x-ankusa-replay-id"] == "rid"
    refute Map.has_key?(h, "authorization")
    refute Map.has_key?(h, "x-ankusa-whatever")
    refute Map.has_key?(h, "content-length")
  end

  test "an allowlist forwards only the named headers", %{capture: capture} do
    env = envelope(%{headers: [{"x-keep", "1"}, {"x-drop", "2"}]})

    assert :ok =
             Sink.Http.deliver(env, %{attempt: 1, forward_headers: ["x-keep"]}, opts("/hooks"))

    assert [{"POST", "/hooks", headers, _body}] = Agent.get(capture, & &1)
    h = Map.new(headers)
    assert h["x-keep"] == "1"
    refute Map.has_key?(h, "x-drop")
  end

  test "a forwarded name colliding with a sink-set or opts header is dropped", %{
    capture: capture
  } do
    # The provider sends its own x-ankusa-id; the sink's identity header wins.
    env = envelope(%{headers: [{"x-ankusa-id", "forged"}, {"x-trace", "provider"}]})

    assert :ok =
             Sink.Http.deliver(
               env,
               %{attempt: 1, forward_headers: :default},
               opts("/hooks", headers: [{"x-trace", "operator"}])
             )

    assert [{"POST", "/hooks", headers, _body}] = Agent.get(capture, & &1)
    h = Map.new(headers)
    assert h["x-ankusa-id"] == env.id
    assert h["x-trace"] == "operator"
  end

  test "no dedupe key or replay id means no such headers", %{capture: capture} do
    env = envelope()
    assert :ok = Sink.Http.deliver(env, %{attempt: 1}, opts("/hooks"))

    assert [{"POST", "/hooks", headers, _body}] = Agent.get(capture, & &1)
    h = Map.new(headers)
    assert h["x-ankusa-idempotency-key"] == env.id
    refute Map.has_key?(h, "x-ankusa-dedupe-key")
    refute Map.has_key?(h, "x-ankusa-replay-id")
  end
end
