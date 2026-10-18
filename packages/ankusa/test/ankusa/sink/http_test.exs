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

  # A public address literal under `https`: the default egress policy admits it
  # without a DNS lookup, and `Req.Test` answers before any socket opens.
  defp opts(path, extra \\ []) do
    [url: "https://203.0.113.10" <> path, req_options: [plug: {Req.Test, __MODULE__}]] ++ extra
  end

  test "forwards the body verbatim, with the identity headers and content type", %{
    capture: capture
  } do
    env = envelope(%{verification: %Ankusa.Verification{status: :ok}})

    assert :ok = Sink.Http.deliver(env, %{attempt: 1}, opts("/hooks"))

    assert [{"POST", "/hooks", headers, body}] = Agent.get(capture, & &1)
    h = Map.new(headers)

    assert body == env.body
    assert h["x-ankusa-id"] == env.id
    assert h["x-ankusa-source"] == "stripe"
    assert h["x-ankusa-tenant"] == "acme"
    assert h["content-type"] == "application/json"
    assert h["x-ankusa-verification"] == "ok"
    refute Map.has_key?(h, "x-ankusa-verification-flagged")
  end

  test "a hook accepted after a failed check is marked flagged, without the reason", %{
    capture: capture
  } do
    env =
      envelope(%{
        verification: %Ankusa.Verification{status: :failed, flagged: true, reason: :no_match}
      })

    assert :ok = Sink.Http.deliver(env, %{attempt: 1}, opts("/hooks"))

    assert [{"POST", "/hooks", headers, _body}] = Agent.get(capture, & &1)
    h = Map.new(headers)
    assert h["x-ankusa-verification"] == "failed"
    assert h["x-ankusa-verification-flagged"] == "true"
    refute Enum.any?(headers, fn {_name, value} -> value =~ "no_match" end)
  end

  test "an envelope nothing verified is skipped, and a provider cannot claim otherwise", %{
    capture: capture
  } do
    env =
      envelope(%{
        verification: nil,
        headers: [{"x-ankusa-verification", "ok"}, {"x-ankusa-verification-flagged", "true"}]
      })

    assert :ok = Sink.Http.deliver(env, %{attempt: 1, forward_headers: :default}, opts("/hooks"))

    assert [{"POST", "/hooks", headers, _body}] = Agent.get(capture, & &1)
    h = Map.new(headers)
    assert h["x-ankusa-verification"] == "skipped"
    refute Map.has_key?(h, "x-ankusa-verification-flagged")
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
        url: "https://203.0.113.10/hooks",
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

  describe "responses" do
    defp respond(status, headers \\ [], body \\ "") do
      Req.Test.stub(__MODULE__, fn conn ->
        conn = Enum.reduce(headers, conn, fn {k, v}, c -> Plug.Conn.put_resp_header(c, k, v) end)
        Plug.Conn.send_resp(conn, status, body)
      end)
    end

    test "a status no retry can fix is permanent" do
      for status <- [400, 401, 403, 404, 410, 413, 422] do
        respond(status)

        assert Sink.Http.deliver(envelope(), %{attempt: 1}, opts("/h")) ==
                 {:error, {:permanent, {:status, status}}}
      end
    end

    test "Retry-After on 408, 429 and 5xx becomes a minimum delay" do
      respond(503, [{"retry-after", "7"}])

      assert Sink.Http.deliver(envelope(), %{attempt: 1}, opts("/h")) ==
               {:error, {:retry_after, 7_000, {:status, 503}}}

      respond(429, [{"retry-after", "1"}])

      assert Sink.Http.deliver(envelope(), %{attempt: 1}, opts("/h")) ==
               {:error, {:retry_after, 1_000, {:status, 429}}}

      later = DateTime.utc_now() |> DateTime.add(120, :second)
      date = Calendar.strftime(later, "%a, %d %b %Y %H:%M:%S GMT")
      respond(503, [{"retry-after", date}])

      assert {:error, {:retry_after, ms, {:status, 503}}} =
               Sink.Http.deliver(envelope(), %{attempt: 1}, opts("/h"))

      assert ms in 100_000..120_000
    end

    test "a 5xx without Retry-After, or with an unusable one, is transient" do
      respond(502)
      assert Sink.Http.deliver(envelope(), %{attempt: 1}, opts("/h")) == {:error, {:status, 502}}

      respond(503, [{"retry-after", "Sun, 06 Nov 1994 08:49:37 GMT"}])
      assert Sink.Http.deliver(envelope(), %{attempt: 1}, opts("/h")) == {:error, {:status, 503}}
    end

    test "a response body past max_response_bytes is cut off, and the status still counts" do
      respond(200, [], :binary.copy("x", 1_048_576))

      assert :ok =
               Sink.Http.deliver(envelope(), %{attempt: 1}, opts("/h", max_response_bytes: 1_024))

      assert {:ok, 200, {:truncated, read}} =
               Ankusa.HttpClient.request(:post, "http://sink.test/h", [], "x", 5_000,
                 plug: {Req.Test, __MODULE__},
                 max_response_bytes: 1_024
               )

      assert read > 1_024
    end
  end

  describe "signing" do
    @key :crypto.strong_rand_bytes(32)
    @secret "whsec_" <> Base.encode64(@key)

    test "a signed delivery verifies with the Standard Webhooks verifier", %{capture: capture} do
      env = envelope()
      assert :ok = Sink.Http.deliver(env, %{attempt: 1}, opts("/hooks", secret: @secret))

      assert [{"POST", "/hooks", headers, body}] = Agent.get(capture, & &1)
      h = Map.new(headers)
      assert h["webhook-id"] == env.id
      assert ["v1," <> _] = String.split(h["webhook-signature"], " ")

      received = %{env | headers: headers, body: body}

      assert :ok =
               Ankusa.Verifier.Hmac.verify(received, scheme: :standard_webhooks, secret: @secret)
    end

    test "two secrets sign twice; a forwarded webhook-* header never overrides them", %{
      capture: capture
    } do
      other = "whsec_" <> Base.encode64(:crypto.strong_rand_bytes(32))
      env = envelope(%{headers: [{"webhook-signature", "v1,forged"}]})

      assert :ok =
               Sink.Http.deliver(
                 env,
                 %{attempt: 1, forward_headers: ["webhook-signature"]},
                 opts("/hooks", secret: [@secret, other])
               )

      assert [{"POST", "/hooks", headers, body}] = Agent.get(capture, & &1)
      assert [signature] = for({"webhook-signature", v} <- headers, do: v)
      assert [_, _] = String.split(signature, " ")

      received = %{env | headers: headers, body: body}

      assert :ok =
               Ankusa.Verifier.Hmac.verify(received, scheme: :standard_webhooks, secret: other)
    end

    test "a secret that does not decode is a permanent failure, and nothing is sent", %{
      capture: capture
    } do
      assert Sink.Http.deliver(envelope(), %{attempt: 1}, opts("/hooks", secret: "whsec_!!!")) ==
               {:error, {:permanent, :bad_secret}}

      assert Agent.get(capture, & &1) == []
    end

    test "signs exactly what the SDK conformance vectors expect" do
      %{"cases" => cases} =
        "../../conformance/cases/signature.json" |> File.read!() |> JSON.decode!()

      %{"input" => input} = Enum.find(cases, &(&1["id"] == "signature.ok.reference"))
      %{"headers" => headers, "body" => %{"text" => body}, "secrets" => [secret]} = input

      assert {:ok, signed} =
               Sink.Http.Signer.headers(
                 headers["webhook-id"],
                 body,
                 secret,
                 String.to_integer(headers["webhook-timestamp"])
               )

      assert Map.new(signed)["webhook-signature"] == headers["webhook-signature"]
    end
  end

  describe "egress policy" do
    @instance :sink_http_egress_test

    defmodule HostEcho do
      @moduledoc false
      @behaviour Plug
      import Plug.Conn

      @impl true
      def init(opts), do: opts

      @impl true
      def call(conn, opts) do
        send(opts[:test], {:request, get_req_header(conn, "host"), conn.request_path})
        send_resp(conn, 200, "ok")
      end
    end

    # Stores an instance config carrying `egress`, and returns the ctx dispatch
    # would hand the sink for it.
    defp ctx(egress \\ %{}, extra \\ %{}) do
      Ankusa.put_config(Ankusa.Config.new(instance: @instance, egress: egress))
      on_exit(fn -> :persistent_term.erase({Ankusa, :config, @instance}) end)
      Map.merge(%{instance: @instance, attempt: 1}, extra)
    end

    # DNS from a map of name => addresses; an IP literal answers as itself, as
    # `:inet.getaddrs/2` does, and any other name is NXDOMAIN.
    defp zone(names) do
      fn name, family ->
        addresses =
          case :inet.parse_address(name) do
            {:ok, literal} -> [literal]
            {:error, _} -> Map.get(names, List.to_string(name), [])
          end

        case Enum.filter(addresses, &(tuple_size(&1) == 4 == (family == :inet))) do
          [] -> {:error, :nxdomain}
          matching -> {:ok, matching}
        end
      end
    end

    defp send_to(url, ctx, extra \\ []) do
      Sink.Http.deliver(
        envelope(),
        ctx,
        [url: url, req_options: [plug: {Req.Test, __MODULE__}]] ++ extra
      )
    end

    test "denies loopback", %{capture: capture} do
      ctx = ctx(%{}, %{egress_resolver: zone(%{"localhost" => [{127, 0, 0, 1}]})})

      for url <- [
            "https://127.0.0.1:4003/v1/sources",
            "https://127.0.0.1/",
            "https://[::1]/",
            "https://localhost/",
            "https://0.0.0.0/",
            "https://[::ffff:127.0.0.1]/"
          ] do
        assert {:error, {:permanent, {:egress_denied, {:address, _host, _ip}}}} =
                 send_to(url, ctx),
               url
      end

      assert Agent.get(capture, & &1) == [], "nothing was sent"
    end

    test "denies metadata", %{capture: capture} do
      ctx = ctx()

      assert {:error, {:permanent, {:egress_denied, {:address, _, "169.254.169.254"}}}} =
               send_to("https://169.254.169.254/latest/meta-data/iam/security-credentials/", ctx)

      assert {:error, {:permanent, {:egress_denied, {:address, _, "fd00:ec2::254"}}}} =
               send_to("https://[fd00:ec2::254]/latest/meta-data/", ctx)

      assert {:error, {:permanent, {:egress_denied, {:host, "metadata.google.internal"}}}} =
               send_to("https://metadata.google.internal/computeMetadata/v1/", ctx)

      assert Agent.get(capture, & &1) == []
    end

    test "denies RFC1918", %{capture: capture} do
      ctx = ctx()

      for url <- ["https://10.1.2.3/", "https://172.16.5.5/", "https://192.168.0.10/"] do
        assert {:error, {:permanent, {:egress_denied, {:address, _, _}}}} = send_to(url, ctx), url
      end

      assert Agent.get(capture, & &1) == []
    end

    test "denies rebinding", %{capture: capture} do
      # One public record, one private: the stub a rebinding attacker serves.
      mixed = zone(%{"rebind.example" => [{203, 0, 113, 10}, {127, 0, 0, 1}]})

      assert {:error, {:permanent, {:egress_denied, {:address, "rebind.example", "127.0.0.1"}}}} =
               send_to("https://rebind.example/", ctx(%{}, %{egress_resolver: mixed}))

      # The answer moves between attempts: every attempt looks again.
      {:ok, answer} = Agent.start_link(fn -> {203, 0, 113, 10} end)

      flipping = fn _name, family ->
        if family == :inet, do: {:ok, [Agent.get(answer, & &1)]}, else: {:error, :nxdomain}
      end

      ctx = ctx(%{}, %{egress_resolver: flipping})
      assert :ok = send_to("https://flip.example/", ctx)

      Agent.update(answer, fn _ -> {169, 254, 169, 254} end)

      assert {:error,
              {:permanent, {:egress_denied, {:address, "flip.example", "169.254.169.254"}}}} =
               send_to("https://flip.example/", ctx)

      assert [_one_delivery] = Agent.get(capture, & &1)
    end

    test "a name that does not resolve is retried, not dead-lettered" do
      ctx = ctx(%{}, %{egress_resolver: zone(%{})})

      assert {:error, {:egress_denied, {:resolve, "gone.example", :nxdomain}}} =
               send_to("https://gone.example/", ctx)
    end

    test "plain http is denied by default, allowed by allow_insecure_http", %{capture: capture} do
      assert {:error, {:permanent, {:egress_denied, {:scheme, "http"}}}} =
               send_to("http://203.0.113.10/hooks", ctx())

      assert :ok = send_to("http://203.0.113.10/hooks", ctx(%{allow_insecure_http: true}))
      assert [{"POST", "/hooks", _, _}] = Agent.get(capture, & &1)
    end

    test "allows allow_cidrs and allow_hosts", %{capture: capture} do
      internal = zone(%{"hooks.internal" => [{10, 1, 2, 3}], "worker" => [{172, 18, 0, 3}]})

      assert :ok =
               send_to(
                 "https://hooks.internal/deliveries",
                 ctx(%{allow_cidrs: ["10.0.0.0/8"]}, %{egress_resolver: internal})
               )

      assert :ok =
               send_to(
                 "https://worker:8080/deliveries",
                 ctx(%{allow_hosts: ["worker"]}, %{egress_resolver: internal})
               )

      # the allowance is as wide as it was written
      assert {:error, {:permanent, {:egress_denied, {:address, "worker", "172.18.0.3"}}}} =
               send_to(
                 "https://worker:8080/deliveries",
                 ctx(%{allow_cidrs: ["10.0.0.0/8"]}, %{egress_resolver: internal})
               )

      assert length(Agent.get(capture, & &1)) == 2
    end

    test "an instance that never started gets the default policy, not no policy" do
      ctx = %{instance: :sink_http_never_started, attempt: 1}

      assert {:error, {:permanent, {:egress_denied, {:address, _, _}}}} =
               send_to("https://127.0.0.1/", ctx)
    end

    test "an http connection goes to the address that was checked, under the name's host header" do
      port = Ankusa.TestHelpers.free_port()

      start_supervised!(
        {Bandit, plug: {HostEcho, test: self()}, scheme: :http, ip: {127, 0, 0, 1}, port: port}
      )

      # `.invalid` never resolves: only the pinned address can reach the listener.
      ctx =
        ctx(%{allow_insecure_http: true, allow_cidrs: ["127.0.0.0/8"]}, %{
          egress_resolver: zone(%{"hooks.invalid" => [{127, 0, 0, 1}]})
        })

      assert :ok =
               Sink.Http.deliver(envelope(), ctx, url: "http://hooks.invalid:#{port}/deliveries")

      assert_received {:request, ["hooks.invalid:" <> _], "/deliveries"}
    end
  end

  describe "TLS verification" do
    @insecure [connect_options: [transport_opts: [verify: :verify_none]]]

    test "rejects verify_none" do
      assert_raise ArgumentError, ~r/verify_none/, fn ->
        Sink.Http.validate_opts!(url: "https://hooks.example/", req_options: @insecure)
      end

      # at delivery too, for a sink that never went through config validation
      assert {:error, {:raised, %ArgumentError{message: message}}} =
               Sink.safe_deliver(Sink.Http, envelope(), %{attempt: 1},
                 url: "https://203.0.113.10/",
                 req_options: @insecure
               )

      assert message =~ "verify_none"
    end

    test "allow_insecure_tls is the one way past it, per sink" do
      assert :ok =
               Sink.Http.validate_opts!(
                 url: "https://hooks.example/",
                 req_options: @insecure,
                 allow_insecure_tls: true
               )

      assert_raise ArgumentError, ~r/allow_insecure_tls must be true or false/, fn ->
        Sink.Http.validate_opts!(url: "https://hooks.example/", allow_insecure_tls: "yes")
      end
    end
  end
end
