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
  # without a DNS lookup, and `Req.Test` answers before any socket opens. Sinks
  # are signed by default (SEC-10): these tests are about everything else, so
  # they opt out unless they bring a `:secret`.
  defp opts(path, extra \\ []) do
    [url: "https://203.0.113.10" <> path, req_options: [plug: {Req.Test, __MODULE__}]] ++
      unsigned(extra) ++ extra
  end

  defp unsigned(extra),
    do: if(Keyword.has_key?(extra, :secret), do: [], else: [allow_unsigned: true])

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
        allow_unsigned: true,
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

    test "allow_unsigned: true sends no signature headers", %{capture: capture} do
      assert :ok = Sink.Http.deliver(envelope(), %{attempt: 1}, opts("/hooks"))

      assert [{"POST", "/hooks", headers, _body}] = Agent.get(capture, & &1)
      refute Enum.any?(headers, fn {name, _} -> String.starts_with?(name, "webhook-") end)
    end

    test "allow_unsigned beside a secret still signs", %{capture: capture} do
      assert :ok =
               Sink.Http.deliver(
                 envelope(),
                 %{attempt: 1},
                 opts("/hooks", secret: @secret, allow_unsigned: true)
               )

      assert [{"POST", "/hooks", headers, _body}] = Agent.get(capture, & &1)
      assert Enum.any?(headers, fn {name, _} -> name == "webhook-signature" end)
    end

    test "a sink with no secret that skipped validation is dead-lettered, and nothing is sent", %{
      capture: capture
    } do
      unsigned = [url: "https://203.0.113.10/hooks", req_options: [plug: {Req.Test, __MODULE__}]]

      assert Sink.Http.deliver(envelope(), %{attempt: 1}, unsigned) ==
               {:error, {:permanent, :unsigned_sink}}

      assert Agent.get(capture, & &1) == []
    end

    test "requires secret: validate_opts! refuses a sink with neither a secret nor allow_unsigned" do
      assert_raise ArgumentError, ~r/requires secret/, fn ->
        Sink.Http.validate_opts!(url: "https://hooks.example/")
      end

      assert_raise ArgumentError, ~r/requires secret/, fn ->
        Sink.Http.validate_opts!(url: "https://hooks.example/", allow_unsigned: false)
      end

      for empty <- ["", []] do
        assert_raise ArgumentError, ~r/requires secret/, fn ->
          Sink.Http.validate_opts!(url: "https://hooks.example/", secret: empty)
        end
      end

      assert :ok = Sink.Http.validate_opts!(url: "https://hooks.example/", secret: @secret)
      assert :ok = Sink.Http.validate_opts!(url: "https://hooks.example/", allow_unsigned: true)

      assert :ok =
               Sink.Http.validate_opts!(
                 url: "https://hooks.example/",
                 secret: [@secret],
                 allow_unsigned: true
               )
    end

    test "allow_unsigned must be a boolean" do
      assert_raise ArgumentError, ~r/allow_unsigned must be true or false/, fn ->
        Sink.Http.validate_opts!(url: "https://hooks.example/", allow_unsigned: "yes")
      end
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
        [url: url, allow_unsigned: true, req_options: [plug: {Req.Test, __MODULE__}]] ++ extra
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
               Sink.Http.deliver(envelope(), ctx,
                 url: "http://hooks.invalid:#{port}/deliveries",
                 allow_unsigned: true
               )

      assert_received {:request, ["hooks.invalid:" <> _], "/deliveries"}
    end
  end

  describe "TLS verification" do
    @insecure [connect_options: [transport_opts: [verify: :verify_none]]]

    test "rejects verify_none" do
      assert_raise ArgumentError, ~r/verify_none/, fn ->
        Sink.Http.validate_opts!(
          url: "https://hooks.example/",
          allow_unsigned: true,
          req_options: @insecure
        )
      end

      # at delivery too, for a sink that never went through config validation
      assert {:error, {:raised, %ArgumentError{message: message}}} =
               Sink.safe_deliver(Sink.Http, envelope(), %{attempt: 1},
                 url: "https://203.0.113.10/",
                 allow_unsigned: true,
                 req_options: @insecure
               )

      assert message =~ "verify_none"
    end

    test "allow_insecure_tls is the one way past it, per sink" do
      assert :ok =
               Sink.Http.validate_opts!(
                 url: "https://hooks.example/",
                 allow_unsigned: true,
                 req_options: @insecure,
                 allow_insecure_tls: true
               )

      assert_raise ArgumentError, ~r/allow_insecure_tls must be true or false/, fn ->
        Sink.Http.validate_opts!(
          url: "https://hooks.example/",
          allow_unsigned: true,
          allow_insecure_tls: "yes"
        )
      end
    end
  end

  describe "computed destinations" do
    alias Ankusa.Sink.Http.Pool

    @instance :sink_http_computed_test
    @key :crypto.strong_rand_bytes(32)
    @acme_secret "whsec_" <> Base.encode64(@key)

    defmodule Tenants do
      @moduledoc false
      def endpoint(env, base), do: base <> "/t/" <> env.tenant_id
    end

    defmodule Echo do
      @moduledoc false
      @behaviour Plug
      import Plug.Conn

      @impl true
      def init(opts), do: opts

      @impl true
      def call(conn, opts) do
        send(opts[:test], {:request, conn.request_path})
        send_resp(conn, 200, "ok")
      end
    end

    defp computed(extra),
      do: [req_options: [plug: {Req.Test, __MODULE__}], allow_unsigned: true] ++ extra

    defp paths(capture),
      do:
        capture
        |> Agent.get(& &1)
        |> Enum.map(fn {_method, path, _h, _b} -> path end)
        |> Enum.sort()

    test "computed url per tenant", %{capture: capture} do
      per_tenant = fn env -> "https://203.0.113.10/t/" <> env.tenant_id end

      for url <- [per_tenant, {Tenants, :endpoint, ["https://203.0.113.10"]}],
          tenant <- ["acme", "globex"] do
        assert :ok =
                 Sink.Http.deliver(
                   envelope(%{tenant_id: tenant}),
                   %{attempt: 1},
                   computed(url: url)
                 )
      end

      assert paths(capture) == ["/t/acme", "/t/acme", "/t/globex", "/t/globex"]
    end

    test "computed url denied", %{capture: capture} do
      # One sink, two tenants: the one whose receiver sits on the private network
      # fails closed, on its own attempt, and the other is delivered.
      by_tenant = fn
        %{tenant_id: "acme"} -> "https://203.0.113.10/t/acme"
        %{tenant_id: "globex"} -> "https://10.1.2.3/t/globex"
        %{tenant_id: "imds"} -> "https://169.254.169.254/latest/meta-data/"
        %{tenant_id: "loop"} -> "https://127.0.0.1/"
      end

      deliver = fn tenant ->
        Sink.Http.deliver(envelope(%{tenant_id: tenant}), %{attempt: 1}, computed(url: by_tenant))
      end

      assert :ok = deliver.("acme")

      for tenant <- ["globex", "imds", "loop"] do
        assert {:error, {:permanent, {:egress_denied, {:address, _host, _address}}}} =
                 deliver.(tenant)
      end

      assert paths(capture) == ["/t/acme"]
    end

    test "a computed url is checked against the resolved name on every attempt" do
      {:ok, answers} = Agent.start_link(fn -> [{203, 0, 113, 10}, {10, 0, 0, 5}] end)

      resolver = fn name, family ->
        case {List.to_string(name), family} do
          {"hooks.example", :inet} ->
            {:ok, [Agent.get_and_update(answers, fn [head | rest] -> {head, rest ++ [head]} end)]}

          _other ->
            {:error, :nxdomain}
        end
      end

      Ankusa.put_config(Ankusa.Config.new(instance: @instance))
      on_exit(fn -> :persistent_term.erase({Ankusa, :config, @instance}) end)
      ctx = %{instance: @instance, attempt: 1, egress_resolver: resolver}
      opts = computed(url: fn _env -> "https://hooks.example/t" end)

      assert :ok = Sink.Http.deliver(envelope(), ctx, opts)

      assert {:error, {:permanent, {:egress_denied, {:address, "hooks.example", "10.0.0.5"}}}} =
               Sink.Http.deliver(envelope(), ctx, opts)
    end

    test "computed headers per tenant", %{capture: capture} do
      token = fn env -> [{"x-tenant-token", "tok-" <> env.tenant_id}, {:"x-plan", :gold}] end

      for tenant <- ["acme", "globex"] do
        assert :ok =
                 Sink.Http.deliver(
                   envelope(%{tenant_id: tenant}),
                   %{attempt: 1},
                   computed(url: "https://203.0.113.10/h", headers: token)
                 )
      end

      sent =
        capture
        |> Agent.get(& &1)
        |> Enum.map(fn {_m, _p, headers, _b} -> Map.new(headers) end)

      assert Enum.sort(Enum.map(sent, & &1["x-tenant-token"])) == ["tok-acme", "tok-globex"]
      assert Enum.all?(sent, &(&1["x-plan"] == "gold"))
    end

    test "computed secret per tenant signs with that tenant's secret", %{capture: capture} do
      secret_for = fn
        %{tenant_id: "acme"} -> @acme_secret
        %{tenant_id: "ghost"} -> nil
      end

      opts = [
        url: "https://203.0.113.10/s",
        secret: secret_for,
        req_options: [plug: {Req.Test, __MODULE__}]
      ]

      env = envelope(%{tenant_id: "acme"})
      assert :ok = Sink.Http.deliver(env, %{attempt: 1}, opts)
      assert [{"POST", "/s", headers, body}] = Agent.get(capture, & &1)
      received = %{env | headers: headers, body: body}

      assert :ok =
               Ankusa.Verifier.Hmac.verify(received,
                 scheme: :standard_webhooks,
                 secret: @acme_secret
               )

      # A tenant with no secret is not sent unsigned: the rule holds per destination.
      assert {:error, {:permanent, :unsigned_sink}} =
               Sink.Http.deliver(envelope(%{tenant_id: "ghost"}), %{attempt: 1}, opts)

      assert length(Agent.get(capture, & &1)) == 1
    end

    test "what a computed option returns is checked, and never echoed" do
      deliver = fn key, value ->
        opts = computed(url: "https://203.0.113.10/c") |> Keyword.put(key, fn _env -> value end)
        Sink.Http.deliver(envelope(), %{attempt: 1}, opts)
      end

      assert deliver.(:url, 12_345) == {:error, {:permanent, {:computed, :url, :invalid}}}
      assert deliver.(:url, "") == {:error, {:permanent, {:computed, :url, :invalid}}}

      assert deliver.(:headers, [:oops]) ==
               {:error, {:permanent, {:computed, :headers, :invalid}}}

      assert deliver.(:secret, 5) == {:error, {:permanent, {:computed, :secret, :invalid}}}
      assert deliver.(:secret, [:a]) == {:error, {:permanent, {:computed, :secret, :invalid}}}

      # The function's own refusal passes through; a raise is a failed attempt.
      assert deliver.(:url, {:error, {:permanent, :no_endpoint}}) ==
               {:error, {:permanent, :no_endpoint}}

      assert {:error, {:raised, %RuntimeError{message: "no tenant"}}} =
               Sink.safe_deliver(
                 Sink.Http,
                 envelope(),
                 %{attempt: 1},
                 computed(url: fn _env -> raise "no tenant" end)
               )
    end

    test "validate_opts! accepts computed options and refuses ones that cannot work" do
      assert :ok =
               Sink.Http.validate_opts!(
                 url: fn _env -> "https://a.example/" end,
                 secret: @acme_secret
               )

      assert :ok =
               Sink.Http.validate_opts!(
                 url: {Tenants, :endpoint, ["https://a.example"]},
                 allow_unsigned: true
               )

      assert :ok =
               Sink.Http.validate_opts!(
                 url: "https://a.example/",
                 secret: fn _env -> @acme_secret end
               )

      assert :ok =
               Sink.Http.validate_opts!(
                 url: "https://a.example/",
                 allow_unsigned: true,
                 headers: fn _env -> [] end
               )

      assert_raise ArgumentError, ~r/arity 1/, fn ->
        Sink.Http.validate_opts!(url: fn _a, _b -> "x" end, allow_unsigned: true)
      end

      assert_raise ArgumentError, ~r/Tenants.nope\/2 is not defined/, fn ->
        Sink.Http.validate_opts!(
          url: {Tenants, :nope, ["https://a.example"]},
          allow_unsigned: true
        )
      end

      assert_raise ArgumentError, ~r/requires secret/, fn ->
        Sink.Http.validate_opts!(url: fn _env -> "https://a.example/" end)
      end

      assert_raise ArgumentError, ~r/cannot combine a computed :url/, fn ->
        Sink.Http.validate_opts!(
          url: fn _env -> "https://a.example/" end,
          allow_unsigned: true,
          allow_insecure_tls: true
        )
      end

      # and a sink that skipped validation is refused at delivery, nothing sent
      assert {:error, {:permanent, {:computed, :url, :tls_override}}} =
               Sink.Http.deliver(
                 envelope(),
                 %{attempt: 1},
                 computed(url: fn _env -> "https://203.0.113.10/" end, allow_insecure_tls: true)
               )
    end

    test "the description reports what is computed" do
      subject = %{source_id: "stripe", tenant_id: nil}

      assert %Ankusa.Sink.Description{
               protocol: "http",
               host: nil,
               payload: :body,
               computed: [:url, :secret]
             } =
               Sink.describe(Sink.Http, subject,
                 url: fn _env -> "https://a.example/" end,
                 secret: {Tenants, :endpoint, ["x"]}
               )

      assert %Ankusa.Sink.Description{
               protocol: "https",
               host: "hooks.example",
               payload: :body,
               computed: []
             } =
               Sink.describe(Sink.Http, subject, url: "https://user:pw@hooks.example/path?k=v")

      assert %Ankusa.Sink.Description{host: "hooks.example:8443"} =
               Sink.describe(Sink.Http, subject, url: "https://hooks.example:8443/")
    end

    # A real https listener whose certificate chains to a private CA, an instance
    # that lets `localhost` through the egress policy, and its computed-URL pool
    # trusting that CA and reaping idle origins after `idle_ms`.
    defp pool_fixture(idle_ms) do
      %{client_config: client, server_config: server} =
        :public_key.pkix_test_data(%{
          server_chain: %{
            root: [digest: :sha256, key: {:namedCurve, :secp256r1}],
            peer: [
              digest: :sha256,
              key: {:namedCurve, :secp256r1},
              extensions: [{:Extension, {2, 5, 29, 17}, false, [{:dNSName, ~c"localhost"}]}]
            ]
          },
          client_chain: %{root: [], peer: []}
        })

      dir = Path.join(System.tmp_dir!(), "ankusa-pool-#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      {key_type, key_der} = server[:key]
      cert = Path.join(dir, "cert.pem")
      key = Path.join(dir, "key.pem")
      ca = Path.join(dir, "ca.pem")
      File.write!(cert, :public_key.pem_encode([{:Certificate, server[:cert], :not_encrypted}]))
      File.write!(key, :public_key.pem_encode([{key_type, key_der, :not_encrypted}]))

      File.write!(
        ca,
        :public_key.pem_encode(
          for der <- client[:cacerts], do: {:Certificate, der, :not_encrypted}
        )
      )

      port = Ankusa.TestHelpers.free_port()

      start_supervised!(
        {Bandit,
         plug: {Echo, test: self()},
         scheme: :https,
         ip: {127, 0, 0, 1},
         port: port,
         certfile: cert,
         keyfile: key}
      )

      Ankusa.put_config(
        Ankusa.Config.new(instance: @instance, egress: %{allow_hosts: ["localhost"]})
      )

      on_exit(fn -> :persistent_term.erase({Ankusa, :config, @instance}) end)

      start_supervised!(
        {Pool, instance: @instance, pool_max_idle_ms: idle_ms, transport_opts: [cacertfile: ca]}
      )

      %{port: port, ca: ca, ctx: %{instance: @instance, attempt: 1}}
    end

    defp eventually(fun, tries \\ 60) do
      cond do
        fun.() -> :ok
        tries == 0 -> flunk("condition never held")
        true -> Process.sleep(100) && eventually(fun, tries - 1)
      end
    end

    test "computed urls share the instance's bounded pool; static ones do not" do
      %{port: port, ca: ca, ctx: ctx} = pool_fixture(500)
      url = "https://localhost:#{port}"

      assert :ok =
               Sink.Http.deliver(envelope(), ctx,
                 url: url <> "/static",
                 allow_unsigned: true,
                 cacertfile: ca
               )

      assert_received {:request, "/static"}
      assert Pool.origins(@instance) == 0

      per_tenant = fn env -> url <> "/t/" <> env.tenant_id end

      for tenant <- ["acme", "globex"] do
        assert :ok =
                 Sink.Http.deliver(
                   envelope(%{tenant_id: tenant}),
                   ctx,
                   url: per_tenant,
                   allow_unsigned: true
                 )

        expected = "/t/" <> tenant
        assert_received {:request, ^expected}
      end

      # Both tenants are one origin here (one host and port).
      assert Pool.origins(@instance) == 1
      assert {:ok, 1} = Finch.get_pool_count(Pool.name(@instance), url)

      # An origin nobody delivers to is reaped: the pool does not keep one per
      # tenant receiver for the life of the node.
      eventually(fn -> Pool.origins(@instance) == 0 end)
    end

    test "a new origin is refused at the ceiling; an open one never is" do
      %{port: port, ctx: ctx} = pool_fixture(60_000)
      url = "https://localhost:#{port}/x"

      assert :ok =
               Sink.Http.deliver(envelope(), ctx, url: fn _env -> url end, allow_unsigned: true)

      assert Pool.origins(@instance) == 1

      assert :ok = Pool.admit(@instance, url, 1)
      assert :ok = Pool.admit(@instance, "https://other.example/", 2)

      assert {:error, :pool_origins_exhausted} =
               Pool.admit(@instance, "https://other.example/", 1)
    end
  end
end
