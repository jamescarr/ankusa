defmodule Ankusa.VerifierTest do
  use ExUnit.Case, async: true

  alias Ankusa.Envelope
  alias Ankusa.Verifier.Hmac
  alias Ankusa.Verifier.Hmac.Scheme

  defp env(headers, body) do
    %Envelope{
      id: "e1",
      source_id: "src",
      received_at: 0,
      method: "POST",
      path: "/hook",
      headers: headers,
      body: body
    }
  end

  describe "Ankusa.Verifier.None" do
    test "always accepts" do
      assert Ankusa.Verifier.None.verify(env([], "anything"), []) == :ok
      assert Ankusa.Verifier.None.verify(env([{"x", "y"}], ""), secret: "ignored") == :ok
    end
  end

  describe "Ankusa.Verifier.Hmac (standard_webhooks)" do
    setup do
      key = :crypto.strong_rand_bytes(24)
      secret = "whsec_" <> Base.encode64(key)
      %{key: key, secret: secret}
    end

    defp swh_env(id, ts, sig_header, body) do
      env(
        [
          {"webhook-id", id},
          {"webhook-timestamp", Integer.to_string(ts)},
          {"webhook-signature", sig_header}
        ],
        body
      )
    end

    defp swh_sign(key, id, ts, body) do
      signed = "#{id}.#{ts}.#{body}"
      "v1," <> Base.encode64(:crypto.mac(:hmac, :sha256, key, signed))
    end

    test "accepts a valid signature", %{key: key, secret: secret} do
      ts = System.system_time(:second)
      body = ~s({"hello":"world"})
      sig = swh_sign(key, "msg_1", ts, body)

      assert Hmac.verify(
               swh_env("msg_1", ts, sig, body),
               scheme: :standard_webhooks,
               secret: secret
             ) == :ok
    end

    test "accepts when any token in the list matches", %{key: key, secret: secret} do
      ts = System.system_time(:second)
      body = "payload"
      good = swh_sign(key, "msg_1", ts, body)
      header = "v1,AAAAbogus== #{good}"

      assert Hmac.verify(
               swh_env("msg_1", ts, header, body),
               scheme: :standard_webhooks,
               secret: secret
             ) == :ok
    end

    test "rejects a tampered body", %{key: key, secret: secret} do
      ts = System.system_time(:second)
      sig = swh_sign(key, "msg_1", ts, "original")

      assert {:error, :no_match} =
               Hmac.verify(
                 swh_env("msg_1", ts, sig, "tampered"),
                 scheme: :standard_webhooks,
                 secret: secret
               )
    end

    test "rejects a stale timestamp", %{key: key, secret: secret} do
      ts = System.system_time(:second) - 10_000
      body = "payload"
      sig = swh_sign(key, "msg_1", ts, body)

      assert {:error, :timestamp_out_of_tolerance} =
               Hmac.verify(
                 swh_env("msg_1", ts, sig, body),
                 scheme: :standard_webhooks,
                 secret: secret
               )
    end

    test "judges the timestamp window at `:now` when given", %{key: key, secret: secret} do
      ts = System.system_time(:second) - 10_000
      body = "payload"
      env = swh_env("msg_1", ts, swh_sign(key, "msg_1", ts, body), body)
      opts = [scheme: :standard_webhooks, secret: secret]

      assert Hmac.verify(env, [{:now, ts + 60} | opts]) == :ok

      assert {:error, :timestamp_out_of_tolerance} =
               Hmac.verify(env, [{:now, ts + 10_000} | opts])
    end

    test "a list of secrets accepts a hook signed with any of them", %{key: key, secret: secret} do
      old_key = :crypto.strong_rand_bytes(24)
      old_secret = "whsec_" <> Base.encode64(old_key)
      ts = System.system_time(:second)
      opts = [scheme: :standard_webhooks, secret: [secret, old_secret]]

      for k <- [key, old_key] do
        env = swh_env("msg_1", ts, swh_sign(k, "msg_1", ts, "payload"), "payload")
        assert Hmac.verify(env, opts) == :ok
      end

      stranger = swh_sign(:crypto.strong_rand_bytes(24), "msg_1", ts, "payload")

      assert {:error, :no_match} =
               Hmac.verify(swh_env("msg_1", ts, stranger, "payload"), opts)
    end

    test "an empty key fails closed, even for a hook signed with it", %{key: key, secret: secret} do
      ts = System.system_time(:second)
      # Signed with the empty key: what anyone can forge.
      forged = swh_env("msg_1", ts, swh_sign("", "msg_1", ts, "payload"), "payload")

      for bad <- [
            nil,
            "",
            "whsec_",
            [],
            [secret, ""],
            [secret, "whsec_not base64!"],
            [secret, 42]
          ] do
        assert {:error, :bad_secret} =
                 Hmac.verify(forged, scheme: :standard_webhooks, secret: bad)
      end

      assert {:error, :bad_secret} = Hmac.verify(forged, scheme: :standard_webhooks)

      good = swh_env("msg_1", ts, swh_sign(key, "msg_1", ts, "payload"), "payload")
      assert Hmac.verify(good, scheme: :standard_webhooks, secret: [secret]) == :ok
    end

    test "rejects missing headers", %{secret: secret} do
      assert {:error, :missing_signature} =
               Hmac.verify(env([], "body"), scheme: :standard_webhooks, secret: secret)
    end
  end

  describe "Ankusa.Verifier.Hmac (stripe)" do
    setup do
      %{secret: "whsec_" <> Base.encode16(:crypto.strong_rand_bytes(16))}
    end

    defp stripe_sign(secret, t, body) do
      signed = "#{t}.#{body}"
      "t=#{t},v1=" <> Base.encode16(:crypto.mac(:hmac, :sha256, secret, signed), case: :lower)
    end

    test "accepts a valid signature", %{secret: secret} do
      t = System.system_time(:second)
      body = ~s({"id":"evt_1"})
      header = stripe_sign(secret, t, body)

      assert Hmac.verify(
               env([{"Stripe-Signature", header}], body),
               scheme: :stripe,
               secret: secret
             ) == :ok
    end

    test "rejects a tampered body", %{secret: secret} do
      t = System.system_time(:second)
      header = stripe_sign(secret, t, "original")

      assert {:error, :no_match} =
               Hmac.verify(
                 env([{"Stripe-Signature", header}], "tampered"),
                 scheme: :stripe,
                 secret: secret
               )
    end

    test "rejects a stale timestamp", %{secret: secret} do
      t = System.system_time(:second) - 10_000
      body = "payload"
      header = stripe_sign(secret, t, body)

      assert {:error, :timestamp_out_of_tolerance} =
               Hmac.verify(
                 env([{"Stripe-Signature", header}], body),
                 scheme: :stripe,
                 secret: secret
               )
    end

    test "rejects a missing header", %{secret: secret} do
      assert {:error, :missing_signature} =
               Hmac.verify(env([], "body"), scheme: :stripe, secret: secret)
    end

    test "an empty secret fails closed, even for a hook signed with the empty key" do
      t = System.system_time(:second)
      env = env([{"Stripe-Signature", stripe_sign("", t, "payload")}], "payload")

      assert {:error, :bad_secret} = Hmac.verify(env, scheme: :stripe, secret: "")
    end
  end

  describe "Ankusa.Verifier.Hmac (github)" do
    setup do
      %{secret: "It's a Secret to Everybody"}
    end

    defp gh_sign(secret, body) do
      "sha256=" <> Base.encode16(:crypto.mac(:hmac, :sha256, secret, body), case: :lower)
    end

    test "accepts a valid signature", %{secret: secret} do
      body = ~s({"zen":"Design for failure."})
      header = gh_sign(secret, body)

      assert Hmac.verify(
               env([{"X-Hub-Signature-256", header}], body),
               scheme: :github,
               secret: secret
             ) == :ok
    end

    test "rejects a tampered body", %{secret: secret} do
      header = gh_sign(secret, "original")

      assert {:error, :no_match} =
               Hmac.verify(
                 env([{"X-Hub-Signature-256", header}], "tampered"),
                 scheme: :github,
                 secret: secret
               )
    end

    test "rejects a missing header", %{secret: secret} do
      assert {:error, :missing_signature} =
               Hmac.verify(env([], "body"), scheme: :github, secret: secret)
    end
  end

  describe "Ankusa.Verifier.Hmac (shopify)" do
    setup do
      %{secret: "shpss_" <> Base.encode16(:crypto.strong_rand_bytes(16))}
    end

    defp shopify_sign(secret, body) do
      Base.encode64(:crypto.mac(:hmac, :sha256, secret, body))
    end

    test "accepts a valid signature", %{secret: secret} do
      body = ~s({"id":820982911946154508})
      header = shopify_sign(secret, body)

      assert Hmac.verify(
               env([{"X-Shopify-Hmac-SHA256", header}], body),
               scheme: :shopify,
               secret: secret
             ) == :ok
    end

    test "rejects a tampered body", %{secret: secret} do
      header = shopify_sign(secret, "original")

      assert {:error, :no_match} =
               Hmac.verify(
                 env([{"X-Shopify-Hmac-SHA256", header}], "tampered"),
                 scheme: :shopify,
                 secret: secret
               )
    end

    test "rejects a missing header", %{secret: secret} do
      assert {:error, :missing_signature} =
               Hmac.verify(env([], "body"), scheme: :shopify, secret: secret)
    end
  end

  describe "Ankusa.Verifier.Hmac (slack)" do
    setup do
      %{secret: "hunter2"}
    end

    defp slack_sign(secret, ts, body) do
      "v0=" <>
        Base.encode16(:crypto.mac(:hmac, :sha256, secret, "v0:#{ts}:#{body}"), case: :lower)
    end

    test "accepts a valid signature", %{secret: secret} do
      ts = System.system_time(:second)
      body = ~s({"type":"url_verification"})
      sig = slack_sign(secret, ts, body)

      headers = [
        {"X-Slack-Signature", sig},
        {"X-Slack-Request-Timestamp", Integer.to_string(ts)}
      ]

      assert Hmac.verify(env(headers, body), scheme: :slack, secret: secret) == :ok
    end

    test "rejects a tampered body", %{secret: secret} do
      ts = System.system_time(:second)
      sig = slack_sign(secret, ts, "original")

      headers = [
        {"X-Slack-Signature", sig},
        {"X-Slack-Request-Timestamp", Integer.to_string(ts)}
      ]

      assert {:error, :no_match} =
               Hmac.verify(env(headers, "tampered"), scheme: :slack, secret: secret)
    end

    test "rejects a stale timestamp", %{secret: secret} do
      ts = System.system_time(:second) - 10_000
      body = "payload"
      sig = slack_sign(secret, ts, body)

      headers = [
        {"X-Slack-Signature", sig},
        {"X-Slack-Request-Timestamp", Integer.to_string(ts)}
      ]

      assert {:error, :timestamp_out_of_tolerance} =
               Hmac.verify(env(headers, body), scheme: :slack, secret: secret)
    end

    test "rejects a missing header", %{secret: secret} do
      assert {:error, :missing_signature} =
               Hmac.verify(env([], "body"), scheme: :slack, secret: secret)
    end
  end

  describe "Ankusa.Verifier.Hmac (custom scheme)" do
    setup do
      %{
        secret: "custom-secret",
        scheme: %Scheme{
          signature_header: "X-Custom-Sig",
          sig_prefix: "sha256=",
          signed: "{body}",
          hash: :sha256,
          encoding: :hex
        }
      }
    end

    defp custom_sign(secret, body) do
      "sha256=" <> Base.encode16(:crypto.mac(:hmac, :sha256, secret, body), case: :lower)
    end

    test "accepts a signature computed against its own scheme", %{secret: secret, scheme: scheme} do
      body = "hello"
      header = custom_sign(secret, body)

      assert Hmac.verify(env([{"X-Custom-Sig", header}], body), scheme: scheme, secret: secret) ==
               :ok
    end

    test "rejects a tampered body", %{secret: secret, scheme: scheme} do
      header = custom_sign(secret, "original")

      assert {:error, :no_match} =
               Hmac.verify(env([{"X-Custom-Sig", header}], "tampered"),
                 scheme: scheme,
                 secret: secret
               )
    end
  end

  describe "scheme_name/1 and unknown schemes" do
    test "names a preset by its atom" do
      assert Hmac.scheme_name(scheme: :stripe) == "stripe"
      assert Hmac.scheme_name(scheme: :standard_webhooks) == "standard_webhooks"
    end

    test "names an inline scheme \"custom\"" do
      scheme = %Scheme{signature_header: "X-Custom-Sig"}
      assert Hmac.scheme_name(scheme: scheme) == "custom"
    end

    test "an unknown or missing scheme is :bad_scheme" do
      assert Hmac.verify(env([], "body"), secret: "x") == {:error, :bad_scheme}
      assert Hmac.verify(env([], "body"), scheme: :twilio, secret: "x") == {:error, :bad_scheme}
    end
  end

  describe "warn_unverified_shared/1" do
    import ExUnit.CaptureLog

    defp warning(resolver, source_opts, roles \\ [:edge]) do
      config =
        Ankusa.Config.new(
          instance: :"verifier_warn_#{System.unique_integer([:positive])}",
          data_dir: System.tmp_dir!(),
          roles: roles,
          route_resolver: resolver,
          source_store: {Ankusa.SourceStore.Static, sources: %{"stripe" => source_opts}}
        )

      capture_log(fn -> assert Ankusa.Verifier.warn_unverified_shared(config) == :ok end)
    end

    test "names a shared source with no verifier when the URL carries the tenant" do
      assert warning({Ankusa.RouteResolver.TenantPath, []}, []) =~
               "source stripe is shared"
    end

    test "says nothing when the tenant does not come from the URL, or the source is bound or verified" do
      assert warning({Ankusa.RouteResolver.Path, []}, []) == ""
      assert warning({Ankusa.RouteResolver.TenantPath, []}, tenant_id: "acme") == ""

      verified = [verifier: {Hmac, scheme: :stripe, secret: "whsec_x"}]
      assert warning({Ankusa.RouteResolver.TenantPath, []}, verified) == ""
      assert warning({Ankusa.RouteResolver.TenantPath, []}, [], [:dispatch]) == ""
    end
  end
end
