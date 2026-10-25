defmodule Ankusa.VerifierTest do
  use ExUnit.Case, async: true

  alias Ankusa.Envelope
  alias Ankusa.Verifier.Asymmetric
  alias Ankusa.Verifier.ClientCert
  alias Ankusa.Verifier.Hmac
  alias Ankusa.Verifier.Hmac.Scheme
  alias Ankusa.Verifier.Token

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

    # Found by `property/hmac_property_test.exs`: `:sha1` is the scheme's name for
    # the hash (docs/configuration.md), but `:crypto.mac/4` calls it `:sha`, so
    # every hook for a `hash: sha1` source raised.
    test "a :sha1 scheme verifies a SHA-1 signature", %{secret: secret, scheme: scheme} do
      scheme = %{scheme | hash: :sha1, sig_prefix: nil}
      header = Base.encode16(:crypto.mac(:hmac, :sha, secret, "hello"), case: :lower)

      assert Hmac.verify(env([{"X-Custom-Sig", header}], "hello"), scheme: scheme, secret: secret) ==
               :ok

      assert {:error, :no_match} =
               Hmac.verify(env([{"X-Custom-Sig", header}], "hellp"),
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

    # Found by `property/hmac_property_test.exs`: `validate!/1`'s ArgumentError
    # escaped `verify/2`, so a stored source with a bad inline scheme raised in
    # the request path instead of failing its hooks like any other bad config.
    test "an inline scheme that fails validation is :bad_scheme, not a raise" do
      for scheme <- [
            %Scheme{signature_header: "X-Sig", signed: "{nope}"},
            %Scheme{signature_header: "", signed: "{body}"},
            %Scheme{signature_header: "!", timestamp: {:sig, "!"}}
          ] do
        request = env([{"X-Sig", "abc"}], "body")

        assert Hmac.verify(request, scheme: scheme, secret: "x") == {:error, :bad_scheme}
        assert Hmac.validate_opts(scheme: scheme, secret: "x") == {:error, :bad_scheme}
      end
    end
  end

  describe "tolerance range and weak hashes" do
    test "tolerance range: 0..3600 is valid, anything else is :bad_tolerance" do
      for ok <- [0, 1, 300, 3600] do
        assert Hmac.validate_opts(scheme: :stripe, secret: "k", tolerance: ok) == :ok
      end

      assert Hmac.validate_opts(scheme: :stripe, secret: "k") == :ok

      for bad <- [3601, 86_400, -1, 300.0, "300", :infinity] do
        assert Hmac.validate_opts(scheme: :stripe, secret: "k", tolerance: bad) ==
                 {:error, :bad_tolerance}
      end
    end

    test "verify/2 fails every hook closed on an out-of-range tolerance" do
      ts = Integer.to_string(System.system_time(:second))
      sig = Base.encode16(:crypto.mac(:hmac, :sha256, "k", "#{ts}.{}"), case: :lower)
      request = env([{"Stripe-Signature", "t=#{ts},v1=#{sig}"}], "{}")

      assert Hmac.verify(request, scheme: :stripe, secret: "k", tolerance: 3600) == :ok

      assert Hmac.verify(request, scheme: :stripe, secret: "k", tolerance: 3601) ==
               {:error, :bad_tolerance}
    end

    test "validate_config!/1 refuses a source with tolerance 3601 and names it" do
      config = hmac_config(scheme: :stripe, secret: "k", tolerance: 3601)

      assert_raise ArgumentError,
                   ~r/^source weak-probe: verifier tolerance must be an integer/,
                   fn ->
                     Ankusa.Verifier.validate_config!(config)
                   end

      assert :ok =
               Ankusa.Verifier.validate_config!(
                 hmac_config(scheme: :stripe, secret: "k", tolerance: 3600)
               )
    end

    test "no preset verifies with SHA-1, so only an inline scheme is weak" do
      for {name, scheme} <- Ankusa.Verifier.Schemes.all() do
        refute scheme.hash == :sha1, "preset #{name} uses SHA-1"
        refute Hmac.weak_hash?(scheme: name)
      end

      assert Hmac.weak_hash?(scheme: %Scheme{signature_header: "X-Sig", hash: :sha1})
      refute Hmac.weak_hash?(scheme: %Scheme{signature_header: "X-Sig", hash: :sha256})
      assert Ankusa.Verifier.weak_hash?(Hmac, scheme: %Scheme{signature_header: "X", hash: :sha1})
      refute Ankusa.Verifier.weak_hash?(Ankusa.Verifier.None, [])
    end

    test "a sha1 source boots with one warning naming it; sha256 boots silently" do
      weak = %Scheme{signature_header: "X-Sig", hash: :sha1}
      strong = %Scheme{signature_header: "X-Sig", hash: :sha256}

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert :ok = Ankusa.Verifier.validate_config!(hmac_config(scheme: weak, secret: "k"))
        end)

      assert log =~ "source weak-probe verifies with SHA-1"
      assert log =~ "ankusa_verifier_weak_hash_total"

      quiet =
        ExUnit.CaptureLog.capture_log(fn ->
          assert :ok = Ankusa.Verifier.validate_config!(hmac_config(scheme: strong, secret: "k"))
        end)

      refute quiet =~ "SHA-1"
    end

    defp hmac_config(opts) do
      Ankusa.Config.new(
        instance: :"verifier_weak_#{System.unique_integer([:positive])}",
        data_dir: System.tmp_dir!(),
        roles: [:edge],
        source_store:
          {Ankusa.SourceStore.Static, sources: %{"weak-probe" => [verifier: {Hmac, opts}]}}
      )
    end
  end

  describe "validate_shared!/1" do
    defp shared_config(resolver, source_opts, roles \\ [:edge]) do
      Ankusa.Config.new(
        instance: :"verifier_shared_#{System.unique_integer([:positive])}",
        data_dir: System.tmp_dir!(),
        roles: roles,
        route_resolver: resolver,
        source_store: {Ankusa.SourceStore.Static, sources: %{"e5-probe" => source_opts}}
      )
    end

    test "refuses a shared source with no verifier when the request names the tenant, whatever the node's roles" do
      for roles <- [[:edge], [:dispatch]] do
        config = shared_config({Ankusa.RouteResolver.TenantPath, []}, [], roles)

        assert_raise ArgumentError,
                     ~r/^source e5-probe is shared \(tenant_id "default"\) and has no verifier/,
                     fn -> Ankusa.Verifier.validate_shared!(config) end
      end
    end

    test "accepts a source that trusts the request's tenant, is bound, or is verified, and any source when the tenant does not come from the request" do
      tenant_path = {Ankusa.RouteResolver.TenantPath, []}
      verified = [verifier: {Hmac, scheme: :stripe, secret: "whsec_x"}]

      for {resolver, source_opts} <- [
            {tenant_path, [trust_url_tenant: true]},
            {tenant_path, [tenant_id: "acme"]},
            {tenant_path, verified},
            {{Ankusa.RouteResolver.Path, []}, []}
          ] do
        config = shared_config(resolver, source_opts)
        assert Ankusa.Verifier.validate_shared!(config) == :ok
      end
    end
  end

  # The Standard Webhooks specification's own reference message (its example
  # id, timestamp and minified payload), signed with the RFC 8032 test key 1:
  # the specification publishes no asymmetric key pair, and Ed25519 is
  # deterministic, so anyone can recompute this signature. The same message and
  # signature are `signature.ok.asymmetric_*` in `conformance/cases/signature.json`.
  @asym_id "msg_2KWPBgLlAfxdpx2AI54pPJ85f4W"
  @asym_ts "1674087231"
  @asym_now 1_674_087_231
  @asym_body ~s({"type":"contact.created","timestamp":"2022-11-03T20:26:10.344522Z","data":{"id":"1f81eb52-5198-4599-803e-771906343485"}})
  @asym_pub "whpk_11qYAYKxCrfVS/7TyWQHOg7hcvPapiMlrwIaaPcHURo="
  @asym_other_pub "whpk_PUAXw+hDiVqStwqnTRt+vJyYLM8uxJaMwM1V8Sr0Zgw="
  @asym_sig "v1a,pbpYBMlty2hExn4zt0UTGb6BaP2Vq5AfyzjB9GGV3x/wCJKd8UjOCf8Qhaji6TKY9C5eNMnlF0GG4udaO6B7Ag=="
  @asym_hmac_sig "v1,ARw42xaAApl/nxRo+iPGYwSaMQaOwMo2eyH5JBRA+bQ="

  defp asym_env(sig \\ @asym_sig, body \\ @asym_body, id \\ @asym_id) do
    env(
      [{"webhook-id", id}, {"webhook-timestamp", @asym_ts}, {"webhook-signature", sig}],
      body
    )
  end

  defp asym_opts(extra \\ []),
    do:
      Keyword.merge(
        [scheme: :standard_webhooks_asym, public_key: @asym_pub, now: @asym_now],
        extra
      )

  describe "Ankusa.Verifier.Asymmetric (standard_webhooks_asym)" do
    test "the reference vector verifies" do
      assert Asymmetric.verify(asym_env(), asym_opts()) == :ok
      assert Asymmetric.validate_opts(asym_opts()) == :ok
    end

    test "a v1 (HMAC) entry beside the v1a one is ignored, in either order" do
      assert Asymmetric.verify(asym_env(@asym_hmac_sig <> " " <> @asym_sig), asym_opts()) == :ok
      assert Asymmetric.verify(asym_env(@asym_sig <> " " <> @asym_hmac_sig), asym_opts()) == :ok

      assert Asymmetric.verify(asym_env(@asym_hmac_sig), asym_opts()) ==
               {:error, :malformed_signature}
    end

    test "a wrong key, a tampered body and a tampered id fail" do
      assert Asymmetric.verify(asym_env(), asym_opts(public_key: @asym_other_pub)) ==
               {:error, :no_match}

      tampered = String.replace(@asym_body, "contact.created", "contact.deleted")
      assert Asymmetric.verify(asym_env(@asym_sig, tampered), asym_opts()) == {:error, :no_match}

      assert Asymmetric.verify(asym_env(@asym_sig, @asym_body, "msg_other"), asym_opts()) ==
               {:error, :no_match}
    end

    test "a stale or future timestamp fails, and the tolerance widens or closes the window" do
      for now <- [@asym_now + 301, @asym_now - 301] do
        assert Asymmetric.verify(asym_env(), asym_opts(now: now)) ==
                 {:error, :timestamp_out_of_tolerance}
      end

      assert Asymmetric.verify(asym_env(), asym_opts(now: @asym_now + 300)) == :ok
      assert Asymmetric.verify(asym_env(), asym_opts(now: @asym_now + 301, tolerance: 301)) == :ok
      assert Asymmetric.verify(asym_env(), asym_opts(tolerance: 0)) == :ok

      assert Asymmetric.verify(asym_env(), asym_opts(now: @asym_now + 1, tolerance: 0)) ==
               {:error, :timestamp_out_of_tolerance}
    end

    test "a rotation list verifies a hook signed with any key of it, in any order" do
      for keys <- [[@asym_pub, @asym_other_pub], [@asym_other_pub, @asym_pub]] do
        assert Asymmetric.verify(asym_env(), asym_opts(public_key: keys)) == :ok
      end

      assert Asymmetric.verify(asym_env(), asym_opts(public_key: [@asym_other_pub])) ==
               {:error, :no_match}
    end

    test "missing headers fail closed" do
      assert Asymmetric.verify(env([], @asym_body), asym_opts()) == {:error, :missing_signature}

      no_id = env([{"webhook-timestamp", @asym_ts}, {"webhook-signature", @asym_sig}], @asym_body)
      assert Asymmetric.verify(no_id, asym_opts()) == {:error, :missing_signature}

      no_sig = env([{"webhook-id", @asym_id}, {"webhook-timestamp", @asym_ts}], @asym_body)
      assert Asymmetric.verify(no_sig, asym_opts()) == {:error, :missing_signature}
    end

    test "junk candidates never match; wrong-length junk cannot crowd the genuine signature out" do
      not_base64 = "v1a,!!!!"
      short = "v1a," <> Base.encode64("short")
      wrong_length = for _ <- 1..200, do: "v1a," <> Base.encode64(:crypto.strong_rand_bytes(63))

      assert Asymmetric.verify(asym_env(not_base64 <> " " <> short), asym_opts()) ==
               {:error, :no_match}

      assert Asymmetric.verify(
               asym_env(Enum.join(wrong_length, " ") <> " " <> @asym_sig),
               asym_opts()
             ) ==
               :ok
    end

    test "at most 8 signatures of the right length are tried, so a forged flood costs a bounded number of verifications" do
      right_length = for _ <- 1..8, do: "v1a," <> Base.encode64(:crypto.strong_rand_bytes(64))
      header = Enum.join(right_length, " ")

      assert Asymmetric.verify(asym_env(header <> " " <> @asym_sig), asym_opts()) ==
               {:error, :no_match}

      assert Asymmetric.verify(
               asym_env(Enum.join(Enum.take(right_length, 7), " ") <> " " <> @asym_sig),
               asym_opts()
             ) ==
               :ok
    end

    test "a key is the whpk_ form, bare base64, or a PEM SubjectPublicKeyInfo" do
      "whpk_" <> bare = @asym_pub
      raw = Base.decode64!(bare)

      spki = <<0x30, 0x2A, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x03, 0x21, 0x00>> <> raw
      pem = :public_key.pem_encode([{:SubjectPublicKeyInfo, spki, :not_encrypted}])

      for key <- [@asym_pub, bare, pem, " " <> @asym_pub <> "\n"] do
        assert Asymmetric.verify(asym_env(), asym_opts(public_key: key)) == :ok
      end
    end

    test "an unusable key fails every hook with :bad_key, and validate_opts says so" do
      rsa = :public_key.generate_key({:rsa, 2048, 65_537})
      {:RSAPrivateKey, _, n, e, _, _, _, _, _, _, _} = rsa

      rsa_pem =
        :public_key.pem_encode([
          :public_key.pem_entry_encode(:SubjectPublicKeyInfo, {:RSAPublicKey, n, e})
        ])

      for bad <- [
            nil,
            "",
            [],
            "whpk_",
            "whpk_!!!",
            "whpk_" <> Base.encode64("short"),
            "not a key",
            42,
            rsa_pem,
            [@asym_pub, ""],
            List.duplicate(@asym_pub, 9)
          ] do
        assert Asymmetric.validate_opts(asym_opts(public_key: bad)) == {:error, :bad_key}
        assert Asymmetric.verify(asym_env(), asym_opts(public_key: bad)) == {:error, :bad_key}
      end

      assert Asymmetric.validate_opts(asym_opts(public_key: List.duplicate(@asym_pub, 8))) == :ok
    end

    test "an unknown scheme and an out-of-range tolerance are refused" do
      for scheme <- [nil, :nope, :standard_webhooks, :stripe, "standard_webhooks_asym"] do
        assert Asymmetric.validate_opts(asym_opts(scheme: scheme)) == {:error, :bad_scheme}
      end

      for tolerance <- [3601, -1, "300"] do
        assert Asymmetric.validate_opts(asym_opts(tolerance: tolerance)) ==
                 {:error, :bad_tolerance}
      end

      assert {:ok, %Asymmetric.Scheme{version: "v1a,"}} =
               Ankusa.Verifier.Schemes.fetch_asymmetric(:standard_webhooks_asym)

      assert Ankusa.Verifier.Schemes.fetch_asymmetric(:stripe) == :error
      assert Ankusa.Verifier.Schemes.fetch(:standard_webhooks_asym) == :error

      assert_raise ArgumentError, ~r/scheme.algorithm/, fn ->
        Ankusa.Verifier.Schemes.validate!(%Asymmetric.Scheme{
          signature_header: "X",
          algorithm: :dsa
        })
      end
    end

    test "the scheme is named for telemetry" do
      assert Asymmetric.scheme_name(scheme: :standard_webhooks_asym) == "standard_webhooks_asym"
      assert Asymmetric.scheme_name(scheme: %Asymmetric.Scheme{signature_header: "X"}) == "custom"
    end

    test "the conformance cases carry exactly the bytes this suite verifies, HMAC half and v1a half" do
      %{"cases" => cases} =
        "../../../../conformance/cases/signature.json"
        |> Path.expand(__DIR__)
        |> File.read!()
        |> JSON.decode!()

      case_input = fn id -> Enum.find(cases, &(&1["id"] == id))["input"] end

      %{"headers" => headers, "body" => %{"text" => body}, "secrets" => [secret], "now" => now} =
        case_input.("signature.ok.asymmetric_alongside")

      assert body == @asym_body
      assert headers["webhook-id"] == @asym_id
      assert headers["webhook-timestamp"] == @asym_ts
      assert headers["webhook-signature"] == @asym_sig <> " " <> @asym_hmac_sig

      request = env(Map.to_list(headers), body)
      assert Asymmetric.verify(request, asym_opts(now: now)) == :ok

      hmac_opts = [scheme: :standard_webhooks, secret: secret, now: now]
      assert Hmac.verify(request, hmac_opts) == :ok

      # Only the v1a entry: the asymmetric verifier takes it, the HMAC one finds
      # no v1 entry (SDKs report that as `no_matching_signature`).
      %{"headers" => only} = case_input.("signature.error.asymmetric_only")
      only_request = env(Map.to_list(only), body)
      assert only["webhook-signature"] == @asym_sig
      assert Asymmetric.verify(only_request, asym_opts(now: now)) == :ok
      assert Hmac.verify(only_request, hmac_opts) == {:error, :malformed_signature}
    end
  end

  describe "Ankusa.Verifier.Asymmetric (rsa_sha256, inline scheme)" do
    setup do
      rsa = :public_key.generate_key({:rsa, 2048, 65_537})
      {:RSAPrivateKey, _, n, e, _, _, _, _, _, _, _} = rsa
      pub = {:RSAPublicKey, n, e}

      spki_pem =
        :public_key.pem_encode([:public_key.pem_entry_encode(:SubjectPublicKeyInfo, pub)])

      [{:SubjectPublicKeyInfo, spki_der, _}] = :public_key.pem_decode(spki_pem)

      pkcs1_pem =
        :public_key.pem_encode([
          {:RSAPublicKey, :public_key.der_encode(:RSAPublicKey, pub), :not_encrypted}
        ])

      scheme = %Asymmetric.Scheme{
        signature_header: "X-Signature",
        algorithm: :rsa_sha256,
        encoding: :hex,
        sig_prefix: "rsa="
      }

      %{
        rsa: rsa,
        scheme: scheme,
        keys: [spki_pem, pkcs1_pem, Base.encode64(spki_der)]
      }
    end

    defp rsa_env(rsa, body) do
      sig = body |> :public_key.sign(:sha256, rsa) |> Base.encode16(case: :lower)
      env([{"X-Signature", "rsa=" <> sig}], body)
    end

    test "verifies a PKCS#1 v1.5 signature under a PEM, a PKCS#1 PEM or a bare base64 DER key", %{
      rsa: rsa,
      scheme: scheme,
      keys: keys
    } do
      for key <- keys do
        opts = [scheme: scheme, public_key: key]
        assert Asymmetric.validate_opts(opts) == :ok
        assert Asymmetric.verify(rsa_env(rsa, ~s({"a":1})), opts) == :ok
      end
    end

    test "a tampered body, a signature of another key and a truncated one fail", %{
      rsa: rsa,
      scheme: scheme,
      keys: [key | _]
    } do
      opts = [scheme: scheme, public_key: key]
      signed = rsa_env(rsa, ~s({"a":1}))

      assert Asymmetric.verify(%{signed | body: ~s({"a":2})}, opts) == {:error, :no_match}

      other = :public_key.generate_key({:rsa, 2048, 65_537})
      assert Asymmetric.verify(rsa_env(other, ~s({"a":1})), opts) == {:error, :no_match}

      {"X-Signature", "rsa=" <> hex} = List.keyfind(signed.headers, "X-Signature", 0)
      truncated = %{signed | headers: [{"X-Signature", "rsa=" <> binary_part(hex, 0, 100)}]}
      assert Asymmetric.verify(truncated, opts) == {:error, :no_match}

      assert Asymmetric.verify(%{signed | headers: []}, opts) == {:error, :missing_signature}
    end

    test "a key under 2048 bits or an ed25519 key under an RSA scheme is :bad_key", %{
      scheme: scheme
    } do
      small = :public_key.generate_key({:rsa, 1024, 65_537})
      {:RSAPrivateKey, _, n, e, _, _, _, _, _, _, _} = small

      pem =
        :public_key.pem_encode([
          :public_key.pem_entry_encode(:SubjectPublicKeyInfo, {:RSAPublicKey, n, e})
        ])

      for key <- [pem, @asym_pub, "whpk_" <> Base.encode64(:crypto.strong_rand_bytes(32))] do
        assert Asymmetric.validate_opts(scheme: scheme, public_key: key) == {:error, :bad_key}
      end
    end
  end

  describe "Ankusa.Verifier.Token" do
    defp token_env(headers), do: env(headers, "{}")

    test "a bearer token, in any case of the scheme word" do
      opts = [secret: "s3cret"]

      for word <- ["Bearer", "bearer", "BEARER"] do
        assert Token.verify(token_env([{"authorization", word <> " s3cret"}]), opts) == :ok
      end

      assert Token.verify(token_env([{"authorization", "Bearer wrong"}]), opts) ==
               {:error, :no_match}

      assert Token.verify(token_env([{"authorization", "Bearer s3cre"}]), opts) ==
               {:error, :no_match}

      assert Token.verify(token_env([{"authorization", "Bearer s3cretX"}]), opts) ==
               {:error, :no_match}

      assert Token.verify(token_env([]), opts) == {:error, :missing_signature}

      for malformed <- ["s3cret", "Basic czNjcmV0", "Bearer", "Bearer "] do
        assert Token.verify(token_env([{"authorization", malformed}]), opts) ==
                 {:error, :malformed_signature}
      end
    end

    test "basic auth compares the whole user:password" do
      opts = [secret: "svc:p4ss:word", location: :basic]
      good = "Basic " <> Base.encode64("svc:p4ss:word")

      assert Token.verify(token_env([{"authorization", good}]), opts) == :ok

      assert Token.verify(
               token_env([{"authorization", "Basic " <> Base.encode64("svc:other")}]),
               opts
             ) ==
               {:error, :no_match}

      assert Token.verify(token_env([{"authorization", "Basic !!!"}]), opts) ==
               {:error, :malformed_signature}

      assert Token.verify(
               token_env([{"authorization", "Bearer " <> Base.encode64("svc:p4ss:word")}]),
               opts
             ) ==
               {:error, :malformed_signature}

      assert Token.verify(token_env([]), opts) == {:error, :missing_signature}
    end

    test "a token in a header of the provider's choosing" do
      opts = [secret: "s3cret", location: {:header, "X-Webhook-Token"}]

      assert Token.verify(token_env([{"x-webhook-token", "s3cret"}]), opts) == :ok
      assert Token.verify(token_env([{"X-Webhook-Token", "s3cret"}]), opts) == :ok
      assert Token.verify(token_env([{"x-webhook-token", "nope"}]), opts) == {:error, :no_match}

      assert Token.verify(token_env([{"x-webhook-token", ""}]), opts) ==
               {:error, :missing_signature}

      assert Token.verify(token_env([{"authorization", "Bearer s3cret"}]), opts) ==
               {:error, :missing_signature}
    end

    test "a token in the query string, decoded, from the context the edge supplies" do
      opts = [secret: "s3cret!", location: {:query, "token"}]

      assert Token.verify(token_env([]), [query: "a=1&token=s3cret%21&b=2"] ++ opts) == :ok
      assert Token.verify(token_env([]), [query: "token=s3cret%21"] ++ opts) == :ok
      assert Token.verify(token_env([]), [query: "token=nope"] ++ opts) == {:error, :no_match}
      assert Token.verify(token_env([]), [query: "token=%zz"] ++ opts) == {:error, :no_match}

      assert Token.verify(token_env([]), [query: "xtoken=s3cret%21"] ++ opts) ==
               {:error, :missing_signature}

      assert Token.verify(token_env([]), [query: "token="] ++ opts) ==
               {:error, :missing_signature}

      assert Token.verify(token_env([]), [query: ""] ++ opts) == {:error, :missing_signature}
      assert Token.verify(token_env([]), opts) == {:error, :missing_signature}

      assert Token.verify(token_env([{"x-token", "s3cret!"}]), [query: nil] ++ opts) ==
               {:error, :missing_signature}
    end

    test "a rotation list accepts any of its secrets" do
      opts = [secret: ["new-token", "old-token"]]

      for token <- ["new-token", "old-token"] do
        assert Token.verify(token_env([{"authorization", "Bearer " <> token}]), opts) == :ok
      end

      assert Token.verify(token_env([{"authorization", "Bearer older"}]), opts) ==
               {:error, :no_match}
    end

    test "an empty or missing secret fails every hook with :bad_secret, even the empty request" do
      for bad <- [nil, "", [], [""], ["ok", ""], 42, List.duplicate("k", 9)] do
        opts = [secret: bad]
        assert Token.validate_opts(opts) == {:error, :bad_secret}

        assert Token.verify(token_env([{"authorization", "Bearer "}]), opts) ==
                 {:error, :bad_secret}
      end

      assert Token.validate_opts(secret: "x:y", location: :basic) == :ok
      assert Token.validate_opts(secret: "no-colon", location: :basic) == {:error, :bad_secret}
      assert Token.validate_opts(secret: List.duplicate("k", 8)) == :ok
    end

    test "an unknown location is :bad_scheme" do
      for location <- [:digest, {:header, ""}, {:header, :x}, {:cookie, "a"}, "bearer"] do
        assert Token.validate_opts(secret: "k", location: location) == {:error, :bad_scheme}
      end
    end

    test "credential_headers names the header the token travels in, for the stored-header policy" do
      assert Token.credential_headers(secret: "k") == ["authorization"]
      assert Token.credential_headers(secret: "k", location: :basic) == ["authorization"]

      assert Token.credential_headers(secret: "k", location: {:header, "X-Hook-Token"}) == [
               "x-hook-token"
             ]

      assert Token.credential_headers(secret: "k", location: {:query, "t"}) == []
    end

    test "the scheme is named for telemetry" do
      assert Token.scheme_name([]) == "bearer"
      assert Token.scheme_name(location: :basic) == "basic"
      assert Token.scheme_name(location: {:header, "X"}) == "header"
      assert Token.scheme_name(location: {:query, "t"}) == "query"
    end
  end

  describe "Ankusa.Verifier.ClientCert" do
    @tls_dir Path.expand("../fixtures/tls", __DIR__)

    defp cert_der(name) do
      [{:Certificate, der, _}] =
        @tls_dir |> Path.join(name) |> File.read!() |> :public_key.pem_decode()

      der
    end

    defp fingerprint(der), do: Base.encode16(:crypto.hash(:sha256, der), case: :lower)

    defp cert_env, do: env([], "{}")

    test "a certificate whose fingerprint is pinned is accepted, however the pin is written" do
      der = cert_der("client.pem")
      lower = fingerprint(der)

      colons =
        lower
        |> String.upcase()
        |> String.graphemes()
        |> Enum.chunk_every(2)
        |> Enum.map_join(":", &Enum.join/1)

      for pin <- [
            lower,
            String.upcase(lower),
            colons,
            [colons, "00" <> binary_part(lower, 2, 62)]
          ] do
        assert ClientCert.verify(cert_env(), fingerprints: pin, peer_cert: der) == :ok
      end
    end

    test "a certificate nobody pinned is :no_match, though its CA vouches for it" do
      server = cert_der("server.pem")
      stranger = cert_der("stranger.pem")
      pinned = fingerprint(cert_der("client.pem"))

      for der <- [server, stranger] do
        assert ClientCert.verify(cert_env(), fingerprints: [pinned], peer_cert: der) ==
                 {:error, :no_match}
      end
    end

    test "a subject pin matches the common name or a subject alternative name" do
      client = cert_der("client.pem")
      server = cert_der("server.pem")

      assert ClientCert.verify(cert_env(), subjects: ["cn:ankusa-test-client"], peer_cert: client) ==
               :ok

      assert ClientCert.verify(cert_env(),
               subjects: ["cn:other", "cn:ankusa-test-client"],
               peer_cert: client
             ) == :ok

      assert ClientCert.verify(cert_env(), subjects: ["dns:localhost"], peer_cert: server) == :ok
      assert ClientCert.verify(cert_env(), subjects: ["ip:127.0.0.1"], peer_cert: server) == :ok

      assert ClientCert.verify(cert_env(), subjects: ["cn:ankusa-test-client"], peer_cert: server) ==
               {:error, :no_match}

      assert ClientCert.verify(cert_env(),
               subjects: ["dns:ankusa-test-client"],
               peer_cert: client
             ) == {:error, :no_match}
    end

    test "either pin admits the certificate" do
      client = cert_der("client.pem")

      opts = [
        fingerprints: [String.duplicate("0", 64)],
        subjects: ["cn:ankusa-test-client"],
        peer_cert: client
      ]

      assert ClientCert.verify(cert_env(), opts) == :ok

      assert ClientCert.verify(cert_env(), Keyword.put(opts, :subjects, ["cn:x"])) ==
               {:error, :no_match}
    end

    test "no certificate is :missing_signature: plain HTTP, a request the edge gave none, a re-verification" do
      for opts <- [
            [fingerprints: [String.duplicate("a", 64)]],
            [fingerprints: [String.duplicate("a", 64)], peer_cert: nil],
            [fingerprints: [String.duplicate("a", 64)], peer_cert: ""],
            [subjects: ["cn:x"], peer_cert: nil]
          ] do
        assert ClientCert.verify(cert_env(), opts) == {:error, :missing_signature}
      end
    end

    test "bytes that are not a certificate never match a fingerprint or a subject" do
      assert ClientCert.verify(cert_env(), subjects: ["cn:x"], peer_cert: "garbage") ==
               {:error, :malformed_signature}

      assert ClientCert.verify(cert_env(),
               fingerprints: [String.duplicate("a", 64)],
               peer_cert: "garbage"
             ) ==
               {:error, :no_match}
    end

    test "a source that pins nothing usable fails every hook with :bad_pin" do
      der = cert_der("client.pem")

      for bad <- [
            [],
            [fingerprints: []],
            [subjects: []],
            [fingerprints: [String.duplicate("a", 63)]],
            [fingerprints: [String.duplicate("g", 64)]],
            [fingerprints: [nil]],
            [subjects: ["bob"]],
            [subjects: ["cn:"]],
            [subjects: [:cn]],
            [fingerprints: [String.duplicate("a", 64)], subjects: ["bob"]]
          ] do
        assert ClientCert.validate_opts(bad) == {:error, :bad_pin}
        assert ClientCert.verify(cert_env(), [peer_cert: der] ++ bad) == {:error, :bad_pin}
      end

      assert ClientCert.validate_opts(
               subjects: ["dns:a", "email:a@b", "uri:spiffe://x", "ip:10.0.0.1", "cn:a"]
             ) == :ok

      assert ClientCert.normalize_fingerprint(String.duplicate("AB:", 31) <> "AB") ==
               {:ok, String.duplicate("ab", 32)}

      assert ClientCert.scheme_name([]) == "client_cert"
    end
  end

  describe "validate_config!/1 for the asymmetric, token and client-certificate verifiers" do
    defp verified_config(verifier, tls \\ nil) do
      Ankusa.Config.new(
        instance: :"verifier_breadth_#{System.unique_integer([:positive])}",
        data_dir: System.tmp_dir!(),
        roles: [:edge],
        listener: %{tls: tls},
        source_store: {Ankusa.SourceStore.Static, sources: %{"probe" => [verifier: verifier]}}
      )
    end

    defp refuses(verifier, message, tls \\ nil) do
      assert_raise ArgumentError, message, fn ->
        Ankusa.Verifier.validate_config!(verified_config(verifier, tls))
      end
    end

    test "an unusable token secret or location refuses the boot, naming the source" do
      refuses({Token, secret: ""}, "source probe: verifier secret is missing or undecodable")

      refuses(
        {Token, secret: "no-colon", location: :basic},
        "source probe: verifier secret is missing or undecodable"
      )

      refuses(
        {Token, secret: "k", location: :digest},
        "source probe: verifier scheme is missing or unknown"
      )

      assert :ok = Ankusa.Verifier.validate_config!(verified_config({Token, secret: "k"}))
    end

    test "an unusable public key, scheme or tolerance refuses the boot, naming the source" do
      refuses(
        {Asymmetric, asym_opts(public_key: "nope")},
        "source probe: verifier public_key is missing or is not a key of the scheme's algorithm"
      )

      refuses(
        {Asymmetric, asym_opts(scheme: :nope)},
        "source probe: verifier scheme is missing or unknown"
      )

      refuses(
        {Asymmetric, asym_opts(tolerance: 3601)},
        ~r/^source probe: verifier tolerance must be an integer/
      )

      assert :ok = Ankusa.Verifier.validate_config!(verified_config({Asymmetric, asym_opts()}))
    end

    test "a certificate source pins something, and needs a listener that asks for a client certificate" do
      refuses({ClientCert, []}, ~r/^source probe: verifier pins no certificate/)

      pinned = {ClientCert, subjects: ["cn:ankusa-test-client"]}

      needs_mtls =
        ~r/^source probe: client_cert verification needs listener.tls with verify: verify_peer/

      refuses(pinned, needs_mtls)

      tls = [
        certfile: Path.join(@tls_dir, "server.pem"),
        keyfile: Path.join(@tls_dir, "server-key.pem")
      ]

      refuses(pinned, needs_mtls, tls)

      mtls = tls ++ [cacertfile: Path.join(@tls_dir, "ca.pem"), verify: :verify_peer]
      assert :ok = Ankusa.Verifier.validate_config!(verified_config(pinned, mtls))
    end
  end

  describe "allocations per verify (PERF-1)" do
    # The refc binary bytes a verify call leaves in its process beyond what the
    # process held before it: the heap is sized so no collection runs, so a
    # second copy of the body shows up as its size.
    defp new_binary_bytes(fun) do
      parent = self()

      {_pid, ref} =
        :erlang.spawn_opt(
          fn ->
            held = fn -> elem(Process.info(self(), :binary), 1) end
            before = MapSet.new(held.(), fn {id, _size, _refc} -> id end)
            result = fun.()

            fresh = for {id, size, _refc} <- held.(), not MapSet.member?(before, id), do: size
            send(parent, {:measured, result, Enum.sum(fresh)})
          end,
          [:monitor, min_heap_size: 2_000_000, min_bin_vheap_size: 200_000_000]
        )

      measured =
        receive do
          {:measured, result, bytes} -> {result, bytes}
        after
          30_000 -> flunk("the measurement did not finish")
        end

      assert_receive {:DOWN, ^ref, :process, _pid, _reason}, 5_000
      measured
    end

    test "verifying an 8 MB body does not copy the body" do
      secret = "whsec_" <> Base.encode16(:crypto.strong_rand_bytes(16))
      body = :crypto.strong_rand_bytes(8 * 1024 * 1024)
      t = System.system_time(:second)
      header = stripe_sign(secret, t, body)
      request = env([{"Stripe-Signature", header}], body)

      {result, bytes} =
        new_binary_bytes(fn -> Hmac.verify(request, scheme: :stripe, secret: secret) end)

      assert result == :ok
      assert bytes < div(byte_size(body), 2)
    end

    test "a header token and a literal in the template are filled without a flattened copy" do
      key = :crypto.strong_rand_bytes(24)
      secret = "whsec_" <> Base.encode64(key)
      body = :crypto.strong_rand_bytes(8 * 1024 * 1024)
      ts = System.system_time(:second)
      sig = "v1," <> Base.encode64(:crypto.mac(:hmac, :sha256, key, "msg_1.#{ts}." <> body))
      request = swh_env("msg_1", ts, sig, body)

      {result, bytes} =
        new_binary_bytes(fn ->
          Hmac.verify(request, scheme: :standard_webhooks, secret: secret)
        end)

      assert result == :ok
      assert bytes < div(byte_size(body), 2)
    end

    test "an inline scheme with a header token compiles its template per verify and still verifies" do
      scheme = %Scheme{
        signature_header: "x-sig",
        signed: "{header:X-Req-Id}|{body}|{header:x-req-id}",
        encoding: :hex
      }

      sig = Base.encode16(:crypto.mac(:hmac, :sha256, "k", "r1|payload|r1"), case: :lower)

      assert Hmac.verify(env([{"X-Sig", sig}, {"x-REQ-id", "r1"}], "payload"),
               scheme: scheme,
               secret: "k"
             ) == :ok

      assert {:error, :missing_signature} =
               Hmac.verify(env([{"X-Sig", sig}], "payload"), scheme: scheme, secret: "k")
    end

    test "a template compiles to literals, body, ts and lowercased header tokens" do
      assert Ankusa.Verifier.Template.compile("{header:Webhook-Id}.{ts}.{body}") ==
               [{:header, "webhook-id"}, ".", :ts, ".", :body]

      assert Ankusa.Verifier.Template.compile("{body}") == [:body]
      assert Ankusa.Verifier.Template.compile("v0:{ts}:{body}") == ["v0:", :ts, ":", :body]
      assert Ankusa.Verifier.Template.compile("{body}{ts}") == [:body, :ts]

      # Text that only looks like a token is a literal, as `Regex.replace` left it.
      assert Ankusa.Verifier.Template.compile("a{header:x") == ["a{header:x"]
      assert Ankusa.Verifier.Template.compile("{foo}{body}") == ["{foo}", :body]
    end
  end
end
