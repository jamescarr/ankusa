defmodule Ankusa.VerifierTest do
  use ExUnit.Case, async: true

  alias Ankusa.Envelope

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

  describe "Ankusa.Verifier.StandardWebhooks" do
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

      assert Ankusa.Verifier.StandardWebhooks.verify(
               swh_env("msg_1", ts, sig, body),
               secret: secret
             ) == :ok
    end

    test "accepts when any token in the list matches", %{key: key, secret: secret} do
      ts = System.system_time(:second)
      body = "payload"
      good = swh_sign(key, "msg_1", ts, body)
      header = "v1,AAAAbogus== #{good}"

      assert Ankusa.Verifier.StandardWebhooks.verify(
               swh_env("msg_1", ts, header, body),
               secret: secret
             ) == :ok
    end

    test "rejects a tampered body", %{key: key, secret: secret} do
      ts = System.system_time(:second)
      sig = swh_sign(key, "msg_1", ts, "original")

      assert {:error, :no_match} =
               Ankusa.Verifier.StandardWebhooks.verify(
                 swh_env("msg_1", ts, sig, "tampered"),
                 secret: secret
               )
    end

    test "rejects a stale timestamp", %{key: key, secret: secret} do
      ts = System.system_time(:second) - 10_000
      body = "payload"
      sig = swh_sign(key, "msg_1", ts, body)

      assert {:error, :timestamp_out_of_tolerance} =
               Ankusa.Verifier.StandardWebhooks.verify(
                 swh_env("msg_1", ts, sig, body),
                 secret: secret
               )
    end

    test "rejects missing headers", %{secret: secret} do
      assert {:error, :missing_signature} =
               Ankusa.Verifier.StandardWebhooks.verify(env([], "body"), secret: secret)
    end
  end

  describe "Ankusa.Verifier.Stripe" do
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

      assert Ankusa.Verifier.Stripe.verify(
               env([{"Stripe-Signature", header}], body),
               secret: secret
             ) == :ok
    end

    test "rejects a tampered body", %{secret: secret} do
      t = System.system_time(:second)
      header = stripe_sign(secret, t, "original")

      assert {:error, :no_match} =
               Ankusa.Verifier.Stripe.verify(
                 env([{"Stripe-Signature", header}], "tampered"),
                 secret: secret
               )
    end

    test "rejects a stale timestamp", %{secret: secret} do
      t = System.system_time(:second) - 10_000
      body = "payload"
      header = stripe_sign(secret, t, body)

      assert {:error, :timestamp_out_of_tolerance} =
               Ankusa.Verifier.Stripe.verify(
                 env([{"Stripe-Signature", header}], body),
                 secret: secret
               )
    end

    test "rejects a missing header", %{secret: secret} do
      assert {:error, :missing_signature} =
               Ankusa.Verifier.Stripe.verify(env([], "body"), secret: secret)
    end
  end

  describe "Ankusa.Verifier.GitHub" do
    setup do
      %{secret: "It's a Secret to Everybody"}
    end

    defp gh_sign(secret, body) do
      "sha256=" <> Base.encode16(:crypto.mac(:hmac, :sha256, secret, body), case: :lower)
    end

    test "accepts a valid signature", %{secret: secret} do
      body = ~s({"zen":"Design for failure."})
      header = gh_sign(secret, body)

      assert Ankusa.Verifier.GitHub.verify(
               env([{"X-Hub-Signature-256", header}], body),
               secret: secret
             ) == :ok
    end

    test "rejects a tampered body", %{secret: secret} do
      header = gh_sign(secret, "original")

      assert {:error, :no_match} =
               Ankusa.Verifier.GitHub.verify(
                 env([{"X-Hub-Signature-256", header}], "tampered"),
                 secret: secret
               )
    end

    test "rejects a missing header", %{secret: secret} do
      assert {:error, :missing_signature} =
               Ankusa.Verifier.GitHub.verify(env([], "body"), secret: secret)
    end
  end
end
