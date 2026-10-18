defmodule Ankusa.CryptoTest do
  @moduledoc """
  `Ankusa.Crypto.Envelope`: the value format, the keyring (static keys, a
  provider, rotation), and which fields of a source spec are encrypted. The
  stores that use it are covered where they live: `source_store_persistent_test.exs`
  (no plaintext at rest, rewrap), `secrets_rewrap_task_test.exs`, and the Redis
  store's suite.
  """

  use ExUnit.Case, async: true

  alias Ankusa.Admin.Redact
  alias Ankusa.Crypto.Envelope

  @key_a :binary.copy(<<1>>, 32)
  @key_b :binary.copy(<<2>>, 32)
  @kek_a Base.encode64(@key_a)
  @kek_b Base.encode64(@key_b)

  @no_secrets %{kek: nil, kek_previous: [], kek_provider: nil}

  defp ring!(overrides) do
    assert {:ok, ring} =
             overrides |> Map.new() |> then(&Map.merge(@no_secrets, &1)) |> Envelope.keyring()

    ring
  end

  # Called through `kek_provider`.
  def provider(keys), do: {:ok, keys}
  def failing_provider, do: {:error, :kms_down}
  def raising_provider, do: raise("boom: #{@kek_a}")

  describe "values" do
    test "the documented layout: AENC, version, key id, iv, tag, ciphertext" do
      ring = ring!(kek: "prod-2026:" <> @kek_a)

      assert <<"AENC", 1::8, 9::8, "prod-2026", _iv::binary-size(12), _tag::binary-size(16),
               ciphertext::binary>> = value = Envelope.encrypt(ring, "hunter2")

      assert byte_size(ciphertext) == byte_size("hunter2")
      assert Envelope.encrypted?(value)
      refute Envelope.encrypted?("hunter2")
      assert Envelope.decrypt(ring, value) == {:ok, "hunter2"}
    end

    test "a key without an id is named by a digest of the key, never by the key" do
      ring = ring!(kek: @kek_a)
      id = Envelope.current_key_id(ring)

      assert id ==
               :sha256 |> :crypto.hash(@key_a) |> Base.encode16(case: :lower) |> binary_part(0, 8)

      refute String.contains?(inspect(ring), @kek_a)
      refute String.contains?(inspect(ring), Base.encode16(@key_a, case: :lower))
    end

    test "every encryption has its own IV" do
      ring = ring!(kek: @kek_a)
      refute Envelope.encrypt(ring, "same") == Envelope.encrypt(ring, "same")
    end

    test "an empty value round-trips" do
      ring = ring!(kek: @kek_a)
      assert Envelope.decrypt(ring, Envelope.encrypt(ring, "")) == {:ok, ""}
    end

    test "a flipped bit anywhere in the ciphertext, tag or IV is :tampered" do
      ring = ring!(kek: "k1:" <> @kek_a)
      value = Envelope.encrypt(ring, "hunter2")
      header = byte_size("AENC") + 2 + byte_size("k1")

      for index <- header..(byte_size(value) - 1) do
        <<before::binary-size(^index), byte, rest::binary>> = value

        assert Envelope.decrypt(ring, <<before::binary, Bitwise.bxor(byte, 1), rest::binary>>) ==
                 {:error, :tampered}
      end
    end

    test "the key id is authenticated: a value relabelled with another key's id is :tampered" do
      ring = ring!(kek: "k1:" <> @kek_a, kek_previous: ["k2:" <> @kek_b])
      <<"AENC", 1, 2, "k1", rest::binary>> = Envelope.encrypt(ring, "hunter2")

      assert Envelope.decrypt(ring, <<"AENC", 1, 2, "k2", rest::binary>>) == {:error, :tampered}
    end

    test "refusals say why, and never include key bytes" do
      ring = ring!(kek: "k1:" <> @kek_a)
      value = Envelope.encrypt(ring, "hunter2")
      other = ring!(kek: "k9:" <> @kek_b)

      assert Envelope.decrypt(other, value) == {:error, {:unknown_key, "k1"}}
      assert Envelope.decrypt(ring!([]), value) == {:error, :no_kek}
      assert Envelope.decrypt(ring, "plain") == {:error, :not_encrypted}
      assert Envelope.decrypt(ring, binary_part(value, 0, 12)) == {:error, :malformed}

      <<"AENC", 1, rest::binary>> = value

      assert Envelope.decrypt(ring, <<"AENC", 2, rest::binary>>) ==
               {:error, {:unsupported_version, 2}}
    end

    test "encrypt raises without a KEK rather than writing plaintext" do
      assert_raise ArgumentError, ~r/no secrets.kek/, fn -> Envelope.encrypt(ring!([]), "x") end
    end
  end

  describe "keys" do
    test "kek_previous decrypts but never encrypts" do
      old = ring!(kek: "old:" <> @kek_a)
      value = Envelope.encrypt(old, "hunter2")

      rotated = ring!(kek: "new:" <> @kek_b, kek_previous: ["old:" <> @kek_a])
      assert Envelope.decrypt(rotated, value) == {:ok, "hunter2"}
      assert Envelope.current_key_id(rotated) == "new"
      assert <<"AENC", 1, 3, "new", _::binary>> = Envelope.encrypt(rotated, "x")
    end

    test "a provider returns the current key first, then the previous ones" do
      provider =
        {__MODULE__, :provider, [["cur:" <> @kek_b, {"old", @key_a}]]}

      ring = ring!(kek_provider: provider)
      assert Envelope.current_key_id(ring) == "cur"
      assert {:ok, "x"} = ring |> Envelope.encrypt("x") |> then(&Envelope.decrypt(ring, &1))

      old = ring!(kek: "old:" <> @kek_a)
      assert Envelope.decrypt(ring, Envelope.encrypt(old, "legacy")) == {:ok, "legacy"}
    end

    test "a failing or raising provider is an error value that does not echo what it raised" do
      assert Envelope.keyring(%{@no_secrets | kek_provider: {__MODULE__, :failing_provider, []}}) ==
               {:error, {:kek_provider, :kms_down}}

      assert Envelope.keyring(%{@no_secrets | kek_provider: {__MODULE__, :raising_provider, []}}) ==
               {:error, {:kek_provider, {:raised, RuntimeError}}}

      assert Envelope.keyring(%{@no_secrets | kek_provider: {__MODULE__, :provider, [[]]}}) ==
               {:error, {:kek_provider, :no_keys}}
    end

    test "malformed keys are errors that name the problem, not the key" do
      assert Envelope.keyring(%{@no_secrets | kek: "not base64!"}) ==
               {:error, {:bad_kek, :not_base64}}

      assert Envelope.keyring(%{@no_secrets | kek: Base.encode64("short")}) ==
               {:error, {:bad_kek, :wrong_size}}

      assert Envelope.keyring(%{@no_secrets | kek: "bad id:" <> @kek_a}) ==
               {:error, {:bad_kek, :bad_id}}

      assert Envelope.keyring(%{
               @no_secrets
               | kek: "k:" <> @kek_a,
                 kek_previous: ["k:" <> @kek_b]
             }) ==
               {:error, {:bad_kek, :duplicate_id}}
    end

    test "the same key listed twice is one key" do
      assert {:ok, _} =
               Envelope.keyring(%{
                 @no_secrets
                 | kek: "k:" <> @kek_a,
                   kek_previous: ["k:" <> @kek_a]
               })
    end

    test "Config validates the section at build time without printing a key" do
      assert %Ankusa.Config{} = Ankusa.Config.new(secrets: [kek: @kek_a, kek_previous: [@kek_b]])

      for {secrets, message} <- [
            {[kek: "nope!"], ~r/secrets.kek must be a base64-encoded 32-byte key/},
            {[kek: @kek_a, kek_provider: {__MODULE__, :provider, [[]]}], ~r/mutually exclusive/},
            {[kek_previous: [@kek_a]], ~r/needs secrets.kek or secrets.kek_provider/},
            {[kek: @kek_a, kek_previous: ["nope!"]], ~r/secrets.kek_previous must be/},
            {[kek: @kek_a, kek_previous: "string"], ~r/kek_previous must be a list/},
            {[kek: 42], ~r/secrets.kek must be a string/},
            {[kek_provider: :nope], ~r/kek_provider must be nil or/},
            {[bogus: 1], ~r/unknown Ankusa.Config key: secrets.bogus/}
          ] do
        error = assert_raise ArgumentError, fn -> Ankusa.Config.new(secrets: secrets) end
        assert error.message =~ message
        refute error.message =~ @kek_a
      end
    end

    test "the startup line names the key id, or says secrets are plaintext" do
      with_kek = Ankusa.Config.new(secrets: [kek: "prod-1:" <> @kek_a])
      assert Envelope.describe(with_kek) == "secrets envelope-encrypted with key prod-1"

      assert Envelope.describe(Ankusa.Config.new()) =~ "plaintext"

      provided = Ankusa.Config.new(secrets: [kek_provider: {__MODULE__, :provider, [[]]}])
      assert Envelope.describe(provided) =~ "key provider Ankusa.CryptoTest.provider/1"
    end
  end

  describe "specs" do
    @spec_map %{
      "trust_url_tenant" => false,
      "on_verify_failure" => "reject",
      "verify" => %{
        "type" => "hmac",
        "secret" => "whsec-canary",
        "signature_header" => "X-Signature",
        "tolerance_seconds" => 300
      },
      "sinks" => [
        %{
          "type" => "http",
          "url" => "https://relay:url-pass-canary@sink.example/hook",
          "method" => "POST",
          "timeout_ms" => 5000,
          "headers" => %{"authorization" => "Bearer header-canary", "x-trace" => "t"},
          "secret" => "sink-secret-canary"
        },
        %{"type" => "http", "url" => "https://open.example/hook"},
        %{
          "type" => "kafka",
          "brokers" => ["k1:9092"],
          "topic" => "hooks",
          "sasl" => %{"mechanism" => "plain", "username" => "svc", "password" => "sasl-canary"}
        },
        %{
          "type" => "sqs",
          "queue_url" => "https://sqs.example/q",
          "region" => "us-east-1",
          "access_key_id" => "AKIA-canary",
          "secret_access_key" => "aws-canary"
        }
      ],
      "forward_headers" => ["x-keep"]
    }

    @canaries ~w(whsec-canary url-pass-canary header-canary sink-secret-canary sasl-canary
                 AKIA-canary aws-canary)

    test "every secret-bearing field is encrypted, identity and structure are not" do
      ring = ring!(kek: @kek_a)
      sealed = Envelope.seal(ring, @spec_map)
      json = JSON.encode!(sealed)

      for canary <- @canaries, do: refute(json =~ canary, "#{canary} is in the clear")

      # still readable and diffable
      assert sealed["trust_url_tenant"] == false
      assert sealed["verify"]["type"] == "hmac"
      assert sealed["verify"]["signature_header"] == "X-Signature"
      assert sealed["verify"]["tolerance_seconds"] == 300
      assert [http, open, kafka, sqs] = sealed["sinks"]
      assert http["method"] == "POST" and http["timeout_ms"] == 5000
      assert Map.keys(http["headers"]) |> Enum.sort() == ["authorization", "x-trace"]
      assert open["url"] == "https://open.example/hook"
      assert kafka["topic"] == "hooks" and kafka["sasl"]["username"] == "svc"
      assert sqs["queue_url"] == "https://sqs.example/q" and sqs["region"] == "us-east-1"
      assert sealed["forward_headers"] == ["x-keep"]

      assert String.starts_with?(sealed["verify"]["secret"], "AENC:")
      assert String.starts_with?(http["url"], "AENC:")
    end

    test "what Redact hides from the admin API is what is encrypted" do
      ring = ring!(kek: @kek_a)
      sealed = Envelope.seal(ring, @spec_map)

      shown =
        Redact.source_entry(%{tenant: "acme", name: "n", source_id: "acme.n", spec: @spec_map})

      [http, open, kafka, sqs] = shown["sinks"]
      assert shown["verify"]["secret"] == "[REDACTED]"
      assert http["secret"] == "[REDACTED]" and http["headers"]["authorization"] == "[REDACTED]"
      assert kafka["sasl"]["password"] == "[REDACTED]"
      assert sqs["secret_access_key"] == "[REDACTED]"
      assert open["url"] == "https://open.example/hook"

      # the same fields, found by the same walk
      {_spec, hidden} =
        Redact.reduce_hidden(@spec_map, [], fn string, acc -> {string, [string | acc]} end)

      assert "whsec-canary" in hidden and "Bearer header-canary" in hidden
      assert "https://relay:url-pass-canary@sink.example/hook" in hidden
      refute "https://open.example/hook" in hidden
      refute "hooks" in hidden

      assert {:ok, @spec_map} = Envelope.unseal(ring, sealed)
    end

    test "a rotation list of secrets is encrypted element by element" do
      ring = ring!(kek: @kek_a)
      spec = %{"verify" => %{"type" => "stripe", "secret" => ["new-canary", "old-canary"]}}
      sealed = Envelope.seal(ring, spec)

      refute JSON.encode!(sealed) =~ "canary"
      assert [<<"AENC:", _::binary>>, <<"AENC:", _::binary>>] = sealed["verify"]["secret"]
      assert Envelope.unseal(ring, sealed) == {:ok, spec}
    end

    test "without a KEK a spec is stored as it is, and a plain spec unseals without one" do
      assert Envelope.seal(ring!([]), @spec_map) == @spec_map
      assert Envelope.unseal(ring!([]), @spec_map) == {:ok, @spec_map}
    end

    test "unseal refuses what no key opens, whole, never half-decrypted" do
      sealed = Envelope.seal(ring!(kek: "k1:" <> @kek_a), @spec_map)

      assert Envelope.unseal(ring!([]), sealed) == {:error, :no_kek}

      assert Envelope.unseal(ring!(kek: "k2:" <> @kek_b), sealed) ==
               {:error, {:unknown_key, "k1"}}

      tampered =
        put_in(
          sealed,
          ["verify", "secret"],
          "AENC:" <> Base.encode64(<<"AENC", 1, 2, "k1", 0::size(28 * 8)>>)
        )

      assert Envelope.unseal(ring!(kek: "k1:" <> @kek_a), tampered) == {:error, :tampered}
    end

    test "a string that only looks like the prefix is somebody's plaintext" do
      ring = ring!(kek: @kek_a)
      spec = %{"verify" => %{"type" => "AENC:not-an-envelope", "secret" => "AENC:!!"}}
      assert Envelope.unseal(ring, spec) == {:ok, spec}

      assert {:ok, sealed} = Envelope.rewrap_spec(ring, spec)
      assert Envelope.unseal(ring, sealed) == {:ok, spec}
    end

    test "rewrap_spec moves plaintext and other-key values to the current key, and leaves current ones" do
      old = ring!(kek: "old:" <> @kek_a)
      current = ring!(kek: "new:" <> @kek_b, kek_previous: ["old:" <> @kek_a])

      assert {:ok, from_plain} = Envelope.rewrap_spec(current, @spec_map)
      assert Envelope.rewrap_spec(current, from_plain) == :current
      assert JSON.encode!(from_plain) =~ ~s(AENC:)

      under_old = Envelope.seal(old, @spec_map)
      assert {:ok, moved} = Envelope.rewrap_spec(current, under_old)
      assert Envelope.rewrap_spec(current, moved) == :current

      # the moved row no longer needs the old key
      assert {:ok, @spec_map} = Envelope.unseal(ring!(kek: "new:" <> @kek_b), moved)

      assert Envelope.unseal(ring!(kek: "new:" <> @kek_b), under_old) ==
               {:error, {:unknown_key, "old"}}

      assert Envelope.rewrap_spec(ring!([]), @spec_map) == {:error, :no_kek}
    end
  end

  describe "the admin API's config view" do
    test "never shows a key, and shows a provider as a function reference" do
      config =
        Ankusa.Config.new(secrets: [kek: "k1:" <> @kek_a, kek_previous: [@kek_b]])

      shown = Redact.config(config)

      assert shown["secrets"] == %{
               "kek" => "[REDACTED]",
               "kek_previous" => "[REDACTED]",
               "kek_provider" => nil
             }

      json = JSON.encode!(shown)
      refute json =~ @kek_a
      refute json =~ @kek_b

      provided = Ankusa.Config.new(secrets: [kek_provider: {__MODULE__, :provider, [[]]}])
      assert Redact.config(provided)["secrets"]["kek_provider"] == "Ankusa.CryptoTest.provider/1"
    end
  end
end
