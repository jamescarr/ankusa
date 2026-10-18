defmodule Ankusa.ClaimCheck.RefTest do
  use ExUnit.Case, async: true

  alias Ankusa.ClaimCheck.Ref

  # 2026-09-24T14:03:11Z and the first 8 bytes of sha256("example"), encoded
  # independently of this code: the example ids the docs and OpenAPI use.
  @ms 1_790_258_591_000
  @entropy binary_part(:crypto.hash(:sha256, "example"), 0, 8)
  @pack_id "01M39VMD8RA3C5HR4RBV67Y000"
  @claim_id "01M39VMD8RA3C5HR4RBV67Y002"

  defp urn(tenant, claim_id), do: "urn:ankusa:claim:v1:#{tenant}:#{claim_id}"

  test "pack and claim ids are canonical ULIDs: timestamp, entropy, then position" do
    assert Ref.pack_id(@ms, @entropy) == @pack_id
    assert Ref.claim_id(@pack_id, 2) == @claim_id
  end

  test "parse/1 and to_string/1 round-trip, and the path is the two segments" do
    string = urn("Acme_Corp-1", @claim_id)

    assert {:ok, ref} = Ref.parse(string)
    assert ref == %Ref{tenant_id: "Acme_Corp-1", claim_id: @claim_id}
    assert Ref.to_string(ref) == string
    assert to_string(ref) == string
    assert Ref.path(ref) == "/v1/claims/Acme_Corp-1/#{@claim_id}"
  end

  test "a claim id locates its pack and position at both ends of the 16-bit range" do
    pack_id = Ref.new_pack_id()

    for index <- [0, 1, 0xFFFF] do
      assert Ref.locate(Ref.claim_id(pack_id, index)) == {:ok, pack_id, index}
    end
  end

  test "claim ids in one pack sort in pack order" do
    pack_id = Ref.new_pack_id()
    ids = Enum.map([0, 1, 31, 32, 1023, 1024, 0xFFFF], &Ref.claim_id(pack_id, &1))

    assert Enum.sort(ids) == ids
  end

  test "validate_pack_id/1 rejects an id with position bits set" do
    assert Ref.validate_pack_id(@pack_id) == :ok
    assert Ref.validate_pack_id(@claim_id) == {:error, :invalid_id}
  end

  test "parse/1 rejects anything outside the v1 grammar" do
    for bad <- [
          # tenant: a colon would add a segment, % or / would need encoding
          urn("ac:me", @claim_id),
          urn("ac%2Fme", @claim_id),
          urn("ac/me", @claim_id),
          urn("", @claim_id),
          urn(String.duplicate("a", 65), @claim_id),
          # claim id: canonical ULID only
          urn("acme", String.downcase(@claim_id)),
          urn("acme", String.slice(@claim_id, 0, 25)),
          urn("acme", @claim_id <> "0"),
          urn("acme", "8" <> String.slice(@claim_id, 1, 25)),
          urn("acme", "01M39VMD8RA3C5HR4RBV67YOO2"),
          urn("acme", "01M39VMD8RA3C5HR4RBV67YII2"),
          urn("acme", "01M39VMD8RA3C5HR4RBV67YLL2"),
          urn("acme", "01M39VMD8RA3C5HR4RBV67YUU2"),
          # a timestamp past year 9999 has no dt= partition
          urn("acme", "7ZZZZZZZZZZZZZZZZZZZZZZZZZ"),
          # version and shape
          String.replace(urn("acme", @claim_id), ":v1:", ":v2:"),
          urn("acme", @claim_id) <> ":sha256-" <> String.duplicate("ab", 32),
          "urn:ankusa:claim:v1:acme:0199a1c2-7b3e-7d4a-9c1f-2e5b8a6d4f10:66:3145728:sha256-" <>
            String.duplicate("ab", 32),
          "not a urn"
        ] do
      assert Ref.parse(bad) == {:error, :invalid_ref}, "accepted #{inspect(bad)}"
    end
  end

  # HMAC-SHA256 under @key of "urn:ankusa:claim:v2:acme:<claim>:<exp>", computed
  # independently of this code (Python's hmac/hashlib), base64url, unpadded.
  @key "0123456789abcdef0123456789abcdef"
  @exp 1_790_345_000
  @sig "MAmydaNzNwA3sV38ggZG15ElUTajA8QtBkKevsaq1Ac"
  @v2 "urn:ankusa:claim:v2:acme:#{@claim_id}:#{@exp}:#{@sig}"

  describe "v2 (signed, expiring) refs" do
    test "sign/3 matches an independently computed HMAC, and parse/1 and to_string/1 round-trip" do
      ref = Ref.sign(%Ref{tenant_id: "acme", claim_id: @claim_id}, @key, @exp)

      assert ref.signature == @sig
      assert Ref.to_string(ref) == @v2
      assert {:ok, ^ref} = Ref.parse(@v2)
      assert Ref.path(ref) == "/v1/claims/acme/#{@claim_id}?exp=#{@exp}&sig=#{@sig}"
    end

    test "verify/3 accepts the current key and the previous key, refuses any other" do
      {:ok, ref} = Ref.parse(@v2)
      now = @exp - 10

      assert Ref.verify(ref, [@key], now) == :ok
      assert Ref.verify(ref, ["another-key-of-sixteen-bytes", @key], now) == :ok
      assert Ref.verify(ref, ["another-key-of-sixteen-bytes"], now) == {:error, :bad_signature}
      assert Ref.verify(ref, [], now) == {:error, :bad_signature}
    end

    test "verify/3: the expiry second itself is valid, the next one is not" do
      {:ok, ref} = Ref.parse(@v2)

      assert Ref.verify(ref, [@key], @exp) == :ok
      assert Ref.verify(ref, [@key], @exp + 1) == {:error, :ref_expired}
    end

    test "verify/3: a changed tenant, claim or expiry fails the signature, not the clock" do
      {:ok, ref} = Ref.parse(@v2)

      for tampered <- [
            %{ref | tenant_id: "globex"},
            %{ref | claim_id: Ref.claim_id(@pack_id, 3)},
            %{ref | expires_at: @exp + 1},
            %{ref | signature: String.replace(@sig, "MA", "MB")}
          ] do
        assert Ref.verify(tampered, [@key], @exp + 1_000_000) == {:error, :bad_signature}
      end
    end

    test "verify/3: a v1 ref carries nothing to check" do
      {:ok, ref} = Ref.parse(urn("acme", @claim_id))
      assert Ref.verify(ref, [@key], 1) == {:error, :unsigned_ref}
      assert Ref.path(ref) == "/v1/claims/acme/#{@claim_id}"
    end

    test "signed/4 builds a ref from path and query strings, or says what is wrong" do
      assert {:ok, %Ref{expires_at: @exp, signature: @sig}} =
               Ref.signed("acme", @claim_id, Integer.to_string(@exp), @sig)

      assert Ref.signed("ac me", @claim_id, "1", @sig) == {:error, :invalid_tenant}
      assert Ref.signed("acme", "nope", "1", @sig) == {:error, :invalid_id}

      for {exp, sig} <- [
            {"01", @sig},
            {"0", @sig},
            {"-1", @sig},
            {"1x", @sig},
            {"1234567890123", @sig},
            {"1", String.slice(@sig, 0, 42)},
            {"1", @sig <> "A"},
            {"1", @sig <> "="},
            {"1", "+" <> String.slice(@sig, 1, 42)},
            {nil, @sig},
            {"1", nil}
          ] do
        assert Ref.signed("acme", @claim_id, exp, sig) == {:error, :bad_signature},
               "accepted #{inspect({exp, sig})}"
      end
    end

    test "parse/1 rejects a v2 outside its grammar" do
      for bad <- [
            "urn:ankusa:claim:v2:acme:#{@claim_id}",
            "urn:ankusa:claim:v2:acme:#{@claim_id}:#{@exp}",
            @v2 <> ":x",
            @v2 <> "\n",
            "urn:ankusa:claim:v2:acme:#{@claim_id}:0#{@exp}:#{@sig}",
            "urn:ankusa:claim:v2:acme:#{@claim_id}:#{@exp}:#{String.slice(@sig, 0, 42)}",
            "urn:ankusa:claim:v2:acme:#{@claim_id}:#{@exp}:#{@sig}=",
            "urn:ankusa:claim:v2:acme:#{String.downcase(@claim_id)}:#{@exp}:#{@sig}",
            "urn:ankusa:claim:v2:ac:me:#{@claim_id}:#{@exp}:#{@sig}",
            "urn:ankusa:claim:v3:acme:#{@claim_id}:#{@exp}:#{@sig}"
          ] do
        assert Ref.parse(bad) == {:error, :invalid_ref}, "accepted #{inspect(bad)}"
      end
    end
  end

  test "the key's dt partition is the UTC date of the pack id's timestamp" do
    last_ms_of_day = DateTime.to_unix(~U[2026-09-23 23:59:59.999Z], :millisecond)
    before_midnight = Ref.pack_id(last_ms_of_day, @entropy)
    after_midnight = Ref.pack_id(last_ms_of_day + 1, @entropy)

    assert Ref.object_key("acme", before_midnight, :flat) ==
             "claims/tenant=acme/dt=2026-09-23/#{before_midnight}"

    assert Ref.object_key("acme", after_midnight, :flat) ==
             "claims/tenant=acme/dt=2026-09-24/#{after_midnight}"

    assert Ref.key(%Ref{tenant_id: "acme", claim_id: Ref.claim_id(after_midnight, 7)}, :flat) ==
             Ref.object_key("acme", after_midnight, :flat)
  end

  test "the sharded layout puts shard=<the id's eleventh character> between tenant and dt" do
    sharded = "claims/tenant=acme/shard=A/dt=2026-09-24/#{@pack_id}"
    flat = "claims/tenant=acme/dt=2026-09-24/#{@pack_id}"

    assert Ref.shard(@pack_id) == "A"
    assert Ref.object_key("acme", @pack_id, :sharded) == sharded
    assert Ref.object_key("acme", @pack_id, :flat) == flat
    assert Ref.object_keys("acme", @pack_id, :sharded) == [sharded, flat]
    assert Ref.object_keys("acme", @pack_id, :flat) == [flat, sharded]

    # every claim of the pack shares the pack's shard, so a ref alone finds it
    assert Ref.shard(Ref.claim_id(@pack_id, 0xFFFF)) == "A"
    assert Ref.key(%Ref{tenant_id: "acme", claim_id: @claim_id}, :sharded) == sharded
  end
end
