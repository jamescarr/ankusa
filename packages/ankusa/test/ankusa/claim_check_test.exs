defmodule Ankusa.ClaimCheckTest do
  use ExUnit.Case, async: false

  alias Ankusa.{ClaimCheck, Config, UUIDv7}
  alias Ankusa.ClaimCheck.Ref
  alias Ankusa.Test.CountingBlobStore

  setup do
    inst = :"cc#{System.unique_integer([:positive])}"
    dir = Ankusa.TestHelpers.unique_data_dir(inst)
    on_exit(fn -> File.rm_rf(dir) end)

    config = Config.new(instance: inst, data_dir: dir, roles: [])
    Ankusa.put_config(config)
    %{inst: inst, config: config}
  end

  defp use_store(%{config: config}, store_opts, claim_check \\ %{}) do
    config = %{
      config
      | storage: %{config.storage | blob_store: {CountingBlobStore, [pid: self()] ++ store_opts}},
        claim_check: Map.merge(config.claim_check, claim_check)
    }

    Ankusa.put_config(config)
  end

  defp item(tenant, body), do: %{id: UUIDv7.generate(), tenant_id: tenant, body: body}

  defp puts do
    receive do
      {:blob_put, key} -> [key | puts()]
    after
      0 -> []
    end
  end

  defp redeem(inst, %{ref: ref, sha256: sha256}), do: ClaimCheck.redeem(inst, ref, sha256)

  test "check_in/4 then redeem/3 round-trips every claim in a pack", %{inst: inst} do
    a = item("acme", :crypto.strong_rand_bytes(100_000))
    b = item("acme", "small")
    c = item("acme", "")

    assert {:ok, claims} = ClaimCheck.check_in(inst, "acme", [a, b, c])

    for it <- [a, b, c] do
      assert {:ok, it.body} == redeem(inst, claims[it.id])
      assert claims[it.id].sha256 == Base.encode16(:crypto.hash(:sha256, it.body), case: :lower)
    end

    # a ref's URN string redeems the same way
    assert {:ok, b.body} ==
             ClaimCheck.redeem(inst, Ref.to_string(claims[b.id].ref), claims[b.id].sha256)

    # one pack, claims numbered in input order
    pack_ids =
      for it <- [a, b, c] do
        {:ok, pack_id, index} = Ref.locate(claims[it.id].ref.claim_id)
        {pack_id, index}
      end

    assert [{pack_id, 0}, {pack_id, 1}, {pack_id, 2}] = pack_ids
  end

  test "a digest that doesn't match the bytes fails redeem with :integrity_mismatch", %{
    inst: inst
  } do
    a = item("acme", "original bytes")
    {:ok, claims} = ClaimCheck.check_in(inst, "acme", [a])
    %{ref: ref, sha256: sha256} = claims[a.id]

    {:ok, packed} = Ankusa.BlobStore.get(inst, :claims, Ref.key(ref))
    tampered = :binary.replace(packed, "original", "ORIGINAL")
    :ok = Ankusa.BlobStore.put(inst, :claims, Ref.key(ref), tampered)

    assert {:error, :integrity_mismatch} = ClaimCheck.redeem(inst, ref, sha256)

    :ok = Ankusa.BlobStore.put(inst, :claims, Ref.key(ref), packed)
    assert {:error, :integrity_mismatch} = ClaimCheck.redeem(inst, ref, String.upcase(sha256))
  end

  test "redeem/3 of a pack that was never written is :not_found", %{inst: inst} do
    ref = %Ref{tenant_id: "acme", claim_id: Ref.claim_id(Ref.new_pack_id(), 0)}

    assert {:error, :not_found} = ClaimCheck.redeem(inst, ref, String.duplicate("0", 64))
  end

  test "check_in/4 refuses a pack id with position bits set", %{inst: inst} do
    claim_id = Ref.claim_id(Ref.new_pack_id(), 1)

    assert {:error, :invalid_id} =
             ClaimCheck.check_in(inst, "acme", [item("acme", "b")], pack_id: claim_id)
  end

  test "check_in/4 refuses a tenant outside the grammar", %{inst: inst} do
    assert {:error, :invalid_tenant} = ClaimCheck.check_in(inst, "../etc", [item("x", "b")])
  end

  describe "check_in_batch/2" do
    test "writes one object per tenant, and every ref redeems to its own body", ctx do
      use_store(ctx, [])
      items = [item("acme", "a1"), item("globex", "g1"), item("acme", "a2")]

      results = ClaimCheck.check_in_batch(ctx.inst, items)

      assert length(puts()) == 2

      for it <- items do
        assert {:ok, claim} = results[it.id]
        assert claim.ref.tenant_id == it.tenant_id
        assert {:ok, it.body} == redeem(ctx.inst, claim)
      end
    end

    test "splits a tenant's claims at pack_max_bytes; an oversized body gets its own pack", ctx do
      use_store(ctx, [], %{pack_max_bytes: 5_000})

      small = for _ <- 1..3, do: item("acme", :crypto.strong_rand_bytes(2_000))
      huge = item("acme", :crypto.strong_rand_bytes(20_000))

      results = ClaimCheck.check_in_batch(ctx.inst, small ++ [huge])

      # 2 KB claims with overhead: two fit under 5 KB, the third starts a pack,
      # and the 20 KB body can't share one.
      assert length(puts()) == 3

      packs =
        results
        |> Map.values()
        |> Enum.map(fn {:ok, claim} -> elem(Ref.locate(claim.ref.claim_id), 1) end)

      assert packs |> Enum.uniq() |> length() == 3

      for it <- small ++ [huge] do
        assert {:ok, it.body} == redeem(ctx.inst, elem(results[it.id], 1))
      end
    end

    test "a failed pack fails only its own claims", ctx do
      use_store(ctx, fail_tenant: "globex")
      acme = item("acme", "ok")
      globex = item("globex", "lost")

      results = ClaimCheck.check_in_batch(ctx.inst, [acme, globex])

      assert {:ok, _ref} = results[acme.id]
      assert {:error, {:unavailable, :injected_failure}} = results[globex.id]
    end
  end

  describe "validate_config!/1" do
    test "rejects a non-positive pack_max_bytes" do
      for bad <- [0, -1, 1.5] do
        config = Config.new(claim_check: %{pack_max_bytes: bad})

        assert_raise ArgumentError, ~r/pack_max_bytes must be a positive integer/, fn ->
          ClaimCheck.validate_config!(config)
        end
      end
    end

    test "rejects retention_days against a non-LocalFS blob store" do
      config =
        Config.new(
          storage: %{blob_store: {Ankusa.BlobStore.S3, bucket: "b", region: "us-east-1"}},
          claim_check: %{retention_days: 7}
        )

      assert_raise ArgumentError, ~r/bucket lifecycle rule/, fn ->
        ClaimCheck.validate_config!(config)
      end
    end

    test "rejects a max_body_bytes a pack can't hold" do
      config = Config.new(max_body_bytes: 0xFFFFFFFF)

      assert_raise ArgumentError, ~r/under 4 GiB/, fn ->
        ClaimCheck.validate_config!(config)
      end
    end

    test "accepts the defaults and LocalFS retention" do
      assert :ok = ClaimCheck.validate_config!(Config.new(claim_check: %{retention_days: 7}))
    end

    test "rejects a retention of less than a day, and a non-positive sweep interval" do
      assert_raise ArgumentError,
                   "claim_check.retention_days must be nil or an integer >= 1, got 0",
                   fn ->
                     ClaimCheck.validate_config!(Config.new(claim_check: %{retention_days: 0}))
                   end

      assert_raise ArgumentError,
                   ~r/claim_check.sweep_interval_ms must be a positive integer/,
                   fn ->
                     ClaimCheck.validate_config!(Config.new(claim_check: %{sweep_interval_ms: 0}))
                   end
    end

    test "retention is judged against the claim store, not the segment store" do
      s3 = {Ankusa.BlobStore.S3, bucket: "b", region: "us-east-1"}

      dedicated_local =
        Config.new(
          storage: %{blob_store: s3},
          claim_check: %{retention_days: 7, blob_store: {Ankusa.BlobStore.LocalFS, root: "/x"}}
        )

      assert :ok = ClaimCheck.validate_config!(dedicated_local)

      dedicated_s3 = Config.new(claim_check: %{retention_days: 7, blob_store: s3})

      assert_raise ArgumentError, ~r/bucket lifecycle rule/, fn ->
        ClaimCheck.validate_config!(dedicated_s3)
      end
    end
  end

  describe "a dedicated claim store" do
    test "packs land there, and read/3 reads them back", %{inst: inst} do
      claims_root =
        Path.join(System.tmp_dir!(), "ankusa_claims_#{System.unique_integer([:positive])}")

      on_exit(fn -> File.rm_rf(claims_root) end)

      config = Ankusa.config(inst)

      Ankusa.put_config(%{
        config
        | claim_check: %{
            config.claim_check
            | blob_store: {Ankusa.BlobStore.LocalFS, root: claims_root}
          }
      })

      a = item("acme", "dedicated bytes")
      {:ok, claims} = ClaimCheck.check_in(inst, "acme", [a])
      %{ref: ref} = claims[a.id]

      assert File.exists?(Path.join(claims_root, Ref.key(ref)))
      refute File.exists?(Path.join(Config.path(config, "segments"), Ref.key(ref)))
      assert {:ok, "dedicated bytes"} = ClaimCheck.read(inst, "acme", ref.claim_id)
    end
  end

  test "the removed claim_check keys fail at Config.new/1" do
    for key <- [:api_tokens, :adapter, :max_bytes] do
      assert_raise ArgumentError, ~r/unknown Ankusa.Config key: claim_check.#{key}/, fn ->
        Config.new(claim_check: %{key => nil})
      end
    end
  end
end
