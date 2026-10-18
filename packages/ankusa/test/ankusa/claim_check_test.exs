defmodule Ankusa.ClaimCheckTest do
  use ExUnit.Case, async: false

  alias Ankusa.BlobStore.LocalFS
  alias Ankusa.{ClaimCheck, Config, UUIDv7}
  alias Ankusa.ClaimCheck.{Ref, Stores}
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

  # One `CountingBlobStore` claim store per entry of `stores_opts`, each on a
  # tmp root of its own unless the entry names one. Returns the roots.
  defp use_stores(%{config: config}, stores_opts, claim_check \\ %{}) do
    stores =
      for opts <- stores_opts do
        {root, opts} =
          Keyword.pop_lazy(opts, :root, fn ->
            root =
              Path.join(System.tmp_dir!(), "ankusa_claims_#{System.unique_integer([:positive])}")

            on_exit(fn -> File.rm_rf(root) end)
            root
          end)

        {CountingBlobStore, [pid: self(), root: root] ++ opts}
      end

    cc = config.claim_check |> Map.merge(claim_check) |> Map.put(:blob_stores, stores)
    Ankusa.put_config(%{config | claim_check: cc})
    Enum.map(stores, fn {_mod, opts} -> opts[:root] end)
  end

  # Telemetry `events` of this test's instance, as `{:telemetry, event, meta}`.
  defp attach(inst, events) do
    handler = "claim-check-test-#{System.unique_integer([:positive])}"
    test = self()

    :ok =
      :telemetry.attach_many(
        handler,
        events,
        fn event, _measurements, meta, _config ->
          if meta.instance == inst, do: send(test, {:telemetry, event, meta})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)
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

    [{0, mod, opts}] = Stores.list(Ankusa.config(inst))
    key = Ref.key(ref, :flat)

    {:ok, packed} = mod.get(inst, key, opts)
    tampered = :binary.replace(packed, "original", "ORIGINAL")
    :ok = mod.put(inst, key, tampered, opts)

    assert {:error, :integrity_mismatch} = ClaimCheck.redeem(inst, ref, sha256)

    :ok = mod.put(inst, key, packed, opts)
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

    test "retention is judged against the claim stores, not the segment store" do
      s3 = {Ankusa.BlobStore.S3, bucket: "b", region: "us-east-1"}

      dedicated_local =
        Config.new(
          storage: %{blob_store: s3},
          claim_check: %{retention_days: 7, blob_stores: [{LocalFS, root: "/x"}]}
        )

      assert :ok = ClaimCheck.validate_config!(dedicated_local)

      dedicated_s3 = Config.new(claim_check: %{retention_days: 7, blob_stores: [s3]})

      assert_raise ArgumentError, ~r/bucket lifecycle rule/, fn ->
        ClaimCheck.validate_config!(dedicated_s3)
      end

      # one non-LocalFS store among several is enough to refuse
      mixed =
        Config.new(claim_check: %{retention_days: 7, blob_stores: [s3, {LocalFS, root: "/x"}]})

      assert_raise ArgumentError,
                   ~r/claim store is Ankusa.BlobStore.S3 .* bucket lifecycle rule/,
                   fn ->
                     ClaimCheck.validate_config!(mixed)
                   end
    end

    test "rejects a malformed blob_stores, placement or key_layout" do
      for bad <- [[], {LocalFS, []}, [LocalFS], [{LocalFS, %{}}], [{"LocalFS", []}]] do
        assert_raise ArgumentError,
                     "claim_check.blob_stores must be nil or a non-empty list of {module, opts}, " <>
                       "got #{inspect(bad)}",
                     fn ->
                       ClaimCheck.validate_config!(Config.new(claim_check: %{blob_stores: bad}))
                     end
      end

      assert_raise ArgumentError,
                   "claim_check.placement must be :ordered or :hash, got :random",
                   fn ->
                     ClaimCheck.validate_config!(Config.new(claim_check: %{placement: :random}))
                   end

      assert_raise ArgumentError,
                   "claim_check.key_layout must be :flat or :sharded, got :hashed",
                   fn ->
                     ClaimCheck.validate_config!(Config.new(claim_check: %{key_layout: :hashed}))
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
            | blob_stores: [{LocalFS, root: claims_root}]
          }
      })

      a = item("acme", "dedicated bytes")
      {:ok, claims} = ClaimCheck.check_in(inst, "acme", [a])
      %{ref: ref} = claims[a.id]

      assert File.exists?(Path.join(claims_root, Ref.key(ref, :flat)))
      refute File.exists?(Path.join(Config.path(config, "segments"), Ref.key(ref, :flat)))
      assert {:ok, "dedicated bytes"} = ClaimCheck.read(inst, "acme", ref.claim_id)
    end
  end

  describe "stores" do
    test ":ordered writes to the first store, and to the next while it fails", ctx do
      attach(ctx.inst, [[:ankusa, :claim_check, :check_in]])
      [root_0, root_1] = use_stores(ctx, [[fail_tenant: "acme"], []])
      a = item("acme", "failed over")

      assert {:ok, claims} = ClaimCheck.check_in(ctx.inst, "acme", [a])
      key = Ref.key(claims[a.id].ref, :flat)

      refute File.exists?(Path.join(root_0, key))
      assert File.exists?(Path.join(root_1, key))
      assert {:ok, "failed over"} = redeem(ctx.inst, claims[a.id])
      assert_received {:telemetry, [:ankusa, :claim_check, :check_in], %{store: 1, result: :ok}}
    end

    test ":hash spreads packs by the pack id's random bits, and each redeems", ctx do
      roots = use_stores(ctx, [[], []], %{placement: :hash})
      ms = System.system_time(:millisecond)

      for {entropy, home} <- [{<<0::64>>, 0}, {<<1::64>>, 1}] do
        pack_id = Ref.pack_id(ms, entropy)
        a = item("acme", "home #{home}")

        {:ok, claims} = ClaimCheck.check_in(ctx.inst, "acme", [a], pack_id: pack_id)
        key = Ref.object_key("acme", pack_id, :flat)

        for {root, index} <- Enum.with_index(roots) do
          assert File.exists?(Path.join(root, key)) == (index == home)
        end

        assert {:ok, a.body} == redeem(ctx.inst, claims[a.id])
      end
    end

    @tag :capture_log
    test "a failed store is skipped for store_cooldown_ms, then tried again", ctx do
      start_supervised!({Stores, instance: ctx.inst})
      attach(ctx.inst, [[:ankusa, :claim_check, :store_breaker]])
      {:ok, failures} = Agent.start_link(fn -> 1 end)
      # long enough to outlast store 1's durable (fsynced) write of the first pack
      [root_0, root_1] = use_stores(ctx, [[failures: failures], []], %{store_cooldown_ms: 500})

      # store 0 fails, store 1 takes the pack, store 0's breaker opens
      assert {:ok, _claims} = ClaimCheck.check_in(ctx.inst, "acme", [item("acme", "1")])
      assert length(puts()) == 2
      assert_received {:telemetry, _event, %{store: 0, state: :open}}

      # inside the cooldown store 0 is not even tried
      b = item("acme", "2")
      assert {:ok, claims} = ClaimCheck.check_in(ctx.inst, "acme", [b])
      assert length(puts()) == 1
      assert File.exists?(Path.join(root_1, Ref.key(claims[b.id].ref, :flat)))

      # after it, store 0 is tried again, takes the pack, and its breaker closes
      Process.sleep(550)
      c = item("acme", "3")
      assert {:ok, claims} = ClaimCheck.check_in(ctx.inst, "acme", [c])
      assert length(puts()) == 1
      assert File.exists?(Path.join(root_0, Ref.key(claims[c.id].ref, :flat)))
      assert_received {:telemetry, _event, %{store: 0, state: :closed}}
      refute_received {:telemetry, _event, %{state: :open}}
    end

    test "every store failing is :unavailable with each store's reason, in attempt order", ctx do
      use_stores(ctx, [[fail_tenant: "acme"], [fail_tenant: "acme"]])

      assert {:error, {:unavailable, [{0, :injected_failure}, {1, :injected_failure}]}} =
               ClaimCheck.check_in(ctx.inst, "acme", [item("acme", "nowhere")])

      assert length(puts()) == 2
    end

    test "a miss is :not_found only when every store answered", ctx do
      ref = %Ref{tenant_id: "acme", claim_id: Ref.claim_id(Ref.new_pack_id(), 0)}
      digest = String.duplicate("0", 64)

      use_stores(ctx, [[range_error: :econnrefused], []])
      assert {:error, {:unavailable, :econnrefused}} = ClaimCheck.redeem(ctx.inst, ref, digest)

      use_stores(ctx, [[], []])
      assert {:error, :not_found} = ClaimCheck.redeem(ctx.inst, ref, digest)
    end

    test "a read searches past a store that cannot answer to the one holding the pack", ctx do
      [root_0, root_1] = use_stores(ctx, [[], []], %{placement: :hash})
      pack_id = Ref.pack_id(System.system_time(:millisecond), <<1::64>>)
      a = item("acme", "in store 1")
      {:ok, claims} = ClaimCheck.check_in(ctx.inst, "acme", [a], pack_id: pack_id)

      # the pack's home is now store 0, which errors on every read
      use_stores(ctx, [[root: root_0, range_error: :econnrefused], [root: root_1]], %{
        placement: :ordered
      })

      assert {:ok, "in store 1"} = redeem(ctx.inst, claims[a.id])
    end

    @tag :capture_log
    test "a store that cannot answer a read is searched last, but still searched", ctx do
      start_supervised!({Stores, instance: ctx.inst})
      attach(ctx.inst, [[:ankusa, :claim_check, :store_breaker]])
      [root_0, root_1] = use_stores(ctx, [[], []], %{placement: :hash})
      pack_id = Ref.pack_id(System.system_time(:millisecond), <<1::64>>)
      a = item("acme", "in store 1")
      {:ok, claims} = ClaimCheck.check_in(ctx.inst, "acme", [a], pack_id: pack_id)

      # Store 0 is the pack's home now, and cannot answer a read.
      use_stores(
        ctx,
        [
          [root: root_0, range_error: :econnrefused, notify_range: {self(), 0}],
          [root: root_1, notify_range: {self(), 1}]
        ],
        %{placement: :ordered}
      )

      ranges = fn ->
        for _ <- 1..3,
            do:
              (receive do
                 {:blob_range, s} -> s
               after
                 0 -> nil
               end)
      end

      # The first read pays for store 0 and opens its breaker...
      assert {:ok, "in store 1"} = redeem(ctx.inst, claims[a.id])
      assert ranges.() == [0, 1, 1]
      assert_received {:telemetry, _event, %{store: 0, state: :open, reason: :econnrefused}}

      # ...so the next goes to store 1 first, and store 0 is never reached.
      assert {:ok, "in store 1"} = redeem(ctx.inst, claims[a.id])
      assert ranges.() == [1, 1, nil]

      # A store that is down is still searched: a miss is not :not_found.
      ref = %Ref{tenant_id: "acme", claim_id: Ref.claim_id(Ref.new_pack_id(), 0)}

      assert {:error, {:unavailable, :econnrefused}} =
               ClaimCheck.redeem(ctx.inst, ref, String.duplicate("0", 64))
    end

    @tag :capture_log
    test "a throttled read never opens the store's breaker; a 500 does", ctx do
      start_supervised!({Stores, instance: ctx.inst})
      attach(ctx.inst, [[:ankusa, :claim_check, :store_breaker]])
      ref = %Ref{tenant_id: "acme", claim_id: Ref.claim_id(Ref.new_pack_id(), 0)}
      digest = String.duplicate("0", 64)

      use_stores(ctx, [[range_error: {:status, 503, "SlowDown"}]])

      assert {:error, {:unavailable, {:status, 503, "SlowDown"}}} =
               ClaimCheck.redeem(ctx.inst, ref, digest)

      refute_received {:telemetry, [:ankusa, :claim_check, :store_breaker], _meta}

      use_stores(ctx, [[range_error: {:status, 500, "boom"}]])

      assert {:error, {:unavailable, {:status, 500, "boom"}}} =
               ClaimCheck.redeem(ctx.inst, ref, digest)

      assert_received {:telemetry, _event, %{store: 0, state: :open}}
    end

    @tag :capture_log
    test "a throttled read is an answer: it closes an open breaker", ctx do
      start_supervised!({Stores, instance: ctx.inst})
      attach(ctx.inst, [[:ankusa, :claim_check, :store_breaker]])
      {:ok, failures} = Agent.start_link(fn -> 1 end)
      {:ok, throttles} = Agent.start_link(fn -> 1 end)
      use_stores(ctx, [[failures: failures, range_failures: throttles], []])

      assert {:ok, _claims} = ClaimCheck.check_in(ctx.inst, "acme", [item("acme", "1")])
      assert_received {:telemetry, _event, %{store: 0, state: :open}}

      ref = %Ref{tenant_id: "acme", claim_id: Ref.claim_id(Ref.new_pack_id(), 0)}

      assert {:error, {:unavailable, {:status, 503, "SlowDown"}}} =
               ClaimCheck.redeem(ctx.inst, ref, String.duplicate("0", 64))

      assert_received {:telemetry, _event, %{store: 0, state: :closed}}
    end

    @tag :capture_log
    test "a store that could not answer is not asked again at the other layout; a 403 is", ctx do
      start_supervised!({Stores, instance: ctx.inst})
      [root_0, root_1] = use_stores(ctx, [[], []], %{placement: :hash, key_layout: :sharded})
      pack_id = Ref.pack_id(System.system_time(:millisecond), <<1::64>>)
      a = item("acme", "in store 1, sharded")
      {:ok, claims} = ClaimCheck.check_in(ctx.inst, "acme", [a], pack_id: pack_id)

      ranges = fn ->
        Stream.repeatedly(fn ->
          receive do
            {:blob_range, s} -> s
          after
            0 -> nil
          end
        end)
        |> Enum.take_while(&(&1 != nil))
      end

      stores = fn store_0 ->
        use_stores(
          ctx,
          [
            [root: root_0, notify_range: {self(), 0}] ++ store_0,
            [root: root_1, notify_range: {self(), 1}]
          ],
          %{placement: :ordered, key_layout: :flat}
        )
      end

      # Store 0 is the home now and refuses the credential: it is asked at
      # both layouts before the pack is found in store 1 at the other one.
      stores.(range_error: {:status, 403, "denied"})
      assert {:ok, "in store 1, sharded"} = redeem(ctx.inst, claims[a.id])
      assert ranges.() == [0, 1, 0, 1, 1]

      # Unreachable, it is asked once, and its breaker opens...
      stores.(range_error: :econnrefused)
      assert {:ok, "in store 1, sharded"} = redeem(ctx.inst, claims[a.id])
      assert ranges.() == [0, 1, 1, 1]

      # ...so the next read asks the healthy store at both layouts first.
      assert {:ok, "in store 1, sharded"} = redeem(ctx.inst, claims[a.id])
      assert ranges.() == [1, 1, 1]
    end

    test "a pack written under one key layout is read under the other", ctx do
      for {written, configured} <- [{:flat, :sharded}, {:sharded, :flat}] do
        [root] = use_stores(ctx, [[]], %{key_layout: written})
        a = item("acme", "written #{written}")
        {:ok, claims} = ClaimCheck.check_in(ctx.inst, "acme", [a])

        assert File.exists?(Path.join(root, Ref.key(claims[a.id].ref, written)))
        refute File.exists?(Path.join(root, Ref.key(claims[a.id].ref, configured)))

        use_stores(ctx, [[root: root]], %{key_layout: configured})
        assert {:ok, a.body} == redeem(ctx.inst, claims[a.id])
      end
    end
  end

  test "the removed claim_check keys fail at Config.new/1" do
    for key <- [:api_tokens, :adapter, :max_bytes, :blob_store] do
      assert_raise ArgumentError, ~r/unknown Ankusa.Config key: claim_check.#{key}/, fn ->
        Config.new(claim_check: %{key => nil})
      end
    end
  end
end
