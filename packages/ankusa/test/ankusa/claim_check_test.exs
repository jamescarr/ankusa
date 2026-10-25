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

  describe "signed refs" do
    @key "claim-check-test-signing-key-0123"
    @previous "claim-check-test-previous-key-4567"

    defp sign_with(%{config: config}, keys, claim_check \\ %{}) do
      cc = config.claim_check |> Map.merge(claim_check) |> Map.put(:ref_signing_keys, keys)
      Ankusa.put_config(%{config | claim_check: cc})
    end

    test "without keys, check_in/4 mints v1 refs, and redeem/3 serves them", %{inst: inst} do
      a = item("acme", "plain")
      assert {:ok, [claim]} = checked_in(inst, [a])

      assert %Ref{signature: nil, expires_at: nil} = claim.ref
      assert Ref.to_string(claim.ref) =~ ":v1:"
      assert {:ok, "plain"} = redeem(inst, claim)
    end

    test "with keys, check_in/4 mints v2 refs that expire ref_ttl_ms from now", ctx do
      sign_with(ctx, [@key, @previous], %{ref_ttl_ms: 90_000})
      before = System.system_time(:second)
      assert {:ok, [claim]} = checked_in(ctx.inst, [item("acme", "signed")])

      assert %Ref{signature: signature, expires_at: exp} = claim.ref
      assert byte_size(signature) == 43
      assert exp in (before + 90)..(System.system_time(:second) + 91)
      assert Ref.to_string(claim.ref) =~ ":v2:acme:"
      assert Ref.verify(claim.ref, [@key], before) == :ok
      assert Ref.verify(claim.ref, [@previous], before) == {:error, :bad_signature}
      assert {:ok, "signed"} = redeem(ctx.inst, claim)
    end

    test "the TTL is the retention when ref_ttl_ms is nil, else 30 days", ctx do
      sign_with(ctx, [@key], %{retention_days: 2})
      {:ok, [two_days]} = checked_in(ctx.inst, [item("acme", "a")])
      assert_in_delta two_days.ref.expires_at, System.system_time(:second) + 2 * 86_400, 2

      sign_with(ctx, [@key], %{retention_days: nil})
      {:ok, [default]} = checked_in(ctx.inst, [item("acme", "b")])
      assert_in_delta default.ref.expires_at, System.system_time(:second) + 30 * 86_400, 2
    end

    test "redeem/3 refuses an expired, forged or foreign-signed ref before reading", ctx do
      sign_with(ctx, [@key])
      {:ok, [claim]} = checked_in(ctx.inst, [item("acme", "guarded")])
      bare = %Ref{tenant_id: "acme", claim_id: claim.ref.claim_id}
      now = System.system_time(:second)

      expired = Ref.sign(bare, @key, now - 1)
      foreign = Ref.sign(bare, "a-key-nobody-configured-0000", now + 60)
      forged = %{claim.ref | expires_at: claim.ref.expires_at + 1}

      assert {:error, :ref_expired} = ClaimCheck.redeem(ctx.inst, expired, claim.sha256)
      assert {:error, :bad_signature} = ClaimCheck.redeem(ctx.inst, foreign, claim.sha256)
      assert {:error, :bad_signature} = ClaimCheck.redeem(ctx.inst, forged, claim.sha256)

      # the URN a queue message carries goes through the same check
      urn = Ref.to_string(expired)
      assert {:error, :ref_expired} = ClaimCheck.redeem(ctx.inst, urn, claim.sha256)
    end

    test "a v1 ref is refused once keys are set, unless accept_unsigned_refs says so", ctx do
      {:ok, [claim]} = checked_in(ctx.inst, [item("acme", "old ref")])
      assert {:ok, "old ref"} = redeem(ctx.inst, claim)

      sign_with(ctx, [@key])
      assert {:error, :unsigned_ref} = redeem(ctx.inst, claim)

      sign_with(ctx, [@key], %{accept_unsigned_refs: true})
      assert {:ok, "old ref"} = redeem(ctx.inst, claim)

      # explicit false refuses v1 even without keys' default saying so
      sign_with(ctx, [@key], %{accept_unsigned_refs: false})
      assert {:error, :unsigned_ref} = redeem(ctx.inst, claim)
    end

    test "a previous key still verifies after the rotation", ctx do
      sign_with(ctx, [@previous])
      {:ok, [claim]} = checked_in(ctx.inst, [item("acme", "rotated")])

      sign_with(ctx, [@key, @previous])
      assert {:ok, "rotated"} = redeem(ctx.inst, claim)

      sign_with(ctx, [@key])
      assert {:error, :bad_signature} = redeem(ctx.inst, claim)
    end

    defp checked_in(inst, items) do
      results = ClaimCheck.check_in_batch(inst, items)
      {:ok, for(item <- items, do: ok!(results[item.id]))}
    end

    defp ok!({:ok, claim}), do: claim
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

    test "bucket_expiry_days must outlast retry horizon + DLQ retention + redeem grace" do
      s3 = {Ankusa.BlobStore.S3, bucket: "b", region: "us-east-1"}

      config = fn days, extra ->
        Config.new(
          storage: %{blob_store: s3},
          claim_check: Map.merge(%{bucket_expiry_days: days}, extra)
        )
      end

      # 1 (the default 24 h retry horizon) + 30 (queue.dlq.retention_days) + 1 (24 h grace)
      assert_raise ArgumentError,
                   ~r/bucket_expiry_days is 31 but a claim can be needed for 32 days/,
                   fn -> ClaimCheck.validate_config!(config.(31, %{})) end

      assert :ok = ClaimCheck.validate_config!(config.(32, %{}))

      # A longer grace raises the floor: 2 days of grace make it 33.
      assert_raise ArgumentError,
                   ~r/for 33 days/,
                   fn ->
                     ClaimCheck.validate_config!(config.(32, %{redeem_grace_ms: 172_800_000}))
                   end

      # A LocalFS claim store has leases instead: the key is not consulted.
      assert :ok = ClaimCheck.validate_config!(Config.new(claim_check: %{bucket_expiry_days: 1}))
    end

    @sqs {Ankusa.Sink.SQS,
          queue_url: "https://sqs.us-east-1.amazonaws.com/1/q", region: "us-east-1"}
    @day_ms 86_400_000

    test "the horizon is the longest of the global and every claim-using source's max_elapsed_ms" do
      s3 = {Ankusa.BlobStore.S3, bucket: "b", region: "us-east-1"}

      config = fn days, dispatch, sources ->
        Config.new(
          storage: %{blob_store: s3},
          dispatch: dispatch,
          claim_check: %{bucket_expiry_days: days},
          source_store: {Ankusa.SourceStore.Static, sources: sources}
        )
      end

      long = %{"long" => [sinks: [@sqs], retry: [max_elapsed_ms: 10 * @day_ms]]}

      # global 1 day, but a configured claim-using source retries for 10: 10 + 30 + 1 = 41
      assert_raise ArgumentError,
                   ~r/bucket_expiry_days is 40 .* 41 days \(retry horizon 10 \+/,
                   fn ->
                     ClaimCheck.validate_config!(config.(40, %{}, long))
                   end

      assert :ok = ClaimCheck.validate_config!(config.(41, %{}, long))

      # a source with no claim-checking sink does not count (Log has no inline threshold)
      quiet = %{
        "quiet" => [sinks: [{Ankusa.Sink.Log, []}], retry: [max_elapsed_ms: 10 * @day_ms]]
      }

      assert :ok = ClaimCheck.validate_config!(config.(32, %{}, quiet))

      # the global horizon counts whatever the sources are: 7 + 30 + 1 = 38
      global = %{retry: {Ankusa.RetryPolicy.Exponential, [max_elapsed_ms: 7 * @day_ms]}}

      assert_raise ArgumentError, ~r/bucket_expiry_days is 37 .* 38 days/, fn ->
        ClaimCheck.validate_config!(config.(37, global, %{}))
      end

      assert :ok = ClaimCheck.validate_config!(config.(38, global, %{}))
    end

    test "check_retry_horizon/2 refuses a source that outlives the bucket rule" do
      s3 = {Ankusa.BlobStore.S3, bucket: "b", region: "us-east-1"}

      config = fn days ->
        Config.new(storage: %{blob_store: s3}, claim_check: %{bucket_expiry_days: days})
      end

      source = fn sinks, retry ->
        Ankusa.Source.new("acme.hook", sinks: sinks, retry: retry)
      end

      long = source.([@sqs], max_elapsed_ms: 10 * @day_ms)

      assert {:error, message} = ClaimCheck.check_retry_horizon(config.(40), long)
      assert message =~ "source acme.hook"
      assert message =~ "claim_check.bucket_expiry_days is 40"
      assert message =~ "retry.max_elapsed_ms"

      assert :ok = ClaimCheck.check_retry_horizon(config.(41), long)
      # no override: the global horizon, already checked at boot
      assert :ok = ClaimCheck.check_retry_horizon(config.(40), source.([@sqs], nil))
      # no claim-checking sink: nothing to expire
      assert :ok =
               ClaimCheck.check_retry_horizon(
                 config.(1),
                 source.([{Ankusa.Sink.Log, []}], max_elapsed_ms: 10 * @day_ms)
               )

      # undeclared bucket rule: boot warns, a write is not refused
      assert :ok = ClaimCheck.check_retry_horizon(config.(nil), long)
      # LocalFS claims have leases, not a bucket rule
      assert :ok =
               ClaimCheck.check_retry_horizon(
                 Config.new(claim_check: %{bucket_expiry_days: 1}),
                 long
               )
    end

    test "validate_stored_horizons!/2 refuses a stored source that outlives the bucket rule" do
      s3 = {Ankusa.BlobStore.S3, bucket: "b", region: "us-east-1"}
      config = Config.new(storage: %{blob_store: s3}, claim_check: %{bucket_expiry_days: 40})
      fine = Ankusa.Source.new("acme.fine", sinks: [@sqs])
      long = Ankusa.Source.new("acme.long", sinks: [@sqs], retry: [max_elapsed_ms: 10 * @day_ms])

      assert :ok = ClaimCheck.validate_stored_horizons!(config, [fine])

      assert_raise ArgumentError, ~r/source acme.long: its retry horizon \(10 days\)/, fn ->
        ClaimCheck.validate_stored_horizons!(config, [fine, long])
      end
    end

    @tag :capture_log
    test "an undeclared bucket expiry boots with a warning that names the minimum" do
      s3 = {Ankusa.BlobStore.S3, bucket: "b", region: "us-east-1"}

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert :ok =
                   ClaimCheck.validate_config!(
                     Config.new(roles: [:dispatch], storage: %{blob_store: s3})
                   )
        end)

      assert log =~ "claim_check.bucket_expiry_days is not declared"
      assert log =~ "no earlier than 32 days"
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

    test "ref_signing_keys: a list of strings of at least 16 bytes; the error never echoes one" do
      good = String.duplicate("k", 16)

      assert :ok =
               ClaimCheck.validate_config!(Config.new(claim_check: %{ref_signing_keys: [good]}))

      for bad <- [["short"], [good, 42], "not-a-list", [nil]] do
        error =
          assert_raise ArgumentError, ~r/ref_signing_keys must be a list of strings/, fn ->
            ClaimCheck.validate_config!(Config.new(claim_check: %{ref_signing_keys: bad}))
          end

        refute error.message =~ "short"
      end
    end

    test "accept_unsigned_refs: false needs a key, or no ref could be minted or accepted" do
      key = String.duplicate("k", 16)

      assert_raise ArgumentError, ~r/accept_unsigned_refs is false but .* is empty/, fn ->
        ClaimCheck.validate_config!(Config.new(claim_check: %{accept_unsigned_refs: false}))
      end

      assert :ok =
               ClaimCheck.validate_config!(
                 Config.new(claim_check: %{accept_unsigned_refs: false, ref_signing_keys: [key]})
               )
    end

    test "authorize: nil or an exported {module, fun, args}" do
      assert :ok = ClaimCheck.validate_config!(Config.new(claim_check: %{authorize: nil}))

      assert :ok =
               ClaimCheck.validate_config!(
                 Config.new(claim_check: %{authorize: {__MODULE__, :allow_all, [:extra]}})
               )

      for bad <- [
            {__MODULE__, :missing, []},
            # arity counts the context argument
            {__MODULE__, :allow_all, []},
            {NoSuchModule, :allow, []}
          ] do
        assert_raise ArgumentError, ~r/claim_check.authorize .* is not defined/, fn ->
          ClaimCheck.validate_config!(Config.new(claim_check: %{authorize: bad}))
        end
      end

      for bad <- [:yes, {__MODULE__, :allow_all}, {"m", :f, []}, {__MODULE__, :allow_all, :x}] do
        assert_raise ArgumentError, ~r/claim_check.authorize must be nil or/, fn ->
          ClaimCheck.validate_config!(Config.new(claim_check: %{authorize: bad}))
        end
      end
    end

    def allow_all(_context, _extra), do: :ok
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

  # A claim store that says every key is taken, counting the writes it refused.
  defmodule TakenStore do
    @moduledoc false
    @behaviour Ankusa.BlobStore

    @impl true
    def put(_instance, _key, _data, _opts), do: :ok
    @impl true
    def put_if_absent(_instance, key, _data, opts) do
      send(Keyword.fetch!(opts, :pid), {:refused, key})
      {:error, :already_exists}
    end

    @impl true
    def get(_instance, _key, _opts), do: {:error, :not_found}
    @impl true
    def get_range(_instance, _key, _offset, _length, _opts), do: {:error, :not_found}
    @impl true
    def delete(_instance, _key, _opts), do: :ok
    @impl true
    def list(_instance, _prefix, _opts), do: {:ok, []}
  end

  describe "conditional pack writes" do
    test "a pack id that already holds an object gets a fresh id, and the original stays intact",
         ctx do
      attach(ctx.inst, [
        [:ankusa, :claim_check, :pack_collision],
        [:ankusa, :claim_check, :check_in]
      ])

      [root] = use_stores(ctx, [[]])
      pack_id = Ref.new_pack_id()

      first = item("acme", "the original")
      assert {:ok, claims_1} = ClaimCheck.check_in(ctx.inst, "acme", [first], pack_id: pack_id)
      original_path = Path.join(root, Ref.object_key("acme", pack_id, :flat))
      original = File.read!(original_path)

      second = item("acme", "a different body")
      assert {:ok, claims_2} = ClaimCheck.check_in(ctx.inst, "acme", [second], pack_id: pack_id)

      # The object under the colliding id is byte for byte what it was.
      assert File.read!(original_path) == original
      assert {:ok, "the original"} = redeem(ctx.inst, claims_1[first.id])

      # The second claim went under another id, and redeems.
      assert {:ok, new_pack_id, 0} = Ref.locate(claims_2[second.id].ref.claim_id)
      refute new_pack_id == pack_id
      assert {:ok, "a different body"} = redeem(ctx.inst, claims_2[second.id])

      assert_received {:telemetry, [:ankusa, :claim_check, :pack_collision], %{pack_id: ^pack_id}}

      assert_received {:telemetry, [:ankusa, :claim_check, :check_in],
                       %{pack_id: ^new_pack_id, result: :ok}}
    end

    test "an id that collides on every attempt is :pack_id_collision after three", ctx do
      config = Ankusa.config(ctx.inst)

      Ankusa.put_config(%{
        config
        | claim_check: Map.put(config.claim_check, :blob_stores, [{TakenStore, pid: self()}])
      })

      attach(ctx.inst, [[:ankusa, :claim_check, :failure]])

      assert {:error, {:unavailable, :pack_id_collision}} =
               ClaimCheck.check_in(ctx.inst, "acme", [item("acme", "never lands")])

      # A store that cannot take the pack is the node's to fix: counted by op.
      assert_received {:telemetry, [:ankusa, :claim_check, :failure],
                       %{op: :check_in, tenant_id: "acme", reason: {:unavailable, _}}}

      keys =
        for _attempt <- 1..3 do
          assert_received {:refused, key}
          key
        end

      assert length(Enum.uniq(keys)) == 3
      refute_received {:refused, _}
    end

    # An S3 claim store answering through a stub.
    defp use_s3_stub(ctx, tag, extra) do
      config = Ankusa.config(ctx.inst)
      store = Ankusa.Test.BlobStoreAdapters.opts(Ankusa.BlobStore.S3, tag, __MODULE__, extra)

      Ankusa.put_config(%{
        config
        | claim_check: Map.put(config.claim_check, :blob_stores, [store])
      })
    end

    test "a Retry-After that does not fit the deadline is returned at once, not slept", ctx do
      Req.Test.stub(__MODULE__, fn conn ->
        conn
        |> Plug.Conn.put_resp_header("retry-after", "3600")
        |> Plug.Conn.send_resp(503, "SlowDown")
      end)

      use_s3_stub(ctx, "cc-throttle", op_deadline_ms: 5_000)

      {us, result} =
        :timer.tc(fn -> ClaimCheck.check_in(ctx.inst, "acme", [item("acme", "throttled")]) end)

      assert {:error, {:unavailable, {:throttled, 3600}}} = result
      assert div(us, 1000) < 1_000

      # The batch the dispatch pipeline packs says the same for every item, and returns.
      items = [item("acme", "one"), item("globex", "two")]
      results = ClaimCheck.check_in_batch(ctx.inst, items)

      for it <- items do
        assert {:error, {:unavailable, {:throttled, 3600}}} = results[it.id]
      end
    end

    test "a store that keeps failing is given up on at the operation deadline", ctx do
      Req.Test.stub(__MODULE__, &Plug.Conn.send_resp(&1, 500, "boom"))
      use_s3_stub(ctx, "cc-deadline", op_deadline_ms: 1_000, retries: 10, breaker_failures: 0)

      {us, result} =
        :timer.tc(fn -> ClaimCheck.check_in(ctx.inst, "acme", [item("acme", "failing")]) end)

      assert {:error, {:unavailable, {:status, 500, "boom"}}} = result
      assert us < 1_500_000
    end

    test "a store that answers slowly cannot hold a check-in past the deadline", ctx do
      Req.Test.stub(__MODULE__, fn conn ->
        Process.sleep(400)
        Plug.Conn.send_resp(conn, 500, "slow")
      end)

      use_s3_stub(ctx, "cc-slow", op_deadline_ms: 1_000, retries: 10, breaker_failures: 0)

      {us, result} =
        :timer.tc(fn -> ClaimCheck.check_in(ctx.inst, "acme", [item("acme", "slow")]) end)

      assert {:error, {:unavailable, _reason}} = result
      assert us < 2_000_000
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
