defmodule Ankusa.ClaimCheck.TicketTest do
  use ExUnit.Case, async: true

  alias Ankusa.ClaimCheck.Ticket
  alias Ankusa.UUIDv7

  test "new/2 computes size and sha256 from the actual bytes, never trusting the caller" do
    id = UUIDv7.generate()
    body = "hello claim check"

    assert {:ok, ticket} =
             Ticket.new(%{tenant_id: "acme", id: id, content_type: "text/plain"}, body)

    assert ticket.tenant_id == "acme"
    assert ticket.id == id
    assert ticket.size == byte_size(body)
    assert ticket.sha256 == Base.encode16(:crypto.hash(:sha256, body), case: :lower)
    assert ticket.content_type == "text/plain"
    assert ticket.v == 1
  end

  test "new/2 rejects a non-UUIDv7 id" do
    assert {:error, :invalid_id} = Ticket.new(%{tenant_id: "acme", id: "not-a-uuid"}, "x")

    assert {:error, :invalid_id} =
             Ticket.new(%{tenant_id: "acme", id: "00000000-0000-4000-8000-000000000000"}, "x")
  end

  test "new/2 rejects an empty or oversized tenant" do
    id = UUIDv7.generate()
    assert {:error, :invalid_tenant} = Ticket.new(%{tenant_id: "", id: id}, "x")

    assert {:error, :invalid_tenant} =
             Ticket.new(%{tenant_id: String.duplicate("a", 257), id: id}, "x")
  end

  test "key/1 derives claims/<tenant>/<id> and is injective/traversal-safe" do
    id = UUIDv7.generate()
    {:ok, plain} = Ticket.new(%{tenant_id: "acme", id: id}, "x")
    assert Ticket.key(plain) == "claims/acme/#{id}"

    {:ok, traversal} = Ticket.new(%{tenant_id: "../../etc", id: id}, "x")
    key = Ticket.key(traversal)
    refute String.contains?(key, "..")
    assert String.starts_with?(key, "claims/")
    # different tenants never collide after encoding
    {:ok, other} = Ticket.new(%{tenant_id: "acme/../etc", id: id}, "x")
    assert Ticket.key(other) != key
  end

  test "to_map/1 and from_map/1 round-trip" do
    {:ok, ticket} =
      Ticket.new(
        %{tenant_id: "acme", id: UUIDv7.generate(), content_type: "application/json"},
        "body"
      )

    assert {:ok, ^ticket} = ticket |> Ticket.to_map() |> Ticket.from_map()
  end

  test "from_map/1 rejects an unsupported version" do
    map = %{
      "v" => 2,
      "tenant_id" => "acme",
      "id" => UUIDv7.generate(),
      "size" => 1,
      "sha256" => String.duplicate("a", 64)
    }

    assert {:error, :unsupported_ticket_version} = Ticket.from_map(map)
    assert {:error, :unsupported_ticket_version} = Ticket.from_map(%{})
  end
end

defmodule Ankusa.ClaimCheckTest do
  use ExUnit.Case, async: false

  alias Ankusa.{ClaimCheck, Config, UUIDv7}
  alias Ankusa.ClaimCheck.{Direct, Ticket}
  alias Ankusa.BlobStore.LocalFS

  setup do
    inst = :"cc#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{inst}")
    on_exit(fn -> File.rm_rf(dir) end)

    config = Config.new(instance: inst, data_dir: dir, roles: [])
    Ankusa.put_config(config)

    %{inst: inst, dir: dir}
  end

  test "check_in/4 then redeem/3 round-trips the exact bytes via Direct/LocalFS", %{inst: inst} do
    body = :crypto.strong_rand_bytes(2048)
    meta = %{tenant_id: "acme", id: UUIDv7.generate(), content_type: "application/octet-stream"}

    assert {:ok, ticket} = ClaimCheck.check_in(inst, body, meta)
    assert ticket.size == byte_size(body)
    assert {:ok, ^body} = ClaimCheck.redeem(inst, ticket)
  end

  test "re-checking in the same (tenant_id, id) with the same bytes is idempotent", %{inst: inst} do
    body = "same bytes"
    meta = %{tenant_id: "acme", id: UUIDv7.generate()}

    assert {:ok, t1} = ClaimCheck.check_in(inst, body, meta)
    assert {:ok, t2} = ClaimCheck.check_in(inst, body, meta)
    assert t1 == t2
    assert {:ok, ^body} = ClaimCheck.redeem(inst, t1)
  end

  test "a tampered object on disk fails redeem with :integrity_mismatch", %{inst: inst} do
    meta = %{tenant_id: "acme", id: UUIDv7.generate()}
    assert {:ok, ticket} = ClaimCheck.check_in(inst, "original bytes", meta)

    :ok = LocalFS.put(inst, Ticket.key(ticket), "corrupted!!", [])

    assert {:error, :integrity_mismatch} = ClaimCheck.redeem(inst, ticket)
  end

  test "redeeming a ticket for a claim that was never checked in is :not_found", %{inst: inst} do
    {:ok, ticket} = Ticket.new(%{tenant_id: "acme", id: UUIDv7.generate()}, "")
    assert {:error, :not_found} = ClaimCheck.redeem(inst, ticket)
  end

  test "check_in/4 rejects a body over claim_check.max_bytes", %{inst: inst, dir: dir} do
    config = Config.new(instance: inst, data_dir: dir, roles: [], claim_check: %{max_bytes: 10})
    Ankusa.put_config(config)

    meta = %{tenant_id: "acme", id: UUIDv7.generate()}
    assert {:error, :too_large} = ClaimCheck.check_in(inst, String.duplicate("x", 11), meta)
  end

  test "check_in/4 with :expect_sha256 fails on mismatch", %{inst: inst} do
    meta = %{tenant_id: "acme", id: UUIDv7.generate()}

    assert {:error, :integrity_mismatch} =
             ClaimCheck.check_in(inst, "actual body", meta,
               expect_sha256: String.duplicate("0", 64)
             )
  end

  test "the :adapter opt overrides the configured adapter for one call", %{inst: inst} do
    meta = %{tenant_id: "acme", id: UUIDv7.generate()}
    assert {:ok, ticket} = ClaimCheck.check_in(inst, "x", meta, adapter: {Direct, []})
    assert {:ok, "x"} = ClaimCheck.redeem(inst, ticket, adapter: {Direct, []})
  end

  describe "validate_config!/1" do
    test "rejects a :claim_check-role node configured with the Remote adapter" do
      config =
        Config.new(
          instance: :cc_validate1,
          roles: [:claim_check],
          claim_check: %{
            adapter: {Ankusa.ClaimCheck.Remote, url: "http://x", token: "t"},
            api_tokens: %{"t" => :all}
          }
        )

      assert_raise ArgumentError, ~r/must not be Ankusa.ClaimCheck.Remote/, fn ->
        ClaimCheck.validate_config!(config)
      end
    end

    test "accepts a :claim_check-role node with no api_tokens (open gateway)" do
      config = Config.new(instance: :cc_validate2, roles: [:claim_check])

      assert :ok = ClaimCheck.validate_config!(config)
    end

    test "accepts a :claim_check-role node with Direct adapter and tokens configured" do
      config =
        Config.new(
          instance: :cc_validate3,
          roles: [:claim_check],
          claim_check: %{api_tokens: %{"secret" => :all}}
        )

      assert :ok = ClaimCheck.validate_config!(config)
    end

    test "rejects claim_check.max_bytes smaller than max_body_bytes on a :dispatch node" do
      config =
        Config.new(
          instance: :cc_validate4,
          roles: [:dispatch],
          max_body_bytes: 1_000,
          claim_check: %{max_bytes: 100}
        )

      assert_raise ArgumentError, ~r/smaller than max_body_bytes/, fn ->
        ClaimCheck.validate_config!(config)
      end
    end

    test "rejects retention_days set against a non-LocalFS claim store" do
      config =
        Config.new(
          instance: :cc_validate5,
          roles: [],
          claim_check: %{
            retention_days: 7,
            adapter: {Direct, blob_store: {Ankusa.BlobStore.S3, bucket: "b", region: "r"}}
          }
        )

      assert_raise ArgumentError, ~r/LocalFS sweeper/, fn ->
        ClaimCheck.validate_config!(config)
      end
    end

    test "accepts retention_days set against LocalFS" do
      config = Config.new(instance: :cc_validate6, roles: [], claim_check: %{retention_days: 7})
      assert :ok = ClaimCheck.validate_config!(config)
    end
  end
end

defmodule Ankusa.ClaimCheck.DirectTest do
  use ExUnit.Case, async: false

  alias Ankusa.{Config, UUIDv7}
  alias Ankusa.ClaimCheck.{Direct, Ticket}

  setup do
    inst = :"ccd#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{inst}")
    on_exit(fn -> File.rm_rf(dir) end)
    Ankusa.put_config(Config.new(instance: inst, data_dir: dir, roles: []))
    %{inst: inst}
  end

  test "store/4 then fetch/3 round-trips via the instance's configured BlobStore", %{inst: inst} do
    {:ok, ticket} = Ticket.new(%{tenant_id: "acme", id: UUIDv7.generate()}, "payload")

    assert :ok = Direct.store(inst, ticket, "payload", [])
    assert {:ok, "payload"} = Direct.fetch(inst, ticket, [])
  end

  test "fetch/3 on a missing key is :not_found", %{inst: inst} do
    {:ok, ticket} = Ticket.new(%{tenant_id: "acme", id: UUIDv7.generate()}, "")
    assert {:error, :not_found} = Direct.fetch(inst, ticket, [])
  end
end
