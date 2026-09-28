defmodule Ankusa.Sink.MessageTest do
  use ExUnit.Case, async: true

  alias Ankusa.{ClaimCheck, Envelope, UUIDv7}
  alias Ankusa.ClaimCheck.Ref
  alias Ankusa.Sink.Message

  setup do
    instance = :"msg_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{instance}")
    Ankusa.put_config(Ankusa.Config.new(instance: instance, data_dir: dir))
    on_exit(fn -> File.rm_rf(dir) end)
    %{ctx: %{instance: instance, source_id: "src", tenant_id: "t1", attempt: 1}}
  end

  defp envelope(body, overrides \\ %{}) do
    struct(
      %Envelope{
        id: UUIDv7.generate(),
        source_id: "src",
        tenant_id: "t1",
        received_at: 1_737_500_000_000,
        method: "POST",
        path: "/hooks/src",
        headers: [],
        content_type: "application/octet-stream",
        body: body,
        size: byte_size(body)
      },
      overrides
    )
  end

  test "a body of exactly inline_max_bytes rides inline", %{ctx: ctx} do
    env = envelope(:binary.copy("a", 100))

    assert {:ok, json} = Message.encode(env, ctx, 100)
    decoded = JSON.decode!(json)

    assert decoded["v"] == 1
    assert decoded["id"] == env.id
    assert decoded["received_at"] == env.received_at
    assert Base.decode64!(decoded["body_base64"]) == env.body
    refute Map.has_key?(decoded, "claim")
  end

  test "one byte over inline_max_bytes carries a claim ref string that redeems to the body",
       %{ctx: ctx} do
    body = :crypto.strong_rand_bytes(101)
    env = envelope(body)

    assert {:ok, json} = Message.encode(env, ctx, 100)
    decoded = JSON.decode!(json)

    assert decoded["v"] == 1
    assert decoded["size"] == 101
    refute Map.has_key?(decoded, "body_base64")

    assert "urn:ankusa:claim:v1:t1:" <> _ = decoded["claim"]
    assert decoded["sha256"] == Base.encode16(:crypto.hash(:sha256, body), case: :lower)
    assert {:ok, ^body} = ClaimCheck.redeem(ctx.instance, decoded["claim"], decoded["sha256"])
  end

  test "checking the same envelope in again rewrites its object instead of adding one",
       %{ctx: ctx} do
    env = envelope(:crypto.strong_rand_bytes(101))

    assert {:ok, first} = Message.encode(env, ctx, 100)
    assert {:ok, second} = Message.encode(env, ctx, 100)

    assert JSON.decode!(first)["claim"] == JSON.decode!(second)["claim"]
    assert [_one] = Ankusa.BlobStore.list(ctx.instance, "claims/")
  end

  test "a claim dispatch already checked in is used as-is, with no second write", %{ctx: ctx} do
    claim = %{
      ref: %Ref{tenant_id: "t1", claim_id: Ref.claim_id(Ref.new_pack_id(), 3)},
      sha256: String.duplicate("0", 64)
    }

    env = envelope(:crypto.strong_rand_bytes(101))

    assert {:ok, json} = Message.encode(env, Map.put(ctx, :claim, claim), 100)
    decoded = JSON.decode!(json)
    assert {decoded["claim"], decoded["sha256"]} == {Ref.to_string(claim.ref), claim.sha256}
    assert Ankusa.BlobStore.list(ctx.instance, "claims/") == []
  end

  test "a failed check-in is tagged :claim_check", %{ctx: ctx} do
    env = envelope(:crypto.strong_rand_bytes(101), %{tenant_id: ""})

    assert {:error, {:claim_check, :invalid_tenant}} = Message.encode(env, ctx, 100)
  end
end
