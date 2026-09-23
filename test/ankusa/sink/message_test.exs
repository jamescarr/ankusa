defmodule Ankusa.Sink.MessageTest do
  use ExUnit.Case, async: true

  alias Ankusa.{ClaimCheck, Envelope, UUIDv7}
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

  test "one byte over inline_max_bytes is claim-checked and redeems to the body", %{ctx: ctx} do
    body = :crypto.strong_rand_bytes(101)
    env = envelope(body)

    assert {:ok, json} = Message.encode(env, ctx, 100)
    decoded = JSON.decode!(json)

    assert decoded["v"] == 1
    assert decoded["size"] == 101
    refute Map.has_key?(decoded, "body_base64")

    assert {:ok, ticket} = ClaimCheck.Ticket.from_map(decoded["claim"])
    assert {:ok, ^body} = ClaimCheck.redeem(ctx.instance, ticket)
  end

  test "a failed check-in is tagged :claim_check", %{ctx: ctx} do
    env = envelope(:crypto.strong_rand_bytes(101), %{tenant_id: ""})

    assert {:error, {:claim_check, :invalid_tenant}} = Message.encode(env, ctx, 100)
  end
end
