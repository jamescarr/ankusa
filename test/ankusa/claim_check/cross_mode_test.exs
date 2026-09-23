defmodule Ankusa.ClaimCheck.CrossModeTest do
  @moduledoc """
  Proves the plan's central claim: a ticket issued by `Direct` is redeemable
  through `Remote`, and vice versa — the same bytes, crossing a real TCP/HTTP
  boundary (`ThousandIsland.listener_info/1` resolves the OS-assigned port a
  live `:claim_check`-role `Ankusa.Instance` bound, and `Remote` talks to it
  over `:httpc`, not an in-process `Plug.Test` call).
  """

  use ExUnit.Case, async: false

  alias Ankusa.{ClaimCheck, Config, UUIDv7}
  alias Ankusa.ClaimCheck.{Direct, Remote, Ticket}

  setup do
    inst = :"ccx#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "ankusa_#{inst}")
    on_exit(fn -> File.rm_rf(dir) end)

    config =
      Config.new(
        instance: inst,
        data_dir: dir,
        roles: [:claim_check],
        claim_check: %{port: 0, api_tokens: %{"secret" => :all, "scoped" => ["acme"]}}
      )

    start_supervised!({Ankusa.Instance, config})

    %{inst: inst, base_url: "http://127.0.0.1:#{bound_port(inst)}"}
  end

  defp bound_port(inst) do
    {_id, pid, _type, _mods} =
      inst
      |> then(&Ankusa.via(&1, :instance))
      |> GenServer.whereis()
      |> Supervisor.which_children()
      |> Enum.find(fn {id, _pid, _type, _mods} -> id == Ankusa.ClaimCheck.Router end)

    {:ok, {_address, port}} = ThousandIsland.listener_info(pid)
    port
  end

  test "Direct check-in redeems through Remote over a real HTTP hop", %{inst: inst, base_url: url} do
    body = :crypto.strong_rand_bytes(4096)
    meta = %{tenant_id: "acme", id: UUIDv7.generate(), content_type: "application/octet-stream"}

    assert {:ok, ticket} = ClaimCheck.check_in(inst, body, meta, adapter: {Direct, []})

    assert {:ok, ^body} =
             ClaimCheck.redeem(inst, ticket, adapter: {Remote, url: url, token: "secret"})
  end

  test "Remote check-in over a real HTTP hop redeems through Direct", %{inst: inst, base_url: url} do
    body = "hello over the wire"
    meta = %{tenant_id: "acme", id: UUIDv7.generate()}

    assert {:ok, ticket} =
             ClaimCheck.check_in(inst, body, meta, adapter: {Remote, url: url, token: "secret"})

    assert {:ok, ^body} = ClaimCheck.redeem(inst, ticket, adapter: {Direct, []})
  end

  test "Remote check-in with a wrong token is :unauthorized, and nothing is stored", %{
    inst: inst,
    base_url: url
  } do
    meta = %{tenant_id: "acme", id: UUIDv7.generate()}

    assert {:error, :unauthorized} =
             ClaimCheck.check_in(inst, "x", meta, adapter: {Remote, url: url, token: "wrong"})

    {:ok, ticket} = Ticket.new(meta, "x")
    assert {:error, :not_found} = ClaimCheck.redeem(inst, ticket, adapter: {Direct, []})
  end

  test "Remote redeem for a tenant outside the token's scope is :forbidden", %{
    inst: inst,
    base_url: url
  } do
    meta = %{tenant_id: "globex", id: UUIDv7.generate()}
    assert {:ok, ticket} = ClaimCheck.check_in(inst, "x", meta, adapter: {Direct, []})

    assert {:error, :forbidden} =
             ClaimCheck.redeem(inst, ticket, adapter: {Remote, url: url, token: "scoped"})
  end

  test "Remote redeem of a claim that was never checked in is :not_found", %{
    inst: inst,
    base_url: url
  } do
    {:ok, ticket} = Ticket.new(%{tenant_id: "acme", id: UUIDv7.generate()}, "")

    assert {:error, :not_found} =
             ClaimCheck.redeem(inst, ticket, adapter: {Remote, url: url, token: "secret"})
  end
end
