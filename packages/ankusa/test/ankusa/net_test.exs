defmodule Ankusa.NetTest do
  @moduledoc """
  `Ankusa.Net`: the addressing layer every route rule is built on. Addresses are
  `:inet.ip_address()` tuples, the representation `Plug.Conn.remote_ip` and the
  `cidr` package already share, so a parsed address matches a CIDR rule without
  conversion. CIDR parse/match/to_string is owned by the `cidr` package.
  """

  use ExUnit.Case, async: true

  alias Ankusa.Net

  describe "parse/1 and normalize/1" do
    test "parses IPv4 and IPv6 into :inet tuples" do
      assert Net.parse("10.1.2.3") == {:ok, {10, 1, 2, 3}}
      assert Net.parse("::1") == {:ok, {0, 0, 0, 0, 0, 0, 0, 1}}
      assert Net.parse("2001:db8::1") == {:ok, {0x2001, 0x0DB8, 0, 0, 0, 0, 0, 1}}
    end

    test "returns :error for anything :inet can't parse" do
      assert Net.parse("nonsense") == :error
      assert Net.parse("10.0.0.256") == :error
      assert Net.parse("10.0.0.1abc") == :error
      assert Net.parse("") == :error
    end

    test "follows :inet.parse_address/1 on its lax cases" do
      # Pinned because it is surprising in a rule parser: inet shorthand is
      # accepted, and a scope suffix is dropped rather than rejected.
      assert Net.parse("10") == {:ok, {0, 0, 0, 10}}
      assert Net.parse("fe80::1%eth0") == Net.parse("fe80::1")
    end

    test "renders back to the canonical form" do
      assert Net.to_string({10, 1, 2, 3}) == "10.1.2.3"
      assert Net.to_string({0, 0, 0, 0, 0, 0, 0, 1}) == "::1"
    end

    test "normalizes an IPv4-mapped IPv6 address to IPv4" do
      assert Net.parse("::ffff:10.1.2.3") == {:ok, {10, 1, 2, 3}}
      assert Net.normalize({0, 0, 0, 0, 0, 0xFFFF, 0x0A01, 0x0203}) == {10, 1, 2, 3}
      assert Net.normalize({10, 1, 2, 3}) == {10, 1, 2, 3}
    end

    test "leaves a genuine IPv6 address alone" do
      assert Net.normalize({0x2001, 0x0DB8, 0, 0, 0, 0, 0, 1}) ==
               {0x2001, 0x0DB8, 0, 0, 0, 0, 0, 1}
    end
  end
end
