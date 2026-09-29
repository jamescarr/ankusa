defmodule Ankusa.NetTest do
  @moduledoc """
  `Ankusa.Net`: the addressing layer every route rule is built on. Addresses are
  `:inet.ip_address()` tuples, the representation `Plug.Conn.remote_ip` and the
  `cidr` package already share, so a parsed address matches a CIDR rule without
  conversion. CIDR parse/match/to_string is owned by the `cidr` package;
  `parse_cidr/1` wraps it for the strings an operator writes.
  """

  use ExUnit.Case, async: true

  alias Ankusa.Net
  alias CIDR

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

    test "returns :error — never raises — for a non-UTF-8 binary" do
      # A forwarded-for entry is bytes an attacker chose. `String.to_charlist/1`
      # raised `UnicodeConversionError` here, which the guard turned into a 500.
      assert Net.parse(<<0xFF>>) == :error
      assert Net.parse(<<0xFF, ?/, ?8>>) == :error
      assert Net.parse(<<0xC3>>) == :error
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

  describe "parse_cidr/1" do
    test "parses IPv4, IPv6 and bare addresses into %CIDR{}" do
      assert {:ok, %CIDR{first: {10, 0, 0, 0}, last: {10, 255, 255, 255}, mask: 8}} =
               Net.parse_cidr("10.0.0.0/8")

      assert {:ok, %CIDR{first: {0x2001, 0x0DB8, 0, 0, 0, 0, 0, 0}, mask: 32}} =
               Net.parse_cidr("2001:db8::/32")

      assert {:ok, %CIDR{first: {10, 0, 0, 1}, last: {10, 0, 0, 1}, mask: 32}} =
               Net.parse_cidr("10.0.0.1")
    end

    test "masks off host bits, as CIDR.parse/1 does" do
      assert {:ok, %CIDR{mask: 8, first: {10, 0, 0, 0}, last: {10, 255, 255, 255}}} =
               Net.parse_cidr("10.0.0.5/8")
    end

    test "returns an error message, never a raise, for junk" do
      assert {:error, message} = Net.parse_cidr("10.0.0.0/abc")
      assert message =~ "10.0.0.0/abc"

      assert {:error, message} = Net.parse_cidr("garbage")
      assert message =~ "garbage"

      assert {:error, message} = Net.parse_cidr("10.0.0.0/33")
      assert message =~ "10.0.0.0/33"

      # `CIDR.parse/1` raises `UnicodeConversionError` here; a config value or
      # an API payload can carry any bytes.
      assert {:error, message} = Net.parse_cidr(<<0xFF>>)
      assert is_binary(message)
    end

    test "returns an error message for a non-binary" do
      assert {:error, message} = Net.parse_cidr(nil)
      assert message =~ "nil"

      assert {:error, message} = Net.parse_cidr(10)
      assert message =~ "10"
    end

    test "rejects an IPv4-mapped IPv6 range, which could never match" do
      assert {:error, message} = Net.parse_cidr("::ffff:10.0.0.0/104")
      assert message =~ "::ffff:10.0.0.0/104"
      assert message =~ "IPv4"

      assert {:error, _message} = Net.parse_cidr("::ffff:0:0/96")
      assert {:error, _message} = Net.parse_cidr("::ffff:10.0.0.1")
    end

    test "accepts a range that merely contains mapped space" do
      assert {:ok, %CIDR{mask: 0}} = Net.parse_cidr("::/0")
      assert {:ok, %CIDR{mask: 80}} = Net.parse_cidr("::/80")
    end
  end
end
