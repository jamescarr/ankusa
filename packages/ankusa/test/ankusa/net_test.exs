defmodule Ankusa.NetTest do
  @moduledoc """
  `Ankusa.Net` and `Ankusa.Net.CIDR`: the addressing layer every route rule is
  built on. The property tests exist because the parse/to_string and mask
  arithmetic are the parts a hand-written implementation gets subtly wrong for
  one prefix length out of 129.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  import Bitwise

  alias Ankusa.Net
  alias Ankusa.Net.CIDR

  # 128-bit values built with the top 64 bits shifted up are never in the
  # IPv4-mapped ::ffff:0:0/96 range, so normalize/1 leaves them 128-bit and the
  # round trips below compare like with like.
  defp ip_gen do
    one_of([
      map(integer(0..0xFFFFFFFF), &{32, &1}),
      map(integer(0..((1 <<< 64) - 1)), &{128, &1 <<< 64 ||| 1})
    ])
  end

  describe "parse/1 and to_tuple/1" do
    test "parses IPv4 and IPv6 into the tagged integer form" do
      assert Net.parse("10.1.2.3") == {:ok, {32, 0x0A010203}}
      assert Net.parse("::1") == {:ok, {128, 1}}
      assert Net.parse("2001:db8::1") == {:ok, {128, 0x20010DB8000000000000000000000001}}
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
      assert Net.parse("10") == {:ok, {32, 10}}
      assert Net.parse("fe80::1%eth0") == Net.parse("fe80::1")
    end

    test "renders back to the canonical form" do
      assert Net.to_string({32, 0x0A010203}) == "10.1.2.3"
      assert Net.to_string({128, 1}) == "::1"
    end

    test "normalizes an IPv4-mapped IPv6 address to IPv4" do
      assert Net.parse("::ffff:10.1.2.3") == {:ok, {32, 0x0A010203}}
      assert Net.normalize({128, 0xFFFF00000000 ||| 0x0A010203}) == {32, 0x0A010203}
      assert Net.normalize({32, 0x0A010203}) == {32, 0x0A010203}
    end

    test "leaves a genuine IPv6 address alone" do
      assert Net.normalize({128, 0x20010DB8000000000000000000000001}) ==
               {128, 0x20010DB8000000000000000000000001}
    end
  end

  describe "CIDR.parse/1" do
    test "0.0.0.0/0 contains every IPv4 address and no IPv6 one" do
      assert {:ok, any_v4} = CIDR.parse("0.0.0.0/0")
      assert CIDR.contains?(any_v4, {32, 0})
      assert CIDR.contains?(any_v4, {32, 0xFFFFFFFF})
      refute CIDR.contains?(any_v4, {128, 0})
    end

    test "::/0 contains every IPv6 address and no IPv4 one" do
      assert {:ok, any_v6} = CIDR.parse("::/0")
      assert CIDR.contains?(any_v6, {128, 0})
      assert CIDR.contains?(any_v6, {128, 0x20010DB8000000000000000000000001})
      refute CIDR.contains?(any_v6, {32, 0})
    end

    test "10.0.0.0/8 contains 10.1.2.3 and not 11.0.0.1" do
      assert {:ok, cidr} = CIDR.parse("10.0.0.0/8")
      assert CIDR.contains?(cidr, {32, 0x0A010203})
      refute CIDR.contains?(cidr, {32, 0x0B000001})
    end

    test "a mapped IPv6 client matches the IPv4 rule for the same address" do
      assert {:ok, cidr} = CIDR.parse("10.0.0.0/8")
      {:ok, client} = Net.parse("::ffff:10.1.2.3")

      assert CIDR.contains?(cidr, Net.normalize(client))
    end

    test "a bare address is a full-length prefix" do
      assert {:ok, bare} = CIDR.parse("10.1.2.3")
      assert bare.bits == 32
      assert CIDR.to_string(bare) == "10.1.2.3/32"

      assert {:ok, bare6} = CIDR.parse("2001:db8::1")
      assert CIDR.to_string(bare6) == "2001:db8::1/128"
    end

    test "masking the network means a host address prints as its network" do
      assert {:ok, cidr} = CIDR.parse("10.1.2.3/8")
      assert cidr.network == 0x0A000000
      assert CIDR.to_string(cidr) == "10.0.0.0/8"
    end

    test "rejects every malformed form" do
      for bad <- ~w(10.0.0.0/33 10.0.0.0/ 10.0.0.0/8/8 /8 nonsense ::/129 10.0.0.0/-1) do
        assert CIDR.parse(bad) == {:error, :invalid_cidr}, "expected #{bad} to be invalid"
      end
    end

    test "parse!/1 raises with the offending value" do
      assert_raise ArgumentError, ~r/invalid CIDR "10.0.0.0\/33"/, fn ->
        CIDR.parse!("10.0.0.0/33")
      end
    end

    test "a /0 renders as /0 rather than as a full prefix" do
      assert {:ok, cidr} = CIDR.parse("10.1.2.3/0")
      assert CIDR.to_string(cidr) == "0.0.0.0/0"
    end
  end

  describe "properties" do
    property "parse/1 round-trips through to_string/1, and a network contains itself" do
      check all(
              {bits, value} <- ip_gen(),
              prefix <- integer(0..bits),
              max_runs: 200
            ) do
        text = "#{Net.to_string({bits, value})}/#{prefix}"
        assert {:ok, cidr} = CIDR.parse(text)

        # Computed by shifting the host bits out and back — a different
        # expression from the mask arithmetic inside parse/1.
        network = if prefix == 0, do: 0, else: (value >>> (bits - prefix)) <<< (bits - prefix)

        assert CIDR.to_string(cidr) == "#{Net.to_string({bits, network})}/#{prefix}"
        assert CIDR.contains?(cidr, {bits, value})
      end
    end

    property "contains?/2 agrees with a naive comparison of the top `prefix` bits" do
      check all(
              {bits, value} <- ip_gen(),
              {other_bits, other} <- ip_gen(),
              prefix <- integer(0..bits),
              max_runs: 200
            ) do
        cidr = CIDR.parse!("#{Net.to_string({bits, value})}/#{prefix}")

        top = fn v -> if prefix == 0, do: 0, else: v >>> (bits - prefix) end
        expected = other_bits == bits and top.(other) == top.(value)

        assert CIDR.contains?(cidr, {other_bits, other}) == expected
      end
    end

    property "to_tuple/1 and from_tuple/1 round-trip" do
      check all({_bits, _value} = ip <- ip_gen(), max_runs: 200) do
        assert Net.from_tuple(Net.to_tuple(ip)) == ip
      end
    end
  end
end
