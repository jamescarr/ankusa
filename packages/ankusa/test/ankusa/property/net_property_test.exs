defmodule Ankusa.Property.NetTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @moduletag timeout: 3_600_000

  import Bitwise

  alias Ankusa.Generators, as: G
  alias Ankusa.Net

  property "parse_cidr/1 never raises: an error, or a range whose ends belong together" do
    check all input <- G.cidr_string() do
      case Net.parse_cidr(input) do
        {:ok, %CIDR{first: first, last: last, mask: mask}} ->
          assert tuple_size(first) == tuple_size(last)
          assert tuple_size(first) in [4, 8]
          assert first <= last
          assert mask in 0..128
          refute mapped?(first) and mapped?(last)

        {:error, :mapped_range} ->
          assert String.valid?(input)

        {:error, message} ->
          assert is_binary(message)
      end
    end
  end

  property "a well-formed CIDR parses to the range its prefix names" do
    check all {text, ip, prefix} <- G.cidr_parts() do
      bits = tuple_size(ip) * if(tuple_size(ip) == 4, do: 8, else: 16)
      prefix = prefix || bits
      host_bits = bits - prefix
      number = to_number(ip)
      first = (number >>> host_bits) <<< host_bits
      last = first ||| (1 <<< host_bits) - 1

      result = Net.parse_cidr(text)

      if mapped?(from_number(first, bits)) and mapped?(from_number(last, bits)) do
        assert result == {:error, :mapped_range}
      else
        assert {:ok, %CIDR{} = cidr} = result
        assert cidr.first == from_number(first, bits)
        assert cidr.last == from_number(last, bits)
        assert cidr.mask == prefix
      end
    end
  end

  property "an address inside a parsed CIDR matches it, and the ones just outside do not" do
    check all {text, ip, _prefix} <- G.cidr_parts() do
      case Net.parse_cidr(text) do
        {:ok, cidr} ->
          bits = if tuple_size(ip) == 4, do: 32, else: 128

          assert CIDR.match!(cidr, ip)
          assert CIDR.match!(cidr, cidr.first)
          assert CIDR.match!(cidr, cidr.last)

          after_last = to_number(cidr.last) + 1
          before_first = to_number(cidr.first) - 1

          if after_last < 1 <<< bits, do: refute(CIDR.match!(cidr, from_number(after_last, bits)))
          if before_first >= 0, do: refute(CIDR.match!(cidr, from_number(before_first, bits)))

        {:error, :mapped_range} ->
          assert mapped?(ip)
      end
    end
  end

  property "a prefix past the address family's width is refused" do
    check all ip <- one_of([G.ipv4(), G.ipv6()]), over <- integer(1..1000) do
      width = if tuple_size(ip) == 4, do: 32, else: 128
      text = "#{:inet.ntoa(ip)}/#{width + over}"

      assert {:error, message} = Net.parse_cidr(text)
      assert is_binary(message)
    end
  end

  property "parse/1 never raises on any bytes: an address, or :error" do
    check all input <-
                one_of([
                  G.bytes(64),
                  map(G.cidr_string(), & &1),
                  map(one_of([G.ipv4(), G.ipv6()]), &to_string(:inet.ntoa(&1)))
                ]) do
      case Net.parse(input) do
        :error -> :ok
        {:ok, ip} -> assert tuple_size(ip) in [4, 8]
      end
    end
  end

  property "normalize/1 is idempotent and only ever collapses a mapped address" do
    check all ip <- one_of([G.ipv4(), G.ipv6()]) do
      once = Net.normalize(ip)
      assert Net.normalize(once) == once
      if tuple_size(once) == 8, do: refute(mapped?(once))
      if tuple_size(ip) == 4, do: assert(once == ip)
    end
  end

  property "parse_cidr/1 refuses anything that is not a binary" do
    check all term <- one_of([integer(), atom(:alphanumeric), list_of(integer()), constant(nil)]) do
      assert {:error, message} = Net.parse_cidr(term)
      assert is_binary(message)
    end
  end

  defp mapped?({0, 0, 0, 0, 0, 0xFFFF, _, _}), do: true
  defp mapped?(_), do: false

  defp to_number({a, b, c, d}), do: a <<< 24 ||| b <<< 16 ||| c <<< 8 ||| d

  defp to_number(ip) when tuple_size(ip) == 8 do
    ip |> Tuple.to_list() |> Enum.reduce(0, fn group, acc -> acc <<< 16 ||| group end)
  end

  defp from_number(n, 32), do: {n >>> 24 &&& 255, n >>> 16 &&& 255, n >>> 8 &&& 255, n &&& 255}

  defp from_number(n, 128) do
    groups = for shift <- 112..0//-16, do: n >>> shift &&& 0xFFFF
    List.to_tuple(groups)
  end
end
