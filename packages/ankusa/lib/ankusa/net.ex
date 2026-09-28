defmodule Ankusa.Net do
  @moduledoc """
  IP addresses in the form the route-management hot path wants them: a tagged
  integer, `{bits, value}`, that compare and mask with bit operators only.

  `:inet` tuples are the boundary format (a `Plug.Conn.remote_ip`, an
  `:inet.ntoa/1` result); everything inside `Ankusa.Routes` and
  `Ankusa.Net.CIDR` is normalized here first. The point of the normalization is
  one rule, applied once:

    * an IPv4-mapped IPv6 address — `::ffff:1.2.3.4`, what a dual-stack listener
      reports for an IPv4 client — becomes `{32, 0x01020304}`, so an IPv4 client
      matches an IPv4 rule and never an IPv6 one (and vice versa).

  A `{128, _}` address outside that mapped range stays `{128, _}`. Nothing here
  normalizes on the way *into* `Ankusa.Net.CIDR.contains?/2`, so a caller that
  hands it an unnormalized address gets a miss, not a wrong match.
  """

  @typedoc "A normalized IP address: 32 or 128 significant bits, as an integer."
  @type ip :: {32 | 128, non_neg_integer()}

  import Bitwise

  @ipv4_mapped_prefix 0xFFFF

  @doc """
  Parse an address in any `:inet.parse_address/1` syntax (`"10.0.0.1"`,
  `"::1"`, `"::ffff:10.0.0.1"`), normalized.

  `:inet.parse_address/1` is the whole grammar: it is strict about trailing
  junk (`"10.0.0.1abc"`, `"1.2.3.4.5"`) but accepts inet's historical IPv4
  shorthand (`"10"` is `0.0.0.10`, `"0x7f.1"` is `127.0.0.1`) and ignores a
  scope suffix (`"fe80::1%eth0"`), so a config typo like `"10"` means
  `0.0.0.10` rather than an error. `:error` for anything it rejects.
  """
  @spec parse(binary()) :: {:ok, ip()} | :error
  def parse(binary) when is_binary(binary) do
    case :inet.parse_address(String.to_charlist(binary)) do
      {:ok, tuple} -> {:ok, normalize(from_tuple(tuple))}
      {:error, _reason} -> :error
    end
  end

  def parse(_other), do: :error

  @doc "Wrap an `:inet.ip_address()` tuple."
  @spec from_tuple(:inet.ip_address()) :: ip()
  def from_tuple({a, b, c, d}), do: {32, a <<< 24 ||| b <<< 16 ||| c <<< 8 ||| d}

  def from_tuple({a, b, c, d, e, f, g, h}) do
    # Two 64-bit halves, each its four 16-bit groups: `<<<` binds tighter than
    # `|||`, so each shift lands on its own operand.
    high = a <<< 48 ||| b <<< 32 ||| c <<< 16 ||| d
    low = e <<< 48 ||| f <<< 32 ||| g <<< 16 ||| h
    {128, high <<< 64 ||| low}
  end

  @doc "The `:inet.ip_address()` tuple for `ip`."
  @spec to_tuple(ip()) :: :inet.ip_address()
  def to_tuple({32, value}),
    do: {value >>> 24 &&& 0xFF, value >>> 16 &&& 0xFF, value >>> 8 &&& 0xFF, value &&& 0xFF}

  def to_tuple({128, value}) do
    List.to_tuple(for shift <- [112, 96, 80, 64, 48, 32, 16, 0], do: value >>> shift &&& 0xFFFF)
  end

  @doc """
  Collapse an IPv4-mapped IPv6 address to its IPv4 form; pass everything else
  through unchanged.
  """
  @spec normalize(ip()) :: ip()
  def normalize({128, value}) when value >>> 32 == @ipv4_mapped_prefix,
    do: {32, value &&& 0xFFFFFFFF}

  def normalize(ip), do: ip

  @doc "Render `ip` in its canonical `:inet.ntoa/1` form."
  @spec to_string(ip()) :: String.t()
  def to_string(ip), do: ip |> to_tuple() |> :inet.ntoa() |> Kernel.to_string()
end
