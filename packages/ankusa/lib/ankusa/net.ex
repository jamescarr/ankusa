defmodule Ankusa.Net do
  @moduledoc """
  IP addresses in the form the route-management hot path and the `cidr` package
  already share: an `:inet.ip_address()` tuple — a 4-tuple for IPv4, an 8-tuple
  for IPv6.

  `:inet` tuples are the boundary format (a `Plug.Conn.remote_ip`, an
  `:inet.ntoa/1` result) and the representation every CIDR rule carries, so no
  conversion happens on the way into a membership test. The point of the one
  normalization below is one rule, applied once:

    * an IPv4-mapped IPv6 address — `::ffff:1.2.3.4`, what a dual-stack listener
      reports for an IPv4 client — becomes `{1, 2, 3, 4}`, so an IPv4 client
      matches an IPv4 rule and never an IPv6 one (and vice versa).

  A genuine IPv6 address stays an 8-tuple. Nothing here normalizes on the way
  *into* the rule scan, so a caller that hands it an unnormalized address gets a
  miss, not a wrong match.
  """

  @typedoc "A normalized IP address: an `:inet.ip_address()` tuple."
  @type ip :: :inet.ip_address()

  import Bitwise

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
      {:ok, ip} -> {:ok, normalize(ip)}
      {:error, _reason} -> :error
    end
  end

  def parse(_other), do: :error

  @doc """
  Collapse an IPv4-mapped IPv6 address to its IPv4 form; pass everything else
  through unchanged.
  """
  @spec normalize(ip()) :: ip()
  def normalize({0, 0, 0, 0, 0, 0xFFFF, hi, lo}),
    do: {hi >>> 8, hi &&& 0xFF, lo >>> 8, lo &&& 0xFF}

  def normalize(ip), do: ip

  @doc "Render `ip` in its canonical `:inet.ntoa/1` form."
  @spec to_string(ip()) :: String.t()
  def to_string(ip), do: ip |> :inet.ntoa() |> Kernel.to_string()
end
