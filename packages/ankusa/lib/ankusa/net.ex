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

  The other half of the layer is `parse_cidr/1`: a CIDR string as an operator
  writes it — in YAML config or in an admin API payload — becomes a `%CIDR{}`.
  It never raises and never accepts an IPv4-mapped IPv6 range, which could not
  match a normalized address anyway. Rule values and `X-Forwarded-For` entries
  are attacker-reachable, so `parse/1` reads a binary as bytes rather than as
  UTF-8 text: junk is an `:error`, never a raise that would land in the guard.
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

  The binary reaches inet as bytes (`:binary.bin_to_list/1`), not as UTF-8 text
  (`String.to_charlist/1`, which raises on invalid UTF-8): a forwarded-for
  entry is attacker-reachable, and a raise here is a 500 in the guard rather
  than an `:error` the guard denies on.
  """
  @spec parse(binary()) :: {:ok, ip()} | :error
  def parse(binary) when is_binary(binary) do
    case :inet.parse_address(:binary.bin_to_list(binary)) do
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

  @doc """
  Parse a CIDR string — `"10.0.0.0/8"`, `"::1"`, `"2001:db8::/32"` — the one
  entry point for every CIDR an operator writes (`routes.trusted_proxies`, a
  route's `ip_rules`).

  `CIDR.parse/1` is the parser; this wrapper covers what it leaves open:

    * it never raises and its `{:error, message}` is always a string, so a
      config typo or a bad API payload is something to report rather than a
      crash (`CIDR.parse/1` sends the address through `String.to_charlist/1`,
      which raises on a non-UTF-8 binary);
    * it rejects an IPv4-mapped IPv6 range — any range inside `::ffff:0:0/96`,
      e.g. `::ffff:10.0.0.0/104`. Addresses are normalized to IPv4 before
      matching (`normalize/1`), so such a range can never match one; as a
      *deny* rule it would silently let its own traffic through. Ranges that
      merely contain mapped space (`::/0`, `::/80`) are fine.

  `{:error, message}` for any non-binary, for a string that is not a CIDR, and
  for a mapped range; the last one's message names the equivalent IPv4 CIDR to
  write instead. A host whose bits fall outside its mask (`"10.0.0.5/8"`) is
  masked off, as `CIDR.parse/1` already does.
  """
  @spec parse_cidr(binary()) :: {:ok, %CIDR{}} | {:error, String.t()}
  def parse_cidr(cidr) when is_binary(cidr) do
    case parse_cidr_string(cidr) do
      %CIDR{} = parsed -> reject_mapped(cidr, parsed)
      {:error, reason} -> {:error, invalid_cidr(cidr, reason)}
    end
  end

  def parse_cidr(other), do: {:error, "invalid CIDR #{inspect(other)}"}

  defp parse_cidr_string(cidr) do
    CIDR.parse(cidr)
  rescue
    _error -> {:error, :einval}
  end

  defp reject_mapped(cidr, %CIDR{first: first, last: last} = parsed) do
    if mapped?(first) and mapped?(last) do
      {:error,
       "invalid CIDR #{inspect(cidr)}: it is inside ::ffff:0:0/96, and addresses are " <>
         "normalized to IPv4 before matching, so it can never match — write it as the " <>
         "equivalent IPv4 CIDR"}
    else
      {:ok, parsed}
    end
  end

  # Both endpoints mapped means the whole (contiguous) range sits inside the
  # mapped space; a range that merely contains it has an endpoint outside.
  defp mapped?({0, 0, 0, 0, 0, 0xFFFF, _hi, _lo}), do: true
  defp mapped?(_address), do: false

  # `CIDR.parse/1` reports reasons as strings and as bare atoms (`:einval`).
  defp invalid_cidr(cidr, reason) when is_binary(reason),
    do: "invalid CIDR #{inspect(cidr)}: #{reason}"

  defp invalid_cidr(cidr, _reason), do: "invalid CIDR #{inspect(cidr)}"
end
