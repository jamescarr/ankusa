defmodule Ankusa.Net.ClientIP do
  @moduledoc """
  Decide which address a request actually came from.

  This is the piece an allowlist cannot afford to get wrong: `X-Forwarded-For`
  is client-controlled, so reading it for a peer that is not itself a trusted
  proxy is the same as letting the caller choose its own IP and walk straight
  through the route guard.

  The rule, in full:

    1. The peer is `conn.remote_ip`, normalized.
    2. No trusted proxies configured, or the peer matches none of them → the
       peer is the client. The header is not read at all.
    3. Otherwise the peer is a proxy, so the chain is trustworthy as far as this
       node can tell: take the rightmost `X-Forwarded-For` entry that is not
       itself a trusted proxy (proxies append, so the rightmost untrusted hop is
       the closest client we can vouch for). All entries trusted → the leftmost,
       which is the chain's origin. No header, or an empty one → the peer.
    4. A header with *any* unparseable entry is discarded whole and the peer
       stands. One junk entry means the chain was not written by the proxies we
       trust, and the safe reading of a header we do not believe is "not
       present".

  The result is normalized, so an IPv4-mapped `::ffff:1.2.3.4` peer resolves to
  an IPv4 address and matches IPv4 rules.
  """

  alias Ankusa.Net
  alias CIDR

  @forwarded_for "x-forwarded-for"

  @doc """
  Resolve the client address for `conn`, given the CIDRs whose peers may set
  `X-Forwarded-For`.

  `:error` only when `conn.remote_ip` is nil — a conn with no peer at all
  (a test conn with the field cleared). Callers treat that as an IP rejection:
  there is no address to match against rules.
  """
  @spec resolve(Plug.Conn.t(), [CIDR.t()]) :: {:ok, :inet.ip_address()} | :error
  def resolve(%Plug.Conn{remote_ip: nil}, _trusted_proxies), do: :error

  def resolve(%Plug.Conn{remote_ip: peer} = conn, trusted_proxies) do
    peer = Net.normalize(peer)

    if forwarded?(peer, trusted_proxies) do
      {:ok, client(conn, peer, trusted_proxies)}
    else
      {:ok, peer}
    end
  end

  # ── internals ───────────────────────────────────────────────────────────────

  defp forwarded?(peer, trusted_proxies),
    do: Enum.any?(trusted_proxies, &contains?(&1, peer))

  defp contains?(cidr, ip), do: ip >= cidr.first and ip <= cidr.last

  defp client(conn, peer, trusted_proxies) do
    case chain(conn) do
      [] -> peer
      entries -> hop(entries, trusted_proxies)
    end
  end

  # ── the forwarded chain ─────────────────────────────────────────────────────

  # The rightmost entry is what the peer itself appended. `get_req_header/2`
  # returns the header values in order, so `List.last/1` is the one a client
  # that sends several ended with — also the one the proxy appended to.
  defp chain(conn) do
    conn
    |> Plug.Conn.get_req_header(@forwarded_for)
    |> List.last()
    |> parse_chain()
  end

  defp parse_chain(nil), do: []

  defp parse_chain(value) do
    entries =
      value
      |> String.split(",")
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))

    # All or nothing: a partially trusted chain is a chain we do not trust.
    case parse_all(entries) do
      :error -> []
      {:ok, ips} -> ips
    end
  end

  defp parse_all(entries) do
    with true <- Enum.all?(entries, fn e -> match?({:ok, _}, Net.parse(e)) end) do
      {:ok, Enum.map(entries, fn e -> elem(Net.parse(e), 1) end)}
    else
      false -> :error
    end
  end

  # Right to left: proxies append, so the rightmost entries are the ones this
  # node's own proxies wrote and the rightmost *untrusted* entry is the closest
  # client we can vouch for. Walking left to right and letting each untrusted
  # entry overwrite the previous one lands on that same entry without building a
  # reversed list. All entries trusted leaves nothing to return but the chain's
  # origin, the leftmost.
  defp hop(entries, trusted_proxies) do
    trusted? = fn entry -> Enum.any?(trusted_proxies, &contains?(&1, entry)) end

    Enum.reduce(entries, nil, fn entry, acc ->
      if trusted?.(entry), do: acc, else: entry
    end) || hd(entries)
  end
end
