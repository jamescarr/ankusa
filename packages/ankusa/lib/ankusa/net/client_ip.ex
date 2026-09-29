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
       node can tell: every `x-forwarded-for` value is joined in the order the
       request carried them, split on commas and trimmed, and empty entries are
       dropped. Nothing left → the peer.
    4. The chain is walked right to left, one entry at a time and only as far as
       needed. Proxies append, so the rightmost entries are the ones this node's
       own proxies wrote, and the rightmost *untrusted* entry is the closest
       client we can vouch for: that entry is the client, and the entries to its
       left — the prefix a client can forge — are never even parsed. An entry
       that cannot be read is `:error`: a chain we cannot fully account for is
       not one we believe, and the request is denied rather than falling back to
       the proxy's own address, which is exactly what a forged entry would be
       trying to achieve. If every entry is trusted, the leftmost one is the
       client.
    5. An entry is a bare IPv4 or IPv6 address, or one of the `host:port` forms
       `1.2.3.4:5678` and `[2001:db8::1]:443` (the brackets are how an IPv6
       address, which owns colons itself, carries a port). Anything else —
       `garbage`, `1.2.3.4:abc`, `1.2.3.4:99999`, `[::1`, a non-UTF-8 byte — is
       an entry that cannot be read.

  The result is normalized, so a peer or an entry written as the IPv4-mapped
  `::ffff:1.2.3.4` resolves to an IPv4 address and matches IPv4 rules.
  """

  alias Ankusa.Net
  alias CIDR

  @forwarded_for "x-forwarded-for"

  @doc """
  Resolve the client address for `conn`, given the CIDRs whose peers may set
  `X-Forwarded-For`.

  `:error` when `conn.remote_ip` is nil — a conn with no peer at all (a test
  conn with the field cleared) — or when an entry the chain depends on cannot be
  read. Callers treat both as an IP rejection: there is no address to match
  against rules, and neither case may quietly become the proxy's own address.
  """
  @spec resolve(Plug.Conn.t(), [%CIDR{}]) :: {:ok, :inet.ip_address()} | :error
  def resolve(%Plug.Conn{remote_ip: nil}, _trusted_proxies), do: :error

  def resolve(%Plug.Conn{remote_ip: remote_ip} = conn, trusted_proxies) do
    peer = Net.normalize(remote_ip)

    if trusted?(peer, trusted_proxies) do
      forwarded(conn, peer, trusted_proxies)
    else
      {:ok, peer}
    end
  end

  # ── the forwarded chain ─────────────────────────────────────────────────────

  defp forwarded(conn, peer, trusted_proxies) do
    case entries(conn) do
      [] ->
        {:ok, peer}

      entries ->
        case walk(entries, trusted_proxies) do
          {:client, ip} -> {:ok, ip}
          {:trusted, ip} -> {:ok, ip}
          :error -> :error
        end
    end
  end

  # Every `x-forwarded-for` value, in the order the request carried them, as one
  # list of trimmed, non-empty entries. A proxy in front of this node appends to
  # the header rather than replacing it, so a conn can carry the header more
  # than once and all of it is the chain.
  defp entries(conn) do
    conn
    |> Plug.Conn.get_req_header(@forwarded_for)
    |> Enum.join(",")
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end

  # Right to left, parsing each entry once and stopping at the client: an entry
  # to the left of it is never read, which is what keeps a client-forged prefix
  # from mattering. `{:trusted, ip}` carries the address of the leftmost entry
  # walked so far — every frame further left overwrites it, so the chain's
  # origin survives to the caller when the whole chain is trusted.
  defp walk([entry | rest], trusted_proxies) do
    case walk(rest, trusted_proxies) do
      {:client, ip} -> {:client, ip}
      :error -> :error
      {:trusted, _ip} -> hop(entry, trusted_proxies)
    end
  end

  defp walk([], _trusted_proxies), do: {:trusted, nil}

  defp hop(entry, trusted_proxies) do
    case parse_entry(entry) do
      {:ok, ip} ->
        if trusted?(ip, trusted_proxies), do: {:trusted, ip}, else: {:client, ip}

      :error ->
        :error
    end
  end

  defp trusted?(ip, trusted_proxies), do: Enum.any?(trusted_proxies, &contains?(&1, ip))

  defp contains?(cidr, ip), do: ip >= cidr.first and ip <= cidr.last

  # ── entry grammar ───────────────────────────────────────────────────────────

  defp parse_entry(entry) do
    case Net.parse(entry) do
      {:ok, ip} -> {:ok, ip}
      :error -> host_port(entry)
    end
  end

  # `[2001:db8::1]` / `[2001:db8::1]:443`. The brackets exist precisely because
  # an IPv6 literal contains colons of its own, so the bracketed form is the
  # IPv6 one.
  defp host_port("[" <> rest) do
    case :binary.split(rest, "]") do
      [host, tail] -> bracketed(host, tail)
      [_unterminated] -> :error
    end
  end

  # `1.2.3.4:5678`: a bare address with a port, and the address half must be
  # IPv4 — a bare IPv6 address with `:port` appended is indistinguishable from
  # the address itself, so a ported IPv6 entry arrives bracketed.
  defp host_port(entry) do
    case :binary.split(entry, ":") do
      [host, port] -> ipv4_host_port(host, port)
      [_no_port] -> :error
    end
  end

  defp ipv4_host_port(host, port) do
    if port?(port) do
      case Net.parse(host) do
        {:ok, ip} when tuple_size(ip) == 4 -> {:ok, ip}
        _other -> :error
      end
    else
      :error
    end
  end

  defp bracketed(host, tail) do
    if String.contains?(host, ":") and bracket_port?(tail) do
      Net.parse(host)
    else
      :error
    end
  end

  defp bracket_port?(""), do: true
  defp bracket_port?(":" <> digits), do: port?(digits)
  defp bracket_port?(_other), do: false

  # Digits only, and at most the five a 16-bit port can have: a header is
  # attacker-reachable, so a million-digit "port" must not reach `Integer`.
  defp port?(digits) do
    byte_size(digits) in 1..5 and digits?(digits) and String.to_integer(digits) <= 65_535
  end

  defp digits?(digits), do: digits |> :binary.bin_to_list() |> Enum.all?(&(&1 in ?0..?9))
end
