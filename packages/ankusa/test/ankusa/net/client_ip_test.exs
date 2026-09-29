defmodule Ankusa.Net.ClientIPTest do
  @moduledoc """
  Client-IP resolution. The header cases are the whole game: an allowlist that
  reads `X-Forwarded-For` from an untrusted peer is an allowlist the sender
  chooses.
  """

  use ExUnit.Case, async: true

  alias Ankusa.Net
  alias Ankusa.Net.ClientIP
  alias CIDR

  defp proxies(cidrs) do
    Enum.map(cidrs, fn cidr ->
      case CIDR.parse(cidr) do
        %CIDR{} = c -> c
        {:error, _} -> flunk("bad proxy #{cidr}")
      end
    end)
  end

  defp conn(peer, headers \\ []) do
    Enum.reduce(headers, Plug.Test.conn(:post, "/hooks/x", "{}"), fn {name, value}, conn ->
      Plug.Conn.put_req_header(conn, name, value)
    end)
    |> Map.put(:remote_ip, peer)
  end

  defp ip(text), do: elem(Net.parse(text), 1)

  test "the peer is the client when no proxies are trusted" do
    conn = conn(ip("1.2.3.4"), [{"x-forwarded-for", "9.9.9.9"}])

    assert ClientIP.resolve(conn, []) == {:ok, ip("1.2.3.4")}
  end

  test "an untrusted peer's X-Forwarded-For is ignored entirely" do
    conn = conn(ip("192.168.1.1"), [{"x-forwarded-for", "1.2.3.4"}])

    assert ClientIP.resolve(conn, proxies(["10.0.0.0/8"])) == {:ok, ip("192.168.1.1")}
  end

  test "a trusted peer's chain resolves to the rightmost untrusted entry" do
    conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", "1.2.3.4, 10.0.0.7"}])

    assert ClientIP.resolve(conn, proxies(["10.0.0.0/8"])) == {:ok, ip("1.2.3.4")}
  end

  test "a chain of only trusted entries resolves to the leftmost" do
    conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", "10.0.0.7, 10.0.0.8"}])

    assert ClientIP.resolve(conn, proxies(["10.0.0.0/8"])) == {:ok, ip("10.0.0.7")}
  end

  test "a header with one junk entry is discarded whole" do
    conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", "1.2.3.4, not-an-ip"}])

    assert ClientIP.resolve(conn, proxies(["10.0.0.0/8"])) == {:ok, ip("10.0.0.9")}
  end

  test "an absent or empty header falls back to the peer" do
    trusted = proxies(["10.0.0.0/8"])

    assert ClientIP.resolve(conn(ip("10.0.0.9")), trusted) == {:ok, ip("10.0.0.9")}

    assert ClientIP.resolve(conn(ip("10.0.0.9"), [{"x-forwarded-for", ""}]), trusted) ==
             {:ok, ip("10.0.0.9")}

    assert ClientIP.resolve(conn(ip("10.0.0.9"), [{"x-forwarded-for", " , "}]), trusted) ==
             {:ok, ip("10.0.0.9")}
  end

  test "the last of several X-Forwarded-For headers wins" do
    # `put_req_header/3` replaces, so repeat the header at the tuple level: a
    # proxy in front of Ankusa can hand us a conn that carries it twice.
    conn =
      conn(ip("10.0.0.9"))
      |> Map.put(:req_headers, [
        {"x-forwarded-for", "1.1.1.1"},
        {"x-forwarded-for", "2.2.2.2"}
      ])

    assert ClientIP.resolve(conn, proxies(["10.0.0.0/8"])) == {:ok, ip("2.2.2.2")}
  end

  test "an IPv6 peer is matched against an IPv6 proxy range" do
    conn = conn(ip("2001:db8::9"), [{"x-forwarded-for", "2001:db8::1"}])

    assert ClientIP.resolve(conn, proxies(["2001:db8::/32"])) == {:ok, ip("2001:db8::1")}
  end

  test "an IPv4-mapped IPv6 peer resolves to its IPv4 form" do
    mapped = {0, 0, 0, 0, 0, 0xFFFF, 0x0A00, 0x0009}

    assert ClientIP.resolve(conn(mapped), proxies(["10.0.0.0/8"])) == {:ok, ip("10.0.0.9")}
  end

  test "a header entry with whitespace is trimmed" do
    conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", " 1.2.3.4 ,10.0.0.7"}])

    assert ClientIP.resolve(conn, proxies(["10.0.0.0/8"])) == {:ok, ip("1.2.3.4")}
  end

  test "a conn with no peer is :error" do
    assert ClientIP.resolve(%Plug.Conn{remote_ip: nil}, []) == :error
  end
end
