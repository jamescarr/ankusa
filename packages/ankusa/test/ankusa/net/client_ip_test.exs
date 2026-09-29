defmodule Ankusa.Net.ClientIPTest do
  @moduledoc """
  Client-IP resolution. The header cases are the whole game: an allowlist that
  reads `X-Forwarded-For` from an untrusted peer is an allowlist the sender
  chooses, and an entry the trusted proxies cannot be shown to have written must
  never turn into the peer's own address — that is the address an attacker wants
  the rules to see.
  """

  use ExUnit.Case, async: true

  alias Ankusa.Net
  alias Ankusa.Net.ClientIP
  alias CIDR

  @trusted ["10.0.0.0/8"]

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

  # `put_req_header/3` replaces, so a repeated header has to be built at the
  # tuple level: a proxy in front of Ankusa appends to `x-forwarded-for`, so a
  # conn can arrive carrying it more than once.
  defp conn_lines(peer, lines) do
    conn(peer)
    |> Map.put(:req_headers, Enum.map(lines, &{"x-forwarded-for", &1}))
  end

  defp ip(text), do: elem(Net.parse(text), 1)

  test "the peer is the client when no proxies are trusted" do
    conn = conn(ip("1.2.3.4"), [{"x-forwarded-for", "9.9.9.9"}])

    assert ClientIP.resolve(conn, []) == {:ok, ip("1.2.3.4")}
  end

  test "an untrusted peer's X-Forwarded-For is ignored entirely" do
    conn = conn(ip("192.168.1.1"), [{"x-forwarded-for", "1.2.3.4"}])

    assert ClientIP.resolve(conn, proxies(@trusted)) == {:ok, ip("192.168.1.1")}
  end

  test "a trusted peer's single entry is the client" do
    conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", "1.2.3.4"}])

    assert ClientIP.resolve(conn, proxies(@trusted)) == {:ok, ip("1.2.3.4")}
  end

  test "a trusted peer's chain resolves to the rightmost untrusted entry" do
    conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", "1.2.3.4, 10.0.0.7"}])

    assert ClientIP.resolve(conn, proxies(@trusted)) == {:ok, ip("1.2.3.4")}
  end

  test "a client-forged prefix never becomes the answer" do
    trusted = proxies(@trusted)

    # Right to left, `10.0.0.7` is what this node's proxy wrote and `1.2.3.4`
    # is the hop it saw; `6.6.6.6` is the client's own claim about itself, to
    # the left of the client, and is never even parsed.
    conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", "6.6.6.6, 1.2.3.4, 10.0.0.7"}])

    assert ClientIP.resolve(conn, trusted) == {:ok, ip("1.2.3.4")}

    # Same chain with junk anywhere in the forged prefix: the prefix is not
    # read, so junk there cannot change the answer (or discard the header).
    for prefix <- ["garbage, ", "garbage, 6.6.6.6, ", "1.2.3.4:abc, "] do
      conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", prefix <> "1.2.3.4, 10.0.0.7"}])

      assert ClientIP.resolve(conn, trusted) == {:ok, ip("1.2.3.4")}, prefix
    end
  end

  test "the rightmost untrusted entry wins even sandwiched between trusted ones" do
    conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", "1.2.3.4, 10.0.0.5, 6.6.6.6, 10.0.0.7"}])

    assert ClientIP.resolve(conn, proxies(@trusted)) == {:ok, ip("6.6.6.6")}
  end

  test "a chain of only trusted entries resolves to the leftmost" do
    conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", "10.0.0.2, 10.0.0.7"}])

    assert ClientIP.resolve(conn, proxies(@trusted)) == {:ok, ip("10.0.0.2")}
  end

  test "an unreadable entry left of the client is never parsed" do
    conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", "garbage, 1.2.3.4"}])

    assert ClientIP.resolve(conn, proxies(@trusted)) == {:ok, ip("1.2.3.4")}
  end

  test "an unreadable entry at or right of the client denies the request" do
    trusted = proxies(@trusted)

    # Rightmost: the chain stops being one we believe immediately.
    assert ClientIP.resolve(
             conn(ip("10.0.0.9"), [{"x-forwarded-for", "1.2.3.4, garbage"}]),
             trusted
           ) == :error

    # The client hop itself, under a trusted proxy.
    assert ClientIP.resolve(
             conn(ip("10.0.0.9"), [{"x-forwarded-for", "1.2.3.4, garbage, 10.0.0.7"}]),
             trusted
           ) == :error

    # Never the proxy's own address — the fail-open this replaced.
    assert ClientIP.resolve(
             conn(ip("10.0.0.9"), [{"x-forwarded-for", "garbage, 1.2.3.4, garbage"}]),
             trusted
           ) == :error
  end

  test "an absent or empty header falls back to the peer" do
    trusted = proxies(@trusted)

    assert ClientIP.resolve(conn(ip("10.0.0.9")), trusted) == {:ok, ip("10.0.0.9")}

    assert ClientIP.resolve(conn(ip("10.0.0.9"), [{"x-forwarded-for", ""}]), trusted) ==
             {:ok, ip("10.0.0.9")}

    assert ClientIP.resolve(conn(ip("10.0.0.9"), [{"x-forwarded-for", " , "}]), trusted) ==
             {:ok, ip("10.0.0.9")}
  end

  test "several X-Forwarded-For lines are joined in the order received" do
    trusted = proxies(@trusted)

    # Two lines are the same chain as one line: the walk does not care how the
    # proxy in front framed them.
    lines = conn_lines(ip("10.0.0.9"), ["6.6.6.6", "1.2.3.4"])
    one_line = conn(ip("10.0.0.9"), [{"x-forwarded-for", "6.6.6.6, 1.2.3.4"}])

    assert ClientIP.resolve(lines, trusted) == {:ok, ip("1.2.3.4")}
    assert ClientIP.resolve(lines, trusted) == ClientIP.resolve(one_line, trusted)

    # Reading only the last line would leave just `10.0.0.7`, a trusted proxy,
    # and resolve to it; the full chain has the client in front of it.
    assert ClientIP.resolve(conn_lines(ip("10.0.0.9"), ["1.2.3.4", "10.0.0.7"]), trusted) ==
             {:ok, ip("1.2.3.4")}
  end

  test "an IPv6 peer is matched against an IPv6 proxy range" do
    conn = conn(ip("2001:db8::9"), [{"x-forwarded-for", "2001:db8::1"}])

    assert ClientIP.resolve(conn, proxies(["2001:db8::/32"])) == {:ok, ip("2001:db8::1")}
  end

  test "an IPv4-mapped IPv6 peer resolves to its IPv4 form" do
    mapped = {0, 0, 0, 0, 0, 0xFFFF, 0x0A00, 0x0009}

    assert ClientIP.resolve(conn(mapped), proxies(@trusted)) == {:ok, ip("10.0.0.9")}
  end

  test "an IPv4-mapped entry resolves to its IPv4 form" do
    conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", "::ffff:1.2.3.4"}])

    assert ClientIP.resolve(conn, proxies(@trusted)) == {:ok, ip("1.2.3.4")}
  end

  test "a mapped entry naming a trusted proxy is recognized as trusted" do
    # `::ffff:10.0.0.7` normalizes to `10.0.0.7`, inside 10.0.0.0/8, so the
    # walk keeps going left and reaches the client.
    conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", "::ffff:10.0.0.7, 6.6.6.6"}])

    assert ClientIP.resolve(conn, proxies(@trusted)) == {:ok, ip("6.6.6.6")}
  end

  test "a header entry with whitespace is trimmed" do
    conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", " 1.2.3.4 ,10.0.0.7"}])

    assert ClientIP.resolve(conn, proxies(@trusted)) == {:ok, ip("1.2.3.4")}
  end

  test "an IPv4 entry may carry a port" do
    conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", "1.2.3.4:5678"}])

    assert ClientIP.resolve(conn, proxies(@trusted)) == {:ok, ip("1.2.3.4")}
  end

  test "an IPv6 entry may be bracketed, with or without a port" do
    trusted = proxies(@trusted)

    for entry <- ["[2001:db8::1]:443", "[2001:db8::1]", "2001:db8::1"] do
      conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", entry}])

      assert ClientIP.resolve(conn, trusted) == {:ok, ip("2001:db8::1")}, entry
    end
  end

  test "an entry whose port cannot be read is unreadable" do
    trusted = proxies(@trusted)

    for entry <- [
          "1.2.3.4:abc",
          "1.2.3.4:99999",
          "1.2.3.4:",
          "1.2.3.4:5678:9",
          "[2001:db8::1]:x",
          "[2001:db8::1]:65536",
          "[::1",
          "[1.2.3.4]"
        ] do
      conn = conn(ip("10.0.0.9"), [{"x-forwarded-for", entry}])

      assert ClientIP.resolve(conn, trusted) == :error, entry
    end
  end

  test "a non-UTF-8 entry denies the request instead of raising" do
    trusted = proxies(@trusted)

    assert ClientIP.resolve(conn(ip("10.0.0.9"), [{"x-forwarded-for", <<0xFF>>}]), trusted) ==
             :error

    # Rightmost, with a readable entry to its left.
    assert ClientIP.resolve(conn_lines(ip("10.0.0.9"), ["10.0.0.7", <<0xFF>>]), trusted) ==
             :error
  end

  test "a conn with no peer is :error" do
    assert ClientIP.resolve(%Plug.Conn{remote_ip: nil}, []) == :error
  end
end
