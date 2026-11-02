# The IPv6 fleet-replay test starts a real socket on `::1`; a machine without
# IPv6 loopback (a CI image with it off) skips it rather than failing the suite.
exclude =
  case :gen_tcp.listen(0, [:inet6, ip: {0, 0, 0, 0, 0, 0, 0, 1}]) do
    {:ok, socket} ->
      :gen_tcp.close(socket)
      []

    {:error, _reason} ->
      [:ipv6]
  end

ExUnit.start(exclude: exclude)
