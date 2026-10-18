defmodule Ankusa.HttpClientTest do
  @moduledoc """
  `Ankusa.HttpClient` against a real socket: what Finch's connection pool
  does when it is exhausted, which no `Req.Test` plug reaches.
  """

  # `capture_io(:stderr, ...)` is VM-wide.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Ankusa.HttpClient

  defmodule SlowPlug do
    @behaviour Plug
    import Plug.Conn

    @impl true
    def init(opts), do: opts

    @impl true
    def call(conn, _opts) do
      Process.sleep(300)
      send_resp(conn, 200, "ok")
    end
  end

  test "an exhausted connection pool is a transport error, never a raise" do
    port = Ankusa.TestHelpers.free_port()
    start_supervised!({Bandit, plug: SlowPlug, scheme: :http, ip: {127, 0, 0, 1}, port: port})
    url = "http://127.0.0.1:#{port}/"
    test = self()

    # A connect timeout nobody else uses gets this test a pool of its own (50
    # connections); 70 requests that each hold one for 300 ms with a 20 ms
    # checkout timeout must leave some without one. Req warns on the
    # top-level `pool_timeout` key, which is why stderr is captured.
    capture_io(:stderr, fn ->
      results =
        1..70
        |> Task.async_stream(
          fn _ ->
            HttpClient.request(:get, url, [], nil, 5_000,
              connect_options: [timeout: 7_777],
              pool_timeout: 20
            )
          end,
          max_concurrency: 70,
          timeout: 10_000
        )
        |> Enum.map(fn {:ok, result} -> result end)

      send(test, {:results, results})
    end)

    assert_received {:results, results}
    assert Enum.count(results, &match?({:ok, 200, "ok"}, &1)) >= 50

    errors = Enum.reject(results, &match?({:ok, 200, _}, &1))
    assert errors != []
    assert Enum.all?(errors, &match?({:error, %RuntimeError{}}, &1))
  end

  describe "TLS verification" do
    defmodule Ok do
      @moduledoc false
      @behaviour Plug
      import Plug.Conn

      @impl true
      def init(opts), do: opts

      @impl true
      def call(conn, _opts), do: send_resp(conn, 200, "ok")
    end

    @insecure [connect_options: [transport_opts: [verify: :verify_none]]]

    # An https listener whose certificate (SAN `localhost`) chains to a private CA
    # nobody trusts by default; `ca` is that CA's PEM bundle.
    setup do
      %{client_config: client, server_config: server} =
        :public_key.pkix_test_data(%{
          # sha256 + P-256: what a TLS 1.3 handshake accepts (the test data's
          # default digest, sha1, gets `no_suitable_signature_algorithm`).
          server_chain: %{
            root: [digest: :sha256, key: {:namedCurve, :secp256r1}],
            peer: [
              digest: :sha256,
              key: {:namedCurve, :secp256r1},
              extensions: [{:Extension, {2, 5, 29, 17}, false, [{:dNSName, ~c"localhost"}]}]
            ]
          },
          client_chain: %{root: [], peer: []}
        })

      dir = Path.join(System.tmp_dir!(), "ankusa-tls-#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      {key_type, key_der} = server[:key]
      cert = Path.join(dir, "cert.pem")
      key = Path.join(dir, "key.pem")
      ca = Path.join(dir, "ca.pem")

      File.write!(cert, :public_key.pem_encode([{:Certificate, server[:cert], :not_encrypted}]))
      File.write!(key, :public_key.pem_encode([{key_type, key_der, :not_encrypted}]))

      File.write!(
        ca,
        :public_key.pem_encode(
          for der <- client[:cacerts], do: {:Certificate, der, :not_encrypted}
        )
      )

      port = Ankusa.TestHelpers.free_port()

      start_supervised!(
        {Bandit,
         plug: Ok, scheme: :https, ip: {127, 0, 0, 1}, port: port, certfile: cert, keyfile: key}
      )

      %{port: port, ca: ca}
    end

    test "a certificate the system does not trust is refused", %{port: port} do
      assert {:error, reason} =
               HttpClient.request(:get, "https://localhost:#{port}/", [], nil, 5_000)

      assert inspect(reason) =~ "unknown_ca"
    end

    test "cacertfile names a private CA to verify against", %{port: port, ca: ca} do
      assert {:ok, 200, "ok"} =
               HttpClient.request(:get, "https://localhost:#{port}/", [], nil, 5_000,
                 cacertfile: ca
               )
    end

    test "the hostname is verified too: the right CA is not enough", %{port: port, ca: ca} do
      assert {:error, reason} =
               HttpClient.request(:get, "https://127.0.0.1:#{port}/", [], nil, 5_000,
                 cacertfile: ca
               )

      assert inspect(reason) =~ "handshake_failure" or inspect(reason) =~ "hostname" or
               inspect(reason) =~ "bad_cert"
    end

    test "rejects verify_none", %{port: port} do
      assert_raise ArgumentError, ~r/verify_none/, fn ->
        HttpClient.request(:get, "https://localhost:#{port}/", [], nil, 5_000, @insecure)
      end

      assert_raise ArgumentError, ~r/verify_none/, fn -> HttpClient.validate!(@insecure) end
      assert :ok = HttpClient.validate!(@insecure, true)
    end

    test "allow_insecure_tls connects to a certificate nobody vouches for", %{port: port} do
      assert {:ok, 200, "ok"} =
               HttpClient.request(:get, "https://localhost:#{port}/", [], nil, 5_000,
                 allow_insecure_tls: true
               )
    end
  end
end
