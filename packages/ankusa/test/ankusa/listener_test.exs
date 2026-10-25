defmodule Ankusa.ListenerTest do
  use ExUnit.Case, async: true

  alias Ankusa.{Config, Listener}

  @tls_dir Path.expand("../fixtures/tls", __DIR__)

  defp tls(overrides) do
    Keyword.merge(
      [
        certfile: Path.join(@tls_dir, "server.pem"),
        keyfile: Path.join(@tls_dir, "server-key.pem")
      ],
      overrides
    )
  end

  describe "bandit_options/2" do
    test "applies the listener limits over Bandit's defaults, plain HTTP without a TLS block" do
      config = Config.new(listener: %{max_connections: 1_000, num_acceptors: 8})
      opts = Listener.bandit_options(config, nil)

      assert opts[:scheme] == :http

      assert opts[:thousand_island_options] == [
               num_acceptors: 8,
               num_connections: 125,
               read_timeout: 10_000
             ]

      assert opts[:http_1_options] == [
               max_header_count: 64,
               max_header_length: 16_384,
               max_requests: 1_000
             ]

      assert opts[:http_2_options] == [enabled: false]
    end

    test "the connection cap is rounded up per acceptor, never below one" do
      assert [num_acceptors: 3, num_connections: 4, read_timeout: _] =
               Listener.bandit_options(
                 Config.new(listener: %{max_connections: 10, num_acceptors: 3}),
                 nil
               )[:thousand_island_options]

      assert [num_acceptors: 4, num_connections: 1, read_timeout: _] =
               Listener.bandit_options(
                 Config.new(listener: %{max_connections: 2, num_acceptors: 4}),
                 nil
               )[:thousand_island_options]
    end

    test "http2 is opt-in" do
      assert Listener.bandit_options(Config.new(listener: %{http2: true}), nil)[:http_2_options] ==
               [enabled: true]
    end

    test "extra Thousand Island options are appended, TLS or not" do
      plain = Config.new([])
      secure = Config.new(listener: %{tls: tls([])})
      extra = [shutdown_timeout: 22_000]

      assert Listener.bandit_options(plain, nil, extra)[:thousand_island_options][
               :shutdown_timeout
             ] == 22_000

      assert Listener.bandit_options(secure, secure.listener.tls, extra)[:thousand_island_options][
               :shutdown_timeout
             ] == 22_000

      refute Keyword.has_key?(
               Listener.bandit_options(plain, nil)[:thousand_island_options],
               :shutdown_timeout
             )
    end

    test "a TLS block turns the listener into https and demands a client certificate under verify_peer" do
      ca = Path.join(@tls_dir, "ca.pem")
      config = Config.new(listener: %{tls: tls(verify: :verify_peer, cacertfile: ca)})
      opts = Listener.bandit_options(config, config.listener.tls)

      assert opts[:scheme] == :https
      assert opts[:certfile] == Path.join(@tls_dir, "server.pem")
      assert opts[:keyfile] == Path.join(@tls_dir, "server-key.pem")

      transport = opts[:thousand_island_options][:transport_options]
      assert transport[:verify] == :verify_peer
      assert transport[:fail_if_no_peer_cert] == true
      assert transport[:cacertfile] == ca
      assert transport[:versions] == [:"tlsv1.2", :"tlsv1.3"]
      refute Keyword.has_key?(transport, :ciphers)
      assert opts[:thousand_island_options][:read_timeout] == 10_000
    end

    test "verify_none sends no client-certificate demand, and ciphers become suites" do
      config = Config.new(listener: %{tls: tls(ciphers: ["ECDHE-RSA-AES256-GCM-SHA384"])})

      transport =
        Listener.bandit_options(config, config.listener.tls)[:thousand_island_options][
          :transport_options
        ]

      assert transport[:verify] == :verify_none
      refute Keyword.has_key?(transport, :fail_if_no_peer_cert)
      assert [%{cipher: :aes_256_gcm, key_exchange: :ecdhe_rsa}] = transport[:ciphers]
    end
  end
end
