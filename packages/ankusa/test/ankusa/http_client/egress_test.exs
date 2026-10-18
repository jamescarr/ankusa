defmodule Ankusa.HttpClient.EgressTest do
  @moduledoc """
  `Ankusa.HttpClient.Egress`: what a policy refuses, what it lets through, what
  `pin/3` says to send. DNS is a stub; no socket is opened.
  """

  use ExUnit.Case, async: true

  alias Ankusa.{Config, HttpClient.Egress}

  @public {93, 184, 216, 34}

  # A resolver from a map of name => addresses. An IP literal answers as
  # `:inet.getaddrs/2` does (in whatever spelling inet parses); any other name
  # outside the zone is NXDOMAIN.
  defp resolver(zone) do
    fn name, family ->
      addresses =
        case :inet.parse_address(name) do
          {:ok, literal} -> [literal]
          {:error, _} -> Map.get(zone, List.to_string(name), [])
        end

      case Enum.filter(addresses, &(family == address_family(&1))) do
        [] -> {:error, :nxdomain}
        matching -> {:ok, matching}
      end
    end
  end

  defp address_family(address), do: if(tuple_size(address) == 4, do: :inet, else: :inet6)

  defp check(url, policy_overrides \\ %{}, zone \\ %{}) do
    policy = Map.merge(Egress.default_policy(), policy_overrides)
    Egress.validate_target(url, policy, resolver: resolver(zone))
  end

  describe "denies loopback" do
    test "IPv4 literals, in every spelling inet accepts" do
      for url <- [
            "https://127.0.0.1/",
            "https://127.1.2.3:4003/hooks",
            "https://2130706433/",
            "https://0x7f.1/",
            "https://0.0.0.0/"
          ] do
        assert {:error, {:egress_denied, {:address, _host, _ip}}} = check(url), url
      end
    end

    test "IPv6 literals, and IPv4-mapped IPv6 as the IPv4 address it stands for" do
      for url <- [
            "https://[::1]/",
            "https://[::]/",
            "https://[::ffff:127.0.0.1]/",
            "https://[::ffff:7f00:1]:4003/"
          ] do
        assert {:error, {:egress_denied, {:address, _host, _ip}}} = check(url), url
      end
    end

    test "a name that resolves to loopback" do
      zone = %{"localhost" => [{127, 0, 0, 1}], "me.example" => [{0, 0, 0, 0, 0, 0, 0, 1}]}

      assert {:error, {:egress_denied, {:address, "localhost", "127.0.0.1"}}} =
               check("https://localhost/", %{}, zone)

      assert {:error, {:egress_denied, {:address, "me.example", "::1"}}} =
               check("https://me.example/", %{}, zone)
    end
  end

  describe "denies metadata" do
    test "the link-local address every cloud serves it on, and the IPv6 one" do
      for url <- ["https://169.254.169.254/latest/meta-data/", "https://[fd00:ec2::254]/"] do
        assert {:error, {:egress_denied, {:address, _host, _ip}}} = check(url), url
      end

      assert {:error, {:egress_denied, {:address, _, "fe80::1"}}} =
               check("https://[fe80::1]/")
    end

    test "metadata.google.internal is refused by name, before any lookup" do
      boom = fn _name, _family -> flunk("resolved a denied host") end

      for host <- [
            "metadata.google.internal",
            "Metadata.Google.Internal.",
            "METADATA.GOOGLE.INTERNAL"
          ] do
        assert {:error, {:egress_denied, {:host, "metadata.google.internal"}}} =
                 Egress.validate_target(
                   "https://#{host}/computeMetadata/v1/",
                   Egress.default_policy(),
                   resolver: boom
                 )
      end
    end

    test "deny_hosts wins over allow_hosts" do
      assert {:error, {:egress_denied, {:host, "vault.internal"}}} =
               check("https://vault.internal/", %{
                 deny_hosts: ["vault.internal"],
                 allow_hosts: ["vault.internal"]
               })
    end
  end

  describe "denies private ranges" do
    test "RFC 1918, carrier-grade NAT and unique-local IPv6" do
      for url <- [
            "https://10.0.0.5/",
            "https://172.16.0.1/",
            "https://172.31.255.254/",
            "https://192.168.1.1/",
            "https://100.64.0.1/",
            "https://[fc00::1]/",
            "https://[fdff::1]/"
          ] do
        assert {:error, {:egress_denied, {:address, _host, _ip}}} = check(url), url
      end
    end

    test "the edges of the ranges are public" do
      for url <- ["https://172.15.255.255/", "https://172.32.0.0/", "https://100.128.0.1/"] do
        assert :ok = check(url), url
      end
    end
  end

  describe "denies rebinding" do
    test "a name with one public and one private record is refused" do
      zone = %{"rebind.example" => [@public, {10, 0, 0, 7}]}

      assert {:error, {:egress_denied, {:address, "rebind.example", "10.0.0.7"}}} =
               check("https://rebind.example/", %{}, zone)
    end

    test "a private AAAA beside a public A is refused: both families are looked up" do
      zone = %{"dual.example" => [@public, {0, 0, 0, 0, 0, 0, 0, 1}]}

      assert {:error, {:egress_denied, {:address, "dual.example", "::1"}}} =
               check("https://dual.example/", %{}, zone)
    end

    test "every call resolves again: a name that moved is caught on the retry" do
      {:ok, answers} = Agent.start_link(fn -> [@public] end)

      flipping = fn _name, family ->
        if family == :inet,
          do: {:ok, Agent.get(answers, & &1)},
          else: {:error, :nxdomain}
      end

      policy = Egress.default_policy()
      assert :ok = Egress.validate_target("https://flip.example/", policy, resolver: flipping)

      Agent.update(answers, fn _ -> [{169, 254, 169, 254}] end)

      assert {:error, {:egress_denied, {:address, "flip.example", "169.254.169.254"}}} =
               Egress.validate_target("https://flip.example/", policy, resolver: flipping)
    end

    test "a resolver that raises or exits is a failed lookup, never an allow" do
      policy = Egress.default_policy()

      for resolver <- [fn _, _ -> raise "dns down" end, fn _, _ -> exit(:timeout) end] do
        assert {:error, {:egress_denied, {:resolve, "x.example", _}}} =
                 Egress.validate_target("https://x.example/", policy, resolver: resolver)
      end
    end

    test "a name that does not resolve is a resolve error, not an allow" do
      assert {:error, {:egress_denied, {:resolve, "nope.example", :nxdomain}}} =
               check("https://nope.example/")
    end
  end

  describe "allows allow_cidrs" do
    test "an internal range the operator opted in to" do
      zone = %{"worker.internal" => [{10, 20, 1, 5}]}
      policy = %{allow_cidrs: ["10.20.0.0/16"]}

      assert :ok = check("https://worker.internal/hooks", policy, zone)
      assert :ok = check("https://10.20.9.9/hooks", policy)

      # ... and only that range
      assert {:error, {:egress_denied, {:address, _, "10.21.0.1"}}} =
               check("https://10.21.0.1/", policy)
    end

    test "an allowed range excepts one deny entry, not the rest of the list" do
      zone = %{"mixed.example" => [{10, 20, 1, 5}, {127, 0, 0, 1}]}

      assert {:error, {:egress_denied, {:address, "mixed.example", "127.0.0.1"}}} =
               check("https://mixed.example/", %{allow_cidrs: ["10.20.0.0/16"]}, zone)
    end

    test "allow_hosts exempts a service name from the address check, not from lookup" do
      zone = %{"worker" => [{172, 18, 0, 3}]}

      assert {:error, {:egress_denied, {:address, "worker", "172.18.0.3"}}} =
               check("https://worker:8080/", %{}, zone)

      assert :ok = check("https://worker:8080/", %{allow_hosts: ["Worker"]}, zone)

      assert {:error, {:egress_denied, {:resolve, "worker", :nxdomain}}} =
               check("https://worker:8080/", %{allow_hosts: ["worker"]}, %{})
    end

    test "an empty deny list turns the address check off" do
      assert :ok = check("https://127.0.0.1/", %{deny_cidrs: []})
    end
  end

  describe "scheme and URL" do
    test "plain http is refused unless the policy allows it" do
      assert {:error, {:egress_denied, {:scheme, "http"}}} = check("http://93.184.216.34/")
      assert :ok = check("http://93.184.216.34/", %{allow_insecure_http: true})
    end

    test "anything but http(s) is refused" do
      for url <- ["ftp://93.184.216.34/", "file:///etc/passwd", "gopher://93.184.216.34/"] do
        assert {:error, {:egress_denied, {kind, _}}} = check(url, %{allow_insecure_http: true})
        assert kind in [:scheme, :url]
      end
    end

    test "a URL with no host, or not a string, is refused" do
      assert {:error, {:egress_denied, {:url, _}}} = check("https:///path")
      assert {:error, {:egress_denied, {:url, _}}} = check("/just/a/path")
      assert {:error, {:egress_denied, {:url, _}}} = check(:not_a_url)
    end

    test "a denial never carries the URL: it may hold credentials" do
      assert {:error, {:egress_denied, reason}} = check("https://user:s3cret@127.0.0.1/?t=tok")
      refute inspect(reason) =~ "s3cret"
      refute inspect(reason) =~ "tok"
    end

    test "a malformed policy fails closed" do
      policy = %{Egress.default_policy() | deny_cidrs: ["not-a-cidr"]}

      assert {:error, {:egress_denied, {:policy, _}}} =
               Egress.validate_target("https://93.184.216.34/", policy)
    end
  end

  describe "pin/3" do
    test "http is rewritten to the checked address, with a host header for the name" do
      zone = %{"hooks.example" => [@public]}
      policy = %{Egress.default_policy() | allow_insecure_http: true}

      assert {:ok, pinned} =
               Egress.pin("http://u:p@hooks.example:8080/a/b?x=1", policy,
                 resolver: resolver(zone)
               )

      assert pinned.url == "http://u:p@93.184.216.34:8080/a/b?x=1"
      assert pinned.headers == [{"host", "hooks.example:8080"}]
      assert pinned.connect_options == []
    end

    test "the default port is left out of the host header" do
      policy = %{Egress.default_policy() | allow_insecure_http: true}

      assert {:ok, %{url: "http://93.184.216.34/", headers: [{"host", "x.example"}]}} =
               Egress.pin("http://x.example/", policy,
                 resolver: resolver(%{"x.example" => [@public]})
               )
    end

    test "https is left alone: the certificate check is what binds the connection to the name" do
      zone = %{"hooks.example" => [@public]}

      assert {:ok, %{url: "https://hooks.example/h", headers: [], connect_options: []}} =
               Egress.pin("https://hooks.example/h", Egress.default_policy(),
                 resolver: resolver(zone)
               )
    end

    test "https is pinned, with the name as the TLS hostname, for a sink that skips verification" do
      zone = %{"hooks.example" => [@public]}

      assert {:ok, pinned} =
               Egress.pin("https://hooks.example/h", Egress.default_policy(),
                 resolver: resolver(zone),
                 pin_https: true
               )

      assert pinned.url == "https://93.184.216.34/h"
      assert pinned.headers == [{"host", "hooks.example"}]
      assert pinned.connect_options == [hostname: "hooks.example"]
    end

    test "an IPv6-only target is pinned with brackets and an inet6 transport" do
      zone = %{"v6.example" => [{0x2001, 0xDB8, 0, 0, 0, 0, 0, 1}]}
      policy = %{Egress.default_policy() | allow_insecure_http: true}

      assert {:ok, pinned} = Egress.pin("http://v6.example:81/", policy, resolver: resolver(zone))
      assert pinned.url == "http://[2001:db8::1]:81/"
      assert pinned.headers == [{"host", "v6.example:81"}]
      assert pinned.connect_options == [transport_opts: [inet6: true]]
    end

    test "an IPv4 address is preferred when a name has both" do
      zone = %{"both.example" => [{0x2001, 0xDB8, 0, 0, 0, 0, 0, 1}, @public]}
      policy = %{Egress.default_policy() | allow_insecure_http: true}

      assert {:ok, %{url: "http://93.184.216.34/", connect_options: []}} =
               Egress.pin("http://both.example/", policy, resolver: resolver(zone))
    end
  end

  describe "validate_config!/1" do
    test "the defaults are valid, and are what Config ships" do
      assert Egress.default_policy() == %Config{}.egress
      assert :ok = Egress.validate_policy!(Egress.default_policy())
      assert %Config{} = Config.new([])
    end

    test "names the key a bad value is under" do
      assert_raise ArgumentError, ~r/egress.deny_cidrs: invalid CIDR "10.0.0.0\/99"/, fn ->
        Config.new(egress: %{deny_cidrs: ["10.0.0.0/99"]})
      end

      assert_raise ArgumentError, ~r/egress.allow_cidrs must be a list of CIDRs/, fn ->
        Config.new(egress: %{allow_cidrs: "10.0.0.0/8"})
      end

      assert_raise ArgumentError,
                   ~r/egress.allow_hosts must be a list of non-empty hostnames/,
                   fn ->
                     Config.new(egress: %{allow_hosts: [""]})
                   end

      assert_raise ArgumentError, ~r/egress.allow_insecure_http must be true or false/, fn ->
        Config.new(egress: %{allow_insecure_http: "yes"})
      end

      assert_raise ArgumentError, ~r/unknown Ankusa.Config key: egress.allow_everything/, fn ->
        Config.new(egress: %{allow_everything: true})
      end
    end

    test "an IPv4-mapped CIDR is refused with the reason" do
      assert_raise ArgumentError, ~r/egress.deny_cidrs.*::ffff:0:0\/96/s, fn ->
        Config.new(egress: %{deny_cidrs: ["::ffff:10.0.0.0/104"]})
      end
    end

    test "an egress section keeps the default keys it does not set" do
      config = Config.new(egress: %{allow_cidrs: ["10.0.0.0/8"]})

      assert config.egress.allow_cidrs == ["10.0.0.0/8"]
      assert config.egress.deny_cidrs == Egress.default_policy().deny_cidrs
      assert config.egress.allow_insecure_http == false
    end

    test "rejects verify_none on any HTTP sink the config names" do
      insecure = [
        url: "https://hooks.example/",
        req_options: [connect_options: [transport_opts: [verify: :verify_none]]]
      ]

      lifecycle = Config.new(lifecycle: %{sinks: [{Ankusa.Sink.Http, insecure}]})
      assert_raise ArgumentError, ~r/verify_none/, fn -> Egress.validate_config!(lifecycle) end

      source =
        Config.new(
          source_store:
            {Ankusa.SourceStore.Static,
             sources: %{"s" => [sinks: [{Ankusa.Sink.Http, insecure}]]}}
        )

      assert_raise ArgumentError, ~r/verify_none/, fn -> Egress.validate_config!(source) end

      # the one way through is the explicit, per-sink flag
      allowed =
        Config.new(
          lifecycle: %{sinks: [{Ankusa.Sink.Http, [{:allow_insecure_tls, true} | insecure]}]}
        )

      assert :ok = Egress.validate_config!(allowed)
    end

    test "a cacertfile that is not a file fails at load" do
      config =
        Config.new(
          lifecycle: %{
            sinks: [{Ankusa.Sink.Http, url: "https://h.example/", cacertfile: "/nope/ca.pem"}]
          }
        )

      assert_raise ArgumentError, ~r/cacertfile "\/nope\/ca.pem" is not a readable file/, fn ->
        Egress.validate_config!(config)
      end
    end
  end
end
