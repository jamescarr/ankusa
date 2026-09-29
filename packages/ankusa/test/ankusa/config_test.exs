defmodule Ankusa.ConfigTest do
  @moduledoc """
  `Ankusa.Config` fails fast on a typo instead of silently accepting it or
  clobbering a defaults map — these pin the three bugs the release-readiness
  review found.
  """

  use ExUnit.Case, async: true

  alias Ankusa.Config

  describe "parse_roles!/1" do
    test "splits, trims, and maps to role atoms" do
      assert Config.parse_roles!("edge, dispatch") == [:edge, :dispatch]
    end

    test "raises ArgumentError on an unknown role name" do
      assert_raise ArgumentError, ~r/unknown Ankusa role/, fn ->
        Config.parse_roles!("edgee")
      end
    end
  end

  test "Config.new/1 raises on an unknown nested key" do
    assert_raise ArgumentError, ~r/batcher\.max_queu/, fn ->
      Config.new(batcher: %{max_queu: 1})
    end
  end

  test "Config.new/1 deep-merges a keyword-list section over the defaults" do
    config = Config.new(batcher: [max_queue: 5])

    assert config.batcher.max_batch == 256
    assert config.batcher.max_queue == 5
  end

  describe "the routes section" do
    test "defaults to off, with an ETS store and no trusted proxies" do
      config = Config.new()

      refute config.routes.enabled
      assert config.routes.store == {Ankusa.Routes.Store.ETS, []}
      assert config.routes.trusted_proxies == []
      assert config.routes.admin == %{port: 4003}
    end

    test "merges one nested level, so routes.cache.max_size keeps the other cache keys" do
      config = Config.new(routes: %{cache: %{max_size: 5}})

      assert config.routes.cache.max_size == 5
      assert config.routes.cache.ttl_ms == 30_000
      assert config.routes.cache.gc_interval_ms == 60_000
    end

    test "accepts keyword lists as well as maps" do
      config = Config.new(routes: [enabled: true, trusted_proxies: ["10.0.0.0/8"]])

      assert config.routes.enabled
      assert config.routes.trusted_proxies == ["10.0.0.0/8"]
    end

    test "rejects an unknown routes key, and an unknown key one level down" do
      assert_raise ArgumentError, ~r/routes\.cach\b/, fn ->
        Config.new(routes: %{cach: %{}})
      end

      assert_raise ArgumentError, ~r/routes\.cache\.nope/, fn ->
        Config.new(routes: %{cache: %{nope: 1}})
      end

      assert_raise ArgumentError, ~r/routes\.admin\.prt/, fn ->
        Config.new(routes: [admin: [prt: 4004]])
      end
    end

    test "rejects a routes value that is not a map or keyword list" do
      assert_raise ArgumentError, ~r/routes/, fn -> Config.new(routes: "on") end
      assert_raise ArgumentError, ~r/routes\.cache/, fn -> Config.new(routes: %{cache: 1}) end
    end
  end

  describe "Ankusa.Routes.validate_config!/1" do
    defp validated(routes), do: Ankusa.Routes.validate_config!(Config.new(routes: routes))

    test "accepts the defaults" do
      assert :ok = Ankusa.Routes.validate_config!(Config.new())
    end

    test "rejects a TTL at or past the cache's garbage-collection interval" do
      assert_raise ArgumentError, ~r/routes\.cache\.ttl_ms/, fn ->
        validated(cache: [ttl_ms: 60_000, gc_interval_ms: 60_000])
      end

      assert_raise ArgumentError, ~r/routes\.cache\.max_size/, fn ->
        validated(cache: [max_size: 0])
      end
    end

    test "rejects numeric limits and statuses outside their range" do
      assert_raise ArgumentError, ~r/routes\.max_routes/, fn -> validated(max_routes: 0) end
      assert_raise ArgumentError, ~r/routes\.log_sample/, fn -> validated(log_sample: -1) end

      assert_raise ArgumentError, ~r/routes\.ip_denied_status/, fn ->
        validated(ip_denied_status: 500)
      end

      assert_raise ArgumentError, ~r/routes\.admin\.port/, fn ->
        validated(admin: [port: 65_536])
      end
    end

    test "rejects a store that is not a {module, opts} pair" do
      assert_raise ArgumentError, ~r/routes\.store/, fn -> validated(store: "ets") end
    end

    test "rejects a bad CIDR in the proxies or the global rules" do
      assert_raise ArgumentError, ~r/routes\.trusted_proxies/, fn ->
        validated(trusted_proxies: ["10.0.0.0/8", "nonsense"])
      end

      assert_raise ArgumentError, ~r/routes\.ip_rules\.rules/, fn ->
        validated(ip_rules: [default: :allow, rules: [%{action: :allow, cidr: "10.0.0.0/33"}]])
      end

      assert_raise ArgumentError, ~r/routes\.ip_rules\.default/, fn ->
        validated(ip_rules: [default: :maybe, rules: []])
      end
    end

    test "checks the seed only when routes are enabled" do
      bad_seed = [seed: [%{"id" => "s", "path" => "not-rooted"}]]

      # Disabled: the seed is dead weight, and the admin API that would have
      # created it is off.
      assert :ok = validated(bad_seed)

      assert_raise ArgumentError, ~r/routes\.seed\[0\] is invalid: path/, fn ->
        validated([enabled: true] ++ bad_seed)
      end
    end

    test "rejects seed ids that collide, seeds that collide, and an oversized seed" do
      enabled = [enabled: true]

      assert_raise ArgumentError, ~r/routes\.seed\[2\] reuses route id "s"/, fn ->
        validated(
          enabled ++
            [
              seed: [
                %{"id" => "s", "path" => "/hooks/a"},
                %{"id" => "t", "path" => "/hooks/b"},
                %{"id" => "s", "path" => "/hooks/c"}
              ]
            ]
        )
      end

      assert_raise ArgumentError, ~r/routes\.seed\[1\] conflicts with routes\.seed\[0\]/, fn ->
        validated(
          enabled ++
            [
              seed: [
                %{"id" => "a", "path" => "/hooks/x"},
                %{"id" => "b", "path" => "/hooks/x"}
              ]
            ]
        )
      end

      assert_raise ArgumentError, ~r/more than routes\.max_routes/, fn ->
        validated(
          enabled ++
            [
              max_routes: 1,
              seed: [
                %{"id" => "a", "path" => "/hooks/a"},
                %{"id" => "b", "path" => "/hooks/b"}
              ]
            ]
        )
      end
    end
  end

  describe "wal: :none" do
    test "drops the WAL's reader roles, keeping the rest" do
      assert Config.new(wal: :none).roles == [:edge]

      config = Config.new(wal: :none, roles: [:edge, :dispatch, :storage, :claim_check])
      assert config.roles == [:edge, :claim_check]

      # The default roles only narrow under :none; a disk config is untouched.
      assert Config.new().roles == [:edge, :dispatch, :storage]
    end

    test "raises when no role is left to run the edge" do
      assert_raise ArgumentError, ~r/wal: :none requires the :edge role/, fn ->
        Config.new(wal: :none, roles: [:dispatch])
      end

      assert_raise ArgumentError, ~r/wal: :none requires the :edge role/, fn ->
        Config.new(wal: :none, roles: [:dispatch, :storage])
      end

      # A role the WAL does not serve survives on its own.
      assert Config.new(wal: :none, roles: [:claim_check]).roles == [:claim_check]
    end
  end

  describe "Ankusa.WAL.validate_config!/1" do
    defp wal_config(opts), do: Config.new([wal: :none] ++ opts)

    defp source(sinks) do
      {Ankusa.SourceStore.Static, sources: %{"demo" => [sinks: sinks]}}
    end

    test "is :ok for a disk WAL whatever the sinks" do
      assert :ok = Ankusa.WAL.validate_config!(Config.new())

      assert :ok =
               Ankusa.WAL.validate_config!(
                 Config.new(source_store: source([{Ankusa.Sink.Log, []}]))
               )
    end

    test "requires at least one durable sink per static source" do
      assert_raise ArgumentError, ~r/source "demo": wal: :none acks/, fn ->
        Ankusa.WAL.validate_config!(wal_config(source_store: source([{Ankusa.Sink.Log, []}])))
      end

      # A source that names no sinks gets the Log default, which is not durable.
      assert_raise ArgumentError, ~r/source "demo"/, fn ->
        Ankusa.WAL.validate_config!(
          wal_config(source_store: {Ankusa.SourceStore.Static, sources: %{"demo" => []}})
        )
      end
    end

    test "a durable sink anywhere in the list satisfies it" do
      sinks = [{Ankusa.Sink.Log, []}, {Ankusa.Sink.Http, [url: "http://sink.test"]}]
      assert :ok = Ankusa.WAL.validate_config!(wal_config(source_store: source(sinks)))
    end
  end
end
