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
      assert Map.delete(config.routes.admin, :auth) == %{port: 4003, ip: "127.0.0.1", tls: nil}
    end

    test "listeners bind loopback by default" do
      config = Config.new()
      assert config.admin.ip == "127.0.0.1"
      assert config.claim_check.ip == "127.0.0.1"
      assert config.routes.admin.ip == "127.0.0.1"
    end

    test "listener ip must be a strict IP address" do
      assert_raise ArgumentError, ~r/admin\.ip must be an IP address/, fn ->
        Config.new(admin: [ip: "10"])
      end

      assert_raise ArgumentError, ~r/admin\.ip must be an IP address/, fn ->
        Config.new(admin: [ip: "localhost"])
      end

      for ip <- ["0.0.0.0", "::1", {0, 0, 0, 0}] do
        assert Config.new(admin: [ip: ip]).admin.ip == ip
      end
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

    test "checks max_routes against routes.memory_budget_bytes at boot" do
      assert_raise ArgumentError, ~r/routes\.memory_budget_bytes/, fn ->
        validated(memory_budget_bytes: 0)
      end

      # The default cap fits the default budget; a larger cap does not, and the
      # message names both keys.
      assert :ok = validated(max_routes: 1_000_000)

      error =
        assert_raise ArgumentError, ~r/routes\.max_routes \(10000000\)/, fn ->
          validated(max_routes: 10_000_000)
        end

      assert error.message =~ "routes.memory_budget_bytes (8589934592)"

      assert :ok = validated(max_routes: 10_000_000, memory_budget_bytes: 100 * 1_073_741_824)
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

  describe "Ankusa.Edge.RateLimiter.validate_config!/1" do
    defp validated_limits(rate_limits),
      do: Ankusa.Edge.RateLimiter.validate_config!(Config.new(rate_limits: rate_limits))

    test "accepts the defaults, and mixed integer and fractional rates" do
      assert :ok = Ankusa.Edge.RateLimiter.validate_config!(Config.new())

      assert :ok =
               validated_limits(%{
                 default: %{rate: 0.5, burst: 5},
                 tenants: %{
                   "acme" => %{rate: 100, burst: 200},
                   "globex" => %{rate: 0.5, burst: 1}
                 }
               })
    end

    test "rejects an unknown key in the section, before any value is looked at" do
      assert_raise ArgumentError, ~r/rate_limits\.defualt/, fn ->
        Config.new(rate_limits: [defualt: nil])
      end
    end

    test "rejects a limit that is not rate and burst (and the optional keys), or out of range" do
      for {rate_limits, message} <- [
            {%{default: %{rate: 0, burst: 1}},
             "rate_limits.default.rate must be a number greater than 0, got 0"},
            {%{tenants: %{"acme" => %{rate: 1, burst: 0}}},
             "rate_limits.tenants.acme.burst must be an integer of at least 1, got 0"},
            {%{tenants: %{"acme" => [rate: 1, burst: 1]}},
             "rate_limits.tenants.acme must be a map with :rate and :burst and, optionally, " <>
               ":bytes_per_second, :burst_bytes, :max_body_bytes, :max_queue, :max_queue_bytes, " <>
               "got [rate: 1, burst: 1]"},
            {%{tenants: %{"bad.tenant" => %{rate: 1, burst: 1}}},
             "rate_limits.tenants: tenant id \"bad.tenant\" must match [A-Za-z0-9_-]{1,64}"},
            {%{tenants: []}, "rate_limits.tenants must be a map of tenant id to limit, got []"}
          ] do
        error = assert_raise ArgumentError, fn -> validated_limits(rate_limits) end
        assert error.message == message
      end
    end

    test "rejects a limit carrying a key it does not know" do
      # The message embeds the offending map, whose inspect order is the atom
      # table's, so only the prefix is pinned.
      assert_raise ArgumentError,
                   ~r/^rate_limits\.tenants\.acme must be a map with :rate and :burst and, optionally/,
                   fn ->
                     validated_limits(%{tenants: %{"acme" => %{rate: 1, burst: 1, per: 1}}})
                   end
    end

    test "accepts the admission keys: per ip, node, sources, and a tenant's bytes, body and queue" do
      assert :ok =
               validated_limits(%{
                 per_ip: %{rate: 20, burst: 40},
                 node: %{rate: 0.5, burst: 1},
                 sources: %{default: %{bytes_per_second: 50_000_000.5, burst_bytes: 8_000_000}},
                 default: %{rate: 5, burst: 5, max_queue: 100},
                 tenants: %{
                   "acme" => %{
                     rate: 100,
                     burst: 200,
                     bytes_per_second: 1_000_000,
                     burst_bytes: 2_000_000,
                     max_body_bytes: 8_000_000,
                     max_queue: 1,
                     max_queue_bytes: 1
                   }
                 }
               })

      assert :ok = validated_limits(%{sources: %{default: nil}})
      assert :ok = validated_limits(%{sources: %{}})
    end

    test "the defaults: nothing but a generous per-source byte ceiling, per node" do
      assert %Config{}.rate_limits == %{
               default: nil,
               tenants: %{},
               per_ip: nil,
               node: nil,
               sources: %{default: %{bytes_per_second: 100_000_000}},
               scope: :node
             }
    end

    test "range-checks every admission key" do
      acme = fn extra -> %{tenants: %{"acme" => Map.merge(%{rate: 1, burst: 1}, extra)}} end

      for {rate_limits, message} <- [
            {%{per_ip: %{rate: 0, burst: 1}},
             "rate_limits.per_ip.rate must be a number greater than 0, got 0"},
            {%{per_ip: %{rate: 1, burst: 0}},
             "rate_limits.per_ip.burst must be an integer of at least 1, got 0"},
            {%{per_ip: [rate: 1, burst: 1]},
             "rate_limits.per_ip must be a map with exactly :rate and :burst, " <>
               "got [rate: 1, burst: 1]"},
            {%{node: %{rate: -1, burst: 1}},
             "rate_limits.node.rate must be a number greater than 0, got -1"},
            {%{node: %{rate: 1, burst: 1.5}},
             "rate_limits.node.burst must be an integer of at least 1, got 1.5"},
            {%{sources: %{default: %{bytes_per_second: 0}}},
             "rate_limits.sources.default.bytes_per_second must be a number greater than 0, " <>
               "got 0"},
            {%{sources: %{default: %{bytes_per_second: 1, burst_bytes: 0}}},
             "rate_limits.sources.default.burst_bytes must be an integer of at least 1, got 0"},
            {%{sources: %{default: %{burst_bytes: 5}}},
             "rate_limits.sources.default must be nil or a map with :bytes_per_second and, " <>
               "optionally, :burst_bytes, got %{burst_bytes: 5}"},
            {%{sources: %{default: 5}},
             "rate_limits.sources.default must be nil or a map with :bytes_per_second and, " <>
               "optionally, :burst_bytes, got 5"},
            {%{sources: %{other: %{bytes_per_second: 1}}},
             "rate_limits.sources: unknown key :other; only :default is supported"},
            {%{sources: []}, "rate_limits.sources must be a map, got []"},
            {acme.(%{bytes_per_second: 0}),
             "rate_limits.tenants.acme.bytes_per_second must be a number greater than 0, got 0"},
            {acme.(%{bytes_per_second: "fast"}),
             "rate_limits.tenants.acme.bytes_per_second must be a number greater than 0, " <>
               "got \"fast\""},
            {acme.(%{bytes_per_second: 1, burst_bytes: 0}),
             "rate_limits.tenants.acme.burst_bytes must be an integer of at least 1, got 0"},
            {acme.(%{burst_bytes: 10}),
             "rate_limits.tenants.acme.burst_bytes needs bytes_per_second"},
            {acme.(%{max_body_bytes: 0}),
             "rate_limits.tenants.acme.max_body_bytes must be an integer of at least 1 and at " <>
               "most the instance's max_body_bytes (8000000), got 0"},
            {acme.(%{max_body_bytes: 8_000_001}),
             "rate_limits.tenants.acme.max_body_bytes must be an integer of at least 1 and at " <>
               "most the instance's max_body_bytes (8000000), got 8000001"},
            {acme.(%{max_queue: 0}),
             "rate_limits.tenants.acme.max_queue must be an integer of at least 1, got 0"},
            {acme.(%{max_queue_bytes: -5}),
             "rate_limits.tenants.acme.max_queue_bytes must be an integer of at least 1, got -5"},
            {%{default: %{rate: 1, burst: 1, max_queue: 0.5}},
             "rate_limits.default.max_queue must be an integer of at least 1, got 0.5"}
          ] do
        error = assert_raise ArgumentError, fn -> validated_limits(rate_limits) end
        assert error.message == message
      end

      # The message embeds the offending map, whose inspect order is the atom
      # table's, so only the prefix is pinned.
      assert_raise ArgumentError,
                   ~r/^rate_limits\.per_ip must be a map with exactly :rate and :burst, got/,
                   fn -> validated_limits(%{per_ip: %{rate: 1, burst: 1, extra: 1}}) end
    end

    test "the instance's own max_body_bytes is the ceiling a tenant's may not pass" do
      config =
        Config.new(
          max_body_bytes: 100,
          rate_limits: %{tenants: %{"acme" => %{rate: 1, burst: 1, max_body_bytes: 100}}}
        )

      assert :ok = Ankusa.Edge.RateLimiter.validate_config!(config)

      assert_raise ArgumentError, ~r/max_body_bytes \(100\), got 101/, fn ->
        Config.new(
          max_body_bytes: 100,
          rate_limits: %{tenants: %{"acme" => %{rate: 1, burst: 1, max_body_bytes: 101}}}
        )
        |> Ankusa.Edge.RateLimiter.validate_config!()
      end
    end

    test "a tenant-carrying URL scheme needs a default limit: the URL names the tenant" do
      tenant_path = {Ankusa.RouteResolver.TenantPath, []}

      error =
        assert_raise ArgumentError, fn ->
          Ankusa.Edge.RateLimiter.validate_config!(
            Config.new(
              route_resolver: tenant_path,
              rate_limits: %{tenants: %{"acme" => %{rate: 1, burst: 1}}}
            )
          )
        end

      assert error.message =~ "rate_limits.default is required with routing: tenant_path"

      assert :ok =
               Ankusa.Edge.RateLimiter.validate_config!(
                 Config.new(
                   route_resolver: tenant_path,
                   rate_limits: %{default: %{rate: 50, burst: 100}}
                 )
               )

      # The path scheme names no tenant, so it stays unlimited by default.
      assert :ok = Ankusa.Edge.RateLimiter.validate_config!(Config.new())
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

  describe "Ankusa.Queue.validate_config!/1" do
    defp wal_config(opts), do: Config.new([wal: :none] ++ opts)

    defp source(sinks) do
      {Ankusa.SourceStore.Static, sources: %{"demo" => [sinks: sinks]}}
    end

    test "is :ok for a disk WAL whatever the sinks" do
      assert :ok = Ankusa.Queue.validate_config!(Config.new())

      assert :ok =
               Ankusa.Queue.validate_config!(
                 Config.new(source_store: source([{Ankusa.Sink.Log, []}]))
               )
    end

    test "requires at least one durable sink per static source" do
      assert_raise ArgumentError, ~r/source "demo": wal: :none acks/, fn ->
        Ankusa.Queue.validate_config!(wal_config(source_store: source([{Ankusa.Sink.Log, []}])))
      end

      # A source that names no sinks gets the Log default, which is not durable.
      assert_raise ArgumentError, ~r/source "demo"/, fn ->
        Ankusa.Queue.validate_config!(
          wal_config(source_store: {Ankusa.SourceStore.Static, sources: %{"demo" => []}})
        )
      end
    end

    test "a durable sink anywhere in the list satisfies it" do
      sinks = [{Ankusa.Sink.Log, []}, {Ankusa.Sink.Http, [url: "http://sink.test"]}]
      assert :ok = Ankusa.Queue.validate_config!(wal_config(source_store: source(sinks)))
    end
  end

  describe "wal normalization" do
    test "the removed disk-log tuple raises with a hint" do
      assert_raise ArgumentError, ~r/use wal: :disk/, fn ->
        Config.new(wal: {Ankusa.WAL.DiskLog, []})
      end
    end

    test "an unknown wal value raises" do
      assert_raise ArgumentError, ~r/wal must be :disk or :none/, fn ->
        Config.new(wal: :bogus)
      end
    end

    test "dispatch.poll_ms is no longer a config key" do
      assert_raise ArgumentError, ~r/unknown Ankusa.Config key: dispatch\.poll_ms/, fn ->
        Config.new(dispatch: %{poll_ms: 200})
      end
    end
  end

  describe "Ankusa.Dispatch.Pipeline.validate_config!/1" do
    test "defaults to a 30 s attempt deadline, and accepts it" do
      config = Config.new()

      assert config.dispatch.attempt_timeout_ms == 30_000
      assert :ok = Ankusa.Dispatch.Pipeline.validate_config!(config)
    end

    test "rejects a deadline that is not a positive integer" do
      for bad <- [0, -5, 1.5, nil, "30000"] do
        assert_raise ArgumentError,
                     ~r/dispatch.attempt_timeout_ms must be a positive integer, got #{Regex.escape(inspect(bad))}/,
                     fn ->
                       Ankusa.Dispatch.Pipeline.validate_config!(
                         Config.new(dispatch: %{attempt_timeout_ms: bad})
                       )
                     end
      end
    end

    test "sink_concurrency defaults to 8 and is nil or a positive integer" do
      assert Config.new().dispatch.sink_concurrency == 8

      # More than `concurrency` is not an error: the global cap simply binds first.
      for ok <- [nil, 1, 8, 64] do
        assert :ok =
                 Ankusa.Dispatch.Pipeline.validate_config!(
                   Config.new(dispatch: %{concurrency: 4, sink_concurrency: ok})
                 )
      end

      for bad <- [0, -1, 1.5, "8"] do
        assert_raise ArgumentError,
                     ~r/dispatch.sink_concurrency must be nil or a positive integer, got #{Regex.escape(inspect(bad))}/,
                     fn ->
                       Ankusa.Dispatch.Pipeline.validate_config!(
                         Config.new(dispatch: %{sink_concurrency: bad})
                       )
                     end
      end
    end

    test "tenants.default has the three per-tenant limits, and each is a positive integer" do
      assert Config.new().dispatch.tenants == %{
               default: %{max_inflight: 1024, max_inflight_bytes: 33_554_432, concurrency: 16}
             }

      assert :ok = Ankusa.Dispatch.Pipeline.validate_config!(Config.new())

      for key <- [:max_inflight, :max_inflight_bytes, :concurrency], bad <- [0, -1, 1.5, nil] do
        assert_raise ArgumentError,
                     "dispatch.tenants.default.#{key} must be a positive integer, got #{inspect(bad)}",
                     fn ->
                       Ankusa.Dispatch.Pipeline.validate_config!(
                         Config.new(dispatch: %{tenants: %{default: %{key => bad}}})
                       )
                     end
      end
    end

    test "a tenant's entry overrides some keys, and keeps the built-in default" do
      config =
        Config.new(
          dispatch: %{
            tenants: %{:default => %{max_inflight: 10}, "acme" => %{concurrency: 2}}
          }
        )

      assert config.dispatch.tenants == %{
               :default => %{max_inflight: 10, max_inflight_bytes: 33_554_432, concurrency: 16},
               "acme" => %{concurrency: 2}
             }

      assert :ok = Ankusa.Dispatch.Pipeline.validate_config!(config)

      # a named tenant alone leaves `default` untouched
      assert Config.new(dispatch: %{tenants: %{"acme" => %{}}}).dispatch.tenants.default ==
               Config.new().dispatch.tenants.default
    end

    test "a malformed tenants map is refused, naming the key" do
      for {tenants, message} <- [
            {[], ~r/dispatch.tenants must be a map with a :default entry/},
            {%{"acme" => %{}, default: 5},
             ~r/dispatch.tenants must be a map with a :default entry/},
            {%{"acme" => 5}, ~r/dispatch.tenants\["acme"\] must be a map of limits, got 5/},
            {%{"acme" => %{inflight: 1}},
             ~r/dispatch.tenants\["acme"\] has unknown key :inflight/},
            {%{"acme" => %{concurrency: 0}},
             ~r/dispatch.tenants\["acme"\].concurrency must be a positive integer, got 0/},
            {%{"" => %{concurrency: 1}}, ~r/keys must be :default or a non-empty tenant id/},
            {%{acme: %{concurrency: 1}}, ~r/keys must be :default or a non-empty tenant id/}
          ] do
        assert_raise ArgumentError, message, fn ->
          Ankusa.Dispatch.Pipeline.validate_config!(Config.new(dispatch: %{tenants: tenants}))
        end
      end
    end
  end

  describe "ranges" do
    test "a numeric key out of range fails at Config.new/1, naming the key" do
      assert_raise ArgumentError, "batcher.partitions must be a positive integer, got 0", fn ->
        Config.new(batcher: %{partitions: 0})
      end

      assert_raise ArgumentError, "dispatch.concurrency must be a positive integer, got 0", fn ->
        Config.new(dispatch: %{concurrency: 0})
      end

      assert_raise ArgumentError, "port must be an integer from 0 to 65535, got 70000", fn ->
        Config.new(port: 70_000)
      end

      assert_raise ArgumentError,
                   "batcher.max_delay_ms must be a non-negative integer, got -1",
                   fn ->
                     Config.new(batcher: %{max_delay_ms: -1})
                   end

      for key <- [:read_retries, :read_queue_max, :read_cache_bytes, :max_concurrent_reads] do
        assert_raise ArgumentError,
                     "claim_check.#{key} must be a non-negative integer, got -1",
                     fn -> Config.new(claim_check: %{key => -1}) end
      end

      for bad <- [0, -1, 1.5, "1000"] do
        assert_raise ArgumentError,
                     "claim_check.ref_ttl_ms must be nil or a positive integer, got #{inspect(bad)}",
                     fn -> Config.new(claim_check: %{ref_ttl_ms: bad}) end
      end

      assert %Config{} = Config.new(claim_check: %{ref_ttl_ms: 1})
      assert %Config{} = Config.new(claim_check: %{max_concurrent_reads: 0})

      for bad <- [0, "true", :yes] do
        assert_raise ArgumentError,
                     "claim_check.accept_unsigned_refs must be nil, true or false, got #{inspect(bad)}",
                     fn -> Config.new(claim_check: %{accept_unsigned_refs: bad}) end
      end

      for key <- [
            :store_cooldown_ms,
            :read_deadline_ms,
            :read_timeout_ms,
            :read_max_inflight,
            :read_cache_max_entry_bytes
          ] do
        assert_raise ArgumentError,
                     "claim_check.#{key} must be a positive integer, got 0",
                     fn -> Config.new(claim_check: %{key => 0}) end
      end
    end

    test "a key prefix is empty or slash-terminated segments" do
      assert %Config{} = Config.new(storage: %{key_prefix: "fleet/node-a/"})

      for bad <- ["node-a", "/node-a/", "a//", "a b/"] do
        assert_raise ArgumentError, ~r/storage.key_prefix must be/, fn ->
          Config.new(storage: %{key_prefix: bad})
        end
      end
    end
  end

  describe "the backup section" do
    test "a zero interval or keep fails at Config.new/1, naming the key" do
      assert_raise ArgumentError, "backup.interval_ms must be a positive integer, got 0", fn ->
        Config.new(backup: %{interval_ms: 0})
      end

      assert_raise ArgumentError, "backup.keep must be a positive integer, got 0", fn ->
        Config.new(backup: %{keep: 0})
      end
    end

    test "Ankusa.Store.Backup.validate_config!/1 checks the flag and the store's shape" do
      assert :ok = Ankusa.Store.Backup.validate_config!(Config.new(backup: %{enabled: true}))

      assert_raise ArgumentError, ~s(backup.enabled must be a boolean, got "yes"), fn ->
        Ankusa.Store.Backup.validate_config!(Config.new(backup: %{enabled: "yes"}))
      end

      # The bad value is not echoed: it may carry credentials.
      assert_raise ArgumentError, "backup.blob_store must be nil or {module, opts}", fn ->
        Ankusa.Store.Backup.validate_config!(Config.new(backup: %{blob_store: "s3://secret"}))
      end
    end
  end

  describe "the listener and metrics sections" do
    @tls_dir Path.expand("../fixtures/tls", __DIR__)
    @listener_positive ~w(max_connections num_acceptors read_timeout_ms body_read_timeout_ms
                          request_deadline_ms max_requests_per_connection max_header_count
                          max_header_length)a

    defp tls(overrides \\ []) do
      Keyword.merge(
        [
          certfile: Path.join(@tls_dir, "server.pem"),
          keyfile: Path.join(@tls_dir, "server-key.pem")
        ],
        overrides
      )
    end

    test "defaults: hardened limits, no TLS, a metrics listener that is off" do
      config = Config.new()

      assert %{
               tls: nil,
               max_connections: 20_000,
               read_timeout_ms: 10_000,
               body_read_timeout_ms: 15_000,
               request_deadline_ms: 30_000,
               max_requests_per_connection: 1_000,
               max_header_count: 64,
               max_header_length: 16_384,
               http2: false
             } = config.listener

      assert config.listener.num_acceptors == System.schedulers_online() * 2
      assert config.metrics == %{enabled: false, port: 4004, ip: "127.0.0.1", tls: nil}

      for section <- [config.admin, config.claim_check, config.routes.admin] do
        assert section.tls == nil
      end
    end

    test "an explicit acceptor count is kept" do
      assert Config.new(listener: %{num_acceptors: 7}).listener.num_acceptors == 7
    end

    test "every numeric listener key is a positive integer" do
      for key <- @listener_positive do
        assert_raise ArgumentError,
                     "listener.#{key} must be a positive integer, got 0",
                     fn -> Config.new(listener: %{key => 0}) end
      end
    end

    test "listener.http2 and metrics.enabled are booleans, metrics.port a port, metrics.ip an IP" do
      assert_raise ArgumentError, ~s(listener.http2 must be true or false, got "yes"), fn ->
        Config.new(listener: %{http2: "yes"})
      end

      assert_raise ArgumentError, ~s(metrics.enabled must be true or false, got "yes"), fn ->
        Config.new(metrics: %{enabled: "yes"})
      end

      assert_raise ArgumentError,
                   "metrics.port must be an integer from 0 to 65535, got 70000",
                   fn ->
                     Config.new(metrics: %{port: 70_000})
                   end

      assert_raise ArgumentError, ~r/metrics\.ip must be an IP address/, fn ->
        Config.new(metrics: %{ip: "localhost"})
      end
    end

    test "an unknown listener or metrics key is refused" do
      assert_raise ArgumentError, "unknown Ankusa.Config key: listener.timeout", fn ->
        Config.new(listener: %{timeout: 1})
      end

      assert_raise ArgumentError, "unknown Ankusa.Config key: metrics.token", fn ->
        Config.new(metrics: %{token: "x"})
      end
    end

    test "a TLS block is normalised on every listener that takes one" do
      for {opts, get} <- [
            {[listener: %{tls: tls()}], & &1.listener.tls},
            {[admin: %{tls: tls()}], & &1.admin.tls},
            {[claim_check: %{tls: tls()}], & &1.claim_check.tls},
            {[metrics: %{tls: tls()}], & &1.metrics.tls},
            {[routes: %{admin: %{tls: tls()}}], & &1.routes.admin.tls}
          ] do
        assert %{
                 certfile: certfile,
                 keyfile: keyfile,
                 cacertfile: nil,
                 verify: :verify_none,
                 versions: [:"tlsv1.2", :"tlsv1.3"],
                 ciphers: nil
               } = opts |> Config.new() |> get.()

        assert certfile == Path.join(@tls_dir, "server.pem")
        assert keyfile == Path.join(@tls_dir, "server-key.pem")
      end
    end

    test "verify_peer needs the CA that signs client certificates" do
      ca = Path.join(@tls_dir, "ca.pem")

      assert_raise ArgumentError,
                   ~r/listener\.tls\.cacertfile is required with .*verify_peer/,
                   fn ->
                     Config.new(listener: %{tls: tls(verify: :verify_peer)})
                   end

      tls = Config.new(listener: %{tls: tls(verify: :verify_peer, cacertfile: ca)}).listener.tls
      assert %{verify: :verify_peer, cacertfile: ^ca} = tls
    end

    test "a TLS block with a missing file, unknown key or bad value fails naming the key" do
      assert_raise ArgumentError, "admin.tls.certfile is required when admin.tls is set", fn ->
        Config.new(admin: %{tls: [keyfile: Path.join(@tls_dir, "server-key.pem")]})
      end

      assert_raise ArgumentError, ~r/claim_check\.tls\.keyfile must be a readable file/, fn ->
        Config.new(claim_check: %{tls: tls(keyfile: "/no/such/key.pem")})
      end

      assert_raise ArgumentError, "unknown Ankusa.Config key: metrics.tls.password", fn ->
        Config.new(metrics: %{tls: tls(password: "x")})
      end

      assert_raise ArgumentError,
                   ~r/listener\.tls\.verify must be :verify_peer or :verify_none/,
                   fn ->
                     Config.new(listener: %{tls: tls(verify: :maybe)})
                   end

      assert_raise ArgumentError, ~r/listener\.tls\.versions must be a non-empty list/, fn ->
        Config.new(listener: %{tls: tls(versions: [:tlsv1])})
      end

      assert_raise ArgumentError,
                   ~r/listener\.tls\.ciphers has an unknown cipher suite: "nope"/,
                   fn ->
                     Config.new(listener: %{tls: tls(ciphers: ["nope"])})
                   end

      assert_raise ArgumentError, ~r/listener\.tls must be nil or a map/, fn ->
        Config.new(listener: %{tls: "on"})
      end
    end

    test "known cipher suites are accepted" do
      config = Config.new(listener: %{tls: tls(ciphers: ["ECDHE-RSA-AES256-GCM-SHA384"])})
      assert config.listener.tls.ciphers == ["ECDHE-RSA-AES256-GCM-SHA384"]
    end
  end

  describe "control-plane auth" do
    @tls_dir Path.expand("../fixtures/tls", __DIR__)
    @sha String.duplicate("ab", 32)

    defp public_jwk do
      {_, map} =
        {:ec, "P-256"} |> JOSE.JWK.generate_key() |> JOSE.JWK.to_public() |> JOSE.JWK.to_map()

      map
    end

    defp mtls_tls(section \\ %{}) do
      Map.merge(
        %{
          certfile: Path.join(@tls_dir, "server.pem"),
          keyfile: Path.join(@tls_dir, "server-key.pem"),
          cacertfile: Path.join(@tls_dir, "ca.pem"),
          verify: :verify_peer
        },
        section
      )
    end

    defp token(overrides \\ %{}),
      do: Map.merge(%{id: "ci", sha256: @sha, scopes: ["dlq:read", "tenant:acme"]}, overrides)

    @sections [
      {"admin", :admin},
      {"routes.admin", :routes_admin},
      {"claim_check", :claim_check}
    ]

    # Boot options that start one control plane's listener, with `section_opts`
    # (a map merged into that section) applied.
    defp opts(:admin, section_opts), do: [admin: Map.merge(%{enabled: true}, section_opts)]

    defp opts(:routes_admin, section_opts),
      do: [roles: [:edge], routes: %{enabled: true, admin: section_opts}]

    defp opts(:claim_check, section_opts), do: [roles: [:claim_check], claim_check: section_opts]

    defp auth_of(config, :admin), do: config.admin.auth
    defp auth_of(config, :routes_admin), do: config.routes.admin.auth
    defp auth_of(config, :claim_check), do: config.claim_check.auth

    test "every control plane defaults to mode none with the other blocks empty" do
      config = Config.new()

      for {_key, section} <- @sections do
        assert %{
                 mode: :none,
                 allow_insecure: false,
                 tokens: [],
                 jwt: %{
                   issuer: nil,
                   audience: nil,
                   jwks_url: nil,
                   keys: [],
                   refresh_ms: 300_000,
                   min_refresh_ms: 10_000,
                   leeway_s: 30,
                   scope_claim: "scope",
                   tenants_claim: "ankusa_tenants"
                 },
                 mtls: %{subject_map: %{}}
               } = auth_of(config, section)
      end
    end

    test "refuses none on 0.0.0.0, for every control plane that runs" do
      for {key, section} <- @sections, ip <- ["0.0.0.0", "::", "10.1.2.3", {0, 0, 0, 0}] do
        message =
          ~r/#{Regex.escape(key)}\.auth\.mode is :none but #{Regex.escape(key)}\.ip \(.*\) is not a loopback address/

        assert_raise ArgumentError, message, fn ->
          section |> opts(%{ip: ip}) |> Config.new()
        end
      end
    end

    test "none is accepted on loopback, in either family" do
      for {_key, section} <- @sections,
          ip <- ["127.0.0.1", "127.9.9.9", "::1", "::ffff:127.0.0.1", {127, 0, 0, 1}] do
        assert section |> opts(%{ip: ip}) |> Config.new() |> auth_of(section) |> Map.fetch!(:mode) ==
                 :none
      end
    end

    test "a listener this instance does not run is not held to the rule" do
      assert Config.new(admin: %{enabled: false, ip: "0.0.0.0"}).admin.ip == "0.0.0.0"
      assert Config.new(claim_check: %{ip: "0.0.0.0"}).claim_check.ip == "0.0.0.0"

      assert Config.new(routes: %{enabled: false, admin: %{ip: "0.0.0.0"}}).routes.admin.ip ==
               "0.0.0.0"

      # Route management only runs on the edge.
      assert Config.new(roles: [:dispatch], routes: %{enabled: true, admin: %{ip: "0.0.0.0"}}).routes.enabled
    end

    test "allow_insecure says something in front authenticates, and is the only way to keep none" do
      for {_key, section} <- @sections do
        config =
          section
          |> opts(%{ip: "0.0.0.0", auth: %{mode: :none, allow_insecure: true}})
          |> Config.new()

        assert %{mode: :none, allow_insecure: true} = auth_of(config, section)
      end

      # A client-certificate listener is still not authentication by itself.
      assert_raise ArgumentError, ~r/admin\.auth\.mode is :none but admin\.ip/, fn ->
        Config.new(admin: %{enabled: true, ip: "0.0.0.0", tls: mtls_tls()})
      end
    end

    test "the authenticating modes are accepted on any address" do
      jwt = %{issuer: "https://idp.test/", audience: "ankusa", keys: [public_jwk()]}

      for {_key, section} <- @sections,
          auth <- [
            %{mode: :token, tokens: [token()]},
            %{mode: :jwt, jwt: jwt},
            %{mode: :mtls, mtls: %{subject_map: %{"cn:ops" => ["dlq:read"]}}}
          ] do
        tls = if auth.mode == :mtls, do: mtls_tls(), else: nil
        config = section |> opts(%{ip: "0.0.0.0", tls: tls, auth: auth}) |> Config.new()
        assert auth_of(config, section).mode == auth.mode
      end
    end

    test "mtls needs a verify_peer listener and a subject map" do
      auth = %{mode: :mtls, mtls: %{subject_map: %{"cn:ops" => ["dlq:read"]}}}

      assert_raise ArgumentError,
                   ~r/admin\.auth\.mode: :mtls needs admin\.tls with verify: :verify_peer/,
                   fn ->
                     Config.new(admin: %{auth: auth})
                   end

      assert_raise ArgumentError,
                   ~r/admin\.auth\.mode: :mtls needs admin\.tls with verify: :verify_peer/,
                   fn ->
                     Config.new(
                       admin: %{
                         auth: auth,
                         tls: mtls_tls(%{verify: :verify_none, cacertfile: nil})
                       }
                     )
                   end

      assert_raise ArgumentError,
                   ~r/admin\.auth\.mtls\.subject_map must be a non-empty map/,
                   fn ->
                     Config.new(admin: %{auth: %{mode: :mtls}, tls: mtls_tls()})
                   end

      config = Config.new(admin: %{auth: auth, tls: mtls_tls()})
      assert config.admin.auth.mtls.subject_map == %{"cn:ops" => ["dlq:read"]}
    end

    test "mode token needs tokens, each a unique id, a sha256 and known scopes" do
      assert_raise ArgumentError,
                   ~r/admin\.auth\.tokens must be a non-empty list with admin\.auth\.mode: :token/,
                   fn ->
                     Config.new(admin: %{auth: %{mode: :token}})
                   end

      bad = [
        {token(%{sha256: "nothex"}),
         ~r/admin\.auth\.tokens\[0\]\.sha256 must be the 64-character hex SHA-256/},
        {token(%{id: ""}), ~r/admin\.auth\.tokens\[0\]\.id must be a non-empty printable string/},
        {token(%{scopes: []}), ~r/admin\.auth\.tokens\[0\]\.scopes must be a non-empty list/},
        {token(%{scopes: ["tenant:acme"]}),
         ~r/must name at least one scope besides tenant scopes/},
        {token(%{scopes: ["dlq:read", "admin:all"]}), ~r/has unknown scopes \["admin:all"\]/},
        {Map.put(token(), :secret, "x"),
         ~r/unknown Ankusa.Config key: admin\.auth\.tokens\[0\]\.secret/}
      ]

      for {entry, message} <- bad do
        assert_raise ArgumentError, message, fn ->
          Config.new(admin: %{auth: %{mode: :token, tokens: [entry]}})
        end
      end

      assert_raise ArgumentError, ~r/admin\.auth\.tokens has two entries with the same id/, fn ->
        Config.new(
          admin: %{
            auth: %{mode: :token, tokens: [token(), token(%{sha256: String.duplicate("cd", 32)})]}
          }
        )
      end
    end

    test "a token's sha256 is normalised to lowercase" do
      upper = String.upcase(@sha)
      config = Config.new(admin: %{auth: %{mode: :token, tokens: [token(%{sha256: upper})]}})
      assert [%{sha256: @sha}] = config.admin.auth.tokens
    end

    test "mode jwt needs an issuer, an audience and a key source" do
      jwk = public_jwk()
      ok = %{issuer: "https://idp.test/", audience: "ankusa", keys: [jwk]}

      for {missing, message} <- [
            {:issuer, ~r/admin\.auth\.jwt\.issuer must be set with admin\.auth\.mode: :jwt/},
            {:audience, ~r/admin\.auth\.jwt\.audience must be set with admin\.auth\.mode: :jwt/},
            {:keys, ~r/needs admin\.auth\.jwt\.jwks_url or admin\.auth\.jwt\.keys/}
          ] do
        assert_raise ArgumentError, message, fn ->
          Config.new(admin: %{auth: %{mode: :jwt, jwt: Map.delete(ok, missing)}})
        end
      end

      config = Config.new(admin: %{auth: %{mode: :jwt, jwt: %{ok | audience: ["a", "b"]}}})
      assert config.admin.auth.jwt.audience == ["a", "b"]
    end

    test "the jwks_url is https, or http to a loopback host" do
      ok = %{issuer: "https://idp.test/", audience: "ankusa"}

      for url <- [
            "https://idp.test/jwks.json",
            "http://127.0.0.1:9/jwks",
            "http://localhost/jwks",
            "http://[::1]:9/jwks"
          ] do
        assert Config.new(admin: %{auth: %{mode: :jwt, jwt: Map.put(ok, :jwks_url, url)}}).admin.auth.jwt.jwks_url ==
                 url
      end

      for url <- ["http://idp.test/jwks.json", "ftp://idp.test/jwks", "idp.test/jwks", 42] do
        assert_raise ArgumentError, ~r/admin\.auth\.jwt\.jwks_url must be/, fn ->
          Config.new(admin: %{auth: %{mode: :jwt, jwt: Map.put(ok, :jwks_url, url)}})
        end
      end
    end

    test "jwt keys must be public asymmetric keys, and the numeric keys are range checked" do
      ok = %{issuer: "https://idp.test/", audience: "ankusa", keys: [public_jwk()]}
      {_, private} = {:ec, "P-256"} |> JOSE.JWK.generate_key() |> JOSE.JWK.to_map()

      assert_raise ArgumentError,
                   ~r/admin\.auth\.jwt\.keys\[0\] must be a public RSA, EC or OKP key/,
                   fn ->
                     Config.new(admin: %{auth: %{mode: :jwt, jwt: %{ok | keys: [private]}}})
                   end

      for {key, value} <- [
            refresh_ms: 999,
            min_refresh_ms: -1,
            leeway_s: 3_601,
            scope_claim: "",
            tenants_claim: 1
          ] do
        assert_raise ArgumentError, ~r/admin\.auth\.jwt\.#{key} must be/, fn ->
          Config.new(admin: %{auth: %{mode: :jwt, jwt: Map.put(ok, key, value)}})
        end
      end
    end

    test "an unknown auth key, a bad mode or a bad flag is refused naming the key" do
      assert_raise ArgumentError, "unknown Ankusa.Config key: admin.auth.token", fn ->
        Config.new(admin: %{auth: %{token: "x"}})
      end

      assert_raise ArgumentError, "unknown Ankusa.Config key: claim_check.auth.jwt.secret", fn ->
        Config.new(claim_check: %{auth: %{jwt: %{secret: "x"}}})
      end

      assert_raise ArgumentError,
                   ~r/routes\.admin\.auth\.mode must be one of \[:jwt, :mtls, :none, :token\], got :basic/,
                   fn ->
                     Config.new(routes: %{admin: %{auth: %{mode: :basic}}})
                   end

      assert_raise ArgumentError, ~r/admin\.auth\.allow_insecure must be true or false/, fn ->
        Config.new(admin: %{auth: %{allow_insecure: "yes"}})
      end

      assert_raise ArgumentError, ~r/admin\.auth must be a map or keyword list/, fn ->
        Config.new(admin: %{auth: "jwt"})
      end
    end

    test "an auth block may be a keyword list, like every other section" do
      config =
        Config.new(
          admin: [auth: [mode: :token, tokens: [[id: "ci", sha256: @sha, scopes: ["dlq:read"]]]]]
        )

      assert [%{id: "ci", scopes: ["dlq:read"]}] = config.admin.auth.tokens
    end
  end

  describe "fleet scopes" do
    @store {Ankusa.RateLimiter.Store.Redis, url: "redis://localhost:6399"}

    test "default to node scope with no fleet store" do
      config = Config.new()

      assert config.rate_limits.scope == :node
      assert config.dedupe == %{scope: :node}
      assert config.fleet.store == nil
      assert config.fleet.timeout_ms == 20
    end

    test "a fleet scope needs the fleet store" do
      assert_raise ArgumentError, ~r/rate_limits\.scope: :fleet needs fleet\.store/, fn ->
        Config.new(rate_limits: [scope: :fleet])
      end

      assert_raise ArgumentError, ~r/dedupe\.scope: :fleet needs fleet\.store/, fn ->
        Config.new(dedupe: [scope: :fleet])
      end

      config =
        Config.new(rate_limits: [scope: :fleet], dedupe: [scope: :fleet], fleet: [store: @store])

      assert config.rate_limits.scope == :fleet
      assert config.dedupe.scope == :fleet
    end

    test "scope is :node or :fleet" do
      assert_raise ArgumentError,
                   ~r/rate_limits\.scope must be :node or :fleet, got :cluster/,
                   fn ->
                     Config.new(rate_limits: [scope: :cluster])
                   end

      assert_raise ArgumentError, ~r/dedupe\.scope must be :node or :fleet, got "fleet"/, fn ->
        Config.new(dedupe: [scope: "fleet"])
      end
    end

    test "the call budget, breaker and pending TTL are range checked" do
      for {key, value, message} <- [
            {:timeout_ms, 0, ~r/fleet\.timeout_ms must be an integer from 1 to 1000, got 0/},
            {:timeout_ms, 1_001, ~r/fleet\.timeout_ms must be an integer from 1 to 1000/},
            {:pending_ttl_ms, 0, ~r/fleet\.pending_ttl_ms must be a positive integer/},
            {:breaker_window_ms, -1, ~r/fleet\.breaker_window_ms must be a positive integer/},
            {:breaker_min_calls, 0, ~r/fleet\.breaker_min_calls must be a positive integer/},
            {:breaker_open_ms, 0, ~r/fleet\.breaker_open_ms must be a positive integer/},
            {:breaker_failure_rate, 0, ~r/fleet\.breaker_failure_rate must be a number above 0/},
            {:breaker_failure_rate, 1.5,
             ~r/fleet\.breaker_failure_rate must be a number above 0/},
            {:store, :redis, ~r/fleet\.store must be nil or \{module, keyword\}/}
          ] do
        assert_raise ArgumentError, message, fn -> Config.new(fleet: [{key, value}]) end
      end
    end
  end
end
