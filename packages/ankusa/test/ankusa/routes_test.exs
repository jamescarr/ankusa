defmodule Ankusa.RoutesTest do
  @moduledoc """
  The route-management context: what the guard decides, what the dry run
  reports, and the definition rules the stores deliberately don't enforce.

  Every test boots a real instance, so the store, the snapshot, and the decision
  cache are the ones a request would hit. Telemetry assertions keep the whole
  module `async: false`: `:telemetry` handlers are global, and a concurrent suite
  would show up in the event stream.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Net
  alias Ankusa.Routes
  alias Ankusa.Routes.{Matcher, Route}

  defp start(routes_opts), do: start_routes(routes_opts)

  defp seed(routes \\ [{"s", "/hooks/s"}]) do
    Enum.map(routes, fn {id, path} -> %{"id" => id, "path" => path} end)
  end

  defp ip(text), do: elem(Net.parse(text), 1)

  defp authorize(config, method, path, ip_text \\ "1.2.3.4") do
    {:ok, segments} = Matcher.normalize(String.split(path, "/", trim: true), path)
    Routes.authorize(config.instance, method, segments, ip(ip_text))
  end

  defp create(instance, attrs), do: Routes.create(instance, attrs)

  defp decide(config, method, path, ip_text) do
    segments = String.split(path, "/", trim: true)
    Routes.authorize_path(config.instance, method, segments, path, ip(ip_text))
  end

  # Decide the seeded route once, so a decision is in the cache, and prove it is.
  defp prime(config) do
    assert {{:ok, "s"}, false} = decide(config, "POST", "/hooks/s", "1.2.3.4")
    assert {{:ok, "s"}, true} = decide(config, "POST", "/hooks/s", "1.2.3.4")
  end

  # Freeze the store, start every function, and let the store go only once each of
  # them is waiting on it. Each writer has then read and validated against the SAME
  # table — the worst interleaving there is — and nothing about it depends on
  # scheduler timing: an unguarded store lets every writer win, a version-checked
  # one lets exactly the right ones, on every run.
  defp hold_store(config, funs) do
    store = Ankusa.whereis(config.instance, :routes_store)
    :sys.suspend(store)

    tasks = Enum.map(funs, &Task.async/1)
    await_queue(store, length(funs))
    :sys.resume(store)

    Task.await_many(tasks, 30_000)
  end

  # Wait until `pid` has exactly `count` store calls queued: each writer's one call,
  # made after it has read the snapshot. Only calls are counted, so an unrelated
  # message landing in a frozen store's mailbox cannot throw the count off.
  defp await_queue(pid, count, attempts \\ 500) do
    cond do
      queued_calls(pid) == count ->
        :ok

      attempts == 0 ->
        flunk("the store never had #{count} calls waiting")

      true ->
        Process.sleep(10)
        await_queue(pid, count, attempts - 1)
    end
  end

  defp queued_calls(pid) do
    {:messages, messages} = Process.info(pid, :messages)
    Enum.count(messages, &match?({:"$gen_call", _from, _request}, &1))
  end

  defmodule AlwaysStale do
    @moduledoc false
    # A store that answers every versioned write with `:stale`, the way a store
    # does when it keeps losing to a faster writer, and counts the attempts.

    @behaviour Ankusa.Routes.Store

    def child_spec(opts), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}

    @impl true
    def start_link(opts) do
      instance = Keyword.fetch!(opts, :instance)
      {:ok, pid} = Agent.start_link(fn -> 0 end, name: Ankusa.via(instance, :routes_store))

      Ankusa.Routes.Snapshot.publish(%{
        instance: instance,
        routes: %{},
        ip_rules: %{default: :allow, rules: []},
        version: 1
      })

      {:ok, pid}
    end

    def attempts(instance), do: Agent.get(Ankusa.via(instance, :routes_store), & &1)

    @impl true
    def insert(instance, _route, _version), do: stale(instance)

    @impl true
    def replace(instance, _route, _version), do: stale(instance)

    @impl true
    def delete(_instance, _id), do: {:error, :not_found}

    @impl true
    def put_ip_rules(_instance, _ip_rules), do: :ok

    defp stale(instance) do
      Agent.update(Ankusa.via(instance, :routes_store), &(&1 + 1))
      {:error, :stale}
    end
  end

  describe "boot" do
    test "a config seed is live straight after boot" do
      config =
        start(seed: [%{"id" => "s", "path" => "/hooks/s", "metadata" => %{"owner" => "acme"}}])

      assert {:ok, %Route{} = route} = Routes.get(config.instance, "s")
      assert route.id == "s"
      assert route.path == "/hooks/s"
      assert route.metadata == %{"owner" => "acme"}
      assert route.methods == ["POST"]
      assert route.enabled
      assert route.ip_rules == []
    end

    test "seeds publish a snapshot the guard can read" do
      config = start(seed: [%{"id" => "s", "path" => "/hooks/s"}])
      meta = Routes.meta(config.instance)

      assert meta.version == 1
      assert config.instance |> Ankusa.Routes.Snapshot.routes(meta) |> Enum.map(& &1.id) == ["s"]
    end

    test "routes off means no store and no snapshot" do
      config = start(enabled: false)

      assert Routes.enabled?(config.instance) == false
      assert Routes.meta(config.instance) == nil
    end
  end

  describe "seed validation" do
    test "a disabled seed route never conflicts with an earlier enabled one" do
      config =
        start(
          seed: [
            %{"id" => "a", "path" => "/hooks/x"},
            %{"id" => "b", "path" => "/hooks/x", "enabled" => false}
          ]
        )

      assert {:ok, %Route{enabled: true}} = Routes.get(config.instance, "a")
      assert {:ok, %Route{enabled: false}} = Routes.get(config.instance, "b")
    end

    test "nor with a later one: the order a seed is listed in does not matter" do
      config =
        start(
          seed: [
            %{"id" => "a", "path" => "/hooks/x", "enabled" => false},
            %{"id" => "b", "path" => "/hooks/x"}
          ]
        )

      assert {:ok, %Route{enabled: false}} = Routes.get(config.instance, "a")
      assert {:ok, %Route{enabled: true}} = Routes.get(config.instance, "b")
    end

    test "two enabled seed routes for one path and method still fail boot" do
      config =
        test_config(
          routes: [
            enabled: true,
            seed: [%{"id" => "a", "path" => "/hooks/x"}, %{"id" => "b", "path" => "/hooks/x"}]
          ]
        )

      assert_raise ArgumentError, ~r/routes\.seed\[1\] conflicts with routes\.seed\[0\]/, fn ->
        Routes.validate_config!(config)
      end
    end

    test "routes.enabled must be a boolean" do
      config = test_config(routes: [enabled: "false"])

      assert_raise ArgumentError, ~r/routes\.enabled must be true or false/, fn ->
        Routes.validate_config!(config)
      end
    end
  end

  describe "authorize/4" do
    test "matches an enabled route, and distinguishes a miss from a method mismatch" do
      config = start(seed: seed())

      assert authorize(config, "POST", "/hooks/s") == {:ok, "s"}
      assert authorize(config, "GET", "/hooks/s") == {:reject, :method}
      assert authorize(config, "POST", "/nope") == {:reject, :no_route}
      assert authorize(config, "POST", "/hooks/s/extra") == {:reject, :no_route}
    end

    test "a disabled route captures nothing" do
      config = start(seed: [%{"id" => "s", "path" => "/hooks/s", "enabled" => false}])

      assert authorize(config, "POST", "/hooks/s") == {:reject, :no_route}

      assert {:ok, _} = Routes.update(config.instance, "s", %{"enabled" => true})
      assert authorize(config, "POST", "/hooks/s") == {:ok, "s"}
    end

    test "a param route matches one segment and a wildcard one or more" do
      config =
        start(
          seed: [
            %{"id" => "p", "path" => "/hooks/:tenant/gh", "methods" => ["POST"]},
            %{"id" => "w", "path" => "/hooks/shopify/*"}
          ]
        )

      assert authorize(config, "POST", "/hooks/acme/gh") == {:ok, "p"}
      assert authorize(config, "POST", "/hooks/shopify/a/b") == {:ok, "w"}
      assert authorize(config, "POST", "/hooks/shopify") == {:reject, :no_route}
    end

    test "a global deny rule rejects, and a global default deny is the floor" do
      deny =
        start(
          ip_rules: [default: :allow, rules: [%{action: :deny, cidr: "10.0.0.0/8"}]],
          seed: seed()
        )

      assert authorize(deny, "POST", "/hooks/s", "10.1.2.3") == {:reject, :ip_denied}
      assert authorize(deny, "POST", "/hooks/s", "1.2.3.4") == {:ok, "s"}

      allow =
        start(
          ip_rules: [default: :deny, rules: [%{action: :allow, cidr: "10.0.0.0/8"}]],
          seed: seed()
        )

      assert authorize(allow, "POST", "/hooks/s", "1.2.3.4") == {:reject, :ip_denied}
      assert authorize(allow, "POST", "/hooks/s", "10.1.2.3") == {:ok, "s"}
    end

    test "a route's own ip_rules replace the global allow list" do
      config =
        start(
          seed: [
            %{
              "id" => "s",
              "path" => "/hooks/s",
              "ip_rules" => [%{"action" => "allow", "cidr" => "192.168.0.0/16"}]
            }
          ]
        )

      assert authorize(config, "POST", "/hooks/s", "192.168.1.1") == {:ok, "s"}
      assert authorize(config, "POST", "/hooks/s", "1.2.3.4") == {:reject, :ip_denied}
    end

    test "a global deny beats a route's own allow" do
      config =
        start(
          ip_rules: [default: :allow, rules: [%{action: :deny, cidr: "10.0.0.0/8"}]],
          seed: [
            %{
              "id" => "s",
              "path" => "/hooks/s",
              "ip_rules" => [%{"action" => "allow", "cidr" => "10.0.0.0/8"}]
            }
          ]
        )

      assert authorize(config, "POST", "/hooks/s", "10.1.2.3") == {:reject, :ip_denied}
    end

    test "an IPv4-mapped IPv6 client is matched as IPv4" do
      config = start(seed: seed())

      assert authorize(config, "POST", "/hooks/s", "::ffff:1.2.3.4") == {:ok, "s"}
    end
  end

  describe "authorize_path/5 and the decision cache" do
    test "reports a cache hit on the second identical decision" do
      config = start(seed: seed())
      client = ip("1.2.3.4")

      assert {{:ok, "s"}, false} =
               Routes.authorize_path(config.instance, "POST", ["hooks", "s"], "/hooks/s", client)

      assert {{:ok, "s"}, true} =
               Routes.authorize_path(config.instance, "POST", ["hooks", "s"], "/hooks/s", client)

      assert {{:reject, :no_route}, false} =
               Routes.authorize_path(config.instance, "POST", ["nope"], "/nope", client)

      assert {{:reject, :no_route}, true} =
               Routes.authorize_path(config.instance, "POST", ["nope"], "/nope", client)
    end

    test "creating a route resolves a previously-rejected path immediately" do
      config = start(seed: seed())

      assert authorize(config, "POST", "/hooks/new") == {:reject, :no_route}

      assert {:ok, %Route{id: "n"}} =
               create(config.instance, %{"id" => "n", "path" => "/hooks/new"})

      # Epoch-keyed: no TTL wait, no cache delete pass.
      assert authorize(config, "POST", "/hooks/new") == {:ok, "n"}
    end

    test "an unmatchable request path is a rejection, not a crash" do
      config = start(seed: seed())
      client = ip("1.2.3.4")

      assert {{:reject, :no_route}, false} =
               Routes.authorize_path(
                 config.instance,
                 "POST",
                 ["hooks", "a%2Fb"],
                 "/hooks/a%2Fb",
                 client
               )

      assert {{:reject, :no_route}, false} =
               Routes.authorize_path(
                 config.instance,
                 "POST",
                 ["hooks", ".."],
                 "/hooks/..",
                 client
               )
    end
  end

  describe "a cached decision never outlives the table it was made against" do
    setup do
      %{config: start(seed: seed())}
    end

    test "a delete", %{config: config} do
      prime(config)
      assert :ok = Routes.delete(config.instance, "s")
      assert {{:reject, :no_route}, false} = decide(config, "POST", "/hooks/s", "1.2.3.4")
    end

    test "a disable", %{config: config} do
      prime(config)
      assert {:ok, _} = Routes.update(config.instance, "s", %{"enabled" => false})
      assert {{:reject, :no_route}, false} = decide(config, "POST", "/hooks/s", "1.2.3.4")
    end

    test "a PUT that moves the route", %{config: config} do
      prime(config)
      assert {:ok, _} = Routes.replace(config.instance, "s", %{"path" => "/hooks/moved"})
      assert {{:reject, :no_route}, false} = decide(config, "POST", "/hooks/s", "1.2.3.4")
      assert {{:ok, "s"}, false} = decide(config, "POST", "/hooks/moved", "1.2.3.4")
    end

    test "a global deny", %{config: config} do
      prime(config)

      assert {:ok, _} =
               Routes.put_ip_rules(config.instance, %{
                 "default" => "allow",
                 "rules" => [%{"action" => "deny", "cidr" => "1.2.3.0/24"}]
               })

      assert {{:reject, :ip_denied}, false} = decide(config, "POST", "/hooks/s", "1.2.3.4")
    end

    test "a table whose version counter starts over (a flushed Redis reloaded)", %{config: config} do
      instance = config.instance
      assert {:ok, _} = create(instance, %{"id" => "a", "path" => "/hooks/a"})
      assert {{:ok, "a"}, false} = decide(config, "POST", "/hooks/a", "1.2.3.4")
      assert {{:ok, "a"}, true} = decide(config, "POST", "/hooks/a", "1.2.3.4")

      # The store publishes an empty table at version 1 again, as a Redis store
      # does when its namespace was flushed and reloaded. (The process owns the
      # table, so the republish runs inside it.)
      store = Ankusa.whereis(instance, :routes_store)

      :sys.replace_state(store, fn state ->
        state = %{state | routes: %{}, version: 1}
        :ok = Ankusa.Routes.Snapshot.publish(state)
        state
      end)

      # Walk the new counter back up to the version the stale entry was cached under.
      assert {:ok, _} = create(instance, %{"id" => "b", "path" => "/hooks/b"})

      assert {{:reject, :no_route}, false} = decide(config, "POST", "/hooks/a", "1.2.3.4")
    end
  end

  describe "the decision cache's key bound" do
    test "cacheable?/1 admits at most 16 segments and 256 bytes of path" do
      cacheable? = &Ankusa.Routes.Cache.cacheable?/1

      assert cacheable?.([])
      assert cacheable?.(List.duplicate("a", 16))
      refute cacheable?.(List.duplicate("a", 17))
      assert cacheable?.([String.duplicate("a", 256)])
      refute cacheable?.([String.duplicate("a", 257)])
      assert cacheable?.([String.duplicate("a", 128), String.duplicate("b", 128)])
      refute cacheable?.([String.duplicate("a", 128), String.duplicate("b", 129)])
    end

    test "an over-long or over-deep path is decided but never cached" do
      config = start(seed: seed())

      for segments <- [[String.duplicate("a", 300)], Enum.map(1..17, &"s#{&1}")] do
        path = "/" <> Enum.join(segments, "/")
        client = ip("1.2.3.4")

        assert {{:reject, :no_route}, false} =
                 Routes.authorize_path(config.instance, "POST", segments, path, client)

        assert {{:reject, :no_route}, false} =
                 Routes.authorize_path(config.instance, "POST", segments, path, client)
      end

      # A path of ordinary size still is.
      prime(config)
    end
  end

  describe "dry_run/2 and the decision cache" do
    test "a dry run leaves no cache entry behind" do
      config = start(seed: seed())

      assert {:ok, %{decision: :allow, reason: :matched}} =
               Routes.dry_run(config.instance, %{
                 "method" => "POST",
                 "path" => "/hooks/s",
                 "ip" => "1.2.3.4"
               })

      # Had the dry run cached its answer, this first real decision would be a hit.
      assert {{:ok, "s"}, false} = decide(config, "POST", "/hooks/s", "1.2.3.4")
    end
  end

  describe "IP rules under a cached route match" do
    test "the match is cached, but every sender is still checked against the route's rules" do
      config =
        start(
          seed: [
            %{
              "id" => "pinned",
              "path" => "/hooks/p",
              "ip_rules" => [%{"action" => "allow", "cidr" => "203.0.113.0/24"}]
            }
          ]
        )

      assert {{:ok, "pinned"}, false} = decide(config, "POST", "/hooks/p", "203.0.113.7")
      # Same method and path, so the match is a cache hit. The sender is not.
      assert {{:reject, :ip_denied}, true} = decide(config, "POST", "/hooks/p", "198.51.100.9")
      assert {{:ok, "pinned"}, true} = decide(config, "POST", "/hooks/p", "203.0.113.8")
    end

    test "a route with only deny rules refuses every sender they do not name" do
      config =
        start(
          seed: [
            %{
              "id" => "d",
              "path" => "/hooks/d",
              "ip_rules" => [%{"action" => "deny", "cidr" => "203.0.113.0/24"}]
            }
          ]
        )

      assert {{:reject, :ip_denied}, false} = decide(config, "POST", "/hooks/d", "203.0.113.7")
      # Declaring rules replaces the global allow list, so what they do not name is
      # refused as well: a route that pins ranges is closed to everyone else.
      assert {{:reject, :ip_denied}, true} = decide(config, "POST", "/hooks/d", "198.51.100.9")
    end

    test "a global default of deny is a floor a route's allow rule cannot lift" do
      config =
        start(
          seed: [
            %{
              "id" => "p",
              "path" => "/hooks/p",
              "ip_rules" => [%{"action" => "allow", "cidr" => "203.0.113.0/24"}]
            }
          ],
          ip_rules: [default: :deny, rules: []]
        )

      assert {{:reject, :ip_denied}, false} = decide(config, "POST", "/hooks/p", "203.0.113.7")
    end
  end

  describe "concurrent writes" do
    test "creating one id: exactly one wins and the rest conflict" do
      config = start([])

      results =
        hold_store(
          config,
          for i <- 1..20 do
            fn -> create(config.instance, %{"id" => "same", "path" => "/hooks/p#{i}"}) end
          end
        )

      assert Enum.count(results, &match?({:ok, %Route{id: "same"}}, &1)) == 1
      assert Enum.count(results, &(&1 == {:error, {:conflict, "same"}})) == 19
      assert {:ok, %{routes: [%Route{id: "same"}]}} = Routes.list(config.instance)
    end

    test "creating one enabled path and method: exactly one wins" do
      config = start([])

      results =
        hold_store(
          config,
          for i <- 1..20 do
            fn -> create(config.instance, %{"id" => "r#{i}", "path" => "/hooks/x"}) end
          end
        )

      assert Enum.count(results, &match?({:ok, %Route{}}, &1)) == 1
      assert Enum.count(results, &match?({:error, {:conflict, _}}, &1)) == 19
      assert {:ok, %{routes: [_only]}} = Routes.list(config.instance)
    end

    test "creating distinct ids: every one lands, though all 20 validated against one table" do
      config = start([])

      # The worst case for the retry loop: 19 of the 20 lose the first round and the
      # last one loses 19 times before it wins. The bound has to outlast that.
      results =
        hold_store(
          config,
          for i <- 1..20 do
            fn -> create(config.instance, %{"id" => "r#{i}", "path" => "/hooks/r#{i}"}) end
          end
        )

      assert Enum.all?(results, &match?({:ok, %Route{}}, &1))
      assert {:ok, %{routes: routes}} = Routes.list(config.instance)
      assert length(routes) == 20
    end

    test "two PATCHes of different fields both land" do
      config = start([])
      assert {:ok, _} = create(config.instance, %{"id" => "r", "path" => "/hooks/r"})

      results =
        hold_store(config, [
          fn -> Routes.update(config.instance, "r", %{"enabled" => false}) end,
          fn -> Routes.update(config.instance, "r", %{"methods" => ["POST", "PUT"]}) end
        ])

      assert Enum.all?(results, &match?({:ok, %Route{}}, &1))

      assert {:ok, %Route{enabled: false, methods: ["POST", "PUT"]}} =
               Routes.get(config.instance, "r")
    end

    test "a PATCH that read the route before it was deleted never brings it back" do
      config = start([])
      assert {:ok, _} = create(config.instance, %{"id" => "r", "path" => "/hooks/r"})
      store = Ankusa.whereis(config.instance, :routes_store)

      # The DELETE reaches the store first. The PATCH reads the route while the store
      # is frozen, so the delete has not happened yet, and reaches the store second:
      # the order in which an unguarded upsert resurrects what was just deleted.
      :sys.suspend(store)
      deleter = Task.async(fn -> Routes.delete(config.instance, "r") end)
      await_queue(store, 1)
      patcher = Task.async(fn -> Routes.update(config.instance, "r", %{"enabled" => false}) end)
      await_queue(store, 2)
      :sys.resume(store)

      assert Task.await(deleter) == :ok
      assert Task.await(patcher) == {:error, :not_found}
      assert Routes.get(config.instance, "r") == {:error, :not_found}
    end

    test "a write that keeps losing gives up as store_unavailable instead of spinning" do
      config = start(store: {AlwaysStale, []})

      assert create(config.instance, %{"id" => "x", "path" => "/hooks/x"}) ==
               {:error, :store_unavailable}

      assert AlwaysStale.attempts(config.instance) > 1
    end
  end

  describe "a refused write" do
    test "changes nothing and announces nothing" do
      config = start(max_routes: 1, seed: seed())
      handler = "routes-refused-#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach(
        handler,
        [:ankusa, :routes, :changed],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      before = Routes.meta(config.instance)

      assert create(config.instance, %{"id" => "x", "path" => "/hooks/x"}) ==
               {:error, :too_many_routes}

      assert create(config.instance, %{"id" => "s", "path" => "/hooks/other"}) ==
               {:error, {:conflict, "s"}}

      assert {:error, {:invalid, "path", _message}} =
               create(config.instance, %{"id" => "y", "path" => "/hooks/a b"})

      assert Routes.delete(config.instance, "nope") == {:error, :not_found}

      assert {:error, {:invalid, "default", _message}} =
               Routes.put_ip_rules(config.instance, %{"default" => "maybe", "rules" => []})

      refused = Routes.meta(config.instance)
      assert {refused.version, refused.epoch} == {before.version, before.epoch}
      refute_receive {:telemetry, _, _, _}
    end
  end

  describe "dry_run/2" do
    setup do
      config =
        start(
          seed: [
            %{"id" => "s", "path" => "/hooks/s"},
            %{
              "id" => "pinned",
              "path" => "/hooks/pinned",
              "ip_rules" => [%{"action" => "allow", "cidr" => "192.168.0.0/16"}]
            }
          ],
          ip_rules: [default: :allow, rules: [%{action: :deny, cidr: "10.0.0.0/8"}]]
        )

      %{config: config}
    end

    test "reports a match and the route it matched", %{config: config} do
      assert {:ok, result} =
               Routes.dry_run(config.instance, %{
                 "method" => "POST",
                 "path" => "/hooks/s",
                 "ip" => "1.2.3.4"
               })

      assert result == %{decision: :allow, reason: :matched, route_id: "s", ip_rule: nil}
    end

    test "reports no route, a method mismatch, and a global rule", %{config: config} do
      assert {:ok, %{decision: :deny, reason: :no_route, route_id: nil, ip_rule: nil}} =
               Routes.dry_run(config.instance, %{
                 "method" => "POST",
                 "path" => "/nope",
                 "ip" => "1.2.3.4"
               })

      assert {:ok, %{decision: :deny, reason: :method, route_id: nil}} =
               Routes.dry_run(config.instance, %{
                 "method" => "GET",
                 "path" => "/hooks/s",
                 "ip" => "1.2.3.4"
               })

      assert {:ok,
              %{
                decision: :deny,
                reason: :ip_denied,
                ip_rule: %{action: :deny, cidr: "10.0.0.0/8", scope: "global"}
              }} =
               Routes.dry_run(config.instance, %{
                 "method" => "POST",
                 "path" => "/hooks/s",
                 "ip" => "10.1.2.3"
               })
    end

    test "names the route and its own rule when the route denied the sender", %{config: config} do
      # `pinned` allows only 192.168.0.0/16, so a sender outside it is denied with
      # no rule to name — the denial is the absence of a match.
      assert {:ok, outside} =
               Routes.dry_run(config.instance, %{
                 "method" => "POST",
                 "path" => "/hooks/pinned",
                 "ip" => "1.2.3.4"
               })

      assert outside == %{
               decision: :deny,
               reason: :ip_denied,
               route_id: "pinned",
               ip_rule: nil
             }

      {:ok, _} =
        Routes.create(config.instance, %{
          "id" => "banned",
          "path" => "/hooks/banned",
          "ip_rules" => [%{"action" => "deny", "cidr" => "203.0.113.0/24"}]
        })

      assert {:ok, denied_by_route} =
               Routes.dry_run(config.instance, %{
                 "method" => "POST",
                 "path" => "/hooks/banned",
                 "ip" => "203.0.113.7"
               })

      assert denied_by_route == %{
               decision: :deny,
               reason: :ip_denied,
               route_id: "banned",
               ip_rule: %{action: :deny, cidr: "203.0.113.0/24", scope: "route"}
             }

      assert {:ok, allowed_by_route} =
               Routes.dry_run(config.instance, %{
                 "method" => "POST",
                 "path" => "/hooks/pinned",
                 "ip" => "192.168.1.1"
               })

      assert allowed_by_route == %{
               decision: :allow,
               reason: :matched,
               route_id: "pinned",
               ip_rule: %{action: :allow, cidr: "192.168.0.0/16", scope: "route"}
             }
    end

    test "validates its input", %{config: config} do
      assert {:error, {:invalid, "ip", _message}} =
               Routes.dry_run(config.instance, %{
                 "method" => "POST",
                 "path" => "/hooks/s",
                 "ip" => "nonsense"
               })

      assert {:error, {:invalid, "path", _message}} =
               Routes.dry_run(config.instance, %{
                 "method" => "POST",
                 "path" => "/hooks/a%2Fb",
                 "ip" => "1.2.3.4"
               })

      assert {:error, {:invalid, "method", _message}} =
               Routes.dry_run(config.instance, %{"path" => "/hooks/s", "ip" => "1.2.3.4"})

      assert {:error, {:invalid, "request", _message}} = Routes.dry_run(config.instance, "nope")
    end
  end

  describe "create/replace/update/delete" do
    test "a route past max_routes is refused and nothing is evicted" do
      config = start(max_routes: 2)

      assert {:ok, %Route{id: "a"}} =
               create(config.instance, %{"id" => "a", "path" => "/hooks/a"})

      assert {:ok, %Route{id: "b"}} =
               create(config.instance, %{"id" => "b", "path" => "/hooks/b"})

      assert create(config.instance, %{"id" => "c", "path" => "/hooks/c"}) ==
               {:error, :too_many_routes}

      assert {:ok, %{routes: routes}} = Routes.list(config.instance)
      assert Enum.map(routes, & &1.id) == ["a", "b"]
      assert authorize(config, "POST", "/hooks/a") == {:ok, "a"}
    end

    test "an enabled route cannot duplicate another enabled route's path and method" do
      config = start([])

      assert {:ok, %Route{id: "a"}} =
               create(config.instance, %{"id" => "a", "path" => "/hooks/x"})

      assert create(config.instance, %{"id" => "b", "path" => "/hooks/x"}) ==
               {:error, {:conflict, "a"}}

      # The same path for a method the first route does not serve is not a
      # collision, and neither is a disabled sibling.
      assert {:ok, %Route{id: "c"}} =
               create(config.instance, %{
                 "id" => "c",
                 "path" => "/hooks/x",
                 "methods" => ["PUT"]
               })

      assert {:ok, %Route{id: "d"}} =
               create(config.instance, %{
                 "id" => "d",
                 "path" => "/hooks/x",
                 "enabled" => false
               })

      # Disabling the holder frees the path up.
      assert {:ok, _} = Routes.update(config.instance, "a", %{"enabled" => false})

      assert {:ok, %Route{id: "e"}} =
               create(config.instance, %{"id" => "e", "path" => "/hooks/x"})
    end

    test "an id that is already taken is a conflict" do
      config = start(seed: seed())

      assert create(config.instance, %{"id" => "s", "path" => "/hooks/other"}) ==
               {:error, {:conflict, "s"}}
    end

    test "PUT replaces wholesale, keeps inserted_at, and creates when it does not exist" do
      config = start(seed: seed())
      {:ok, existing} = Routes.get(config.instance, "s")

      Process.sleep(1_100)

      assert {:ok, %Route{} = replaced} =
               Routes.replace(config.instance, "s", %{"path" => "/hooks/moved"})

      assert replaced.inserted_at == existing.inserted_at
      assert replaced.updated_at > existing.updated_at
      assert replaced.path == "/hooks/moved"
      assert authorize(config, "POST", "/hooks/moved") == {:ok, "s"}

      assert {:ok, %Route{id: "fresh"}} =
               Routes.replace(config.instance, "fresh", %{"path" => "/hooks/fresh"})

      assert authorize(config, "POST", "/hooks/fresh") == {:ok, "fresh"}
    end

    test "PUT past max_routes is refused for a new id but not for an existing one" do
      config = start(max_routes: 1, seed: seed())

      assert Routes.replace(config.instance, "new", %{"path" => "/hooks/new"}) ==
               {:error, :too_many_routes}

      assert {:ok, %Route{id: "s"}} =
               Routes.replace(config.instance, "s", %{"path" => "/hooks/s2"})
    end

    test "PATCH changes only the mutable fields" do
      config = start(seed: seed())

      assert {:ok, %Route{enabled: false, path: "/hooks/s"}} =
               Routes.update(config.instance, "s", %{"enabled" => false})

      assert {:error, {:invalid, "path", "immutable; use PUT"}} =
               Routes.update(config.instance, "s", %{"path" => "/hooks/moved"})

      assert {:error, {:invalid, "id", "immutable; use PUT"}} =
               Routes.update(config.instance, "s", %{"id" => "other"})

      assert {:error, {:invalid, "foo", "unknown field"}} =
               Routes.update(config.instance, "s", %{"foo" => 1})

      assert {:error, :not_found} = Routes.update(config.instance, "nope", %{"enabled" => true})
    end

    test "DELETE removes the route, and a second DELETE is not_found" do
      config = start(seed: seed())

      assert :ok = Routes.delete(config.instance, "s")
      assert {:error, :not_found} = Routes.get(config.instance, "s")
      assert {:error, :not_found} = Routes.delete(config.instance, "s")
      assert authorize(config, "POST", "/hooks/s") == {:reject, :no_route}
    end

    test "a disabled route never captures, even when its path is requested" do
      config = start(seed: seed())
      assert {:ok, _} = Routes.update(config.instance, "s", %{"enabled" => false})
      assert authorize(config, "POST", "/hooks/s") == {:reject, :no_route}

      # Re-enabling takes effect on the next request, with no restart.
      assert {:ok, _} = Routes.update(config.instance, "s", %{"enabled" => true})
      assert authorize(config, "POST", "/hooks/s") == {:ok, "s"}
    end
  end

  describe "list/2" do
    test "filters by enabled and paginates by id" do
      config = start([])

      for id <- ["a", "b", "c"] do
        {:ok, _} = create(config.instance, %{"id" => id, "path" => "/hooks/#{id}"})
      end

      {:ok, _} = Routes.update(config.instance, "b", %{"enabled" => false})

      assert {:ok, %{routes: all, next_cursor: nil}} = Routes.list(config.instance)
      assert Enum.map(all, & &1.id) == ["a", "b", "c"]

      assert {:ok, %{routes: enabled}} = Routes.list(config.instance, enabled: true)
      assert Enum.map(enabled, & &1.id) == ["a", "c"]

      assert {:ok, %{routes: disabled}} = Routes.list(config.instance, enabled: false)
      assert Enum.map(disabled, & &1.id) == ["b"]

      assert {:ok, %{routes: [%Route{id: "a"}], next_cursor: "a"}} =
               Routes.list(config.instance, limit: 1)

      assert {:ok, %{routes: [%Route{id: "c"}], next_cursor: nil}} =
               Routes.list(config.instance, limit: 1, cursor: "b")

      # Clamped, never a crash and never an unbounded page.
      assert {:ok, %{routes: routes}} = Routes.list(config.instance, limit: 0)
      assert length(routes) == 1
    end
  end

  describe "ip_rules/1 and put_ip_rules/2" do
    test "replaces the global list and default" do
      config = start(seed: seed())

      assert Routes.ip_rules(config.instance) == %{default: :allow, rules: []}

      assert {:ok, rules} =
               Routes.put_ip_rules(config.instance, %{
                 "default" => "deny",
                 "rules" => [%{"action" => "allow", "cidr" => "10.0.0.0/8"}]
               })

      assert rules.default == :deny
      assert [%{action: :allow, cidr: cidr}] = rules.rules
      assert to_string(cidr) == "10.0.0.0/8"
      assert Routes.ip_rules(config.instance) == rules

      assert authorize(config, "POST", "/hooks/s", "10.1.2.3") == {:ok, "s"}
      assert authorize(config, "POST", "/hooks/s", "1.2.3.4") == {:reject, :ip_denied}
    end

    test "rejects a malformed rule, naming the index" do
      config = start(seed: seed())

      assert {:error, {:invalid, "rules", "rule 0: invalid cidr \"10.0.0.0/33\""}} =
               Routes.put_ip_rules(config.instance, %{
                 "default" => "allow",
                 "rules" => [%{"action" => "allow", "cidr" => "10.0.0.0/33"}]
               })

      assert {:error, {:invalid, "default", _message}} =
               Routes.put_ip_rules(config.instance, %{"default" => "maybe"})
    end

    test "both default and rules are required: nothing is filled in on the caller's behalf" do
      config = start(seed: seed())

      assert {:ok, _} =
               Routes.put_ip_rules(config.instance, %{"default" => "deny", "rules" => []})

      assert {:error, {:invalid, "default", "is required"}} =
               Routes.put_ip_rules(config.instance, %{
                 "rules" => [%{"action" => "allow", "cidr" => "10.0.0.0/8"}]
               })

      assert {:error, {:invalid, "rules", "is required"}} =
               Routes.put_ip_rules(config.instance, %{"default" => "allow"})

      assert {:error, {:invalid, "default", "is required"}} =
               Routes.put_ip_rules(config.instance, %{})

      # An omitted default used to mean allow, which would have opened this list.
      assert Routes.ip_rules(config.instance).default == :deny
    end
  end

  describe "an IPv4-mapped range written as a rule or a trusted proxy" do
    test "the API refuses it and says what to write instead" do
      config = start(seed: seed())

      assert {:error, {:invalid, "rules", message}} =
               Routes.put_ip_rules(config.instance, %{
                 "default" => "allow",
                 "rules" => [%{"action" => "deny", "cidr" => "::ffff:10.0.0.0/104"}]
               })

      # It parses, so nothing about it looks wrong: the message has to explain.
      assert message =~ ~s(rule 0: invalid cidr "::ffff:10.0.0.0/104")
      assert message =~ "IPv4 CIDR"

      # The rules that were in force are untouched.
      assert Routes.ip_rules(config.instance) == %{default: :allow, rules: []}
    end

    test "a route's own rules refuse it the same way" do
      assert {:error, {:invalid, "ip_rules", message}} =
               Route.from_attrs(%{
                 "path" => "/hooks/x",
                 "ip_rules" => [%{"action" => "allow", "cidr" => "::ffff:10.0.0.0/104"}]
               })

      assert message =~ "IPv4 CIDR"
    end

    test "boot refuses it as a trusted proxy, with the same explanation" do
      config = test_config(routes: [enabled: true, trusted_proxies: ["::ffff:10.0.0.0/104"]])

      assert_raise ArgumentError,
                   ~r/must be CIDRs, got "::ffff:10\.0\.0\.0\/104": .*IPv4 CIDR/,
                   fn ->
                     Routes.validate_config!(config)
                   end
    end

    test "an ordinary bad CIDR keeps its plain message, with no explanation attached" do
      config = test_config(routes: [enabled: true, trusted_proxies: ["10.0.0.0/33"]])

      error =
        assert_raise ArgumentError, fn -> Routes.validate_config!(config) end

      assert error.message == ~s(routes.trusted_proxies entries must be CIDRs, got "10.0.0.0/33")
    end
  end

  describe "the JSON wire form" do
    test "a route survives to_json/from_json, rules and metadata included" do
      {:ok, route} =
        Route.from_attrs(%{
          "id" => "r",
          "path" => "/hooks/:tenant/r",
          "methods" => ["POST", "PUT"],
          "enabled" => false,
          "ip_rules" => [
            %{"action" => "allow", "cidr" => "10.0.0.0/8"},
            %{"action" => "deny", "cidr" => "10.1.0.0/16"}
          ],
          "metadata" => %{"owner" => "acme", "nested" => %{"n" => 1}}
        })

      json = Route.to_json(route)

      # The admin API's response body and the Redis store's value are this map,
      # so a definition must come back exactly as it went out.
      assert {:ok, ^route} = Route.from_json(json)
    end

    test "from_json refuses a value it cannot trust" do
      assert {:error, {:invalid, field, _message}} = Route.from_json(%{"inserted_at" => "nope"})
      assert field == "inserted_at"

      assert {:error, {:invalid, "route", _message}} = Route.from_json("not a map")
    end

    test "from_json refuses a definition with no id instead of minting one" do
      json = %{
        "path" => "/hooks/x",
        "inserted_at" => "2026-01-01T00:00:00Z",
        "updated_at" => "2026-01-01T00:00:00Z"
      }

      assert {:error, {:invalid, "id", "is required"}} = Route.from_json(json)
    end

    test "an id that is not a string is refused, not replaced by a generated one" do
      for id <- [123, false, %{}, ["a"]] do
        assert {:error, {:invalid, "id", _message}} =
                 Route.from_attrs(%{"id" => id, "path" => "/hooks/x"})
      end
    end

    test "a trailing newline does not slip past the id or method rules" do
      assert {:error, {:invalid, "id", _message}} =
               Route.from_attrs(%{"id" => "stripe\n", "path" => "/hooks/x"})

      assert {:error, {:invalid, "methods", _message}} =
               Route.from_attrs(%{"path" => "/hooks/x", "methods" => ["POST\n"]})
    end
  end

  describe "telemetry" do
    test "every mutation emits exactly one routes :changed event" do
      config = start([])
      handler = "routes-changed-#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach_many(
        handler,
        [[:ankusa, :routes, :changed]],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      {:ok, _} = create(config.instance, %{"id" => "a", "path" => "/hooks/a"})

      assert_receive {:telemetry, [:ankusa, :routes, :changed], %{},
                      %{instance: instance, action: :insert, route_id: "a", version: 2}}

      assert instance == config.instance

      {:ok, _} = Routes.update(config.instance, "a", %{"enabled" => false})
      assert_receive {:telemetry, _, _, %{action: :replace, route_id: "a", version: 3}}

      {:ok, _} = Routes.put_ip_rules(config.instance, %{"default" => "deny", "rules" => []})
      assert_receive {:telemetry, _, _, %{action: :ip_rules, route_id: nil, version: 4}}

      :ok = Routes.delete(config.instance, "a")
      assert_receive {:telemetry, _, _, %{action: :delete, route_id: "a", version: 5}}

      refute_receive {:telemetry, _, _, _}
    end
  end

  describe "the snapshot table" do
    test "after 1,000 API creates the first and the last route still resolve" do
      config = start(max_routes: 2_000)

      for i <- 1..1_000 do
        assert {:ok, _} = create(config.instance, %{"id" => "r#{i}", "path" => "/hooks/r#{i}"})
      end

      assert authorize(config, "POST", "/hooks/r1") == {:ok, "r1"}
      assert authorize(config, "POST", "/hooks/r1000") == {:ok, "r1000"}
      assert Ankusa.Routes.Snapshot.count(config.instance) == 1_000
    end

    test "a routes store that crashes comes back with the routes it had, API-created ones included" do
      config = start(seed: seed())
      instance = config.instance
      edge = Ankusa.whereis(instance, :edge)
      store = Ankusa.whereis(instance, :routes_store)

      assert {:ok, _} = create(instance, %{"id" => "api", "path" => "/hooks/api"})
      version = Ankusa.Routes.Snapshot.meta(instance).version

      ref = Process.monitor(store)
      Process.exit(store, :kill)
      assert_receive {:DOWN, ^ref, :process, _, :killed}

      # The supervisor restarts the store, and everything after it, while
      # handling the exit; a sys call returns once that is done.
      _ = :sys.get_state(edge)
      new_store = Ankusa.whereis(instance, :routes_store)
      assert new_store != store

      assert authorize(config, "POST", "/hooks/api") == {:ok, "api"}
      assert authorize(config, "POST", "/hooks/s") == {:ok, "s"}
      assert Ankusa.Routes.Snapshot.meta(instance).version == version
      assert :ets.info(Ankusa.Routes.Snapshot.table(instance), :owner) == new_store

      # The adopted store can still write the table.
      assert {:ok, _} = create(instance, %{"id" => "after", "path" => "/hooks/after"})
      assert authorize(config, "POST", "/hooks/after") == {:ok, "after"}
    end

    test "a route whose path is replaced matches its new path and not its old one" do
      config = start([])
      assert {:ok, _} = create(config.instance, %{"id" => "r", "path" => "/hooks/old"})
      assert authorize(config, "POST", "/hooks/old") == {:ok, "r"}

      assert {:ok, _} = Routes.replace(config.instance, "r", %{"path" => "/hooks/new/:id"})

      assert authorize(config, "POST", "/hooks/new/1") == {:ok, "r"}
      assert authorize(config, "POST", "/hooks/old") == {:reject, :no_route}
    end

    test "readers see one generation or the other while a whole table is republished" do
      config = start(max_routes: 10_000)
      instance = config.instance
      store = Ankusa.whereis(instance, :routes_store)

      table = fn tag ->
        routes =
          for i <- 1..5_000, into: %{} do
            {:ok, route} =
              Route.from_attrs(%{"id" => "#{tag}#{i}", "path" => "/hooks/#{tag}/#{i}"})

            {route.id, route}
          end

        {:ok, probe} = Route.from_attrs(%{"id" => "probe", "path" => "/hooks/probe/:id"})
        Map.put(routes, "probe", probe)
      end

      # A Redis-style reload: the owner writes a whole new generation.
      republish = fn routes ->
        :sys.replace_state(store, fn state ->
          state = %{state | routes: routes, version: state.version + 1}
          :ok = Ankusa.Routes.Snapshot.publish(state)
          state
        end)
      end

      republish.(table.("a"))
      assert authorize(config, "POST", "/hooks/probe/1") == {:ok, "probe"}

      stop = :atomics.new(1, [])

      readers =
        for _ <- 1..8 do
          Task.async(fn ->
            # The first answer that is not the probe, or the last one once the
            # republishing is over.
            Stream.repeatedly(fn ->
              {authorize(config, "POST", "/hooks/probe/7"), :atomics.get(stop, 1)}
            end)
            |> Enum.find(fn {result, stopped} -> result != {:ok, "probe"} or stopped == 1 end)
            |> elem(0)
          end)
        end

      for tag <- ["b", "c", "d"], do: republish.(table.(tag))
      :atomics.put(stop, 1, 1)

      assert Enum.map(readers, &Task.await(&1, 30_000)) == List.duplicate({:ok, "probe"}, 8)
      assert authorize(config, "POST", "/hooks/d/5000") == {:ok, "d5000"}
    end
  end
end
