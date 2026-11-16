defmodule Ankusa.Gateway.AuthzEngineTest do
  @moduledoc """
  The engine's caching (`Ankusa.Gateway.Authz.decide/4` over `:inline`,
  `:cached` and `:remote` authorizers): keys, TTLs, refresh-ahead, stale
  answers on an outage, the deadline, and the share-link restriction.
  """

  use ExUnit.Case, async: false

  @moduletag :capture_log

  import Ankusa.TestHelpers

  alias Ankusa.Gateway.{Authn, Authz, Cache}
  alias Ankusa.Gateway.Authz.Request
  alias Ankusa.Principal

  defmodule Stub do
    @moduledoc false
    @behaviour Ankusa.Gateway.Authorizer

    @impl true
    def init!(opts, key),
      do: Map.put(opts, :remote, Ankusa.Gateway.Authz.Remote.options!(opts, key))

    @impl true
    def kind(%{kind: kind}), do: kind

    @impl true
    def decide(request, %{test: test, tag: tag, answer: answer}, _ctx) do
      send(test, {:decided, tag, request.resource.key})
      Agent.get(answer, & &1).(request)
    end
  end

  defmodule Scoped do
    @moduledoc false
    @behaviour Ankusa.Gateway.Authorizer
    alias Ankusa.Gateway.AuthzEngineTest.Stub

    @impl true
    def init!(opts, key), do: Stub.init!(opts, key)

    @impl true
    def kind(state), do: Stub.kind(state)

    @impl true
    def decide(request, state, ctx), do: Stub.decide(request, state, ctx)

    # Every key under one folder is one decision.
    @impl true
    def cache_scope(request, _state), do: request.resource.key |> Path.dirname()
  end

  setup do
    answer = start_supervised!({Agent, fn -> fn _request -> {:ok, :allow, %{}} end end})
    %{answer: answer}
  end

  defp boot(%{answer: answer}, authorizers, steps, authz \\ %{}) do
    config = test_config(roles: [])
    start_supervised!({Ankusa.Instance, config})

    authorizers =
      Map.new(authorizers, fn {name, {module, kind, extra}} ->
        {name, {module, Map.merge(%{kind: kind, test: self(), tag: name, answer: answer}, extra)}}
      end)

    gateway =
      Ankusa.Gateway.Config.new(
        instance: config.instance,
        port: 0,
        authz:
          Map.merge(
            %{authorizers: authorizers, policies: %{"p" => %{mode: :first, steps: steps}}},
            authz
          )
      )

    start_supervised!({Ankusa.Gateway, gateway})
    config.instance
  end

  defp request(opts \\ []) do
    principal = Keyword.get(opts, :principal, Principal.new("alice", ["claims:read"]))
    via = Keyword.get(opts, :via, :jwt)

    resource =
      Keyword.get(opts, :resource, %{kind: :object, space: "docs", key: "a/b.txt", tenant: nil})

    %Request{
      principal: principal,
      via: via,
      subject_key: Authn.subject_key(via, principal),
      action: Keyword.get(opts, :action, :read),
      resource: resource
    }
  end

  defp set(%{answer: answer}, fun), do: Agent.update(answer, fn _ -> fun end)

  describe "kinds" do
    test "an :inline step runs every time; :cached and :remote ones once per freshness", ctx do
      inst =
        boot(ctx, %{"i" => {Stub, :inline, %{}}}, ["i"])

      for _ <- 1..3, do: assert({:allow, _} = Authz.decide(inst, "p", request()))
      for _ <- 1..3, do: assert_received({:decided, "i", "a/b.txt"})
    end

    test "a :cached step is decided once, then served from the cache", ctx do
      inst = boot(ctx, %{"c" => {Stub, :cached, %{}}}, ["c"])

      for _ <- 1..3, do: assert({:allow, _} = Authz.decide(inst, "p", request()))
      assert_received {:decided, "c", _}
      refute_received {:decided, "c", _}
    end

    test "a :remote step is decided once through its caller, then served from the cache", ctx do
      inst = boot(ctx, %{"r" => {Stub, :remote, %{}}}, ["r"])

      for _ <- 1..3, do: assert({:allow, _} = Authz.decide(inst, "p", request()))
      assert_received {:decided, "r", _}
      refute_received {:decided, "r", _}
    end
  end

  describe "freshness" do
    test "an allow lives allow_ttl_ms, a deny deny_ttl_ms", ctx do
      inst =
        boot(ctx, %{"c" => {Stub, :cached, %{}}}, ["c"], %{
          cache: %{allow_ttl_ms: 300, deny_ttl_ms: 40}
        })

      set(ctx, fn _ -> {:ok, :deny, %{}} end)
      assert {:deny, _} = Authz.decide(inst, "p", request())
      Process.sleep(60)
      set(ctx, fn _ -> {:ok, :allow, %{}} end)
      assert {:allow, _} = Authz.decide(inst, "p", request())
      Process.sleep(60)
      # Still fresh: the allow lives 300 ms.
      set(ctx, fn _ -> {:ok, :deny, %{}} end)
      assert {:allow, _} = Authz.decide(inst, "p", request())
    end

    test "a ttl_ms hint is taken for an allow, clamped by allow_ttl_max_ms", ctx do
      inst =
        boot(ctx, %{"c" => {Stub, :cached, %{allow_ttl_max_ms: 50}}}, ["c"], %{
          cache: %{allow_ttl_ms: 5_000}
        })

      set(ctx, fn _ -> {:ok, :allow, %{ttl_ms: 600_000}} end)
      assert {:allow, _} = Authz.decide(inst, "p", request())
      Process.sleep(70)
      set(ctx, fn _ -> {:ok, :deny, %{}} end)
      assert {:deny, _} = Authz.decide(inst, "p", request())
    end

    test "an entry written by a run that started one TTL ago is already stale", ctx do
      inst = boot(ctx, %{"c" => {Stub, :cached, %{}}}, ["c"], %{cache: %{allow_ttl_ms: 1_000}})
      gateway = Ankusa.Gateway.config(inst)
      %{state: state} = gateway.authz.authorizers["c"]
      key = Authz.cache_key("c", Stub, state, request())
      started = System.monotonic_time(:millisecond) - 1_000

      Authz.store(inst, gateway, state, key, {:ok, :deny, %{}}, started)
      assert {:allow, _} = Authz.decide(inst, "p", request())
      assert_received {:decided, "c", _}
    end

    test "refresh-ahead: a hit in the last fifth of its TTL refreshes once in the background",
         ctx do
      inst = boot(ctx, %{"r" => {Stub, :remote, %{}}}, ["r"], %{cache: %{allow_ttl_ms: 500}})
      test = self()

      assert {:allow, _} = Authz.decide(inst, "p", request())
      assert_receive {:decided, "r", _}

      # The refresh's run is held open: hits during it start no second one.
      set(ctx, fn _ ->
        send(test, {:refreshing, self()})

        receive do
          :go -> {:ok, :allow, %{}}
        end
      end)

      Process.sleep(420)
      assert {:allow, _} = Authz.decide(inst, "p", request())
      assert_receive {:refreshing, runner}, 1_000
      assert {:allow, _} = Authz.decide(inst, "p", request())
      refute_receive {:refreshing, _}, 30
      send(runner, :go)
    end

    test "serve_stale_ms answers with the last decision while the backend is down, then stops",
         ctx do
      inst =
        boot(ctx, %{"r" => {Stub, :remote, %{}}}, ["r"], %{
          cache: %{allow_ttl_ms: 50, serve_stale_ms: 200, refresh_ahead: false}
        })

      assert {:allow, _} = Authz.decide(inst, "p", request())
      set(ctx, fn _ -> {:error, {:unavailable, :down}} end)
      Process.sleep(80)
      assert {:allow, _} = Authz.decide(inst, "p", request())
      Process.sleep(250)
      assert {:error, {:unavailable, :down}} = Authz.decide(inst, "p", request())
    end
  end

  describe "keys" do
    test "a changed group set, or a changed claim, is a miss", ctx do
      inst = boot(ctx, %{"c" => {Stub, :cached, %{}}}, ["c"])
      alice = Principal.new("alice", ["claims:read"])

      Authz.decide(inst, "p", request(principal: alice))
      Authz.decide(inst, "p", request(principal: %{alice | groups: ["eng"]}))
      Authz.decide(inst, "p", request(principal: %{alice | claims: %{"team" => "x"}}))

      for _ <- 1..3, do: assert_received({:decided, "c", _})
    end

    test "two tenants' claims with one claim id do not share an entry", ctx do
      inst = boot(ctx, %{"c" => {Stub, :cached, %{}}}, ["c"])

      claim = fn tenant ->
        %{kind: :claim, space: "claims", key: tenant <> "/01ABC", tenant: tenant}
      end

      Authz.decide(inst, "p", request(resource: claim.("acme")))
      Authz.decide(inst, "p", request(resource: claim.("globex")))
      assert_received {:decided, "c", "acme/01ABC"}
      assert_received {:decided, "c", "globex/01ABC"}
    end

    test "a shared cache_scope shares one decision across keys", ctx do
      inst = boot(ctx, %{"s" => {Scoped, :cached, %{}}}, ["s"])
      object = fn key -> %{kind: :object, space: "docs", key: key, tenant: nil} end

      Authz.decide(inst, "p", request(resource: object.("team/a.txt")))
      Authz.decide(inst, "p", request(resource: object.("team/b.txt")))
      assert_received {:decided, "s", "team/a.txt"}
      refute_received {:decided, "s", "team/b.txt"}
    end
  end

  test "the policy deadline bounds a slow remote step", ctx do
    inst = boot(ctx, %{"r" => {Stub, :remote, %{}}}, ["r"], %{deadline_ms: 50})
    set(ctx, fn _ -> Process.sleep(500) && {:ok, :allow, %{}} end)

    {micros, result} = :timer.tc(fn -> Authz.decide(inst, "p", request()) end)
    assert result == {:error, {:unavailable, :deadline}}
    assert div(micros, 1000) < 200
  end

  describe "share links" do
    test "a link is denied anything but reading an object, before any step runs", ctx do
      inst =
        boot(ctx, %{"grants" => {Stub, :inline, %{}}, "x" => {Stub, :inline, %{}}}, [
          "x",
          "grants"
        ])

      link = Principal.new("link:o123.01ABC", [])

      assert {:deny, _} =
               Authz.decide(inst, "p", request(principal: link, via: :link, action: :write))

      prefix = %{kind: :prefix, space: "docs", key: "a/", tenant: nil}

      assert {:deny, _} =
               Authz.decide(
                 inst,
                 "p",
                 request(principal: link, via: :link, action: :read, resource: prefix)
               )

      refute_received {:decided, _, _}
    end

    test "reading an object by link runs only the grants step", ctx do
      inst =
        boot(ctx, %{"grants" => {Stub, :inline, %{}}, "x" => {Stub, :inline, %{}}}, [
          "x",
          "grants"
        ])

      link = Principal.new("link:o123.01ABC", [])

      assert {:allow, %{authorizer: "grants"}} =
               Authz.decide(inst, "p", request(principal: link, via: :link))

      assert_received {:decided, "grants", _}
      refute_received {:decided, "x", _}
    end
  end

  test "the caches are bounded and report their size" do
    config = test_config(roles: [])
    start_supervised!({Ankusa.Instance, config})

    start_supervised!(
      {Ankusa.Gateway, Ankusa.Gateway.Config.new(instance: config.instance, port: 0)}
    )

    Cache.put(config.instance, :authz, :k, :v, 10_000)
    assert {:ok, :v} = Cache.fetch(config.instance, :authz, :k)
    assert %{entries: 1, bytes: bytes} = Cache.stats(config.instance, :authz)
    assert bytes > 0
    Cache.delete(config.instance, :authz, :k)
    assert :error = Cache.fetch(config.instance, :authz, :k)
  end
end
