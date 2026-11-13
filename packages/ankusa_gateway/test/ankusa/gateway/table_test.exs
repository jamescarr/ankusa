defmodule Ankusa.Gateway.Authz.TableTest do
  @moduledoc """
  The `table` authorizer: row matching (ancestors, `*`, deny precedence,
  group and tenant identities), snapshots (version skips, size caps, staleness,
  a deleted generation), the engine's generation-keyed cache, readiness, and
  the overlap check with `gateway.spaces`.
  """

  use ExUnit.Case, async: false

  @moduletag :capture_log

  import Ankusa.TestHelpers, only: [unique_instance: 0, test_config: 1]

  alias Ankusa.Gateway.Authz
  alias Ankusa.Gateway.Authz.Request
  alias Ankusa.Gateway.Authz.Table
  alias Ankusa.Gateway.Authz.Table.Snapshot
  alias Ankusa.Gateway.Health
  alias Ankusa.Gateway.Router
  alias Ankusa.Principal

  defmodule Stub do
    @moduledoc false
    @behaviour Ankusa.Gateway.Authz.Table.Source

    @impl true
    def init!(opts, _key), do: Map.new(opts)

    @impl true
    def version(%{agent: agent}, _ctx) do
      case Agent.get(agent, & &1.version) do
        nil -> :unknown
        version -> {:ok, version}
      end
    end

    @impl true
    def load(%{agent: agent}, _ctx, acc, fun) do
      case Agent.get(agent, & &1.error) do
        nil -> stream(Agent.get(agent, & &1.rows), acc, fun)
        error -> {:error, error}
      end
    end

    @impl true
    def lookup(%{agent: agent}, _ctx, identities, _action, _space, _key) do
      case Agent.get(agent, & &1.error) do
        nil ->
          rows =
            Agent.get(agent, & &1.rows)
            |> Enum.filter(&(&1["principal"] in identities))

          {:ok, rows}

        error ->
          {:error, error}
      end
    end

    defp stream(rows, acc, fun) do
      Enum.reduce_while(rows, {:cont, acc}, fn row, {:cont, acc} ->
        case fun.(row, acc) do
          {:cont, acc} -> {:cont, {:cont, acc}}
          {:halt, acc} -> {:halt, {:halt, acc}}
        end
      end)
      |> case do
        {:cont, acc} -> {:ok, acc}
        {:halt, acc} -> {:ok, acc}
      end
    end
  end

  @key "gateway.authz.authorizers.team"

  defp row(principal, action, space, prefix, effect \\ "allow") do
    %{
      "principal" => principal,
      "action" => action,
      "space" => space,
      "prefix" => prefix,
      "effect" => effect
    }
  end

  defp agent(rows, opts \\ []) do
    {:ok, agent} =
      Agent.start_link(fn ->
        %{rows: rows, version: Keyword.get(opts, :version, 1), error: Keyword.get(opts, :error)}
      end)

    agent
  end

  defp state(agent, mode \\ :snapshot, extra \\ %{}) do
    Table.init!(
      Map.merge(
        %{
          mode: mode,
          source: {Stub, %{agent: agent}},
          refresh_ms: 1_000,
          stale_after_ms: 1_500,
          ready_timeout_ms: 0
        },
        extra
      ),
      @key
    )
  end

  defp start_snapshot(table_state) do
    instance = unique_instance()
    table_state = Map.put(table_state, :instance, instance)

    start_supervised!(%{
      id: {:table_supervisor, instance},
      start:
        {Supervisor, :start_link,
         [Table.child_specs(instance, "team", table_state), [strategy: :one_for_one]]},
      type: :supervisor
    })

    {instance, table_state}
  end

  defp request(principal, opts \\ []) do
    %Request{
      principal: principal,
      via: Keyword.get(opts, :via, :jwt),
      subject_key: Ankusa.Gateway.Authn.subject_key(:jwt, principal),
      action: Keyword.get(opts, :action, :read),
      resource:
        Keyword.get(opts, :resource, %{
          kind: :object,
          space: "docs",
          key: "teams/eng/report.pdf",
          tenant: nil
        })
    }
  end

  defp decide(instance, table_state, principal, opts \\ []) do
    Table.decide(request(principal, opts), table_state, %{
      instance: instance,
      name: "team",
      deadline: System.monotonic_time(:millisecond) + 2_000
    })
  end

  defp alice, do: %{Principal.new("alice", ["claims:read"]) | groups: ["eng"]}
  defp bob, do: Principal.new("bob", ["claims:read"])

  defp generation(instance) do
    case :ets.lookup(Table.tables_table(instance), "team") do
      [{"team", _tid, generation, _loaded_ms, _has_deny?}] -> generation
      [] -> nil
    end
  end

  defp await_generation(instance, previous, deadline) do
    case generation(instance) do
      ^previous ->
        if System.monotonic_time(:millisecond) > deadline do
          flunk("the snapshot was not refreshed within the deadline")
        else
          Process.sleep(20)
          await_generation(instance, previous, deadline)
        end

      nil ->
        flunk("no snapshot was ever published")

      _new ->
        :ok
    end
  end

  defp attach(event) do
    id = "table-test-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      id,
      event,
      fn _event, measurements, meta, _config ->
        send(parent, {:telemetry, measurements, meta})
      end,
      nil
    )

    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(id) end)
  end

  describe "row matching" do
    test "ancestor prefixes match, and a deny anywhere wins" do
      agent =
        agent([
          row("user:alice", "read", "docs", "teams/"),
          row("user:alice", "read", "docs", "teams/eng/", "deny"),
          row("user:alice", "read", "docs", "teams/eng/report.pdf")
        ])

      {instance, state} = start_snapshot(state(agent))
      assert :ok = Snapshot.await(instance, "team")

      assert {:ok, :deny, %{reason: :table}} = decide(instance, state, alice())
    end

    test "* names every action and every space, and groups and tenants are identities" do
      agent =
        agent([
          row("group:eng", "*", "*", ""),
          row("tenant:acme", "write", "docs", "teams/")
        ])

      {instance, state} = start_snapshot(state(agent))
      assert :ok = Snapshot.await(instance, "team")

      assert {:ok, :allow, _} = decide(instance, state, alice(), action: :write)
      assert {:ok, :allow, _} = decide(instance, state, alice(), action: :delete)

      acme = %{Principal.new("carol", ["claims:read"], ["acme"]) | groups: []}
      assert {:ok, :allow, _} = decide(instance, state, acme, action: :write)
      assert {:ok, :abstain, _} = decide(instance, state, acme, action: :read)

      assert {:ok, :abstain, _} = decide(instance, state, bob())
    end

    test "a prefix request matches its own prefix and its ancestors" do
      agent = agent([row("user:alice", "list", "docs", "teams/eng/")])
      {instance, state} = start_snapshot(state(agent))
      assert :ok = Snapshot.await(instance, "team")

      prefix = %{kind: :prefix, space: "docs", key: "teams/eng/", tenant: nil}

      assert {:ok, :allow, _} =
               decide(instance, state, alice(), action: :list, resource: prefix)
    end

    test "more identities than max_identities is unavailable, not a decision" do
      agent = agent([])
      table_state = state(agent, :snapshot, %{max_identities: 2})

      many = %{Principal.new("alice", ["claims:read"]) | groups: ["a", "b", "c"]}

      assert {:error, {:unavailable, :too_many_identities}} =
               decide(unique_instance(), table_state, many)
    end
  end

  describe "lookup mode" do
    test "a row the source returns counts only when it matches as a snapshot row would" do
      alice = Principal.new("alice", [])

      ctx = %{
        instance: :default,
        name: "team",
        deadline: System.monotonic_time(:millisecond) + 2_000
      }

      # What a sloppy query returns for teams/eng/report.pdf: a partial name, a
      # LIKE wildcard, another action, another space, and a deny under a
      # sibling directory.
      near_misses = [
        row("user:alice", "read", "docs", "teams/en"),
        row("user:alice", "read", "docs", "team_/eng/"),
        row("user:alice", "write", "docs", "teams/eng/"),
        row("user:alice", "read", "other", "teams/eng/"),
        row("user:alice", "read", "docs", "teams/eng/x/", "deny")
      ]

      table = state(agent(near_misses), :lookup)
      assert Table.decide(request(alice), table, ctx) == {:ok, :abstain, %{}}

      table = state(agent(near_misses ++ [row("user:alice", "*", "*", "teams/")]), :lookup)
      assert Table.decide(request(alice), table, ctx) == {:ok, :allow, %{}}
    end
  end

  describe "snapshots" do
    test "an unchanged version skips the load" do
      attach([:ankusa, :gateway, :table_refresh])
      agent = agent([row("user:alice", "read", "docs", "")])
      {instance, _state} = start_snapshot(state(agent))

      assert :ok = Snapshot.await(instance, "team")
      assert_receive {:telemetry, %{rows: 1}, %{instance: ^instance, result: :ok}}, 5_000

      assert_receive {:telemetry, _measurements, %{instance: ^instance, result: :unchanged}},
                     5_000
    end

    test "over max_rows keeps the previous snapshot" do
      attach([:ankusa, :gateway, :table_refresh])
      agent = agent([row("user:alice", "read", "docs", "")])
      {instance, state} = start_snapshot(state(agent, :snapshot, %{max_rows: 2}))
      assert :ok = Snapshot.await(instance, "team")
      assert {:ok, :allow, _} = decide(instance, state, alice())

      Agent.update(agent, fn current ->
        %{
          current
          | version: 2,
            rows: [row("user:bob", "read", "docs", "") | current.rows] ++ current.rows
        }
      end)

      Snapshot.refresh(instance, "team")

      assert_receive {:telemetry, _measurements, %{instance: ^instance, result: :too_large}},
                     5_000

      assert {:ok, :allow, _} = decide(instance, state, alice())
      assert {:ok, :abstain, _} = decide(instance, state, bob())
    end

    test "a snapshot older than stale_after_ms is unavailable until a load succeeds" do
      agent = agent([row("user:alice", "read", "docs", "")])

      {instance, state} =
        start_snapshot(state(agent, :snapshot, %{refresh_ms: 1_000, stale_after_ms: 1_000}))

      assert :ok = Snapshot.await(instance, "team")
      assert {:ok, :allow, _} = decide(instance, state, alice())

      Agent.update(agent, &%{&1 | version: 2, error: :down})
      Process.sleep(1_100)

      assert {:error, {:unavailable, :stale_snapshot}} = decide(instance, state, alice())
    end

    test "a deleted generation is unavailable, not a crash and not an abstain" do
      agent = agent([row("user:alice", "read", "docs", "")])
      {instance, state} = start_snapshot(state(agent))
      assert :ok = Snapshot.await(instance, "team")

      # A generation this process owns, published as the snapshot's and then
      # dropped: the reader finds the row, then the generation is gone.
      tid = :ets.new(:deleted_generation, [:set])
      :ets.insert(tid, {{"user:alice", "docs", "read", ""}, :allow})

      :ets.insert(
        Table.tables_table(instance),
        {"team", tid, System.unique_integer([:monotonic, :positive]),
         System.monotonic_time(:millisecond), false}
      )

      :ets.delete(tid)

      assert {:error, {:unavailable, :stale_snapshot}} = decide(instance, state, alice())
    end

    test "max_bytes is enforced on the published table" do
      rows =
        for i <- 1..40_000 do
          row("user:g#{rem(i, 64)}", "read", "docs", "p#{i}/")
        end

      agent = agent(rows)
      attach([:ankusa, :gateway, :table_refresh])

      {first, _state} = start_snapshot(state(agent, :snapshot, %{max_bytes: 17_179_869_184}))
      assert :ok = Snapshot.await(first, "team", 20_000)
      assert_receive {:telemetry, %{bytes: bytes}, %{instance: ^first, result: :ok}}, 20_000
      assert bytes > 1_048_576

      {over, _state} =
        start_snapshot(state(agent, :snapshot, %{max_bytes: bytes - 1}))

      assert_receive {:telemetry, _measurements, %{instance: ^over, result: :too_large}},
                     20_000

      {under, _state} = start_snapshot(state(agent, :snapshot, %{max_bytes: bytes + 1_000_000}))
      assert :ok = Snapshot.await(under, "team", 20_000)
    end
  end

  describe "the engine" do
    test "a decision cached before a refresh is not served after it" do
      agent = agent([row("user:alice", "read", "docs", "teams/")])
      attach([:ankusa, :gateway, :authz])

      config = test_config(roles: [])

      gateway =
        Ankusa.Gateway.Config.new(
          instance: config.instance,
          port: 0,
          authz: %{
            authorizers: %{
              "team" =>
                {Table,
                 %{
                   mode: :snapshot,
                   source: {Stub, %{agent: agent}},
                   refresh_ms: 1_000,
                   stale_after_ms: 1_500,
                   ready_timeout_ms: 0
                 }}
            },
            policies: %{"p" => %{mode: :first, steps: ["team"]}}
          }
        )

      start_supervised!({Ankusa.Instance, config})
      start_supervised!({Ankusa.Gateway, gateway})

      inst = config.instance
      assert :ok = Snapshot.await(inst, "team")
      assert {:allow, _} = Authz.decide(inst, "p", request(alice()))
      assert_receive {:telemetry, _measurements, %{decision: :allow, cached: false}}

      before = generation(inst)

      Agent.update(agent, fn current ->
        %{current | version: 2, rows: [row("user:alice", "read", "docs", "teams/", "deny")]}
      end)

      Snapshot.refresh(inst, "team")
      await_generation(inst, before, System.monotonic_time(:millisecond) + 5_000)

      assert {:deny, _} = Authz.decide(inst, "p", request(alice()))
      assert_receive {:telemetry, _measurements, %{decision: :deny, cached: false}}

      assert {:deny, _} = Authz.decide(inst, "p", request(alice()))
      assert_receive {:telemetry, _measurements, %{decision: :deny, cached: true}}
    end
  end

  describe "readiness" do
    test "/ready holds at 503 until a load succeeds, then serves while the source is down" do
      agent = agent([], error: :down)

      config = test_config(roles: [])

      gateway =
        Ankusa.Gateway.Config.new(
          instance: config.instance,
          port: 0,
          claims: %{policy: "p"},
          authz: %{
            authorizers: %{
              "team" =>
                {Table,
                 %{
                   mode: :snapshot,
                   source: {Stub, %{agent: agent}},
                   refresh_ms: 1_000,
                   stale_after_ms: 1_500,
                   ready_timeout_ms: 300
                 }}
            },
            policies: %{"p" => %{mode: :first, steps: ["team"]}}
          }
        )

      start_supervised!({Ankusa.Instance, config})
      start_supervised!({Ankusa.Gateway, gateway})

      inst = config.instance

      assert {:error, body} = Health.ready(inst)
      assert body.authorizers == %{"team" => "not_ready"}

      conn =
        Router.call(
          Plug.Test.conn(:get, "/v1/claims/acme/01M39VMD8RA3C5HR4RBV67Y002"),
          Router.init(instance: inst)
        )

      assert conn.status == 503
      assert JSON.decode!(conn.resp_body) == %{"error" => "authz_unavailable"}

      Process.sleep(350)

      assert {:ok, body} = Health.ready(inst)
      assert body.authorizers == %{"team" => "ok"}

      conn =
        Router.call(
          Plug.Test.conn(:get, "/v1/claims/acme/01M39VMD8RA3C5HR4RBV67Y002"),
          Router.init(instance: inst)
        )

      assert conn.status == 503
    end
  end

  describe "configuration" do
    test "an object source inside a space is refused" do
      dir =
        Path.join(System.tmp_dir!(), "ankusa-table-overlap-#{System.unique_integer([:positive])}")

      on_exit(fn -> File.rm_rf(dir) end)
      store = {Ankusa.BlobStore.LocalFS, [root: dir]}

      assert_raise ArgumentError, ~r/its store and prefix overlap gateway\.spaces\.docs/, fn ->
        Ankusa.Gateway.Config.new(
          authz: %{
            authorizers: %{
              "team" =>
                {Table,
                 %{
                   mode: :snapshot,
                   source:
                     {Ankusa.Gateway.Authz.Table.Source.Object,
                      %{store: store, key: "docs/perms.ndjson", max_object_bytes: 1_048_576}}
                 }}
            },
            policies: %{"p" => %{steps: ["team"]}}
          },
          spaces: %{
            "docs" => %{store: store, policy: "p"},
            "other" => %{store: {Ankusa.BlobStore.LocalFS, [root: dir <> "-other"]}, policy: "p"}
          }
        )
      end

      # The same store with a disjoint space prefix is accepted.
      config =
        Ankusa.Gateway.Config.new(
          authz: %{
            authorizers: %{
              "team" =>
                {Table,
                 %{
                   mode: :snapshot,
                   source:
                     {Ankusa.Gateway.Authz.Table.Source.Object, %{store: store, prefix: "perms/"}}
                 }}
            },
            policies: %{"p" => %{steps: ["team"]}}
          },
          spaces: %{"docs" => %{store: store, policy: "p", prefix: "docs/"}}
        )

      assert Map.has_key?(config.authz.authorizers, "team")
    end
  end
end
