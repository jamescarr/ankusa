defmodule Ankusa.Gateway.Postgres.SourceTest do
  @moduledoc """
  `Ankusa.Gateway.Postgres.Source` against a real Postgres (the package's
  `docker-compose.yml`, 127.0.0.1:5499): snapshot and lookup modes over a
  users/groups/folders schema the customer might already have, a refresh that
  picks up a deleted membership, a version that skips an unchanged load, a
  database that goes away, a read-only transaction, and bound parameters.
  """

  use ExUnit.Case, async: false

  @moduletag :capture_log

  import Ankusa.TestHelpers

  alias Ankusa.Gateway.Authz.{Request, Table}
  alias Ankusa.Gateway.Authz.Table.Snapshot
  alias Ankusa.Gateway.Postgres.Source
  alias Ankusa.Principal

  @key "gateway.authz.authorizers.team"

  @conn [
    hostname: "127.0.0.1",
    port: 5499,
    username: "postgres",
    password: "ankusa",
    database: "postgres"
  ]

  setup_all do
    {:ok, conn} = Postgrex.start_link(@conn ++ [pool_size: 1])
    setup_schema(conn)
    %{conn: conn}
  end

  setup %{conn: conn} do
    Postgrex.query!(conn, "DELETE FROM memberships", [])
    Postgrex.query!(conn, "DELETE FROM role_bindings", [])

    Postgrex.query!(
      conn,
      "INSERT INTO memberships (user_id, group_id) VALUES ('alice', 'eng')",
      []
    )

    Postgrex.query!(
      conn,
      "INSERT INTO role_bindings (principal, role_id, space) VALUES ('group:eng', 'reader', 'docs')",
      []
    )

    :ok
  end

  # Six tables and a view: the join is the customer's, the view is the one
  # object the gateway's `query` reads.
  defp setup_schema(conn) do
    for statement <- [
          "DROP VIEW IF EXISTS gateway_permissions",
          "DROP TABLE IF EXISTS role_bindings, roles, folders, memberships, groups, users CASCADE",
          "CREATE TABLE users (id text PRIMARY KEY)",
          "CREATE TABLE groups (id text PRIMARY KEY, name text NOT NULL)",
          """
          CREATE TABLE memberships (
            user_id text NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            group_id text NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
            PRIMARY KEY (user_id, group_id)
          )
          """,
          "CREATE TABLE folders (path text PRIMARY KEY)",
          """
          CREATE TABLE roles (
            id text PRIMARY KEY, action text NOT NULL, prefix text NOT NULL,
            effect text NOT NULL DEFAULT 'allow'
          )
          """,
          """
          CREATE TABLE role_bindings (
            principal text NOT NULL,
            role_id text NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
            space text NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (principal, role_id, space)
          )
          """,
          "INSERT INTO users (id) VALUES ('alice'), ('bob')",
          "INSERT INTO groups (id, name) VALUES ('eng', 'eng')",
          "INSERT INTO folders (path) VALUES ('teams/eng/')",
          "INSERT INTO roles (id, action, prefix) VALUES ('reader', 'read', 'teams/eng/')",
          """
          CREATE VIEW gateway_permissions AS
            SELECT 'user:' || u.id AS principal, r.action, rb.space, r.prefix, r.effect
            FROM users u
            JOIN memberships m ON m.user_id = u.id
            JOIN groups g ON g.id = m.group_id
            JOIN role_bindings rb ON rb.principal = 'group:' || g.name
            JOIN roles r ON r.id = rb.role_id
            JOIN folders f ON f.path = r.prefix
            UNION ALL
            SELECT rb.principal, r.action, rb.space, r.prefix, r.effect
            FROM role_bindings rb
            JOIN roles r ON r.id = rb.role_id
            JOIN folders f ON f.path = r.prefix
            WHERE rb.principal LIKE 'user:%'
          """
        ] do
      Postgrex.query!(conn, statement, [])
    end

    :ok
  end

  defp source_opts(extra) do
    Keyword.merge(
      [
        url: "postgres://postgres:ankusa@127.0.0.1:5499/postgres",
        query: "SELECT * FROM gateway_permissions",
        # Everything the view reads: a membership change is a new version too.
        version_query:
          "SELECT (SELECT count(*) FROM memberships)::text || '|' || " <>
            "(SELECT max(updated_at)::text FROM role_bindings)",
        timeout_ms: 5_000
      ],
      extra
    )
  end

  defp url, do: "postgres://postgres:ankusa@127.0.0.1:5499/postgres"

  defp lookup_query do
    """
    SELECT principal, action, space, prefix, effect
    FROM gateway_permissions
    WHERE principal = ANY($1) AND action IN ($2, '*') AND space IN ($3, '*')
      AND (prefix = '' OR $4 = prefix OR $4 LIKE prefix || '%')
    """
  end

  defp table_state(mode, extra) do
    Table.init!(
      %{
        mode: mode,
        source: {Source, source_opts(extra)},
        refresh_ms: 1_000,
        stale_after_ms: 1_200,
        ready_timeout_ms: 0
      },
      @key
    )
  end

  defp start_snapshot(state) do
    instance = unique_instance()
    state = Map.put(state, :instance, instance)

    start_supervised!(%{
      id: {:postgres_table_supervisor, instance},
      start:
        {Supervisor, :start_link,
         [Table.child_specs(instance, "team", state), [strategy: :one_for_one]]},
      type: :supervisor
    })

    {instance, state}
  end

  defp start_pool(instance, state) do
    [spec] = Source.child_specs(instance, "team", state.source.state)
    start_supervised!(spec)
  end

  defp request(principal) do
    %Request{
      principal: principal,
      via: :jwt,
      subject_key: Ankusa.Gateway.Authn.subject_key(:jwt, principal),
      action: :read,
      resource: %{kind: :object, space: "docs", key: "teams/eng/report.pdf", tenant: nil}
    }
  end

  defp decide(instance, state, principal) do
    Table.decide(request(principal), state, %{
      instance: instance,
      name: "team",
      deadline: System.monotonic_time(:millisecond) + 2_000
    })
  end

  defp alice, do: %{Principal.new("alice", ["claims:read"]) | groups: ["eng"]}
  defp bob, do: Principal.new("bob", ["claims:read"])

  defp attach_refreshes do
    id = "postgres-source-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      id,
      [:ankusa, :gateway, :table_refresh],
      fn _event, measurements, meta, _config ->
        send(parent, {:table_refresh, measurements, meta})
      end,
      nil
    )

    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(id) end)
  end

  defp generation(instance) do
    case :ets.lookup(Table.tables_table(instance), "team") do
      [{"team", _tid, generation, _loaded_ms, _has_deny?}] -> generation
      [] -> nil
    end
  end

  defp await_generation(instance, previous, deadline) do
    case generation(instance) do
      nil ->
        flunk("no snapshot was ever published")

      ^previous ->
        if System.monotonic_time(:millisecond) > deadline do
          flunk("the snapshot was not refreshed within the deadline")
        else
          Process.sleep(20)
          await_generation(instance, previous, deadline)
        end

      _new ->
        :ok
    end
  end

  describe "snapshot mode" do
    test "a member of the group reads; a non-member abstains" do
      {instance, state} = start_snapshot(table_state(:snapshot, []))
      assert :ok = Snapshot.await(instance, "team", 10_000)

      assert {:ok, :allow, _meta} = decide(instance, state, alice())
      assert {:ok, :abstain, _meta} = decide(instance, state, bob())
    end

    test "deleting the membership row denies within one refresh_ms", %{conn: conn} do
      {instance, state} = start_snapshot(table_state(:snapshot, []))
      assert :ok = Snapshot.await(instance, "team", 10_000)
      assert {:ok, :allow, _} = decide(instance, state, alice())

      Postgrex.query!(conn, "DELETE FROM memberships WHERE user_id = 'alice'", [])

      await_generation(
        instance,
        generation(instance),
        System.monotonic_time(:millisecond) + 5_000
      )

      assert {:ok, :abstain, _} = decide(instance, state, alice())
    end

    test "an unchanged version_query skips the load", %{conn: conn} do
      attach_refreshes()
      {instance, _state} = start_snapshot(table_state(:snapshot, []))

      assert :ok = Snapshot.await(instance, "team", 10_000)
      assert_receive {:table_refresh, %{rows: rows}, %{result: :ok}}, 5_000
      assert rows == 1

      assert_receive {:table_refresh, _measurements, %{result: :unchanged}}, 5_000

      # A version that moves loads again.
      Postgrex.query!(conn, "UPDATE role_bindings SET updated_at = now() + interval '1s'", [])
      assert_receive {:table_refresh, _measurements, %{result: :ok}}, 5_000
    end

    test "a query that tries to write fails read-only and writes nothing", %{conn: conn} do
      state =
        Table.init!(
          %{
            mode: :snapshot,
            source: {Source, source_opts(query: "INSERT INTO users (id) VALUES ('mallory')")},
            ready_timeout_ms: 0
          },
          @key
        )

      instance = unique_instance()
      start_pool(instance, state)

      ctx = %{
        instance: instance,
        name: "team",
        deadline: System.monotonic_time(:millisecond) + 5_000
      }

      assert {:error, %Postgrex.Error{postgres: %{code: :read_only_sql_transaction}}} =
               Source.load(state.source.state, ctx, %{rows: 0}, fn _row, acc -> {:cont, acc} end)

      assert {:ok, %Postgrex.Result{rows: [[0]]}} =
               Postgrex.query(conn, "SELECT count(*) FROM users WHERE id = 'mallory'", [])
    end

    test "a database that goes away keeps answers until stale_after_ms", %{conn: conn} do
      state = table_state(:snapshot, [])
      instance = unique_instance()
      state = Map.put(state, :instance, instance)

      [spec] = Source.child_specs(instance, "team", state.source.state)
      {module, function, args} = spec.start
      {:ok, pool} = apply(module, function, args)

      start_supervised!({Snapshot, instance: instance, name: "team", state: state})
      assert :ok = Snapshot.await(instance, "team", 10_000)

      :ok = GenServer.stop(pool)
      assert {:ok, :allow, _} = decide(instance, state, alice())

      # One refresh_ms later the load has failed, and past stale_after_ms the
      # snapshot is no longer served: unavailable, not a stale "allow".
      Process.sleep(1_400)
      assert {:error, {:unavailable, :stale_snapshot}} = decide(instance, state, alice())

      _ = conn
    end
  end

  describe "an unreachable database" do
    test "/ready holds 503 for ready_timeout_ms, then serves while requests stay 503" do
      unreachable =
        Keyword.put(source_opts([]), :url, "postgres://postgres:ankusa@127.0.0.1:1/postgres")

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
                   source: {Source, unreachable},
                   refresh_ms: 60_000,
                   stale_after_ms: 300_000,
                   ready_timeout_ms: 300
                 }}
            },
            policies: %{"p" => %{mode: :first, steps: ["team"]}}
          }
        )

      start_supervised!({Ankusa.Instance, config})
      start_supervised!({Ankusa.Gateway, gateway})

      assert {:error, body} = Ankusa.Gateway.Health.ready(config.instance)
      assert body.authorizers == %{"team" => "not_ready"}

      Process.sleep(350)

      assert {:ok, %{authorizers: %{"team" => "ok"}}} =
               Ankusa.Gateway.Health.ready(config.instance)

      conn =
        Ankusa.Gateway.Router.call(
          Plug.Test.conn(:get, "/v1/claims/acme/01M39VMD8RA3C5HR4RBV67Y002"),
          Ankusa.Gateway.Router.init(instance: config.instance)
        )

      assert conn.status == 503
      assert JSON.decode!(conn.resp_body) == %{"error" => "authz_unavailable"}
    end
  end

  describe "lookup mode" do
    test "answers the same cases through lookup_query" do
      state = table_state(:lookup, lookup_query: lookup_query())

      instance = unique_instance()
      state = Map.put(state, :instance, instance)

      start_pool(instance, state)

      assert {:ok, :allow, _} = decide(instance, state, alice())
      assert {:ok, :abstain, _} = decide(instance, state, bob())
    end

    test "an identity is a bound parameter, never SQL", %{conn: conn} do
      state = table_state(:lookup, lookup_query: lookup_query())

      instance = unique_instance()
      start_pool(instance, state)
      ctx = %{instance: instance, name: "team", deadline: 0}
      injection = "x'; DROP TABLE users; --"

      assert {:ok, []} =
               Source.lookup(state.source.state, ctx, [injection], "read", "docs", "a.txt")

      assert {:ok, %Postgrex.Result{rows: [[2]]}} =
               Postgrex.query(conn, "SELECT count(*) FROM users", [])
    end
  end

  describe "fail-closed paths" do
    test "a source with only a lookup_query refuses to load a snapshot" do
      state = Source.init!(%{url: url(), lookup_query: "SELECT 1"}, @key)
      ctx = %{instance: unique_instance(), name: "team", deadline: 0}
      assert {:error, :no_query} = Source.load(state, ctx, [], fn _row, acc -> {:cont, acc} end)
    end

    test "a source with only a query refuses a lookup" do
      state = Source.init!(%{url: url(), query: "SELECT 1"}, @key)
      ctx = %{instance: unique_instance(), name: "team", deadline: 0}

      assert {:error, :no_lookup_query} =
               Source.lookup(state, ctx, ["user:alice"], "read", "docs", "a")
    end

    test "no version_query is :unknown, and no pool is an error, not a crash" do
      state = Source.init!(%{url: url(), query: "SELECT 1"}, @key)

      assert Source.version(state, %{instance: unique_instance(), name: "team", deadline: 0}) ==
               :unknown

      versioned = Source.init!(%{url: url(), query: "SELECT 1", version_query: "SELECT 1"}, @key)

      assert {:error, :not_running} =
               Source.version(versioned, %{instance: unique_instance(), name: "team", deadline: 0})
    end

    test "one of query and lookup_query is required, and ranges are checked" do
      assert_raise ArgumentError, ~r/one of query .* and lookup_query .* is required/, fn ->
        Source.init!(%{url: url()}, @key)
      end

      assert_raise ArgumentError, ~r/pool_size must be an integer from 1 to 64/, fn ->
        Source.init!(%{url: url(), query: "SELECT 1", pool_size: 0}, @key)
      end

      assert_raise ArgumentError, ~r/ssl must be true or false/, fn ->
        Source.init!(%{url: url(), query: "SELECT 1", ssl: "yes"}, @key)
      end
    end
  end

  describe "options" do
    test "url and hostname are alternatives, url must be postgres" do
      assert_raise ArgumentError, ~r/url or hostname is required/, fn ->
        Source.init!(%{}, @key)
      end

      assert_raise ArgumentError, ~r/set url or hostname, not both/, fn ->
        Source.init!(%{url: "postgres://h/db", hostname: "h", database: "db"}, @key)
      end

      assert_raise ArgumentError, ~r/must be a postgres:\/\/ URL/, fn ->
        Source.init!(%{url: "mysql://h/db"}, @key)
      end
    end

    test "ssl defaults off for a loopback host and on otherwise" do
      loopback = Source.init!(%{hostname: "127.0.0.1", database: "db", query: "SELECT 1"}, @key)
      remote = Source.init!(%{hostname: "db.internal", database: "db", query: "SELECT 1"}, @key)

      assert loopback.conn[:ssl] == false
      assert remote.conn[:ssl] == true
    end

    test "the URL's parts become the connection, secrets decoded" do
      state =
        Source.init!(
          %{url: "postgres://user%40x:p%3Ass@db.internal:6432/permissions", query: "SELECT 1"},
          @key
        )

      assert state.conn[:hostname] == "db.internal"
      assert state.conn[:port] == 6432
      assert state.conn[:database] == "permissions"
      assert state.conn[:username] == "user@x"
      assert state.conn[:password] == "p:ss"
    end
  end
end
