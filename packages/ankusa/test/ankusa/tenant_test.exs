defmodule Ankusa.TenantTest do
  @moduledoc """
  Tenant erase (`Ankusa.Tenant`, TEN-5): after it, nothing the tenant had is
  left in the store, the claim stores or the edge's tables; its lifecycle
  consumers are told; a running attempt cannot put a row back; archived hooks
  answer `410`; and an erase that was killed resumes.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Audit
  alias Ankusa.Audit.Context
  alias Ankusa.Dispatch.Pipeline
  alias Ankusa.Edge.{Ingest, Quarantine, RateLimiter}

  alias Ankusa.{
    Envelope,
    EraseHarness,
    Queue,
    Replay,
    Routes,
    SourceStore,
    Storage,
    Store,
    Tenant
  }

  alias Ankusa.Queue.Tenants
  alias Ankusa.Storage.Compactor
  alias Ankusa.Store.Keys
  alias Ankusa.Tenant.Gate

  # ── sinks ──────────────────────────────────────────────────────────────────

  defmodule PermanentSink do
    @moduledoc "Dead-letters every row on its first attempt."
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: {:error, {:permanent, :nope}}
  end

  defmodule FleetStore do
    @moduledoc "A fleet limiter store that only says which tenants' buckets were reset."
    @behaviour Ankusa.RateLimiter.Store

    def child_spec(opts),
      do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}

    @impl true
    def start_link(_opts), do: :ignore

    @impl true
    def hit(_instance, _charges, _claim, _timeout_ms), do: :ok

    @impl true
    def reset(_instance, tenant, _timeout_ms) do
      send(:tenant_test, {:fleet_reset, tenant})
      :ok
    end
  end

  defmodule TransientSink do
    @moduledoc "Fails every attempt, so the row is retried (an hour later)."
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: {:error, :down}
  end

  defmodule SlowSink do
    @moduledoc "Reports the start of every attempt, takes `ms`, then fails it, so the settle is a retry row."
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, ctx, opts) do
      send(:tenant_test, {:started, env.id, ctx.attempt})
      Process.sleep(Keyword.fetch!(opts, :ms))
      {:error, :down}
    end
  end

  defmodule CaptureSink do
    @moduledoc "Reports every lifecycle event to the test."
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      send(Keyword.fetch!(opts, :to), {:lifecycle, env, JSON.decode!(env.body)})
      :ok
    end
  end

  @doc false
  def decoder(_source_id, spec) do
    sink =
      case spec["mode"] do
        "permanent" -> {PermanentSink, []}
        "transient" -> {TransientSink, []}
        "slow" -> {SlowSink, [ms: spec["ms"] || 1_000]}
      end

    [sinks: [sink]]
  end

  # ── helpers ────────────────────────────────────────────────────────────────

  defp start_instance(overrides \\ []) do
    Process.register(self(), :tenant_test)

    base = [
      roles: [:edge, :dispatch],
      admin: %{enabled: true, port: 0},
      source_store: {SourceStore.Persistent, decoder: &__MODULE__.decoder/2},
      routes: [enabled: true, admin: [port: 0]],
      lifecycle: %{sinks: [{CaptureSink, to: self()}]},
      dispatch: %{
        retry:
          {Ankusa.RetryPolicy.Exponential, base_ms: 3_600_000, max_attempts: 10, jitter: false},
        breaker_failures: 0
      }
    ]

    config = test_config(Keyword.merge(base, overrides))
    put_config(config)
    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp source!(inst, tenant, name, mode, extra \\ %{}) do
    {:ok, _} = SourceStore.put(inst, tenant, name, Map.merge(%{"mode" => mode}, extra), :create)
    "#{tenant}.#{name}"
  end

  defp ingest!(inst, source_id, body) do
    {:ok, env} = Ingest.ingest(inst, request(source_id, body))
    env
  end

  defp erase!(inst, tenant) do
    assert {:ok, :started} =
             Tenant.delete(inst, tenant, confirm: tenant, audit: %Context{actor: "tester"})

    await_erased(inst, tenant)
  end

  defp await_erased(inst, tenant, deadline_ms \\ 30_000) do
    eventually(
      fn -> match?({:ok, %{state: :erased}}, Tenant.status(inst, tenant)) end,
      deadline_ms,
      fn -> "the erase of #{tenant} did not finish: #{inspect(Tenant.status(inst, tenant))}" end
    )
  end

  defp eventually(fun, deadline_ms, message) do
    deadline = System.monotonic_time(:millisecond) + deadline_ms
    do_eventually(fun, deadline, message)
  end

  defp do_eventually(fun, deadline, message) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk(if is_function(message, 0), do: message.(), else: message)

      true ->
        Process.sleep(25)
        do_eventually(fun, deadline, message)
    end
  end

  # Every lifecycle event up to and including the first `type`.
  defp events_until(type, acc \\ []) do
    receive do
      {:lifecycle, _env, %{"type" => ^type} = event} -> Enum.reverse([event | acc])
      {:lifecycle, _env, event} -> events_until(type, [event | acc])
    after
      15_000 -> flunk("no #{type} event; got #{inspect(Enum.map(acc, & &1["type"]))}")
    end
  end

  defp audit_rows(inst) do
    {:ok, entries} = Audit.page(inst, 0, 10_000)
    for {_seq, {:ok, row}} <- entries, do: row
  end

  defp envelope(tenant, source_id) do
    %Envelope{
      id: Ankusa.UUIDv7.generate(),
      source_id: source_id,
      tenant_id: tenant,
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/webhooks/#{source_id}",
      headers: [],
      body: "held",
      size: 4
    }
  end

  # Everything in the store that names `tenant` or its ordinal `ord`.
  defp leftovers(inst, tenant, ord) do
    {:ok, hooks} = Queue.hooks(inst, 0, 100_000)

    keys = fn family, decode ->
      %{lo: lo, hi: hi} = Keys.family(family)

      {:ok, found} =
        Store.fold(inst, family, {lo, hi}, [], fn key, _value, acc ->
          {:cont, [decode.(key) | acc]}
        end)

      found
    end

    of_ord = fn tuple -> elem(tuple, 0) == ord end

    point = fn cf, key ->
      case Store.get(inst, cf, key) do
        :not_found -> []
        {:ok, _} -> [{cf, key}]
      end
    end

    [
      hooks: for(h <- hooks, h.tenant_id == tenant, do: h.id),
      due: Enum.filter(keys.(:due, &Keys.decode_due/1), of_ord),
      inflight: Enum.filter(keys.(:inflight, &Keys.decode_inflight/1), of_ord),
      dead_by_tenant: Enum.filter(keys.(:dead_by_tenant, &Keys.decode_dead_by_tenant/1), of_ord),
      active:
        Enum.filter(keys.(:active, fn k -> k |> Keys.decode_active() |> swap() end), of_ord),
      rows:
        Enum.flat_map(
          [
            {:default, Keys.source(tenant, "billing")},
            {:default, Keys.source(tenant, "retry")},
            {:default, Keys.source_revision(tenant, "billing")},
            {:default, Keys.rate_limit(tenant)},
            {:default, Keys.erase(tenant)},
            {:tenants, Keys.tenant_ord(tenant)},
            {:index, Keys.dedupe(tenant, "#{tenant}.billing", "evt-1")}
          ] ++ for(f <- 1..6, do: {:counters, Keys.counter(ord, f)}),
          fn {cf, key} -> point.(cf, key) end
        ),
      quarantine:
        Enum.filter(keys.(:quarantine, & &1), fn <<?s, _at::64, _id::binary>> = key ->
          {:ok, value} = Store.get(inst, :quarantine, key)
          :erlang.binary_to_term(value)[:tenant_id] == tenant
        end),
      dedupe_expiry:
        Enum.filter(keys.(:dedupe_expiry, &Keys.decode_dedupe_expiry/1), fn
          {_at, ^tenant, _source, _key} -> true
          _ -> false
        end)
    ]
    |> Enum.reject(fn {_name, found} -> found == [] end)
  end

  defp swap({bucket, ord}), do: {ord, bucket}

  # ── the erase ──────────────────────────────────────────────────────────────

  describe "erase" do
    test "erase leaves no trace" do
      config = start_instance()
      inst = config.instance
      root = Ankusa.Config.path(config, "segments")

      source!(inst, "acme", "billing", "permanent")
      source!(inst, "acme", "retry", "transient")
      source!(inst, "globex", "orders", "permanent")

      acme_dead = for n <- 1..3, do: ingest!(inst, "acme.billing", "d#{n}")
      for n <- 1..2, do: ingest!(inst, "acme.retry", "r#{n}")
      globex = for n <- 1..2, do: ingest!(inst, "globex.orders", "g#{n}")
      {:ok, _settled} = Pipeline.tick(inst)

      assert {:ok, %{scheduled: 2}} = Queue.pending_for_tenant(inst, "acme")
      assert {:ok, %{dead: 3}} = Queue.dead_for_tenant(inst, "acme")
      {:ok, ord} = Tenants.ord(inst, "acme")

      # What else a tenant has: routes, an override, held hooks, dedupe keys,
      # claim objects.
      route = fn id, tenant ->
        {:ok, _} =
          Routes.create(inst, %{
            "id" => id,
            "path" => "/hooks/#{id}",
            "methods" => ["POST"],
            "tenant_id" => tenant
          })
      end

      route.("acme-route", "acme")
      route.("globex-route", "globex")
      {:ok, _} = RateLimiter.put_override(inst, "acme", %{"rate" => 5, "burst" => 10})
      {:ok, _} = RateLimiter.put_override(inst, "globex", %{"rate" => 5, "burst" => 10})
      assert :ok = Quarantine.put(inst, envelope("acme", "acme.billing"), :bad_signature)
      assert :ok = Quarantine.put(inst, envelope("globex", "globex.orders"), :bad_signature)

      expires = System.system_time(:millisecond) + 3_600_000

      dedupe = fn tenant ->
        [
          {:put, :index, Keys.dedupe(tenant, "#{tenant}.billing", "evt-1"),
           <<expires::64, "original">>},
          {:put, :index, Keys.dedupe_expiry(expires, tenant, "#{tenant}.billing", "evt-1"), <<>>}
        ]
      end

      :ok = Store.write(inst, dedupe.("acme") ++ dedupe.("globex"), sync: true)

      for tenant <- ["acme", "globex"] do
        dir = Path.join([root, "claims", "tenant=#{tenant}", "dt=2026-10-19"])
        File.mkdir_p!(dir)
        File.write!(Path.join(dir, "pack-1"), "claim bytes")
      end

      assert leftovers(inst, "acme", ord) != []

      erase!(inst, "acme")

      # Nothing in the store, the claim stores or the edge's tables names acme.
      assert leftovers(inst, "acme", ord) == []
      refute File.exists?(Path.join([root, "claims", "tenant=acme"]))
      assert {:ok, %{routes: []}} = Routes.list(inst, tenant: "acme")
      assert %{tenants: tenants} = RateLimiter.list(inst)
      refute Enum.any?(tenants, fn {tenant, _limit, _source} -> tenant == "acme" end)
      assert Tenants.name(inst, ord) == :not_found

      assert {:ok, %{pending: 0, scheduled: 0, inflight: 0, bytes: 0}} =
               Queue.pending_for_tenant(inst, "acme")

      # The tombstone is there, and it is the only thing that is.
      assert {:ok, at} = Tenant.erased_at(inst, "acme")
      assert {:ok, %{state: :erased, erased_at: ^at}} = Tenant.status(inst, "acme")

      # Another tenant lost nothing, and the counters still equal the index.
      assert File.exists?(Path.join([root, "claims", "tenant=globex", "dt=2026-10-19", "pack-1"]))
      assert {:ok, %{routes: [%{id: "globex-route"}]}} = Routes.list(inst, tenant: "globex")
      assert {:ok, %{dead: 2}} = Queue.dead_for_tenant(inst, "globex")
      assert Enum.sort(stored_ids(inst)) == Enum.sort(Enum.map(globex, & &1.id))
      assert stored_counters(inst) == folded_counters(inst)
      refute Enum.any?(acme_dead, &(&1.id in stored_ids(inst)))

      # One audit row of its own, saying what each stage took.
      assert [row] = Enum.filter(audit_rows(inst), &(&1.action == "tenant.delete"))
      assert row.tenant == "acme"
      assert row.actor == "tester"

      assert %{
               "sources" => 2,
               "routes" => 1,
               "rate_limits" => 1,
               "pending" => 2,
               "dead" => 3,
               "quarantine" => 1,
               "claims" => 1
             } = row.detail

      # A hook for the erased tenant is a hook of nobody until a source exists again.
      assert {:error, :unknown_source} = Ingest.ingest(inst, request("acme.billing", "late"))
    end

    test "a lifecycle consumer is told, in order, and a replay of the window re-emits nothing of the tenant" do
      config = start_instance()
      inst = config.instance

      source!(inst, "acme", "billing", "permanent")
      source!(inst, "globex", "orders", "permanent")

      {:ok, _} =
        Routes.create(inst, %{
          "id" => "acme-route",
          "path" => "/hooks/acme",
          "methods" => ["POST"],
          "tenant_id" => "acme"
        })

      erase!(inst, "acme")

      events = events_until("io.ankusa.tenant.deleted")
      types = Enum.map(events, &{&1["type"], &1["subject"]})

      # What the consumer's read model needs to forget, then the erase itself.
      assert {"io.ankusa.source.created", "acme.billing"} in types
      assert {"io.ankusa.source.deleted", "acme.billing"} in types
      assert {"io.ankusa.route.deleted", "acme-route"} in types
      assert List.last(types) == {"io.ankusa.tenant.deleted", "acme"}
      assert List.last(events)["data"] == %{"tenant" => "acme"}

      deleted = Enum.find_index(types, &(&1 == {"io.ankusa.source.deleted", "acme.billing"}))
      assert deleted < length(types) - 1

      # Replaying the whole trail: acme's creations, updates and deletions are
      # not re-announced (their entities are gone), globex's are, and the
      # erase itself is, for a consumer that was down when it happened.
      assert {:ok, :created, job} = Replay.start(inst, %{kind: :lifecycle})
      replayed = replayed_events(inst, job.id)

      assert {"io.ankusa.source.created", "globex.orders"} in replayed
      assert {"io.ankusa.tenant.deleted", "acme"} in replayed

      refute Enum.any?(replayed, fn {type, subject} ->
               type != "io.ankusa.tenant.deleted" and
                 String.starts_with?(subject, "acme")
             end)

      # And the same for the tenant's window alone: only its erase.
      assert {:ok, :created, only} = Replay.start(inst, %{kind: :lifecycle, tenants: ["acme"]})
      assert replayed_events(inst, only.id) == [{"io.ankusa.tenant.deleted", "acme"}]

      # The entities of acme's rows are gone, globex's are not.
      rows = Enum.filter(audit_rows(inst), & &1[:publish])

      for row <- rows, row.action != "tenant.delete" do
        expected = if row.tenant == "acme", do: :not_found, else: :ok
        found = if match?({:ok, _}, Audit.entity(inst, row.seq)), do: :ok, else: :not_found
        assert found == expected, "#{row.action} of #{row.tenant}: entity #{found}"
      end
    end

    test "a running attempt cannot put a row back" do
      config = start_instance()
      inst = config.instance

      source!(inst, "acme", "slow", "slow", %{"ms" => 1_500})
      source!(inst, "globex", "orders", "permanent")

      ids = for n <- 1..2, do: ingest!(inst, "acme.slow", "s#{n}").id
      ingest!(inst, "globex.orders", "g")
      {:ok, ord} = Tenants.ord(inst, "acme")

      # Both attempts are running (their settle would be a retry row an hour
      # out) when the erase starts.
      for id <- ids, do: assert_receive({:started, ^id, 1}, 5_000)
      assert {:ok, %{inflight: 2}} = Queue.pending_for_tenant(inst, "acme")

      erase!(inst, "acme")

      assert leftovers(inst, "acme", ord) == []
      assert stored_counters(inst) == folded_counters(inst)

      assert {:ok, %{pending: 0, scheduled: 0, inflight: 0}} =
               Queue.pending_for_tenant(inst, "acme")

      # Nothing is redelivered, now or when the retry would have been due.
      {:ok, _} = Pipeline.tick(inst)
      refute_receive {:started, _, _}, 500
      assert leftovers(inst, "acme", ord) == []
    end

    test "a tenant that is being erased takes no hook, and one that was erased is a new tenant" do
      config = start_instance(roles: [:edge])
      inst = config.instance

      source!(inst, "acme", "billing", "permanent")
      ingest!(inst, "acme.billing", "one")
      {:ok, old} = Tenants.ord(inst, "acme")

      # Closed: the writer and the pen refuse, the others do not notice.
      Gate.close(inst, "acme", old)
      assert {:error, :tenant_erasing} = Ingest.ingest(inst, request("acme.billing", "two"))

      assert {:error, :tenant_erasing} =
               Quarantine.put(inst, envelope("acme", "acme.billing"), :bad_signature)

      source!(inst, "globex", "orders", "permanent")
      assert {:ok, _} = Ingest.ingest(inst, request("globex.orders", "other"))

      # Opened again without an erase: the same ordinal, nothing lost.
      Gate.open(inst, "acme")
      assert {:ok, _} = Ingest.ingest(inst, request("acme.billing", "three"))
      assert Tenants.ord(inst, "acme") == {:ok, old}
      assert {:ok, %{pending: 2}} = Queue.pending_for_tenant(inst, "acme")

      # Erased: the tenant's next hook gets an ordinal of its own, never the old one.
      erase!(inst, "acme")
      source!(inst, "acme", "billing", "permanent")
      assert {:ok, _} = Ingest.ingest(inst, request("acme.billing", "four"))
      assert {:ok, new} = Tenants.ord(inst, "acme")
      assert new > old
      assert {:ok, %{pending: 1}} = Queue.pending_for_tenant(inst, "acme")
      assert stored_counters(inst) == folded_counters(inst)
    end
  end

  defp replayed_events(inst, id) do
    eventually(
      fn -> match?({:ok, %{state: :done}}, Replay.get(inst, id)) end,
      15_000,
      "the replay did not finish"
    )

    collect_replayed([])
  end

  defp collect_replayed(acc) do
    receive do
      {:lifecycle, _env, %{"replayid" => _} = event} ->
        collect_replayed([{event["type"], event["subject"]} | acc])
    after
      500 -> Enum.reverse(acc)
    end
  end

  # ── what stays behind ──────────────────────────────────────────────────────

  describe "archived hooks" do
    test "a fetch of an erased hook is gone, the others are not, and archive replay skips them" do
      config =
        start_instance(
          roles: [:edge, :dispatch, :storage],
          storage: %{interval_ms: 0, roll_ms: 0}
        )

      inst = config.instance

      source!(inst, "acme", "billing", "permanent")
      source!(inst, "globex", "orders", "permanent")
      [a, b] = for n <- 1..2, do: ingest!(inst, "acme.billing", "a#{n}")
      c = ingest!(inst, "globex.orders", "c")
      {:ok, _} = Pipeline.tick(inst)

      assert {:ok, 1} = Compactor.tick(inst)
      assert {:ok, %{id: id}} = Storage.fetch(inst, a.id)
      assert id == a.id

      erase!(inst, "acme")

      assert Storage.fetch(inst, a.id) == {:error, :gone}
      assert Storage.fetch(inst, b.id) == {:error, :gone}
      assert {:ok, %{id: _}} = Storage.fetch(inst, c.id)

      # A hook received after the erase is the new tenant's, and is not gone.
      Process.sleep(5)
      source!(inst, "acme", "billing", "permanent")
      d = ingest!(inst, "acme.billing", "d")
      {:ok, _} = Pipeline.tick(inst)
      assert {:ok, 1} = Compactor.tick(inst)
      assert {:ok, %{id: _}} = Storage.fetch(inst, d.id)

      # Replaying the archive for the tenant brings back its new hook only.
      to = System.system_time(:millisecond)
      Process.sleep(5)

      assert {:ok, :created, job} =
               Replay.start(inst, %{kind: :archive, from: 0, to: to, tenants: ["acme"]})

      eventually(
        fn -> match?({:ok, %{state: :done}}, Replay.get(inst, job.id)) end,
        15_000,
        "the archive replay did not finish"
      )

      assert {:ok, done} = Replay.get(inst, job.id)
      assert done.moved == 1
      assert done.skipped >= 2
      refute a.id in stored_ids(inst)
      refute b.id in stored_ids(inst)
    end
  end

  # ── a kill ─────────────────────────────────────────────────────────────────

  describe "a killed erase" do
    @harness Path.expand("../support/erase_harness.exs", __DIR__)

    test "resumes after kill -9: the tenant stays closed, the claimed rows go, the event comes" do
      data_dir = unique_data_dir(:erase_harness)
      on_exit(fn -> File.rm_rf(data_dir) end)

      # The child has claimed acme's rows (its sink never answers) and is in the
      # `queue` stage of its erase, waiting for them, when it is killed.
      port = start_child(data_dir)
      on_exit(fn -> kill_if_alive(port) end)
      {:os_pid, os_pid} = Port.info(port, :os_pid)
      await_line(port, "STAGE :queue")

      {_, 0} = System.cmd("kill", ["-9", Integer.to_string(os_pid)])
      assert_receive {^port, {:exit_status, 137}}, 10_000

      config = EraseHarness.config(data_dir, lifecycle: %{sinks: [{CaptureSink, to: self()}]})
      inst = config.instance
      put_config(config)
      start_supervised!({Ankusa.Instance, config})

      # Before the job has even resumed, the boot closed the tenant.
      assert {:ok, %{state: :erasing, stage: :queue}} = Tenant.status(inst, "acme")
      assert Gate.closed?(inst, "acme")

      assert {:error, :tenant_erasing} =
               Quarantine.put(inst, envelope("acme", "acme.billing"), :bad_signature)

      await_erased(inst, "acme", 60_000)

      # acme was the first tenant the child saw.
      assert Tenants.name(inst, 1) == :not_found
      assert Tenants.name(inst, 2) == {:ok, "globex"}
      assert leftovers(inst, "acme", 1) == []
      assert stored_counters(inst) == folded_counters(inst)

      assert [_] = Enum.filter(audit_rows(inst), &(&1.action == "tenant.delete"))
      events = events_until("io.ankusa.tenant.deleted")
      assert List.last(events)["subject"] == "acme"
      refute_receive {:lifecycle, _, %{"type" => "io.ankusa.tenant.deleted"}}, 300

      # globex was not touched: its hooks are all still stored.
      {:ok, hooks} = Queue.hooks(inst, 0, 1_000)
      assert Enum.frequencies_by(hooks, & &1.tenant_id) == %{"globex" => EraseHarness.hooks()}
    end

    defp start_child(data_dir) do
      Port.open({:spawn_executable, System.find_executable("elixir")}, [
        :binary,
        :exit_status,
        {:line, 4096},
        args: [@harness, "serve"],
        env: [
          {~c"ERL_LIBS", String.to_charlist(Mix.Project.build_path() <> "/lib")},
          {~c"DATA_DIR", String.to_charlist(data_dir)}
        ]
      ])
    end

    defp await_line(port, line) do
      receive do
        {^port, {:data, {:eol, ^line}}} -> :ok
        {^port, {:data, {:eol, _other}}} -> await_line(port, line)
        {^port, {:exit_status, status}} -> flunk("the harness exited with #{status}")
      after
        90_000 -> flunk("the harness never printed #{line}")
      end
    end

    defp kill_if_alive(port) do
      case Port.info(port, :os_pid) do
        {:os_pid, os_pid} ->
          System.cmd("kill", ["-9", Integer.to_string(os_pid)], stderr_to_stdout: true)
          :ok

        nil ->
          :ok
      end
    end
  end

  # ── the request ────────────────────────────────────────────────────────────

  describe "the request" do
    test "it is refused for a wrong confirmation, the default tenant, a bad name and configured sources" do
      config =
        start_instance(
          source_store:
            {SourceStore.Persistent,
             decoder: &__MODULE__.decoder/2,
             sources: %{"acme.seeded" => [tenant_id: "acme", sinks: [{PermanentSink, []}]]}}
        )

      inst = config.instance

      assert {:error, :confirm} = Tenant.delete(inst, "acme", confirm: "acme-2")
      assert {:error, :confirm} = Tenant.delete(inst, "acme")
      assert {:error, :default_tenant} = Tenant.delete(inst, "default", confirm: "default")
      assert {:error, :invalid_tenant} = Tenant.delete(inst, "a.b", confirm: "a.b")

      assert {:error, {:static_sources, ["acme.seeded"]}} =
               Tenant.delete(inst, "acme", confirm: "acme")

      # Nothing started, and nothing closed.
      assert Tenant.status(inst, "acme") == :not_found
      refute Gate.closed?(inst, "acme")
    end

    test "DELETE /v1/tenants/:tenant erases, GET says where it is, and the audit row names the caller" do
      config = start_instance()
      inst = config.instance
      source!(inst, "acme", "billing", "permanent")
      source!(inst, "globex", "orders", "permanent")
      ingest!(inst, "acme.billing", "one")

      call = fn method, path ->
        conn = Plug.Test.conn(method, path)
        conn = Ankusa.Admin.Router.call(conn, Ankusa.Admin.Router.init(instance: inst))
        {conn.status, if(conn.resp_body == "", do: %{}, else: JSON.decode!(conn.resp_body))}
      end

      assert {404, %{"error" => "erase_not_found"}} = call.(:get, "/v1/tenants/acme/erase")
      assert {400, %{"error" => "confirm_mismatch"}} = call.(:delete, "/v1/tenants/acme")

      assert {400, %{"error" => "confirm_mismatch"}} =
               call.(:delete, "/v1/tenants/acme?confirm=globex")

      assert {409, %{"error" => "default_tenant"}} =
               call.(:delete, "/v1/tenants/default?confirm=default")

      assert {400, _} = call.(:delete, "/v1/tenants/a.b?confirm=a.b")

      assert {202, %{"tenant" => "acme", "state" => "erasing"}} =
               call.(:delete, "/v1/tenants/acme?confirm=acme")

      await_erased(inst, "acme")

      assert {200, %{"tenant" => "acme", "state" => "erased", "erased_at" => at}} =
               call.(:get, "/v1/tenants/acme/erase")

      assert {:ok, _, _} = DateTime.from_iso8601(at)

      assert [row] = Enum.filter(audit_rows(inst), &(&1.action == "tenant.delete"))
      assert row.tenant == "acme"
      assert row.actor == "anonymous"

      # globex was not touched.
      assert {:ok, _} = SourceStore.get(inst, "globex", "orders")
    end

    test "with fleet rate limits the tenant's fleet buckets are reset" do
      start_instance(
        rate_limits: %{scope: :fleet},
        fleet: %{store: {FleetStore, []}, timeout_ms: 20}
      )
      |> Map.fetch!(:instance)
      |> then(fn inst ->
        source!(inst, "acme", "billing", "permanent")
        erase!(inst, "acme")
        assert_received {:fleet_reset, "acme"}
      end)
    end
  end
end
