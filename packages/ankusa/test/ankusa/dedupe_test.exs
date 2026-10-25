defmodule Ankusa.DedupeTest do
  use ExUnit.Case, async: false

  import Ankusa.TestHelpers
  alias Ankusa.Edge.{Ingest, Router}
  alias Ankusa.{Envelope, Store}
  alias Ankusa.Store.Keys

  @secret "whsec_" <> Base.encode64("supersecret-key")

  defp start_edge(sources, extra \\ []) do
    sources =
      Map.new(sources, fn {id, opts} ->
        {id, Keyword.put_new(opts, :sinks, [{Ankusa.Sink.Log, []}])}
      end)

    config =
      test_config(
        [roles: [:edge], source_store: {Ankusa.SourceStore.Static, sources: sources}] ++ extra
      )

    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp route(config, req) do
    conn =
      Plug.Test.conn(:post, req.path, req.body)
      |> then(fn c ->
        Enum.reduce(req.headers, c, fn {k, v}, c -> Plug.Conn.put_req_header(c, k, v) end)
      end)

    Router.call(conn, Router.init(instance: config.instance))
  end

  defp gh_request(body, delivery_id) do
    request("demo", body, [{"x-github-delivery", delivery_id}])
  end

  test "ingest collapses a provider retry on the delivery header, same id, duplicate: true" do
    config = start_edge(%{"demo" => [dedupe: :github]})

    first = route(config, gh_request(~s({"a":1}), "d1"))
    second = route(config, gh_request(~s({"a":1}), "d1"))

    assert first.status == 201
    assert second.status == 201
    assert %{"status" => "accepted", "id" => id1} = JSON.decode!(first.resp_body)

    assert %{"status" => "accepted", "id" => id2, "duplicate" => true} =
             JSON.decode!(second.resp_body)

    assert id1 == id2
    assert stored_ids(config.instance) == [id1]
  end

  test "a different delivery id commits" do
    config = start_edge(%{"demo" => [dedupe: :github]})

    first = route(config, gh_request("x", "d1"))
    second = route(config, gh_request("x", "d2"))

    assert %{"id" => id1} = JSON.decode!(first.resp_body)
    assert %{"id" => id2} = JSON.decode!(second.resp_body)
    assert id1 != id2
    assert length(stored_ids(config.instance)) == 2
  end

  test "two entries sharing a key in one batch: one committed, one duplicate" do
    config = start_edge(%{"demo" => []})

    env = fn ->
      %Envelope{
        id: Ankusa.UUIDv7.generate(),
        source_id: "demo",
        received_at: System.system_time(:millisecond),
        method: "POST",
        path: "/demo",
        headers: [{"x-github-delivery", "d1"}],
        body: "x",
        dedupe_key: "d1"
      }
    end

    e1 = env.()
    e2 = env.()

    assert {:ok, [{:committed, committed}, {:duplicate, duplicate}]} =
             Ankusa.Queue.enqueue(config.instance, [
               %{envelope: e1, sinks: [{Ankusa.Sink.Log, []}], dedupe_ttl_ms: 1_000},
               %{envelope: e2, sinks: [{Ankusa.Sink.Log, []}], dedupe_ttl_ms: 1_000}
             ])

    assert committed.id == e1.id
    assert is_integer(committed.seq)
    assert duplicate.id == e1.id
    assert duplicate.seq == nil
    assert stored_ids(config.instance) == [e1.id]
  end

  test "a key past its ttl commits again" do
    config = start_edge(%{"demo" => []})

    env = fn id ->
      %Envelope{
        id: id,
        source_id: "demo",
        received_at: System.system_time(:millisecond),
        method: "POST",
        path: "/demo",
        headers: [{"x-github-delivery", "d1"}],
        body: "x",
        dedupe_key: "d1"
      }
    end

    e1 = env.(Ankusa.UUIDv7.generate())
    e2 = env.(Ankusa.UUIDv7.generate())

    {:ok, [{:committed, _}]} =
      Ankusa.Queue.enqueue(config.instance, [
        %{envelope: e1, sinks: [{Ankusa.Sink.Log, []}], dedupe_ttl_ms: 50}
      ])

    Process.sleep(100)

    assert {:ok, [{:committed, committed}]} =
             Ankusa.Queue.enqueue(config.instance, [
               %{envelope: e2, sinks: [{Ankusa.Sink.Log, []}], dedupe_ttl_ms: 50}
             ])

    assert committed.id == e2.id
    assert stored_ids(config.instance) == [e1.id, e2.id]
  end

  test "a Stripe JSON id key" do
    config = start_edge(%{"stripe" => [dedupe: :stripe]})

    first = route(config, request("stripe", ~s({"id":"evt_1","x":2})))
    second = route(config, request("stripe", ~s({"id":"evt_1","x":2})))

    assert %{"id" => id1} = JSON.decode!(first.resp_body)
    assert %{"id" => id2, "duplicate" => true} = JSON.decode!(second.resp_body)
    assert id1 == id2
    assert stored_ids(config.instance) == [id1]
  end

  test "salesforce preset: a retried callout with the same id collapses to the original hook" do
    assert Ankusa.Dedupe.new!(:salesforce).from == {:json, ["id"]}

    config = start_edge(%{"sf" => [dedupe: :salesforce]})
    body = ~s({"id":"a0B5e00000XyZ1AEAV","Name":"Acme","StageName":"Closed Won"})

    first = route(config, request("sf", body))
    retry = route(config, request("sf", body))
    other = route(config, request("sf", ~s({"id":"a0B5e00000XyZ2AEAV"})))

    assert first.status == 201
    assert %{"id" => id1} = JSON.decode!(first.resp_body)
    assert %{"id" => ^id1, "duplicate" => true} = JSON.decode!(retry.resp_body)
    assert %{"id" => id3} = JSON.decode!(other.resp_body)
    assert id3 != id1
    assert stored_ids(config.instance) |> Enum.sort() == Enum.sort([id1, id3])
  end

  test "an edge boots with one warning per source without dedupe, and none for a deduped one" do
    log =
      ExUnit.CaptureLog.capture_log(fn ->
        start_edge(%{
          "boot-plain" => [],
          "boot-sf" => [dedupe: :salesforce]
        })
      end)

    assert length(Regex.scan(~r/source boot-plain has no dedupe:/, log)) == 1
    refute log =~ "source boot-sf has no dedupe"
  end

  test "a flag-accepted forged hook never claims a key" do
    sources = %{
      "demo" => [
        verifier: {Ankusa.Verifier.Hmac, scheme: :standard_webhooks, secret: @secret},
        on_verify_failure: :accept_flag,
        dedupe: :github
      ]
    }

    config = start_edge(sources)
    body = ~s({"a":1})

    first = route(config, gh_request(body, "d1"))
    second = route(config, gh_request(body, "d1"))

    assert %{"id" => id1} = JSON.decode!(first.resp_body)
    assert %{"id" => id2} = JSON.decode!(second.resp_body)
    assert id1 != id2
    assert length(stored_ids(config.instance)) == 2
  end

  test "the sweeper deletes expired ?u and ?e keys" do
    config = start_edge(%{"demo" => []})

    env = %Envelope{
      id: Ankusa.UUIDv7.generate(),
      source_id: "demo",
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/demo",
      headers: [{"x-github-delivery", "d1"}],
      body: "x",
      dedupe_key: "d1"
    }

    {:ok, [{:committed, _}]} =
      Ankusa.Queue.enqueue(config.instance, [
        %{envelope: env, sinks: [{Ankusa.Sink.Log, []}], dedupe_ttl_ms: 50}
      ])

    u_key = Keys.dedupe("default", "demo", "d1")
    assert {:ok, <<_at::64, _id::binary>>} = Store.get(config.instance, :index, u_key)

    Process.sleep(100)

    assert {:ok, 1} = Ankusa.Queue.DedupeSweeper.sweep(config.instance)

    # The sweep deletes synchronously; wait until both keys are gone.
    deadline = System.monotonic_time(:millisecond) + 2_000

    wait = fn wait ->
      case Store.get(config.instance, :index, u_key) do
        :not_found ->
          :ok

        _other ->
          if System.monotonic_time(:millisecond) > deadline do
            flunk("dedupe key was not swept within 2s")
          else
            Process.sleep(20)
            wait.(wait)
          end
      end
    end

    wait.(wait)

    # and no ?e keys remain in range
    {:ok, {0, []}} =
      Store.fold(
        config.instance,
        :dedupe_expiry,
        {<<?e>>, <<?e, System.system_time(:millisecond) + 1::64>>},
        {0, []},
        fn k, _v, {n, acc} -> {:cont, {n + 1, [k | acc]}} end
      )
  end

  test "the same source and event key under two tenants are two events, each deduped on its own" do
    config = start_edge(%{"demo" => [dedupe: :github]})
    base = request("demo", "x", [{"x-github-delivery", "d1"}])
    acme = Map.put(base, :tenant_id, "acme")
    globex = Map.put(base, :tenant_id, "globex")

    assert {:ok, acme1} = Ingest.ingest(config.instance, acme)
    assert {:ok, globex1} = Ingest.ingest(config.instance, globex)
    assert acme1.id != globex1.id

    # A retry collapses onto its own tenant's original, never the other's.
    assert {:duplicate, acme2} = Ingest.ingest(config.instance, acme)
    assert {:duplicate, globex2} = Ingest.ingest(config.instance, globex)
    assert acme2.id == acme1.id
    assert globex2.id == globex1.id
    assert length(stored_ids(config.instance)) == 2
  end

  defp direct_env(tenant_id) do
    %Envelope{
      id: Ankusa.UUIDv7.generate(),
      source_id: "demo",
      tenant_id: tenant_id,
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/demo",
      headers: [],
      body: "x",
      dedupe_key: "d1"
    }
  end

  defp direct_entry(env),
    do: %{envelope: env, sinks: [{Ankusa.Sink.Log, []}], dedupe_ttl_ms: 60_000}

  test "a direct enqueue with no tenant dedupes under the default tenant" do
    config = start_edge(%{"demo" => []})
    inst = config.instance

    assert {:ok, [{:committed, first}]} =
             Ankusa.Queue.enqueue(inst, [direct_entry(direct_env(nil))])

    assert {:ok, [{:duplicate, again}]} =
             Ankusa.Queue.enqueue(inst, [direct_entry(direct_env(nil))])

    assert again.id == first.id

    assert {:ok, <<_at::64, _id::binary>>} =
             Store.get(inst, :index, Keys.dedupe("default", "demo", "d1"))
  end

  test "a tenant the key encoding cannot carry skips dedupe and leaves the writer alive" do
    config = start_edge(%{"demo" => []})
    inst = config.instance
    writer = Ankusa.whereis(inst, :queue_writer)

    for tenant <- ["a" <> <<0>> <> "b", :acme] do
      assert {:ok, [{:committed, _}]} =
               Ankusa.Queue.enqueue(inst, [direct_entry(direct_env(tenant))])

      assert {:ok, [{:committed, _}]} =
               Ankusa.Queue.enqueue(inst, [direct_entry(direct_env(tenant))])
    end

    assert Ankusa.whereis(inst, :queue_writer) == writer
    assert length(stored_ids(inst)) == 4
  end

  test "a stored dedupe value that does not decode commits fresh and heals the key" do
    config = start_edge(%{"demo" => []})
    inst = config.instance
    u_key = Keys.dedupe("default", "demo", "d1")

    # Shorter than the 8-byte expiry it should start with.
    :ok = Store.write(inst, [{:put, :index, u_key, <<1, 2, 3>>}], sync: true)

    assert {:ok, [{:committed, first}]} =
             Ankusa.Queue.enqueue(inst, [direct_entry(direct_env("default"))])

    # The corrupt value was overwritten by the commit, so the key dedupes again.
    assert {:ok, <<_at::64, id::binary>>} = Store.get(inst, :index, u_key)
    assert id == first.id

    assert {:ok, [{:duplicate, again}]} =
             Ankusa.Queue.enqueue(inst, [direct_entry(direct_env("default"))])

    assert again.id == first.id
  end

  test "the sweep drops an expiry key it cannot decode instead of failing" do
    config = start_edge(%{"demo" => []})
    inst = config.instance

    # No tenant/source separator: `decode_dedupe_expiry/1` answers `:error`.
    bad = <<?e, 1::64, "no-separator">>
    :ok = Store.write(inst, [{:put, :index, bad, <<>>}], sync: true)

    assert {:ok, 0} = Ankusa.Queue.DedupeSweeper.sweep(inst)

    deadline = System.monotonic_time(:millisecond) + 2_000

    wait = fn wait ->
      case Store.get(inst, :index, bad) do
        :not_found ->
          :ok

        _present ->
          if System.monotonic_time(:millisecond) > deadline,
            do: flunk("malformed expiry key was not swept within 2s"),
            else: Process.sleep(20) && wait.(wait)
      end
    end

    wait.(wait)
  end

  describe "the dedupe sweeper and ceilings" do
    alias Ankusa.Queue.DedupeSweeper
    alias Ankusa.Queue.Deliveries.Counters
    alias Ankusa.Queue.Tenants

    defp keyed_entry(tenant, key, ttl_ms) do
      env = %{direct_env(tenant) | dedupe_key: key}
      %{envelope: env, sinks: [{Ankusa.Sink.Log, []}], dedupe_ttl_ms: ttl_ms}
    end

    defp assert_counters_equal_fold(inst),
      do: assert(stored_counters(inst) == folded_counters(inst))

    test "dedupe bucket sweep: expires every key past its ttl, a chunk at a time, outside the writer" do
      inst = start_edge(%{"demo" => []}).instance
      entries = for n <- 1..2_500, do: keyed_entry("acme", "k#{n}", 600)

      assert {:ok, results} = Ankusa.Queue.enqueue(inst, entries)
      assert Enum.all?(results, &match?({:committed, _}, &1))

      ord = tenant_ord!(inst, "acme")
      assert {:ok, %{dedupe: 2_500}} = Counters.read(inst, 0)
      assert {:ok, %{dedupe: 2_500}} = Counters.read(inst, ord)
      assert_counters_equal_fold(inst)

      sweeper = Ankusa.whereis(inst, :dedupe_sweeper)
      assert is_pid(sweeper)
      refute sweeper == Ankusa.whereis(inst, :queue_writer)

      # Past the ttl: more than one chunk of expired keys.
      Process.sleep(700)

      # The sweep keeps going until none is left.
      assert {:ok, 2_500} = DedupeSweeper.sweep(inst)
      assert {:ok, %{dedupe: 0}} = Counters.read(inst, 0)
      assert {:ok, %{dedupe: 0}} = Counters.read(inst, ord)
      assert :not_found = Store.get(inst, :index, Keys.dedupe("acme", "demo", "k1"))
      assert :not_found = Store.get(inst, :index, Keys.dedupe("acme", "demo", "k2500"))
      assert_counters_equal_fold(inst)
    end

    test "a key refreshed after the sweeper read it survives its stale delete" do
      inst = start_edge(%{"demo" => []}).instance
      u_key = Keys.dedupe("acme", "demo", "d1")

      assert {:ok, [{:committed, _first}]} =
               Ankusa.Queue.enqueue(inst, [keyed_entry("acme", "d1", 30)])

      {:ok, <<old_at::64, _id::binary>>} = Store.get(inst, :index, u_key)
      Process.sleep(60)

      # The expired key is refreshed in place: the live count stays at one.
      assert {:ok, [{:committed, second}]} =
               Ankusa.Queue.enqueue(inst, [keyed_entry("acme", "d1", 60_000)])

      ord = tenant_ord!(inst, "acme")
      stale = {:expired, Keys.dedupe_expiry(old_at, "acme", "demo", "d1"), u_key, old_at, ord}

      assert {:ok, 0} =
               GenServer.call(Ankusa.whereis(inst, :queue_writer), {:expire_dedupe, [stale]})

      assert {:ok, <<_at::64, id::binary>>} = Store.get(inst, :index, u_key)
      assert id == second.id
      assert {:ok, %{dedupe: 1}} = Counters.read(inst, ord)
      assert {:ok, [{:duplicate, _}]} = Ankusa.Queue.enqueue(inst, [keyed_entry("acme", "d1", 1)])
      assert_counters_equal_fold(inst)
    end

    test "dedupe.max_entries refuses a new key, never skips its dedupe, and frees up on expiry" do
      inst =
        start_edge(%{"demo" => []}, dedupe: %{max_entries: 2, max_entries_per_tenant: 100}).instance

      assert {:ok, [{:committed, _}, {:committed, _}, {:error, :dedupe_quota}]} =
               Ankusa.Queue.enqueue(inst, [
                 keyed_entry("acme", "k1", 60_000),
                 keyed_entry("globex", "k2", 50),
                 keyed_entry("acme", "k3", 60_000)
               ])

      # The refused hook was not stored, and its key was not written.
      assert length(stored_ids(inst)) == 2
      assert :not_found = Store.get(inst, :index, Keys.dedupe("acme", "demo", "k3"))
      assert {:ok, %{dedupe: 2}} = Counters.read(inst, 0)

      # A retry of a live key is a duplicate, not a refusal.
      assert {:ok, [{:duplicate, _}]} =
               Ankusa.Queue.enqueue(inst, [keyed_entry("acme", "k1", 60_000)])

      # Still full: the next new key is refused again.
      assert {:ok, [{:error, :dedupe_quota}]} =
               Ankusa.Queue.enqueue(inst, [keyed_entry("acme", "k3", 60_000)])

      # A key past its ttl is replaced in place, so it is admitted at the ceiling.
      Process.sleep(80)

      assert {:ok, [{:committed, _}]} =
               Ankusa.Queue.enqueue(inst, [keyed_entry("globex", "k2", 60_000)])

      assert {:ok, %{dedupe: 2}} = Counters.read(inst, 0)
      assert_counters_equal_fold(inst)
    end

    test "dedupe.max_entries frees room as the sweeper expires keys" do
      inst = start_edge(%{"demo" => []}, dedupe: %{max_entries: 1}).instance

      assert {:ok, [{:committed, _}]} =
               Ankusa.Queue.enqueue(inst, [keyed_entry("acme", "k1", 30)])

      assert {:ok, [{:error, :dedupe_quota}]} =
               Ankusa.Queue.enqueue(inst, [keyed_entry("acme", "k2", 60_000)])

      Process.sleep(60)
      assert {:ok, 1} = DedupeSweeper.sweep(inst)

      assert {:ok, [{:committed, _}]} =
               Ankusa.Queue.enqueue(inst, [keyed_entry("acme", "k2", 60_000)])

      assert_counters_equal_fold(inst)
    end

    test "dedupe.max_entries_per_tenant caps one tenant and leaves the others their share" do
      inst =
        start_edge(%{"demo" => []}, dedupe: %{max_entries: 100, max_entries_per_tenant: 1}).instance

      assert {:ok,
              [
                {:committed, _},
                {:error, :dedupe_quota},
                {:committed, _}
              ]} =
               Ankusa.Queue.enqueue(inst, [
                 keyed_entry("acme", "k1", 60_000),
                 keyed_entry("acme", "k2", 60_000),
                 keyed_entry("globex", "k1", 60_000)
               ])

      assert {:ok, ord} = Tenants.ord(inst, "acme")
      assert {:ok, %{dedupe: 1}} = Counters.read(inst, ord)
      assert_counters_equal_fold(inst)
    end

    test "an ingest past the ceiling is a 503 dedupe_quota with Retry-After, and a retry still dedupes" do
      config = start_edge(%{"demo" => [dedupe: :github]}, dedupe: %{max_entries: 1})

      assert %{status: 201} = route(config, gh_request("x", "d1"))

      refused = route(config, gh_request("x", "d2"))
      assert refused.status == 503
      assert JSON.decode!(refused.resp_body) == %{"error" => "dedupe_quota"}
      assert Plug.Conn.get_resp_header(refused, "retry-after") == ["60"]
      assert length(stored_ids(config.instance)) == 1

      assert %{"duplicate" => true} =
               JSON.decode!(route(config, gh_request("x", "d1")).resp_body)
    end
  end

  test "new!/1 rejects malformed specs" do
    assert_raise ArgumentError, ~r/^dedupe: /, fn -> Ankusa.Dedupe.new!(%{preset: :nope}) end
    assert_raise ArgumentError, ~r/^dedupe: /, fn -> Ankusa.Dedupe.new!(%{ttl_ms: 10}) end
    assert_raise ArgumentError, ~r/^dedupe: /, fn -> Ankusa.Dedupe.new!(%{header: ""}) end
    assert_raise ArgumentError, ~r/^dedupe: /, fn -> Ankusa.Dedupe.new!(%{json: "a..b"}) end

    assert_raise ArgumentError, ~r/^dedupe: /, fn ->
      Ankusa.Dedupe.new!(%{header: "a", json: "b"})
    end

    assert_raise ArgumentError, ~r/^dedupe: /, fn -> Ankusa.Dedupe.new!(%{ttl_ms: 0}) end
  end

  test "a key longer than 200 bytes is hashed" do
    long = String.duplicate("k", 300)
    dedupe = Ankusa.Dedupe.new!(header: "x-k")

    env = %Envelope{
      id: Ankusa.UUIDv7.generate(),
      source_id: "demo",
      received_at: 0,
      method: "POST",
      path: "/demo",
      headers: [{"x-k", long}],
      body: "x"
    }

    assert Ankusa.Dedupe.key(dedupe, env) ==
             "sha256:" <> Base.encode16(:crypto.hash(:sha256, long), case: :lower)
  end

  test "a JSON key with a non-string final value yields nil" do
    dedupe = Ankusa.Dedupe.new!(json: "data.id")

    env = %Envelope{
      id: "x",
      source_id: "demo",
      received_at: 0,
      method: "POST",
      path: "/demo",
      headers: [],
      body: ~s({"data":{"id":[1,2]}})
    }

    assert Ankusa.Dedupe.key(dedupe, env) == nil

    env = %{env | body: ~s({"data":{"id":42}})}
    assert Ankusa.Dedupe.key(dedupe, env) == "42"

    env = %{env | body: "not json"}
    assert Ankusa.Dedupe.key(dedupe, env) == nil
  end

  test "ingest responds 201 duplicate for a direct call through Ingest" do
    config = start_edge(%{"demo" => [dedupe: :github]})

    assert {:ok, env1} = Ingest.ingest(config.instance, gh_request("x", "d9"))
    assert {:duplicate, env2} = Ingest.ingest(config.instance, gh_request("x", "d9"))
    assert env1.id == env2.id
    assert env2.seq == nil
  end

  describe "the same key on two batcher partitions" do
    alias Ankusa.Edge.Batcher

    # Both partitions read the key as absent (the writer is suspended, so neither
    # commit has happened) before the writer sees either call.
    defp racing_commits(config) do
      inst = config.instance
      writer = Ankusa.whereis(inst, :queue_writer)
      :ok = :sys.suspend(writer)

      tasks =
        for partition <- [0, 1] do
          Task.async(fn -> Batcher.commit(inst, partition, direct_entry(direct_env(nil))) end)
        end

      assert wait_for(fn ->
               Process.info(writer, :message_queue_len) == {:message_queue_len, 2}
             end)

      :ok = :sys.resume(writer)
      Enum.map(tasks, &Task.await(&1, 10_000))
    end

    defp wait_for(fun, tries \\ 200) do
      cond do
        fun.() -> true
        tries == 0 -> false
        true -> Process.sleep(10) && wait_for(fun, tries - 1)
      end
    end

    defp delivery_rows(inst) do
      %{lo: lo, hi: hi} = Keys.family(:deliveries)

      {:ok, n} =
        Store.fold(inst, :deliveries, {lo, hi}, 0, fn _key, _value, n -> {:cont, n + 1} end)

      n
    end

    defp assert_one_hook(inst, results) do
      assert [{:committed, committed}] = for({:committed, _} = r <- results, do: r)
      assert [{:duplicate, duplicate}] = for({:duplicate, _} = r <- results, do: r)

      assert duplicate.id == committed.id
      assert stored_ids(inst) == [committed.id]
      assert delivery_rows(inst) == 1
    end

    test "merged into one group, one hook id and one delivery" do
      config = start_edge(%{"demo" => []}, batcher: %{partitions: 2})

      results = racing_commits(config)

      assert_one_hook(config.instance, results)
    end

    test "in two groups, the second still meets the first's key although it read it as absent" do
      # one hook per group: the second call's prefetch predates the first commit
      config =
        start_edge(%{"demo" => []}, batcher: %{partitions: 2, group_commit_max_records: 1})

      results = racing_commits(config)

      assert_one_hook(config.instance, results)
    end

    test "a key the sweeper deleted is not remembered, and commits again" do
      config = start_edge(%{"demo" => []})
      inst = config.instance

      env = fn -> direct_env(nil) end
      entry = fn -> %{direct_entry(env.()) | dedupe_ttl_ms: 30} end

      assert {:ok, [{:committed, _}]} = Ankusa.Queue.enqueue(inst, [entry.()])
      Process.sleep(60)
      assert {:ok, _} = Ankusa.Queue.DedupeSweeper.sweep(inst)

      assert {:ok, [{:committed, _}]} = Ankusa.Queue.enqueue(inst, [entry.()])
      assert length(stored_ids(inst)) == 2
      assert_counters_equal_fold(inst)
    end
  end
end
