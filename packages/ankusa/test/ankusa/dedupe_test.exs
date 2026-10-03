defmodule Ankusa.DedupeTest do
  use ExUnit.Case, async: false

  import Ankusa.TestHelpers
  alias Ankusa.{Envelope, Store}
  alias Ankusa.Edge.{Ingest, Router}
  alias Ankusa.Store.Keys

  @secret "whsec_" <> Base.encode64("supersecret-key")

  defp start_edge(sources) do
    sources =
      Map.new(sources, fn {id, opts} ->
        {id, Keyword.put_new(opts, :sinks, [{Ankusa.Sink.Log, []}])}
      end)

    config =
      test_config(roles: [:edge], source_store: {Ankusa.SourceStore.Static, sources: sources})

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

  test "the writer sweep deletes expired ?u and ?e keys" do
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

    writer = Ankusa.whereis(config.instance, :queue_writer)
    send(writer, :sweep_dedupe)

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

    send(Ankusa.whereis(inst, :queue_writer), :sweep_dedupe)

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
end
