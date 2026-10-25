defmodule Ankusa.TestHelpers do
  @moduledoc "Shared helpers for framework tests."

  import ExUnit.Callbacks

  alias Ankusa.Config

  @doc "A unique instance atom for isolation."
  def unique_instance, do: :"t#{System.unique_integer([:positive])}"

  @doc """
  A temp data dir no other test run can have used. `System.unique_integer/1`
  restarts at 1 in every VM, so a run that was SIGKILLed (the loss test does
  that to a child; a developer's ^C can do it to this one) leaves stores behind
  that a later run would otherwise open and inherit.
  """
  def unique_data_dir(inst) do
    Path.join(
      System.tmp_dir!(),
      "ankusa_#{inst}_#{System.unique_integer([:positive])}_#{System.os_time(:microsecond)}"
    )
  end

  @doc """
  Build an isolated `%Ankusa.Config{}` with a fresh temp `data_dir` (auto-cleaned)
  and an ephemeral HTTP port. Pass `overrides` as keyword to `Ankusa.Config.new/1`.
  """
  def test_config(overrides \\ []) do
    inst = Keyword.get(overrides, :instance, unique_instance())
    dir = unique_data_dir(inst)
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf(dir) end)

    overrides
    |> Keyword.merge(instance: inst, data_dir: dir)
    |> Keyword.put_new(:port, 0)
    |> Config.new()
  end

  @doc "Put config into persistent_term (needed before starting WAL-facade callers)."
  def put_config(config), do: Ankusa.put_config(config)

  @doc "A free TCP port from a closed listener, for a test that needs one before Bandit binds."
  def free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  @doc """
  A private CA and a server certificate it signed for the DNS name `san`, as PEM
  files in a temp dir (removed on exit): `%{cert: path, key: path, ca: path}`.
  `ca` is the bundle a client verifies the server against (`cacertfile`).
  """
  def tls_fixture(san \\ "localhost") do
    %{client_config: client, server_config: server} =
      :public_key.pkix_test_data(%{
        # sha256 + P-256: what a TLS 1.3 handshake accepts (the test data's
        # default digest, sha1, gets `no_suitable_signature_algorithm`).
        server_chain: %{
          root: [digest: :sha256, key: {:namedCurve, :secp256r1}],
          peer: [
            digest: :sha256,
            key: {:namedCurve, :secp256r1},
            extensions: [
              {:Extension, {2, 5, 29, 17}, false, [{:dNSName, String.to_charlist(san)}]}
            ]
          ]
        },
        client_chain: %{root: [], peer: []}
      })

    dir = Path.join(System.tmp_dir!(), "ankusa-tls-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf!(dir) end)

    {key_type, key_der} = server[:key]
    cert = Path.join(dir, "cert.pem")
    key = Path.join(dir, "key.pem")
    ca = Path.join(dir, "ca.pem")
    File.write!(cert, :public_key.pem_encode([{:Certificate, server[:cert], :not_encrypted}]))
    File.write!(key, :public_key.pem_encode([{key_type, key_der, :not_encrypted}]))

    File.write!(
      ca,
      :public_key.pem_encode(for der <- client[:cacerts], do: {:Certificate, der, :not_encrypted})
    )

    %{cert: cert, key: key, ca: ca}
  end

  @doc """
  Boot a routes-enabled `:edge` instance and return its config.

  `routes_opts` is the keyword under `config.routes` (minus a nested `:admin`
  list, which is merged over `port: 0`). `extra_opts` is merged into the
  top-level config, so a caller can pass `source_store:` or the like.
  """
  def start_routes(routes_opts \\ [], extra_opts \\ []) do
    {admin, routes_opts} = Keyword.pop(routes_opts, :admin, [])

    config =
      test_config(
        Keyword.merge(extra_opts,
          roles: [:edge],
          routes:
            routes_opts
            |> Keyword.put_new(:enabled, true)
            |> Keyword.put(:admin, Keyword.merge([port: 0], admin))
        )
      )

    start_supervised!({Ankusa.Instance, config})
    config
  end

  @doc """
  Commit `env` straight through `Ankusa.Queue`, with the sinks its source has
  right now (none if the source is unknown). Returns the committed envelope.
  """
  def enqueue!(instance, %Ankusa.Envelope{} = env) do
    sinks =
      case Ankusa.SourceStore.fetch(instance, env.source_id) do
        {:ok, source} -> source.sinks
        :error -> []
      end

    {:ok, [{:committed, committed}]} =
      Ankusa.Queue.enqueue(instance, [%{envelope: env, sinks: sinks}])

    committed
  end

  @doc "Ids of every hook still in the store, in seq order."
  def stored_ids(instance) do
    {:ok, hooks} = Ankusa.Queue.hooks(instance, 0, 100_000)
    Enum.map(hooks, & &1.id)
  end

  @doc """
  `env` encoded as a newer node (N+1) would write it: `version` 0 (bare term) or
  1 (`"AE", 1`), plus one extra key this node knows nothing about. The key is
  spliced into the encoded bytes under a random name, so its atom is not in
  this VM's atom table (a test that built the map with `Map.put/3` would intern
  it itself and prove nothing). Returns `{field_name, binary}`.
  """
  def envelope_from_the_future(%Ankusa.Envelope{} = env, version \\ 1) do
    name = "n1_" <> Base.encode16(:crypto.strong_rand_bytes(10), case: :lower)
    placeholder = String.duplicate("p", byte_size(name))

    term =
      env
      |> Map.from_struct()
      |> Map.put(String.to_atom(placeholder), "added by N+1")
      |> :erlang.term_to_binary([:deterministic])
      |> :binary.replace(placeholder, name)

    bin = if version == 1, do: <<"AE", 1, term::binary>>, else: term
    {name, bin}
  end

  @doc "Build a raw ingest request map for `Ankusa.Edge.Ingest`/router."
  def request(source_id, body, headers \\ []) do
    %{
      source_id: source_id,
      method: "POST",
      path: "/webhooks/#{source_id}",
      headers: headers,
      body: body
    }
  end

  @doc """
  Send a `request/3` map through `Ankusa.Edge.Router` for `config`'s instance
  and return the `Plug.Conn`. No socket: the router runs on a `Plug.Test` conn.
  A `:peer_cert` key on `req` (a DER certificate) is the client certificate the
  connection presents, as Bandit would report it for a mutual-TLS listener; a
  query string in `req.path` is the request's query string.
  """
  def route_through_edge(config, req) do
    conn =
      Enum.reduce(
        req.headers,
        Plug.Test.conn(:post, req.path, req.body),
        fn {k, v}, conn -> Plug.Conn.put_req_header(conn, k, v) end
      )

    conn =
      case Map.get(req, :peer_cert) do
        nil ->
          conn

        der ->
          Plug.Test.put_peer_data(conn, %{address: {127, 0, 0, 1}, port: 1, ssl_cert: der})
      end

    Ankusa.Edge.Router.call(conn, Ankusa.Edge.Router.init(instance: config.instance))
  end

  @doc "Build valid Standard Webhooks signature headers for `body` and `secret`."
  def standard_webhooks_headers(id, body, secret, ts \\ nil) do
    ts = ts || System.system_time(:second)
    key = secret |> String.replace_prefix("whsec_", "") |> Base.decode64!()
    signed = "#{id}.#{ts}.#{body}"
    sig = Base.encode64(:crypto.mac(:hmac, :sha256, key, signed))

    [
      {"webhook-id", id},
      {"webhook-timestamp", to_string(ts)},
      {"webhook-signature", "v1,#{sig}"}
    ]
  end

  @doc """
  The ordinal of `tenant`, allocated through the writer (the one process that
  allocates) when the tenant has none yet: a hook with no sink and no archive
  commits nothing but the ordinal.
  """
  def tenant_ord!(instance, tenant \\ "default") do
    case Ankusa.Queue.Tenants.ord(instance, tenant) do
      {:ok, ord} ->
        ord

      :not_found ->
        env = %Ankusa.Envelope{
          id: Ankusa.UUIDv7.generate(),
          source_id: "ord-probe",
          tenant_id: tenant,
          received_at: System.system_time(:millisecond),
          method: "POST",
          path: "/",
          headers: [],
          body: "",
          size: 0
        }

        {:ok, [{:committed, _}]} = Ankusa.Queue.enqueue(instance, [%{envelope: env, sinks: []}])
        {:ok, ord} = Ankusa.Queue.Tenants.ord(instance, tenant)
        ord
    end
  end

  @doc """
  The index side of a pending row at `at`, counters included: what a commit
  leaves beside the hook and its delivery row.
  """
  def due_index_ops(instance, seq, sink, at, size, tenant \\ "default") do
    ord = tenant_ord!(instance, tenant)

    Ankusa.Queue.Deliveries.due_ops(ord, at, seq, sink, size, 0) ++
      Ankusa.Queue.Deliveries.Counters.ops([{ord, :pending, 1}, {ord, :bytes, size}])
  end

  @doc "The index side of a dead row: both dead keys and the counters."
  def dead_index_ops(instance, seq, sink, at, source_id, id, tenant \\ "default", size \\ 0) do
    ord = tenant_ord!(instance, tenant)

    [
      {:put, :index, Ankusa.Store.Keys.dead(at, seq, sink),
       :erlang.term_to_binary({source_id, id, ord})},
      {:put, :index, Ankusa.Store.Keys.dead_by_tenant(ord, at, seq, sink), <<>>}
    ] ++
      Ankusa.Queue.Deliveries.Counters.ops([
        {ord, :dead, 1},
        {ord, :bytes, size},
        {ord, :dead_bytes, size}
      ])
  end

  @doc """
  What the queue counters must be, folded from the index families: `%{ord =>
  %{pending, scheduled, inflight, dead, bytes, archive_pending, dedupe, dead_bytes, claim_leases}, ordinal 0
  the sum of the others. Only tenants with a non-zero counter appear.
  """
  def folded_counters(instance) do
    alias Ankusa.Store
    alias Ankusa.Store.Keys

    zero = %{
      pending: 0,
      scheduled: 0,
      inflight: 0,
      dead: 0,
      bytes: 0,
      archive_pending: 0,
      dedupe: 0,
      dead_bytes: 0,
      claim_leases: 0
    }

    bump = fn acc, ord, field, n ->
      Map.update(acc, ord, %{zero | field => n}, &%{&1 | field => &1[field] + n})
    end

    fold = fn family, acc, fun ->
      %{lo: lo, hi: hi} = Keys.family(family)

      {:ok, acc} =
        Store.fold(instance, family, {lo, hi}, acc, fn k, v, acc -> {:cont, fun.(k, v, acc)} end)

      acc
    end

    acc =
      fold.(:due, %{}, fn key, <<size::32, kind::8>>, acc ->
        {ord, _at, _seq, _sink} = Keys.decode_due(key)

        acc
        |> bump.(ord, if(kind == 0, do: :pending, else: :scheduled), 1)
        |> bump.(ord, :bytes, size)
      end)

    acc =
      fold.(:inflight, acc, fn key, <<size::32>>, acc ->
        {ord, _seq, _sink} = Keys.decode_inflight(key)
        acc |> bump.(ord, :inflight, 1) |> bump.(ord, :bytes, size)
      end)

    acc =
      fold.(:dead_by_tenant, acc, fn key, _v, acc ->
        {ord, at, seq, sink} = Keys.decode_dead_by_tenant(key)

        size =
          with {:ok, bin} <- Store.get(instance, :deliveries, Keys.delivery(seq, sink)),
               {:ok, %{size: size}} <- Ankusa.Queue.Deliveries.decode_row(bin) do
            size
          else
            _ -> 0
          end

        _ = at

        acc
        |> bump.(ord, :dead, 1)
        |> bump.(ord, :bytes, size)
        |> bump.(ord, :dead_bytes, size)
      end)

    acc =
      fold.(:archive_pending, acc, fn _key, <<_size::32, ord::32>>, acc ->
        bump.(acc, ord, :archive_pending, 1)
      end)

    acc =
      fold.(:dedupe_expiry, acc, fn key, _v, acc ->
        {_at, tenant, _source, _key} = Keys.decode_dedupe_expiry(key)
        bump.(acc, tenant_ord!(instance, tenant), :dedupe, 1)
      end)

    acc =
      fold.(:claim_leases, acc, fn key, _v, acc ->
        {tenant, _pack, _seq} = Keys.decode_claim_lease(key)
        bump.(acc, tenant_ord!(instance, tenant), :claim_leases, 1)
      end)

    total =
      Enum.reduce(acc, zero, fn {_ord, counters}, total ->
        Map.merge(total, counters, fn _field, a, b -> a + b end)
      end)

    acc |> Map.put(0, total) |> Map.reject(fn {_ord, c} -> c == zero end)
  end

  @doc "The counters as stored, for the ordinals of `folded_counters/1` and any more with a row."
  def stored_counters(instance) do
    {:ok, next} = Ankusa.Queue.Tenants.next_ord(instance)

    zero = %{
      pending: 0,
      scheduled: 0,
      inflight: 0,
      dead: 0,
      bytes: 0,
      archive_pending: 0,
      dedupe: 0,
      dead_bytes: 0,
      claim_leases: 0
    }

    for ord <- 0..(next - 1), reduce: %{} do
      acc ->
        {:ok, counters} = Ankusa.Queue.Deliveries.Counters.read(instance, ord)
        if counters == zero, do: acc, else: Map.put(acc, ord, counters)
    end
  end

  @doc """
  Runs `fun` in a fresh process traced for calls into the store's read API
  and returns `{result, calls}`: `{:fold, family}`, `{:first, family}`,
  `{:get, cf, [key]}` and `{:multi_get, cf, keys}`, in call order.

  A process cannot be its own tracer in a way that delivers (the trace messages
  never arrive), so `fun` runs in a spawned process traced by the caller.
  """
  def store_calls(fun) do
    me = self()

    pid =
      spawn_link(fn ->
        receive do
          :go ->
            result =
              try do
                {:ok, fun.()}
              catch
                kind, reason -> {:raised, kind, reason, __STACKTRACE__}
              end

            send(me, {:store_calls_result, self(), result})
        end
      end)

    patterns = [fold: 6, first: 4, get: 3, multi_get: 3]
    for {f, a} <- patterns, do: :erlang.trace_pattern({Ankusa.Store, f, a}, true, [:local])
    1 = :erlang.trace(pid, true, [:call, {:tracer, me}])
    send(pid, :go)

    outcome =
      receive do
        {:store_calls_result, ^pid, outcome} -> outcome
      after
        60_000 -> raise "store_calls: the traced function did not return"
      end

    # The traced process exits right after replying, which ends its tracing.
    for {f, a} <- patterns, do: :erlang.trace_pattern({Ankusa.Store, f, a}, false, [:local])
    calls = collect_store_calls(pid, [])

    case outcome do
      {:ok, result} -> {result, calls}
      {:raised, kind, reason, stack} -> :erlang.raise(kind, reason, stack)
    end
  end

  defp collect_store_calls(pid, acc) do
    receive do
      {:trace, ^pid, :call, {Ankusa.Store, :fold, [_instance, family | _]}} ->
        collect_store_calls(pid, [{:fold, family} | acc])

      {:trace, ^pid, :call, {Ankusa.Store, :first, [_instance, family | _]}} ->
        collect_store_calls(pid, [{:first, family} | acc])

      {:trace, ^pid, :call, {Ankusa.Store, :get, [_instance, cf, key]}} ->
        collect_store_calls(pid, [{:get, cf, [key]} | acc])

      {:trace, ^pid, :call, {Ankusa.Store, :multi_get, [_instance, cf, keys]}} ->
        collect_store_calls(pid, [{:multi_get, cf, keys} | acc])
    after
      100 -> Enum.reverse(acc)
    end
  end
end
