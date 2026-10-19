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
  """
  def route_through_edge(config, req) do
    conn =
      Enum.reduce(
        req.headers,
        Plug.Test.conn(:post, req.path, req.body),
        fn {k, v}, conn -> Plug.Conn.put_req_header(conn, k, v) end
      )

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
    ] ++ Ankusa.Queue.Deliveries.Counters.ops([{ord, :dead, 1}, {ord, :bytes, size}])
  end

  @doc """
  What the queue counters must be, folded from the index families: `%{ord =>
  %{pending, scheduled, inflight, dead, bytes, archive_pending}}`, ordinal 0
  the sum of the others. Only tenants with a non-zero counter appear.
  """
  def folded_counters(instance) do
    alias Ankusa.Store
    alias Ankusa.Store.Keys

    zero = %{pending: 0, scheduled: 0, inflight: 0, dead: 0, bytes: 0, archive_pending: 0}
    bump = fn acc, ord, field, n -> Map.update(acc, ord, %{zero | field => n}, &%{&1 | field => &1[field] + n}) end

    fold = fn family, acc, fun ->
      %{lo: lo, hi: hi} = Keys.family(family)
      {:ok, acc} = Store.fold(instance, family, {lo, hi}, acc, fn k, v, acc -> {:cont, fun.(k, v, acc)} end)
      acc
    end

    acc =
      fold.(:due, %{}, fn key, <<size::32, kind::8>>, acc ->
        {ord, _at, _seq, _sink} = Keys.decode_due(key)
        acc |> bump.(ord, if(kind == 0, do: :pending, else: :scheduled), 1) |> bump.(ord, :bytes, size)
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
        acc |> bump.(ord, :dead, 1) |> bump.(ord, :bytes, size)
      end)

    acc =
      fold.(:archive_pending, acc, fn _key, <<_size::32, ord::32>>, acc ->
        bump.(acc, ord, :archive_pending, 1)
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
    zero = %{pending: 0, scheduled: 0, inflight: 0, dead: 0, bytes: 0, archive_pending: 0}

    for ord <- 0..(next - 1), reduce: %{} do
      acc ->
        {:ok, counters} = Ankusa.Queue.Deliveries.Counters.read(instance, ord)
        if counters == zero, do: acc, else: Map.put(acc, ord, counters)
    end
  end
end
