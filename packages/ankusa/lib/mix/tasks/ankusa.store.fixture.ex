defmodule Mix.Tasks.Ankusa.Store.Fixture do
  @shortdoc "Writes a store upgrade fixture from a seeded instance"

  @moduledoc """
  Seeds an instance and packs its data directory as an upgrade fixture.

      mix ankusa.store.fixture test/fixtures/store-v1.tar.zst

  Run it at the release that introduces a store format and check the result in
  as `test/fixtures/store-v<version>.tar.zst`: `test/ankusa/store_upgrade_test.exs`
  boots current code on every fixture, so each later release has to read the
  stores this one wrote.

  The seeded instance holds hooks still waiting on a sink (with their ingest
  dedupe keys), dead-lettered hooks, hooks that were delivered and archived (so
  only the archive catalogue and its segment know them), quarantined hooks and
  API-managed sources (one of them changed since, so it is at revision 2), a
  tenant's rate-limit override and the audit trail those changes wrote. The
  archive holds `store/` (the RocksDB database), `segments/` (the compacted
  objects) and `manifest.json`, the ids, revisions, audit rows and counts the
  upgrade test asserts after the boot. Compressed with zstd.
  """

  use Mix.Task

  alias Ankusa.{Audit, Config, Envelope, Queue, SourceStore, Store}
  alias Ankusa.Dispatch.Pipeline
  alias Ankusa.Edge.{Ingest, Quarantine}
  alias Ankusa.Storage.Compactor
  alias Ankusa.Store.Keys

  defmodule RetrySink do
    @moduledoc false
    @behaviour Ankusa.Sink

    # The row waits an hour and is retried, so it is still pending whenever the
    # fixture is booted.
    @impl true
    def deliver(_env, _ctx, _opts), do: {:error, {:retry_after, 3_600_000, :fixture}}
  end

  defmodule DeadSink do
    @moduledoc false
    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: {:error, {:permanent, :fixture}}
  end

  defmodule OrderedSink do
    @moduledoc false
    @behaviour Ankusa.Sink

    # A head waits an hour out a retry, like `RetrySink`, so a lane's rows are
    # still in the store whenever the fixture is booted. A test that boots the
    # fixture hands it a `:pid` (the row binds to this module) and it reports
    # what it is given instead.
    @impl true
    def deliver(env, _ctx, opts) do
      case Keyword.fetch(opts, :pid) do
        {:ok, pid} ->
          send(pid, {:delivered, env.id})
          :ok

        :error ->
          {:error, {:retry_after, 3_600_000, :fixture}}
      end
    end
  end

  @instance :fixture

  # Twenty years: a fixture is booted long after it was written, and its dedupe
  # keys must still be inside their window.
  @dedupe_ttl_ms 20 * 365 * 24 * 3_600_000

  @impl Mix.Task
  def run(args) do
    path =
      case OptionParser.parse(args, strict: []) do
        {[], [path], []} ->
          path

        _ ->
          Mix.raise("usage: mix ankusa.store.fixture <path>, e.g. test/fixtures/store-v1.tar.zst")
      end

    Mix.Task.run("app.config")
    # The default instance must not boot (and bind a port) because this task
    # started the application.
    Application.put_env(:ankusa, :autostart, false)
    {:ok, _} = Application.ensure_all_started(:ankusa)

    manifest = build(path)

    Mix.shell().info(
      "wrote #{path}: store format #{manifest["format_version"]}, " <>
        "#{length(manifest["hook_ids"])} stored hooks, #{length(manifest["archived_ids"])} archived"
    )
  end

  @doc false
  # The sources the fixture instance serves, for the test that boots it again.
  def sources do
    none = {Ankusa.Verifier.None, []}

    %{
      "demo" => [
        verifier: none,
        sinks: [{RetrySink, []}],
        dedupe: %{header: "x-event-id", ttl_ms: @dedupe_ttl_ms}
      ],
      "doomed" => [verifier: none, sinks: [{DeadSink, []}]],
      "ordered" => [
        verifier: none,
        sinks: [{OrderedSink, ordered: true, ordering_key: {:header, "x-entity"}}]
      ],
      "archived" => [verifier: none, sinks: [{Ankusa.Sink.Log, []}]]
    }
  end

  @doc false
  def decoder(_source_id, _spec),
    do: [verifier: {Ankusa.Verifier.None, []}, sinks: [{Ankusa.Sink.Log, []}]]

  @doc false
  # `Ankusa.Config.new/1` options shared by the seeding run and the test.
  def config_opts do
    [
      roles: [:edge, :dispatch, :storage],
      storage: %{interval_ms: 0},
      source_store:
        {Ankusa.SourceStore.Persistent, decoder: &__MODULE__.decoder/2, sources: sources()}
    ]
  end

  @doc """
  Seeds an instance in a scratch directory and writes the fixture archive to
  `path`. Returns the manifest packed beside the data.
  """
  @spec build(Path.t()) :: map()
  def build(path) do
    data_dir =
      Path.join(System.tmp_dir!(), "ankusa_fixture_#{System.unique_integer([:positive])}")

    config = Config.new([instance: @instance, data_dir: data_dir, port: 0] ++ config_opts())

    try do
      manifest = seed(config)
      pack(path, config, manifest)
      manifest
    after
      File.rm_rf(data_dir)
    end
  end

  # Two lives of one instance. The first delivers: hooks of the default tenant
  # and of `acme` wait out a retry, `doomed` ones (of the default tenant and of
  # `deadonly`, a tenant with nothing else) are dead-lettered, and the archived
  # ones are compacted. The second runs no dispatch, so what it ingests stays
  # pending with its archive obligation, and one row is claimed and left, as a
  # crash leaves it.
  defp seed(config) do
    inst = config.instance
    {:ok, sup} = Ankusa.Instance.start_link(config)

    delivered =
      try do
        seed_delivered(inst)
      after
        Supervisor.stop(sup)
      end

    {:ok, sup} = Ankusa.Instance.start_link(%{config | roles: [:edge, :storage]})

    try do
      seed_waiting(inst, delivered)
    after
      Supervisor.stop(sup)
    end
  end

  defp seed_delivered(inst) do
    demo = for n <- 1..5, do: ingest!(inst, "demo", "demo-#{n}", [{"x-event-id", "evt-#{n}"}])
    dup = Ingest.ingest(inst, request("demo", "demo-1", [{"x-event-id", "evt-1"}]))
    {:duplicate, %Envelope{id: original}} = dup
    original == hd(demo) or Mix.raise("the dedupe key did not collapse onto its first hook")

    acme =
      for n <- 1..2, do: ingest!(inst, "demo", "acme-#{n}", [{"x-event-id", "acme-#{n}"}], "acme")

    doomed = for n <- 1..3, do: ingest!(inst, "doomed", "doomed-#{n}", [])
    dead_only = for n <- 1..2, do: ingest!(inst, "doomed", "deadonly-#{n}", [], "deadonly")
    archived = for n <- 1..4, do: ingest!(inst, "archived", "archived-#{n}", [])
    acme_archived = [ingest!(inst, "archived", "acme-archived", [], "acme")]

    # Ordering lanes (format 8): the head of `k1` and of the keyless lane wait
    # out a retry (due kind 3) with members behind them.
    k1 = for n <- 1..3, do: ingest!(inst, "ordered", "k1-#{n}", [{"x-entity", "k1"}])
    keyless = for n <- 1..2, do: ingest!(inst, "ordered", "keyless-#{n}", [])

    {:ok, _settled} = Pipeline.tick(inst)
    {:ok, segments} = Compactor.tick(inst)
    segments >= 1 or Mix.raise("nothing was archived")

    quarantined = for n <- 1..2, do: quarantine!(inst, "q-#{n}")
    for name <- ["billing", "crm"], do: put_source!(inst, "acme", name)

    {:ok, _updated} =
      SourceStore.put(inst, "acme", "crm", %{"sinks" => [%{"type" => "log"}]}, :update)

    {:ok, _limit} =
      Ankusa.Edge.RateLimiter.put_override(inst, "acme", %{"rate" => 5, "burst" => 10})

    {:ok, %{entries: trail}} = Audit.list(inst, limit: 1000)

    %{
      demo: demo,
      acme: acme,
      doomed: doomed ++ dead_only,
      archived: demo ++ acme ++ doomed ++ dead_only ++ archived ++ acme_archived,
      reclaimed: archived ++ acme_archived,
      quarantined: quarantined,
      trail: trail,
      segments: segments,
      k1: k1,
      keyless: keyless
    }
  end

  defp seed_waiting(inst, delivered) do
    now = System.system_time(:millisecond)

    waiting =
      for tenant <- ["acme", "globex"], n <- 1..2 do
        ingest!(inst, "demo", "waiting-#{tenant}-#{n}", [], tenant)
      end

    # One waiting hook has its body checked in: a claim row, its pack under
    # `segments/claims/`, and the held lease (format 6) that protects the pack.
    claimed = claim!(inst, hd(waiting), "acme")

    {:ok, ord} = Ankusa.Queue.Tenants.ord(inst, "acme")

    {:ok, [due | _]} =
      Ankusa.Queue.Deliveries.due_for_tenant(inst, ord, now + 60_000, 0, 1, 1_000_000)

    :ok = Ankusa.Queue.Deliveries.claim(inst, [due])

    # A busy lane: `k2`'s first hook is its head, promoted and claimed (as a crash
    # leaves it), the second a member behind it.
    k2 = for n <- 1..2, do: ingest!(inst, "ordered", "k2-#{n}", [{"x-entity", "k2"}])
    {:ok, default_ord} = Ankusa.Queue.Tenants.ord(inst, "default")

    {:ok, ops, _more} =
      Ankusa.Queue.Deliveries.Lanes.sweep(
        inst,
        default_ord,
        System.system_time(:millisecond) + 1,
        100
      )

    :ok = Store.write(inst, Ankusa.Queue.Deliveries.coalesce(ops), sync: true)

    {:ok, dues} =
      Ankusa.Queue.Deliveries.due_for_tenant(inst, default_ord, now + 60_000, 0, 100, 1_000_000)

    head = Enum.find(dues, &(&1.kind == 2)) || Mix.raise("no lane head was promoted")
    :ok = Ankusa.Queue.Deliveries.claim(inst, [head])

    {:ok, version} = Store.get(inst, :default, Keys.format_version())
    {:ok, hooks} = Queue.hooks(inst, 0, 100_000)
    stored = Enum.map(hooks, & &1.id)

    Enum.all?(delivered.reclaimed, &(&1 not in stored)) or
      Mix.raise("an archived hook was not reclaimed")

    %{
      "format_version" => String.to_integer(version),
      "hook_ids" => stored,
      "claim_hook_ids" => [claimed],
      "pending_ids" => delivered.demo ++ delivered.acme ++ waiting,
      "dead_ids" => delivered.doomed,
      "archived_ids" => delivered.archived,
      "quarantine_ids" => delivered.quarantined,
      "sources" => [["acme", "billing"], ["acme", "crm"]],
      "revisions" => %{"acme.billing" => 1, "acme.crm" => 2},
      "audit" => delivered.trail |> Enum.reverse() |> Enum.map(&[&1.action, &1.resource]),
      "dedupe" =>
        for({id, n} <- Enum.with_index(delivered.demo, 1), do: ["demo", "evt-#{n}", id]),
      "segments" => delivered.segments,
      "tenants" => ["default", "acme", "globex", "deadonly"],
      "lanes" => %{"k1" => delivered.k1, "keyless" => delivered.keyless, "k2" => k2}
    }
  end

  defp claim!(inst, id, tenant) do
    {:ok, claims} =
      Ankusa.ClaimCheck.check_in(inst, tenant, [%{id: id, body: "claimed body of #{id}"}])

    {:ok, hooks} = Queue.hooks(inst, 0, 100_000)
    %{seq: seq} = Enum.find(hooks, &(&1.id == id))
    {:ok, ord} = Ankusa.Queue.Tenants.ord(inst, tenant)
    :ok = Store.write(inst, Ankusa.Queue.Deliveries.claim_ops(seq, ord, claims[id]), sync: true)
    id
  end

  defp request(source_id, body, headers, tenant \\ nil) do
    %{
      source_id: source_id,
      method: "POST",
      path: "/webhooks/#{source_id}",
      headers: headers,
      body: body
    }
    |> then(&if(tenant, do: Map.put(&1, :tenant_id, tenant), else: &1))
  end

  defp ingest!(inst, source_id, body, headers, tenant \\ nil) do
    {:ok, %Envelope{id: id}} = Ingest.ingest(inst, request(source_id, body, headers, tenant))
    id
  end

  defp quarantine!(inst, id) do
    env = %Envelope{
      id: id,
      source_id: "demo",
      tenant_id: nil,
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/webhooks/demo",
      headers: [{"x-signature", "nope"}],
      content_type: "text/plain",
      body: "forged",
      size: 6,
      seq: nil
    }

    :ok = Quarantine.put(inst, env, {:verification_failed, :bad_signature})
    id
  end

  defp put_source!(inst, tenant, name) do
    {:ok, _stored} =
      SourceStore.put(inst, tenant, name, %{"sinks" => [%{"type" => "log"}]}, :create)
  end

  # `store/`, `segments/` and `manifest.json`, as members relative to the
  # instance directory, then zstd.
  defp pack(path, config, manifest) do
    root = Config.path(config, "")
    manifest_path = Path.join(root, "manifest.json")
    File.write!(manifest_path, JSON.encode!(manifest))

    tar = Path.join(config.data_dir, "fixture.tar")

    members =
      for name <- ["manifest.json", "store", "segments"],
          do: {String.to_charlist(name), String.to_charlist(Path.join(root, name))}

    :ok = :erl_tar.create(String.to_charlist(tar), members)

    File.mkdir_p!(Path.dirname(path))
    File.write!(path, tar |> File.read!() |> :zstd.compress() |> IO.iodata_to_binary())
  end
end
