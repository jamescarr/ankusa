defmodule Ankusa.SourceStorePersistentTest do
  @moduledoc """
  The writable store and its facade. The store owns the tenant-scoped sources
  the admin API manages; the facade is what every caller uses, so most of this
  suite drives `Ankusa.SourceStore` rather than the store module directly.
  """

  use ExUnit.Case, async: true

  import Ankusa.TestHelpers

  alias Ankusa.{Source, SourceStore, Store}
  alias Ankusa.SourceStore.Persistent
  alias Ankusa.Store.Keys

  @spec_map %{
    "verify" => %{"type" => "hmac", "secret" => "s3cr3t", "signature_header" => "X-Sig"},
    "on_verify_failure" => "reject",
    "sinks" => [%{"type" => "log"}]
  }

  @log_spec %{"sinks" => [%{"type" => "log"}]}

  # ── the facade, and stores that cannot write ────────────────────────────────

  describe "a store without the write callbacks" do
    setup do
      config = test_config()
      put_config(config)
      %{config: config}
    end

    test "put, get, and list_tenant report read-only", %{config: config} do
      assert SourceStore.put(config.instance, "acme", "billing", @spec_map, :create) ==
               {:error, :read_only}

      assert SourceStore.get(config.instance, "acme", "billing") == :error
      assert SourceStore.list_tenant(config.instance, "acme") == []
      assert SourceStore.delete(config.instance, "acme", "billing") == {:error, :read_only}
    end

    test "a bad tenant is rejected before the store is consulted", %{config: config} do
      assert {:error, :invalid, message} =
               SourceStore.put(config.instance, "bad tenant", "billing", @spec_map, :create)

      assert message =~ "tenant"

      assert {:error, :invalid, delete_message} =
               SourceStore.delete(config.instance, "bad tenant", "billing")

      assert delete_message =~ "tenant"
    end
  end

  # ── create / list / update ──────────────────────────────────────────────────

  test "create, list, and update one source" do
    {config, _pid} = start_store()
    instance = config.instance

    assert {:ok, stored} = SourceStore.put(instance, "acme", "billing", @spec_map, :create)

    assert stored == %{
             tenant: "acme",
             name: "billing",
             source_id: "acme.billing",
             spec: @spec_map
           }

    assert SourceStore.get(instance, "acme", "billing") == {:ok, stored}
    assert SourceStore.list_tenant(instance, "acme") == [stored]
    assert SourceStore.list(instance) == ["acme.billing"]

    assert {:ok, %Source{id: "acme.billing", tenant_id: "acme"}} =
             SourceStore.fetch(instance, "acme.billing")

    assert {:ok, updated} = SourceStore.put(instance, "acme", "billing", @log_spec, :update)
    assert updated.spec == @log_spec
    assert SourceStore.get(instance, "acme", "billing") == {:ok, updated}
  end

  test "create of an existing source is :exists; update of a missing one is :not_found" do
    {config, _pid} = start_store()
    instance = config.instance

    assert {:ok, _} = SourceStore.put(instance, "acme", "billing", @spec_map, :create)
    assert SourceStore.put(instance, "acme", "billing", @spec_map, :create) == {:error, :exists}

    assert SourceStore.put(instance, "acme", "nope", @spec_map, :update) == {:error, :not_found}
  end

  test "list_tenant is sorted by name and scoped to its tenant" do
    {config, _pid} = start_store()
    instance = config.instance

    for name <- ~w(zebra apple mango) do
      assert {:ok, _} = SourceStore.put(instance, "acme", name, @log_spec, :create)
    end

    assert {:ok, _} = SourceStore.put(instance, "beta", "apple", @log_spec, :create)

    assert Enum.map(SourceStore.list_tenant(instance, "acme"), & &1.name) ==
             ~w(apple mango zebra)

    assert Enum.map(SourceStore.list_tenant(instance, "beta"), & &1.name) == ["apple"]
    assert SourceStore.list_tenant(instance, "gamma") == []
  end

  # ── tenant isolation ────────────────────────────────────────────────────────

  test "tenant A cannot read or overwrite tenant B's same-named source" do
    {config, _pid} = start_store()
    instance = config.instance

    assert {:ok, acme} = SourceStore.put(instance, "acme", "billing", @spec_map, :create)

    # The same name under another tenant is a different source, not a conflict.
    assert {:ok, beta} = SourceStore.put(instance, "beta", "billing", @log_spec, :create)
    assert beta.source_id == "beta.billing"

    assert {:ok, read_beta} = SourceStore.get(instance, "beta", "billing")
    assert read_beta.spec == @log_spec
    assert read_beta.source_id == "beta.billing"

    # Updating B leaves A untouched.
    assert {:ok, _} = SourceStore.put(instance, "beta", "billing", @log_spec, :update)
    assert SourceStore.get(instance, "acme", "billing") == {:ok, acme}

    # And each tenant only lists its own.
    assert Enum.map(SourceStore.list_tenant(instance, "acme"), & &1.source_id) == ["acme.billing"]
    assert Enum.map(SourceStore.list_tenant(instance, "beta"), & &1.source_id) == ["beta.billing"]
  end

  # ── the secret survives an edit ─────────────────────────────────────────────

  test "an update whose verify omits the secret keeps the stored one" do
    {config, _pid} = start_store()
    instance = config.instance

    assert {:ok, _} = SourceStore.put(instance, "acme", "billing", @spec_map, :create)

    edit = %{
      "verify" => %{"type" => "hmac", "signature_header" => "X-Other"},
      "sinks" => [%{"type" => "log"}]
    }

    assert {:ok, stored} = SourceStore.put(instance, "acme", "billing", edit, :update)

    assert stored.spec["verify"] == %{
             "type" => "hmac",
             "signature_header" => "X-Other",
             "secret" => "s3cr3t"
           }

    # A different verify type does not inherit the secret.
    rotated = %{"verify" => %{"type" => "stripe"}, "sinks" => [%{"type" => "log"}]}

    assert {:ok, rotated_stored} =
             SourceStore.put(instance, "acme", "billing", rotated, :update)

    refute Map.has_key?(rotated_stored.spec["verify"], "secret")
  end

  # ── delete ──────────────────────────────────────────────────────────────────

  test "delete removes the source from ETS and the list" do
    {config, _pid} = start_store()
    instance = config.instance

    assert {:ok, _} = SourceStore.put(instance, "acme", "billing", @spec_map, :create)
    assert {:ok, _} = SourceStore.put(instance, "acme", "other", @log_spec, :create)
    assert {:ok, _} = SourceStore.put(instance, "beta", "billing", @log_spec, :create)

    assert SourceStore.delete(instance, "acme", "billing") == :ok

    assert SourceStore.get(instance, "acme", "billing") == :error
    assert SourceStore.fetch(instance, "acme.billing") == :error
    assert Enum.sort(SourceStore.list(instance)) == ["acme.other", "beta.billing"]
    assert Enum.map(SourceStore.list_tenant(instance, "acme"), & &1.name) == ["other"]
    assert Enum.map(SourceStore.list_tenant(instance, "beta"), & &1.source_id) == ["beta.billing"]
  end

  test "delete survives a store restart" do
    {config, pid} = start_store()
    instance = config.instance

    assert {:ok, _} = SourceStore.put(instance, "acme", "billing", @spec_map, :create)
    assert {:ok, _} = SourceStore.put(instance, "acme", "other", @log_spec, :create)
    assert SourceStore.delete(instance, "acme", "billing") == :ok

    restart_store(config, pid)

    assert SourceStore.get(instance, "acme", "billing") == :error
    assert SourceStore.fetch(instance, "acme.billing") == :error
    assert Enum.map(SourceStore.list_tenant(instance, "acme"), & &1.name) == ["other"]
  end

  test "deleting a name that is not there is :not_found" do
    {config, _pid} = start_store()
    instance = config.instance

    assert SourceStore.delete(instance, "acme", "nope") == {:error, :not_found}

    assert {:ok, _} = SourceStore.put(instance, "acme", "billing", @spec_map, :create)
    assert SourceStore.delete(instance, "beta", "billing") == {:error, :not_found}
    assert {:ok, _} = SourceStore.get(instance, "acme", "billing")
  end

  test "a seeded source is never deletable" do
    {config, _pid} =
      start_store(
        sources: %{"acme.billing" => [tenant_id: "acme", sinks: [{Ankusa.Sink.Log, []}]]}
      )

    instance = config.instance

    assert {:error, :invalid, message} = SourceStore.delete(instance, "acme", "billing")
    assert message =~ "read-only"
    assert {:ok, %Source{id: "acme.billing"}} = SourceStore.fetch(instance, "acme.billing")
  end

  test "a bad tenant or name on delete is {:error, :invalid, message}" do
    {config, _pid} = start_store()
    instance = config.instance

    assert {:error, :invalid, tenant_msg} =
             SourceStore.delete(instance, "bad tenant", "billing")

    assert tenant_msg =~ "tenant"

    assert {:error, :invalid, name_msg} = SourceStore.delete(instance, "acme", "bad/name")
    assert name_msg =~ "name"

    assert {:error, :invalid, long_msg} =
             SourceStore.delete(instance, "acme", String.duplicate("a", 65))

    assert long_msg =~ "name"
  end

  # ── persistence across a restart ────────────────────────────────────────────

  test "sources reload across a store restart" do
    {config, pid} = start_store()
    instance = config.instance

    assert {:ok, stored} = SourceStore.put(instance, "acme", "billing", @spec_map, :create)

    restart_store(config, pid)

    assert SourceStore.get(instance, "acme", "billing") == {:ok, stored}
    assert SourceStore.list_tenant(instance, "acme") == [stored]

    assert {:ok, %Source{tenant_id: "acme", sinks: [{Ankusa.Sink.Log, []}]}} =
             SourceStore.fetch(instance, "acme.billing")
  end

  test "an entry that no longer decodes is skipped at boot, stays stored, and returns once it decodes" do
    {config, pid} = start_store()
    instance = config.instance
    assert {:ok, stored} = SourceStore.put(instance, "acme", "billing", @spec_map, :create)

    # A decoder that rejects the stored spec simulates schema drift. Same
    # data_dir and instance, so it reads what the first store wrote.
    pid2 = restart_store(drifted_config(config), pid)

    assert SourceStore.get(instance, "acme", "billing") == :error
    assert SourceStore.list_tenant(instance, "acme") == []

    # Later writes neither drop the skipped row nor trip over it...
    assert {:ok, _} = SourceStore.put(instance, "acme", "other", @log_spec, :create)
    assert {:ok, _} = Store.get(instance, :default, Keys.source("acme", "billing"))

    # ...and rolling the decoder back brings it straight back.
    restart_store(config, pid2)
    assert SourceStore.get(instance, "acme", "billing") == {:ok, stored}
  end

  test "a stored value that is not a JSON object is skipped at boot, not fatal" do
    config = test_config(source_store: {Persistent, [decoder: &decoder/2]})
    put_config(config)
    instance = config.instance
    start_supervised!({Store, instance: instance, config: config})

    :ok =
      Store.write(
        instance,
        [
          {:put, :default, Keys.source("acme", "garbage"), "{ this is not json"},
          {:put, :default, Keys.source("acme", "list"), "[1, 2]"}
        ],
        sync: true
      )

    {:ok, pid} = Persistent.start_link(config)
    on_exit(fn -> stop(pid) end)

    assert SourceStore.list_tenant(instance, "acme") == []
    assert {:ok, _} = SourceStore.put(instance, "acme", "billing", @log_spec, :create)
    assert Enum.map(SourceStore.list_tenant(instance, "acme"), & &1.name) == ["billing"]

    # Skipped, not deleted: an operator can still find and repair them.
    assert Store.get(instance, :default, Keys.source("acme", "garbage")) ==
             {:ok, "{ this is not json"}

    assert Store.get(instance, :default, Keys.source("acme", "list")) == {:ok, "[1, 2]"}
  end

  @tag :capture_log
  test "boot refuses to start when the store cannot be read" do
    config = test_config(source_store: {Persistent, [decoder: &decoder/2]})
    put_config(config)
    Process.flag(:trap_exit, true)

    assert {:error, {:source_store_load_failed, :store_unavailable}} =
             Persistent.start_link(config)
  end

  test "a persisted entry that collides with a seed never shadows it" do
    seed = %{"acme.billing" => [tenant_id: "acme", sinks: [{Ankusa.Sink.Log, []}]]}
    config = test_config(source_store: {Persistent, [decoder: &decoder/2, sources: seed]})
    put_config(config)
    instance = config.instance

    start_supervised!({Store, instance: instance, config: config})

    :ok =
      Store.write(
        instance,
        [{:put, :default, Keys.source("acme", "billing"), JSON.encode!(@log_spec)}],
        sync: true
      )

    {:ok, pid} = Persistent.start_link(config)
    on_exit(fn -> stop(pid) end)

    # The seed still wins: unlisted by tenant, read-only, and not deletable.
    assert SourceStore.get(instance, "acme", "billing") == :error
    assert SourceStore.list_tenant(instance, "acme") == []
    assert {:ok, %Source{id: "acme.billing"}} = SourceStore.fetch(instance, "acme.billing")
    assert {:error, :invalid, message} = SourceStore.delete(instance, "acme", "billing")
    assert message =~ "read-only"

    # The colliding row is left in the store untouched.
    assert {:ok, _} = SourceStore.put(instance, "acme", "other", @log_spec, :create)
    assert {:ok, _} = Store.get(instance, :default, Keys.source("acme", "billing"))
  end

  # ── seeded (YAML) sources ───────────────────────────────────────────────────

  test "seeds are readable and listed on list/1 but are read-only and unlisted by tenant" do
    {config, _pid} =
      start_store(
        sources: %{"acme.billing" => [tenant_id: "acme", sinks: [{Ankusa.Sink.Log, []}]]}
      )

    instance = config.instance

    assert {:ok, %Source{id: "acme.billing"}} = SourceStore.fetch(instance, "acme.billing")
    assert SourceStore.list(instance) == ["acme.billing"]

    # A seed is not an API-managed source: it never appears under its tenant...
    assert SourceStore.list_tenant(instance, "acme") == []
    assert SourceStore.get(instance, "acme", "billing") == :error

    # ...and cannot be created or overwritten through the store.
    assert {:error, :invalid, message} =
             SourceStore.put(instance, "acme", "billing", @spec_map, :create)

    assert message =~ "read-only"

    assert {:error, :invalid, _} =
             SourceStore.put(instance, "acme", "billing", @log_spec, :update)
  end

  # ── validation ──────────────────────────────────────────────────────────────

  test "a bad tenant, name, or spec is {:error, :invalid, message}" do
    {config, _pid} = start_store()
    instance = config.instance

    assert {:error, :invalid, tenant_msg} =
             SourceStore.put(instance, "bad tenant", "billing", @spec_map, :create)

    assert tenant_msg =~ "tenant"

    assert {:error, :invalid, name_msg} =
             SourceStore.put(instance, "acme", "bad/name", @spec_map, :create)

    assert name_msg =~ "name"

    long = String.duplicate("a", 65)

    assert {:error, :invalid, long_msg} =
             SourceStore.put(instance, "acme", long, @spec_map, :create)

    assert long_msg =~ "name"

    # The decoder's rejection of a bad spec is the same shape.
    assert {:error, :invalid, spec_msg} =
             SourceStore.put(instance, "acme", "billing", %{"sinks" => []}, :create)

    assert spec_msg =~ "sinks"

    assert {:error, :invalid, _} = SourceStore.put(instance, "acme", "billing", %{}, :create)
    assert {:error, :invalid, _} = SourceStore.put(instance, "acme", "billing", "nope", :create)

    # Nothing was created by any of the failures.
    assert SourceStore.list_tenant(instance, "acme") == []
  end

  # ── helpers ─────────────────────────────────────────────────────────────────

  defp start_store(overrides \\ []) do
    store_opts =
      Keyword.merge([decoder: &decoder/2], Keyword.take(overrides, [:sources, :decoder]))

    overrides = Keyword.drop(overrides, [:sources, :decoder])

    config =
      test_config(Keyword.merge([source_store: {Persistent, store_opts}], overrides))

    put_config(config)
    # The sources live in the node's store, which must be up first.
    start_supervised!({Store, instance: config.instance, config: config})
    {:ok, pid} = Persistent.start_link(config)
    on_exit(fn -> stop(pid) end)
    {config, pid}
  end

  # A full node restart as far as sources go: the source store and the RocksDB
  # store both stop, then both start again on the same data dir. `config` may
  # carry a different decoder, to simulate a config change across the restart.
  defp restart_store(config, pid) do
    stop(pid)
    stop_supervised!({Store, config.instance})
    put_config(config)
    start_supervised!({Store, instance: config.instance, config: config})
    {:ok, pid2} = Persistent.start_link(config)
    on_exit(fn -> stop(pid2) end)
    pid2
  end

  defp drifted_config(config) do
    Ankusa.Config.new(
      instance: config.instance,
      data_dir: config.data_dir,
      port: 0,
      source_store: {Persistent, [decoder: &drifted_decoder/2]}
    )
  end

  defp stop(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  catch
    :exit, _ -> :ok
  end

  # Stands in for `AnkusaServer.Config.source_from_map!/2`: the store only cares
  # that the decoder returns source options or raises.
  defp decoder(_source_id, spec) do
    case Map.get(spec, "sinks") do
      [_ | _] = sinks -> [sinks: Enum.map(sinks, &sink/1)]
      _ -> raise ArgumentError, "sinks must be a non-empty list"
    end
  end

  # A decoder that simulates schema drift: it rejects every spec carrying a
  # `verify` block, but still accepts the plain log specs the tests write.
  defp drifted_decoder(_source_id, %{"verify" => _}), do: raise(ArgumentError, "schema drift")
  defp drifted_decoder(source_id, spec), do: decoder(source_id, spec)

  defp sink(%{"type" => "log"}), do: {Ankusa.Sink.Log, []}
  defp sink(other), do: raise(ArgumentError, "unknown sink: #{inspect(other)}")
end
