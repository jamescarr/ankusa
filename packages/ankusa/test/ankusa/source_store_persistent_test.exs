defmodule Ankusa.SourceStorePersistentTest do
  @moduledoc """
  The writable store and its facade. The store owns the tenant-scoped sources
  the admin API manages; the facade is what every caller uses, so most of this
  suite drives `Ankusa.SourceStore` rather than the store module directly.
  """

  use ExUnit.Case, async: true

  import Ankusa.TestHelpers

  alias Ankusa.{Source, SourceStore}
  alias Ankusa.SourceStore.Persistent

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

    GenServer.stop(pid)

    {:ok, pid2} = Persistent.start_link(config)
    on_exit(fn -> stop(pid2) end)

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
      start_store(sources: %{"acme.billing" => [tenant_id: "acme", sinks: [{Ankusa.Sink.Log, []}]]})

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
    assert File.exists?(Ankusa.Config.path(config, "sources.json"))

    GenServer.stop(pid)

    {:ok, pid2} = Persistent.start_link(config)
    on_exit(fn -> stop(pid2) end)

    assert SourceStore.get(instance, "acme", "billing") == {:ok, stored}
    assert SourceStore.list_tenant(instance, "acme") == [stored]

    assert {:ok, %Source{tenant_id: "acme", sinks: [{Ankusa.Sink.Log, []}]}} =
             SourceStore.fetch(instance, "acme.billing")
  end

  test "an entry that no longer decodes is skipped at boot, not fatal" do
    {config, pid} = start_store()
    instance = config.instance
    assert {:ok, _} = SourceStore.put(instance, "acme", "billing", @spec_map, :create)
    GenServer.stop(pid)

    # A decoder that rejects the stored spec simulates schema drift. Same
    # data_dir and instance, so it reads the file the first store wrote.
    clashing =
      Ankusa.Config.new(
        instance: instance,
        data_dir: config.data_dir,
        port: 0,
        source_store: {Persistent, [decoder: fn _, _ -> raise "nope" end]}
      )

    put_config(clashing)
    {:ok, pid2} = Persistent.start_link(clashing)
    on_exit(fn -> stop(pid2) end)

    assert SourceStore.get(instance, "acme", "billing") == :error
    assert SourceStore.list_tenant(instance, "acme") == []
  end

  # ── seeded (YAML) sources ───────────────────────────────────────────────────

  test "seeds are readable and listed on list/1 but are read-only and unlisted by tenant" do
    {config, _pid} =
      start_store(sources: %{"acme.billing" => [tenant_id: "acme", sinks: [{Ankusa.Sink.Log, []}]]})

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
    assert {:error, :invalid, _} = SourceStore.put(instance, "acme", "billing", @log_spec, :update)
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
    store_opts = Keyword.merge([decoder: &decoder/2], Keyword.take(overrides, [:sources, :decoder]))
    overrides = Keyword.drop(overrides, [:sources, :decoder])

    config =
      test_config(
        Keyword.merge([source_store: {Persistent, store_opts}], overrides)
      )

    put_config(config)
    {:ok, pid} = Persistent.start_link(config)
    on_exit(fn -> stop(pid) end)
    {config, pid}
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

  defp sink(%{"type" => "log"}), do: {Ankusa.Sink.Log, []}
  defp sink(other), do: raise(ArgumentError, "unknown sink: #{inspect(other)}")
end
