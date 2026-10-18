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
    start_supervised!({Store, instance: instance})

    :ok =
      Store.write(
        instance,
        [
          {:put, :default, Keys.source("acme", "garbage"), "{ this is not json"},
          {:put, :default, Keys.source("acme", "list"), "[1, 2]"}
        ],
        sync: true
      )

    {:ok, pid} = Persistent.start_link(instance: instance)
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
             Persistent.start_link(instance: config.instance)
  end

  test "a persisted entry that collides with a seed never shadows it" do
    seed = %{"acme.billing" => [tenant_id: "acme", sinks: [{Ankusa.Sink.Log, []}]]}
    config = test_config(source_store: {Persistent, [decoder: &decoder/2, sources: seed]})
    put_config(config)
    instance = config.instance

    start_supervised!({Store, instance: instance})

    :ok =
      Store.write(
        instance,
        [{:put, :default, Keys.source("acme", "billing"), JSON.encode!(@log_spec)}],
        sync: true
      )

    {:ok, pid} = Persistent.start_link(instance: instance)
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

  # ── shared sources, when the request names the tenant ───────────────────────

  test "an unverified shared source is refused unless it trusts the request's tenant" do
    {config, _pid} = start_store(route_resolver: {Ankusa.RouteResolver.TenantPath, []})
    inst = config.instance

    assert {:error, :invalid, message} =
             SourceStore.put(inst, "default", "open", @log_spec, :create)

    assert message =~ "source default.open is shared"
    assert SourceStore.get(inst, "default", "open") == :error

    trusted = Map.put(@log_spec, "trust_url_tenant", true)
    assert {:ok, _} = SourceStore.put(inst, "default", "open", trusted, :create)
    assert {:ok, %Source{trust_url_tenant: true}} = SourceStore.fetch(inst, "default.open")

    # A source with a tenant of its own is bound to it: nothing to trust.
    assert {:ok, _} = SourceStore.put(inst, "acme", "own", @log_spec, :create)
  end

  test "a stored unverified shared source keeps serving, with a warning at boot" do
    config =
      test_config(
        route_resolver: {Ankusa.RouteResolver.TenantPath, []},
        source_store: {Persistent, [decoder: &decoder/2]}
      )

    put_config(config)
    inst = config.instance
    start_supervised!({Store, instance: inst})

    :ok =
      Store.write(
        inst,
        [{:put, :default, Keys.source("default", "legacy"), JSON.encode!(@log_spec)}],
        sync: true
      )

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        {:ok, pid} = Persistent.start_link(instance: inst)
        on_exit(fn -> stop(pid) end)
      end)

    assert log =~ "stored source default.legacy is shared"
    assert {:ok, %Source{trust_url_tenant: false}} = SourceStore.fetch(inst, "default.legacy")
  end

  # ── durability under wal: :none ─────────────────────────────────────────────

  test "under wal: :none, a source with no durable sink is refused when written" do
    {config, _pid} = start_store(wal: :none)
    inst = config.instance

    assert {:error, :invalid, message} =
             SourceStore.put(inst, "acme", "logonly", @log_spec, :create)

    assert message =~ ~s(source "acme.logonly")
    assert message =~ "none of its sinks is durable"
    assert SourceStore.get(inst, "acme", "logonly") == :error

    hooked = %{"sinks" => [%{"type" => "log"}, %{"type" => "http", "url" => "http://sink.test"}]}
    assert {:ok, _} = SourceStore.put(inst, "acme", "hooked", hooked, :create)

    # An update that drops the durable sink is refused too, and changes nothing.
    assert {:error, :invalid, _} = SourceStore.put(inst, "acme", "hooked", @log_spec, :update)
    assert {:ok, %{spec: ^hooked}} = SourceStore.get(inst, "acme", "hooked")
  end

  # ── secrets at rest ─────────────────────────────────────────────────────────

  describe "secrets at rest" do
    @kek_a Base.encode64(:binary.copy(<<1>>, 32))
    @kek_b Base.encode64(:binary.copy(<<2>>, 32))

    @secret_spec %{
      "verify" => %{"type" => "hmac", "secret" => "whsec-canary", "signature_header" => "X-Sig"},
      "sinks" => [
        %{
          "type" => "http",
          "url" => "https://relay:url-pass-canary@sink.example/hook",
          "headers" => %{"authorization" => "Bearer header-canary"}
        }
      ]
    }

    @canaries ~w(whsec-canary url-pass-canary header-canary)

    test "no plaintext at rest" do
      {config, pid} = start_store(secrets: [kek: "k1:" <> @kek_a])
      instance = config.instance

      assert {:ok, stored} = SourceStore.put(instance, "acme", "billing", @secret_spec, :create)

      # the row itself, read raw through the store
      raw = raw_row(instance, "acme", "billing")
      for canary <- @canaries, do: refute(raw =~ canary, "#{canary} is in the stored row")

      # identity and structure stay readable
      assert raw =~ ~s("type":"hmac")
      assert raw =~ "X-Sig"

      # the callers still see the plain spec: ETS, the facade, the admin API's merge
      assert stored.spec == @secret_spec
      assert {:ok, %{spec: @secret_spec}} = SourceStore.get(instance, "acme", "billing")

      # and no file the node wrote holds one either (the WAL has the synced write)
      for canary <- @canaries,
          do: refute(files_contain?(config.data_dir, canary), "#{canary} is in a store file")

      # a restart decrypts
      restart_store(config, pid)
      assert {:ok, %{spec: @secret_spec}} = SourceStore.get(instance, "acme", "billing")
      assert {:ok, %Source{id: "acme.billing"}} = SourceStore.fetch(instance, "acme.billing")
    end

    test "without a KEK the row is plain JSON, as it always was (and the scan can tell)" do
      {config, _pid} = start_store()
      assert {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @secret_spec, :create)

      assert raw_row(config.instance, "acme", "billing") =~ "whsec-canary"
      assert files_contain?(config.data_dir, "whsec-canary")
    end

    test "an update that omits the verify secret keeps the stored one, across a restart" do
      {config, pid} = start_store(secrets: [kek: @kek_a])
      instance = config.instance
      assert {:ok, _} = SourceStore.put(instance, "acme", "billing", @secret_spec, :create)

      edit = %{
        "verify" => %{"type" => "hmac", "signature_header" => "X-New"},
        "sinks" => @secret_spec["sinks"]
      }

      assert {:ok, updated} = SourceStore.put(instance, "acme", "billing", edit, :update)
      assert updated.spec["verify"]["secret"] == "whsec-canary"

      refute raw_row(instance, "acme", "billing") =~ "whsec-canary"

      restart_store(config, pid)

      assert {:ok,
              %{spec: %{"verify" => %{"secret" => "whsec-canary", "signature_header" => "X-New"}}}} =
               SourceStore.get(instance, "acme", "billing")
    end

    test "rows written before a KEK existed load, and rewrap encrypts them" do
      {plain, pid} = start_store()
      instance = plain.instance
      assert {:ok, _} = SourceStore.put(instance, "acme", "billing", @secret_spec, :create)
      assert {:ok, _} = SourceStore.put(instance, "acme", "plain", @log_spec, :create)

      keyed = config_like(plain, kek: "k1:" <> @kek_a)
      restart_store(keyed, pid)
      assert {:ok, %{spec: @secret_spec}} = SourceStore.get(instance, "acme", "billing")
      assert raw_row(instance, "acme", "billing") =~ "whsec-canary"

      assert Persistent.rewrap(instance) == {:ok, %{rewrapped: 1, current: 1}}
      refute raw_row(instance, "acme", "billing") =~ "whsec-canary"
      assert {:ok, %{spec: @secret_spec}} = SourceStore.get(instance, "acme", "billing")

      # nothing left to do, and the row with nothing to hide was never touched
      assert Persistent.rewrap(instance) == {:ok, %{rewrapped: 0, current: 2}}
      assert raw_row(instance, "acme", "plain") == JSON.encode!(@log_spec)
    end

    @tag :capture_log
    test "boot refuses an encrypted row when no KEK is configured" do
      {config, pid} = start_store(secrets: [kek: "k1:" <> @kek_a])
      assert {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @secret_spec, :create)
      assert {:ok, _} = SourceStore.put(config.instance, "acme", "plain", @log_spec, :create)

      assert {:error, {:source_store_load_failed, {:secrets, "acme.billing", :no_kek}}} =
               boot_refused(config_like(config, []), pid)
    end

    @tag :capture_log
    test "boot refuses a row encrypted under a key it was not given" do
      {config, pid} = start_store(secrets: [kek: "k1:" <> @kek_a])
      assert {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @secret_spec, :create)

      assert {:error,
              {:source_store_load_failed, {:secrets, "acme.billing", {:unknown_key, "k1"}}}} =
               boot_refused(config_like(config, kek: "k2:" <> @kek_b), pid)
    end

    @tag :capture_log
    test "boot refuses a row whose ciphertext was altered" do
      {config, pid} = start_store(secrets: [kek: "k1:" <> @kek_a])
      instance = config.instance
      assert {:ok, _} = SourceStore.put(instance, "acme", "billing", @secret_spec, :create)

      altered =
        instance
        |> raw_row("acme", "billing")
        |> JSON.decode!()
        |> update_in(["verify", "secret"], fn "AENC:" <> encoded ->
          bytes = Base.decode64!(encoded)
          size = byte_size(bytes) - 1
          <<head::binary-size(^size), last>> = bytes
          "AENC:" <> Base.encode64(<<head::binary, Bitwise.bxor(last, 1)>>)
        end)

      :ok =
        Store.write(
          instance,
          [{:put, :default, Keys.source("acme", "billing"), JSON.encode!(altered)}],
          sync: true
        )

      assert {:error, {:source_store_load_failed, {:secrets, "acme.billing", :tampered}}} =
               boot_refused(config, pid)
    end

    @tag :capture_log
    test "a KEK provider that fails refuses the boot instead of storing plaintext" do
      config =
        test_config(
          source_store: {Persistent, [decoder: &decoder/2]},
          secrets: [kek_provider: {__MODULE__, :provider, []}]
        )

      put_config(config)
      start_supervised!({Store, instance: config.instance})
      Process.flag(:trap_exit, true)

      assert {:error, {:source_store_load_failed, {:secrets, {:kek_provider, :kms_down}}}} =
               Persistent.start_link(instance: config.instance)
    end

    test "rewrap moves rows to the current key, and the old key can then be dropped" do
      {config, pid} = start_store(secrets: [kek: "k1:" <> @kek_a])
      instance = config.instance
      assert {:ok, _} = SourceStore.put(instance, "acme", "billing", @secret_spec, :create)
      assert {:ok, _} = SourceStore.put(instance, "acme", "plain", @log_spec, :create)

      # roll out k2 with k1 still readable: every row loads
      rotated = config_like(config, kek: "k2:" <> @kek_b, kek_previous: ["k1:" <> @kek_a])
      pid = restart_store(rotated, pid)
      assert {:ok, %{spec: @secret_spec}} = SourceStore.get(instance, "acme", "billing")
      assert raw_row(instance, "acme", "billing") =~ "AENC:"

      # a dry run counts and writes nothing
      before = raw_row(instance, "acme", "billing")
      {:ok, ring} = Ankusa.Crypto.Envelope.keyring(rotated.secrets)

      assert Persistent.rewrap_rows(instance, ring, dry_run: true) ==
               {:ok, %{rewrapped: 1, current: 1}}

      assert raw_row(instance, "acme", "billing") == before

      assert Persistent.rewrap(instance) == {:ok, %{rewrapped: 1, current: 1}}
      refute raw_row(instance, "acme", "billing") == before

      # k1 can go: the same rows load under k2 alone
      pid = restart_store(config_like(config, kek: "k2:" <> @kek_b), pid)
      assert {:ok, %{spec: @secret_spec}} = SourceStore.get(instance, "acme", "billing")

      # and k1 alone no longer opens them
      assert {:error,
              {:source_store_load_failed, {:secrets, "acme.billing", {:unknown_key, "k2"}}}} =
               boot_refused(config_like(config, kek: "k1:" <> @kek_a), pid)
    end

    test "rewrap_rows works in batches, over any number of rows" do
      config = test_config()
      put_config(config)
      instance = config.instance
      start_supervised!({Store, instance: instance})

      count = 700

      ops =
        for n <- 1..count do
          spec = %{
            "verify" => %{"type" => "hmac", "secret" => "bulk-canary-#{n}"},
            "sinks" => [%{"type" => "log"}]
          }

          {:put, :default, Keys.source("bulk", "s#{n}"), JSON.encode!(spec)}
        end

      :ok = Store.write(instance, ops, sync: true)

      {:ok, ring} =
        Ankusa.Crypto.Envelope.keyring(%{kek: @kek_a, kek_previous: [], kek_provider: nil})

      assert Persistent.rewrap_rows(instance, ring) == {:ok, %{rewrapped: count, current: 0}}
      assert Persistent.rewrap_rows(instance, ring) == {:ok, %{rewrapped: 0, current: count}}

      for n <- [1, 256, 257, 512, 700] do
        refute raw_row(instance, "bulk", "s#{n}") =~ "bulk-canary"
      end
    end

    test "rewrap_rows refuses without a KEK, and on a row it cannot read, writing nothing" do
      config = test_config()
      put_config(config)
      instance = config.instance
      start_supervised!({Store, instance: instance})

      :ok =
        Store.write(
          instance,
          [{:put, :default, Keys.source("acme", "billing"), JSON.encode!(@secret_spec)}],
          sync: true
        )

      {:ok, keyless} =
        Ankusa.Crypto.Envelope.keyring(%{kek: nil, kek_previous: [], kek_provider: nil})

      assert Persistent.rewrap_rows(instance, keyless) ==
               {:error, {:secrets, "acme.billing", :no_kek}}

      assert raw_row(instance, "acme", "billing") =~ "whsec-canary"
    end

    test "the startup log says how secrets rest" do
      config =
        test_config(
          source_store: {Persistent, [decoder: &decoder/2]},
          secrets: [kek: "k1:" <> @kek_a]
        )

      put_config(config)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          start_supervised!({Store, instance: config.instance})
        end)

      assert log =~
               "store files rely on volume encryption; secrets envelope-encrypted with key k1"

      plain = test_config(source_store: {Persistent, [decoder: &decoder/2]})
      put_config(plain)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          start_supervised!({Store, instance: plain.instance}, id: :plain)
        end)

      assert log =~ "store files rely on volume encryption; secrets are stored in plaintext"
    end
  end

  # Called through `secrets.kek_provider`.
  def provider, do: {:error, :kms_down}

  # ── helpers ─────────────────────────────────────────────────────────────────

  defp start_store(overrides \\ []) do
    store_opts =
      Keyword.merge([decoder: &decoder/2], Keyword.take(overrides, [:sources, :decoder]))

    overrides = Keyword.drop(overrides, [:sources, :decoder])

    config =
      test_config(Keyword.merge([source_store: {Persistent, store_opts}], overrides))

    put_config(config)
    # The sources live in the node's store, which must be up first.
    start_supervised!({Store, instance: config.instance})
    {:ok, pid} = Persistent.start_link(instance: config.instance)
    on_exit(fn -> stop(pid) end)
    {config, pid}
  end

  defp raw_row(instance, tenant, name) do
    assert {:ok, raw} = Store.get(instance, :default, Keys.source(tenant, name))
    raw
  end

  # Every file under `dir` (the RocksDB files and their WAL included), not only
  # what a `get` returns.
  defp files_contain?(dir, needle) do
    dir
    |> Path.join("**")
    |> Path.wildcard(match_dot: true)
    |> Enum.filter(&File.regular?/1)
    |> Enum.any?(&String.contains?(File.read!(&1), needle))
  end

  # The same node (instance, data dir, decoder) with other `secrets`.
  defp config_like(config, secrets) do
    Ankusa.Config.new(
      instance: config.instance,
      data_dir: config.data_dir,
      port: 0,
      source_store: {Persistent, [decoder: &decoder/2]},
      secrets: secrets
    )
  end

  # Stop the node's source store and store, start the store again under
  # `config`, and return what starting the source store answers.
  defp boot_refused(config, pid) do
    stop(pid)
    stop_supervised!({Store, config.instance})
    put_config(config)
    start_supervised!({Store, instance: config.instance})
    Process.flag(:trap_exit, true)
    Persistent.start_link(instance: config.instance)
  end

  # A full node restart as far as sources go: the source store and the RocksDB
  # store both stop, then both start again on the same data dir. `config` may
  # carry a different decoder, to simulate a config change across the restart.
  defp restart_store(config, pid) do
    stop(pid)
    stop_supervised!({Store, config.instance})
    put_config(config)
    start_supervised!({Store, instance: config.instance})
    {:ok, pid2} = Persistent.start_link(instance: config.instance)
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
      [_ | _] = sinks ->
        [
          sinks: Enum.map(sinks, &sink/1),
          trust_url_tenant: Map.get(spec, "trust_url_tenant", false)
        ]

      _ ->
        raise ArgumentError, "sinks must be a non-empty list"
    end
  end

  # A decoder that simulates schema drift: it rejects every spec carrying a
  # `verify` block, but still accepts the plain log specs the tests write.
  defp drifted_decoder(_source_id, %{"verify" => _}), do: raise(ArgumentError, "schema drift")
  defp drifted_decoder(source_id, spec), do: decoder(source_id, spec)

  defp sink(%{"type" => "log"}), do: {Ankusa.Sink.Log, []}
  defp sink(%{"type" => "http", "url" => url}), do: {Ankusa.Sink.Http, [url: url]}
  defp sink(other), do: raise(ArgumentError, "unknown sink: #{inspect(other)}")
end
