defmodule Ankusa.TenantS3IntegrationTest do
  @moduledoc """
  A tenant erase deletes the tenant's `claims/tenant=<t>/` prefix from an object
  store, and only that prefix. Integration test against the floci S3 emulator;
  requires `docker compose -f docker-compose.integration.yml up -d floci s3-bootstrap`
  and `mix test --include integration` (`mise run test:integration`).
  """

  use ExUnit.Case, async: false
  @moduletag :integration

  import Ankusa.TestHelpers

  alias Ankusa.BlobStore.S3
  alias Ankusa.Tenant

  @opts [
    bucket: "ankusa-segments-dev",
    region: "us-east-1",
    endpoint: "http://localhost:4566",
    access_key_id: "test",
    secret_access_key: "test"
  ]

  @doc false
  def decoder(_source_id, _spec), do: [sinks: []]

  test "erase leaves no claim object of the tenant on S3, and every other tenant's stays" do
    suffix = System.unique_integer([:positive])
    erased = "erase-s3-#{suffix}"
    kept = "keep-s3-#{suffix}"

    config =
      test_config(
        roles: [:edge, :dispatch],
        source_store: {Ankusa.SourceStore.Persistent, decoder: &__MODULE__.decoder/2},
        claim_check: %{blob_stores: [{S3, @opts}]}
      )

    put_config(config)
    start_supervised!({Ankusa.Instance, config})
    inst = config.instance

    key = fn tenant, pack -> "claims/tenant=#{tenant}/dt=2026-10-19/#{pack}" end

    for tenant <- [erased, kept], pack <- ["pack-1", "pack-2"] do
      assert :ok = S3.put(inst, key.(tenant, pack), "claim bytes", @opts)
    end

    assert {:ok, [_, _]} = S3.list(inst, "claims/tenant=#{erased}/", @opts)

    assert {:ok, :started} = Tenant.delete(inst, erased, confirm: erased)

    eventually(fn -> match?({:ok, %{state: :erased}}, Tenant.status(inst, erased)) end)

    assert {:ok, []} = S3.list(inst, "claims/tenant=#{erased}/", @opts)
    assert {:error, :not_found} = S3.get(inst, key.(erased, "pack-1"), @opts)

    assert {:ok, [_, _]} = S3.list(inst, "claims/tenant=#{kept}/", @opts)
    assert {:ok, "claim bytes"} = S3.get(inst, key.(kept, "pack-1"), @opts)

    for pack <- ["pack-1", "pack-2"], do: S3.delete(inst, key.(kept, pack), @opts)
  end

  defp eventually(fun, deadline \\ System.monotonic_time(:millisecond) + 30_000) do
    cond do
      fun.() -> :ok
      System.monotonic_time(:millisecond) > deadline -> flunk("the erase did not finish")
      true -> Process.sleep(50) && eventually(fun, deadline)
    end
  end
end
