defmodule Ankusa.Store.BackupS3IntegrationTest do
  @moduledoc """
  The host-loss drill through a real S3 API (the floci emulator). Requires
  `docker compose -f docker-compose.integration.yml up -d floci s3-bootstrap`;
  run with `mix test --include integration`.
  """

  use ExUnit.Case, async: false
  @moduletag :integration

  import Ankusa.TestHelpers

  alias Ankusa.BlobStore.S3
  alias Ankusa.Envelope
  alias Ankusa.Store.Backup

  @opts [
    bucket: "ankusa-segments-dev",
    region: "us-east-1",
    endpoint: "http://localhost:4566",
    access_key_id: "test",
    secret_access_key: "test"
  ]

  test "a backup in S3 brings every hook back after the data dir is lost" do
    prefix = "it-backup-#{System.unique_integer([:positive])}-#{System.os_time(:millisecond)}/"

    config =
      test_config(
        roles: [:edge],
        source_store:
          {Ankusa.SourceStore.Static, sources: %{"acme" => %{sinks: [{Ankusa.Sink.Log, []}]}}},
        storage: %{interval_ms: 0, blob_store: {S3, @opts}, key_prefix: prefix},
        backup: %{enabled: true, interval_ms: 3_600_000}
      )

    inst = config.instance
    start_supervised!({Ankusa.Instance, config})

    committed = for n <- 1..200, do: enqueue!(inst, envelope(n))
    assert {:ok, %{id: id, uploaded: uploaded}} = Backup.run(inst)
    assert uploaded > 0

    assert {:ok, keys} = S3.list(inst, prefix <> "backup/", @opts)
    assert (prefix <> "backup/LATEST") in keys
    assert (prefix <> "backup/#{id}/manifest.json") in keys
    assert Enum.any?(keys, &String.starts_with?(&1, prefix <> "backup/sst/"))

    stop_supervised!({Ankusa.Instance, inst})
    File.rm_rf!(config.data_dir)
    start_supervised!({Ankusa.Instance, config})

    assert stored_ids(inst) == Enum.map(committed, & &1.id)
  end

  defp envelope(n) do
    body = ~s({"n":#{n}})

    %Envelope{
      id: "evt_" <> Integer.to_string(System.unique_integer([:positive])),
      source_id: "acme",
      tenant_id: "default",
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/hooks/acme",
      headers: [{"content-type", "application/json"}],
      content_type: "application/json",
      body: body,
      size: byte_size(body)
    }
  end
end
