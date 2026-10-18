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
  alias Ankusa.ClaimCheck
  alias Ankusa.Envelope
  alias Ankusa.Storage.Compactor
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

  # SEC-4. Every object this node writes goes through the adapter's `put/4` with
  # the options of the store it is configured on, so one `sse:` on the store is
  # on every PUT: the segments, the backup, and the claim pack alike. The
  # requests are recorded on their way to the emulator (a `Req` plug that
  # forwards), because the emulator itself does not echo what it was sent.
  test "sse header on put: segment, backup and claim PUTs all carry it" do
    {:ok, seen} = Agent.start_link(fn -> [] end)
    prefix = "it-sse-#{System.unique_integer([:positive])}-#{System.os_time(:millisecond)}/"
    opts = [sse: %{type: :aes256}, req_options: [plug: recording_forwarder(seen)]] ++ @opts

    config =
      test_config(
        roles: [:edge, :storage],
        source_store:
          {Ankusa.SourceStore.Static, sources: %{"acme" => %{sinks: [{Ankusa.Sink.Log, []}]}}},
        storage: %{interval_ms: 0, blob_store: {S3, opts}, key_prefix: prefix},
        backup: %{enabled: true, interval_ms: 3_600_000}
      )

    inst = config.instance
    start_supervised!({Ankusa.Instance, config})

    for n <- 1..20, do: enqueue!(inst, envelope(n))
    assert {:ok, segments} = Compactor.tick(inst)
    assert segments > 0
    assert {:ok, %{uploaded: uploaded}} = Backup.run(inst)
    assert uploaded > 0

    item = %{id: Ankusa.UUIDv7.generate(), tenant_id: "acme", body: "a claimed body"}
    assert {:ok, _claims} = ClaimCheck.check_in(inst, "acme", [item])

    puts =
      seen |> Agent.get(& &1) |> Enum.filter(fn {method, _path, _headers} -> method == "PUT" end)

    for {kind, marker} <- [
          segment: "/" <> prefix <> "seg/",
          backup: "/" <> prefix <> "backup/",
          claim: "/claims/"
        ] do
      of_kind =
        Enum.filter(puts, fn {_method, path, _headers} -> String.contains?(path, marker) end)

      assert of_kind != [], "no #{kind} PUT was recorded"
    end

    for {_method, path, headers} <- puts do
      assert headers["x-amz-server-side-encryption"] == "AES256", "PUT #{path} has no SSE header"

      assert headers["authorization"] =~ "x-amz-server-side-encryption",
             "#{path}: header not signed"
    end

    # and the emulator took the objects: what was written reads back
    assert {:ok, keys} = S3.list(inst, prefix <> "backup/", @opts)
    assert (prefix <> "backup/LATEST") in keys
  end

  # Records every request, then replays it against the emulator.
  defp recording_forwarder(seen) do
    fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn, length: 100_000_000)

      Agent.update(seen, &[{conn.method, conn.request_path, Map.new(conn.req_headers)} | &1])

      query = if conn.query_string == "", do: "", else: "?" <> conn.query_string
      url = "http://localhost:4566" <> conn.request_path <> query

      headers =
        for {name, value} <- conn.req_headers,
            name not in ["host", "content-length"],
            do: {name, value}

      method = conn.method |> String.downcase() |> String.to_existing_atom()

      {:ok, status, reply} =
        Ankusa.HttpClient.request(
          method,
          url,
          headers,
          if(body == "", do: nil, else: body),
          10_000
        )

      Plug.Conn.send_resp(conn, status, reply)
    end
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
