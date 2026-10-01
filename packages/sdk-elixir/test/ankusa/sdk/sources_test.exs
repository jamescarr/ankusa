defmodule Ankusa.SDK.SourcesTest do
  use ExUnit.Case, async: true

  alias Ankusa.SDK.{
    PlugTransport,
    SourceConflictError,
    SourceInvalidError,
    SourceNotFoundError,
    SourceStoreReadOnlyError,
    Sources,
    SourcesUnavailableError,
    VersionMismatchError
  }

  alias Ankusa.SDK.Sources.{Source, Spec}

  @base_url "http://admin.test"

  @entry %{
    "tenant" => "acme",
    "name" => "billing",
    "source_id" => "acme.billing",
    "ingest_path" => "/webhooks/acme.billing",
    "verify" => %{"type" => "hmac", "secret" => "[REDACTED]", "signature_header" => "X-Sig"},
    "on_verify_failure" => "reject",
    "sinks" => [%{"type" => "log"}]
  }

  @source_spec %Spec{
    sinks: [%{"type" => "log"}],
    verify: %{"type" => "hmac", "secret" => "s3cr3t", "signature_header" => "X-Sig"},
    on_verify_failure: "reject"
  }

  @invalid_ids ["..", "a/b", "a?b=1", "a#b", String.duplicate("a", 65), ""]

  defp client(responder, opts \\ []) do
    {plug, recorder} = PlugTransport.transport(responder)
    {Sources.new(@base_url, Keyword.merge([req_options: [plug: plug]], opts)), recorder}
  end

  defp health(conn, version \\ "0.3.0") do
    PlugTransport.json(conn, 200, %{"status" => "ok", "version" => version})
  end

  describe "Spec.to_json/1" do
    test "keeps every set field" do
      assert Spec.to_json(@source_spec) == %{
               "sinks" => [%{"type" => "log"}],
               "verify" => %{
                 "type" => "hmac",
                 "secret" => "s3cr3t",
                 "signature_header" => "X-Sig"
               },
               "on_verify_failure" => "reject"
             }
    end

    test "omits unset verify and failure mode" do
      assert Spec.to_json(%Spec{sinks: [%{"type" => "log"}]}) == %{
               "sinks" => [%{"type" => "log"}]
             }
    end

    test "omits unset fields, sinks included" do
      assert Spec.to_json(%Spec{}) == %{}
    end
  end

  describe "the version latch" do
    test "server_version reads the health version" do
      {client, recorder} = client(&health/1)

      assert {:ok, "0.3.0"} = Sources.server_version(client)
      assert [%{"method" => "GET", "path" => "/health"}] = PlugTransport.requests(recorder)
    end

    test "verify_version caches, so later calls make no health request" do
      {client, recorder} =
        client(fn conn ->
          if conn.request_path == "/health" do
            health(conn)
          else
            PlugTransport.json(conn, 200, %{"tenant" => "acme", "entries" => [@entry]})
          end
        end)

      assert {:ok, client} = Sources.verify_version(client)
      assert {:ok, "0.3.0"} = Sources.server_version(client)
      assert {:ok, [%Source{source_id: "acme.billing"}]} = Sources.list_sources(client, "acme")

      assert Enum.count(PlugTransport.requests(recorder), &(&1["path"] == "/health")) == 1
    end

    test "a mismatch fails before the API call" do
      {client, recorder} = client(&health/1, expected_version: "9.9.9")

      assert {:error, %VersionMismatchError{status: nil, body: nil} = error} =
               Sources.list_sources(client, "acme")

      assert Exception.message(error) =~ "9.9.9"
      assert Exception.message(error) =~ "0.3.0"
      assert [%{"path" => "/health"}] = PlugTransport.requests(recorder)
    end

    test "a matching version passes" do
      {client, recorder} =
        client(
          fn conn ->
            if conn.request_path == "/health" do
              health(conn)
            else
              PlugTransport.json(conn, 200, %{"tenant" => "acme", "entries" => [@entry]})
            end
          end,
          expected_version: "0.3.0"
        )

      assert {:ok, [%Source{name: "billing"}]} = Sources.list_sources(client, "acme")
      assert Enum.count(PlugTransport.requests(recorder), &(&1["path"] == "/health")) == 1
    end

    test "without expected_version no probe is made" do
      {client, recorder} =
        client(fn conn ->
          PlugTransport.json(conn, 200, %{"tenant" => "acme", "entries" => []})
        end)

      assert {:ok, []} = Sources.list_sources(client, "acme")

      assert [%{"method" => "GET", "path" => "/v1/tenants/acme/sources"}] =
               PlugTransport.requests(recorder)
    end
  end

  describe "list and get" do
    test "list_sources parses the entries" do
      {client, _recorder} =
        client(fn conn ->
          PlugTransport.json(conn, 200, %{"tenant" => "acme", "entries" => [@entry]})
        end)

      assert {:ok, [source]} = Sources.list_sources(client, "acme")

      assert source == %Source{
               tenant: "acme",
               name: "billing",
               source_id: "acme.billing",
               ingest_path: "/webhooks/acme.billing",
               verify: %{
                 "type" => "hmac",
                 "secret" => "[REDACTED]",
                 "signature_header" => "X-Sig"
               },
               on_verify_failure: "reject",
               sinks: [%{"type" => "log"}]
             }
    end

    test "a malformed entry is an unavailable error" do
      {client, _recorder} =
        client(fn conn ->
          PlugTransport.json(conn, 200, %{"entries" => [%{"name" => "billing"}]})
        end)

      assert {:error, %SourcesUnavailableError{message: "malformed source in response"}} =
               Sources.list_sources(client, "acme")
    end

    test "get_source parses one source" do
      {client, recorder} = client(fn conn -> PlugTransport.json(conn, 200, @entry) end)

      assert {:ok, %Source{ingest_path: "/webhooks/acme.billing"}} =
               Sources.get_source(client, "acme", "billing")

      assert [%{"path" => "/v1/tenants/acme/sources/billing"}] = PlugTransport.requests(recorder)
    end
  end

  describe "writes" do
    test "create_source posts the spec plus the name" do
      {client, recorder} = client(fn conn -> PlugTransport.json(conn, 201, @entry) end)

      assert {:ok, %Source{source_id: "acme.billing"}} =
               Sources.create_source(client, "acme", "billing", @source_spec)

      assert [request] = PlugTransport.requests(recorder)
      assert request["method"] == "POST"
      assert request["path"] == "/v1/tenants/acme/sources"
      assert request["body"] == Map.put(Spec.to_json(@source_spec), "name", "billing")
    end

    test "create_source omits an unset verify" do
      {client, recorder} = client(fn conn -> PlugTransport.json(conn, 201, @entry) end)

      assert {:ok, %Source{}} =
               Sources.create_source(client, "acme", "billing", %Spec{sinks: [%{"type" => "log"}]})

      assert [request] = PlugTransport.requests(recorder)
      assert request["body"] == %{"sinks" => [%{"type" => "log"}], "name" => "billing"}
    end

    test "update_source puts the spec without a name" do
      {client, recorder} = client(fn conn -> PlugTransport.json(conn, 200, @entry) end)

      assert {:ok, %Source{name: "billing"}} =
               Sources.update_source(client, "acme", "billing", @source_spec)

      assert [request] = PlugTransport.requests(recorder)
      assert request["method"] == "PUT"
      assert request["path"] == "/v1/tenants/acme/sources/billing"
      assert request["body"] == Spec.to_json(@source_spec)
      refute Map.has_key?(request["body"], "name")
    end

    test "delete_source succeeds with no body" do
      {client, recorder} = client(fn conn -> PlugTransport.text(conn, 204, "") end)

      assert :ok = Sources.delete_source(client, "acme", "billing")

      assert [
               %{
                 "method" => "DELETE",
                 "path" => "/v1/tenants/acme/sources/billing",
                 "body" => nil
               }
             ] =
               PlugTransport.requests(recorder)
    end
  end

  describe "error mapping" do
    test "404 is a not-found error carrying status and body" do
      {client, _recorder} =
        client(fn conn -> PlugTransport.json(conn, 404, %{"error" => "source_not_found"}) end)

      assert {:error, %SourceNotFoundError{status: 404, body: %{"error" => "source_not_found"}}} =
               Sources.get_source(client, "acme", "missing")
    end

    test "409 source_exists is a conflict error" do
      {client, _recorder} =
        client(fn conn -> PlugTransport.json(conn, 409, %{"error" => "source_exists"}) end)

      assert {:error, %SourceConflictError{status: 409, body: %{"error" => "source_exists"}}} =
               Sources.create_source(client, "acme", "billing", @source_spec)
    end

    test "409 source_store_read_only is its own error" do
      {client, _recorder} =
        client(fn conn ->
          PlugTransport.json(conn, 409, %{"error" => "source_store_read_only"})
        end)

      assert {:error, %SourceStoreReadOnlyError{status: 409}} =
               Sources.update_source(client, "acme", "billing", @source_spec)
    end

    test "400 carries the server's message" do
      {client, _recorder} =
        client(fn conn ->
          PlugTransport.json(conn, 400, %{
            "error" => "invalid_source",
            "message" => "sinks must not be empty"
          })
        end)

      assert {:error, %SourceInvalidError{status: 400} = error} =
               Sources.create_source(client, "acme", "billing", @source_spec)

      assert Exception.message(error) == "sinks must not be empty"
    end

    test "400 without a message carries the error code" do
      {client, _recorder} =
        client(fn conn -> PlugTransport.json(conn, 400, %{"error" => "invalid_tenant"}) end)

      assert {:error, %SourceInvalidError{} = error} = Sources.list_sources(client, "acme")
      assert Exception.message(error) == "invalid_tenant"
    end

    test "5xx is an unavailable error carrying status and body" do
      {client, _recorder} =
        client(fn conn -> PlugTransport.json(conn, 503, %{"error" => "boom"}) end)

      assert {:error, %SourcesUnavailableError{status: 503, body: %{"error" => "boom"}}} =
               Sources.list_sources(client, "acme")
    end

    test "an unreachable server is an unavailable error with no status or body" do
      {client, _recorder} =
        client(fn conn -> PlugTransport.transport_error(conn, :econnrefused) end)

      assert {:error, %SourcesUnavailableError{status: nil, body: nil}} =
               Sources.get_source(client, "acme", "billing")
    end
  end

  describe "input validation" do
    test "every method refuses an invalid tenant without a request" do
      for value <- @invalid_ids do
        {client, recorder} =
          client(fn conn -> flunk("no request expected, got #{conn.request_path}") end)

        calls = [
          fn -> Sources.list_sources(client, value) end,
          fn -> Sources.get_source(client, value, "billing") end,
          fn -> Sources.create_source(client, value, "billing", @source_spec) end,
          fn -> Sources.update_source(client, value, "billing", @source_spec) end,
          fn -> Sources.delete_source(client, value, "billing") end
        ]

        for call <- calls do
          assert {:error, %SourceInvalidError{status: nil, body: nil} = error} = call.()
          assert Exception.message(error) == "invalid tenant: #{inspect(value)}"
        end

        assert PlugTransport.requests(recorder) == []
      end
    end

    test "a source method refuses an invalid name without a request" do
      for value <- @invalid_ids do
        {client, recorder} =
          client(fn conn -> flunk("no request expected, got #{conn.request_path}") end)

        calls = [
          fn -> Sources.get_source(client, "acme", value) end,
          fn -> Sources.create_source(client, "acme", value, @source_spec) end,
          fn -> Sources.update_source(client, "acme", value, @source_spec) end,
          fn -> Sources.delete_source(client, "acme", value) end
        ]

        for call <- calls do
          assert {:error, %SourceInvalidError{status: nil} = error} = call.()
          assert Exception.message(error) == "invalid source name: #{inspect(value)}"
        end

        assert PlugTransport.requests(recorder) == []
      end
    end

    test "dashes and underscores travel as the path" do
      {client, recorder} =
        client(fn conn ->
          if conn.method == "DELETE" do
            PlugTransport.text(conn, 204, "")
          else
            PlugTransport.json(conn, 200, %{"tenant" => "acme-corp", "entries" => []})
          end
        end)

      assert {:ok, []} = Sources.list_sources(client, "acme-corp")
      assert :ok = Sources.delete_source(client, "acme-corp", "my_source-1")

      assert Enum.map(PlugTransport.requests(recorder), & &1["path"]) == [
               "/v1/tenants/acme-corp/sources",
               "/v1/tenants/acme-corp/sources/my_source-1"
             ]
    end
  end
end
