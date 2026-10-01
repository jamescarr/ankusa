# frozen_string_literal: true

require "test_helper"

# Unit tests for the source-management client, mirroring the conformance
# vectors: happy paths, error classification, and the version latch. The
# sources client has no conformance vectors, so this is its only coverage.
class SourcesTest < Minitest::Test
  BASE_URL = "http://admin.test"

  ENTRY = {
    "tenant" => "acme",
    "name" => "billing",
    "source_id" => "acme.billing",
    "ingest_path" => "/webhooks/acme.billing",
    "verify" => {"type" => "hmac", "secret" => "[REDACTED]", "signature_header" => "X-Sig"},
    "on_verify_failure" => "reject",
    "sinks" => [{"type" => "log"}]
  }.freeze

  SPEC = Ankusa::SourceSpec.new(
    sinks: [{"type" => "log"}],
    verify: {"type" => "hmac", "secret" => "s3cr3t", "signature_header" => "X-Sig"},
    on_verify_failure: "reject"
  )

  INVALID_IDS = ["..", "a/b", "a?b=1", "a#b", "a" * 65, ""].freeze

  def make_client(responder, expected_version: nil)
    transport, recorded = injected(responder)
    client = Ankusa::SourcesClient.new(BASE_URL, expected_version: expected_version, transport: transport)
    [client, recorded]
  end

  def health(request, version = "0.3.0")
    assert_equal "/health", request.url.path
    json_response(200, {"status" => "ok", "version" => version})
  end

  # --- SourceSpec ------------------------------------------------------------

  def test_source_spec_to_request_body_omits_none_fields
    assert_equal(
      {
        "sinks" => [{"type" => "log"}],
        "verify" => {"type" => "hmac", "secret" => "s3cr3t", "signature_header" => "X-Sig"},
        "on_verify_failure" => "reject"
      },
      SPEC.to_request_body
    )
  end

  def test_source_spec_to_request_body_omits_unset_verify_and_failure_mode
    assert_equal({"sinks" => [{"type" => "log"}]}, Ankusa::SourceSpec.new(sinks: [{"type" => "log"}]).to_request_body)
  end

  # --- server_version / version latch ----------------------------------------

  def test_server_version_reads_the_health_version
    client, = make_client(method(:health))
    assert_equal "0.3.0", client.server_version
  end

  def test_server_version_fetches_health_once_and_caches_it
    handler = lambda do |request|
      if request.url.path == "/health"
        health(request)
      else
        json_response(200, {"tenant" => "acme", "entries" => []})
      end
    end

    client, recorded = make_client(handler)
    client.server_version
    client.list_sources("acme")
    assert_equal 1, recorded.count { |request| request.url.path == "/health" }
  end

  def test_list_sources_raises_version_mismatch_when_expected_version_differs
    client, = make_client(method(:health), expected_version: "9.9.9")
    error = assert_raises(Ankusa::VersionMismatchError) { client.list_sources("acme") }
    assert_includes error.message, "9.9.9"
    assert_includes error.message, "0.3.0"
    assert_nil error.status
    assert_nil error.body
  end

  def test_version_mismatch_is_rechecked_from_cache_without_another_health_request
    client, recorded = make_client(method(:health), expected_version: "9.9.9")
    assert_raises(Ankusa::VersionMismatchError) { client.list_sources("acme") }
    assert_raises(Ankusa::VersionMismatchError) { client.get_source("acme", "billing") }
    assert_equal 1, recorded.count { |request| request.url.path == "/health" }
  end

  def test_expected_version_match_does_not_raise
    handler = lambda do |request|
      if request.url.path == "/health"
        health(request)
      else
        json_response(200, {"tenant" => "acme", "entries" => [ENTRY]})
      end
    end

    client, = make_client(handler, expected_version: "0.3.0")
    assert_equal [Ankusa::Source.from_h(ENTRY)], client.list_sources("acme")
  end

  # --- list / get / create / update / delete ---------------------------------

  def test_list_sources_parses_entries
    handler = lambda do |request|
      if request.url.path == "/health"
        health(request)
      else
        assert_equal "GET", request.http_method
        assert_equal "/v1/tenants/acme/sources", request.url.path
        json_response(200, {"tenant" => "acme", "entries" => [ENTRY]})
      end
    end

    client, = make_client(handler)
    assert_equal [Ankusa::Source.from_h(ENTRY)], client.list_sources("acme")
  end

  def test_get_source_requires_no_version_probe_without_expected_version
    handler = lambda do |request|
      assert_equal "/v1/tenants/acme/sources/billing", request.url.path
      json_response(200, ENTRY)
    end

    client, = make_client(handler)
    source = client.get_source("acme", "billing")
    assert_equal "acme.billing", source.source_id
    assert_equal "/webhooks/acme.billing", source.ingest_path
    assert_equal({"type" => "hmac", "secret" => "[REDACTED]", "signature_header" => "X-Sig"}, source.verify)
    assert_equal "reject", source.on_verify_failure
    assert_equal [{"type" => "log"}], source.sinks
  end

  def test_create_source_posts_the_spec_plus_name
    handler = lambda do |request|
      assert_equal "POST", request.http_method
      assert_equal "/v1/tenants/acme/sources", request.url.path
      assert_equal SPEC.to_request_body.merge("name" => "billing"), JSON.parse(request.body)
      json_response(201, ENTRY)
    end

    client, = make_client(handler)
    assert_equal Ankusa::Source.from_h(ENTRY), client.create_source("acme", "billing", SPEC)
  end

  def test_create_source_sends_no_verify_when_spec_has_none
    handler = lambda do |request|
      assert_equal({"sinks" => [{"type" => "log"}], "name" => "billing"}, JSON.parse(request.body))
      json_response(201, ENTRY)
    end

    client, = make_client(handler)
    client.create_source("acme", "billing", Ankusa::SourceSpec.new(sinks: [{"type" => "log"}]))
  end

  def test_update_source_puts_to_the_named_path
    handler = lambda do |request|
      assert_equal "PUT", request.http_method
      assert_equal "/v1/tenants/acme/sources/billing", request.url.path
      assert_equal SPEC.to_request_body, JSON.parse(request.body)
      json_response(200, ENTRY)
    end

    client, = make_client(handler)
    assert_equal Ankusa::Source.from_h(ENTRY), client.update_source("acme", "billing", SPEC)
  end

  def test_delete_source_returns_none_on_204
    handler = lambda do |request|
      assert_equal "DELETE", request.http_method
      assert_equal "/v1/tenants/acme/sources/billing", request.url.path
      assert_nil request.body
      Ankusa::Transport::Response.new(status: 204, headers: {}, body: "")
    end

    client, = make_client(handler)
    assert_nil client.delete_source("acme", "billing")
  end

  def test_delete_source_404_maps_to_source_not_found_error
    handler = lambda do |request|
      assert_equal "DELETE", request.http_method
      assert_equal "/v1/tenants/acme/sources/billing", request.url.path
      json_response(404, {"error" => "source_not_found"})
    end

    client, = make_client(handler)
    error = assert_raises(Ankusa::SourceNotFoundError) { client.delete_source("acme", "billing") }
    assert_equal 404, error.status
    assert_equal({"error" => "source_not_found"}, error.body)
  end

  def test_delete_source_409_source_store_read_only_maps_to_source_store_read_only_error
    client, = make_client(->(_request) { json_response(409, {"error" => "source_store_read_only"}) })
    error = assert_raises(Ankusa::SourceStoreReadOnlyError) { client.delete_source("acme", "billing") }
    assert_equal 409, error.status
    assert_equal({"error" => "source_store_read_only"}, error.body)
  end

  def test_delete_source_400_maps_to_source_invalid_error
    body = {"error" => "invalid_source", "message" => "cannot delete a seeded source"}
    client, = make_client(->(_request) { json_response(400, body) })
    error = assert_raises(Ankusa::SourceInvalidError) { client.delete_source("acme", "billing") }
    assert_equal 400, error.status
    assert_equal "cannot delete a seeded source", error.message
    assert_equal body, error.body
  end

  # --- error mapping ---------------------------------------------------------

  def test_404_maps_to_source_not_found_error
    client, = make_client(->(_request) { json_response(404, {"error" => "source_not_found"}) })
    error = assert_raises(Ankusa::SourceNotFoundError) { client.get_source("acme", "missing") }
    assert_equal 404, error.status
    assert_equal({"error" => "source_not_found"}, error.body)
  end

  def test_409_source_exists_maps_to_source_conflict_error
    client, = make_client(->(_request) { json_response(409, {"error" => "source_exists"}) })
    error = assert_raises(Ankusa::SourceConflictError) { client.create_source("acme", "billing", SPEC) }
    assert_equal 409, error.status
    assert_equal({"error" => "source_exists"}, error.body)
  end

  def test_409_source_store_read_only_maps_to_source_store_read_only_error
    client, = make_client(->(_request) { json_response(409, {"error" => "source_store_read_only"}) })
    error = assert_raises(Ankusa::SourceStoreReadOnlyError) { client.update_source("acme", "billing", SPEC) }
    assert_equal 409, error.status
    assert_equal({"error" => "source_store_read_only"}, error.body)
  end

  def test_400_with_message_carries_the_server_message
    body = {"error" => "invalid_source", "message" => "sinks must not be empty"}
    client, = make_client(->(_request) { json_response(400, body) })
    error = assert_raises(Ankusa::SourceInvalidError) { client.create_source("acme", "billing", SPEC) }
    assert_equal 400, error.status
    assert_equal "sinks must not be empty", error.message
    assert_equal body, error.body
  end

  def test_400_without_message_carries_the_error_code
    client, = make_client(->(_request) { json_response(400, {"error" => "invalid_tenant"}) })
    error = assert_raises(Ankusa::SourceInvalidError) { client.list_sources("acme") }
    assert_equal 400, error.status
    assert_equal "invalid_tenant", error.message
  end

  def test_5xx_maps_to_sources_unavailable_error
    client, = make_client(->(_request) { json_response(503, {"error" => "boom"}) })
    error = assert_raises(Ankusa::SourcesUnavailableError) { client.list_sources("acme") }
    assert_equal 503, error.status
    assert_equal({"error" => "boom"}, error.body)
  end

  def test_transport_error_maps_to_sources_unavailable_error
    client, = make_client(->(_request) { raise Ankusa::Transport::Error, "connection refused" })
    error = assert_raises(Ankusa::SourcesUnavailableError) { client.get_source("acme", "billing") }
    assert_nil error.status
    assert_nil error.body
  end

  def test_every_error_is_a_sources_error
    [
      Ankusa::SourceNotFoundError,
      Ankusa::SourceConflictError,
      Ankusa::SourceStoreReadOnlyError,
      Ankusa::SourceInvalidError,
      Ankusa::SourcesUnavailableError,
      Ankusa::VersionMismatchError
    ].each do |error|
      assert_operator error, :<, Ankusa::SourcesError
    end
  end

  # --- input validation ------------------------------------------------------

  def test_every_method_rejects_an_invalid_tenant_without_a_request
    INVALID_IDS.each do |value|
      calls = [
        ->(client) { client.list_sources(value) },
        ->(client) { client.get_source(value, "billing") },
        ->(client) { client.create_source(value, "billing", SPEC) },
        ->(client) { client.update_source(value, "billing", SPEC) },
        ->(client) { client.delete_source(value, "billing") }
      ]

      calls.each do |call|
        client, recorded = make_client(->(_request) { flunk "a request was sent" })
        error = assert_raises(Ankusa::SourceInvalidError) { call.call(client) }
        assert_nil error.status
        assert_nil error.body
        assert_equal "invalid tenant: #{value.inspect}", error.message
        assert_empty recorded
      end
    end
  end

  def test_source_methods_reject_an_invalid_name_without_a_request
    INVALID_IDS.each do |value|
      calls = [
        ->(client) { client.get_source("acme", value) },
        ->(client) { client.create_source("acme", value, SPEC) },
        ->(client) { client.update_source("acme", value, SPEC) },
        ->(client) { client.delete_source("acme", value) }
      ]

      calls.each do |call|
        client, recorded = make_client(->(_request) { flunk "a request was sent" })
        error = assert_raises(Ankusa::SourceInvalidError) { call.call(client) }
        assert_nil error.status
        assert_equal "invalid source name: #{value.inspect}", error.message
        assert_empty recorded
      end
    end
  end

  def test_valid_identifiers_with_dash_and_underscore_are_sent_as_the_path
    paths = []
    handler = lambda do |request|
      paths << request.url.path
      if request.http_method == "DELETE"
        Ankusa::Transport::Response.new(status: 204, headers: {}, body: "")
      else
        json_response(200, {"tenant" => "acme-corp", "entries" => []})
      end
    end

    client, = make_client(handler)
    client.list_sources("acme-corp")
    client.delete_source("acme-corp", "my_source-1")

    assert_equal ["/v1/tenants/acme-corp/sources", "/v1/tenants/acme-corp/sources/my_source-1"], paths
  end
end
