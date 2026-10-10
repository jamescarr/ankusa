# frozen_string_literal: true

require "socket"
require "test_helper"

# Runs the language-neutral conformance vectors in conformance/ against this
# SDK: one test per vector, named by its `id`. See conformance/README.md for
# the vector format and the runner contract every SDK follows.
CONFORMANCE_DIR = File.expand_path("../../../conformance", __dir__)

CONFORMANCE_CASES = Dir[File.join(CONFORMANCE_DIR, "cases", "*.json")].sort.flat_map do |path|
  JSON.parse(File.read(path)).fetch("cases")
end

raise "no conformance cases found under #{CONFORMANCE_DIR}" if CONFORMANCE_CASES.empty?

# The exact exported class names the vectors name; the runner matches by
# identity (`instance_of?`), never by subclass.
CONFORMANCE_ERROR_CLASSES = %w[
  InvalidClaimRefError
  ClaimNotFoundError
  ClaimRejectedError
  ClaimIntegrityError
  ClaimCheckUnavailableError
  MissingHookIdError
  InvalidSignatureError
  RoutesError
  InvalidRouteIdError
  RoutesUnavailableError
  RouteNotFoundError
  RoutesRejectedError
  AdminError
  AdminUnavailableError
  RoleNotEnabledError
  AdminRejectedError
  InvalidMessageError
].to_h { |name| [name, Ankusa.const_get(name)] }

# The attributes a vector may assert on a thrown error. `retryable` reads
# `retryable?`; the rest read the method of the same name.
CONFORMANCE_ERROR_ATTRIBUTES = %w[retryable status body code field message conflicting_id max_routes role].freeze

class ConformanceTest < Minitest::Test
  def run_conformance_case(case_hash)
    requests = []
    ok = nil
    error = nil

    begin
      ok = run_operation(case_hash.fetch("operation"), case_hash, requests)
    rescue Ankusa::Error => e
      name = CONFORMANCE_ERROR_CLASSES.find { |_, klass| e.instance_of?(klass) }&.first
      raise if name.nil?

      error = {"class" => name}
      CONFORMANCE_ERROR_ATTRIBUTES.each do |key|
        reader = (key == "retryable") ? :retryable? : key.to_sym
        next unless e.respond_to?(reader)

        error[key] = e.public_send(reader)
      end
    end

    expect = case_hash.fetch("expect")
    if expect.key?("ok")
      assert_nil error, "expected ok, got #{error.inspect}"
      expected = expected_ok(case_hash, expect["ok"])
      # `routes_delete` expects `ok: null`; Minitest 6 refuses assert_equal nil.
      if expected.nil?
        assert_nil ok, "expected ok null, got #{ok.inspect}"
      else
        assert_equal expected, ok
      end
    elsif expect.key?("error")
      want = expect.fetch("error")
      refute_nil error, "expected error #{want.inspect}, got ok #{ok.inspect}"
      assert_equal want.fetch("class"), error.fetch("class")
      want.each do |key, value|
        next if key == "class"

        assert error.key?(key), "#{error["class"]} has no attribute #{key.inspect} (expected #{value.inspect})"
        # A vector may assert a null attribute (`InvalidMessageError#field`);
        # Minitest 6 refuses assert_equal nil.
        if value.nil?
          assert_nil error[key], "#{error["class"]}.#{key}"
        else
          assert_equal value, error[key], "#{error["class"]}.#{key}"
        end
      end
    end

    assert_requests(requests, expect["requests"]) if expect.key?("requests")
  end

  private

  def run_operation(operation, case_hash, requests)
    input = case_hash.fetch("input")

    case operation
    when "parse_claim_ref"
      ref = Ankusa.parse_claim_ref(input.fetch("ref"))
      {"tenant_id" => ref.tenant_id, "claim_id" => ref.claim_id, "path" => ref.path}
    when "verify_signature"
      options = {now: input.fetch("now")}
      options[:tolerance_seconds] = input.fetch("tolerance_seconds") if input.key?("tolerance_seconds")
      verified = Ankusa.verify_signature(input.fetch("headers"), body_bytes(input["body"]), input.fetch("secrets"), **options)
      {"id" => verified.id, "timestamp" => verified.timestamp}
    when "parse_headers"
      headers = Ankusa.parse_headers(input.fetch("headers"))
      {
        "id" => headers.id,
        "source" => headers.source,
        "tenant" => headers.tenant,
        "content_type" => headers.content_type,
        "dedupe_key" => headers.dedupe_key,
        "replay_id" => headers.replay_id,
        "idempotency_key" => headers.to_h[:idempotency_key]
      }
    when "decode_message"
      message = Ankusa.decode_message(input.fetch("message"))
      {
        "v" => message.v,
        "id" => message.id,
        "source_id" => message.source_id,
        "tenant_id" => message.tenant_id,
        "received_at" => message.received_at,
        "content_type" => message.content_type,
        "size" => message.size,
        "body_base64" => message.body.nil? ? nil : [message.body].pack("m0"),
        "claim" => message.claim,
        "sha256" => message.sha256,
        "dedupe_key" => message.dedupe_key,
        "replay_id" => message.replay_id,
        "idempotency_key" => message.to_h[:idempotency_key],
        "headers" => message.headers
      }
    when "idempotency_key"
      include_replay = input.fetch("include_replay", false)
      key =
        if input.key?("message")
          Ankusa.decode_message(input.fetch("message")).idempotency_key(include_replay: include_replay)
        else
          Ankusa.parse_headers(input.fetch("headers")).idempotency_key(include_replay: include_replay)
        end
      {"key" => key}
    when "redeem"
      with_client(Ankusa::ClaimCheckClient, input, requests) do |client|
        body = client.redeem(input.fetch("ref"), input.fetch("sha256"))
        {"body" => {"base64" => [body].pack("m0")}}
      end
    when "health"
      with_client(Ankusa::ClaimCheckClient, input, requests, &:health)
    when "routes_health"
      with_client(Ankusa::RoutesClient, input, requests, &:health)
    when "routes_list"
      with_client(Ankusa::RoutesClient, input, requests) { |client| client.list_routes(input["params"]) }
    when "routes_create"
      with_client(Ankusa::RoutesClient, input, requests) { |client| client.create_route(input.fetch("input")) }
    when "routes_get"
      with_client(Ankusa::RoutesClient, input, requests) { |client| client.get_route(input.fetch("id")) }
    when "routes_replace"
      with_client(Ankusa::RoutesClient, input, requests) do |client|
        client.replace_route(input.fetch("id"), input.fetch("input"))
      end
    when "routes_update"
      with_client(Ankusa::RoutesClient, input, requests) do |client|
        client.update_route(input.fetch("id"), input.fetch("patch"))
      end
    when "routes_delete"
      with_client(Ankusa::RoutesClient, input, requests) { |client| client.delete_route(input.fetch("id")) }
      nil
    when "routes_ip_rules_get"
      with_client(Ankusa::RoutesClient, input, requests, &:get_ip_rules)
    when "routes_ip_rules_put"
      with_client(Ankusa::RoutesClient, input, requests) { |client| client.put_ip_rules(input.fetch("rules")) }
    when "routes_test"
      with_client(Ankusa::RoutesClient, input, requests) { |client| client.test_route(input.fetch("request")) }
    when "admin_health"
      with_client(Ankusa::AdminClient, input, requests, &:health)
    when "admin_metrics"
      with_client(Ankusa::AdminClient, input, requests) { |client| {"text" => client.metrics} }
    when "admin_config"
      with_client(Ankusa::AdminClient, input, requests, &:config)
    when "admin_dlq_list"
      with_client(Ankusa::AdminClient, input, requests) { |client| client.list_dead_letters(input["params"]) }
    when "admin_replay_create"
      with_client(Ankusa::AdminClient, input, requests) { |client| client.create_replay(input.fetch("spec")) }
    when "admin_replay_get"
      with_client(Ankusa::AdminClient, input, requests) { |client| client.get_replay(input.fetch("id")) }
    when "admin_replay_list"
      with_client(Ankusa::AdminClient, input, requests, &:list_replays)
    when "admin_replay_update"
      with_client(Ankusa::AdminClient, input, requests) do |client|
        client.update_replay(input.fetch("id"), input.fetch("patch"))
      end
    when "admin_quarantine"
      with_client(Ankusa::AdminClient, input, requests) { |client| client.list_quarantined(input["params"]) }
    else
      flunk("unknown conformance operation #{operation.inspect}")
    end
  end

  # Builds the client the case's `input.client` asks for: an injected transport
  # (no server, base URL http://gateway.invalid) or a real client against the
  # mock gateway.
  def with_client(client_class, input, requests, &block)
    client_spec = input["client"] || {}
    headers = client_spec["headers"]
    timeout = client_spec.key?("timeout_ms") ? client_spec["timeout_ms"] / 1000.0 : 10.0

    if client_spec["transport"] == "injected"
      transport = injected_transport(input.fetch("gateway"), requests)
      block.call(client_class.new("http://gateway.invalid", headers: headers, timeout: timeout, transport: transport))
    else
      with_gateway(input.fetch("gateway"), requests) do |base_url|
        block.call(client_class.new(base_url, headers: headers, timeout: timeout, transport: nil))
      end
    end
  end

  # The transport an `"injected"` case replaces the network with: it records the
  # request in the same shape the real gateway mock does, and answers with the
  # case's `gateway` response.
  def injected_transport(spec, requests)
    status = spec.fetch("status")
    headers = spec["headers"] || {}
    payload = body_bytes(spec["body"])

    lambda do |request|
      requests << {
        "method" => request.http_method,
        "path" => request.url.request_uri,
        "headers" => request.headers.to_h { |key, value| [key.to_s.downcase, value] },
        "body" => request.body.nil? ? nil : JSON.parse(request.body)
      }

      Ankusa::Transport::Response.new(
        status: status,
        headers: headers.merge("content-length" => payload.bytesize.to_s),
        body: payload
      )
    end
  end

  # A real HTTP server standing in for a deployment's gateway: it answers every
  # request with `spec` and records what it saw. Hand-written on TCPServer --
  # WEBrick left the stdlib, and its proc handler doesn't cover every verb.
  def with_gateway(spec, requests)
    if spec["unreachable"]
      yield "http://127.0.0.1:1"
      return
    end

    status = spec.fetch("status")
    headers = spec["headers"] || {}
    payload = body_bytes(spec["body"])
    delay = (spec["delay_ms"] || 0) / 1000.0

    server = TCPServer.new("127.0.0.1", 0)
    port = server.addr[1]
    workers = []
    acceptor = Thread.new do
      loop do
        socket = begin
          server.accept
        rescue IOError, SystemCallError
          break
        end
        workers << Thread.new(socket) { |conn| serve(conn, requests, status, headers, payload, delay) }
      end
    end

    begin
      yield "http://127.0.0.1:#{port}"
    ensure
      server.close
      acceptor.kill
      workers.each(&:kill)
    end
  end

  def serve(conn, requests, status, headers, payload, delay)
    request_line = conn.gets
    return if request_line.nil?

    method_name, target, = request_line.split(" ")
    request_headers = {}
    while (line = conn.gets)
      line = line.chomp
      break if line.empty?

      name, value = line.split(":", 2)
      request_headers[name.strip.downcase] = value.to_s.strip
    end

    length = request_headers["content-length"].to_i
    raw_body = (length > 0) ? conn.read(length) : ""

    requests << {
      "method" => method_name,
      "path" => target,
      "headers" => request_headers,
      "body" => (raw_body.nil? || raw_body.empty?) ? nil : JSON.parse(raw_body)
    }

    sleep(delay) if delay > 0

    response = "HTTP/1.1 #{status} X\r\n"
    headers.each { |key, value| response << "#{key}: #{value}\r\n" }
    response << "content-length: #{payload.bytesize}\r\n"
    response << "connection: close\r\n\r\n"
    conn.write(response)
    conn.write(payload)
  rescue IOError, SystemCallError
    # The client may already have timed out and gone; the request it did send is
    # recorded above.
  ensure
    begin
      conn.close
    rescue IOError, SystemCallError
      nil
    end
  end

  # A vector Body as bytes: text is UTF-8, base64 is decoded, json is compact
  # JSON, and a missing body is zero bytes.
  def body_bytes(body)
    return "".b if body.nil?
    return body.fetch("text").dup.force_encoding(Encoding::UTF_8).b if body.key?("text")
    return body.fetch("base64").unpack1("m0") if body.key?("base64")
    return JSON.generate(body.fetch("json")).b if body.key?("json")

    raise "unknown Body: #{body.inspect}"
  end

  # Bytes can't round-trip through JSON: both sides become base64.
  def expected_ok(case_hash, expected)
    return expected unless case_hash.fetch("operation") == "redeem"

    {"body" => {"base64" => [body_bytes(expected["body"])].pack("m0")}}
  end

  def assert_requests(actual, expected)
    assert_equal expected.length, actual.length, "requests: #{actual.inspect}"
    actual.zip(expected).each do |got, want|
      assert_equal want["method"], got["method"], "method"
      assert_equal want["path"], got["path"], "path"
      (want["headers"] || {}).each do |name, value|
        assert_equal value, got["headers"][name], "header #{name.inspect}"
      end
      assert_equal want["body"], got["body"], "body" if want.key?("body")
    end
  end
end

CONFORMANCE_CASES.each do |case_hash|
  ConformanceTest.define_method("test_#{case_hash.fetch("id")}") do
    run_conformance_case(case_hash)
  end
end
