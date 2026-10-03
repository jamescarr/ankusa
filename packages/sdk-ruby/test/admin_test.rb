# frozen_string_literal: true

require "test_helper"

# Unit tests for the operator (admin) client's request shapes, which no
# conformance vector covers.
class AdminTest < Minitest::Test
  def test_query_params_sent_only_when_present
    transport, recorded = injected(->(_request) { json_response(200, {"entries" => []}) })
    client = Ankusa::AdminClient.new("http://gateway.invalid", transport: transport)

    client.list_dead_letters({"source_id" => "demo", "since" => 1720000000000, "limit" => 10})
    client.list_dead_letters({"limit" => 5})
    client.list_quarantined({"limit" => 7})
    client.list_quarantined({})

    assert_equal(
      ["source_id=demo&since=1720000000000&limit=10", "limit=5", "limit=7", ""],
      recorded.map { |request| request.url.query.to_s }
    )
  end
end
