# frozen_string_literal: true

require "test_helper"

# Unit test for the route-management client's query-string behavior, which no
# conformance vector covers.
class RoutesTest < Minitest::Test
  def test_list_routes_sends_only_present_query_params
    transport, recorded = injected(->(_request) { json_response(200, {"routes" => []}) })
    client = Ankusa::RoutesClient.new("http://gateway.invalid", transport: transport)

    client.list_routes({"enabled" => true, "limit" => 50, "cursor" => "stripe"})
    client.list_routes({"enabled" => false})
    client.list_routes({})

    assert_equal(
      ["enabled=true&limit=50&cursor=stripe", "enabled=false", ""],
      recorded.map { |request| request.url.query.to_s }
    )
  end
end
