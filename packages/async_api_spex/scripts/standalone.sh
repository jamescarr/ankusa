#!/usr/bin/env bash
#
# The standalone gate: async_api_spex is for other people's projects, so prove
# it in one. Scaffold a fresh `mix new` app that depends on nothing but this
# checkout (by path) and plug, decorate a producer and a consumer, assemble the
# document with `otp_app:`, serve it with RenderSpec and the Viewer, and write
# it with `mix async_api_spex.gen`.
#
#   packages/async_api_spex/scripts/standalone.sh
#
# Run by `mise run check:package async_api_spex`. KEEP=1 leaves the scaffold in
# place (its path is printed) for poking at by hand.
set -euo pipefail

LIB="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"

cleanup() {
  if [ "${KEEP:-}" = 1 ]; then
    echo "kept $WORK/shop_standalone" >&2
  else
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

echo "==> async_api_spex: no Ankusa in lib/"
if grep -rIil ankusa "$LIB/lib"; then
  fail "the files above mention Ankusa; the library must not depend on it"
fi

echo "==> async_api_spex: scaffold a fresh app in $WORK"
cd "$WORK"
mix new shop_standalone >/dev/null
cd shop_standalone
mkdir -p lib/shop_standalone

cat >mix.exs <<EOF
defmodule ShopStandalone.MixProject do
  use Mix.Project

  def project do
    [
      app: :shop_standalone,
      version: "0.1.0",
      elixir: "~> 1.18",
      elixirc_options: [warnings_as_errors: true],
      deps: [{:async_api_spex, path: "$LIB"}, {:plug, "~> 1.18"}]
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end
end
EOF

cat >lib/shop_standalone/events.ex <<'EOF'
defmodule ShopStandalone.Events.OrderCreated do
  defstruct [:id, :customer_id, :placed_at]

  use AsyncApiSpex.Schema,
    name: "OrderCreated",
    fields: [id: [type: :string, required: true], customer_id: :string, placed_at: :datetime]
end

defmodule ShopStandalone.Events.OrderShipped do
  defstruct [:id, :carrier]

  use AsyncApiSpex.Schema,
    name: "OrderShipped",
    fields: [id: [type: :string, required: true], carrier: {:enum, ["ups", "dhl"]}]
end
EOF

cat >lib/shop_standalone/order_producer.ex <<'EOF'
defmodule ShopStandalone.OrderProducer do
  use AsyncApiSpex.Channel,
    address: "shop.orders",
    server: [id: "kafka", host: "kafka:9092", protocol: "kafka"],
    messages: [ShopStandalone.Events.OrderCreated]

  def publish(_event), do: :ok
end
EOF

cat >lib/shop_standalone/shipment_consumer.ex <<'EOF'
defmodule ShopStandalone.ShipmentConsumer do
  use AsyncApiSpex.Channel,
    address: "shop.shipments",
    action: :receive,
    server: [id: "kafka", host: "kafka:9092", protocol: "kafka"],
    messages: [ShopStandalone.Events.OrderShipped]

  def handle(_msg), do: :ok
end
EOF

cat >lib/shop_standalone/async_api.ex <<'EOF'
defmodule ShopStandalone.AsyncApi do
  use AsyncApiSpex.Spec,
    otp_app: :shop_standalone,
    info: [title: "Shop", version: "1.0.0"]
end
EOF

cat >lib/shop_standalone/router.ex <<'EOF'
defmodule ShopStandalone.Router do
  use Plug.Router

  plug :match
  plug :dispatch

  forward "/asyncapi.json",
    to: AsyncApiSpex.Plug.RenderSpec,
    init_opts: [spec: ShopStandalone.AsyncApi]

  forward "/asyncapi", to: AsyncApiSpex.Plug.Viewer, init_opts: [path: "/asyncapi.json"]

  match _ do
    send_resp(conn, 404, "")
  end
end
EOF

cat >test/shop_standalone_test.exs <<'EOF'
defmodule ShopStandaloneTest do
  use ExUnit.Case
  import Plug.Test
  import Plug.Conn

  @opts ShopStandalone.Router.init([])

  test "the assembled document is valid" do
    assert AsyncApiSpex.validate(ShopStandalone.AsyncApi.spec()) == :ok
  end

  test "GET /asyncapi.json serves the producer and the consumer" do
    conn = ShopStandalone.Router.call(conn(:get, "/asyncapi.json"), @opts)

    assert conn.status == 200
    assert [content_type] = get_resp_header(conn, "content-type")
    assert String.starts_with?(content_type, "application/asyncapi+json")

    doc = JSON.decode!(conn.resp_body)
    assert Map.keys(doc["servers"]) == ["kafka"]
    assert Enum.sort(Map.keys(doc["channels"])) == ["shop_orders", "shop_shipments"]

    assert Enum.sort(Map.keys(doc["operations"])) ==
             ["receive-shop_shipments", "send-shop_orders"]

    assert doc["operations"]["receive-shop_shipments"]["action"] == "receive"
    assert doc["components"]["schemas"]["OrderCreated"]["required"] == ["id"]

    assert doc["components"]["schemas"]["OrderShipped"]["properties"]["carrier"]["enum"] ==
             ["ups", "dhl"]
  end

  test "GET /asyncapi serves the viewer pointed at the document" do
    conn = ShopStandalone.Router.call(conn(:get, "/asyncapi"), @opts)

    assert conn.status == 200
    assert [content_type] = get_resp_header(conn, "content-type")
    assert String.starts_with?(content_type, "text/html")
    assert conn.resp_body =~ ~s|"/asyncapi.json"|
    assert conn.resp_body =~ "AsyncApiStandalone.render"
  end
end
EOF

echo "==> shop_standalone: deps, compile, test"
mix deps.get
mix compile --warnings-as-errors
mix test

echo "==> shop_standalone: mix async_api_spex.gen"
mix async_api_spex.gen --spec ShopStandalone.AsyncApi --output asyncapi.json
mix run --no-start -e '
doc = JSON.decode!(File.read!("asyncapi.json"))
"3.0.0" = doc["asyncapi"]
["shop_orders", "shop_shipments"] = doc["channels"] |> Map.keys() |> Enum.sort()
'

echo "==> async_api_spex: standalone OK"
