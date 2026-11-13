defmodule Ankusa.Gateway.Router do
  @moduledoc """
  The gateway's HTTP surface. Every route but `GET /health` and `GET /ready`
  passes `Ankusa.Gateway.Authn` first.

    * `GET /health` — liveness: `200 {"status":"ok"}`.
    * `GET /ready` — readiness (`Ankusa.Gateway.Health`): `200`, or `503` with
      `retry-after: 1`.
    * `GET|HEAD /v1/claims/:tenant_id/:claim_id` — `Ankusa.Gateway.Claims`.
    * `GET /v1/objects/:space` (one page of a listing) and
      `GET|HEAD|PUT|DELETE /v1/objects/:space/*key` — `Ankusa.Gateway.Objects`;
      the key is read from the raw `conn.request_path`, never from the glob.
    * anything else — `404 {"error":"not_found"}`.

  Mount it with the instance it serves: `plug: {Ankusa.Gateway.Router,
  [instance: :default]}`; every handler reads it from
  `conn.assigns.ankusa_opts[:instance]`.
  """

  use Plug.Router, copy_opts_to_assign: :ankusa_opts

  alias Ankusa.Http

  plug(Ankusa.Gateway.Authn)
  plug(:match)
  plug(:dispatch)

  get "/health" do
    Http.send_json(conn, 200, %{status: "ok"})
  end

  get "/ready" do
    case Ankusa.Gateway.Health.ready(instance(conn)) do
      {:ok, body} ->
        Http.send_json(conn, 200, body)

      {:error, body} ->
        conn
        |> Plug.Conn.put_resp_header("retry-after", "1")
        |> Http.send_json(503, body)
    end
  end

  get "/v1/claims/:tenant_id/:claim_id" do
    Ankusa.Gateway.Claims.serve(conn, tenant_id, claim_id)
  end

  head "/v1/claims/:tenant_id/:claim_id" do
    Ankusa.Gateway.Claims.serve(conn, tenant_id, claim_id)
  end

  get "/v1/objects/:space" do
    Ankusa.Gateway.Objects.list(conn, space)
  end

  match "/v1/objects/:space/*_key" do
    Ankusa.Gateway.Objects.object(conn, space)
  end

  match _ do
    Http.send_json(conn, 404, %{error: "not_found"})
  end

  defp instance(%Plug.Conn{} = conn),
    do: Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
end
