defmodule Ankusa.ClaimCheck.Router do
  @moduledoc """
  The `:claim_check` role's HTTP API: read-only byte transport for claim-check
  refs. Deliberately its own listener (`claim_check.port`, default `4001`),
  separate from `Ankusa.Edge.Router`: the edge is internet-facing, this is
  internal-network-only.

  It performs **no authentication or authorization**. Whatever fronts the port
  (a mesh, Envoy, an API gateway) decides who may read what; the tenant is a
  path segment so that layer can check it without reading a body. A shared
  cache must sit behind that layer, never in front of it.

  ## API

    * `GET /v1/claims/:tenant_id/:object_id/:offset/:length` — the claim's
      exact bytes, `application/octet-stream`, cacheable forever (objects are
      written once and never rewritten). `400` malformed, `404` no such
      object, `416` range past the object's end, `503` + `Retry-After` when
      the store is unavailable.
    * `GET /health` — liveness.

  No integrity check happens here: the path carries no digest. The reader
  checks the bytes against the sha256 in its ref (`Ankusa.ClaimCheck.redeem/2`
  does this in-process).
  """

  use Plug.Router, copy_opts_to_assign: :ankusa_opts

  alias Ankusa.ClaimCheck
  alias Ankusa.ClaimCheck.Ref
  alias Ankusa.Http

  @immutable "public, max-age=31536000, immutable"

  plug(:match)
  plug(:dispatch)

  get "/health" do
    Http.send_json(conn, 200, %{status: "ok"})
  end

  get "/v1/claims/:tenant_id/:object_id/:offset/:length" do
    with :ok <- Ref.validate_tenant(tenant_id),
         :ok <- Ref.validate_object_id(object_id),
         {:ok, offset, length} <- well_formed(Ref.parse_range(offset, length)),
         {:ok, bin} <- ClaimCheck.read(instance(conn), tenant_id, object_id, offset, length) do
      conn
      |> Plug.Conn.put_resp_content_type("application/octet-stream", nil)
      |> Plug.Conn.put_resp_header("cache-control", @immutable)
      |> Plug.Conn.send_resp(200, bin)
    else
      {:error, reason} -> error_response(conn, reason)
    end
  end

  match _ do
    Http.send_json(conn, 404, %{error: "not_found"})
  end

  defp instance(%Plug.Conn{} = conn) do
    Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
  end

  defp well_formed({:error, :invalid_range}), do: {:error, :malformed_range}
  defp well_formed(ok), do: ok

  # Both are `invalid_range` to the caller; the status says which: a range that
  # could never be valid (400) or one past the end of a real object (416).
  defp error_response(conn, :malformed_range),
    do: Http.send_json(conn, 400, %{error: "invalid_range"})

  defp error_response(conn, :invalid_range),
    do: Http.send_json(conn, 416, %{error: "invalid_range"})

  defp error_response(conn, :invalid_tenant),
    do: Http.send_json(conn, 400, %{error: "invalid_tenant"})

  defp error_response(conn, :invalid_id), do: Http.send_json(conn, 400, %{error: "invalid_id"})
  defp error_response(conn, :not_found), do: Http.send_json(conn, 404, %{error: "not_found"})

  defp error_response(conn, {:unavailable, reason}) do
    conn
    |> Plug.Conn.put_resp_header("retry-after", "1")
    |> Http.send_json(503, %{error: "store_unavailable", reason: inspect(reason)})
  end
end
