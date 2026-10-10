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

    * `GET /v1/claims/:tenant_id/:claim_id` — the claim's exact bytes,
      `application/octet-stream`, `Cache-Control: private, max-age=31536000,
      immutable` (claims are written once and never rewritten; `private`
      because the path is a capability, and a shared cache would serve it to
      whoever asks). `400` malformed, `404` no such claim, `503
      store_unavailable` + `Retry-After: 1` when the store is unavailable, `503
      store_forbidden` + `Retry-After: 60` when the store refused this node's
      credential. A 503 body never carries the store's error; it is logged.
    * `HEAD /v1/claims/:tenant_id/:claim_id` — the same status and headers,
      `content-length` included, without the body.
    * `GET /health` — liveness.

  No integrity check happens here: the path carries no digest. The reader
  checks the bytes against the sha256 its queue message carried
  (`Ankusa.ClaimCheck.redeem/3` does this in-process).
  """

  use Plug.Router, copy_opts_to_assign: :ankusa_opts

  require Logger

  alias Ankusa.{ClaimCheck, Http}

  @cache_control "private, max-age=31536000, immutable"

  plug(:match)
  plug(:dispatch)

  get "/health" do
    Http.send_json(conn, 200, %{status: "ok"})
  end

  get "/v1/claims/:tenant_id/:claim_id" do
    claim(conn, tenant_id, claim_id)
  end

  head "/v1/claims/:tenant_id/:claim_id" do
    claim(conn, tenant_id, claim_id)
  end

  defp claim(conn, tenant_id, claim_id) do
    case ClaimCheck.read(instance(conn), tenant_id, claim_id) do
      {:ok, bin} ->
        conn
        |> Plug.Conn.put_resp_content_type("application/octet-stream", nil)
        |> Plug.Conn.put_resp_header("cache-control", @cache_control)
        |> send_claim(bin)

      {:error, reason} ->
        error_response(conn, reason, tenant_id, claim_id)
    end
  end

  # A HEAD response carries the claim's length and no body: the server keeps a
  # `content-length` the plug set when the body it is handed is empty.
  defp send_claim(%Plug.Conn{method: "HEAD"} = conn, bin) do
    conn
    |> Plug.Conn.put_resp_header("content-length", Integer.to_string(byte_size(bin)))
    |> Plug.Conn.send_resp(200, "")
  end

  defp send_claim(conn, bin), do: Plug.Conn.send_resp(conn, 200, bin)

  match _ do
    Http.send_json(conn, 404, %{error: "not_found"})
  end

  defp instance(%Plug.Conn{} = conn) do
    Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
  end

  defp error_response(conn, :invalid_tenant, _tenant_id, _claim_id),
    do: Http.send_json(conn, 400, %{error: "invalid_tenant"})

  defp error_response(conn, :invalid_id, _tenant_id, _claim_id),
    do: Http.send_json(conn, 400, %{error: "invalid_id"})

  defp error_response(conn, :not_found, _tenant_id, _claim_id),
    do: Http.send_json(conn, 404, %{error: "not_found"})

  # Kept retryable on purpose: a gateway with the wrong credential is a
  # configuration fault, and answering it permanently would make every worker
  # dead-letter claims that are there.
  defp error_response(conn, :forbidden, tenant_id, claim_id) do
    Logger.warning(
      "[ankusa] claim gateway: the object store refused this node's credential reading " <>
        "#{tenant_id}/#{claim_id}"
    )

    conn
    |> Plug.Conn.put_resp_header("retry-after", "60")
    |> Http.send_json(503, %{error: "store_forbidden"})
  end

  defp error_response(conn, {:unavailable, reason}, tenant_id, claim_id) do
    Logger.warning(
      "[ankusa] claim gateway: store unavailable reading #{tenant_id}/#{claim_id}: " <>
        inspect(reason, limit: 20, printable_limit: 512)
    )

    conn
    |> Plug.Conn.put_resp_header("retry-after", "1")
    |> Http.send_json(503, %{error: "store_unavailable"})
  end
end
