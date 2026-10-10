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
      whoever asks), `Accept-Ranges: bytes`. `400` malformed, `404` no such
      claim, `503 store_unavailable` + `Retry-After: 1` when the store is
      unavailable, `503 store_forbidden` + `Retry-After: 60` when the store
      refused this node's credential. A 503 body never carries the store's
      error; it is logged.
    * The same `GET` with one `Range: bytes=a-b`, `bytes=a-` or `bytes=-n` —
      `206` with those bytes and `Content-Range: bytes a-b/length` (`b`
      clamped to the claim's end), read from the store as one ranged read; a
      range that starts past the end (or a zero-length claim, or `bytes=-0`)
      is `416` with `Content-Range: bytes */length`. Several ranges, another
      unit, a malformed spec, or any `If-Range` are ignored: `200` with the
      whole claim.
    * `HEAD /v1/claims/:tenant_id/:claim_id` — the same status and headers,
      `content-length` included, without the body; it reads only the pack's
      index, never the claim. `Range` is ignored.
    * `GET /health` — liveness.

  No integrity check happens here: the path carries no digest. The reader
  checks the bytes against the sha256 its queue message carried
  (`Ankusa.ClaimCheck.redeem/3` does this in-process). The whole response is
  one binary, not a stream: a claim is at most `max_body_bytes`.
  """

  use Plug.Router, copy_opts_to_assign: :ankusa_opts

  require Logger

  alias Ankusa.{ClaimCheck, Http}

  @cache_control "private, max-age=31536000, immutable"
  # One range-spec of the `bytes` unit: `first-last`, `first-`, or `-suffix`.
  @range_spec ~r/\A(\d*)-(\d*)\z/

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

  # A HEAD response carries the claim's length and no body: the server keeps a
  # `content-length` the plug set when the body it is handed is empty.
  defp claim(%Plug.Conn{method: "HEAD"} = conn, tenant_id, claim_id) do
    case ClaimCheck.locate(instance(conn), tenant_id, claim_id) do
      {:ok, location} ->
        conn
        |> claim_headers()
        |> Plug.Conn.put_resp_header("content-length", Integer.to_string(location.length))
        |> Plug.Conn.send_resp(200, "")

      {:error, reason} ->
        error_response(conn, reason, tenant_id, claim_id)
    end
  end

  defp claim(conn, tenant_id, claim_id) do
    instance = instance(conn)

    with {:ok, location} <- ClaimCheck.locate(instance, tenant_id, claim_id),
         range = range(conn, location.length),
         {:ok, bin} <- read_range(instance, location, range) do
      send_range(conn, range, bin, location.length)
    else
      {:error, reason} -> error_response(conn, reason, tenant_id, claim_id)
    end
  end

  # `:all`, `{first, last}` (inclusive, inside the claim) or `:unsatisfiable`.
  # Only a single `bytes` range is served; anything else is ignored, which RFC
  # 9110 §14.2 allows. Claims have no validator to compare an `If-Range`
  # against, so one present means the whole claim.
  defp range(conn, length) do
    with [] <- Plug.Conn.get_req_header(conn, "if-range"),
         ["bytes=" <> spec] <- Plug.Conn.get_req_header(conn, "range") do
      spec |> String.trim() |> parse_range() |> resolve_range(length)
    else
      _ -> :all
    end
  end

  defp parse_range(spec) do
    case Regex.run(@range_spec, spec, capture: :all_but_first) do
      ["", ""] -> :invalid
      ["", suffix] -> {:suffix, String.to_integer(suffix)}
      [first, ""] -> {:from, String.to_integer(first)}
      [first, last] -> span(String.to_integer(first), String.to_integer(last))
      nil -> :invalid
    end
  end

  defp span(first, last) when last >= first, do: {:span, first, last}
  defp span(_first, _last), do: :invalid

  defp resolve_range(:invalid, _length), do: :all
  defp resolve_range(_spec, 0), do: :unsatisfiable
  defp resolve_range({:suffix, 0}, _length), do: :unsatisfiable
  defp resolve_range({:suffix, n}, length), do: {max(length - n, 0), length - 1}
  defp resolve_range({:from, first}, length) when first < length, do: {first, length - 1}

  defp resolve_range({:span, first, last}, length) when first < length,
    do: {first, min(last, length - 1)}

  defp resolve_range(_spec, _length), do: :unsatisfiable

  defp read_range(instance, location, :all),
    do: ClaimCheck.read_bytes(instance, location, 0, location.length)

  defp read_range(_instance, _location, :unsatisfiable), do: {:ok, <<>>}

  defp read_range(instance, location, {first, last}),
    do: ClaimCheck.read_bytes(instance, location, first, last - first + 1)

  defp send_range(conn, :all, bin, _length),
    do: conn |> claim_headers() |> Plug.Conn.send_resp(200, bin)

  defp send_range(conn, :unsatisfiable, _bin, length) do
    conn
    |> Plug.Conn.put_resp_header("accept-ranges", "bytes")
    |> Plug.Conn.put_resp_header("content-range", "bytes */#{length}")
    |> Plug.Conn.send_resp(416, "")
  end

  defp send_range(conn, {first, last}, bin, length) do
    conn
    |> claim_headers()
    |> Plug.Conn.put_resp_header("content-range", "bytes #{first}-#{last}/#{length}")
    |> Plug.Conn.send_resp(206, bin)
  end

  defp claim_headers(conn) do
    conn
    |> Plug.Conn.put_resp_content_type("application/octet-stream", nil)
    |> Plug.Conn.put_resp_header("cache-control", @cache_control)
    |> Plug.Conn.put_resp_header("accept-ranges", "bytes")
  end

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
