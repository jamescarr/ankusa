defmodule Ankusa.ClaimCheck.Remote do
  @moduledoc """
  `Ankusa.ClaimCheck` adapter that talks HTTP to a `:claim_check`-role
  `Ankusa.ClaimCheck.Router`, via `Req` — mirroring `Ankusa.Sink.Http`. Pure
  transport: the facade already built and validated the ticket; this just moves
  bytes over the wire and maps HTTP status codes back to
  `Ankusa.ClaimCheck.reason()`.

  For callers that must not hold blob-store credentials: a non-BEAM
  consumer, or an Ankusa node deliberately isolated from the store. Per
  `architecture.md`'s "no component may require another to be reachable at
  runtime" rule, this adapter belongs only on retryable paths downstream of
  the WAL (dispatch sinks, external consumers) — never the edge's pre-ack
  check-in path.

  opts:

    * `:url`         — required, e.g. `"http://claim-check.internal:4001"`
    * `:token`        — required bearer token
    * `:timeout_ms`  — default `10_000`
    * `:req_options` — transport options for the HTTP client (custom Finch pool,
                       proxy, or `plug:` for `Req.Test` in tests). See
                       `Ankusa.HttpClient` for the accepted keys.
  """

  @behaviour Ankusa.ClaimCheck

  alias Ankusa.HttpClient

  alias Ankusa.ClaimCheck.Ticket

  @impl true
  def store(_instance, %Ticket{} = ticket, data, opts) do
    url = ticket_url(opts, ticket)
    bin = IO.iodata_to_binary(data)
    content_type = ticket.content_type || "application/octet-stream"

    case request(opts, :put, url, bin, [{"x-ankusa-sha256", ticket.sha256}], content_type) do
      {:ok, 201, body} ->
        with {:ok, %{"ticket" => map}} <- JSON.decode(body),
             {:ok, returned} <- Ticket.from_map(map) do
          verify_returned(returned, ticket)
        else
          _ -> {:error, {:unavailable, :invalid_response}}
        end

      {:ok, status, body} ->
        map_error(status, body)

      {:error, reason} ->
        {:error, {:unavailable, reason}}
    end
  end

  @impl true
  def fetch(_instance, %Ticket{} = ticket, opts) do
    url = ticket_url(opts, ticket)

    case request(opts, :get, url, nil, []) do
      {:ok, 200, body} -> {:ok, body}
      {:ok, status, body} -> map_error(status, body)
      {:error, reason} -> {:error, {:unavailable, reason}}
    end
  end

  # `content_type` is advisory only (see `Ticket`'s moduledoc) and, over a
  # real PUT, may legitimately come back different from what was sent — HTTP
  # requires *some* Content-Type, so a `nil` ticket content_type can't
  # round-trip byte-for-byte. Only the fields that actually define the claim
  # matter here.
  defp verify_returned(returned, ticket) do
    if returned.tenant_id == ticket.tenant_id and returned.id == ticket.id and
         returned.size == ticket.size and returned.sha256 == ticket.sha256 do
      :ok
    else
      {:error, :integrity_mismatch}
    end
  end

  # ── wire ────────────────────────────────────────────────────────────────

  defp ticket_url(opts, %Ticket{tenant_id: tenant_id, id: id}) do
    base = Keyword.fetch!(opts, :url)
    "#{base}/v1/claims/#{URI.encode(tenant_id, &URI.char_unreserved?/1)}/#{id}"
  end

  # `content_type` is only supplied for `:put` — the router requires *some*
  # Content-Type on a body, but a GET carries none.
  defp request(opts, method, url, body, extra_headers, content_type \\ nil) do
    token = Keyword.fetch!(opts, :token)
    timeout = Keyword.get(opts, :timeout_ms, 10_000)

    headers =
      [{"authorization", "Bearer #{token}"} | extra_headers] ++
        if content_type, do: [{"content-type", content_type}], else: []

    HttpClient.request(method, url, headers, body, timeout, Keyword.get(opts, :req_options, []))
  end

  defp map_error(400, body), do: {:error, error_atom(body, :invalid_tenant)}
  defp map_error(401, _body), do: {:error, :unauthorized}
  defp map_error(403, _body), do: {:error, :forbidden}
  defp map_error(404, _body), do: {:error, :not_found}
  defp map_error(413, _body), do: {:error, :too_large}
  defp map_error(422, _body), do: {:error, :integrity_mismatch}
  defp map_error(status, body), do: {:error, {:unavailable, {:status, status, body}}}

  defp error_atom(body, default) do
    case JSON.decode(body) do
      {:ok, %{"error" => "invalid_id"}} -> :invalid_id
      {:ok, %{"error" => "invalid_tenant"}} -> :invalid_tenant
      _ -> default
    end
  end
end
