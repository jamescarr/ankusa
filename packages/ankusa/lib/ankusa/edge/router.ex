defmodule Ankusa.Edge.Router do
  @moduledoc """
  The Bandit edge. Minimal work on the hot path: enforce a size limit, capture
  the exact raw body and headers, and hand off to `Ankusa.Edge.Ingest`. No JSON
  parsing here. The catch-URL scheme is pluggable via `Ankusa.RouteResolver`
  (default `POST /webhooks/:source_id`); it maps the request to a `Ankusa.Route`.

  The instance name is passed through `init/1` (`plug: {Ankusa.Edge.Router,
  instance: :default}`) and is available as `opts` inside each route.
  """

  use Plug.Router, copy_opts_to_assign: :ankusa_opts

  alias Ankusa.Edge.{Ingest, RouteGuard}

  plug(:match)
  plug(:dispatch)

  match "/*_glob", via: :post do
    instance = instance(conn)

    # The route guard runs before anything else on the capture path: a request it
    # rejects is never read, verified, or written to the store. With routes off it
    # returns the conn untouched.
    case RouteGuard.call(conn, instance: instance) do
      %Plug.Conn{halted: true} = conn -> conn
      conn -> capture(conn, instance)
    end
  end

  get "/health" do
    send_json(conn, 200, %{status: "ok", instance: to_string(instance(conn))})
  end

  match _ do
    send_json(conn, 404, %{error: "not_found"})
  end

  # ── capture ───────────────────────────────────────────────────────────────

  defp capture(conn, instance) do
    case Ankusa.RouteResolver.resolve(instance, conn) do
      {:ok, route} ->
        max = Ankusa.config(instance).max_body_bytes

        case Ankusa.Http.read_body_limited(conn, max) do
          {:ok, body, conn} ->
            req = %{
              source_id: route.source_id,
              tenant_id: route.tenant_id,
              method: conn.method,
              path: conn.request_path,
              headers: conn.req_headers,
              body: body
            }

            conn |> respond(Ingest.ingest(instance, req))

          {:too_large, conn} ->
            send_json(conn, 413, %{error: "payload_too_large", limit: max})

          {:error, reason, conn} ->
            send_json(conn, 400, %{error: "body_read_failed", reason: inspect(reason)})
        end

      :error ->
        send_json(conn, 404, %{error: "unknown_source"})
    end
  end

  # ── response mapping ──────────────────────────────────────────────────────

  defp respond(conn, {:ok, env}),
    do: send_json(conn, 201, %{status: "accepted", id: env.id})

  defp respond(conn, {:duplicate, env}),
    do: send_json(conn, 201, %{status: "accepted", id: env.id, duplicate: true})

  defp respond(conn, {:quarantined, reason}),
    do: send_json(conn, 202, %{status: "quarantined", reason: inspect(reason)})

  defp respond(conn, {:rejected, reason}),
    do: send_json(conn, 401, %{error: "verification_failed", reason: inspect(reason)})

  defp respond(conn, {:error, :unknown_source}),
    do: send_json(conn, 404, %{error: "unknown_source"})

  defp respond(conn, {:error, {:rate_limited, retry_after_ms}}) do
    conn
    |> Plug.Conn.put_resp_header(
      "retry-after",
      Integer.to_string(div(retry_after_ms + 999, 1000))
    )
    |> send_json(429, %{error: "rate_limited"})
  end

  defp respond(conn, {:error, reason}) when reason in [:overload, :store_unavailable] do
    conn
    |> Plug.Conn.put_resp_header("retry-after", "1")
    |> send_json(503, %{error: to_string(reason)})
  end

  # ── helpers ───────────────────────────────────────────────────────────────

  defp instance(%Plug.Conn{} = conn) do
    Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
  end

  defp send_json(conn, status, payload), do: Ankusa.Http.send_json(conn, status, payload)
end
