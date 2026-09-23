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

  alias Ankusa.Edge.Ingest

  plug(:match)
  plug(:dispatch)

  match "/*_glob", via: :post do
    instance = instance(conn)

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

  get "/health" do
    instance = instance(conn)
    stats = safe_stats(instance)
    send_json(conn, 200, %{status: "ok", instance: to_string(instance), wal: stats})
  end

  get "/stats" do
    instance = instance(conn)
    send_json(conn, 200, %{instance: to_string(instance), wal: safe_stats(instance)})
  end

  match _ do
    send_json(conn, 404, %{error: "not_found"})
  end

  # ── response mapping ──────────────────────────────────────────────────────

  defp respond(conn, {:ok, env}),
    do: send_json(conn, 201, %{status: "accepted", id: env.id, seq: env.seq})

  defp respond(conn, {:duplicate, env}),
    do: send_json(conn, 200, %{status: "duplicate", id: env.id, seq: env.seq})

  defp respond(conn, {:quarantined, reason}),
    do: send_json(conn, 202, %{status: "quarantined", reason: inspect(reason)})

  defp respond(conn, {:rejected, reason}),
    do: send_json(conn, 401, %{error: "verification_failed", reason: inspect(reason)})

  defp respond(conn, {:error, :unknown_source}),
    do: send_json(conn, 404, %{error: "unknown_source"})

  defp respond(conn, {:error, reason}) when reason in [:overload, :store_unavailable] do
    conn
    |> Plug.Conn.put_resp_header("retry-after", "1")
    |> send_json(503, %{error: to_string(reason)})
  end

  # ── helpers ───────────────────────────────────────────────────────────────

  defp safe_stats(instance) do
    Ankusa.WAL.stats(instance)
  rescue
    _ -> %{}
  catch
    :exit, _ -> %{}
  end

  defp instance(%Plug.Conn{} = conn) do
    Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
  end

  defp send_json(conn, status, payload), do: Ankusa.Http.send_json(conn, status, payload)
end
