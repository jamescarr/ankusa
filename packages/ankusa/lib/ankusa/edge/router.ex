defmodule Ankusa.Edge.Router do
  @moduledoc """
  The Bandit edge. Minimal work on the hot path, in the order that keeps
  an unauthenticated caller from costing anything: resolve the catch URL
  (`Ankusa.RouteResolver`, default `POST /webhooks/:source_id`) to a
  `Ankusa.Route`, refuse on a `Content-Length` over the limit, look the source
  up (`Ankusa.Edge.Ingest.lookup/2`), refuse header bytes no sink can carry
  (`400 invalid_header`), only then read the body (bounded), and
  hand off to `Ankusa.Edge.Ingest`. A request refused at any step before the
  read is answered without its body being read. No JSON parsing here.

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

  # Readiness, not liveness: 503 while this node's store cannot take a synced
  # write (a full disk). See `Ankusa.Health`.
  get "/ready" do
    case Ankusa.Health.ready(instance(conn)) do
      {:ok, body} ->
        send_json(conn, 200, body)

      {:error, body} ->
        conn |> Plug.Conn.put_resp_header("retry-after", "1") |> send_json(503, body)
    end
  end

  match _ do
    send_json(conn, 404, %{error: "not_found"})
  end

  # ── capture ───────────────────────────────────────────────────────────────

  defp capture(conn, instance) do
    with {:ok, route} <- resolve(conn, instance),
         max = Ankusa.config(instance).max_body_bytes,
         :ok <- check_content_length(conn, instance, max),
         {:ok, source, tenant_id} <- lookup(conn, instance, route),
         :ok <- check_headers(conn, instance),
         {:ok, body, conn} <- read_body(conn, instance, max) do
      respond(
        conn,
        Ingest.ingest(instance, source, tenant_id, %{
          method: conn.method,
          path: conn.request_path,
          headers: conn.req_headers,
          body: body
        })
      )
    else
      {:refused, %Plug.Conn{} = conn} -> conn
    end
  end

  defp resolve(conn, instance) do
    case Ankusa.RouteResolver.resolve(instance, conn) do
      {:ok, route} ->
        {:ok, route}

      :error ->
        {:refused, refuse(conn, instance, :unknown_source, 404, %{error: "unknown_source"})}
    end
  end

  # A declared length over the limit is refused on the header alone, so the
  # body is never pulled off the socket. Any other shape (absent, chunked,
  # malformed) falls through to the bounded read, which enforces the limit on
  # the bytes actually received; Bandit already rejects malformed framing.
  defp check_content_length(conn, instance, max) do
    with [value] <- Plug.Conn.get_req_header(conn, "content-length"),
         {declared, ""} when declared > max <- Integer.parse(value) do
      {:refused, too_large(conn, instance, max)}
    else
      _ -> :ok
    end
  end

  # `lookup/2` has already counted the refusal.
  defp lookup(conn, instance, route) do
    case Ingest.lookup(instance, route) do
      {:ok, source, tenant_id} ->
        {:ok, source, tenant_id}

      {:error, :unknown_source} ->
        {:refused, send_json(conn, 404, %{error: "unknown_source"})}

      # The source store could not answer: the provider retries.
      {:error, :store_unavailable} ->
        {:refused,
         conn
         |> Plug.Conn.put_resp_header("retry-after", "1")
         |> send_json(503, %{error: "store_unavailable"})}
    end
  end

  defp check_headers(conn, instance) do
    case Ingest.check_headers(conn.req_headers) do
      :ok ->
        :ok

      {:invalid_header, name} ->
        {:refused,
         refuse(conn, instance, :invalid_header, 400, %{error: "invalid_header", header: name})}
    end
  end

  defp read_body(conn, instance, max) do
    case Ankusa.Http.read_body_limited(conn, max) do
      {:ok, body, conn} ->
        {:ok, body, conn}

      {:too_large, conn} ->
        {:refused, too_large(conn, instance, max)}

      {:error, reason, conn} ->
        {:refused,
         refuse(conn, instance, :body_read_failed, 400, %{
           error: "body_read_failed",
           reason: inspect(reason)
         })}
    end
  end

  defp too_large(conn, instance, max),
    do: refuse(conn, instance, :payload_too_large, 413, %{error: "payload_too_large", limit: max})

  defp refuse(conn, instance, reason, status, payload) do
    Ingest.refused(instance, reason)
    send_json(conn, status, payload)
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

  defp respond(conn, {:error, {:quarantine_rate_limited, retry_after_ms}}) do
    conn
    |> Plug.Conn.put_resp_header(
      "retry-after",
      Integer.to_string(div(retry_after_ms + 999, 1000))
    )
    |> send_json(429, %{error: "quarantine_rate_limited"})
  end

  # The pen clears only by operator action (a release or a purge), so the hint
  # is a minute, not a second.
  defp respond(conn, {:error, :quarantine_full}) do
    conn
    |> Plug.Conn.put_resp_header("retry-after", "60")
    |> send_json(503, %{error: "quarantine_full"})
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
