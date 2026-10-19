defmodule Ankusa.Edge.Router do
  @moduledoc """
  The Bandit edge. Minimal work on the hot path, in the order that keeps
  an unauthenticated caller from costing anything: resolve the catch URL
  (`Ankusa.RouteResolver`, default `POST /webhooks/:source_id`) to a
  `Ankusa.Route`, refuse on a `Content-Length` over the limit, look the source
  up (`Ankusa.Edge.Ingest.lookup/2`), refuse a `Content-Length` over the
  tenant's own `max_body_bytes` (`413`), charge the pre-read limits (per client
  address, per node, per source bytes: `429`, `Ankusa.Edge.RateLimiter.pre_read/4`),
  refuse header bytes no sink can carry (`400 invalid_header`), only then read
  the body (bounded in size by the tenant's cap, and in time by
  `listener.request_deadline_ms`: `408`), and hand off to `Ankusa.Edge.Ingest`.
  A body with no declared length is charged to its source once it has been read.
  A request refused at any step before the read is answered without its body
  being read. No JSON parsing here.

  The instance name is passed through `init/1` (`plug: {Ankusa.Edge.Router,
  instance: :default}`) and is available as `opts` inside each route.
  """

  use Plug.Router, copy_opts_to_assign: :ankusa_opts

  alias Ankusa.Edge.{Ingest, RateLimiter, RouteGuard}

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
    config = Ankusa.config(instance)
    max = config.max_body_bytes
    declared = declared_length(conn)

    with {:ok, route} <- resolve(conn, instance),
         :ok <- check_content_length(conn, instance, declared, max),
         {:ok, source, tenant_id} <- lookup(conn, instance, route, conn.assigns[:ankusa_route]),
         {:ok, cap} <- tenant_cap(conn, instance, tenant_id, declared, max),
         :ok <- pre_read(conn, instance, source, declared),
         :ok <- check_headers(conn, instance),
         {:ok, body, conn} <- read_body(conn, instance, config, cap),
         {:ok, conn} <- charge_read(conn, instance, source, declared, body) do
      respond(
        conn,
        Ingest.ingest(instance, source, tenant_id, raw_request(conn, source, body))
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

  # The route the guard matched, if routes are on: ingest reports it.
  defp route_id(%Plug.Conn{assigns: %{ankusa_route: %{id: id}}}), do: id
  defp route_id(%Plug.Conn{}), do: nil

  # The request the ingest path reads. `peer_cert` is a live call into the TLS
  # socket, so it is made for a source verified by `Ankusa.Verifier.ClientCert`
  # and nobody else; no certificate (plain HTTP, a transport that cannot answer)
  # is `nil`, which that verifier fails closed on.
  defp raw_request(conn, %Ankusa.Source{verifier: {verifier, _opts}}, body) do
    request = %{
      method: conn.method,
      path: conn.request_path,
      headers: conn.req_headers,
      query: conn.query_string,
      body: body,
      route_id: route_id(conn)
    }

    if verifier == Ankusa.Verifier.ClientCert,
      do: Map.put(request, :peer_cert, peer_cert(conn)),
      else: request
  end

  defp peer_cert(conn) do
    case Plug.Conn.get_peer_data(conn) do
      %{ssl_cert: der} when is_binary(der) -> der
      _ -> nil
    end
  rescue
    _ -> nil
  end

  # The declared body length, or `nil` for any other shape (absent, chunked,
  # malformed); Bandit already rejects malformed framing.
  defp declared_length(conn) do
    with [value] <- Plug.Conn.get_req_header(conn, "content-length"),
         {declared, ""} <- Integer.parse(value) do
      declared
    else
      _ -> nil
    end
  end

  # A declared length over the limit is refused on the header alone, so the
  # body is never pulled off the socket. Any other shape falls through to the
  # bounded read, which enforces the limit on the bytes actually received.
  defp check_content_length(conn, instance, declared, max) do
    if is_integer(declared) and declared > max,
      do: {:refused, too_large(conn, instance, max)},
      else: :ok
  end

  # The most this tenant's body may be: its own `max_body_bytes` when it has
  # one (never above the instance's), checked against the declared length now
  # and enforced on the bytes received by the read.
  defp tenant_cap(conn, instance, tenant_id, declared, max) do
    cap =
      case RateLimiter.body_limit(instance, tenant_id) do
        nil -> max
        limit -> min(limit, max)
      end

    with :ok <- check_content_length(conn, instance, declared, cap), do: {:ok, cap}
  end

  # Before the body: per client address, per node, per source. Each answers `429`
  # with its own error and `Retry-After`, counted as a refusal under its reason.
  defp pre_read(conn, instance, source, declared) do
    case RateLimiter.pre_read(instance, conn, source.id, declared) do
      :ok -> :ok
      {:error, {:ip, ms}} -> {:refused, limited(conn, instance, :ip_rate_limited, ms)}
      {:error, {:node, ms}} -> {:refused, limited(conn, instance, :node_rate_limited, ms)}
      {:error, {:source, ms}} -> {:refused, limited(conn, instance, :source_rate_limited, ms)}
    end
  end

  # A body with no declared length could not be charged to its source up front.
  defp charge_read(conn, _instance, _source, declared, _body) when is_integer(declared),
    do: {:ok, conn}

  defp charge_read(conn, instance, source, nil, body) do
    case RateLimiter.hit_source_bytes(instance, source.id, byte_size(body)) do
      :ok ->
        {:ok, conn}

      {:error, {:rate_limited, ms}} ->
        {:refused, limited(conn, instance, :source_rate_limited, ms)}
    end
  end

  # `lookup/3` has already counted the refusal.
  defp lookup(conn, instance, route, binding) do
    case Ingest.lookup(instance, route, binding) do
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

  defp read_body(conn, instance, config, cap) do
    case Ankusa.Http.read_body_limited(conn, cap, Ankusa.Http.read_opts(config)) do
      {:ok, body, conn} ->
        {:ok, body, conn}

      {:too_large, conn} ->
        {:refused, too_large(conn, instance, cap)}

      # The body did not arrive within `listener.request_deadline_ms`.
      {:error, :request_deadline, conn} ->
        {:refused, refuse(conn, instance, :body_read_failed, 408, %{error: "request_timeout"})}

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

  # A pre-read limit answered: `reason` is the refusal the metric counts, and
  # the error the sender sees is the same word.
  defp limited(conn, instance, reason, retry_after_ms) do
    Ingest.refused(instance, reason)

    conn
    |> retry_after(retry_after_ms)
    |> send_json(429, %{error: Atom.to_string(reason)})
  end

  defp retry_after(conn, retry_after_ms) do
    Plug.Conn.put_resp_header(
      conn,
      "retry-after",
      Integer.to_string(div(retry_after_ms + 999, 1000))
    )
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
    conn |> retry_after(retry_after_ms) |> send_json(429, %{error: "rate_limited"})
  end

  # The tenant holds its whole share of this node's batchers: its own `429`, so
  # the `503 overload` below stays for the tenants within theirs.
  defp respond(conn, {:error, {:tenant_overload, retry_after_ms}}) do
    conn |> retry_after(retry_after_ms) |> send_json(429, %{error: "tenant_overload"})
  end

  # Another node holds this provider event key and has not made its hook durable
  # yet (`dedupe.scope: :fleet`): not a duplicate (that is a `2xx`, and the hook
  # may still fail to commit), so the sender retries. A claim normally settles in
  # milliseconds, so the store's hint is a second at most.
  defp respond(conn, {:error, {:dedupe_pending, retry_after_ms}}) do
    conn |> retry_after(retry_after_ms) |> send_json(409, %{error: "dedupe_pending"})
  end

  defp respond(conn, {:error, {:quarantine_rate_limited, retry_after_ms}}) do
    conn |> retry_after(retry_after_ms) |> send_json(429, %{error: "quarantine_rate_limited"})
  end

  # The pen clears only by operator action (a release or a purge), so the hint
  # is a minute, not a second.
  defp respond(conn, {:error, :quarantine_full}) do
    conn
    |> Plug.Conn.put_resp_header("retry-after", "60")
    |> send_json(503, %{error: "quarantine_full"})
  end

  # The node already knows `tenants.max` tenants: this one is refused until an
  # operator raises the cap (or erases a tenant), so a short retry is no use.
  defp respond(conn, {:error, :tenant_limit}) do
    conn
    |> Plug.Conn.put_resp_header("retry-after", "300")
    |> send_json(503, %{error: "tenant_limit"})
  end

  # The node holds `dedupe.max_entries` live dedupe keys (or the tenant holds
  # `dedupe.max_entries_per_tenant`): the hook is refused rather than stored
  # without its key. Keys expire on their own, so a minute is a fair retry.
  defp respond(conn, {:error, :dedupe_quota}) do
    conn
    |> Plug.Conn.put_resp_header("retry-after", "60")
    |> send_json(503, %{error: "dedupe_quota"})
  end

  # The dead-letter queue is over `queue.dlq.max_rows`/`max_bytes` and has no row
  # whose hook is archived left to evict (`Ankusa.Queue.DlqReaper`): the node
  # cannot drain, and an acked hook is never deleted to make room, so a new hook
  # is refused until the archive catches up. A minute is a fair retry.
  defp respond(conn, {:error, :dlq_full}) do
    conn
    |> Plug.Conn.put_resp_header("retry-after", "60")
    |> send_json(503, %{error: "dlq_full"})
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
