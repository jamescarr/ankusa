defmodule Ankusa.ClaimCheck.Router do
  @moduledoc """
  The `:claim_check` role's HTTP API — the Remote proxy target for
  `Ankusa.ClaimCheck.Remote`, and the redemption surface for any non-BEAM
  consumer. Deliberately its own listener (`claim_check.port`, default
  `4001`), separate from `Ankusa.Edge.Router`: the edge is internet-facing,
  this is internal-network-only, and the edge's catch-all `POST` would
  swallow these routes anyway.

  Every `/v1/claims/*` request needs `authorization: Bearer <token>`, checked
  against `config.claim_check.api_tokens` — see `authenticate/2`. The server
  always calls `Ankusa.ClaimCheck` with `adapter: {Ankusa.ClaimCheck.Direct,
  opts}`, regardless of the instance's own configured adapter, so a
  `:claim_check` node never proxies to itself (`ClaimCheck.validate_config!/1`
  also rejects that config at boot).

  ## API

    * `PUT /v1/claims/:tenant_id/:id` — raw body; optional `content-type` and
      `x-ankusa-sha256`. `201 {"ticket": {...}}` on success.
    * `GET /v1/claims/:tenant_id/:id` — raw bytes,
      `content-type: application/octet-stream`.
    * `GET /health` — unauthenticated liveness check.
  """

  use Plug.Router, copy_opts_to_assign: :ankusa_opts

  alias Ankusa.ClaimCheck
  alias Ankusa.ClaimCheck.{Direct, Ticket}
  alias Ankusa.Http

  plug(:match)
  plug(:dispatch)

  get "/health" do
    Http.send_json(conn, 200, %{status: "ok"})
  end

  put "/v1/claims/:tenant_id/:id" do
    instance = instance(conn)

    with :ok <- validate_path(tenant_id, id),
         {:ok, scope} <- authenticate(conn, instance),
         :ok <- authorize(scope, tenant_id) do
      max = Ankusa.config(instance).claim_check.max_bytes

      case Http.read_body_limited(conn, max) do
        {:ok, body, conn} ->
          meta = %{tenant_id: tenant_id, id: id, content_type: content_type(conn)}
          check_in(conn, instance, body, meta, expect_sha256(conn))

        {:too_large, conn} ->
          Http.send_json(conn, 413, %{error: "payload_too_large", limit: max})
      end
    else
      {:error, reason} -> error_response(conn, reason)
    end
  end

  # No integrity check here on purpose: the server has no ground-truth
  # ticket for an inbound `GET /v1/claims/:tenant/:id` (the URL carries only
  # `tenant_id`/`id`, never `size`/`sha256`) — integrity is verified end to
  # end by the actual redeemer, in `Ankusa.ClaimCheck.redeem/3`, against the
  # real ticket *it* holds. This handler is pure byte transport.
  get "/v1/claims/:tenant_id/:id" do
    instance = instance(conn)

    with :ok <- validate_path(tenant_id, id),
         {:ok, scope} <- authenticate(conn, instance),
         :ok <- authorize(scope, tenant_id) do
      {mod, adapter_opts} = direct_adapter(instance)
      key_only = %Ticket{tenant_id: tenant_id, id: id, size: 0, sha256: ""}

      case mod.fetch(instance, key_only, adapter_opts) do
        {:ok, bin} ->
          conn
          |> Plug.Conn.put_resp_content_type("application/octet-stream", nil)
          |> Plug.Conn.send_resp(200, bin)

        {:error, reason} ->
          error_response(conn, reason)
      end
    else
      {:error, reason} -> error_response(conn, reason)
    end
  end

  match _ do
    Http.send_json(conn, 404, %{error: "not_found"})
  end

  # ── check-in ────────────────────────────────────────────────────────────

  defp check_in(conn, instance, body, meta, expect_sha256) do
    opts =
      [adapter: direct_adapter(instance)] ++
        if(expect_sha256, do: [expect_sha256: expect_sha256], else: [])

    case ClaimCheck.check_in(instance, body, meta, opts) do
      {:ok, ticket} -> Http.send_json(conn, 201, %{"ticket" => Ticket.to_map(ticket)})
      {:error, reason} -> error_response(conn, reason)
    end
  end

  defp direct_adapter(instance) do
    %Ankusa.Config{claim_check: %{adapter: {Direct, direct_opts}}} = Ankusa.config(instance)
    {Direct, direct_opts}
  end

  # ── auth ────────────────────────────────────────────────────────────────

  # A request hashes its presented token and does one map lookup against
  # sha256(token) => scope — raw tokens are never compared byte by byte, and
  # never logged.
  defp authenticate(conn, instance) do
    %Ankusa.Config{claim_check: %{api_tokens: tokens}} = Ankusa.config(instance)

    with ["Bearer " <> token] <- Plug.Conn.get_req_header(conn, "authorization"),
         hash = Base.encode16(:crypto.hash(:sha256, token), case: :lower),
         %{^hash => scope} <- hash_tokens(tokens) do
      {:ok, scope}
    else
      _ -> {:error, :unauthorized}
    end
  end

  defp hash_tokens(tokens) do
    Map.new(tokens, fn {token, scope} ->
      {Base.encode16(:crypto.hash(:sha256, token), case: :lower), scope}
    end)
  end

  defp authorize(:all, _tenant_id), do: :ok

  defp authorize(scopes, tenant_id) when is_list(scopes) do
    if tenant_id in scopes, do: :ok, else: {:error, :forbidden}
  end

  defp validate_path(tenant_id, id) do
    with :ok <- Ticket.validate_tenant(tenant_id), do: Ticket.validate_id(id)
  end

  # ── request bits ────────────────────────────────────────────────────────

  defp content_type(conn) do
    case Plug.Conn.get_req_header(conn, "content-type") do
      [ct | _] -> ct
      [] -> nil
    end
  end

  defp expect_sha256(conn) do
    case Plug.Conn.get_req_header(conn, "x-ankusa-sha256") do
      [sha | _] -> String.downcase(sha)
      [] -> nil
    end
  end

  defp instance(%Plug.Conn{} = conn) do
    Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
  end

  # ── error mapping ───────────────────────────────────────────────────────

  defp error_response(conn, :invalid_tenant),
    do: Http.send_json(conn, 400, %{error: "invalid_tenant"})

  defp error_response(conn, :invalid_id), do: Http.send_json(conn, 400, %{error: "invalid_id"})

  defp error_response(conn, :unauthorized),
    do: Http.send_json(conn, 401, %{error: "unauthorized"})

  defp error_response(conn, :forbidden),
    do: Http.send_json(conn, 403, %{error: "forbidden_tenant"})

  defp error_response(conn, :not_found), do: Http.send_json(conn, 404, %{error: "not_found"})

  defp error_response(conn, :too_large),
    do: Http.send_json(conn, 413, %{error: "payload_too_large"})

  defp error_response(conn, :integrity_mismatch),
    do: Http.send_json(conn, 422, %{error: "integrity_mismatch"})

  defp error_response(conn, {:unavailable, reason}) do
    conn
    |> Plug.Conn.put_resp_header("retry-after", "1")
    |> Http.send_json(503, %{error: "store_unavailable", reason: inspect(reason)})
  end

  defp error_response(conn, reason), do: Http.send_json(conn, 500, %{error: inspect(reason)})
end
