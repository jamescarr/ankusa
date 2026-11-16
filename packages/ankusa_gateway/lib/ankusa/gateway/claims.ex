defmodule Ankusa.Gateway.Claims do
  @moduledoc """
  The claims API (`priv/openapi/claim_check.v1.yaml`): read-only byte
  transport for claim-check refs, routed by `Ankusa.Gateway.Router`.

    * `GET /v1/claims/:tenant_id/:claim_id` — the claim's exact bytes,
      `application/octet-stream`, `Cache-Control: private, max-age=31536000,
      immutable` (claims are written once and never rewritten; `private`
      because the path is a capability, and a shared cache would serve it to
      whoever asks), `Accept-Ranges: bytes`. `400` malformed, `404` no such
      claim, `503 store_unavailable` + `Retry-After` (a random 1–3 seconds)
      when the store is unavailable, throttling, or slower than the deadline,
      `503 store_forbidden` + `Retry-After: 60` when the store refused this
      node's credential. A 503 body never carries the store's error; it is
      logged. `502 integrity_mismatch` when the bytes read (or the pack's
      manifest) are not what the manifest records.
    * The same `GET` with one `Range` (`Ankusa.Gateway.Range`) — `206` with
      those bytes and `Content-Range: bytes a-b/length`, read from the store as
      one ranged read; an unsatisfiable range is `416` with `Content-Range:
      bytes */length`.
    * `HEAD` — the same status and headers, `content-length` included, without
      the body; it reads only the pack's index. `Range` is ignored.

  ## Order

  Authorization first, before anything is read: the request is
  `%{kind: :claim, space: "claims", key: "<tenant_id>/<claim_id>", tenant:
  tenant_id}` for `:read`, decided by `gateway.claims.policy` (`403
  {"error":"forbidden"}`, with `scope` when the deciding step named one; `503
  authz_unavailable` when a step could not decide). Then the ref's proof: a
  v2 ref is redeemed at the same path with `?exp=<exp_unix>&sig=<signature>`
  and held to `Ankusa.ClaimCheck.verify_ref/2` (`400 bad_signature`, `410
  ref_expired`, `400 unsigned_ref`). Then a slot among
  `gateway.max_concurrent_requests` (`503 overloaded` + `Retry-After`). Then
  the read.

  ## Integrity

  With `gateway.claims.integrity_check: :manifest` (the default) a whole-claim
  `GET` is checked against the sha256 the pack's manifest records for that
  claim (`Ankusa.ClaimCheck.digest/3`): bytes that don't match are `502
  integrity_mismatch`, never sent. A `200` or `206` carries the digest as
  `x-ankusa-sha256`; `HEAD` reads only the index, so it sends none. `:off`
  serves the store's bytes as they are, with no header.

  ## Under load

  Every claim request answers within `claim_check.read_deadline_ms`. Its reads
  go through `Ankusa.ClaimCheck.Cache` and `Ankusa.ClaimCheck.Reader`. Every
  answer past authentication emits `[:ankusa, :gateway, :request]` with `api:
  :claims`, its method, status and duration: a policy's `403` and a
  fail-closed `503 authz_unavailable` are counted beside the reads (a `401`
  is the authentication plug's, before any of it).

  With `gateway.claims.enabled: false` every claim route is `404`.
  """

  alias Ankusa.{ClaimCheck, Http, Telemetry}
  alias Ankusa.ClaimCheck.{Reader, Ref}
  alias Ankusa.Gateway.{Authz, Range}
  alias Ankusa.Gateway.Authz.Request

  @cache_control "private, max-age=31536000, immutable"

  @doc "Serve `GET` or `HEAD` of one claim."
  @spec serve(Plug.Conn.t(), String.t(), String.t()) :: Plug.Conn.t()
  def serve(conn, tenant_id, claim_id) do
    started = System.monotonic_time()
    instance = instance(conn)
    gateway = Ankusa.Gateway.config(instance)

    conn =
      if gateway.claims.enabled do
        authorized(conn, instance, gateway, tenant_id, claim_id)
      else
        Http.send_json(conn, 404, %{error: "not_found"})
      end

    Telemetry.emit(
      [:gateway, :request],
      %{duration: System.monotonic_time() - started},
      %{instance: instance, api: :claims, method: conn.method, status: conn.status}
    )

    conn
  end

  defp authorized(conn, instance, gateway, tenant_id, claim_id) do
    resource = %{
      kind: :claim,
      space: "claims",
      key: tenant_id <> "/" <> claim_id,
      tenant: tenant_id
    }

    request = Request.from_conn(conn, :read, resource)

    case Authz.decide(instance, gateway.claims.policy, request) do
      {:allow, _meta} ->
        serve_claim(conn, instance, gateway, tenant_id, claim_id)

      {:deny, meta} ->
        Http.send_json(conn, 403, forbidden(meta))

      {:error, {:unavailable, _reason}} ->
        conn
        |> Plug.Conn.put_resp_header("retry-after", Integer.to_string(Enum.random(1..3)))
        |> Http.send_json(503, %{error: "authz_unavailable"})
    end
  end

  defp forbidden(%{scope: scope}) when is_binary(scope), do: %{error: "forbidden", scope: scope}
  defp forbidden(_meta), do: %{error: "forbidden"}

  defp serve_claim(conn, instance, gateway, tenant_id, claim_id) do
    config = Ankusa.config(instance)
    proven_and_admitted(conn, config, gateway, instance, tenant_id, claim_id)
  end

  # The ref's proof first (cheap, and a forged ref must not take a slot), then
  # a slot among `gateway.max_concurrent_requests`, held until the response is
  # sent.
  defp proven_and_admitted(conn, config, gateway, instance, tenant_id, claim_id) do
    with :ok <- check_ref(conn, config, tenant_id, claim_id),
         :ok <- admit(instance, gateway) do
      try do
        serve(conn, gateway, instance, tenant_id, claim_id, ClaimCheck.deadline(config))
      after
        Reader.release(instance)
      end
    else
      {:error, reason} -> error_response(conn, reason, tenant_id, claim_id)
    end
  end

  defp admit(instance, gateway) do
    case Reader.admit(instance, gateway.max_concurrent_requests) do
      :ok -> :ok
      :shed -> {:error, :overloaded}
    end
  end

  # A v2 ref's `exp` and `sig` ride the query string, `?exp=<unix>&sig=<mac>`
  # (`Ankusa.ClaimCheck.Ref.path/1`); with neither, the path is a v1 ref. Only
  # one of the two, or a value out of the grammar, is `:bad_signature`.
  defp check_ref(conn, config, tenant_id, claim_id) do
    %{query_params: params} = Plug.Conn.fetch_query_params(conn)
    check_ref(config, tenant_id, claim_id, params["exp"], params["sig"])
  end

  defp check_ref(config, tenant_id, claim_id, nil, nil),
    do: ClaimCheck.verify_ref(config, %Ref{tenant_id: tenant_id, claim_id: claim_id})

  defp check_ref(config, tenant_id, claim_id, exp, sig) do
    with {:ok, ref} <- Ref.signed(tenant_id, claim_id, exp, sig) do
      ClaimCheck.verify_ref(config, ref)
    end
  end

  # A HEAD response carries the claim's length and no body: the server keeps a
  # `content-length` the plug set when the body it is handed is empty.
  defp serve(%Plug.Conn{method: "HEAD"} = conn, _gateway, instance, tenant_id, claim_id, deadline) do
    case ClaimCheck.locate(instance, tenant_id, claim_id, deadline: deadline) do
      {:ok, location} ->
        conn
        |> claim_headers()
        |> Plug.Conn.put_resp_header("content-length", Integer.to_string(location.length))
        |> Plug.Conn.send_resp(200, "")

      {:error, reason} ->
        error_response(conn, reason, tenant_id, claim_id)
    end
  end

  defp serve(conn, gateway, instance, tenant_id, claim_id, deadline) do
    with {:ok, location} <- ClaimCheck.locate(instance, tenant_id, claim_id, deadline: deadline),
         {:ok, digest} <- digest(instance, gateway, location, deadline),
         range = conn |> Range.parse() |> Range.resolve(location.length),
         {:ok, bin} <- read_range(instance, location, range, deadline),
         :ok <- verify(range, bin, digest) do
      conn |> put_digest(range, digest) |> send_range(range, bin, location.length)
    else
      {:error, reason} -> error_response(conn, reason, tenant_id, claim_id)
    end
  end

  defp digest(_instance, %{claims: %{integrity_check: :off}}, _location, _deadline),
    do: {:ok, nil}

  defp digest(instance, _gateway, location, deadline),
    do: ClaimCheck.digest(instance, location, deadline: deadline)

  # Only a whole claim is checked here; a range's digest is the whole claim's,
  # sent in the header for whoever reassembles them.
  defp verify(:all, bin, digest) when is_binary(digest) do
    if ClaimCheck.sha256(bin) == digest, do: :ok, else: {:error, :integrity_mismatch}
  end

  defp verify(_range, _bin, _digest), do: :ok

  defp put_digest(conn, :unsatisfiable, _digest), do: conn
  defp put_digest(conn, _range, nil), do: conn

  defp put_digest(conn, _range, digest),
    do: Plug.Conn.put_resp_header(conn, "x-ankusa-sha256", digest)

  defp read_range(instance, location, :all, deadline),
    do: ClaimCheck.read_bytes(instance, location, 0, location.length, deadline: deadline)

  defp read_range(_instance, _location, :unsatisfiable, _deadline), do: {:ok, <<>>}

  defp read_range(instance, location, {first, last}, deadline),
    do: ClaimCheck.read_bytes(instance, location, first, last - first + 1, deadline: deadline)

  # The body is served as application/octet-stream (claim_headers/1), never HTML.
  # sobelow_skip ["XSS.SendResp"]
  defp send_range(conn, :all, bin, _length),
    do: conn |> claim_headers() |> Plug.Conn.send_resp(200, bin)

  defp send_range(conn, :unsatisfiable, _bin, length) do
    conn
    |> Plug.Conn.put_resp_header("accept-ranges", "bytes")
    |> Plug.Conn.put_resp_header("content-range", "bytes */#{length}")
    |> Plug.Conn.send_resp(416, "")
  end

  # The body is served as application/octet-stream (claim_headers/1), never HTML.
  # sobelow_skip ["XSS.SendResp"]
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

  defp instance(%Plug.Conn{} = conn),
    do: Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)

  defp error_response(conn, :invalid_tenant, _tenant_id, _claim_id),
    do: Http.send_json(conn, 400, %{error: "invalid_tenant"})

  defp error_response(conn, :invalid_id, _tenant_id, _claim_id),
    do: Http.send_json(conn, 400, %{error: "invalid_id"})

  # A forged or mangled proof and a refused v1 ref are the caller's bug, and a
  # ref past its expiry is gone for good: none is worth a retry.
  defp error_response(conn, :bad_signature, _tenant_id, _claim_id),
    do: Http.send_json(conn, 400, %{error: "bad_signature"})

  defp error_response(conn, :unsigned_ref, _tenant_id, _claim_id),
    do: Http.send_json(conn, 400, %{error: "unsigned_ref"})

  defp error_response(conn, :ref_expired, _tenant_id, _claim_id),
    do: Http.send_json(conn, 410, %{error: "ref_expired"})

  # This node is at `gateway.max_concurrent_requests`: expected under load,
  # counted on `[:ankusa, :gateway, :request]` by its status.
  defp error_response(conn, :overloaded, _tenant_id, _claim_id) do
    conn
    |> Plug.Conn.put_resp_header("retry-after", Integer.to_string(Enum.random(1..3)))
    |> Http.send_json(503, %{error: "overloaded"})
  end

  # A store that failed: the answers the objects API gives too.
  defp error_response(conn, reason, _tenant_id, _claim_id),
    do: Ankusa.Gateway.Http.store_error(conn, reason)
end
