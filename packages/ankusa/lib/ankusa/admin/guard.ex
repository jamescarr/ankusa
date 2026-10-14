defmodule Ankusa.Admin.Guard do
  @moduledoc """
  The plug that authenticates, authorizes and audits a request to the admin or
  route-management listener. It runs after the router matched and before it
  dispatches (`plug(:match)`, `plug(Guard, listener: ...)`, `plug(:dispatch)`),
  so a refused request never reaches a handler.

  For each request:

    1. `Ankusa.Admin.Actions.classify/3` names the action, the role it needs
       and its scope. It is given the path the router matched on (percent
       decoded), never the raw one: a guard that disagreed with the router
       about what a path is could be walked around with `%73`.
    2. `:public` actions (`/health`, `/ready`) pass untouched.
    3. A SCIM action (`/scim/v2/...`) needs `scim.token` and nothing else; with
       `scim.enabled: false` it is a `404`.
    4. Any other action: with `auth.enabled: false`, everyone is the anonymous
       principal that holds every grant (the listeners stay open, as before
       auth existed). With auth on, the bearer token is verified
       (`Ankusa.Auth.Verifier`), resolved to a principal (`Ankusa.Auth`), and
       the principal's grants are checked against the action.
    5. With `audit.enabled`, every denial is recorded, and every mutation is
       recorded `pending` before the handler runs (refused with
       `503 audit_unavailable` if that write fails) and completed from the
       response (`Ankusa.Audit`).

  Refusals:

  | status | `error` | when |
  | --- | --- | --- |
  | `401` | `unauthenticated` | no bearer token, or not a `Bearer` header |
  | `401` | `invalid_token` | the verifier rejected the token |
  | `403` | `forbidden` (+ `required_role`) | the principal's grants do not cover the action |
  | `403` | `user_not_provisioned` / `user_inactive` | the token matches no active SCIM user |
  | `503` | `auth_unavailable` | the verifier cannot decide; a good token is not discarded |
  | `503` | `audit_unavailable` | the audit entry of a mutation could not be written |

  Every `401`/`403`/`503` it sends for authentication reasons emits
  `[:ankusa, :auth, :denied]` with the `:reason`. The conn carries the result in
  `conn.private`: `:ankusa_action`, `:ankusa_principal`.

  A handler that crashes is answered here too (`crashed/4`, called from each
  router's `dispatch/2`): a `500` JSON body, and the request's audit entry
  finished as an `error`.
  """

  @behaviour Plug

  import Plug.Conn

  alias Ankusa.{Audit, Auth, Telemetry}
  alias Ankusa.Admin.Actions
  alias Ankusa.Auth.Principal

  @impl true
  def init(opts) do
    case Keyword.fetch!(opts, :listener) do
      listener when listener in [:admin, :routes] -> listener
      other -> raise ArgumentError, "listener must be :admin or :routes, got: #{inspect(other)}"
    end
  end

  @impl true
  def call(conn, listener) do
    instance = Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
    config = Ankusa.config(instance)
    info = Actions.classify(listener, conn.method, Plug.Router.Utils.decode_path_info!(conn))

    conn =
      conn
      |> put_private(:ankusa_instance, instance)
      |> put_private(:ankusa_listener, listener)
      |> put_private(:ankusa_action, info)

    ctx = %{instance: instance, config: config, info: info, ip: Audit.ip(conn)}

    case info.role do
      :public -> conn
      :scim -> scim(conn, ctx)
      _role -> user(conn, ctx)
    end
  end

  # ── SCIM client ────────────────────────────────────────────────────────────

  defp scim(conn, %{config: %{scim: %{enabled: false}}}) do
    conn |> Ankusa.Http.send_json(404, %{error: "not_found"}) |> halt()
  end

  defp scim(conn, %{config: %{scim: %{token: expected}}} = ctx) do
    case bearer(conn) do
      {:ok, token} when is_binary(expected) ->
        if Plug.Crypto.secure_compare(token, expected) do
          authorized(conn, ctx, %Principal{type: :scim, subject: "scim", ip: ctx.ip})
        else
          deny_scim(conn, ctx)
        end

      _ ->
        deny_scim(conn, ctx)
    end
  end

  defp deny_scim(conn, ctx) do
    refuse(conn, ctx, :scim_token, 401, fn conn ->
      conn
      |> put_resp_header("www-authenticate", ~s(Bearer realm="ankusa-scim"))
      |> Ankusa.Scim.Errors.send(401, nil, "A valid SCIM bearer token is required")
    end)
  end

  # ── users ──────────────────────────────────────────────────────────────────

  defp user(conn, %{config: %{auth: %{enabled: false}}} = ctx),
    do: authorized(conn, ctx, Auth.anonymous(ctx.ip))

  defp user(conn, ctx) do
    case bearer(conn) do
      {:ok, token} ->
        case Auth.authenticate(ctx.instance, token, ctx.ip) do
          {:ok, principal} -> authorized(conn, ctx, principal)
          {:error, :invalid_token} -> invalid_token(conn, ctx)
          {:error, :unavailable} -> unavailable(conn, ctx)
          {:error, reason} -> not_provisioned(conn, ctx, reason)
        end

      :error ->
        refuse(conn, ctx, :unauthenticated, 401, fn conn ->
          conn
          |> put_resp_header("www-authenticate", ~s(Bearer realm="ankusa"))
          |> Ankusa.Http.send_json(401, %{error: "unauthenticated"})
        end)
    end
  end

  defp invalid_token(conn, ctx) do
    refuse(conn, ctx, :invalid_token, 401, fn conn ->
      conn
      |> put_resp_header("www-authenticate", ~s(Bearer realm="ankusa", error="invalid_token"))
      |> Ankusa.Http.send_json(401, %{error: "invalid_token"})
    end)
  end

  defp unavailable(conn, ctx) do
    refuse(conn, ctx, :auth_unavailable, 503, fn conn ->
      conn
      |> put_resp_header("retry-after", "1")
      |> Ankusa.Http.send_json(503, %{error: "auth_unavailable"})
    end)
  end

  defp not_provisioned(conn, ctx, reason)
       when reason in [:user_not_provisioned, :user_inactive] do
    refuse(conn, ctx, reason, 403, &Ankusa.Http.send_json(&1, 403, %{error: to_string(reason)}))
  end

  # ── authorization and audit ────────────────────────────────────────────────

  defp authorized(conn, ctx, principal) do
    conn = put_private(conn, :ankusa_principal, principal)

    if principal.type == :scim or Auth.allowed?(principal, ctx.info.role, ctx.info.scope) do
      audit(conn, ctx)
    else
      refuse(conn, ctx, :forbidden, 403, fn conn ->
        Ankusa.Http.send_json(conn, 403, %{
          error: "forbidden",
          required_role: to_string(ctx.info.role)
        })
      end)
    end
  end

  defp audit(conn, %{config: %{audit: %{enabled: true}}, info: %{mutates: true}}) do
    case Audit.begin(conn) do
      {:ok, conn} ->
        register_before_send(conn, &Audit.finish/1)

      {:error, _reason} ->
        conn
        |> put_resp_header("retry-after", "1")
        |> Ankusa.Http.send_json(503, %{error: "audit_unavailable"})
        |> halt()
    end
  end

  defp audit(conn, _ctx), do: conn

  # A refusal: counted, recorded when it is a `401`/`403` and auditing is on
  # (before the response goes out, so a client that saw it can read it back),
  # then sent by `send_fun`.
  defp refuse(conn, ctx, reason, status, send_fun) do
    Telemetry.emit([:auth, :denied], %{}, %{instance: ctx.instance, reason: reason})

    if status in [401, 403] and ctx.config.audit.enabled do
      Audit.record(conn, "denied", status)
    end

    conn |> send_fun.() |> halt()
  end

  @doc """
  Answer a request whose handler crashed, then re-raise so the crash is still
  logged. `conn` is the conn `dispatch` was given, the guard's, so its
  `before_send` finishes the request's audit entry; marked `:ankusa_audit_error`
  it is finished as `error`. A crash Plug maps to a client error (a body that
  cannot be read) keeps that status, and is not an audit error. Nothing is sent
  when the response already went out.
  """
  @spec crashed(Plug.Conn.t(), :error | :exit | :throw, term(), Exception.stacktrace()) ::
          no_return()
  def crashed(conn, kind, reason, stacktrace) do
    receive do
      {:plug_conn, :sent} -> send(self(), {:plug_conn, :sent})
    after
      0 -> respond_to_crash(conn, kind, reason, stacktrace)
    end

    :erlang.raise(kind, reason, stacktrace)
  end

  defp respond_to_crash(conn, kind, reason, stacktrace) do
    {inner_kind, inner, inner_stack} = unwrap(kind, reason, stacktrace)
    status = crash_status(inner_kind, inner, inner_stack)

    if status >= 500 do
      conn
      |> put_private(:ankusa_audit_error, true)
      |> Ankusa.Http.send_json(status, %{error: "internal_error"})
    else
      Ankusa.Http.send_json(conn, status, %{error: "bad_request"})
    end
  end

  defp unwrap(:error, %Plug.Conn.WrapperError{kind: kind, reason: reason, stack: stack}, _stack),
    do: {kind, reason, stack}

  defp unwrap(kind, reason, stack), do: {kind, reason, stack}

  defp crash_status(:error, error, stack),
    do: error |> then(&Exception.normalize(:error, &1, stack)) |> Plug.Exception.status()

  defp crash_status(_kind, _reason, _stack), do: 500

  defp bearer(conn) do
    with [header | _] <- get_req_header(conn, "authorization"),
         [scheme, token] <- String.split(header, " ", parts: 2),
         "bearer" <- String.downcase(scheme),
         token = String.trim(token),
         true <- token != "" do
      {:ok, token}
    else
      _ -> :error
    end
  end
end
