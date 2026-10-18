defmodule Ankusa.Edge.RouteGuard do
  @moduledoc """
  The ingress gate: with routes enabled, a `POST` is captured only if it passes
  the IP rules and matches an enabled route.

  `Ankusa.Edge.Router` calls this before anything else on the capture path, so a
  rejected request is never read, never verified, and never written to the store —
  it cannot become a record, a dispatch, or a delivery. It is also a plain
  `Plug`, so an embedder that owns its own Bandit pipeline can put it in front of
  whatever it already has.

  ## Responses

  | Situation | Status | Body |
  | --- | --- | --- |
  | No route matches | `404` | `{"error": "not_found"}` |
  | Path matches, method does not | `404` | `{"error": "not_found"}` |
  | IP rules deny | `403` | `{"error": "forbidden"}` |
  | IP rules deny, `routes.ip_denied_status: 404` | `404` | `{"error": "not_found"}` |

  A method rejection is a `404` like a path rejection, so a probe cannot learn
  which paths exist by comparing status codes. No `www-authenticate` or
  `retry-after` header is sent: there is nothing for the sender to do.

  One exception is the operator's to make. A sender denied by a *route's own*
  `ip_rules` gets `routes.ip_denied_status` (403 by default) while an unknown path
  gets `404`, so with the default it can tell that the path it hit is a route. A
  global IP denial answers every path alike and leaks nothing. Set
  `routes.ip_denied_status: 404` when that distinction matters.

  ## Senders retry on 4xx

  Some providers retry any non-2xx, some give up, some disable the endpoint after
  enough failures. Ankusa does not retry a rejected hook — it never accepted
  it — so the provider's own policy decides what happens next, and `403`/`404`
  are the only signals it gets. Dry-run a route change
  (`POST /admin/routes/test`) before rolling it out.

  ## Failing closed

  Routes enabled with no snapshot loaded (a store that never published one) is a
  rejection, not a pass: the alternative is capturing everything exactly when the
  allowlist is broken. The same goes for a conn whose peer address is nil —
  there is no address to match rules against, so there is nothing to allow.

  ## Telemetry

    * `[:ankusa, :routes, :match]` — metadata
      `%{instance:, route_id:, cached:, cacheable:}`. `:cached` is true when the
      decision came from `Ankusa.Routes.Cache`. `:cacheable` is false when the
      request path is past the cache's key bound (`Ankusa.Routes.Cache.cacheable?/1`),
      so every request for it is matched by a scan of the route table and none is
      ever a `cached: true`: the way to tell "a legitimate path that is too long
      to cache" from an ordinary miss.
    * `[:ankusa, :routes, :reject]` — metadata
      `%{instance:, reason:, method:, path:}` with `:reason` one of `:no_route`,
      `:method`, `:ip_denied`.

  Rejections are also logged at `:debug`, sampled at `routes.log_sample` (0
  disables it): a scanner hitting random paths must not fill a disk, but an
  operator debugging "why is this one provider failing" needs a line. The warning
  for a node with no route table loaded is sampled the same way, so a store that
  is slow to come up does not write one line per rejected hook.
  """

  @behaviour Plug

  require Logger

  alias Ankusa.Config
  alias Ankusa.Net
  alias Ankusa.Routes

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(%Plug.Conn{} = conn, opts) do
    instance = Keyword.get(opts, :instance, :default)

    # Off means the edge captures everything, as it always has: no snapshot
    # read, no config parse, no telemetry.
    if Routes.enabled?(instance) do
      guard(conn, instance, Ankusa.config(instance))
    else
      conn
    end
  end

  # ── decisions ───────────────────────────────────────────────────────────────

  defp guard(conn, instance, config) do
    case Routes.meta(instance) do
      nil ->
        if sampled?(config) do
          Logger.warning(
            "[ankusa] routes are enabled but no route table is loaded; rejecting " <>
              "#{conn.method} #{conn.request_path}"
          )
        end

        reject(conn, instance, config, :no_route)

      table ->
        case client_ip(conn, table) do
          {:ok, ip} ->
            decide(conn, instance, config, ip)

          :error ->
            # No peer address, or a header chain we do not believe: either way
            # there is nothing to check against the rules.
            reject(conn, instance, config, :ip_denied)
        end
    end
  end

  defp decide(conn, instance, config, ip) do
    {decision, cached} =
      Routes.authorize_path(instance, conn.method, conn.path_info, conn.request_path, ip)

    case decision do
      {:ok, route_id} ->
        Ankusa.Telemetry.emit([:routes, :match], %{}, %{
          instance: instance,
          route_id: route_id,
          cached: cached,
          cacheable: Ankusa.Routes.Cache.cacheable?(conn.path_info)
        })

        Plug.Conn.assign(conn, :ankusa_route, route_id)

      {:reject, reason} ->
        reject(conn, instance, config, reason)
    end
  end

  defp reject(conn, instance, config, reason) do
    Ankusa.Telemetry.emit([:routes, :reject], %{}, %{
      instance: instance,
      reason: reason,
      method: conn.method,
      path: conn.request_path
    })

    log_reject(config, conn, reason)

    conn
    |> respond(config, reason)
    |> Plug.Conn.halt()
  end

  defp respond(conn, _config, reason) when reason in [:no_route, :method],
    do: Ankusa.Http.send_json(conn, 404, %{error: "not_found"})

  defp respond(conn, %Config{routes: %{ip_denied_status: 403}}, :ip_denied),
    do: Ankusa.Http.send_json(conn, 403, %{error: "forbidden"})

  defp respond(conn, %Config{}, :ip_denied),
    do: Ankusa.Http.send_json(conn, 404, %{error: "not_found"})

  # ── client address ──────────────────────────────────────────────────────────

  defp client_ip(conn, table), do: Net.ClientIP.resolve(conn, table.trusted_proxies)

  # ── logging ─────────────────────────────────────────────────────────────────

  defp log_reject(config, conn, reason) do
    if sampled?(config) do
      Logger.debug(
        "[ankusa] route_reject reason=#{reason} method=#{conn.method} path=#{conn.request_path}"
      )
    end
  end

  # One in `routes.log_sample`, drawn independently for every call: a scanner must
  # not fill a disk, but an operator needs a line. `0` never logs.
  defp sampled?(%Config{routes: %{log_sample: sample}}),
    do: sample > 0 and :rand.uniform(sample) == 1
end
