import createClient from "openapi-fetch";

import {
  InvalidRouteIdError,
  RouteNotFoundError,
  RoutesRejectedError,
  RoutesUnavailableError,
} from "./errors.js";
import type { components, paths } from "../admin/admin-schema.d.ts";

export type RoutesClientOptions = {
  /** The route-management listener (`routes.admin.port`), e.g. `http://localhost:4003`. */
  baseUrl: string;
  /**
   * Headers attached to every request. The listener itself does no auth (see
   * `packages/ankusa/priv/openapi/admin.v1.yaml`) — this is for whatever a
   * deployer's own boundary (service mesh, Envoy, an API gateway) expects in
   * front of it.
   */
  headers?: HeadersInit;
  /**
   * Per-request deadline in milliseconds, covering connect through the last
   * body byte. Defaults to `10_000`, matching the Python client's
   * `timeout=10.0`. Exceeding it rejects with `RoutesUnavailableError`.
   */
  timeoutMs?: number;
  /** Override for testing; defaults to the global `fetch`. */
  fetch?: typeof fetch;
};

export type Route = components["schemas"]["Route"];
export type RoutePage = components["schemas"]["RoutePage"];
export type RoutePatch = components["schemas"]["RoutePatch"];
export type IpRule = components["schemas"]["IpRule"];
export type IpRules = components["schemas"]["IpRules"];
export type DryRunRequest = components["schemas"]["DryRunRequest"];
export type DryRunResult = components["schemas"]["DryRunResult"];
export type RoutesHealth = components["schemas"]["RoutesHealth"];

/**
 * A route definition as written. Only `path` is required — the listener
 * defaults `methods` (`[POST]`), `enabled` (`true`), `ip_rules` (`[]`), and
 * `metadata` (`{}`). `id` is optional on create; on `PUT` the path's `id` wins.
 */
export type RouteInput = Omit<
  components["schemas"]["RouteInput"],
  "methods" | "enabled" | "ip_rules" | "metadata"
> &
  Partial<Pick<components["schemas"]["RouteInput"], "methods" | "enabled" | "ip_rules" | "metadata">>;

export type ListRoutesParams = NonNullable<paths["/admin/routes"]["get"]["parameters"]["query"]>;

export type RoutesClient = {
  /** Liveness probe: `GET /health`, `{status, routes}` on the route-management listener. */
  health(): Promise<RoutesHealth>;
  /** List route definitions (ordered by id, paginated). */
  listRoutes(params?: ListRoutesParams): Promise<RoutePage>;
  /** Create a route definition. */
  createRoute(input: RouteInput): Promise<Route>;
  /**
   * Fetch one route definition. An id that is not a string, is empty, or is
   * `.`/`..` raises `InvalidRouteIdError` before any request.
   */
  getRoute(id: string): Promise<Route>;
  /**
   * Replace a route definition (`PUT`-idempotent; creates if absent). An id
   * that is not a string, is empty, or is `.`/`..` raises
   * `InvalidRouteIdError` before any request.
   */
  replaceRoute(id: string, input: RouteInput): Promise<Route>;
  /**
   * Patch a route definition (`enabled`, `methods`, `ip_rules`, `metadata`
   * only). An id that is not a string, is empty, or is `.`/`..` raises
   * `InvalidRouteIdError` before any request.
   */
  updateRoute(id: string, patch: RoutePatch): Promise<Route>;
  /**
   * Delete a route definition. An id that is not a string, is empty, or is
   * `.`/`..` raises `InvalidRouteIdError` before any request.
   */
  deleteRoute(id: string): Promise<void>;
  /** The global IP rules. */
  getIpRules(): Promise<IpRules>;
  /** Replace the global IP rules. */
  putIpRules(rules: IpRules): Promise<IpRules>;
  /** Dry-run a request against the route table. */
  testRoute(request: DryRunRequest): Promise<DryRunResult>;
};

type FetchResult = { data?: unknown; error?: unknown; response: Response };

type RejectedBody = {
  error?: string;
  field?: string;
  message?: string;
  conflicting_id?: string;
  max_routes?: number;
};

/**
 * Ids a URL parser normalizes away: `..` resolves to the collection endpoint,
 * and an empty id or `.` resolves to it too, so the request would fetch the
 * route *list* as if it were one route.
 */
const INVALID_ROUTE_IDS: Record<string, true> = { "": true, ".": true, "..": true };

function assertRouteId(id: unknown): void {
  if (typeof id !== "string" || INVALID_ROUTE_IDS[id] === true) {
    throw new InvalidRouteIdError(id);
  }
}

export function createRoutesClient(options: RoutesClientOptions): RoutesClient {
  const timeoutMs = options.timeoutMs ?? 10_000;
  const http = createClient<paths>({
    baseUrl: options.baseUrl,
    headers: options.headers,
    fetch: options.fetch,
    // A 3xx is a gateway error, not a hop: the Python client doesn't follow
    // redirects either.
    redirect: "manual",
  });

  async function request<T>(call: () => Promise<FetchResult>): Promise<T> {
    let result: FetchResult;
    try {
      result = await call();
    } catch (err) {
      throw new RoutesUnavailableError(`routes gateway unreachable: ${(err as Error).message}`, err);
    }
    const { data, error, response } = result;
    const status = response.status;
    if (status >= 200 && status < 300) {
      return data as T;
    }
    // An empty error body arrives as `undefined`; the Python client reports it
    // as "".
    const body = error === undefined ? "" : error;
    if (status === 404) {
      throw new RouteNotFoundError("route not found");
    }
    // Only a `4xx` is the listener rejecting the request. An unfollowed `3xx`
    // (or a `1xx`) means the caller never reached the listener, same as a
    // `5xx`.
    if (status >= 400 && status < 500) {
      throw rejected(status, body);
    }
    throw new RoutesUnavailableError(`routes gateway error (${status}): ${JSON.stringify(body)}`);
  }

  function rejected(status: number, body: unknown): RoutesRejectedError {
    if (body && typeof body === "object") {
      const e = body as RejectedBody;
      return new RoutesRejectedError(status, e.error, e.field, e.message, e.conflicting_id, e.max_routes);
    }
    return new RoutesRejectedError(status);
  }

  async function health(): Promise<RoutesHealth> {
    try {
      const { data, response } = await http.GET("/health", { signal: AbortSignal.timeout(timeoutMs) });
      if (response.status !== 200 || data === undefined) {
        throw new RoutesUnavailableError(`routes gateway health check failed (${response.status})`);
      }
      return data as RoutesHealth;
    } catch (err) {
      if (err instanceof RoutesUnavailableError) throw err;
      throw new RoutesUnavailableError(`routes gateway unreachable: ${(err as Error).message}`, err);
    }
  }

  async function listRoutes(params?: ListRoutesParams): Promise<RoutePage> {
    return request<RoutePage>(() =>
      http.GET("/admin/routes", { params: { query: params }, signal: AbortSignal.timeout(timeoutMs) }),
    );
  }

  async function createRoute(input: RouteInput): Promise<Route> {
    return request<Route>(() =>
      http.POST("/admin/routes", { body: input as components["schemas"]["RouteInput"], signal: AbortSignal.timeout(timeoutMs) }),
    );
  }

  async function getRoute(id: string): Promise<Route> {
    assertRouteId(id);
    return request<Route>(() =>
      http.GET("/admin/routes/{id}", { params: { path: { id } }, signal: AbortSignal.timeout(timeoutMs) }),
    );
  }

  async function replaceRoute(id: string, input: RouteInput): Promise<Route> {
    assertRouteId(id);
    return request<Route>(() =>
      http.PUT("/admin/routes/{id}", {
        params: { path: { id } },
        body: input as components["schemas"]["RouteInput"],
        signal: AbortSignal.timeout(timeoutMs),
      }),
    );
  }

  async function updateRoute(id: string, patch: RoutePatch): Promise<Route> {
    assertRouteId(id);
    return request<Route>(() =>
      http.PATCH("/admin/routes/{id}", {
        params: { path: { id } },
        body: patch,
        signal: AbortSignal.timeout(timeoutMs),
      }),
    );
  }

  async function deleteRoute(id: string): Promise<void> {
    assertRouteId(id);
    await request<void>(() =>
      http.DELETE("/admin/routes/{id}", { params: { path: { id } }, signal: AbortSignal.timeout(timeoutMs) }),
    );
  }

  async function getIpRules(): Promise<IpRules> {
    return request<IpRules>(() =>
      http.GET("/admin/ip-rules", { signal: AbortSignal.timeout(timeoutMs) }),
    );
  }

  async function putIpRules(rules: IpRules): Promise<IpRules> {
    return request<IpRules>(() =>
      http.PUT("/admin/ip-rules", { body: rules, signal: AbortSignal.timeout(timeoutMs) }),
    );
  }

  async function testRoute(req: DryRunRequest): Promise<DryRunResult> {
    return request<DryRunResult>(() =>
      http.POST("/admin/routes/test", { body: req, signal: AbortSignal.timeout(timeoutMs) }),
    );
  }

  return {
    health,
    listRoutes,
    createRoute,
    getRoute,
    replaceRoute,
    updateRoute,
    deleteRoute,
    getIpRules,
    putIpRules,
    testRoute,
  };
}
