import createClient from "openapi-fetch";

import { AdminRejectedError, AdminUnavailableError, RoleNotEnabledError } from "./errors.js";
import type { components, paths } from "./admin-schema.d.ts";

export type AdminClientOptions = {
  /** The operator listener (`admin.port`), e.g. `http://localhost:4002`. */
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
   * `timeout=10.0`. Exceeding it rejects with `AdminUnavailableError`.
   */
  timeoutMs?: number;
  /** Override for testing; defaults to the global `fetch`. */
  fetch?: typeof fetch;
};

export type Health = components["schemas"]["Health"];
export type DlqPage = components["schemas"]["DlqPage"];
export type QuarantinePage = components["schemas"]["QuarantinePage"];
export type Replayed = components["schemas"]["Replayed"];
export type ReplayFilter = components["schemas"]["ReplayFilter"];

export type ListDeadLettersParams = NonNullable<paths["/v1/dlq"]["get"]["parameters"]["query"]>;
export type ListQuarantinedParams = NonNullable<paths["/v1/quarantine"]["get"]["parameters"]["query"]>;

export type AdminClient = {
  /** Liveness probe: `GET /health`, `{status, instance, roles}` on the operator listener. */
  health(): Promise<Health>;
  /** The node's Prometheus scrape, as text. */
  metrics(): Promise<string>;
  /** The effective, redacted configuration. */
  config(): Promise<Record<string, unknown>>;
  /** List dead-lettered hooks (newest first, metadata only). */
  listDeadLetters(params?: ListDeadLettersParams): Promise<DlqPage>;
  /** Re-deliver matching dead-lettered hooks; returns how many were replayed. */
  replayDeadLetters(filter?: ReplayFilter): Promise<Replayed>;
  /** Recent quarantined hooks (newest first, metadata only). */
  listQuarantined(params?: ListQuarantinedParams): Promise<QuarantinePage>;
};

type FetchResult = { data?: unknown; error?: unknown; response: Response };

type ErrorBody = {
  error?: string;
  role?: string;
};

export function createAdminClient(options: AdminClientOptions): AdminClient {
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
      throw new AdminUnavailableError(`admin gateway unreachable: ${(err as Error).message}`, err);
    }
    const { data, error, response } = result;
    const status = response.status;
    if (status >= 200 && status < 300) {
      return data as T;
    }
    // An empty error body arrives as `undefined`; the Python client reports it
    // as "".
    const body = error === undefined ? "" : error;
    if (status >= 500) {
      throw new AdminUnavailableError(`admin gateway error (${status}): ${JSON.stringify(body)}`);
    }
    const e = (body && typeof body === "object" ? body : {}) as ErrorBody;
    if (status === 409 && e.error === "role_not_enabled") {
      throw new RoleNotEnabledError(`role not enabled${e.role ? `: ${e.role}` : ""}`, e.role);
    }
    throw new AdminRejectedError(`admin rejected (${status}): ${JSON.stringify(body)}`, status, e.error);
  }

  async function health(): Promise<Health> {
    try {
      const { data, response } = await http.GET("/health", { signal: AbortSignal.timeout(timeoutMs) });
      if (response.status !== 200 || data === undefined) {
        throw new AdminUnavailableError(`admin gateway health check failed (${response.status})`);
      }
      return data as Health;
    } catch (err) {
      if (err instanceof AdminUnavailableError) throw err;
      throw new AdminUnavailableError(`admin gateway unreachable: ${(err as Error).message}`, err);
    }
  }

  async function metrics(): Promise<string> {
    return request<string>(() =>
      http.GET("/metrics", { parseAs: "text", signal: AbortSignal.timeout(timeoutMs) }),
    );
  }

  async function config(): Promise<Record<string, unknown>> {
    return request<Record<string, unknown>>(() =>
      http.GET("/v1/config", { signal: AbortSignal.timeout(timeoutMs) }),
    );
  }

  async function listDeadLetters(params?: ListDeadLettersParams): Promise<DlqPage> {
    return request<DlqPage>(() =>
      http.GET("/v1/dlq", { params: { query: params }, signal: AbortSignal.timeout(timeoutMs) }),
    );
  }

  async function replayDeadLetters(filter?: ReplayFilter): Promise<Replayed> {
    return request<Replayed>(() =>
      http.POST("/v1/dlq/replay", { body: filter, signal: AbortSignal.timeout(timeoutMs) }),
    );
  }

  async function listQuarantined(params?: ListQuarantinedParams): Promise<QuarantinePage> {
    return request<QuarantinePage>(() =>
      http.GET("/v1/quarantine", { params: { query: params }, signal: AbortSignal.timeout(timeoutMs) }),
    );
  }

  return { health, metrics, config, listDeadLetters, replayDeadLetters, listQuarantined };
}
