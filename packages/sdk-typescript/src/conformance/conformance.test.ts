import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import http from "node:http";
import { describe, test } from "node:test";

import * as sdk from "../index.js";

// The language-neutral vectors live at the repo root; this file sits one
// directory deep so the `npm test` glob (`src/**/*.test.ts`, expanded by a
// shell without globstar) matches it. See conformance/README.md.

type Body = { text: string } | { base64: string } | { json: unknown };
type Client = { headers?: Record<string, string>; timeout_ms?: number; transport?: "injected" };
type Gateway =
  | { unreachable: true }
  | { status: number; headers?: Record<string, string>; body?: Body; delay_ms?: number };
type ExpectedRequest = { method: string; path: string; headers?: Record<string, string>; body?: unknown };
type Expect = { ok?: unknown; error?: { class: string; [key: string]: unknown }; requests?: ExpectedRequest[] };
type Case = {
  id: string;
  feature: string;
  operation: string;
  input: Record<string, unknown>;
  expect: Expect;
};
type Recorded = {
  method: string;
  path: string;
  headers: Record<string, string | string[] | undefined>;
  /** The request body parsed as JSON; `null` when there was none. */
  body: unknown;
};

const DIR = new URL("../../../../conformance/", import.meta.url);

const CASES: Case[] = readdirSync(new URL("cases/", DIR))
  .filter((name) => name.endsWith(".json"))
  .sort()
  .flatMap(
    (name) => (JSON.parse(readFileSync(new URL(`cases/${name}`, DIR), "utf8")) as { cases: Case[] }).cases,
  );

// An empty directory, or one this file doesn't resolve to, must fail loudly:
// zero cases would otherwise report a green run.
assert.ok(CASES.length > 0, `no conformance cases found under ${DIR.href}`);

// A namespace import, so an export that doesn't exist yet reads as `undefined`
// and fails only its own cases instead of the whole file. This also makes the
// runner a public-export check.
const ERROR_CLASSES: Record<string, unknown> = {
  InvalidClaimRefError: sdk.InvalidClaimRefError,
  ClaimNotFoundError: sdk.ClaimNotFoundError,
  ClaimRejectedError: sdk.ClaimRejectedError,
  ClaimIntegrityError: sdk.ClaimIntegrityError,
  ClaimCheckUnavailableError: sdk.ClaimCheckUnavailableError,
  MissingHookIdError: sdk.MissingHookIdError,
  RoutesError: sdk.RoutesError,
  RoutesUnavailableError: sdk.RoutesUnavailableError,
  RouteNotFoundError: sdk.RouteNotFoundError,
  InvalidRouteIdError: sdk.InvalidRouteIdError,
  RoutesRejectedError: sdk.RoutesRejectedError,
  AdminError: sdk.AdminError,
  AdminUnavailableError: sdk.AdminUnavailableError,
  RoleNotEnabledError: sdk.RoleNotEnabledError,
  AdminRejectedError: sdk.AdminRejectedError,
};

function bodyBytes(body: Body | undefined): Buffer {
  if (body === undefined) return Buffer.alloc(0);
  if ("text" in body) return Buffer.from(body.text);
  if ("base64" in body) return Buffer.from(body.base64, "base64");
  if ("json" in body) return Buffer.from(JSON.stringify(body.json));
  throw new Error(`unknown Body: ${JSON.stringify(body)}`);
}

function isUnreachable(spec: Gateway): spec is { unreachable: true } {
  return "unreachable" in spec;
}

type Built = { client: unknown; close: () => Promise<void> };

type ClientFactory = (baseUrl: string, client: Client, fetchImpl?: typeof fetch) => unknown;

function makeClaimCheckClient(baseUrl: string, client: Client, fetchImpl?: typeof fetch): sdk.ClaimCheckClient {
  return sdk.createClaimCheckClient({
    baseUrl,
    headers: client.headers,
    timeoutMs: client.timeout_ms,
    fetch: fetchImpl,
  });
}

function makeRoutesClient(baseUrl: string, client: Client, fetchImpl?: typeof fetch): sdk.RoutesClient {
  return sdk.createRoutesClient({
    baseUrl,
    headers: client.headers,
    timeoutMs: client.timeout_ms,
    fetch: fetchImpl,
  });
}

function makeAdminClient(baseUrl: string, client: Client, fetchImpl?: typeof fetch): sdk.AdminClient {
  return sdk.createAdminClient({
    baseUrl,
    headers: client.headers,
    timeoutMs: client.timeout_ms,
    fetch: fetchImpl,
  });
}

/** A real HTTP server standing in for a deployment's gateway: it answers every
 * request with `spec` and records what it saw.
 */
async function startGateway(spec: Gateway, requests: Recorded[]): Promise<{ baseUrl: string; close: () => Promise<void> }> {
  if (isUnreachable(spec)) {
    return { baseUrl: "http://127.0.0.1:1", close: async () => {} };
  }

  const bytes = bodyBytes(spec.body);
  const timers: NodeJS.Timeout[] = [];
  const server = http.createServer((req, res) => {
    const record: Recorded = {
      method: req.method ?? "GET",
      path: req.url ?? "",
      headers: req.headers,
      body: null,
    };
    requests.push(record);
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => {
      const text = Buffer.concat(chunks).toString("utf8");
      record.body = text === "" ? null : JSON.parse(text);
    });
    // A client that has already timed out may have closed the connection.
    res.on("error", () => {});
    const respond = () => {
      res.writeHead(spec.status, { ...(spec.headers ?? {}), "content-length": String(bytes.length) });
      res.end(bytes);
    };
    // A real delay: the `*.client.timeout` vectors need a response that
    // outlives the client's deadline, which deterministic fake timers can't
    // drive across a socket.
    if (spec.delay_ms) timers.push(setTimeout(respond, spec.delay_ms));
    else respond();
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (typeof address !== "object" || address === null) throw new Error("no server address");
  return {
    baseUrl: `http://127.0.0.1:${address.port}`,
    close: async () => {
      for (const timer of timers) clearTimeout(timer);
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
    },
  };
}

/** The injected-transport hook: serves `spec` in-process and records requests,
 * with no server at all.
 */
function injectedFetch(spec: Gateway, requests: Recorded[]): typeof fetch {
  const bytes = bodyBytes(isUnreachable(spec) ? undefined : spec.body);
  const status = isUnreachable(spec) ? 200 : spec.status;
  const headers = isUnreachable(spec) ? undefined : spec.headers;
  return (async (input: RequestInfo | URL) => {
    const request = input instanceof Request ? input : new Request(input);
    // Reading the body does not forward it anywhere: this transport serves the
    // spec in-process.
    const text = await request.text();
    // `path` is the request target as sent, query string included: the same thing
    // the real-server transport records (`req.url`) and the Python runner records
    // (`raw_path`), so a vector asserts a query the same way whichever carries it.
    const url = new URL(request.url);
    requests.push({
      method: request.method,
      path: url.pathname + url.search,
      headers: Object.fromEntries(request.headers),
      body: text === "" ? null : JSON.parse(text),
    });
    return new Response(bytes, {
      status,
      headers: { ...(headers ?? {}), "content-length": String(bytes.length) },
    });
  }) as typeof fetch;
}

async function buildClient(
  gateway: Gateway,
  client: Client,
  requests: Recorded[],
  make: ClientFactory,
): Promise<Built> {
  if (client.transport === "injected") {
    return { client: make("http://gateway.invalid", client, injectedFetch(gateway, requests)), close: async () => {} };
  }
  const started = await startGateway(gateway, requests);
  return { client: make(started.baseUrl, client), close: started.close };
}

async function dispatch(c: Case, requests: Recorded[]): Promise<unknown> {
  const input = c.input as {
    ref?: string;
    sha256?: string;
    headers?: Record<string, string>;
    client?: Client;
    gateway?: Gateway;
    id?: string;
    params?: Record<string, unknown>;
    input?: Record<string, unknown>;
    patch?: Record<string, unknown>;
    rules?: Record<string, unknown>;
    request?: Record<string, unknown>;
    filter?: Record<string, unknown>;
  };
  const gateway = input.gateway as Gateway;
  const client = input.client ?? {};
  switch (c.operation) {
    case "parse_claim_ref": {
      const parsed = sdk.parseClaimRef(input.ref as string);
      return { tenant_id: parsed.tenantId, claim_id: parsed.claimId, path: parsed.path };
    }
    case "parse_headers": {
      const parsed = sdk.parseHeaders(input.headers ?? {});
      return {
        id: parsed.id,
        source: parsed.source,
        tenant: parsed.tenant,
        content_type: parsed.contentType,
      };
    }
    case "redeem": {
      const built = await buildClient(gateway, client, requests, makeClaimCheckClient);
      try {
        const bytes = await (built.client as sdk.ClaimCheckClient).redeem(input.ref as string, input.sha256 as string);
        return { body: { base64: bytes.toString("base64") } };
      } finally {
        await built.close();
      }
    }
    case "health": {
      const built = await buildClient(gateway, client, requests, makeClaimCheckClient);
      try {
        return await (built.client as sdk.ClaimCheckClient).health();
      } finally {
        await built.close();
      }
    }
    case "routes_health": {
      const built = await buildClient(gateway, client, requests, makeRoutesClient);
      try {
        return await (built.client as sdk.RoutesClient).health();
      } finally {
        await built.close();
      }
    }
    case "routes_list": {
      const built = await buildClient(gateway, client, requests, makeRoutesClient);
      try {
        return await (built.client as sdk.RoutesClient).listRoutes(input.params as sdk.ListRoutesParams);
      } finally {
        await built.close();
      }
    }
    case "routes_create": {
      const built = await buildClient(gateway, client, requests, makeRoutesClient);
      try {
        return await (built.client as sdk.RoutesClient).createRoute(input.input as sdk.RouteInput);
      } finally {
        await built.close();
      }
    }
    case "routes_get": {
      const built = await buildClient(gateway, client, requests, makeRoutesClient);
      try {
        return await (built.client as sdk.RoutesClient).getRoute(input.id as string);
      } finally {
        await built.close();
      }
    }
    case "routes_replace": {
      const built = await buildClient(gateway, client, requests, makeRoutesClient);
      try {
        return await (built.client as sdk.RoutesClient).replaceRoute(input.id as string, input.input as sdk.RouteInput);
      } finally {
        await built.close();
      }
    }
    case "routes_update": {
      const built = await buildClient(gateway, client, requests, makeRoutesClient);
      try {
        return await (built.client as sdk.RoutesClient).updateRoute(input.id as string, input.patch as sdk.RoutePatch);
      } finally {
        await built.close();
      }
    }
    case "routes_delete": {
      const built = await buildClient(gateway, client, requests, makeRoutesClient);
      try {
        await (built.client as sdk.RoutesClient).deleteRoute(input.id as string);
        return null;
      } finally {
        await built.close();
      }
    }
    case "routes_ip_rules_get": {
      const built = await buildClient(gateway, client, requests, makeRoutesClient);
      try {
        return await (built.client as sdk.RoutesClient).getIpRules();
      } finally {
        await built.close();
      }
    }
    case "routes_ip_rules_put": {
      const built = await buildClient(gateway, client, requests, makeRoutesClient);
      try {
        return await (built.client as sdk.RoutesClient).putIpRules(input.rules as sdk.IpRules);
      } finally {
        await built.close();
      }
    }
    case "routes_test": {
      const built = await buildClient(gateway, client, requests, makeRoutesClient);
      try {
        return await (built.client as sdk.RoutesClient).testRoute(input.request as sdk.DryRunRequest);
      } finally {
        await built.close();
      }
    }
    case "admin_health": {
      const built = await buildClient(gateway, client, requests, makeAdminClient);
      try {
        return await (built.client as sdk.AdminClient).health();
      } finally {
        await built.close();
      }
    }
    case "admin_metrics": {
      const built = await buildClient(gateway, client, requests, makeAdminClient);
      try {
        return { text: await (built.client as sdk.AdminClient).metrics() };
      } finally {
        await built.close();
      }
    }
    case "admin_config": {
      const built = await buildClient(gateway, client, requests, makeAdminClient);
      try {
        return await (built.client as sdk.AdminClient).config();
      } finally {
        await built.close();
      }
    }
    case "admin_dlq_list": {
      const built = await buildClient(gateway, client, requests, makeAdminClient);
      try {
        return await (built.client as sdk.AdminClient).listDeadLetters(input.params as sdk.ListDeadLettersParams);
      } finally {
        await built.close();
      }
    }
    case "admin_dlq_replay": {
      const built = await buildClient(gateway, client, requests, makeAdminClient);
      try {
        return await (built.client as sdk.AdminClient).replayDeadLetters(input.filter as sdk.ReplayFilter);
      } finally {
        await built.close();
      }
    }
    case "admin_quarantine": {
      const built = await buildClient(gateway, client, requests, makeAdminClient);
      try {
        return await (built.client as sdk.AdminClient).listQuarantined(input.params as sdk.ListQuarantinedParams);
      } finally {
        await built.close();
      }
    }
    default:
      return assert.fail(`unknown conformance operation ${c.operation}`);
  }
}

/** Bytes can't round-trip through JSON: both sides become base64. */
function expectedOk(c: Case, expected: unknown): unknown {
  if (c.operation !== "redeem") return expected;
  const body = (expected as { body?: Body }).body;
  return { body: { base64: bodyBytes(body).toString("base64") } };
}

function assertRequests(actual: Recorded[], expected: ExpectedRequest[]): void {
  assert.equal(
    actual.length,
    expected.length,
    `expected ${expected.length} requests, got ${actual.length}: ${JSON.stringify(actual)}`,
  );
  actual.forEach((got, i) => {
    const want = expected[i];
    assert.equal(got.method, want.method, `request ${i} method`);
    assert.equal(got.path, want.path, `request ${i} path`);
    for (const [name, value] of Object.entries(want.headers ?? {})) {
      assert.equal(got.headers[name], value, `request ${i} header ${name}`);
    }
    // Absent means "not asserted": vectors don't all pin the body.
    if ("body" in want) {
      assert.deepEqual(got.body, want.body, `request ${i} body`);
    }
  });
}

async function runCase(c: Case): Promise<void> {
  const requests: Recorded[] = [];
  let ok: unknown;
  let error: Record<string, unknown> | undefined;
  try {
    ok = await dispatch(c, requests);
  } catch (err) {
    const name = Object.keys(ERROR_CLASSES).find(
      (n) => (err as { constructor?: unknown }).constructor === ERROR_CLASSES[n],
    );
    if (name === undefined) throw err;
    error = { class: name };
    for (const key of [
      "retryable",
      "status",
      "body",
      "code",
      "field",
      "message",
      "conflicting_id",
      "max_routes",
      "role",
    ] as const) {
      if (key in (err as object)) error[key] = (err as Record<string, unknown>)[key];
    }
  }

  const expect = c.expect;
  if ("ok" in expect) {
    assert.equal(error, undefined, `expected ok, got ${JSON.stringify(error)}`);
    assert.deepEqual(ok, expectedOk(c, expect.ok));
  } else if ("error" in expect) {
    assert.ok(error, `expected error ${JSON.stringify(expect.error)}, got ok ${JSON.stringify(ok)}`);
    const want = expect.error ?? {};
    assert.equal(error.class, want.class, `expected ${String(want.class)}, got ${String(error.class)}`);
    for (const [key, value] of Object.entries(want)) {
      if (key === "class") continue;
      assert.deepEqual(error[key], value, `${String(error.class)}.${key}`);
    }
  } else {
    // Only `requests` is asserted: the operation must have completed without
    // raising a mapped error (an unmapped error is rethrown by `dispatch`).
    assert.equal(error, undefined, `expected no error, got ${JSON.stringify(error)}`);
  }

  if (expect.requests) assertRequests(requests, expect.requests);
}

describe("conformance", () => {
  for (const c of CASES) {
    test(c.id, async () => runCase(c));
  }
});
