import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  createRoutesClient,
  InvalidRouteIdError,
  RouteNotFoundError,
  RoutesRejectedError,
  RoutesUnavailableError,
} from "./index.js";

type Recorded = { method: string; url: string; headers: Record<string, string> };

/** A fetch stand-in that records every request (including its query string)
 * and answers with `status` + `body`. */
function mockGateway(status: number, body: string, contentType = "application/json") {
  const requests: Recorded[] = [];
  const fetch = (async (input: RequestInfo | URL) => {
    const req = input instanceof Request ? input : new Request(input);
    requests.push({ method: req.method, url: req.url, headers: Object.fromEntries(req.headers) });
    // 204 forbids a body: `new Response("", { status: 204 })` throws.
    const nullBody = status === 204 || status === 205 || status === 304;
    return new Response(nullBody ? null : body, { status, headers: { "content-type": contentType } });
  }) as typeof fetch;
  return { fetch, requests };
}

async function expectError(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (err) {
    return err;
  }
  assert.fail("expected the promise to reject");
}

describe("createRoutesClient", () => {
  test("health returns the routes health body", async () => {
    const { fetch, requests } = mockGateway(200, JSON.stringify({ status: "ok", routes: 3 }));
    const client = createRoutesClient({ baseUrl: "http://gateway", fetch });
    assert.deepEqual(await client.health(), { status: "ok", routes: 3 });
    assert.equal(requests.length, 1);
    assert.equal(requests[0].method, "GET");
    assert.equal(requests[0].url, "http://gateway/health");
  });

  test("createRoute POSTs the input as JSON and returns the route", async () => {
    const route = { id: "stripe", path: "/webhooks/stripe", methods: ["POST"], enabled: true, ip_rules: [], metadata: {}, inserted_at: "2026-09-28T14:16:26Z", updated_at: "2026-09-28T14:16:26Z" };
    const { fetch, requests } = mockGateway(201, JSON.stringify(route));
    const client = createRoutesClient({ baseUrl: "http://gateway", fetch });
    assert.deepEqual(await client.createRoute({ path: "/webhooks/stripe" }), route);
    assert.equal(requests[0].method, "POST");
    assert.equal(requests[0].url, "http://gateway/admin/routes");
  });

  test("deleteRoute resolves on 204 with no body", async () => {
    const { fetch, requests } = mockGateway(204, "");
    const client = createRoutesClient({ baseUrl: "http://gateway", fetch });
    assert.equal(await client.deleteRoute("stripe"), undefined);
    assert.equal(requests[0].method, "DELETE");
    assert.equal(requests[0].url, "http://gateway/admin/routes/stripe");
  });

  test("listRoutes sends only the query params that are present", async () => {
    const { fetch, requests } = mockGateway(200, JSON.stringify({ routes: [], next_cursor: null }));
    const client = createRoutesClient({ baseUrl: "http://gateway", fetch });
    await client.listRoutes({ enabled: "true", limit: 50 });
    const url = new URL(requests[0].url);
    assert.equal(url.pathname, "/admin/routes");
    assert.equal(url.searchParams.get("enabled"), "true");
    assert.equal(url.searchParams.get("limit"), "50");
    assert.equal(url.searchParams.has("cursor"), false);
  });

  test("listRoutes omits absent params entirely", async () => {
    const { fetch, requests } = mockGateway(200, JSON.stringify({ routes: [], next_cursor: null }));
    const client = createRoutesClient({ baseUrl: "http://gateway", fetch });
    await client.listRoutes();
    assert.equal(new URL(requests[0].url).search, "");
  });

  test("getRoute maps 404 to RouteNotFoundError", async () => {
    const { fetch } = mockGateway(404, JSON.stringify({ error: "not_found" }));
    const client = createRoutesClient({ baseUrl: "http://gateway", fetch });
    const err = await expectError(client.getRoute("nope"));
    assert.ok(err instanceof RouteNotFoundError);
    assert.equal((err as RouteNotFoundError).retryable, false);
  });

  test("an id that is not a string, empty, or a dot is rejected before any request", async () => {
    const { fetch, requests } = mockGateway(200, "{}");
    const client = createRoutesClient({ baseUrl: "http://gateway", fetch });

    // A JS caller can pass anything; none of these reach the network.
    for (const id of ["", ".", "..", 42] as Array<string | number>) {
      const err = await expectError(client.getRoute(id as string));
      assert.ok(err instanceof InvalidRouteIdError, `${JSON.stringify(id)} should be invalid`);
      assert.equal((err as InvalidRouteIdError).retryable, false);
    }
    assert.equal(requests.length, 0);
  });

  test("every id-taking method rejects a bad id before sending", async () => {
    const { fetch, requests } = mockGateway(200, "{}");
    const client = createRoutesClient({ baseUrl: "http://gateway", fetch });
    const calls: Array<[string, () => Promise<unknown>]> = [
      ["getRoute", () => client.getRoute("..")],
      ["replaceRoute", () => client.replaceRoute("..", { path: "/hooks/x" })],
      ["updateRoute", () => client.updateRoute("..", { enabled: false })],
      ["deleteRoute", () => client.deleteRoute("..")],
    ];

    for (const [method, call] of calls) {
      const err = await expectError(call());
      assert.ok(err instanceof InvalidRouteIdError, `${method} should reject ".."`);
      assert.equal((err as InvalidRouteIdError).retryable, false);
    }
    assert.equal(requests.length, 0);
  });

  test("getRoute sends the id as one percent-encoded path segment", async () => {
    const { fetch, requests } = mockGateway(404, JSON.stringify({ error: "not_found" }));
    const client = createRoutesClient({ baseUrl: "http://gateway", fetch });
    const err = await expectError(client.getRoute("a/b?c#d e%"));
    assert.ok(err instanceof RouteNotFoundError);
    assert.equal(requests[0].url, "http://gateway/admin/routes/a%2Fb%3Fc%23d%20e%25");
  });

  test("a 302 redirect maps to a retryable RoutesUnavailableError", async () => {
    const { fetch } = mockGateway(302, "", "text/html");
    const client = createRoutesClient({ baseUrl: "http://gateway", fetch });
    const err = (await expectError(client.listRoutes())) as RoutesUnavailableError;
    assert.ok(err instanceof RoutesUnavailableError);
    assert.equal(err.retryable, true);
  });

  test("a 500 maps to a retryable RoutesUnavailableError", async () => {
    const { fetch } = mockGateway(500, JSON.stringify({ error: "boom" }));
    const client = createRoutesClient({ baseUrl: "http://gateway", fetch });
    const err = (await expectError(client.getIpRules())) as RoutesUnavailableError;
    assert.ok(err instanceof RoutesUnavailableError);
    assert.equal(err.retryable, true);
  });

  test("createRoute maps 400 to RoutesRejectedError with code, field, and message", async () => {
    const { fetch } = mockGateway(400, JSON.stringify({ error: "invalid_route", field: "path", message: 'must start with "/"' }));
    const client = createRoutesClient({ baseUrl: "http://gateway", fetch });
    const err = (await expectError(client.createRoute({ path: "hooks/x" }))) as RoutesRejectedError;
    assert.ok(err instanceof RoutesRejectedError);
    assert.equal(err.retryable, false);
    assert.equal(err.status, 400);
    assert.equal(err.code, "invalid_route");
    assert.equal(err.field, "path");
    assert.equal(err.message, 'must start with "/"');
  });

  test("createRoute maps 409 to RoutesRejectedError with conflicting_id", async () => {
    const { fetch } = mockGateway(409, JSON.stringify({ error: "duplicate_route", conflicting_id: "stripe" }));
    const client = createRoutesClient({ baseUrl: "http://gateway", fetch });
    const err = (await expectError(client.createRoute({ id: "other", path: "/webhooks/stripe" }))) as RoutesRejectedError;
    assert.ok(err instanceof RoutesRejectedError);
    assert.equal(err.code, "duplicate_route");
    assert.equal(err.conflicting_id, "stripe");
  });

  test("createRoute maps 503 to a retryable RoutesUnavailableError", async () => {
    const { fetch } = mockGateway(503, JSON.stringify({ error: "store_unavailable" }));
    const client = createRoutesClient({ baseUrl: "http://gateway", fetch });
    const err = (await expectError(client.createRoute({ path: "/hooks/x" }))) as RoutesUnavailableError;
    assert.ok(err instanceof RoutesUnavailableError);
    assert.equal(err.retryable, true);
  });

  test("a network failure maps to a retryable RoutesUnavailableError", async () => {
    const fetch = (async () => {
      throw new Error("ECONNREFUSED");
    }) as typeof fetch;
    const client = createRoutesClient({ baseUrl: "http://gateway", fetch });
    const err = (await expectError(client.getIpRules())) as RoutesUnavailableError;
    assert.ok(err instanceof RoutesUnavailableError);
    assert.equal(err.retryable, true);
  });
});
