import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  AdminRejectedError,
  AdminUnavailableError,
  createAdminClient,
  RoleNotEnabledError,
} from "./index.js";

type Recorded = { method: string; url: string; headers: Record<string, string> };

/** A fetch stand-in that records every request (including its query string)
 * and answers with `status` + `body`. */
function mockGateway(status: number, body: string, contentType = "application/json") {
  const requests: Recorded[] = [];
  const fetch = (async (input: RequestInfo | URL) => {
    const req = input instanceof Request ? input : new Request(input);
    requests.push({ method: req.method, url: req.url, headers: Object.fromEntries(req.headers) });
    return new Response(body, { status, headers: { "content-type": contentType } });
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

describe("createAdminClient", () => {
  test("health returns the operator health body", async () => {
    const { fetch, requests } = mockGateway(200, JSON.stringify({ status: "ok", instance: "default", roles: ["edge", "dispatch"] }));
    const client = createAdminClient({ baseUrl: "http://gateway", fetch });
    assert.deepEqual(await client.health(), { status: "ok", instance: "default", roles: ["edge", "dispatch"] });
    assert.equal(requests[0].method, "GET");
    assert.equal(requests[0].url, "http://gateway/health");
  });

  test("metrics returns the text body verbatim", async () => {
    const text = 'ankusa_ingest_requests_total{instance="default"} 1\n';
    const { fetch, requests } = mockGateway(200, text, "text/plain; version=0.0.4");
    const client = createAdminClient({ baseUrl: "http://gateway", fetch });
    assert.equal(await client.metrics(), text);
    assert.equal(requests[0].url, "http://gateway/metrics");
  });

  test("config returns the JSON object", async () => {
    const { fetch, requests } = mockGateway(200, JSON.stringify({ instance: "default", roles: ["edge"] }));
    const client = createAdminClient({ baseUrl: "http://gateway", fetch });
    assert.deepEqual(await client.config(), { instance: "default", roles: ["edge"] });
    assert.equal(requests[0].url, "http://gateway/v1/config");
  });

  test("listDeadLetters sends only the query params that are present", async () => {
    const { fetch, requests } = mockGateway(200, JSON.stringify({ total: 0, entries: [] }));
    const client = createAdminClient({ baseUrl: "http://gateway", fetch });
    await client.listDeadLetters({ source_id: "demo", since: 1720000000000 });
    const url = new URL(requests[0].url);
    assert.equal(url.pathname, "/v1/dlq");
    assert.equal(url.searchParams.get("source_id"), "demo");
    assert.equal(url.searchParams.get("since"), "1720000000000");
    assert.equal(url.searchParams.has("limit"), false);
  });

  test("createReplay POSTs the spec and returns the job", async () => {
    const job = { id: "r1", kind: "dlq", state: "running", rate: 500 };
    const { fetch, requests } = mockGateway(202, JSON.stringify(job));
    const client = createAdminClient({ baseUrl: "http://gateway", fetch });
    assert.deepEqual(await client.createReplay({ kind: "dlq", source_id: "demo", rate: 500 }), job);
    assert.equal(requests[0].method, "POST");
    assert.equal(requests[0].url, "http://gateway/v1/replays");
  });

  test("getReplay GETs the job by id", async () => {
    const job = { id: "r1", kind: "dlq", state: "running", rate: 500 };
    const { fetch, requests } = mockGateway(200, JSON.stringify(job));
    const client = createAdminClient({ baseUrl: "http://gateway", fetch });
    assert.deepEqual(await client.getReplay("r1"), job);
    assert.equal(requests[0].method, "GET");
    assert.equal(requests[0].url, "http://gateway/v1/replays/r1");
  });

  test("listReplays GETs the collection", async () => {
    const page = { replays: [{ id: "r1", kind: "dlq", state: "running", rate: 500 }] };
    const { fetch, requests } = mockGateway(200, JSON.stringify(page));
    const client = createAdminClient({ baseUrl: "http://gateway", fetch });
    assert.deepEqual(await client.listReplays(), page);
    assert.equal(requests[0].method, "GET");
    assert.equal(requests[0].url, "http://gateway/v1/replays");
  });

  test("updateReplay PATCHes the patch and returns the job", async () => {
    const job = { id: "r1", kind: "dlq", state: "paused", rate: 500 };
    const { fetch, requests } = mockGateway(200, JSON.stringify(job));
    const client = createAdminClient({ baseUrl: "http://gateway", fetch });
    assert.deepEqual(await client.updateReplay("r1", { state: "paused" }), job);
    assert.equal(requests[0].method, "PATCH");
    assert.equal(requests[0].url, "http://gateway/v1/replays/r1");
  });

  test("listQuarantined sends the limit query param", async () => {
    const { fetch, requests } = mockGateway(200, JSON.stringify({ entries: [] }));
    const client = createAdminClient({ baseUrl: "http://gateway", fetch });
    await client.listQuarantined({ limit: 10 });
    const url = new URL(requests[0].url);
    assert.equal(url.pathname, "/v1/quarantine");
    assert.equal(url.searchParams.get("limit"), "10");
  });

  test("maps 409 role_not_enabled to RoleNotEnabledError with the role", async () => {
    const { fetch } = mockGateway(409, JSON.stringify({ error: "role_not_enabled", role: "dispatch" }));
    const client = createAdminClient({ baseUrl: "http://gateway", fetch });
    const err = (await expectError(client.listDeadLetters())) as RoleNotEnabledError;
    assert.ok(err instanceof RoleNotEnabledError);
    assert.equal(err.retryable, false);
    assert.equal(err.role, "dispatch");
  });

  test("maps other 4xx to AdminRejectedError with status and code", async () => {
    const { fetch } = mockGateway(400, JSON.stringify({ error: "invalid_filter", field: "since" }));
    const client = createAdminClient({ baseUrl: "http://gateway", fetch });
    const err = (await expectError(client.listDeadLetters())) as AdminRejectedError;
    assert.ok(err instanceof AdminRejectedError);
    assert.equal(err.retryable, false);
    assert.equal(err.status, 400);
    assert.equal(err.code, "invalid_filter");
  });

  test("maps 5xx to a retryable AdminUnavailableError", async () => {
    const { fetch } = mockGateway(503, JSON.stringify({ error: "boom" }));
    const client = createAdminClient({ baseUrl: "http://gateway", fetch });
    const err = (await expectError(client.config())) as AdminUnavailableError;
    assert.ok(err instanceof AdminUnavailableError);
    assert.equal(err.retryable, true);
  });

  test("maps a 302 redirect to a retryable AdminUnavailableError", async () => {
    const { fetch } = mockGateway(302, "", "text/html");
    const client = createAdminClient({ baseUrl: "http://gateway", fetch });
    const err = (await expectError(client.config())) as AdminUnavailableError;
    assert.ok(err instanceof AdminUnavailableError);
    assert.equal(err.retryable, true);
  });

  test("maps a 500 to a retryable AdminUnavailableError", async () => {
    const { fetch } = mockGateway(500, JSON.stringify({ error: "boom" }));
    const client = createAdminClient({ baseUrl: "http://gateway", fetch });
    const err = (await expectError(client.listDeadLetters())) as AdminUnavailableError;
    assert.ok(err instanceof AdminUnavailableError);
    assert.equal(err.retryable, true);
  });

  test("a network failure maps to a retryable AdminUnavailableError", async () => {
    const fetch = (async () => {
      throw new Error("ECONNREFUSED");
    }) as typeof fetch;
    const client = createAdminClient({ baseUrl: "http://gateway", fetch });
    const err = (await expectError(client.health())) as AdminUnavailableError;
    assert.ok(err instanceof AdminUnavailableError);
    assert.equal(err.retryable, true);
  });
});
