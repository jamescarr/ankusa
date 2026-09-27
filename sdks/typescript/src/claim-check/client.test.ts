import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import http from "node:http";
import { after, before, describe, test } from "node:test";

import { createClaimCheckClient } from "./client.js";
import {
  ClaimCheckUnavailableError,
  ClaimIntegrityError,
  ClaimNotFoundError,
  ClaimRejectedError,
  InvalidClaimRefError,
} from "./errors.js";
import { parseClaimRef } from "./ref.js";

const TENANT = "acme";
const OBJECT_ID = "0199a1c2-7b3e-7d4a-9c1f-2e5b8a6d4f10";
const BODY = Buffer.from("hello claim check");
const SHA256 = createHash("sha256").update(BODY).digest("hex");
const REF = `urn:ankusa:claim:v1:${TENANT}:${OBJECT_ID}:66:${BODY.length}:sha256-${SHA256}`;

describe("parseClaimRef", () => {
  test("splits a well-formed ref into its path segments", () => {
    assert.deepEqual(parseClaimRef(REF), {
      tenantId: TENANT,
      objectId: OBJECT_ID,
      offset: "66",
      length: String(BODY.length),
      sha256: SHA256,
    });
  });

  for (const bad of [
    "not-a-ref",
    "urn:ankusa:claim:v1:acme:not-a-uuid:66:18:sha256-" + SHA256,
    `urn:ankusa:claim:v1:acme:${OBJECT_ID}:007:18:sha256-${SHA256}`, // leading zero
    `urn:ankusa:claim:v1:acme:${OBJECT_ID}:66:18:sha256-deadbeef`, // short digest
  ]) {
    test(`rejects ${JSON.stringify(bad)}`, () => {
      assert.throws(() => parseClaimRef(bad), InvalidClaimRefError);
    });
  }
});

describe("ClaimCheckClient.redeem", () => {
  let server: http.Server;
  let baseUrl: string;
  // Route responses keyed by the redeem path, so each test controls exactly
  // what the "gateway" hands back.
  let handler: (req: http.IncomingMessage, res: http.ServerResponse) => void;

  before(async () => {
    server = http.createServer((req, res) => handler(req, res));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (typeof address !== "object" || address === null) throw new Error("no server address");
    baseUrl = `http://127.0.0.1:${address.port}`;
  });

  after(async () => {
    await new Promise<void>((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
  });

  test("returns verified bytes on a 200 with matching size and sha256", async () => {
    handler = (_req, res) => {
      res.writeHead(200, { "content-type": "application/octet-stream" });
      res.end(BODY);
    };
    const client = createClaimCheckClient({ baseUrl });
    const bytes = await client.redeem(REF);
    assert.equal(bytes.toString(), BODY.toString());
  });

  test("raises ClaimIntegrityError, non-retryable, on a sha256 mismatch", async () => {
    handler = (_req, res) => {
      res.writeHead(200, { "content-type": "application/octet-stream" });
      res.end(Buffer.from("x".repeat(BODY.length))); // right length, wrong bytes
    };
    const client = createClaimCheckClient({ baseUrl });
    await assert.rejects(client.redeem(REF), (err) => {
      assert.ok(err instanceof ClaimIntegrityError);
      assert.equal(err.retryable, false);
      return true;
    });
  });

  test("raises ClaimIntegrityError on a truncated body", async () => {
    handler = (_req, res) => {
      res.writeHead(200, { "content-type": "application/octet-stream" });
      res.end(BODY.subarray(0, BODY.length - 1));
    };
    const client = createClaimCheckClient({ baseUrl });
    await assert.rejects(client.redeem(REF), ClaimIntegrityError);
  });

  test("raises ClaimNotFoundError, non-retryable, on a 404", async () => {
    handler = (_req, res) => {
      res.writeHead(404, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "not_found" }));
    };
    const client = createClaimCheckClient({ baseUrl });
    await assert.rejects(client.redeem(REF), (err) => {
      assert.ok(err instanceof ClaimNotFoundError);
      assert.equal(err.retryable, false);
      return true;
    });
  });

  test("raises ClaimRejectedError, non-retryable, on a 400", async () => {
    handler = (_req, res) => {
      res.writeHead(400, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "invalid_range" }));
    };
    const client = createClaimCheckClient({ baseUrl });
    await assert.rejects(client.redeem(REF), (err) => {
      assert.ok(err instanceof ClaimRejectedError);
      assert.equal(err.retryable, false);
      assert.equal(err.status, 400);
      return true;
    });
  });

  test("raises ClaimCheckUnavailableError, retryable, on a 503", async () => {
    handler = (_req, res) => {
      res.writeHead(503, { "content-type": "application/json", "retry-after": "1" });
      res.end(JSON.stringify({ error: "store_unavailable" }));
    };
    const client = createClaimCheckClient({ baseUrl });
    await assert.rejects(client.redeem(REF), (err) => {
      assert.ok(err instanceof ClaimCheckUnavailableError);
      assert.equal(err.retryable, true);
      return true;
    });
  });

  test("raises ClaimCheckUnavailableError, retryable, when the gateway is unreachable", async () => {
    const client = createClaimCheckClient({ baseUrl: "http://127.0.0.1:1" });
    await assert.rejects(client.redeem(REF), (err) => {
      assert.ok(err instanceof ClaimCheckUnavailableError);
      assert.equal(err.retryable, true);
      return true;
    });
  });

  test("raises InvalidClaimRefError without making a request for a malformed ref", async () => {
    handler = () => {
      throw new Error("must not be called");
    };
    const client = createClaimCheckClient({ baseUrl });
    await assert.rejects(client.redeem("not-a-ref"), InvalidClaimRefError);
  });

  test("health() resolves ok on a healthy gateway", async () => {
    handler = (req, res) => {
      assert.equal(req.url, "/health");
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ status: "ok" }));
    };
    const client = createClaimCheckClient({ baseUrl });
    assert.deepEqual(await client.health(), { status: "ok" });
  });
});
