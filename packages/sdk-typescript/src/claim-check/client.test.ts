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
const CLAIM_ID = "01M39VMD8RA3C5HR4RBV67Y002";
const BODY = Buffer.from("hello claim check");
const SHA256 = createHash("sha256").update(BODY).digest("hex");
const REF = `urn:ankusa:claim:v1:${TENANT}:${CLAIM_ID}`;

describe("parseClaimRef", () => {
  test("splits a well-formed ref into its path segments", () => {
    assert.deepEqual(parseClaimRef(REF), {
      tenantId: TENANT,
      claimId: CLAIM_ID,
      path: `/v1/claims/${TENANT}/${CLAIM_ID}`,
    });
  });

  for (const bad of [
    "not-a-ref",
    `urn:ankusa:claim:v1:acme:${CLAIM_ID.toLowerCase()}`, // lowercase ULID
    `urn:ankusa:claim:v1:acme:${CLAIM_ID.slice(0, 25)}`, // 25 chars
    `urn:ankusa:claim:v1:acme:${CLAIM_ID}0`, // 27 chars
    `urn:ankusa:claim:v1:acme:8${CLAIM_ID.slice(1)}`, // first char above 7
    `urn:ankusa:claim:v1:acme:${CLAIM_ID.slice(0, 25)}I`, // forbidden letter I
    `urn:ankusa:claim:v1:acme:${CLAIM_ID.slice(0, 25)}L`, // forbidden letter L
    `urn:ankusa:claim:v1:acme:${CLAIM_ID.slice(0, 25)}O`, // forbidden letter O
    `urn:ankusa:claim:v1:acme:${CLAIM_ID.slice(0, 25)}U`, // forbidden letter U
    `urn:ankusa:claim:v1:acme:${CLAIM_ID}:extra`, // extra segment
    `urn:ankusa:claim:v1:acme:0199a1c2-7b3e-7d4a-9c1f-2e5b8a6d4f10:66:18:sha256-${SHA256}`, // old format
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

  test("returns verified bytes on a 200 with a matching sha256", async () => {
    handler = (_req, res) => {
      res.writeHead(200, { "content-type": "application/octet-stream" });
      res.end(BODY);
    };
    const client = createClaimCheckClient({ baseUrl });
    const bytes = await client.redeem(REF, SHA256);
    assert.equal(bytes.toString(), BODY.toString());
  });

  test("raises ClaimIntegrityError, non-retryable, on a sha256 mismatch", async () => {
    handler = (_req, res) => {
      res.writeHead(200, { "content-type": "application/octet-stream" });
      res.end(Buffer.from("x".repeat(BODY.length))); // right length, wrong bytes
    };
    const client = createClaimCheckClient({ baseUrl });
    await assert.rejects(client.redeem(REF, SHA256), (err) => {
      assert.ok(err instanceof ClaimIntegrityError);
      assert.equal(err.retryable, false);
      return true;
    });
  });


  test("raises ClaimNotFoundError, non-retryable, on a 404", async () => {
    handler = (_req, res) => {
      res.writeHead(404, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "not_found" }));
    };
    const client = createClaimCheckClient({ baseUrl });
    await assert.rejects(client.redeem(REF, SHA256), (err) => {
      assert.ok(err instanceof ClaimNotFoundError);
      assert.equal(err.retryable, false);
      return true;
    });
  });

  test("raises ClaimRejectedError, non-retryable, on a 400", async () => {
    handler = (_req, res) => {
      res.writeHead(400, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "invalid_id" }));
    };
    const client = createClaimCheckClient({ baseUrl });
    await assert.rejects(client.redeem(REF, SHA256), (err) => {
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
    await assert.rejects(client.redeem(REF, SHA256), (err) => {
      assert.ok(err instanceof ClaimCheckUnavailableError);
      assert.equal(err.retryable, true);
      return true;
    });
  });

  test("raises ClaimCheckUnavailableError, retryable, when the gateway is unreachable", async () => {
    const client = createClaimCheckClient({ baseUrl: "http://127.0.0.1:1" });
    await assert.rejects(client.redeem(REF, SHA256), (err) => {
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
    await assert.rejects(client.redeem("not-a-ref", SHA256), InvalidClaimRefError);
  });

  for (const bad of ["", "deadbeef", SHA256.toUpperCase(), `sha256-${SHA256}`, `${SHA256}0`]) {
    test(`raises InvalidClaimRefError without making a request for sha256 ${JSON.stringify(bad)}`, async () => {
      handler = () => {
        throw new Error("must not be called");
      };
      const client = createClaimCheckClient({ baseUrl });
      await assert.rejects(client.redeem(REF, bad), (err) => {
        assert.ok(err instanceof InvalidClaimRefError);
        assert.equal(err.retryable, false);
        return true;
      });
    });
  }

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
