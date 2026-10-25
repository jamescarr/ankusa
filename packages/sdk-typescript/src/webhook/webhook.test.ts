import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { isTrusted } from "../message/index.js";
import { parseHeaders } from "./headers.js";

// The webhook vectors cover the parsed output for plain string maps. These
// cover the TS-only input shapes a vector can't express.
describe("parseHeaders", () => {
  test("accepts a Headers instance", () => {
    assert.deepEqual(parseHeaders(new Headers({ "X-Ankusa-Id": "01a0", "X-Ankusa-Tenant": "acme" })), {
      id: "01a0",
      source: "",
      tenant: "acme",
      contentType: null,
      dedupeKey: null,
      replayId: null,
      idempotencyKey: null,
      receivedAt: null,
      bodySha256: null,
      verification: null,
    });
  });

  test("accepts Node's IncomingHttpHeaders shape", () => {
    const headers: Record<string, string | string[] | undefined> = {
      "x-ankusa-id": ["01a0", "01a1"],
      "x-ankusa-source": undefined,
      "X-Ankusa-Dedupe-Key": "evt_9",
      "x-ankusa-replay-id": "",
      "X-Ankusa-Idempotency-Key": "acme:stripe:evt_9",
    };
    const parsed = parseHeaders(headers);
    assert.equal(parsed.id, "01a0");
    assert.equal(parsed.source, "");
    // An empty header is the same as absent.
    assert.equal(parsed.dedupeKey, "evt_9");
    assert.equal(parsed.replayId, null);
    assert.equal(parsed.idempotencyKey, "acme:stripe:evt_9");
  });

  test("reads when the edge accepted the hook and the body's SHA-256, and ignores a malformed value", () => {
    const sha256 = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
    const parsed = parseHeaders(
      new Headers({ "x-ankusa-id": "01a0", "x-ankusa-received-at": "1720000000000", "x-ankusa-body-sha256": sha256 }),
    );
    assert.equal(parsed.receivedAt, 1720000000000);
    assert.equal(parsed.bodySha256, sha256);

    const malformed = parseHeaders({
      "x-ankusa-id": "01a0",
      "x-ankusa-received-at": "-1",
      "x-ankusa-body-sha256": sha256.toUpperCase(),
    });
    assert.equal(malformed.receivedAt, null);
    assert.equal(malformed.bodySha256, null);
  });
});

describe("isTrusted", () => {
  test("is true only for an unflagged ok", () => {
    assert.equal(isTrusted(parseHeaders({ "x-ankusa-id": "01a0", "x-ankusa-verification": "ok" })), true);
    assert.equal(
      isTrusted(
        parseHeaders({
          "x-ankusa-id": "01a0",
          "x-ankusa-verification": "failed",
          "x-ankusa-verification-flagged": "true",
        }),
      ),
      false,
    );
    assert.equal(isTrusted(parseHeaders({ "x-ankusa-id": "01a0", "x-ankusa-verification": "skipped" })), false);
    assert.equal(isTrusted(parseHeaders({ "x-ankusa-id": "01a0" })), false);
  });
});
