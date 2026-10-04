import assert from "node:assert/strict";
import { describe, test } from "node:test";

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
});
