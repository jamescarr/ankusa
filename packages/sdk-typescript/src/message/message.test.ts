import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { decodeMessage, idempotencyKey, InvalidMessageError } from "./index.js";
import { parseHeaders } from "../webhook/headers.js";

const INLINE = JSON.stringify({
  v: 1,
  id: "01a0",
  source_id: "stripe",
  received_at: 1720000000000,
  size: 5,
  body_base64: "aGVsbG8=",
  dedupe_key: "evt_1",
  replay_id: "rid-1",
});

// The vectors cover the decode rules field by field; these cover the TS-only
// surfaces they can't express: a `Uint8Array` input, the decoded `body`, and
// the key helper on both shapes.
describe("decodeMessage", () => {
  test("accepts the raw UTF-8 bytes and exposes the decoded body", () => {
    const message = decodeMessage(new TextEncoder().encode(INLINE));
    assert.equal(message.id, "01a0");
    assert.deepEqual(message.body, new TextEncoder().encode("hello"));
    // `body` is non-enumerable: the message still serializes to the wire shape.
    assert.equal("body" in { ...message }, false);
    assert.equal(JSON.parse(JSON.stringify(message)).body, undefined);
  });

  test("InvalidMessageError carries retryable/code/field", () => {
    try {
      decodeMessage(JSON.stringify({ v: 1, id: "01a0", source_id: "demo", received_at: 1, size: 5 }));
      assert.fail("expected decodeMessage to throw");
    } catch (err) {
      assert.ok(err instanceof InvalidMessageError);
      assert.equal(err.retryable, false);
      assert.equal(err.code, "missing_body");
      assert.equal(err.field, null);
    }
  });

  test("a digest mismatch is not retryable", () => {
    const message = JSON.stringify({
      v: 1,
      id: "01a0",
      source_id: "demo",
      received_at: 1,
      size: 5,
      body_base64: "aGVsbG8=",
      sha256: "d9298a10d1b0735837dc4bd85dac641b0f3cef27a47e5d53a54f2f3f5b2fcffa",
    });
    assert.throws(() => decodeMessage(message), (err: unknown) => {
      assert.ok(err instanceof InvalidMessageError);
      assert.equal(err.code, "integrity");
      assert.equal(err.retryable, false);
      return true;
    });
  });
});

describe("idempotencyKey", () => {
  test("from a Message: dedupe_key, with the replay marker only when asked", () => {
    const message = decodeMessage(INLINE);
    assert.equal(idempotencyKey(message), "stripe:evt_1");
    assert.equal(idempotencyKey(message, { includeReplay: true }), "stripe:evt_1#replay:rid-1");
  });

  test("from HookHeaders: source plays source_id", () => {
    const headers = parseHeaders({
      "x-ankusa-id": "01a0",
      "x-ankusa-source": "stripe",
      "x-ankusa-dedupe-key": "evt_1",
    });
    assert.equal(idempotencyKey(headers), "stripe:evt_1");
  });

  test("falls back to id without a dedupe_key", () => {
    const headers = parseHeaders({ "x-ankusa-id": "01a0", "x-ankusa-source": "stripe" });
    assert.equal(idempotencyKey(headers), "01a0");
  });
});
