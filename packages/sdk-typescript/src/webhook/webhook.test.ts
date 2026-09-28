import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { parseHeaders } from "./headers.js";

// The webhook vectors cover the parsed output for plain string maps. These
// cover the TS-only input shapes a vector can't express.
describe("parseHeaders", () => {
  test("accepts a Headers instance", () => {
    assert.deepEqual(parseHeaders(new Headers({ "X-Ankusa-Id": "01a0", "x-ankusa-seq": "3" })), {
      id: "01a0",
      source: "",
      seq: 3,
      tenant: null,
      contentType: null,
    });
  });

  test("accepts Node's IncomingHttpHeaders shape", () => {
    const headers: Record<string, string | string[] | undefined> = {
      "x-ankusa-id": ["01a0", "01a1"],
      "x-ankusa-source": undefined,
    };
    const parsed = parseHeaders(headers);
    assert.equal(parsed.id, "01a0");
    assert.equal(parsed.source, "");
  });
});
