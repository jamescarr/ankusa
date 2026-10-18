/**
 * Verify the Standard Webhooks signature Ankusa's HTTP sink adds when the sink
 * has a `secret` (https://www.standardwebhooks.com/).
 *
 * `webhook-signature` holds space-separated `v1,<base64>` entries, each an
 * HMAC-SHA256 over `<webhook-id>.<webhook-timestamp>.<body>`. A delivery
 * passes when any `v1` entry matches any configured secret (several during a
 * rotation), compared in constant time, and `webhook-timestamp` is within
 * `toleranceSeconds` (default 300) of now.
 */
import { createHmac, timingSafeEqual } from "node:crypto";

import type { HeaderSource } from "./headers.js";

/** Why a delivery's signature was refused. */
export type InvalidSignatureCode =
  | "invalid_secret"
  | "missing_header"
  | "invalid_timestamp"
  | "timestamp_out_of_tolerance"
  | "no_matching_signature";

/**
 * A delivery whose signature does not verify. Never retryable: answer `401`;
 * the sender retries with the same bytes, which will not verify either.
 */
export class InvalidSignatureError extends Error {
  readonly retryable = false;
  readonly code: InvalidSignatureCode;
  /** The header at fault, or `null` (`invalid_secret`). */
  readonly field: string | null;

  constructor(code: InvalidSignatureCode, field: string | null, message: string) {
    super(message);
    this.code = code;
    this.field = field;
  }
}

export type VerifySignatureOptions = {
  headers: HeaderSource;
  /** The raw request body, exactly as received. */
  body: string | Uint8Array;
  /** `whsec_` + base64 key, or any other string used as its own UTF-8 bytes. */
  secrets: string | string[];
  /** Default 300. */
  toleranceSeconds?: number;
  /** Unix seconds; defaults to the clock. */
  now?: number;
};

/** The verified delivery's id and send time. */
export type VerifiedSignature = { id: string; timestamp: number };

/** Verify a delivery; throws `InvalidSignatureError` when it does not. */
export function verifySignature(options: VerifySignatureOptions): VerifiedSignature {
  const keys = decodeSecrets(options.secrets);

  const lowered = new Map<string, string>();
  const entries: Iterable<[string, string | string[] | undefined]> =
    options.headers instanceof Headers ? options.headers.entries() : Object.entries(options.headers);
  for (const [name, value] of entries) {
    if (value === undefined) continue;
    lowered.set(name.toLowerCase(), Array.isArray(value) ? value[0] : value);
  }

  const required = (name: string): string => {
    const value = lowered.get(name);
    if (!value) throw new InvalidSignatureError("missing_header", name, `missing ${name} header`);
    return value;
  };

  const id = required("webhook-id");
  const rawTimestamp = required("webhook-timestamp");
  const signature = required("webhook-signature");

  if (!/^[0-9]+$/.test(rawTimestamp)) {
    throw new InvalidSignatureError("invalid_timestamp", "webhook-timestamp", "webhook-timestamp is not a unix time");
  }
  const timestamp = Number(rawTimestamp);
  const now = options.now ?? Math.floor(Date.now() / 1000);
  const tolerance = options.toleranceSeconds ?? 300;
  if (Math.abs(now - timestamp) > tolerance) {
    throw new InvalidSignatureError(
      "timestamp_out_of_tolerance",
      "webhook-timestamp",
      "webhook-timestamp is outside the tolerance window",
    );
  }

  const body = typeof options.body === "string" ? Buffer.from(options.body, "utf8") : Buffer.from(options.body);
  const signed = Buffer.concat([Buffer.from(`${id}.${rawTimestamp}.`, "utf8"), body]);
  const candidates = signature
    .split(" ")
    .filter((entry) => entry.startsWith("v1,"))
    .map((entry) => Buffer.from(entry.slice(3), "utf8"));

  const matched = keys.some((key) => {
    const expected = Buffer.from(createHmac("sha256", key).update(signed).digest("base64"), "utf8");
    return candidates.some((candidate) => candidate.length === expected.length && timingSafeEqual(candidate, expected));
  });

  if (!matched) {
    throw new InvalidSignatureError("no_matching_signature", "webhook-signature", "no webhook-signature entry matches");
  }
  return { id, timestamp };
}

const BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

function decodeSecrets(secrets: string | string[]): Buffer[] {
  const list = Array.isArray(secrets) ? secrets : [secrets];
  if (list.length === 0) throw new InvalidSignatureError("invalid_secret", null, "no secret configured");
  return list.map((secret) => {
    if (secret.startsWith("whsec_")) {
      const encoded = secret.slice("whsec_".length);
      if (encoded === "" || !BASE64.test(encoded)) {
        throw new InvalidSignatureError("invalid_secret", null, "a whsec_ secret is not valid base64");
      }
      return Buffer.from(encoded, "base64");
    }
    if (secret === "") throw new InvalidSignatureError("invalid_secret", null, "an empty secret");
    return Buffer.from(secret, "utf8");
  });
}
