import { createHash } from "node:crypto";

import { parseClaimRef, type ParsedClaimRef } from "../claim-check/ref.js";
import type { HookHeaders, Verification } from "../webhook/headers.js";

/**
 * A decoded queue message — the same JSON a broker carries, so the field
 * names are the wire's (snake_case), matching the generated admin/route types.
 *
 * `body` is the decoded inline body, exposed as a non-enumerable property so
 * the object still serializes back to exactly the wire shape. It is `null` for
 * the claim body form (the bytes live behind `claim`).
 */
export type Message = {
  /** The wire version, 1 to 2 (`MIN_SUPPORTED_VERSION`, `MAX_SUPPORTED_VERSION`). */
  v: number;
  id: string;
  source_id: string;
  tenant_id: string | null;
  received_at: number;
  content_type: string | null;
  size: number;
  /** The decoded body re-encoded as standard base64; `null` for a claim message. */
  body_base64: string | null;
  /** The claim-check ref; `null` for an inline message. */
  claim: string | null;
  /** Lowercase hex SHA-256 of the body; `null` when the producer sent none. */
  sha256: string | null;
  /** Provider event key extracted at ingest; `null` when the source has none. */
  dedupe_key: string | null;
  /** The replay job id when this delivery is a replay; `null` otherwise. */
  replay_id: string | null;
  /**
   * The tenant-scoped key to dedupe on, computed once by Ankusa
   * (`tenant:source_id:dedupe_key`, else `id`); `null` when the producer predates
   * the field. `idempotencyKey` reads it and falls back to computing it.
   */
  idempotency_key: string | null;
  /** Forwarded provider request headers (lowercase names); `{}` when none. */
  headers: Record<string, string>;
  /** How Ankusa verified the hook at ingest; `null` from a node older than the field. */
  verification: Verification | null;
  /**
   * The hook's W3C `traceparent`, verbatim: continue the trace from it. `null`
   * when the message carries none (or comes from a node older than the field).
   */
  traceparent: string | null;
  /** The W3C `tracestate` that goes with `traceparent`, verbatim; `null` when none. */
  tracestate: string | null;
  /** The name of the Ankusa instance that published the message; `null` when it says so or predates the field. */
  instance: string | null;
  /** The region that instance is configured with; `null` when it has none or predates the field. */
  region: string | null;
  /** The decoded inline body (non-enumerable); `null` for a claim message. */
  body: Uint8Array | null;
};

/** Every `InvalidMessageError` carries one of these `code`s. */
export type InvalidMessageCode =
  | "invalid_json"
  | "not_an_object"
  | "unsupported_version"
  | "invalid_field"
  | "ambiguous_body"
  | "missing_body"
  | "invalid_body_base64"
  | "size_mismatch"
  | "integrity"
  | "tenant_mismatch";

/**
 * A queue message could not be decoded. Never retryable: the same bytes will
 * fail the same way. `code` names the rule that failed and `field` is the
 * offending key (or `null` when the rule isn't about one field).
 */
export class InvalidMessageError extends Error {
  readonly retryable = false;

  constructor(
    readonly code: InvalidMessageCode,
    readonly field: string | null = null,
    message?: string,
  ) {
    super(message ?? `${code}${field ? `: ${field}` : ""}`);
    this.name = "InvalidMessageError";
  }
}

/**
 * The message versions `decodeMessage` understands, both ends inclusive. `v` is
 * the wire format's major version: a `v: 2` message is a `v: 1` message plus
 * additive fields, decoded exactly like it (the keys this decoder does not
 * know are ignored) into the one `Message` type, with `v` as the wire said it.
 * Raising `MAX_SUPPORTED_VERSION` is the whole change an SDK needs for a
 * producer that starts writing the next additive version.
 */
export const MIN_SUPPORTED_VERSION = 1;
export const MAX_SUPPORTED_VERSION = 2;

// The standard base64 alphabet, in whole 4-character groups with canonical
// padding: what `Buffer.from(s, "base64")` accepts is far looser (it skips
// garbage), so validate before decoding.
const BASE64_PATTERN = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

function decodeBase64(value: string): Buffer | null {
  if (!BASE64_PATTERN.test(value)) return null;
  return Buffer.from(value, "base64");
}

function decodeVerification(value: unknown): Verification | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== "object" || Array.isArray(value)) {
    throw new InvalidMessageError("invalid_field", "verification");
  }
  const { status, flagged } = value as Record<string, unknown>;
  if (typeof status !== "string" || status.length === 0 || typeof flagged !== "boolean") {
    throw new InvalidMessageError("invalid_field", "verification");
  }
  return { status, flagged };
}

/**
 * Decode a queue message of any supported `v` (`MIN_SUPPORTED_VERSION` to
 * `MAX_SUPPORTED_VERSION`), verifying its body.
 *
 * `data` is the raw message JSON (a string or its UTF-8 bytes). Every failure
 * raises `InvalidMessageError` with `retryable=false`; the checks run in order
 * and the first failure wins (see "Consuming queue messages" in the README).
 */
export function decodeMessage(data: string | Uint8Array): Message {
  const text = typeof data === "string" ? data : new TextDecoder().decode(data);
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new InvalidMessageError("invalid_json");
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new InvalidMessageError("not_an_object");
  }
  const raw = parsed as Record<string, unknown>;

  const version = raw.v;
  if (
    typeof version !== "number" ||
    !Number.isInteger(version) ||
    version < MIN_SUPPORTED_VERSION ||
    version > MAX_SUPPORTED_VERSION
  ) {
    throw new InvalidMessageError("unsupported_version");
  }

  // Field types, in the contract's order.
  const id = raw.id;
  if (typeof id !== "string" || id.length === 0) throw new InvalidMessageError("invalid_field", "id");
  const sourceId = raw.source_id;
  if (typeof sourceId !== "string") throw new InvalidMessageError("invalid_field", "source_id");
  const receivedAt = raw.received_at;
  if (typeof receivedAt !== "number" || !Number.isInteger(receivedAt)) {
    throw new InvalidMessageError("invalid_field", "received_at");
  }
  const size = raw.size;
  if (typeof size !== "number" || !Number.isInteger(size) || size < 0) {
    throw new InvalidMessageError("invalid_field", "size");
  }
  for (const key of [
    "tenant_id",
    "content_type",
    "dedupe_key",
    "replay_id",
    "idempotency_key",
    "traceparent",
    "tracestate",
    "instance",
    "region",
  ] as const) {
    const value = raw[key];
    if (!(value === undefined || value === null || typeof value === "string")) {
      throw new InvalidMessageError("invalid_field", key);
    }
  }
  let headers: Record<string, string> = {};
  if (raw.headers !== undefined) {
    const value = raw.headers;
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      throw new InvalidMessageError("invalid_field", "headers");
    }
    for (const header of Object.values(value as Record<string, unknown>)) {
      if (typeof header !== "string") throw new InvalidMessageError("invalid_field", "headers");
    }
    headers = { ...(value as Record<string, string>) };
  }
  const sha256 = raw.sha256;
  if (sha256 !== undefined && (typeof sha256 !== "string" || !/^[0-9a-f]{64}$/.test(sha256))) {
    throw new InvalidMessageError("invalid_field", "sha256");
  }
  const verification = decodeVerification(raw.verification);

  // Body form: exactly one of body_base64 and claim.
  const bodyBase64 = raw.body_base64;
  const claim = raw.claim;
  const hasBody = bodyBase64 !== undefined && bodyBase64 !== null;
  const hasClaim = claim !== undefined && claim !== null;
  if (hasBody && hasClaim) throw new InvalidMessageError("ambiguous_body");
  if (!hasBody && !hasClaim) throw new InvalidMessageError("missing_body");

  let body: Buffer | null = null;
  let claimRef: ParsedClaimRef | null = null;
  let encodedBody: string | null = null;
  if (hasBody) {
    if (typeof bodyBase64 !== "string") throw new InvalidMessageError("invalid_body_base64");
    body = decodeBase64(bodyBase64);
    if (body === null) throw new InvalidMessageError("invalid_body_base64");
    encodedBody = body.toString("base64");
    if (body.length !== size) throw new InvalidMessageError("size_mismatch");
    if (typeof sha256 === "string") {
      const digest = createHash("sha256").update(body).digest("hex");
      if (digest !== sha256) throw new InvalidMessageError("integrity");
    }
  } else {
    if (typeof claim !== "string") throw new InvalidMessageError("invalid_field", "claim");
    try {
      claimRef = parseClaimRef(claim);
    } catch {
      throw new InvalidMessageError("invalid_field", "claim");
    }
    if (sha256 === undefined) throw new InvalidMessageError("invalid_field", "sha256");
  }

  const tenantId = (raw.tenant_id ?? null) as string | null;
  if (tenantId !== null && claimRef !== null && claimRef.tenantId !== tenantId) {
    throw new InvalidMessageError("tenant_mismatch");
  }

  const message = {
    v: version,
    id,
    source_id: sourceId,
    tenant_id: tenantId,
    received_at: receivedAt,
    content_type: (raw.content_type ?? null) as string | null,
    size,
    body_base64: encodedBody,
    claim: claimRef === null ? null : (claim as string),
    sha256: (sha256 ?? null) as string | null,
    dedupe_key: (raw.dedupe_key ?? null) as string | null,
    replay_id: (raw.replay_id ?? null) as string | null,
    idempotency_key: (raw.idempotency_key ?? null) as string | null,
    headers,
    verification,
    traceparent: (raw.traceparent ?? null) as string | null,
    tracestate: (raw.tracestate ?? null) as string | null,
    instance: (raw.instance ?? null) as string | null,
    region: (raw.region ?? null) as string | null,
  } as Message;
  // Non-enumerable, so a decoded message still serializes to exactly the wire
  // shape (and deep-equals the conformance vectors).
  Object.defineProperty(message, "body", {
    value: body === null ? null : Uint8Array.from(body),
    enumerable: false,
    writable: false,
    configurable: true,
  });
  return message;
}

/** Options for `idempotencyKey`. */
export type IdempotencyKeyOptions = {
  /**
   * Append `#replay:<replay_id>` when `replay_id` is set. Defaults to `false`,
   * so a consumer that dedupes this way drops replays of events it already
   * processed; a consumer that must reprocess them sets it.
   */
  includeReplay?: boolean;
};

/**
 * The idempotency key for one delivery, from a decoded `Message` or from the
 * `HookHeaders` of an HTTP delivery (where `source_id` is `source`).
 *
 * It is the value Ankusa shipped (the message's `idempotency_key`, or the
 * `x-ankusa-idempotency-key` header) when that is a non-empty string. For a
 * message or delivery from a node that predates the field it is computed:
 * `tenant:source_id:dedupe_key` (tenant `default` when there is none) for a
 * non-empty `dedupe_key`, else `id`. With `includeReplay` and a `replay_id`,
 * `#replay:<replay_id>` is appended.
 */
export function idempotencyKey(
  hook: Message | HookHeaders,
  options: IdempotencyKeyOptions = {},
): string {
  // A Message carries `source_id`; HookHeaders carries `source`.
  const shipped = "source_id" in hook ? hook.idempotency_key : hook.idempotencyKey;

  let key: string;
  if (typeof shipped === "string" && shipped !== "") {
    key = shipped;
  } else {
    const tenant = "source_id" in hook ? hook.tenant_id : hook.tenant;
    const sourceId = "source_id" in hook ? hook.source_id : hook.source;
    const dedupe = "source_id" in hook ? hook.dedupe_key : hook.dedupeKey;
    key = dedupe !== null && dedupe !== "" ? `${tenant ?? "default"}:${sourceId}:${dedupe}` : hook.id;
  }

  const replayId = "source_id" in hook ? hook.replay_id : hook.replayId;
  if (options.includeReplay && replayId !== null) key += `#replay:${replayId}`;
  return key;
}

/**
 * Whether the node verified the hook at ingest and nothing flags it: `true`
 * only for `verification.status === "ok"` with `flagged` false.
 *
 * A flagged hook (accepted despite a failed check, `on_verify_failure:
 * accept_flag`), a `failed` or `skipped` one, any other status, and a hook with
 * no `verification` (a node older than the field) are untrusted: the answer
 * fails closed. `hook` is a decoded `Message` or the `HookHeaders` of an
 * HTTP-sink delivery.
 */
export function isTrusted(hook: Message | HookHeaders): boolean {
  const verification = hook.verification;
  return verification !== null && verification.status === "ok" && verification.flagged === false;
}
