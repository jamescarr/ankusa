/**
 * Parse the headers Ankusa's HTTP sink attaches to every delivery.
 *
 * See "HTTP handoff" in docs/integrations.md for the full contract this
 * mirrors: the raw body arrives verbatim, and identity travels in
 * `x-ankusa-id`, `x-ankusa-source`, `x-ankusa-tenant` (only when the source
 * has a tenant), `content-type`, `x-ankusa-idempotency-key`, and — when the
 * source has a dedupe rule or the delivery is a replay — `x-ankusa-dedupe-key`
 * and `x-ankusa-replay-id`. A receiver dedupes on the key `idempotencyKey`
 * returns (the shipped `x-ankusa-idempotency-key`): delivery is at-least-once,
 * so the same hook can arrive twice after a retry.
 */

/** The identity of one HTTP-sink delivery. */
export type HookHeaders = {
  id: string;
  source: string;
  /** Only present when the source has a tenant. */
  tenant: string | null;
  contentType: string | null;
  /** The provider event key; `null` when absent or empty. */
  dedupeKey: string | null;
  /** The replay job id; `null` when absent or empty. */
  replayId: string | null;
  /**
   * The tenant-scoped key to dedupe on, from `x-ankusa-idempotency-key`; `null`
   * when absent or empty (a sender that predates the header).
   */
  idempotencyKey: string | null;
  /**
   * When the edge accepted the hook, in unix milliseconds, from
   * `x-ankusa-received-at`; `null` when absent, empty or not 1 to 15 decimal
   * digits (advisory: never an error).
   */
  receivedAt: number | null;
  /**
   * The lowercase hex SHA-256 of the body Ankusa sent, from
   * `x-ankusa-body-sha256`; `null` when absent or not exactly 64 lowercase hex
   * characters (advisory: never an error).
   */
  bodySha256: string | null;
  /** How Ankusa verified the hook at ingest; `null` when absent or empty. */
  verification: Verification | null;
};

/** How Ankusa verified the hook at ingest. */
export type Verification = {
  /** `ok`, `failed` or `skipped`; other values pass through. */
  status: string;
  /** `true` when a failed check was accepted (`on_verify_failure: accept_flag`): treat as unverified. */
  flagged: boolean;
};

/**
 * Any header mapping a receiver might have: a `Headers`, a plain object, or
 * Node's `IncomingHttpHeaders` (whose values may be arrays). Lookup is always
 * case-insensitive regardless of what the mapping itself does.
 */
export type HeaderSource = Headers | Record<string, string | string[] | undefined>;

/**
 * Raised by `parseHeaders` when `x-ankusa-id` is absent.
 *
 * Every other Ankusa header is optional; this one is the identity a receiver
 * dedupes on, so a delivery without it is a framework bug, not a malformed but
 * tolerable request.
 */
export class MissingHookIdError extends Error {}

/**
 * Parse the `x-ankusa-*` headers of one delivery.
 *
 * `headers` may be any header mapping — a `Headers`, a plain object from a
 * framework, or Node's `IncomingHttpHeaders`.
 *
 * Throws `MissingHookIdError` if `x-ankusa-id` is absent or empty.
 */
export function parseHeaders(headers: HeaderSource): HookHeaders {
  const lowered = new Map<string, string>();
  const entries: Iterable<[string, string | string[] | undefined]> =
    headers instanceof Headers ? headers.entries() : Object.entries(headers);
  for (const [name, value] of entries) {
    if (value === undefined) continue;
    lowered.set(name.toLowerCase(), Array.isArray(value) ? value[0] : value);
  }

  const id = lowered.get("x-ankusa-id");
  if (!id) {
    throw new MissingHookIdError("missing x-ankusa-id header");
  }

  return {
    id,
    source: lowered.get("x-ankusa-source") ?? "",
    tenant: lowered.get("x-ankusa-tenant") ?? null,
    contentType: lowered.get("content-type") ?? null,
    // An empty value is the same as absent: a receiver must not build an
    // `"source:"` key around nothing.
    dedupeKey: lowered.get("x-ankusa-dedupe-key") || null,
    replayId: lowered.get("x-ankusa-replay-id") || null,
    idempotencyKey: lowered.get("x-ankusa-idempotency-key") || null,
    receivedAt: parseReceivedAt(lowered.get("x-ankusa-received-at")),
    bodySha256: parseBodySha256(lowered.get("x-ankusa-body-sha256")),
    verification: parseVerificationHeaders(lowered),
  };
}

function parseVerificationHeaders(lowered: Map<string, string>): Verification | null {
  const status = lowered.get("x-ankusa-verification");
  if (!status) return null;
  return { status, flagged: lowered.get("x-ankusa-verification-flagged") === "true" };
}

// Advisory metadata: a malformed value is `null`, never an error, so a sender
// that predates the header, or sends something else, still parses.
function parseReceivedAt(value: string | undefined): number | null {
  return value !== undefined && /^[0-9]{1,15}$/.test(value) ? Number(value) : null;
}

function parseBodySha256(value: string | undefined): string | null {
  return value !== undefined && /^[0-9a-f]{64}$/.test(value) ? value : null;
}
