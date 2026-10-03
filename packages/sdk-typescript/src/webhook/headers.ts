/**
 * Parse the headers Ankusa's HTTP sink attaches to every delivery.
 *
 * See "HTTP handoff" in docs/integrations.md for the full contract this
 * mirrors: the raw body arrives verbatim, and identity travels in
 * `x-ankusa-id`, `x-ankusa-source`, `x-ankusa-tenant` (only when the source
 * has a tenant), `content-type`, and — when the source has a dedupe rule or
 * the delivery is a replay — `x-ankusa-dedupe-key` and
 * `x-ankusa-replay-id`. A receiver dedupes on the key `idempotencyKey` derives
 * (`x-ankusa-dedupe-key` when set, else `x-ankusa-id`): delivery is
 * at-least-once, so the same hook can arrive twice after a retry.
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
  };
}
