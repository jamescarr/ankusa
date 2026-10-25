import { InvalidMessageError } from "../message/index.js";

/**
 * A decoded lifecycle event: the CloudEvents 1.0 (structured mode) event an
 * Ankusa instance publishes when a source or a route is created, updated or
 * deleted, when a tenant is erased, or when the instance is restored from a
 * backup. The field names are the wire's.
 *
 * It arrives as the body of a queue message from the instance's lifecycle sinks
 * (content type `application/cloudevents+json`, `source_id`
 * `ankusa:lifecycle`): decode the message with `decodeMessage`, redeem its body,
 * and hand the bytes to `decodeLifecycle`.
 *
 * Delivery is at least once and in commit order. `id` is derived from the
 * change, so the same event always has the same id, and `revision` (per
 * `subject`) lets a consumer ignore a stale update.
 */
export type LifecycleEvent = {
  specversion: "1.0";
  id: string;
  source: string;
  /** `io.ankusa.source.created`, `io.ankusa.route.updated`, …; a type this SDK does not know passes through. */
  type: string;
  subject: string;
  /** When the change committed, as the wire says it (an RFC 3339 string, not parsed). */
  time: string;
  datacontenttype: string | null;
  /** The entity as the admin API shows it, secrets redacted; `null` when the event carries none. */
  data: Record<string, unknown> | null;
  revision: number | null;
  actor: string | null;
  instance: string | null;
  /** The instance's audit sequence number, the decimal string it is on the wire. */
  sequence: string | null;
  replayid: string | null;
  traceparent: string | null;
  tracestate: string | null;
};

function requiredString(raw: Record<string, unknown>, key: string): string {
  const value = raw[key];
  if (typeof value !== "string" || value.length === 0) throw new InvalidMessageError("invalid_field", key);
  return value;
}

function optionalString(raw: Record<string, unknown>, key: string): string | null {
  const value = raw[key];
  if (value === undefined || value === null) return null;
  if (typeof value !== "string") throw new InvalidMessageError("invalid_field", key);
  return value;
}

/**
 * Decode one lifecycle event (the JSON of the CloudEvent; a string or its
 * UTF-8 bytes). Every failure raises `InvalidMessageError` with
 * `retryable=false`: `invalid_json`, `not_an_object`, `unsupported_version`
 * (`specversion` is not `"1.0"`) or `invalid_field` with the offending `field`.
 * The checks run in a fixed order and the first failure wins. Keys and event
 * types this SDK does not know are ignored.
 */
export function decodeLifecycle(data: string | Uint8Array): LifecycleEvent {
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

  if (raw.specversion !== "1.0") throw new InvalidMessageError("unsupported_version");

  const id = requiredString(raw, "id");
  const source = requiredString(raw, "source");
  const type = requiredString(raw, "type");
  const subject = requiredString(raw, "subject");
  const time = requiredString(raw, "time");
  const datacontenttype = optionalString(raw, "datacontenttype");

  const body = raw.data;
  if (body !== undefined && body !== null && (typeof body !== "object" || Array.isArray(body))) {
    throw new InvalidMessageError("invalid_field", "data");
  }

  const revision = raw.revision;
  if (revision !== undefined && revision !== null) {
    if (typeof revision !== "number" || !Number.isInteger(revision) || revision < 1) {
      throw new InvalidMessageError("invalid_field", "revision");
    }
  }

  const actor = optionalString(raw, "actor");
  const instance = optionalString(raw, "instance");

  const sequence = raw.sequence;
  if (sequence !== undefined && sequence !== null) {
    if (typeof sequence !== "string" || !/^[0-9]+$/.test(sequence)) {
      throw new InvalidMessageError("invalid_field", "sequence");
    }
  }

  return {
    specversion: "1.0",
    id,
    source,
    type,
    subject,
    time,
    datacontenttype,
    data: (body ?? null) as Record<string, unknown> | null,
    revision: (revision ?? null) as number | null,
    actor,
    instance,
    sequence: (sequence ?? null) as string | null,
    replayid: optionalString(raw, "replayid"),
    traceparent: optionalString(raw, "traceparent"),
    tracestate: optionalString(raw, "tracestate"),
  };
}
