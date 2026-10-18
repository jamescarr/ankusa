/**
 * The receiver-side decision on one HTTP-sink delivery: is it signed, and what
 * is its identity.
 *
 * Ankusa's HTTP sink signs every delivery unless it is configured
 * `allow_unsigned: true`, so a receiver is closed by default: without the
 * sink's secret (or an explicit `allowUnsigned: true`) it refuses every
 * delivery. Otherwise any host that can reach the receiver's URL could inject
 * hooks.
 */
import { type HeaderSource, type HookHeaders, parseHeaders } from "./headers.js";
import { InvalidSignatureError, verifySignature } from "./signature.js";

export type ReceiveDeliveryOptions = {
  headers: HeaderSource;
  /** The raw request body, exactly as received. */
  body: string | Uint8Array;
  /**
   * The sink's secret (`whsec_` + base64, or any other string used as its own
   * UTF-8 bytes), or a list of them during a rotation. With any, the delivery
   * MUST verify; `allowUnsigned` does not weaken it.
   */
  secrets?: string | string[];
  /** Accept a delivery with no secret configured, unverified. Default `false`. */
  allowUnsigned?: boolean;
  /** `webhook-timestamp` tolerance in seconds. Default 300. */
  toleranceSeconds?: number;
  /** Unix seconds; defaults to the clock. */
  now?: number;
};

/** An accepted delivery: its identity, and whether its signature verified. */
export type ReceivedDelivery = HookHeaders & {
  /** `false` only for an unsigned delivery `allowUnsigned` let through. */
  verified: boolean;
};

/**
 * Judge one delivery. Throws `InvalidSignatureError` (answer `401`) when it has
 * no valid signature — including `invalid_secret` when no secret is configured
 * and `allowUnsigned` is not set — and `MissingHookIdError` (`400`) when it has
 * no `x-ankusa-id`. The signature is judged first.
 */
export function receiveDelivery(options: ReceiveDeliveryOptions): ReceivedDelivery {
  const secrets = options.secrets === undefined ? [] : Array.isArray(options.secrets) ? options.secrets : [options.secrets];

  let verified: boolean;
  if (secrets.length > 0) {
    verifySignature({
      headers: options.headers,
      body: options.body,
      secrets,
      ...(options.toleranceSeconds !== undefined && { toleranceSeconds: options.toleranceSeconds }),
      ...(options.now !== undefined && { now: options.now }),
    });
    verified = true;
  } else if (options.allowUnsigned === true) {
    verified = false;
  } else {
    throw new InvalidSignatureError(
      "invalid_secret",
      null,
      "no secret configured: deliveries are signed, so the receiver needs the sink's secret (or allowUnsigned: true)",
    );
  }

  return { ...parseHeaders(options.headers), verified };
}
