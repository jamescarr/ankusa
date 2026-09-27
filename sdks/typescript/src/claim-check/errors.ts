/**
 * Every failure the client can raise. `retryable` is the whole point of this
 * hierarchy: a caller (a queue consumer, typically) needs exactly one bit —
 * dead-letter or retry — and nothing here requires it to know the gateway's
 * status codes to get that right.
 *
 * Non-retryable: the ref is malformed, the gateway said `404`/other `4xx`, or
 * the bytes that came back don't match the ref's declared size/sha256.
 * Retryable: the gateway said `5xx`/`503`, or the request never completed
 * (network error, timeout).
 */
export abstract class ClaimCheckError extends Error {
  abstract readonly retryable: boolean;
}

/** The ref string isn't a `urn:ankusa:claim:v1:...` claim-check ref. */
export class InvalidClaimRefError extends ClaimCheckError {
  readonly retryable = false;
}

/** The gateway returned `404`: no such object, expired by retention or never written. */
export class ClaimNotFoundError extends ClaimCheckError {
  readonly retryable = false;
}

/** The gateway rejected the request (`400`, `416`, or any other non-404 `4xx`). */
export class ClaimRejectedError extends ClaimCheckError {
  readonly retryable = false;

  constructor(
    message: string,
    readonly status: number,
    readonly body: unknown,
  ) {
    super(message);
  }
}

/**
 * The bytes the gateway returned don't match the ref: wrong length, or the
 * sha256 doesn't match. The gateway itself never checks this — see
 * "Redeem a claim" in docs/claim-check.md — so this is the reader's own
 * end-to-end check, always run before `redeem()` returns.
 */
export class ClaimIntegrityError extends ClaimCheckError {
  readonly retryable = false;
}

/** The gateway is unreachable, or answered `5xx`/`503`. Safe to retry. */
export class ClaimCheckUnavailableError extends ClaimCheckError {
  readonly retryable = true;

  constructor(
    message: string,
    readonly cause?: unknown,
  ) {
    super(message);
  }
}
