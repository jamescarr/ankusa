/**
 * Every failure the admin client can raise. `retryable` is the whole point of
 * this hierarchy, mirroring the claim-check client: a caller needs exactly one
 * bit — retry or don't — without decoding the operator API's status codes.
 *
 * Non-retryable: the node answered `409 role_not_enabled` (this node does not
 * run the role that route needs), or rejected the request with any other
 * `4xx`.
 * Retryable: the node answered `5xx` or an unfollowed `3xx`, or the request
 * never completed (network error, timeout).
 */
export abstract class AdminError extends Error {
  abstract readonly retryable: boolean;
}

/**
 * The node answered `409 role_not_enabled`: it does not run the role the
 * requested route needs. Ask another node. Carries `role`.
 */
export class RoleNotEnabledError extends AdminError {
  readonly retryable = false;

  constructor(
    message: string,
    readonly role?: string,
  ) {
    super(message);
  }
}

/** The node rejected the request (any `4xx` other than `role_not_enabled`). */
export class AdminRejectedError extends AdminError {
  readonly retryable = false;

  constructor(
    message: string,
    readonly status: number,
    readonly code?: string,
  ) {
    super(message);
  }
}

/**
 * The node is unreachable, or answered a non-2xx that is not a `4xx` (`5xx`, an
 * unfollowed `3xx`). Safe to retry.
 */
export class AdminUnavailableError extends AdminError {
  readonly retryable = true;

  constructor(
    message: string,
    readonly cause?: unknown,
  ) {
    super(message);
  }
}
