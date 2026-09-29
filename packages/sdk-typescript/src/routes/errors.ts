/**
 * Every failure the routes client can raise. `retryable` is the whole point of
 * this hierarchy, mirroring the claim-check client: a caller needs exactly one
 * bit — retry or don't — without decoding the route listener's status codes.
 *
 * Non-retryable: the listener said `404` (no such route), or rejected the
 * request with any other `4xx` (invalid route, duplicate, too many routes).
 * Retryable: the listener said `5xx`/`503` (store unavailable), or the request
 * never completed (network error, timeout).
 */
export abstract class RoutesError extends Error {
  abstract readonly retryable: boolean;
}

/** The route listener answered `404`: no such route. */
export class RouteNotFoundError extends RoutesError {
  readonly retryable = false;
}

/**
 * The route listener rejected the request (any `4xx` other than `404`).
 *
 * Carries `status` and `code` (the body's `error` field), plus, when the body
 * provides them, `field`, `message` (also the `Error` message), and the
 * `duplicate_route`/`too_many_routes` detail fields `conflicting_id` and
 * `max_routes`.
 */
export class RoutesRejectedError extends RoutesError {
  readonly retryable = false;

  constructor(
    readonly status: number,
    readonly code?: string,
    readonly field?: string,
    message?: string,
    readonly conflicting_id?: string,
    readonly max_routes?: number,
  ) {
    super(message ?? `routes rejected (${status})${code ? `: ${code}` : ""}`);
  }
}

/** The route listener is unreachable, or answered `5xx`/`503`. Safe to retry. */
export class RoutesUnavailableError extends RoutesError {
  readonly retryable = true;

  constructor(
    message: string,
    readonly cause?: unknown,
  ) {
    super(message);
  }
}
