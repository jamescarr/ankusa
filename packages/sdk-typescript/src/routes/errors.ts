/**
 * Every failure the routes client can raise. `retryable` is the whole point of
 * this hierarchy, mirroring the claim-check client: a caller needs exactly one
 * bit — retry or don't — without decoding the route listener's status codes.
 *
 * Non-retryable: the listener said `404` (no such route), rejected the request
 * with any other `4xx` (invalid route, duplicate, too many routes), or the id
 * never addressed a route at all (`InvalidRouteIdError`).
 * Retryable: the listener said `5xx`/`503` (store unavailable) or answered an
 * unfollowed `3xx`, or the request never completed (network error, timeout).
 */
export abstract class RoutesError extends Error {
  abstract readonly retryable: boolean;
}

/** The route listener answered `404`: no such route. */
export class RouteNotFoundError extends RoutesError {
  readonly retryable = false;
}

/**
 * The id addresses nothing: it is not a string, is empty, or is exactly `.` or
 * `..`. Raised before any request is made.
 *
 * A URL parser normalizes those away — `..` resolves to the collection
 * endpoint, and an empty id or `.` resolves to it too — so the request would
 * hit the wrong resource (the route list) instead of failing.
 */
export class InvalidRouteIdError extends RoutesError {
  readonly retryable = false;

  constructor(readonly id: unknown) {
    super(`invalid route id: ${JSON.stringify(id)}`);
  }
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

/**
 * The route listener is unreachable, or answered a non-2xx that is not a `4xx`
 * (`5xx`, an unfollowed `3xx`). Safe to retry.
 */
export class RoutesUnavailableError extends RoutesError {
  readonly retryable = true;

  constructor(
    message: string,
    readonly cause?: unknown,
  ) {
    super(message);
  }
}
