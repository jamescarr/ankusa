"""Every failure the routes client can raise.

``retryable`` is the whole point of this hierarchy, exactly as in the
claim-check client: a caller (an operator script, a controller loop) needs one
bit -- leave the table alone and retry, or surface the rejection -- and nothing
here requires it to know the listener's status codes to get that right.

Non-retryable: the listener said ``404`` (no such route) or another ``4xx``
(a rejected write, a duplicate, the cap), or the id was unusable before any
request was sent (``InvalidRouteIdError``). Retryable: the listener said
``5xx``/``503 store_unavailable``, or the request never completed (network
error, timeout).
"""

from __future__ import annotations

from typing import Any


class RoutesError(Exception):
    """Base for every error this client raises. See module docstring."""

    retryable: bool = False


class InvalidRouteIdError(RoutesError):
    """The route ``id`` cannot be used to build a path.

    Raised before any request is sent. An id that isn't a string, is empty, or
    is exactly ``.`` or ``..`` is refused: URL parsers normalize those away, so
    ``get_route("..")`` would quietly hit ``/admin/`` and return the list page
    as if it were a route. Every other id is percent-encoded as one path
    segment, never refused.
    """

    retryable = False


class RoutesUnavailableError(RoutesError):
    """The listener is unreachable, or answered ``5xx``/``503``. Safe to retry."""

    retryable = True

    def __init__(self, message: str, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.cause = cause


class RouteNotFoundError(RoutesError):
    """The listener returned ``404``: no such route."""

    retryable = False


class RoutesRejectedError(RoutesError):
    """The listener rejected the request (``400`` or any other non-404 ``4xx``).

    ``code`` is the body's ``error`` field (``invalid_route``,
    ``duplicate_route``, ``too_many_routes``, ...); ``field``, ``message``,
    ``conflicting_id`` and ``max_routes`` are carried through when the body
    supplies them.
    """

    retryable = False

    def __init__(
        self,
        message: str,
        status: int,
        code: str | None,
        *,
        field: Any = None,
        detail: Any = None,
        conflicting_id: Any = None,
        max_routes: Any = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.field = field
        self.message = detail
        self.conflicting_id = conflicting_id
        self.max_routes = max_routes
