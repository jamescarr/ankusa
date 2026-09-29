"""Every failure the admin client can raise.

``retryable`` is the whole point of this hierarchy, exactly as in the
claim-check client: a caller (an operator script, a dashboard) needs one bit --
retry, or surface the rejection -- and nothing here requires it to know the
operator API's status codes to get that right.

Non-retryable: the listener said ``409 role_not_enabled`` (ask another node)
or another ``4xx`` (a rejected filter). Retryable: the listener said ``5xx``,
or the request never completed (network error, timeout).
"""

from __future__ import annotations

from typing import Any


class AdminError(Exception):
    """Base for every error this client raises. See module docstring."""

    retryable: bool = False


class AdminUnavailableError(AdminError):
    """The listener is unreachable, or answered ``5xx``. Safe to retry."""

    retryable = True

    def __init__(self, message: str, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.cause = cause


class RoleNotEnabledError(AdminError):
    """The listener returned ``409 role_not_enabled``: this node does not run
    the role the operation needs. Ask another node; ``role`` names it."""

    retryable = False

    def __init__(self, message: str, role: Any = None) -> None:
        super().__init__(message)
        self.role = role


class AdminRejectedError(AdminError):
    """The listener rejected the request (any other ``4xx``).

    ``status`` is the HTTP status; ``code`` is the body's ``error`` field
    (``invalid_filter``, ...).
    """

    retryable = False

    def __init__(self, message: str, status: int, code: str | None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
