"""Errors raised by the source-management client (`ankusa.sources`)."""

from __future__ import annotations

from typing import Any

__all__ = [
    "SourcesError",
    "SourceNotFoundError",
    "SourceConflictError",
    "SourceStoreReadOnlyError",
    "SourceInvalidError",
    "SourcesUnavailableError",
    "VersionMismatchError",
]


class SourcesError(Exception):
    """Base for every error the admin client raises.

    Every subclass carries the HTTP ``status`` and decoded ``body`` of the
    response that produced it (both ``None`` when no response was involved,
    e.g. a transport failure).
    """

    status: int | None = None
    body: Any = None

    def __init__(self, message: str, status: int | None = None, body: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.body = body


class SourceNotFoundError(SourcesError):
    """``404``: no such source for this tenant."""


class SourceConflictError(SourcesError):
    """``409 source_exists``: a source with that name already exists."""


class SourceStoreReadOnlyError(SourcesError):
    """``409 source_store_read_only``: the deployment's source store is a
    static seed, so writes are impossible."""


class SourceInvalidError(SourcesError):
    """``400``: bad tenant, bad source name, or a spec the server rejected.

    ``.message`` is the server's own ``message`` (for a bad spec) or the
    ``error`` code (for a bad tenant/name, which has no message).
    """

    message: str

    def __init__(self, message: str, status: int | None = None, body: Any = None) -> None:
        super().__init__(message, status, body)
        self.message = message


class SourcesUnavailableError(SourcesError):
    """The admin API is unreachable, timed out, or answered ``5xx``. Safe to retry."""


class VersionMismatchError(SourcesError):
    """``GET /health`` reported a version other than ``expected_version``."""

