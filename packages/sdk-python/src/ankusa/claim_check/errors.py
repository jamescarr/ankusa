"""Every failure the claim-check client can raise.

``retryable`` is the whole point of this hierarchy: a caller (a queue
consumer, typically) needs exactly one bit -- dead-letter or retry -- and
nothing here requires it to know the gateway's status codes to get that
right.

Non-retryable: the ref is malformed, the gateway said ``404``/other ``4xx``,
or the bytes that came back don't match the ref's declared size/sha256.
Retryable: the gateway said ``5xx``/``503``, or the request never completed
(network error, timeout).
"""

from __future__ import annotations

from typing import Any


class ClaimCheckError(Exception):
    """Base for every error this client raises. See module docstring."""

    retryable: bool = False


class InvalidClaimRefError(ClaimCheckError):
    """The ref string isn't a ``urn:ankusa:claim:v1:...`` claim-check ref."""

    retryable = False


class ClaimNotFoundError(ClaimCheckError):
    """The gateway returned ``404``: no such object, expired by retention or never written."""

    retryable = False


class ClaimRejectedError(ClaimCheckError):
    """The gateway rejected the request (``400``, ``416``, or any other non-404 ``4xx``)."""

    retryable = False

    def __init__(self, message: str, status: int, body: Any) -> None:
        super().__init__(message)
        self.status = status
        self.body = body


class ClaimIntegrityError(ClaimCheckError):
    """The bytes the gateway returned don't match the ref: wrong length, or the
    sha256 doesn't match. The gateway itself never checks this -- see
    "Redeem a claim" in docs/claim-check.md -- so this is the reader's own
    end-to-end check, always run before ``redeem()`` returns.
    """

    retryable = False


class ClaimCheckUnavailableError(ClaimCheckError):
    """The gateway is unreachable, or answered ``5xx``/``503``. Safe to retry."""

    retryable = True

    def __init__(self, message: str, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.cause = cause
