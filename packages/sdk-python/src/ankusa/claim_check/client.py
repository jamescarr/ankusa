"""Client for the ``:claim_check`` role's read-only byte transport."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from types import TracebackType
from typing import Any, Self

import httpx

from .errors import (
    ClaimCheckUnavailableError,
    ClaimIntegrityError,
    ClaimNotFoundError,
    ClaimRejectedError,
    InvalidClaimRefError,
)
from .ref import ParsedClaimRef, parse_claim_ref

__all__ = ["ClaimCheckClient"]

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


class ClaimCheckClient:
    """Redeem claim-check refs against a deployment's claim-check gateway.

    The gateway itself does no authentication or authorization (see
    "docs/claim-check.md") -- ``headers`` is for whatever a deployer's own
    boundary (service mesh, an API gateway) expects in front of it.
    """

    def __init__(
        self,
        base_url: str,
        *,
        headers: Mapping[str, str] | None = None,
        timeout: float = 10.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._http = httpx.Client(
            base_url=base_url,
            headers=dict(headers) if headers else None,
            timeout=timeout,
            transport=transport,
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def redeem(self, ref: str, sha256: str) -> bytes:
        """Redeem a claim-check ref: fetch its bytes and verify them against
        ``sha256`` (the queue message's ``sha256`` field, 64-char lowercase
        hex) before returning. The gateway does
        not check integrity itself -- see "Redeem a claim" in
        docs/claim-check.md -- so this end-to-end check always runs here.

        Raises a ``ClaimCheckError``; check ``.retryable`` to sort a failure
        into dead-letter (``False``) or retry (``True``).
        """
        parsed = parse_claim_ref(ref)
        if not isinstance(sha256, str) or _SHA256_PATTERN.fullmatch(sha256) is None:
            raise InvalidClaimRefError(f"invalid claim sha256: {sha256!r}")
        body = self._fetch_bytes(parsed)
        _verify_integrity(parsed, sha256, body)
        return body

    def health(self) -> dict[str, Any]:
        """Liveness probe: ``GET /health``."""
        try:
            response = self._http.get("/health")
        except httpx.HTTPError as err:
            raise ClaimCheckUnavailableError(f"claim-check gateway unreachable: {err}", err) from err

        if response.status_code != 200:
            raise ClaimCheckUnavailableError(f"claim-check gateway health check failed ({response.status_code})")
        try:
            data: dict[str, Any] = response.json()
        except ValueError as err:
            raise ClaimCheckUnavailableError(
                f"claim-check gateway health check returned a non-JSON body ({response.status_code})", err
            ) from err
        return data

    def _fetch_bytes(self, parsed: ParsedClaimRef) -> bytes:
        try:
            response = self._http.get(parsed.path)
        except httpx.HTTPError as err:
            raise ClaimCheckUnavailableError(f"claim-check gateway unreachable: {err}", err) from err

        status = response.status_code
        if status == 404:
            raise ClaimNotFoundError(f"claim not found: {parsed.tenant_id}/{parsed.claim_id}")
        if 400 <= status < 500:
            raise ClaimRejectedError(
                f"claim-check rejected redeem ({status}): {_error_body(response)!r}", status, _error_body(response)
            )
        if status != 200:
            raise ClaimCheckUnavailableError(f"claim-check gateway error ({status}): {_error_body(response)!r}")
        return response.content


def _error_body(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text


def _verify_integrity(parsed: ParsedClaimRef, expected_sha256: str, body: bytes) -> None:
    """Integrity is checked here, end to end, by the actual redeemer -- never
    trusted from the gateway. Same discipline ``Ankusa.ClaimCheck.redeem/3``
    applies on the Elixir side. A matching sha256 implies the size.
    """
    if hashlib.sha256(body).hexdigest() != expected_sha256:
        raise ClaimIntegrityError(f"claim sha256 mismatch for {parsed.tenant_id}/{parsed.claim_id}")
