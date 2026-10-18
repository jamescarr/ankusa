"""Client for Ankusa's tenant-scoped webhook sources.

``Ankusa.Admin.Router`` (its own port, 4002) exposes a small JSON API for
managing a tenant's ingest sources: list, get, create, update, delete. A
tenant is the operator's own customer identifier and a source id is
``"<tenant>.<name>"``; this client speaks in those terms and builds the paths
for you.

The router does no authentication (by design, like the rest of this port),
and every source it returns has its secrets redacted (``secret``,
``password``, ``token``, http sink header values, URL userinfo passwords,
...). So a ``Source`` read back here is never useful for editing: resending
its ``verify`` map is not the same as resending the stored secret. Callers
that hold the secret supply it through ``SourceSpec``.

Only ``^[A-Za-z0-9_-]{1,64}$`` tenants and names are accepted, and both are
checked before any path is built, so a caller-supplied name cannot escape its
tenant through URL normalization.

``expected_version`` is an optional safety latch: when set, the first API call
fetches ``GET /health`` once, compares its ``"version"`` field against the
expected value, caches the fetched version, and raises
``VersionMismatchError`` on any mismatch. Every subsequent call re-checks the
cached value without another request.
"""

from __future__ import annotations

import re
from types import TracebackType
from typing import Any, Self

import httpx

from .errors import (
    SourceConflictError,
    SourceInvalidError,
    SourceNotFoundError,
    SourceStoreReadOnlyError,
    SourcesUnavailableError,
    VersionMismatchError,
)
from .spec import Source, SourceSpec

__all__ = ["SourcesClient"]

# The same rule `Ankusa.ClaimCheck.Ref` uses for its tenant. Anything outside
# it is rejected before a path is built: httpx normalizes dot segments, so an
# unvalidated "../" would escape the tenant scope before the server sees it.
_SAFE_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


class SourcesClient:
    """Manage a deployment's tenant-scoped sources via ``Ankusa.Admin.Router``."""

    def __init__(
        self,
        base_url: str,
        *,
        expected_version: str | None = None,
        timeout: float = 10.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._http = httpx.Client(base_url=base_url, timeout=timeout, transport=transport)
        self._expected_version = expected_version
        self._server_version: str | None = None

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

    def server_version(self) -> str:
        """The deployment's Ankusa version: ``GET /health ["version"]``.

        Fetched once and cached; when ``expected_version`` was set this also
        enforces it, so a mismatched deployment raises ``VersionMismatchError``
        here too.
        """
        if self._server_version is None:
            self._server_version = self._fetch_version()
        self._check_version()
        return self._server_version

    def list_sources(self, tenant: str) -> list[Source]:
        """List a tenant's sources: ``GET /v1/tenants/<tenant>/sources``."""
        _validate_tenant(tenant)
        self._ensure_version()
        response = self._request("GET", f"/v1/tenants/{tenant}/sources")
        self._raise_for_status(response)
        data = response.json()
        return [Source.from_json(entry) for entry in data["entries"]]

    def get_source(self, tenant: str, name: str) -> Source:
        """Fetch one source: ``GET /v1/tenants/<tenant>/sources/<name>``."""
        _validate_tenant(tenant)
        _validate_name(name)
        self._ensure_version()
        response = self._request("GET", f"/v1/tenants/{tenant}/sources/{name}")
        self._raise_for_status(response)
        return Source.from_json(response.json())

    def create_source(self, tenant: str, name: str, spec: SourceSpec) -> Source:
        """Create a source: ``POST /v1/tenants/<tenant>/sources``.

        The source name travels in the body (plus ``name``); the tenant comes
        from the URL and wins over any ``"tenant"`` key inside ``spec``.
        """
        _validate_tenant(tenant)
        _validate_name(name)
        self._ensure_version()
        body = spec.to_json()
        body["name"] = name
        response = self._request("POST", f"/v1/tenants/{tenant}/sources", json=body)
        self._raise_for_status(response)
        return Source.from_json(response.json())

    def update_source(self, tenant: str, name: str, spec: SourceSpec) -> Source:
        """Replace a source: ``PUT /v1/tenants/<tenant>/sources/<name>``.

        The name comes from the URL; a ``"name"`` key inside ``spec`` is never
        sent (``SourceSpec`` has no such field).
        """
        _validate_tenant(tenant)
        _validate_name(name)
        self._ensure_version()
        response = self._request("PUT", f"/v1/tenants/{tenant}/sources/{name}", json=spec.to_json())
        self._raise_for_status(response)
        return Source.from_json(response.json())

    def delete_source(self, tenant: str, name: str) -> None:
        """Delete a source: ``DELETE /v1/tenants/<tenant>/sources/<name>``.

        Succeeds with no return value (the server answers ``204`` with an
        empty body); a missing source raises ``SourceNotFoundError``. A source
        that still has undelivered hooks on the server's node raises
        ``SourceConflictError`` with ``body["error"] == "source_has_deliveries"``
        (and ``pending``/``inflight`` counts); this method never sends the
        admin API's ``?deliveries=dead_letter``.
        """
        _validate_tenant(tenant)
        _validate_name(name)
        self._ensure_version()
        response = self._request("DELETE", f"/v1/tenants/{tenant}/sources/{name}")
        self._raise_for_status(response)

    def _ensure_version(self) -> None:
        """The optional version latch: only when ``expected_version`` is set does
        the first API call fetch ``/health`` once, cache the version, and enforce it.
        """
        if self._expected_version is None:
            return
        if self._server_version is None:
            self._server_version = self._fetch_version()
        self._check_version()

    def _check_version(self) -> None:
        if self._expected_version is not None and self._server_version != self._expected_version:
            raise VersionMismatchError(
                f"expected Ankusa version {self._expected_version!r}, server reports {self._server_version!r}"
            )

    def _fetch_version(self) -> str:
        response = self._request("GET", "/health")
        status = response.status_code
        if status != 200:
            raise SourcesUnavailableError(
                f"ankusa admin API health check failed ({status}): {_error_body(response)!r}",
                status,
                _error_body(response),
            )
        try:
            data = response.json()
        except ValueError as err:
            raise SourcesUnavailableError(
                f"ankusa admin API health check returned a non-JSON body ({status})"
            ) from err
        version = data.get("version")
        if not isinstance(version, str):
            raise SourcesUnavailableError(
                f"ankusa admin API health check returned no version ({status}): {data!r}"
            )
        return version

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
    ) -> httpx.Response:
        try:
            return self._http.request(method, path, json=json)
        except httpx.HTTPError as err:
            raise SourcesUnavailableError(f"ankusa admin API unreachable: {err}") from err

    def _raise_for_status(self, response: httpx.Response) -> None:
        status = response.status_code
        if 200 <= status < 300:
            return
        body = _error_body(response)
        if status == 404:
            raise SourceNotFoundError(f"source not found ({status}): {body!r}", status, body)
        if status == 400:
            raise SourceInvalidError(_invalid_message(body, status), status, body)
        if status == 409:
            if isinstance(body, dict) and body.get("error") == "source_store_read_only":
                raise SourceStoreReadOnlyError(
                    f"source store is read-only ({status}): {body!r}", status, body
                )
            raise SourceConflictError(f"source already exists ({status}): {body!r}", status, body)
        raise SourcesUnavailableError(f"ankusa admin API error ({status}): {body!r}", status, body)



def _error_body(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text


def _invalid_message(body: Any, status: int) -> str:
    if isinstance(body, dict):
        message = body.get("message") or body.get("error")
        if isinstance(message, str):
            return message
    return f"invalid source ({status}): {body!r}"


def _validate_tenant(tenant: str) -> None:
    """Reject a tenant that is not `^[A-Za-z0-9_-]{1,64}$` before any path is built."""
    if not isinstance(tenant, str) or _SAFE_ID.fullmatch(tenant) is None:
        raise SourceInvalidError(f"invalid tenant: {tenant!r}", status=None)


def _validate_name(name: str) -> None:
    """Reject a source name that is not `^[A-Za-z0-9_-]{1,64}$` before any path is built."""
    if not isinstance(name, str) or _SAFE_ID.fullmatch(name) is None:
        raise SourceInvalidError(f"invalid source name: {name!r}", status=None)
