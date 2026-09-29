"""Admin client for Ankusa's tenant-scoped webhook sources.

``Ankusa.Admin.Router`` (its own port, 4002) exposes a small JSON API for
managing a tenant's ingest sources: list, get, create, update, delete. A tenant is a
santati team slug and a source id is ``"<tenant>.<name>"`` -- this client
speaks in those terms and builds the paths for you.

The router does no authentication (by design, like the rest of this port),
and every source it returns has its secrets redacted (``secret``,
``password``, ``token``, http sink header values, URL userinfo passwords,
...). So a ``Source`` read back here is never useful for editing: resending
its ``verify`` map is not the same as resending the stored secret. Callers
that hold the secret supply it through ``SourceSpec``.

``expected_version`` is an optional safety latch: when set, the first API call
fetches ``GET /health`` once, compares its ``"version"`` field against the
expected value, caches the fetched version, and raises
``VersionMismatchError`` on any mismatch. Every subsequent call re-checks the
cached value without another request.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self

import httpx

__all__ = [
    "AdminClient",
    "AdminError",
    "AdminUnavailableError",
    "Source",
    "SourceConflictError",
    "SourceInvalidError",
    "SourceNotFoundError",
    "SourceSpec",
    "SourceStoreReadOnlyError",
    "VersionMismatchError",
]


class AdminError(Exception):
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


class SourceNotFoundError(AdminError):
    """``404``: no such source for this tenant."""


class SourceConflictError(AdminError):
    """``409 source_exists``: a source with that name already exists."""


class SourceStoreReadOnlyError(AdminError):
    """``409 source_store_read_only``: the deployment's source store is a
    static seed, so writes are impossible."""


class SourceInvalidError(AdminError):
    """``400``: bad tenant, bad source name, or a spec the server rejected.

    ``.message`` is the server's own ``message`` (for a bad spec) or the
    ``error`` code (for a bad tenant/name, which has no message).
    """

    message: str

    def __init__(self, message: str, status: int | None = None, body: Any = None) -> None:
        super().__init__(message, status, body)
        self.message = message


class AdminUnavailableError(AdminError):
    """The admin API is unreachable, timed out, or answered ``5xx``. Safe to retry."""


class VersionMismatchError(AdminError):
    """``GET /health`` reported a version other than ``expected_version``."""


@dataclass(frozen=True)
class SourceSpec:
    """The writable fields of a source, as submitted to ``POST``/``PUT``.

    ``verify`` is optional (absent means ``{"type": "none"}`` on the server);
    ``sinks`` is required and must be non-empty -- the server validates all of
    this exactly as the YAML config does.
    """

    sinks: list[dict[str, Any]]
    verify: dict[str, Any] | None = None
    on_verify_failure: str | None = None

    def to_json(self) -> dict[str, Any]:
        """The JSON body for a create/update, omitting unset (``None``) fields."""
        body: dict[str, Any] = {"sinks": self.sinks}
        if self.verify is not None:
            body["verify"] = self.verify
        if self.on_verify_failure is not None:
            body["on_verify_failure"] = self.on_verify_failure
        return body


@dataclass(frozen=True)
class Source:
    """A stored source as the admin API reports it: redacted spec plus the
    derived identity fields."""

    tenant: str
    name: str
    source_id: str
    ingest_path: str
    verify: dict[str, Any]
    on_verify_failure: str | None
    sinks: list[dict[str, Any]]

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> Source:
        return cls(
            tenant=data["tenant"],
            name=data["name"],
            source_id=data["source_id"],
            ingest_path=data["ingest_path"],
            verify=data.get("verify") or {"type": "none"},
            on_verify_failure=data.get("on_verify_failure"),
            sinks=list(data.get("sinks") or []),
        )


class AdminClient:
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
        self._ensure_version()
        response = self._request("GET", f"/v1/tenants/{tenant}/sources")
        self._raise_for_status(response)
        data = response.json()
        return [Source.from_json(entry) for entry in data["entries"]]

    def get_source(self, tenant: str, name: str) -> Source:
        """Fetch one source: ``GET /v1/tenants/<tenant>/sources/<name>``."""
        self._ensure_version()
        response = self._request("GET", f"/v1/tenants/{tenant}/sources/{name}")
        self._raise_for_status(response)
        return Source.from_json(response.json())

    def create_source(self, tenant: str, name: str, spec: SourceSpec) -> Source:
        """Create a source: ``POST /v1/tenants/<tenant>/sources``.

        The source name travels in the body (plus ``name``); the tenant comes
        from the URL and wins over any ``"tenant"`` key inside ``spec``.
        """
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
        self._ensure_version()
        response = self._request("PUT", f"/v1/tenants/{tenant}/sources/{name}", json=spec.to_json())
        self._raise_for_status(response)
        return Source.from_json(response.json())

    def delete_source(self, tenant: str, name: str) -> None:
        """Delete a source: ``DELETE /v1/tenants/<tenant>/sources/<name>``.

        Succeeds with no return value (the server answers ``204`` with an
        empty body); a missing source raises ``SourceNotFoundError``.
        """
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
            raise AdminUnavailableError(
                f"ankusa admin API health check failed ({status}): {_error_body(response)!r}",
                status,
                _error_body(response),
            )
        try:
            data = response.json()
        except ValueError as err:
            raise AdminUnavailableError(
                f"ankusa admin API health check returned a non-JSON body ({status})"
            ) from err
        version = data.get("version")
        if not isinstance(version, str):
            raise AdminUnavailableError(
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
            raise AdminUnavailableError(f"ankusa admin API unreachable: {err}") from err

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
        raise AdminUnavailableError(f"ankusa admin API error ({status}): {body!r}", status, body)


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
