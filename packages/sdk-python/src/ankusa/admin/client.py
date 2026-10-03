"""Client for the operator listener (``admin.port``, default 4002).

This is ``Ankusa.Admin.Router``: health, Prometheus metrics, the redacted
configuration, the dead-letter queue, replay jobs, and the quarantine list.
Responses are node-local by design, so a fleet operator scrapes every node's
admin port.

Like the claim-check client, the listener itself performs no authentication --
``headers`` is for whatever a deployer's own boundary (service mesh, an API
gateway) expects in front of it.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import TracebackType
from typing import Any, Self
from urllib.parse import quote

import httpx

from .errors import AdminRejectedError, AdminUnavailableError, RoleNotEnabledError

__all__ = ["AdminClient"]


class AdminClient:
    """Operate against the operator API: health, metrics, config, DLQ, quarantine."""

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

    def health(self) -> dict[str, Any]:
        """Liveness probe: ``GET /health`` -> ``{status, instance, roles}``."""
        return self._json(self._request("GET", "/health"))

    def metrics(self) -> str:
        """``GET /metrics`` -> the Prometheus text exposition body."""
        return self._request("GET", "/metrics").text

    def config(self) -> dict[str, Any]:
        """``GET /v1/config`` -> the effective, redacted configuration."""
        return self._json(self._request("GET", "/v1/config"))

    def list_dead_letters(
        self,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """``GET /v1/dlq`` -> a page of dead-lettered hooks, newest first.

        ``params`` may carry ``source_id``, ``since`` and ``limit``; absent keys
        are omitted from the query string rather than sent as ``=undefined``.
        """
        return self._json(self._request("GET", "/v1/dlq", params=_query(params)))

    def create_replay(self, spec: Mapping[str, Any]) -> dict[str, Any]:
        """``POST /v1/replays`` -> the replay job (202 for a new one, 200 for an
        existing ``running``/``paused`` job with the same filter, so a proxy
        retry is idempotent).

        ``spec`` is the request body verbatim: ``kind`` (``"dlq"`` or
        ``"archive"``) plus that kind's bounds and any optional ``rate`` /
        ``max_lag_ms``.
        """
        return self._json(self._request("POST", "/v1/replays", json=dict(spec)))

    def get_replay(self, replay_id: str) -> dict[str, Any]:
        """``GET /v1/replays/{id}`` -> the replay job; ``404`` raises
        ``AdminRejectedError`` with ``code="replay_not_found"``."""
        return self._json(self._request("GET", f"/v1/replays/{quote(replay_id, safe='')}"))

    def list_replays(self) -> dict[str, Any]:
        """``GET /v1/replays`` -> ``{"replays": [...]}``, newest first."""
        return self._json(self._request("GET", "/v1/replays"))

    def update_replay(self, replay_id: str, patch: Mapping[str, Any]) -> dict[str, Any]:
        """``PATCH /v1/replays/{id}`` -> the replay job.

        ``patch`` may carry ``state`` (``"running"``, ``"paused"`` or
        ``"cancelled"``), ``rate`` and ``max_lag_ms``.
        """
        return self._json(
            self._request("PATCH", f"/v1/replays/{quote(replay_id, safe='')}", json=dict(patch))
        )

    def list_quarantined(
        self,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """``GET /v1/quarantine`` -> recent quarantined hooks, newest first.

        ``params`` may carry ``limit``; absent keys are omitted from the query
        string rather than sent as ``=undefined``.
        """
        return self._json(self._request("GET", "/v1/quarantine", params=_query(params)))

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Mapping[str, Any] | None = None,
    ) -> httpx.Response:
        try:
            response = self._http.request(method, path, params=params, json=json)
        except httpx.HTTPError as err:
            raise AdminUnavailableError(f"admin listener unreachable: {err}", err) from err
        _raise_for_status(response)
        return response

    @staticmethod
    def _json(response: httpx.Response) -> dict[str, Any]:
        try:
            data: dict[str, Any] = response.json()
        except ValueError as err:
            raise AdminUnavailableError(
                f"admin listener returned a non-JSON body ({response.status_code})", err
            ) from err
        return data


def _query(params: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    """Drop absent query parameters; ``None`` is not sent at all."""
    if params is None:
        return None
    return {key: value for key, value in params.items() if value is not None}


def _raise_for_status(response: httpx.Response) -> None:
    status = response.status_code
    if 200 <= status < 300:
        return
    body = _error_body(response)
    code = body.get("error") if isinstance(body, dict) else None
    if status == 409 and code == "role_not_enabled":
        role = body.get("role") if isinstance(body, dict) else None
        raise RoleNotEnabledError(f"role not enabled on this node: {role!r}", role)
    if 400 <= status < 500:
        raise AdminRejectedError(f"admin listener rejected the request ({status}): {body!r}", status, code)
    # Everything else that isn't 2xx is retryable: 1xx, an unfollowed 3xx
    # redirect, and 5xx.
    raise AdminUnavailableError(f"admin listener error ({status}): {body!r}")


def _error_body(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text
