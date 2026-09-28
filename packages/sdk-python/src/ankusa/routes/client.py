"""Client for the route-management listener (``routes.admin.port``, default 4003).

This is the operator surface for the route table the edge enforces:
``Ankusa.Routes.Router``. It manages route definitions and the global IP rules,
and offers a dry run that replays the guard's decision without capturing
anything.

Like the claim-check client, the listener itself performs no authentication --
``headers`` is for whatever a deployer's own boundary (service mesh, an API
gateway) expects in front of it.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import TracebackType
from typing import Any, Self

import httpx

from .errors import RouteNotFoundError, RoutesRejectedError, RoutesUnavailableError

__all__ = ["RoutesClient"]


class RoutesClient:
    """Manage routes and global IP rules on the route-management listener."""

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
        """Liveness probe: ``GET /health`` -> ``{status, routes}``."""
        return self._json(self._request("GET", "/health"))

    def list_routes(
        self,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """``GET /admin/routes`` -> a page of route definitions.

        ``params`` may carry ``enabled``, ``limit`` and ``cursor``; absent keys
        are omitted from the query string rather than sent as ``=undefined``.
        """
        return self._json(self._request("GET", "/admin/routes", params=_query(params)))

    def create_route(self, input: Mapping[str, Any]) -> dict[str, Any]:
        """``POST /admin/routes`` -> the stored route, timestamps included."""
        return self._json(self._request("POST", "/admin/routes", json=dict(input)))

    def get_route(self, id: str) -> dict[str, Any]:
        """``GET /admin/routes/{id}`` -> the route."""
        return self._json(self._request("GET", f"/admin/routes/{id}"))

    def replace_route(self, id: str, input: Mapping[str, Any]) -> dict[str, Any]:
        """``PUT /admin/routes/{id}`` -> the replaced (or created) route."""
        return self._json(self._request("PUT", f"/admin/routes/{id}", json=dict(input)))

    def update_route(self, id: str, patch: Mapping[str, Any]) -> dict[str, Any]:
        """``PATCH /admin/routes/{id}`` -> the patched route."""
        return self._json(self._request("PATCH", f"/admin/routes/{id}", json=dict(patch)))

    def delete_route(self, id: str) -> None:
        """``DELETE /admin/routes/{id}`` (``204``, no body)."""
        self._request("DELETE", f"/admin/routes/{id}")

    def get_ip_rules(self) -> dict[str, Any]:
        """``GET /admin/ip-rules`` -> the global IP rules."""
        return self._json(self._request("GET", "/admin/ip-rules"))

    def put_ip_rules(self, rules: Mapping[str, Any]) -> dict[str, Any]:
        """``PUT /admin/ip-rules`` -> the stored rules, as parsed."""
        return self._json(self._request("PUT", "/admin/ip-rules", json=dict(rules)))

    def test_route(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """``POST /admin/routes/test`` -> the dry-run decision for ``request``."""
        return self._json(self._request("POST", "/admin/routes/test", json=dict(request)))

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
            raise RoutesUnavailableError(f"routes listener unreachable: {err}", err) from err
        _raise_for_status(response)
        return response

    @staticmethod
    def _json(response: httpx.Response) -> dict[str, Any]:
        try:
            data: dict[str, Any] = response.json()
        except ValueError as err:
            raise RoutesUnavailableError(
                f"routes listener returned a non-JSON body ({response.status_code})", err
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
    if status == 404:
        raise RouteNotFoundError(f"route not found ({status})")
    if 400 <= status < 500:
        body = _error_body(response)
        raise RoutesRejectedError(
            f"routes listener rejected the request ({status}): {body!r}",
            status,
            body.get("error") if isinstance(body, dict) else None,
            field=body.get("field") if isinstance(body, dict) else None,
            detail=body.get("message") if isinstance(body, dict) else None,
            conflicting_id=body.get("conflicting_id") if isinstance(body, dict) else None,
            max_routes=body.get("max_routes") if isinstance(body, dict) else None,
        )
    raise RoutesUnavailableError(f"routes listener error ({status}): {_error_body(response)!r}")


def _error_body(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text
