"""Unit tests for the route-management client, mirroring the conformance
vectors: happy paths, error classification, and query-parameter behavior."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

import ankusa


def _transport(status: int, body: Any, records: list[httpx.Request]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        records.append(request)
        return httpx.Response(status, json=body)

    return httpx.MockTransport(handler)


def test_health_returns_json() -> None:
    records: list[httpx.Request] = []
    with ankusa.RoutesClient("http://gateway.invalid", transport=_transport(200, {"status": "ok", "routes": 3}, records)) as client:
        assert client.health() == {"status": "ok", "routes": 3}
    assert records[0].method == "GET"
    assert records[0].url.path == "/health"


def test_crud_methods_hit_the_right_paths_and_bodies() -> None:
    records: list[httpx.Request] = []
    route = {"id": "stripe", "path": "/webhooks/stripe"}
    with ankusa.RoutesClient("http://gateway.invalid", transport=_transport(200, route, records)) as client:
        assert client.create_route({"path": "/webhooks/stripe"}) == route
        assert client.list_routes() == route
        assert client.get_route("stripe") == route
        assert client.replace_route("stripe", {"path": "/webhooks/stripe"}) == route
        assert client.update_route("stripe", {"enabled": False}) == route
        assert client.delete_route("stripe") is None

    assert [r.method for r in records] == ["POST", "GET", "GET", "PUT", "PATCH", "DELETE"]
    assert [r.url.path for r in records] == [
        "/admin/routes",
        "/admin/routes",
        "/admin/routes/stripe",
        "/admin/routes/stripe",
        "/admin/routes/stripe",
        "/admin/routes/stripe",
    ]
    assert json.loads(records[0].content) == {"path": "/webhooks/stripe"}
    assert json.loads(records[4].content) == {"enabled": False}


def test_ip_rules_and_dry_run() -> None:
    records: list[httpx.Request] = []
    rules = {"default": "deny", "rules": [{"action": "allow", "cidr": "203.0.113.0/24"}]}
    with ankusa.RoutesClient("http://gateway.invalid", transport=_transport(200, rules, records)) as client:
        assert client.get_ip_rules() == rules
        assert client.put_ip_rules(rules) == rules

    decision = {"decision": "allow", "reason": "matched", "route_id": "stripe", "ip_rule": None}
    with ankusa.RoutesClient("http://gateway.invalid", transport=_transport(200, decision, records)) as client:
        assert client.test_route({"method": "POST", "path": "/webhooks/stripe", "ip": "203.0.113.7"}) == decision

    assert [r.method for r in records] == ["GET", "PUT", "POST"]
    assert [r.url.path for r in records] == ["/admin/ip-rules", "/admin/ip-rules", "/admin/routes/test"]
    assert json.loads(records[1].content) == rules
    assert json.loads(records[2].content) == {"method": "POST", "path": "/webhooks/stripe", "ip": "203.0.113.7"}


def test_list_routes_sends_only_present_query_params() -> None:
    records: list[httpx.Request] = []
    with ankusa.RoutesClient("http://gateway.invalid", transport=_transport(200, {"routes": []}, records)) as client:
        client.list_routes({"enabled": True, "limit": 50, "cursor": "stripe"})
        client.list_routes({"enabled": False})
        client.list_routes({})

    queries = [r.url.query.decode() for r in records]
    assert queries == ["enabled=true&limit=50&cursor=stripe", "enabled=false", ""]


def test_unusable_route_ids_are_refused_before_any_request() -> None:
    records: list[httpx.Request] = []
    with ankusa.RoutesClient("http://gateway.invalid", transport=_transport(200, {}, records)) as client:
        for bad in (None, 7, "", ".", ".."):
            calls = [
                lambda: client.get_route(bad),
                lambda: client.replace_route(bad, {"path": "/x"}),
                lambda: client.update_route(bad, {"enabled": True}),
                lambda: client.delete_route(bad),
            ]
            for call in calls:
                with pytest.raises(ankusa.InvalidRouteIdError) as exc:
                    call()
                assert exc.value.retryable is False
                assert repr(bad) in str(exc.value)

    assert records == []


def test_route_ids_are_percent_encoded_as_one_path_segment() -> None:
    records: list[httpx.Request] = []
    with ankusa.RoutesClient("http://gateway.invalid", transport=_transport(200, {"id": "x"}, records)) as client:
        client.get_route("a/b c")
        client.get_route("a/b?c#d e%")
        client.delete_route("a/b c")

    assert [r.url.raw_path for r in records] == [
        b"/admin/routes/a%2Fb%20c",
        b"/admin/routes/a%2Fb%3Fc%23d%20e%25",
        b"/admin/routes/a%2Fb%20c",
    ]


def test_error_classification() -> None:
    with ankusa.RoutesClient("http://gateway.invalid", transport=_transport(404, {"error": "not_found"}, [])) as client:
        with pytest.raises(ankusa.RouteNotFoundError) as exc:
            client.get_route("nope")
        assert exc.value.retryable is False

    body = {"error": "invalid_route", "field": "path", "message": 'must start with "/"'}
    with ankusa.RoutesClient("http://gateway.invalid", transport=_transport(400, body, [])) as client:
        with pytest.raises(ankusa.RoutesRejectedError) as exc:
            client.create_route({"path": "hooks/x"})
        assert exc.value.retryable is False
        assert exc.value.status == 400
        assert exc.value.code == "invalid_route"
        assert exc.value.field == "path"
        assert exc.value.message == 'must start with "/"'

    body = {"error": "duplicate_route", "conflicting_id": "stripe"}
    with ankusa.RoutesClient("http://gateway.invalid", transport=_transport(409, body, [])) as client:
        with pytest.raises(ankusa.RoutesRejectedError) as exc:
            client.create_route({"id": "other", "path": "/webhooks/stripe"})
        assert exc.value.code == "duplicate_route"
        assert exc.value.conflicting_id == "stripe"

    with ankusa.RoutesClient("http://gateway.invalid", transport=_transport(403, {"error": "forbidden"}, [])) as client:
        with pytest.raises(ankusa.RoutesRejectedError) as exc:
            client.list_routes()
        assert exc.value.retryable is False
        assert exc.value.status == 403
        assert exc.value.code == "forbidden"

    with ankusa.RoutesClient("http://gateway.invalid", transport=_transport(503, {"error": "store_unavailable"}, [])) as client:
        with pytest.raises(ankusa.RoutesUnavailableError) as exc:
            client.create_route({"path": "/hooks/x"})
        assert exc.value.retryable is True

    for status in (302, 500):
        with ankusa.RoutesClient("http://gateway.invalid", transport=_transport(status, {"error": "boom"}, [])) as client:
            with pytest.raises(ankusa.RoutesUnavailableError) as exc:
                client.list_routes()
            assert exc.value.retryable is True


def test_unreachable_is_retryable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom")

    with ankusa.RoutesClient("http://gateway.invalid", transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ankusa.RoutesUnavailableError) as exc:
            client.health()
        assert exc.value.retryable is True
