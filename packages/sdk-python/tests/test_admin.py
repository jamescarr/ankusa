"""Unit tests for the operator (admin) client, mirroring the conformance
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
    body = {"status": "ok", "instance": "default", "roles": ["edge", "dispatch", "storage"]}
    with ankusa.AdminClient("http://gateway.invalid", transport=_transport(200, body, records)) as client:
        assert client.health() == body
    assert records[0].method == "GET"
    assert records[0].url.path == "/health"


def test_metrics_returns_text() -> None:
    records: list[httpx.Request] = []
    metrics = 'ankusa_ingest_requests_total{instance="default"} 1\n'

    def handler(request: httpx.Request) -> httpx.Response:
        records.append(request)
        return httpx.Response(200, text=metrics)

    with ankusa.AdminClient("http://gateway.invalid", transport=httpx.MockTransport(handler)) as client:
        assert client.metrics() == metrics
    assert records[0].url.path == "/metrics"


def test_config_returns_json() -> None:
    records: list[httpx.Request] = []
    body = {"instance": "default", "roles": ["edge"], "port": 4000}
    with ankusa.AdminClient("http://gateway.invalid", transport=_transport(200, body, records)) as client:
        assert client.config() == body
    assert records[0].url.path == "/v1/config"


def test_dlq_and_quarantine_hit_the_right_paths() -> None:
    records: list[httpx.Request] = []
    page = {"total": 1, "entries": [{"id": "x"}]}
    with ankusa.AdminClient("http://gateway.invalid", transport=_transport(200, page, records)) as client:
        assert client.list_dead_letters() == page
        assert client.list_quarantined() == page
    with ankusa.AdminClient("http://gateway.invalid", transport=_transport(200, {"replayed": 2}, records)) as client:
        assert client.replay_dead_letters({"source_id": "demo"}) == {"replayed": 2}

    assert [r.method for r in records] == ["GET", "GET", "POST"]
    assert [r.url.path for r in records] == ["/v1/dlq", "/v1/quarantine", "/v1/dlq/replay"]
    assert json.loads(records[2].content) == {"source_id": "demo"}


def test_replay_without_filter_sends_empty_object() -> None:
    records: list[httpx.Request] = []
    with ankusa.AdminClient("http://gateway.invalid", transport=_transport(200, {"replayed": 3}, records)) as client:
        assert client.replay_dead_letters() == {"replayed": 3}
    assert json.loads(records[0].content) == {}


def test_query_params_sent_only_when_present() -> None:
    records: list[httpx.Request] = []
    with ankusa.AdminClient("http://gateway.invalid", transport=_transport(200, {"entries": []}, records)) as client:
        client.list_dead_letters({"source_id": "demo", "since": 1720000000000, "limit": 10})
        client.list_dead_letters({"limit": 5})
        client.list_quarantined({"limit": 7})
        client.list_quarantined({})

    assert [r.url.query.decode() for r in records] == [
        "source_id=demo&since=1720000000000&limit=10",
        "limit=5",
        "limit=7",
        "",
    ]


def test_error_classification() -> None:
    body = {"error": "role_not_enabled", "role": "dispatch"}
    with ankusa.AdminClient("http://gateway.invalid", transport=_transport(409, body, [])) as client:
        with pytest.raises(ankusa.RoleNotEnabledError) as exc:
            client.list_dead_letters()
        assert exc.value.retryable is False
        assert exc.value.role == "dispatch"

    body = {"error": "invalid_filter", "field": "limit", "message": "must be an integer"}
    with ankusa.AdminClient("http://gateway.invalid", transport=_transport(400, body, [])) as client:
        with pytest.raises(ankusa.AdminRejectedError) as exc:
            client.list_dead_letters()
        assert exc.value.retryable is False
        assert exc.value.status == 400
        assert exc.value.code == "invalid_filter"

    body = {"error": "forbidden", "field": "limit", "message": "not allowed"}
    with ankusa.AdminClient("http://gateway.invalid", transport=_transport(403, body, [])) as client:
        with pytest.raises(ankusa.AdminRejectedError) as exc:
            client.list_dead_letters()
        assert exc.value.retryable is False
        assert exc.value.status == 403
        assert exc.value.code == "forbidden"

    for status in (302, 500):
        with ankusa.AdminClient("http://gateway.invalid", transport=_transport(status, {"error": "boom"}, [])) as client:
            with pytest.raises(ankusa.AdminUnavailableError) as exc:
                client.health()
            assert exc.value.retryable is True


def test_unreachable_is_retryable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom")

    with ankusa.AdminClient("http://gateway.invalid", transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ankusa.AdminUnavailableError) as exc:
            client.health()
        assert exc.value.retryable is True
