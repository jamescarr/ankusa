import json
from collections.abc import Callable

import httpx
import pytest

from ankusa import (
    AdminClient,
    AdminError,
    AdminUnavailableError,
    Source,
    SourceConflictError,
    SourceInvalidError,
    SourceNotFoundError,
    SourceSpec,
    SourceStoreReadOnlyError,
    VersionMismatchError,
)

BASE_URL = "http://admin.test"
Handler = Callable[[httpx.Request], httpx.Response]

ENTRY = {
    "tenant": "acme",
    "name": "billing",
    "source_id": "acme.billing",
    "ingest_path": "/webhooks/acme.billing",
    "verify": {"type": "hmac", "secret": "[REDACTED]", "signature_header": "X-Sig"},
    "on_verify_failure": "reject",
    "sinks": [{"type": "log"}],
}

SPEC = SourceSpec(
    sinks=[{"type": "log"}],
    verify={"type": "hmac", "secret": "s3cr3t", "signature_header": "X-Sig"},
    on_verify_failure="reject",
)


def make_client(
    handler: Handler,
    *,
    expected_version: str | None = None,
    requests: "list[httpx.Request] | None" = None,
) -> AdminClient:
    def wrapped(request: httpx.Request) -> httpx.Response:
        if requests is not None:
            requests.append(request)
        return handler(request)

    return AdminClient(
        BASE_URL, expected_version=expected_version, transport=httpx.MockTransport(wrapped)
    )


def health(request: httpx.Request, version: str = "0.3.0") -> httpx.Response:
    assert request.url.path == "/health"
    return httpx.Response(200, json={"status": "ok", "version": version})


# --- SourceSpec ---------------------------------------------------------------


def test_source_spec_to_json_omits_none_fields() -> None:
    assert SPEC.to_json() == {
        "sinks": [{"type": "log"}],
        "verify": {"type": "hmac", "secret": "s3cr3t", "signature_header": "X-Sig"},
        "on_verify_failure": "reject",
    }


def test_source_spec_to_json_omits_unset_verify_and_failure_mode() -> None:
    assert SourceSpec(sinks=[{"type": "log"}]).to_json() == {"sinks": [{"type": "log"}]}


# --- server_version / version latch ------------------------------------------


def test_server_version_reads_the_health_version() -> None:
    with make_client(health) as client:
        assert client.server_version() == "0.3.0"


def test_server_version_fetches_health_once_and_caches_it() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return health(request)
        return httpx.Response(200, json={"tenant": "acme", "entries": []})

    with make_client(handler, requests=requests) as client:
        client.server_version()
        client.list_sources("acme")
        assert len([r for r in requests if r.url.path == "/health"]) == 1


def test_list_sources_raises_version_mismatch_when_expected_version_differs() -> None:
    with make_client(health, expected_version="9.9.9") as client:
        with pytest.raises(VersionMismatchError) as exc_info:
            client.list_sources("acme")
    assert "9.9.9" in str(exc_info.value)
    assert "0.3.0" in str(exc_info.value)
    assert exc_info.value.status is None
    assert exc_info.value.body is None


def test_version_mismatch_is_rechecked_from_cache_without_another_health_request() -> None:
    requests: list[httpx.Request] = []

    with make_client(health, expected_version="9.9.9", requests=requests) as client:
        with pytest.raises(VersionMismatchError):
            client.list_sources("acme")
        with pytest.raises(VersionMismatchError):
            client.get_source("acme", "billing")

    assert len([r for r in requests if r.url.path == "/health"]) == 1


def test_expected_version_match_does_not_raise() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return health(request)
        return httpx.Response(200, json={"tenant": "acme", "entries": [ENTRY]})

    with make_client(handler, expected_version="0.3.0") as client:
        assert client.list_sources("acme") == [Source.from_json(ENTRY)]


# --- list / get / create / update --------------------------------------------


def test_list_sources_parses_entries() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return health(request)
        assert request.method == "GET"
        assert request.url.path == "/v1/tenants/acme/sources"
        return httpx.Response(200, json={"tenant": "acme", "entries": [ENTRY]})

    with make_client(handler) as client:
        assert client.list_sources("acme") == [Source.from_json(ENTRY)]


def test_get_source_requires_no_version_probe_without_expected_version() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/tenants/acme/sources/billing"
        return httpx.Response(200, json=ENTRY)

    with make_client(handler) as client:
        source = client.get_source("acme", "billing")
    assert source.source_id == "acme.billing"
    assert source.ingest_path == "/webhooks/acme.billing"
    assert source.verify == {"type": "hmac", "secret": "[REDACTED]", "signature_header": "X-Sig"}
    assert source.on_verify_failure == "reject"
    assert source.sinks == [{"type": "log"}]


def test_create_source_posts_the_spec_plus_name() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/v1/tenants/acme/sources"
        assert json.loads(request.content) == {**SPEC.to_json(), "name": "billing"}
        return httpx.Response(201, json=ENTRY)

    with make_client(handler) as client:
        assert client.create_source("acme", "billing", SPEC) == Source.from_json(ENTRY)


def test_create_source_sends_no_verify_when_spec_has_none() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert json.loads(request.content) == {"sinks": [{"type": "log"}], "name": "billing"}
        return httpx.Response(201, json=ENTRY)

    with make_client(handler) as client:
        client.create_source("acme", "billing", SourceSpec(sinks=[{"type": "log"}]))


def test_update_source_puts_to_the_named_path() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "PUT"
        assert request.url.path == "/v1/tenants/acme/sources/billing"
        assert json.loads(request.content) == SPEC.to_json()
        return httpx.Response(200, json=ENTRY)

    with make_client(handler) as client:
        assert client.update_source("acme", "billing", SPEC) == Source.from_json(ENTRY)


# --- error mapping ------------------------------------------------------------


def test_404_maps_to_source_not_found_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": "source_not_found"})

    with make_client(handler) as client:
        with pytest.raises(SourceNotFoundError) as exc_info:
            client.get_source("acme", "missing")
    assert exc_info.value.status == 404
    assert exc_info.value.body == {"error": "source_not_found"}


def test_409_source_exists_maps_to_source_conflict_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"error": "source_exists"})

    with make_client(handler) as client:
        with pytest.raises(SourceConflictError) as exc_info:
            client.create_source("acme", "billing", SPEC)
    assert exc_info.value.status == 409
    assert exc_info.value.body == {"error": "source_exists"}


def test_409_source_store_read_only_maps_to_source_store_read_only_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"error": "source_store_read_only"})

    with make_client(handler) as client:
        with pytest.raises(SourceStoreReadOnlyError) as exc_info:
            client.update_source("acme", "billing", SPEC)
    assert exc_info.value.status == 409
    assert exc_info.value.body == {"error": "source_store_read_only"}


def test_400_with_message_carries_the_server_message() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "invalid_source", "message": "sinks must not be empty"})

    with make_client(handler) as client:
        with pytest.raises(SourceInvalidError) as exc_info:
            client.create_source("acme", "billing", SPEC)
    assert exc_info.value.status == 400
    assert exc_info.value.message == "sinks must not be empty"
    assert exc_info.value.body == {"error": "invalid_source", "message": "sinks must not be empty"}


def test_400_without_message_carries_the_error_code() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "invalid_tenant"})

    with make_client(handler) as client:
        with pytest.raises(SourceInvalidError) as exc_info:
            client.list_sources("bad tenant")
    assert exc_info.value.status == 400
    assert exc_info.value.message == "invalid_tenant"


def test_5xx_maps_to_admin_unavailable_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "boom"})

    with make_client(handler) as client:
        with pytest.raises(AdminUnavailableError) as exc_info:
            client.list_sources("acme")
    assert exc_info.value.status == 503
    assert exc_info.value.body == {"error": "boom"}


def test_transport_error_maps_to_admin_unavailable_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    with make_client(handler) as client:
        with pytest.raises(AdminUnavailableError) as exc_info:
            client.get_source("acme", "billing")
    assert exc_info.value.status is None
    assert exc_info.value.body is None


def test_every_error_is_an_admin_error() -> None:
    for error in (
        SourceNotFoundError,
        SourceConflictError,
        SourceStoreReadOnlyError,
        SourceInvalidError,
        AdminUnavailableError,
        VersionMismatchError,
    ):
        assert issubclass(error, AdminError)


# --- lifecycle ----------------------------------------------------------------


def test_client_is_a_context_manager_that_closes_its_transport() -> None:
    with make_client(health) as client:
        assert client.server_version() == "0.3.0"
    assert client._http.is_closed
