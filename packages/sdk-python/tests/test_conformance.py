"""Run the language-neutral conformance vectors in ``conformance/`` against
this SDK.

One test per vector, named by its ``id``. See ``conformance/README.md`` for the
vector format and the runner contract every SDK follows.
"""

from __future__ import annotations

import base64
import contextlib
import dataclasses
import json
import threading
import time
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import httpx
import pytest

import ankusa

CONFORMANCE_DIR = Path(__file__).resolve().parents[3] / "conformance"
CASES: list[dict[str, Any]] = [
    case
    for path in sorted(CONFORMANCE_DIR.glob("cases/*.json"))
    for case in json.loads(path.read_text())["cases"]
]

ERROR_CLASSES: dict[str, type[BaseException]] = {
    name: getattr(ankusa, name)
    for name in (
        "InvalidClaimRefError",
        "ClaimNotFoundError",
        "ClaimRejectedError",
        "ClaimIntegrityError",
        "ClaimCheckUnavailableError",
        "MissingHookIdError",
        "InvalidSignatureError",
        "RoutesError",
        "InvalidRouteIdError",
        "RoutesUnavailableError",
        "RouteNotFoundError",
        "RoutesRejectedError",
        "AdminError",
        "AdminUnavailableError",
        "RoleNotEnabledError",
        "AdminRejectedError",
        "InvalidMessageError",
    )
}

RecordedRequest = dict[str, Any]
Runner = Callable[[dict[str, Any], "list[RecordedRequest]"], Any]


def _body_bytes(body: dict[str, Any] | None) -> bytes:
    if body is None:
        return b""
    if "text" in body:
        return body["text"].encode()
    if "base64" in body:
        return base64.b64decode(body["base64"])
    if "json" in body:
        return json.dumps(body["json"], separators=(",", ":")).encode()
    raise AssertionError(f"unknown Body: {body!r}")


@contextlib.contextmanager
def _gateway(spec: dict[str, Any], requests: list[RecordedRequest]) -> Iterator[str]:
    """A real HTTP server standing in for a deployment's gateway: it answers
    every request with ``spec`` and records what it saw.
    """
    if spec.get("unreachable"):
        yield "http://127.0.0.1:1"
        return

    status = spec["status"]
    headers = spec.get("headers") or {}
    payload = _body_bytes(spec.get("body"))
    delay = spec.get("delay_ms", 0) / 1000

    class Handler(BaseHTTPRequestHandler):
        def _handle(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            requests.append(
                {
                    "method": self.command,
                    "path": self.path,
                    "headers": {key.lower(): value for key, value in self.headers.items()},
                    "body": json.loads(body) if body else None,
                }
            )
            if delay:
                time.sleep(delay)
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            try:
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_GET(self) -> None:  # noqa: N802
            self._handle()

        def do_POST(self) -> None:  # noqa: N802
            self._handle()

        def do_PUT(self) -> None:  # noqa: N802
            self._handle()

        def do_PATCH(self) -> None:  # noqa: N802
            self._handle()

        def do_DELETE(self) -> None:  # noqa: N802
            self._handle()

        def log_message(self, *args: Any) -> None:
            pass

    class Server(ThreadingHTTPServer):
        # A delayed response must not keep `shutdown()`/`server_close()` from
        # returning once the client has already given up (the timeout cases).
        daemon_threads = True
        block_on_close = False

    server = Server(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def _injected_transport(spec: dict[str, Any], requests: list[RecordedRequest]) -> httpx.MockTransport:
    status = spec["status"]
    headers = spec.get("headers") or {}
    payload = _body_bytes(spec.get("body"))

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(
            {
                "method": request.method,
                "path": request.url.raw_path.decode(),
                "headers": {key.lower(): value for key, value in request.headers.items()},
                "body": json.loads(request.content) if request.content else None,
            }
        )
        return httpx.Response(status, headers=headers, content=payload)

    return httpx.MockTransport(handler)


ClientFactory = Callable[..., Any]


@contextlib.contextmanager
def _client(
    factory: ClientFactory,
    base_url: str,
    client: dict[str, Any],
    transport: httpx.BaseTransport | None,
) -> Iterator[Any]:
    with factory(
        base_url,
        headers=client.get("headers"),
        timeout=client["timeout_ms"] / 1000 if "timeout_ms" in client else 10.0,
        transport=transport,
    ) as built:
        yield built


def _connected_with(
    factory: ClientFactory,
    gateway: dict[str, Any],
    client: dict[str, Any],
    requests: list[RecordedRequest],
) -> contextlib.AbstractContextManager[Any]:
    if client.get("transport") == "injected":
        return _client(factory, "http://gateway.invalid", client, _injected_transport(gateway, requests))
    return _real_client(factory, gateway, client, requests)


@contextlib.contextmanager
def _real_client(
    factory: ClientFactory,
    gateway: dict[str, Any],
    client: dict[str, Any],
    requests: list[RecordedRequest],
) -> Iterator[Any]:
    with _gateway(gateway, requests) as base_url:
        with _client(factory, base_url, client, None) as built:
            yield built


def _run_parse_claim_ref(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    return dataclasses.asdict(ankusa.parse_claim_ref(case["input"]["ref"]))


def _run_parse_headers(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    return dataclasses.asdict(ankusa.parse_headers(case["input"]["headers"]))

def _run_verify_signature(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    kwargs: dict[str, Any] = {"now": inp["now"]}
    if "tolerance_seconds" in inp:
        kwargs["tolerance_seconds"] = inp["tolerance_seconds"]
    verified = ankusa.verify_signature(inp["headers"], _body_bytes(inp["body"]), inp["secrets"], **kwargs)
    return {"id": verified.id, "timestamp": verified.timestamp}



def _run_decode_message(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    return dataclasses.asdict(ankusa.decode_message(case["input"]["message"]))


def _run_idempotency_key(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    hook = (
        ankusa.decode_message(inp["message"])
        if "message" in inp
        else ankusa.parse_headers(inp["headers"])
    )
    return {"key": ankusa.idempotency_key(hook, include_replay=inp.get("include_replay", False))}


def _run_redeem(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.ClaimCheckClient, inp["gateway"], inp.get("client") or {}, requests) as claim_check:
        body = claim_check.redeem(inp["ref"], inp["sha256"])
    return {"body": {"base64": base64.b64encode(body).decode()}}


def _run_health(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.ClaimCheckClient, inp["gateway"], inp.get("client") or {}, requests) as claim_check:
        return claim_check.health()


def _run_routes_health(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.RoutesClient, inp["gateway"], inp.get("client") or {}, requests) as routes:
        return routes.health()


def _run_routes_list(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.RoutesClient, inp["gateway"], inp.get("client") or {}, requests) as routes:
        return routes.list_routes(inp.get("params"))


def _run_routes_create(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.RoutesClient, inp["gateway"], inp.get("client") or {}, requests) as routes:
        return routes.create_route(inp["input"])


def _run_routes_get(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.RoutesClient, inp["gateway"], inp.get("client") or {}, requests) as routes:
        return routes.get_route(inp["id"])


def _run_routes_replace(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.RoutesClient, inp["gateway"], inp.get("client") or {}, requests) as routes:
        return routes.replace_route(inp["id"], inp["input"])


def _run_routes_update(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.RoutesClient, inp["gateway"], inp.get("client") or {}, requests) as routes:
        return routes.update_route(inp["id"], inp["patch"])


def _run_routes_delete(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.RoutesClient, inp["gateway"], inp.get("client") or {}, requests) as routes:
        routes.delete_route(inp["id"])
    return None


def _run_routes_ip_rules_get(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.RoutesClient, inp["gateway"], inp.get("client") or {}, requests) as routes:
        return routes.get_ip_rules()


def _run_routes_ip_rules_put(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.RoutesClient, inp["gateway"], inp.get("client") or {}, requests) as routes:
        return routes.put_ip_rules(inp["rules"])


def _run_routes_test(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.RoutesClient, inp["gateway"], inp.get("client") or {}, requests) as routes:
        return routes.test_route(inp["request"])


def _run_admin_health(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.AdminClient, inp["gateway"], inp.get("client") or {}, requests) as admin:
        return admin.health()


def _run_admin_metrics(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.AdminClient, inp["gateway"], inp.get("client") or {}, requests) as admin:
        return {"text": admin.metrics()}


def _run_admin_config(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.AdminClient, inp["gateway"], inp.get("client") or {}, requests) as admin:
        return admin.config()


def _run_admin_dlq_list(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.AdminClient, inp["gateway"], inp.get("client") or {}, requests) as admin:
        return admin.list_dead_letters(inp.get("params"))


def _run_admin_replay_create(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.AdminClient, inp["gateway"], inp.get("client") or {}, requests) as admin:
        return admin.create_replay(inp["spec"])


def _run_admin_replay_get(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.AdminClient, inp["gateway"], inp.get("client") or {}, requests) as admin:
        return admin.get_replay(inp["id"])


def _run_admin_replay_list(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.AdminClient, inp["gateway"], inp.get("client") or {}, requests) as admin:
        return admin.list_replays()


def _run_admin_replay_update(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.AdminClient, inp["gateway"], inp.get("client") or {}, requests) as admin:
        return admin.update_replay(inp["id"], inp["patch"])


def _run_admin_quarantine(case: dict[str, Any], requests: list[RecordedRequest]) -> Any:
    inp = case["input"]
    with _connected_with(ankusa.AdminClient, inp["gateway"], inp.get("client") or {}, requests) as admin:
        return admin.list_quarantined(inp.get("params"))


RUNNERS: dict[str, Runner] = {
    "parse_claim_ref": _run_parse_claim_ref,
    "parse_headers": _run_parse_headers,
    "verify_signature": _run_verify_signature,
    "decode_message": _run_decode_message,
    "idempotency_key": _run_idempotency_key,
    "redeem": _run_redeem,
    "health": _run_health,
    "routes_health": _run_routes_health,
    "routes_list": _run_routes_list,
    "routes_create": _run_routes_create,
    "routes_get": _run_routes_get,
    "routes_replace": _run_routes_replace,
    "routes_update": _run_routes_update,
    "routes_delete": _run_routes_delete,
    "routes_ip_rules_get": _run_routes_ip_rules_get,
    "routes_ip_rules_put": _run_routes_ip_rules_put,
    "routes_test": _run_routes_test,
    "admin_health": _run_admin_health,
    "admin_metrics": _run_admin_metrics,
    "admin_config": _run_admin_config,
    "admin_dlq_list": _run_admin_dlq_list,
    "admin_replay_create": _run_admin_replay_create,
    "admin_replay_get": _run_admin_replay_get,
    "admin_replay_list": _run_admin_replay_list,
    "admin_replay_update": _run_admin_replay_update,
    "admin_quarantine": _run_admin_quarantine,
}


def _expected_ok(case: dict[str, Any], expected: Any) -> Any:
    # Bytes can't round-trip through JSON: both sides become base64.
    if case["operation"] == "redeem":
        return {"body": {"base64": base64.b64encode(_body_bytes(expected["body"])).decode()}}
    return expected


def _assert_requests(actual: list[RecordedRequest], expected: list[dict[str, Any]]) -> None:
    assert len(actual) == len(expected), f"expected {len(expected)} requests, got {len(actual)}: {actual!r}"
    for got, want in zip(actual, expected):
        assert got["method"] == want["method"], f"method: {got['method']!r} != {want['method']!r}"
        assert got["path"] == want["path"], f"path: {got['path']!r} != {want['path']!r}"
        for name, value in (want.get("headers") or {}).items():
            assert got["headers"].get(name) == value, f"header {name!r}: {got['headers'].get(name)!r} != {value!r}"
        if "body" in want:
            assert got["body"] == want["body"], f"body: {got['body']!r} != {want['body']!r}"


@pytest.mark.parametrize("case", CASES, ids=[case["id"] for case in CASES])
def test_conformance(case: dict[str, Any]) -> None:
    operation = case["operation"]
    runner = RUNNERS.get(operation)
    if runner is None:
        pytest.fail(f"unknown conformance operation {operation!r}")

    requests: list[RecordedRequest] = []
    ok: Any = None
    error: dict[str, Any] | None = None
    try:
        ok = runner(case, requests)
    except Exception as err:  # noqa: BLE001 -- unmapped errors are re-raised below
        name = next((n for n, cls in ERROR_CLASSES.items() if type(err) is cls), None)
        if name is None:
            raise
        error = {"class": name, **{k: getattr(err, k) for k in (
            "retryable",
            "status",
            "body",
            "code",
            "field",
            "message",
            "conflicting_id",
            "max_routes",
            "role",
        ) if hasattr(err, k)}}

    expect = case["expect"]
    if "ok" in expect:
        assert error is None, f"expected ok, got {error!r}"
        assert ok == _expected_ok(case, expect["ok"]), f"ok: {ok!r} != {_expected_ok(case, expect['ok'])!r}"
    elif "error" in expect:
        assert error is not None, f"expected error {expect['error']!r}, got ok {ok!r}"
        want = expect["error"]
        assert error["class"] == want["class"], f"expected {want['class']}, got {error['class']}"
        for key, value in want.items():
            if key == "class":
                continue
            assert key in error, f"{error['class']} has no attribute {key!r} (expected {value!r})"
            assert error[key] == value, f"{error['class']}.{key}: {error[key]!r} != {value!r}"

    if "requests" in expect:
        _assert_requests(requests, expect["requests"])
