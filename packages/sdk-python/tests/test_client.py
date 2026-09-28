import hashlib
import json
import threading
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from ankusa import (
    ClaimCheckClient,
    ClaimCheckUnavailableError,
    ClaimIntegrityError,
    ClaimNotFoundError,
    ClaimRejectedError,
    InvalidClaimRefError,
)

TENANT = "acme"
CLAIM_ID = "01M39VMD8RA3C5HR4RBV67Y002"
BODY = b"hello claim check"
SHA256 = hashlib.sha256(BODY).hexdigest()
REF = f"urn:ankusa:claim:v1:{TENANT}:{CLAIM_ID}"

Handler = Callable[[BaseHTTPRequestHandler], None]


@pytest.fixture
def gateway() -> Iterator[tuple[str, "list[Handler]"]]:
    """A real HTTP server standing in for the claim-check gateway. Each test
    installs its own handler via the returned single-element list (so it can
    be swapped per test the same way the TS suite reassigns a closure var).
    """
    box: list[Handler] = []

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            box[0](self)

        def log_message(self, *args: object) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", box
    finally:
        server.shutdown()
        thread.join()


def test_returns_verified_bytes_on_a_200_with_matching_sha256(
    gateway: tuple[str, "list[Handler]"],
) -> None:
    base_url, box = gateway

    def handle(req: BaseHTTPRequestHandler) -> None:
        assert req.path == f"/v1/claims/{TENANT}/{CLAIM_ID}"
        req.send_response(200)
        req.send_header("content-type", "application/octet-stream")
        req.end_headers()
        req.wfile.write(BODY)

    box.append(handle)
    with ClaimCheckClient(base_url) as client:
        assert client.redeem(REF, SHA256) == BODY


def test_raises_claim_integrity_error_retryable_false_on_a_sha256_mismatch(
    gateway: tuple[str, "list[Handler]"],
) -> None:
    base_url, box = gateway
    wrong = b"x" * len(BODY)  # right length, wrong bytes

    def handle(req: BaseHTTPRequestHandler) -> None:
        req.send_response(200)
        req.send_header("content-type", "application/octet-stream")
        req.end_headers()
        req.wfile.write(wrong)

    box.append(handle)
    with ClaimCheckClient(base_url) as client:
        with pytest.raises(ClaimIntegrityError) as exc_info:
            client.redeem(REF, SHA256)
        assert exc_info.value.retryable is False


def test_raises_claim_integrity_error_on_a_truncated_body(gateway: tuple[str, "list[Handler]"]) -> None:
    base_url, box = gateway

    def handle(req: BaseHTTPRequestHandler) -> None:
        req.send_response(200)
        req.send_header("content-type", "application/octet-stream")
        req.end_headers()
        req.wfile.write(BODY[:-1])

    box.append(handle)
    with ClaimCheckClient(base_url) as client:
        with pytest.raises(ClaimIntegrityError):
            client.redeem(REF, SHA256)


def test_raises_claim_not_found_error_retryable_false_on_a_404(gateway: tuple[str, "list[Handler]"]) -> None:
    base_url, box = gateway

    def handle(req: BaseHTTPRequestHandler) -> None:
        payload = json.dumps({"error": "not_found"}).encode()
        req.send_response(404)
        req.send_header("content-type", "application/json")
        req.end_headers()
        req.wfile.write(payload)

    box.append(handle)
    with ClaimCheckClient(base_url) as client:
        with pytest.raises(ClaimNotFoundError) as exc_info:
            client.redeem(REF, SHA256)
        assert exc_info.value.retryable is False


def test_raises_claim_rejected_error_retryable_false_on_a_400(gateway: tuple[str, "list[Handler]"]) -> None:
    base_url, box = gateway

    def handle(req: BaseHTTPRequestHandler) -> None:
        payload = json.dumps({"error": "invalid_id"}).encode()
        req.send_response(400)
        req.send_header("content-type", "application/json")
        req.end_headers()
        req.wfile.write(payload)

    box.append(handle)
    with ClaimCheckClient(base_url) as client:
        with pytest.raises(ClaimRejectedError) as exc_info:
            client.redeem(REF, SHA256)
        assert exc_info.value.retryable is False
        assert exc_info.value.status == 400


def test_raises_claim_check_unavailable_error_retryable_true_on_a_503(gateway: tuple[str, "list[Handler]"]) -> None:
    base_url, box = gateway

    def handle(req: BaseHTTPRequestHandler) -> None:
        payload = json.dumps({"error": "store_unavailable"}).encode()
        req.send_response(503)
        req.send_header("content-type", "application/json")
        req.send_header("retry-after", "1")
        req.end_headers()
        req.wfile.write(payload)

    box.append(handle)
    with ClaimCheckClient(base_url) as client:
        with pytest.raises(ClaimCheckUnavailableError) as exc_info:
            client.redeem(REF, SHA256)
        assert exc_info.value.retryable is True


def test_raises_claim_check_unavailable_error_retryable_true_when_the_gateway_is_unreachable() -> None:
    with ClaimCheckClient("http://127.0.0.1:1", timeout=1.0) as client:
        with pytest.raises(ClaimCheckUnavailableError) as exc_info:
            client.redeem(REF, SHA256)
        assert exc_info.value.retryable is True


def test_raises_invalid_claim_ref_error_without_making_a_request_for_a_malformed_ref(
    gateway: tuple[str, "list[Handler]"],
) -> None:
    base_url, box = gateway

    def handle(req: BaseHTTPRequestHandler) -> None:
        raise AssertionError("must not be called")

    box.append(handle)
    with ClaimCheckClient(base_url) as client:
        with pytest.raises(InvalidClaimRefError):
            client.redeem("not-a-ref", SHA256)


@pytest.mark.parametrize(
    "bad_sha256",
    [
        SHA256.upper(),  # uppercase hex
        SHA256[:-1],  # 63 chars
        SHA256 + "0",  # 65 chars
        f"sha256-{SHA256}",  # prefixed
        "",
    ],
)
def test_raises_invalid_claim_ref_error_without_making_a_request_for_a_malformed_sha256(
    gateway: tuple[str, "list[Handler]"], bad_sha256: str
) -> None:
    base_url, box = gateway

    def handle(req: BaseHTTPRequestHandler) -> None:
        raise AssertionError("must not be called")

    box.append(handle)
    with ClaimCheckClient(base_url) as client:
        with pytest.raises(InvalidClaimRefError) as exc_info:
            client.redeem(REF, bad_sha256)
        assert exc_info.value.retryable is False


def test_health_resolves_ok_on_a_healthy_gateway(gateway: tuple[str, "list[Handler]"]) -> None:
    base_url, box = gateway

    def handle(req: BaseHTTPRequestHandler) -> None:
        assert req.path == "/health"
        payload = json.dumps({"status": "ok"}).encode()
        req.send_response(200)
        req.send_header("content-type", "application/json")
        req.end_headers()
        req.wfile.write(payload)

    box.append(handle)
    with ClaimCheckClient(base_url) as client:
        assert client.health() == {"status": "ok"}
