"""Parse the headers Ankusa's HTTP sink attaches to every delivery.

See "HTTP handoff" in docs/integrations.md for the full contract this
mirrors: the raw body arrives verbatim, and identity travels in
``x-ankusa-id``, ``x-ankusa-source``, ``x-ankusa-tenant`` (only when the
source has a tenant), ``content-type`` and ``x-ankusa-idempotency-key``. The
provider's own event key and, on a replay, the replay job id ride along in
``x-ankusa-dedupe-key`` and ``x-ankusa-replay-id`` when they exist.

A receiver must dedupe: delivery is at-least-once, so the same hook can
arrive twice after a retry. Prefer ``idempotency_key`` (see
``ankusa.message``) over ``x-ankusa-id`` alone -- it returns the tenant-scoped
key Ankusa shipped, which collapses the provider retries that arrive with a
fresh ``id``.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

__all__ = [
    "HookHeaders",
    "InvalidSignatureError",
    "MissingHookIdError",
    "VerifiedSignature",
    "parse_headers",
    "verify_signature",
]


class MissingHookIdError(ValueError):
    """Raised by ``parse_headers`` when ``x-ankusa-id`` is absent.

    Every other Ankusa header is optional; this one is the identity a
    receiver dedupes on, so a delivery without it is a framework bug, not a
    malformed but tolerable request.
    """


@dataclass(frozen=True, slots=True)
class HookHeaders:
    """The identity of one HTTP-sink delivery."""

    id: str
    source: str
    # Only present when the source has a tenant.
    tenant: str | None
    content_type: str | None
    # The provider's own event key, when the source extracts one.
    dedupe_key: str | None
    # Set only on a delivery Ankusa replayed; the replay job id.
    replay_id: str | None
    # The tenant-scoped key to dedupe on, from ``x-ankusa-idempotency-key``;
    # None when absent or empty (a sender that predates the header).
    idempotency_key: str | None


def parse_headers(headers: Mapping[str, str]) -> HookHeaders:
    """Parse the ``x-ankusa-*`` headers of one delivery.

    ``headers`` may be any case-insensitive-or-not header mapping --
    ``http.server.BaseHTTPRequestHandler.headers``, an ``httpx.Headers``, a
    plain ``dict`` from a WSGI/ASGI framework -- lookup here is always
    case-insensitive regardless of what the mapping itself does.

    Raises ``MissingHookIdError`` if ``x-ankusa-id`` is absent.
    """
    lowered = {key.lower(): value for key, value in headers.items()}

    hook_id = lowered.get("x-ankusa-id")
    if not hook_id:
        raise MissingHookIdError("missing x-ankusa-id header")

    return HookHeaders(
        id=hook_id,
        source=lowered.get("x-ankusa-source", ""),
        tenant=lowered.get("x-ankusa-tenant"),
        content_type=lowered.get("content-type"),
        dedupe_key=lowered.get("x-ankusa-dedupe-key") or None,
        replay_id=lowered.get("x-ankusa-replay-id") or None,
        idempotency_key=lowered.get("x-ankusa-idempotency-key") or None,
    )


class InvalidSignatureError(ValueError):
    """A delivery whose Standard Webhooks signature does not verify.

    Never retryable: answer ``401``; the sender retries with the same bytes.
    ``code`` is one of ``invalid_secret``, ``missing_header``,
    ``invalid_timestamp``, ``timestamp_out_of_tolerance``,
    ``no_matching_signature``; ``field`` names the header at fault (``None``
    for ``invalid_secret``).
    """

    retryable = False

    def __init__(self, code: str, field: str | None, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.field = field


@dataclass(frozen=True, slots=True)
class VerifiedSignature:
    """The verified delivery's ``webhook-id`` and ``webhook-timestamp``."""

    id: str
    timestamp: int


def verify_signature(
    headers: Mapping[str, str],
    body: bytes | str,
    secrets: str | Sequence[str],
    *,
    tolerance_seconds: int = 300,
    now: int | None = None,
) -> VerifiedSignature:
    """Verify the Standard Webhooks signature an HTTP sink with a ``secret`` adds.

    ``webhook-signature`` holds space-separated ``v1,<base64>`` entries, each an
    HMAC-SHA256 over ``<webhook-id>.<webhook-timestamp>.<body>``. Any ``v1``
    entry matching any secret (``whsec_`` + base64, or any other string used as
    its own UTF-8 bytes) passes, compared in constant time, provided the
    timestamp is within ``tolerance_seconds`` of ``now`` (the clock by
    default). ``body`` must be the raw bytes received. Raises
    ``InvalidSignatureError``.
    """
    keys = _decode_secrets([secrets] if isinstance(secrets, str) else list(secrets))
    lowered = {key.lower(): value for key, value in headers.items()}

    def required(name: str) -> str:
        value = lowered.get(name)
        if not value:
            raise InvalidSignatureError("missing_header", name, f"missing {name} header")
        return value

    hook_id = required("webhook-id")
    raw_timestamp = required("webhook-timestamp")
    signature = required("webhook-signature")

    if not re.fullmatch(r"[0-9]+", raw_timestamp):
        raise InvalidSignatureError("invalid_timestamp", "webhook-timestamp", "webhook-timestamp is not a unix time")
    timestamp = int(raw_timestamp)
    current = int(time.time()) if now is None else now
    if abs(current - timestamp) > tolerance_seconds:
        raise InvalidSignatureError(
            "timestamp_out_of_tolerance", "webhook-timestamp", "webhook-timestamp is outside the tolerance window"
        )

    raw = body.encode("utf-8") if isinstance(body, str) else body
    signed = f"{hook_id}.{raw_timestamp}.".encode() + raw
    candidates = [entry[3:] for entry in signature.split(" ") if entry.startswith("v1,")]

    for key in keys:
        expected = base64.b64encode(hmac.new(key, signed, hashlib.sha256).digest()).decode("ascii")
        if any(hmac.compare_digest(candidate, expected) for candidate in candidates):
            return VerifiedSignature(id=hook_id, timestamp=timestamp)

    raise InvalidSignatureError("no_matching_signature", "webhook-signature", "no webhook-signature entry matches")


def _decode_secrets(secrets: list[str]) -> list[bytes]:
    if not secrets:
        raise InvalidSignatureError("invalid_secret", None, "no secret configured")
    keys = []
    for secret in secrets:
        if secret.startswith("whsec_"):
            try:
                keys.append(base64.b64decode(secret[len("whsec_") :], validate=True))
            except (binascii.Error, ValueError) as err:
                raise InvalidSignatureError("invalid_secret", None, "a whsec_ secret is not valid base64") from err
        elif secret:
            keys.append(secret.encode("utf-8"))
        else:
            raise InvalidSignatureError("invalid_secret", None, "an empty secret")
    return keys
