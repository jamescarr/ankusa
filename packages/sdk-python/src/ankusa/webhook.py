"""Parse the headers Ankusa's HTTP sink attaches to every delivery.

See "HTTP handoff" in docs/integrations.md for the full contract this
mirrors: the raw body arrives verbatim, and identity travels in
``x-ankusa-id``, ``x-ankusa-source``, ``x-ankusa-seq``, ``x-ankusa-tenant``
(only when the source has a tenant), and ``content-type``. A receiver must
dedupe on ``x-ankusa-id``: delivery is at-least-once, so the same hook can
arrive twice after a retry.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

__all__ = ["HookHeaders", "MissingHookIdError", "parse_headers"]

# ``str.isdigit()`` accepts non-ASCII digits (``"²".isdigit()`` is True) that
# ``int()`` then rejects. The header is a sequence number, so only ASCII
# digits count.
_SEQ_PATTERN = re.compile(r"[0-9]+")


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
    # 1-based, monotonically increasing per source. ``None`` if the header
    # was absent or not an integer.
    seq: int | None
    # Only present when the source has a tenant.
    tenant: str | None
    content_type: str | None


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

    seq_raw = lowered.get("x-ankusa-seq")
    seq = int(seq_raw) if seq_raw is not None and _SEQ_PATTERN.fullmatch(seq_raw) else None

    return HookHeaders(
        id=hook_id,
        source=lowered.get("x-ankusa-source", ""),
        seq=seq,
        tenant=lowered.get("x-ankusa-tenant"),
        content_type=lowered.get("content-type"),
    )
