"""Unit tests for ``ankusa.webhook.parse_headers``' ``verification`` field.

The conformance suite (``test_conformance.py``) covers the full vector set;
these cover the two cases worth a human-readable name in isolation.
"""

from __future__ import annotations

import ankusa


def test_parse_headers_verification_absent_is_null() -> None:
    headers = {"x-ankusa-id": "01a0", "x-ankusa-source": "demo"}

    parsed = ankusa.parse_headers(headers)

    assert parsed.verification is None


def test_parse_headers_verification_flagged_true() -> None:
    headers = {
        "x-ankusa-id": "01a0",
        "x-ankusa-source": "demo",
        "x-ankusa-verification": "failed",
        "x-ankusa-verification-flagged": "true",
    }

    parsed = ankusa.parse_headers(headers)

    assert parsed.verification == ankusa.Verification(status="failed", flagged=True)
