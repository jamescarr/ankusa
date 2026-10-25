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


def test_is_trusted_is_true_only_for_an_unflagged_ok() -> None:
    ok = ankusa.parse_headers({"x-ankusa-id": "01a0", "x-ankusa-verification": "ok"})
    flagged = ankusa.parse_headers(
        {
            "x-ankusa-id": "01a0",
            "x-ankusa-verification": "failed",
            "x-ankusa-verification-flagged": "true",
        }
    )
    skipped = ankusa.parse_headers({"x-ankusa-id": "01a0", "x-ankusa-verification": "skipped"})
    legacy = ankusa.parse_headers({"x-ankusa-id": "01a0"})

    assert ankusa.is_trusted(ok) is True
    assert ankusa.is_trusted(flagged) is False
    assert ankusa.is_trusted(skipped) is False
    assert ankusa.is_trusted(legacy) is False
