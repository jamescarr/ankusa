"""Unit tests for ``ankusa.decode_message``'s ``event_type`` field.

The conformance suite (``test_conformance.py``) covers the decode rules
(``message.event_type``); these name the three shapes a consumer sees.
"""

from __future__ import annotations

import json

import pytest

import ankusa

BASE = {"v": 1, "id": "01a0", "source_id": "demo", "received_at": 1, "size": 5, "body_base64": "aGVsbG8="}


def test_event_type_is_none_when_absent() -> None:
    assert ankusa.decode_message(json.dumps(BASE)).event_type is None


def test_event_type_is_none_when_null() -> None:
    assert ankusa.decode_message(json.dumps({**BASE, "event_type": None})).event_type is None


def test_event_type_is_the_providers_event_name() -> None:
    message = ankusa.decode_message(json.dumps({**BASE, "event_type": "invoice.paid"}))

    assert message.event_type == "invoice.paid"


def test_event_type_must_be_a_string() -> None:
    with pytest.raises(ankusa.InvalidMessageError) as raised:
        ankusa.decode_message(json.dumps({**BASE, "event_type": 7}))

    assert raised.value.code == "invalid_field"
    assert raised.value.field == "event_type"
