"""Decode a lifecycle event: the CloudEvents 1.0 (structured mode) event an
Ankusa instance publishes when a source or a route is created, updated or
deleted, when a tenant is erased, or when the instance is restored from a
backup.

It arrives as the body of a queue message from the instance's lifecycle sinks
(content type ``application/cloudevents+json``, ``source_id``
``ankusa:lifecycle``): decode the message with ``decode_message``, redeem its
body, and hand the bytes to ``decode_lifecycle``.

Delivery is at least once and in commit order. ``id`` is derived from the
change, so the same event always has the same id, and ``revision`` (per
``subject``) lets a consumer ignore a stale update. ``sequence`` is the
instance's audit sequence number, kept as the decimal string it is on the wire.

``decode_lifecycle`` ignores keys it does not know and event types it does not
know, so a consumer keeps working when a newer node announces more. Anything
else wrong raises ``InvalidMessageError`` (never retryable), with ``code`` one
of ``invalid_json``, ``not_an_object``, ``unsupported_version`` (``specversion``
is not ``"1.0"``) or ``invalid_field`` and the offending ``field``.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, NoReturn

from .message import InvalidMessageError

__all__ = ["LifecycleEvent", "decode_lifecycle"]

_DECIMAL = re.compile(r"^[0-9]+$")


@dataclass(frozen=True, slots=True)
class LifecycleEvent:
    """One decoded lifecycle event. ``data`` is the entity as the admin API
    shows it (secrets redacted), as the change left it; for a deletion, as it
    was the moment before. ``None`` fields were absent or ``null`` on the wire.
    """

    specversion: str
    id: str
    source: str
    type: str
    subject: str
    time: str
    datacontenttype: str | None
    data: dict[str, Any] | None
    revision: int | None
    actor: str | None
    instance: str | None
    sequence: str | None
    replayid: str | None
    traceparent: str | None
    tracestate: str | None


def _invalid_field(field: str) -> NoReturn:
    raise InvalidMessageError(f"invalid or missing field: {field}", code="invalid_field", field=field)


def _required_string(payload: dict[str, Any], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        _invalid_field(name)
    return value


def _optional_string(payload: dict[str, Any], name: str) -> str | None:
    value = payload.get(name)
    if value is None:
        return None
    if not isinstance(value, str):
        _invalid_field(name)
    return value


def decode_lifecycle(data: str | bytes) -> LifecycleEvent:
    """Decode and validate one lifecycle event (the JSON of the CloudEvent).

    The checks run in a fixed order and the first failure wins.
    """
    try:
        payload = json.loads(data)
    except (ValueError, TypeError) as err:
        raise InvalidMessageError("event is not valid JSON", code="invalid_json") from err
    if not isinstance(payload, dict):
        raise InvalidMessageError("event is not a JSON object", code="not_an_object")

    if payload.get("specversion") != "1.0":
        raise InvalidMessageError("unsupported specversion", code="unsupported_version")

    event_id = _required_string(payload, "id")
    source = _required_string(payload, "source")
    event_type = _required_string(payload, "type")
    subject = _required_string(payload, "subject")
    time = _required_string(payload, "time")
    datacontenttype = _optional_string(payload, "datacontenttype")

    body = payload.get("data")
    if body is not None and not isinstance(body, dict):
        _invalid_field("data")

    revision = payload.get("revision")
    if revision is not None and (isinstance(revision, bool) or not isinstance(revision, int) or revision < 1):
        _invalid_field("revision")

    actor = _optional_string(payload, "actor")
    instance = _optional_string(payload, "instance")

    sequence = payload.get("sequence")
    if sequence is not None and not (isinstance(sequence, str) and _DECIMAL.fullmatch(sequence)):
        _invalid_field("sequence")

    replayid = _optional_string(payload, "replayid")
    traceparent = _optional_string(payload, "traceparent")
    tracestate = _optional_string(payload, "tracestate")

    return LifecycleEvent(
        specversion="1.0",
        id=event_id,
        source=source,
        type=event_type,
        subject=subject,
        time=time,
        datacontenttype=datacontenttype,
        data=body,
        revision=revision,
        actor=actor,
        instance=instance,
        sequence=sequence,
        replayid=replayid,
        traceparent=traceparent,
        tracestate=tracestate,
    )
