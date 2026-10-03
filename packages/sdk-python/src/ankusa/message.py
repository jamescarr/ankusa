"""Decode the v1 queue message Ankusa publishes to every sink, and compute the
idempotency key a consumer dedupes on.

``Message`` mirrors ``Ankusa.Sink.Message``: the identity fields Ankusa has
always sent (``id``, ``source_id``, ``tenant_id``, ``received_at``,
``content_type``, ``size``), the body in one of two forms -- inline
(``body_base64``) or a claim-check ref (``claim``) -- and the fields added for
end-to-end idempotency: ``sha256`` (the body's digest, now on every message),
``dedupe_key`` (the provider's own event key, extracted at ingest),
``replay_id`` (set only on a replay delivery), and ``headers`` (the forwarded
provider request headers).

``decode_message`` validates all of that before returning, raising
``InvalidMessageError`` with a machine-readable ``code`` (and, for a bad
field, the field name) so a consumer can tell a poison message from a
transient failure. Redelivery is at-least-once, so a consumer is expected to
run ``idempotency_key`` over each decoded message and skip the keys it has
already processed.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, NoReturn, TypeGuard

from .claim_check.errors import InvalidClaimRefError
from .claim_check.ref import parse_claim_ref
from .webhook import HookHeaders

__all__ = ["InvalidMessageError", "Message", "decode_message", "idempotency_key"]

# The message JSON version this decoder understands. Every field since v1 was
# added without bumping it (see "Versioning" in docs/delivery.md), so a v1
# decoder stays correct as senders add fields.
_SUPPORTED_VERSION = 1

_HEX64 = re.compile(r"^[0-9a-f]{64}$")


class InvalidMessageError(ValueError):
    """The bytes handed to ``decode_message`` aren't a valid v1 queue message.

    Never retryable: the same bytes will fail the same way, so a consumer
    should dead-letter them rather than requeue. ``code`` is one of
    ``invalid_json``, ``not_an_object``, ``unsupported_version``,
    ``invalid_field``, ``ambiguous_body``, ``missing_body``,
    ``invalid_body_base64``, ``size_mismatch``, ``integrity`` or
    ``tenant_mismatch``; ``field`` names the offending key for
    ``invalid_field`` and is ``None`` otherwise.
    """

    retryable = False

    def __init__(self, message: str, *, code: str, field: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.field = field


@dataclass(frozen=True, slots=True)
class Message:
    """One decoded v1 queue message.

    ``body_base64`` and ``claim`` are mutually exclusive; ``body_base64`` is
    the decoded body re-encoded as standard base64, so compare bytes.
    ``dedupe_key``, ``replay_id`` and ``sha256`` are ``None`` when the sender
    omitted them; ``headers`` is ``{}`` when there are none.
    """

    v: int
    id: str
    source_id: str
    tenant_id: str | None
    received_at: int
    content_type: str | None
    size: int
    body_base64: str | None
    claim: str | None
    sha256: str | None
    dedupe_key: str | None
    replay_id: str | None
    headers: dict[str, str]


def _is_int(value: Any) -> TypeGuard[int]:
    # ``bool`` is a subclass of ``int`` in Python; JSON ``true`` is not an
    # integer field value.
    return isinstance(value, int) and not isinstance(value, bool)


def _invalid_field(field: str) -> NoReturn:
    raise InvalidMessageError(f"invalid or missing field: {field}", code="invalid_field", field=field)


def _optional_string(payload: dict[str, Any], name: str) -> str | None:
    value = payload.get(name)
    if value is None:
        return None
    if not isinstance(value, str):
        _invalid_field(name)
    return value


def decode_message(data: str | bytes) -> Message:
    """Decode and validate one v1 queue message.

    ``data`` is the JSON string (or its UTF-8 bytes) exactly as the broker / a
    sink delivered it. The checks run in a fixed order and the first failure
    wins, raising ``InvalidMessageError``; see the module docstring for the
    field set. Unknown keys are ignored, on purpose, so a newer producer can
    add fields without breaking this decoder.
    """
    try:
        payload = json.loads(data)
    except (ValueError, TypeError) as err:
        raise InvalidMessageError("message is not valid JSON", code="invalid_json") from err
    if not isinstance(payload, dict):
        raise InvalidMessageError("message is not a JSON object", code="not_an_object")

    version = payload.get("v")
    if not _is_int(version) or version != _SUPPORTED_VERSION:
        raise InvalidMessageError(f"unsupported message version: {version!r}", code="unsupported_version")

    hook_id = payload.get("id")
    if not isinstance(hook_id, str) or not hook_id:
        _invalid_field("id")

    source_id = payload.get("source_id")
    if not isinstance(source_id, str):
        _invalid_field("source_id")

    received_at = payload.get("received_at")
    if not _is_int(received_at):
        _invalid_field("received_at")

    size = payload.get("size")
    if not _is_int(size):
        _invalid_field("size")
    if size < 0:
        _invalid_field("size")

    tenant_id = _optional_string(payload, "tenant_id")
    content_type = _optional_string(payload, "content_type")
    dedupe_key = _optional_string(payload, "dedupe_key")
    replay_id = _optional_string(payload, "replay_id")

    forwarded = payload.get("headers")
    headers: dict[str, str] = {}
    if forwarded is not None:
        if not isinstance(forwarded, dict) or not all(isinstance(value, str) for value in forwarded.values()):
            _invalid_field("headers")
        headers = dict(forwarded)

    sha256 = payload.get("sha256")
    if sha256 is not None and not (isinstance(sha256, str) and _HEX64.fullmatch(sha256)):
        _invalid_field("sha256")

    body_base64 = payload.get("body_base64")
    claim = payload.get("claim")
    if body_base64 is not None and claim is not None:
        raise InvalidMessageError("message carries both body_base64 and claim", code="ambiguous_body")
    if body_base64 is None and claim is None:
        raise InvalidMessageError("message carries neither body_base64 nor claim", code="missing_body")

    body: bytes | None = None
    claim_tenant: str | None = None
    if body_base64 is not None:
        if not isinstance(body_base64, str):
            raise InvalidMessageError("body_base64 is not valid base64", code="invalid_body_base64")
        try:
            body = base64.b64decode(body_base64, validate=True)
        except (binascii.Error, ValueError, TypeError) as err:
            raise InvalidMessageError("body_base64 is not valid base64", code="invalid_body_base64") from err
        if len(body) != size:
            raise InvalidMessageError(f"size {size} does not match the decoded body ({len(body)} bytes)", code="size_mismatch")
        if sha256 is not None and hashlib.sha256(body).hexdigest() != sha256:
            raise InvalidMessageError("body does not match the message sha256", code="integrity")
    else:
        if not isinstance(claim, str):
            _invalid_field("claim")
        try:
            claim_tenant = parse_claim_ref(claim).tenant_id
        except (InvalidClaimRefError, TypeError) as err:
            raise InvalidMessageError(f"claim is not a valid claim ref: {claim!r}", code="invalid_field", field="claim") from err
        if sha256 is None:
            _invalid_field("sha256")
        if tenant_id is not None and claim_tenant != tenant_id:
            raise InvalidMessageError(
                f"claim tenant {claim_tenant!r} does not match tenant_id {tenant_id!r}", code="tenant_mismatch"
            )

    return Message(
        v=_SUPPORTED_VERSION,
        id=hook_id,
        source_id=source_id,
        tenant_id=tenant_id,
        received_at=received_at,
        content_type=content_type,
        size=size,
        body_base64=base64.b64encode(body).decode("ascii") if body is not None else None,
        claim=claim,
        sha256=sha256,
        dedupe_key=dedupe_key,
        replay_id=replay_id,
        headers=headers,
    )


def idempotency_key(hook: Message | HookHeaders, *, include_replay: bool = False) -> str:
    """The key a consumer stores in its processed-ids table.

    Start from ``dedupe_key`` (the provider's own event key): when it is set,
    the key is ``source_id:dedupe_key``, so a provider retry that arrives with
    a fresh Ankusa ``id`` still collapses to the same row. With no
    ``dedupe_key`` the key is ``id`` -- the identity Ankusa has always sent.

    ``include_replay`` defaults to ``False``, so a replay of an event already
    processed is dropped. A consumer that must re-run replays sets it, and the
    key gains a ``#replay:<replay_id>`` suffix. ``hook`` may be a decoded
    ``Message`` or the ``HookHeaders`` of an HTTP-sink delivery (where the
    ``source`` header plays the part of ``source_id``).
    """
    if isinstance(hook, Message):
        source_id, dedupe_key, hook_id, replay_id = hook.source_id, hook.dedupe_key, hook.id, hook.replay_id
    elif isinstance(hook, HookHeaders):
        source_id, dedupe_key, hook_id, replay_id = hook.source, hook.dedupe_key, hook.id, hook.replay_id
    else:
        raise TypeError(f"hook must be a Message or HookHeaders, got {type(hook).__name__}")

    key = f"{source_id}:{dedupe_key}" if dedupe_key else hook_id
    if include_replay and replay_id:
        key = f"{key}#replay:{replay_id}"
    return key
