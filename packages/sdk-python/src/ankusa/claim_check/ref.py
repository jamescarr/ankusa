"""Parse a claim-check ref into the path segments a redeem request needs."""

from __future__ import annotations

import re
from dataclasses import dataclass

from .errors import InvalidClaimRefError


@dataclass(frozen=True, slots=True)
class ParsedClaimRef:
    """A parsed claim-check ref, ready to become a ``GET /v1/claims/...`` request."""

    tenant_id: str
    object_id: str
    offset: str
    length: str
    # Lowercase hex sha256. Never sent to the gateway -- checked against the
    # bytes it returns.
    sha256: str


# Mirrors `#/components/schemas/Ref` in priv/openapi/claim_check.v1.yaml --
# keep the two in sync. A ref is one string:
#   urn:ankusa:claim:v1:<tenant>:<object_id>:<offset>:<length>:sha256-<hex>
_REF_PATTERN = re.compile(
    r"^urn:ankusa:claim:v1:"
    r"(?P<tenant_id>[A-Za-z0-9_-]{1,64}):"
    r"(?P<object_id>[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}):"
    r"(?P<offset>0|[1-9][0-9]{0,11}):"
    r"(?P<length>[1-9][0-9]{0,11}):"
    r"sha256-(?P<sha256>[0-9a-f]{64})$"
)


def parse_claim_ref(ref: str) -> ParsedClaimRef:
    """Parse a claim-check ref (the ``claim`` field of a queue message) into the
    path segments ``GET /v1/claims/{tenant_id}/{object_id}/{offset}/{length}``
    needs. Raises ``InvalidClaimRefError`` -- never worth retrying -- if
    ``ref`` isn't a well-formed ref.
    """
    match = _REF_PATTERN.match(ref)
    if match is None:
        raise InvalidClaimRefError(f"invalid claim-check ref: {ref}")
    return ParsedClaimRef(**match.groupdict())
