"""Parse a claim-check ref into the path a redeem request needs."""

from __future__ import annotations

import re
from dataclasses import dataclass

from .errors import InvalidClaimRefError


@dataclass(frozen=True, slots=True)
class ParsedClaimRef:
    """A parsed claim-check ref, ready to become a ``GET /v1/claims/...`` request."""

    tenant_id: str
    # Canonical (uppercase) ULID.
    claim_id: str
    # ``/v1/claims/{tenant_id}/{claim_id}``, followed by
    # ``?exp={exp_unix}&sig={signature}`` for a v2 ref.
    path: str


# Mirrors `#/components/schemas/Ref` in priv/openapi/claim_check.v1.yaml --
# keep the two in sync. A ref is one string:
#   urn:ankusa:claim:v1:<tenant>:<claim_id>
#   urn:ankusa:claim:v2:<tenant>:<claim_id>:<exp_unix>:<signature>
# <exp_unix> is a decimal of 1-12 digits with no leading zero and <signature>
# 43 unpadded base64url characters. The gateway checks the signature; the SDK
# only carries it. `[0-9]`, never `\d`: `\d` matches every Unicode digit.
_REF_PATTERN = re.compile(
    r"^urn:ankusa:claim:v1:"
    r"(?P<tenant_id>[A-Za-z0-9_-]{1,64}):"
    r"(?P<claim_id>[0-7][0-9A-HJKMNP-TV-Z]{25})$"
)
_REF_V2_PATTERN = re.compile(
    r"^urn:ankusa:claim:v2:"
    r"(?P<tenant_id>[A-Za-z0-9_-]{1,64}):"
    r"(?P<claim_id>[0-7][0-9A-HJKMNP-TV-Z]{25}):"
    r"(?P<exp>[1-9][0-9]{0,11}):"
    r"(?P<sig>[A-Za-z0-9_-]{43})$"
)


def parse_claim_ref(ref: str) -> ParsedClaimRef:
    """Parse a claim-check ref (the ``claim`` field of a queue message) into
    the tenant id, claim id, and ``GET /v1/claims/{tenant_id}/{claim_id}``
    path (with ``?exp=&sig=`` for a v2 ref). Raises ``InvalidClaimRefError``
    -- never worth retrying -- if ``ref`` isn't a well-formed ref.
    """
    match = _REF_V2_PATTERN.fullmatch(ref)
    if match is not None:
        tenant_id, claim_id = match["tenant_id"], match["claim_id"]
        path = f"/v1/claims/{tenant_id}/{claim_id}?exp={match['exp']}&sig={match['sig']}"
        return ParsedClaimRef(tenant_id=tenant_id, claim_id=claim_id, path=path)
    match = _REF_PATTERN.fullmatch(ref)
    if match is None:
        raise InvalidClaimRefError(f"invalid claim-check ref: {ref}")
    tenant_id, claim_id = match["tenant_id"], match["claim_id"]
    return ParsedClaimRef(tenant_id=tenant_id, claim_id=claim_id, path=f"/v1/claims/{tenant_id}/{claim_id}")
