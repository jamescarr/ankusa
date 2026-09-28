import hashlib

import pytest

from ankusa import InvalidClaimRefError, ParsedClaimRef, parse_claim_ref

TENANT = "acme"
OBJECT_ID = "0199a1c2-7b3e-7d4a-9c1f-2e5b8a6d4f10"
BODY = b"hello claim check"
SHA256 = hashlib.sha256(BODY).hexdigest()
REF = f"urn:ankusa:claim:v1:{TENANT}:{OBJECT_ID}:66:{len(BODY)}:sha256-{SHA256}"


def test_splits_a_well_formed_ref_into_its_path_segments() -> None:
    assert parse_claim_ref(REF) == ParsedClaimRef(
        tenant_id=TENANT,
        object_id=OBJECT_ID,
        offset="66",
        length=str(len(BODY)),
        sha256=SHA256,
    )


@pytest.mark.parametrize(
    "bad",
    [
        "not-a-ref",
        f"urn:ankusa:claim:v1:acme:not-a-uuid:66:18:sha256-{SHA256}",
        f"urn:ankusa:claim:v1:acme:{OBJECT_ID}:007:18:sha256-{SHA256}",  # leading zero
        f"urn:ankusa:claim:v1:acme:{OBJECT_ID}:66:18:sha256-deadbeef",  # short digest
    ],
)
def test_rejects_malformed_refs(bad: str) -> None:
    with pytest.raises(InvalidClaimRefError):
        parse_claim_ref(bad)
