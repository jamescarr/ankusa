import pytest

from ankusa import InvalidClaimRefError, ParsedClaimRef, parse_claim_ref

TENANT = "acme"
CLAIM_ID = "01M39VMD8RA3C5HR4RBV67Y002"
REF = f"urn:ankusa:claim:v1:{TENANT}:{CLAIM_ID}"


def test_splits_a_well_formed_ref_into_tenant_claim_id_and_path() -> None:
    assert parse_claim_ref(REF) == ParsedClaimRef(
        tenant_id=TENANT,
        claim_id=CLAIM_ID,
        path=f"/v1/claims/{TENANT}/{CLAIM_ID}",
    )


@pytest.mark.parametrize(
    "bad",
    [
        "not-a-ref",
        f"urn:ankusa:claim:v1:acme:{CLAIM_ID.lower()}",  # lowercase ULID
        f"urn:ankusa:claim:v1:acme:{CLAIM_ID[:-1]}",  # 25 chars
        f"urn:ankusa:claim:v1:acme:{CLAIM_ID}0",  # 27 chars
        f"urn:ankusa:claim:v1:acme:8{CLAIM_ID[1:]}",  # first char above 7
        *(f"urn:ankusa:claim:v1:acme:{CLAIM_ID[:-1]}{c}" for c in "ILOU"),  # forbidden letters
        f"{REF}:extra",  # extra segment
        f"{REF}\n",  # trailing newline
        f"urn:ankusa:claim:v1:bad.tenant:{CLAIM_ID}",
        "urn:ankusa:claim:v1:acme:0199a1c2-7b3e-7d4a-9c1f-2e5b8a6d4f10:66:17:sha256-"
        + "a" * 64,  # old format
    ],
)
def test_rejects_malformed_refs(bad: str) -> None:
    with pytest.raises(InvalidClaimRefError):
        parse_claim_ref(bad)
