import { InvalidClaimRefError } from "./errors.js";

/** A parsed claim-check ref, ready to become a `GET /v1/claims/...` request. */
export type ParsedClaimRef = {
  tenantId: string;
  /** Canonical uppercase ULID naming the claim. */
  claimId: string;
  /**
   * Gateway path to redeem it: `/v1/claims/{tenant_id}/{claim_id}`, followed
   * by `?exp={exp_unix}&sig={signature}` for a v2 ref.
   */
  path: string;
  /** A v2 ref's expiry (Unix seconds) and signature; absent on a v1 ref. */
  proof?: { exp: string; sig: string };
};

// Mirrors `#/components/schemas/Ref` in priv/openapi/claim_check.v1.yaml —
// keep the two in sync. A ref is one string:
//   urn:ankusa:claim:v1:<tenant>:<claim_id>
//   urn:ankusa:claim:v2:<tenant>:<claim_id>:<exp_unix>:<signature>
// where <claim_id> is a canonical uppercase ULID (Crockford base32),
// <exp_unix> a decimal of 1-12 digits with no leading zero, and <signature>
// 43 unpadded base64url characters. The gateway checks the signature; the SDK
// only carries it.
const REF_PATTERN =
  /^urn:ankusa:claim:v1:(?<tenantId>[A-Za-z0-9_-]{1,64}):(?<claimId>[0-7][0-9A-HJKMNP-TV-Z]{25})$/;
const REF_V2_PATTERN =
  /^urn:ankusa:claim:v2:(?<tenantId>[A-Za-z0-9_-]{1,64}):(?<claimId>[0-7][0-9A-HJKMNP-TV-Z]{25}):(?<exp>[1-9][0-9]{0,11}):(?<sig>[A-Za-z0-9_-]{43})$/;

/**
 * Parse a claim-check ref (the `claim` field of a queue message) into the
 * tenant id, claim id, and the `GET /v1/claims/{tenant_id}/{claim_id}` path
 * (with `?exp=&sig=` for a v2 ref). Throws `InvalidClaimRefError` — never worth
 * retrying — if `ref` isn't a well-formed ref.
 */
export function parseClaimRef(ref: string): ParsedClaimRef {
  const signed = REF_V2_PATTERN.exec(ref);
  if (signed?.groups) {
    const { tenantId, claimId, exp, sig } = signed.groups;
    return {
      tenantId,
      claimId,
      path: `/v1/claims/${tenantId}/${claimId}?exp=${exp}&sig=${sig}`,
      proof: { exp, sig },
    };
  }
  const match = REF_PATTERN.exec(ref);
  if (!match?.groups) {
    throw new InvalidClaimRefError(`invalid claim-check ref: ${ref}`);
  }
  const { tenantId, claimId } = match.groups;
  return { tenantId, claimId, path: `/v1/claims/${tenantId}/${claimId}` };
}
