import { InvalidClaimRefError } from "./errors.js";

/** A parsed claim-check ref, ready to become a `GET /v1/claims/...` request. */
export type ParsedClaimRef = {
  tenantId: string;
  /** Canonical uppercase ULID naming the claim. */
  claimId: string;
  /** Gateway path to redeem it: `/v1/claims/{tenant_id}/{claim_id}`. */
  path: string;
};

// Mirrors `#/components/schemas/Ref` in priv/openapi/claim_check.v1.yaml —
// keep the two in sync. A ref is one string:
//   urn:ankusa:claim:v1:<tenant>:<claim_id>
// where <claim_id> is a canonical uppercase ULID (Crockford base32).
const REF_PATTERN =
  /^urn:ankusa:claim:v1:(?<tenantId>[A-Za-z0-9_-]{1,64}):(?<claimId>[0-7][0-9A-HJKMNP-TV-Z]{25})$/;

/**
 * Parse a claim-check ref (the `claim` field of a queue message) into the
 * tenant id, claim id, and the `GET /v1/claims/{tenant_id}/{claim_id}` path.
 * Throws `InvalidClaimRefError` — never worth retrying — if `ref` isn't a
 * well-formed ref.
 */
export function parseClaimRef(ref: string): ParsedClaimRef {
  const match = REF_PATTERN.exec(ref);
  if (!match?.groups) {
    throw new InvalidClaimRefError(`invalid claim-check ref: ${ref}`);
  }
  const { tenantId, claimId } = match.groups;
  return { tenantId, claimId, path: `/v1/claims/${tenantId}/${claimId}` };
}
