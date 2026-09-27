import { InvalidClaimRefError } from "./errors.js";

/** A parsed claim-check ref, ready to become a `GET /v1/claims/...` request. */
export type ParsedClaimRef = {
  tenantId: string;
  objectId: string;
  offset: string;
  length: string;
  /** Lowercase hex sha256. Never sent to the gateway — checked against the bytes it returns. */
  sha256: string;
};

// Mirrors `#/components/schemas/Ref` in priv/openapi/claim_check.v1.yaml —
// keep the two in sync. A ref is one string:
//   urn:ankusa:claim:v1:<tenant>:<object_id>:<offset>:<length>:sha256-<hex>
const REF_PATTERN =
  /^urn:ankusa:claim:v1:(?<tenantId>[A-Za-z0-9_-]{1,64}):(?<objectId>[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}):(?<offset>0|[1-9][0-9]{0,11}):(?<length>[1-9][0-9]{0,11}):sha256-(?<sha256>[0-9a-f]{64})$/;

/**
 * Parse a claim-check ref (the `claim` field of a queue message) into the
 * path segments `GET /v1/claims/{tenant_id}/{object_id}/{offset}/{length}`
 * needs. Throws `InvalidClaimRefError` — never worth retrying — if `ref`
 * isn't a well-formed ref.
 */
export function parseClaimRef(ref: string): ParsedClaimRef {
  const match = REF_PATTERN.exec(ref);
  if (!match?.groups) {
    throw new InvalidClaimRefError(`invalid claim-check ref: ${ref}`);
  }
  const { tenantId, objectId, offset, length, sha256 } = match.groups;
  return { tenantId, objectId, offset, length, sha256 };
}
