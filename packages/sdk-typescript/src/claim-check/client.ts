import { createHash } from "node:crypto";
import createClient from "openapi-fetch";

import {
  ClaimCheckUnavailableError,
  ClaimIntegrityError,
  ClaimNotFoundError,
  ClaimRejectedError,
} from "./errors.js";
import { parseClaimRef, type ParsedClaimRef } from "./ref.js";
import type { paths } from "./claim-check-schema.d.ts";

export type ClaimCheckClientOptions = {
  /** The `:claim_check` role's listener, e.g. `http://localhost:4001`. */
  baseUrl: string;
  /**
   * Headers attached to every request. The gateway itself does no auth (see
   * docs/claim-check.md) — this is for whatever a deployer's own boundary
   * (service mesh, Envoy, an API gateway) expects in front of it.
   */
  headers?: HeadersInit;
  /** Override for testing; defaults to the global `fetch`. */
  fetch?: typeof fetch;
};

export type ClaimCheckClient = {
  /**
   * Redeem a claim-check ref: fetch its bytes and verify them against the
   * ref's own declared size and sha256 before returning. The gateway does
   * not check integrity itself — see "Redeem a claim" in
   * docs/claim-check.md — so this end-to-end check always runs here.
   *
   * Rejects with a `ClaimCheckError`; check `.retryable` to sort a failure
   * into dead-letter (`false`) or retry (`true`).
   */
  redeem(ref: string): Promise<Buffer>;
  /** Liveness probe: `GET /health`. */
  health(): Promise<{ status: "ok" }>;
};

export function createClaimCheckClient(options: ClaimCheckClientOptions): ClaimCheckClient {
  const http = createClient<paths>({
    baseUrl: options.baseUrl,
    headers: options.headers,
    fetch: options.fetch,
  });

  async function redeem(ref: string): Promise<Buffer> {
    const parsed = parseClaimRef(ref);
    const bytes = await fetchBytes(parsed);
    verifyIntegrity(parsed, bytes);
    return bytes;
  }

  async function fetchBytes(parsed: ParsedClaimRef): Promise<Buffer> {
    const { tenantId: tenant_id, objectId: object_id, offset, length } = parsed;
    let data: ArrayBuffer | undefined;
    let status: number;
    let errorBody: unknown;
    try {
      const result = await http.GET("/v1/claims/{tenant_id}/{object_id}/{offset}/{length}", {
        params: { path: { tenant_id, object_id, offset, length } },
        parseAs: "arrayBuffer",
      });
      data = result.data as ArrayBuffer | undefined;
      status = result.response.status;
      errorBody = result.error;
    } catch (err) {
      throw new ClaimCheckUnavailableError(`claim-check gateway unreachable: ${(err as Error).message}`, err);
    }

    if (status === 404) {
      throw new ClaimNotFoundError(`claim not found: ${tenant_id}/${object_id}`);
    }
    if (status >= 400 && status < 500) {
      throw new ClaimRejectedError(`claim-check rejected redeem (${status}): ${JSON.stringify(errorBody)}`, status, errorBody);
    }
    if (data === undefined) {
      throw new ClaimCheckUnavailableError(`claim-check gateway error (${status}): ${JSON.stringify(errorBody)}`);
    }
    return Buffer.from(data);
  }

  async function health(): Promise<{ status: "ok" }> {
    try {
      const { data, response } = await http.GET("/health", {});
      if (!data) {
        throw new ClaimCheckUnavailableError(`claim-check gateway health check failed (${response.status})`);
      }
      return data;
    } catch (err) {
      if (err instanceof ClaimCheckUnavailableError) throw err;
      throw new ClaimCheckUnavailableError(`claim-check gateway unreachable: ${(err as Error).message}`, err);
    }
  }

  return { redeem, health };
}

/**
 * Integrity is checked here, end to end, by the actual redeemer — never
 * trusted from the gateway. Same discipline `Ankusa.ClaimCheck.redeem/2`
 * applies on the Elixir side.
 */
function verifyIntegrity(parsed: ParsedClaimRef, bytes: Buffer): void {
  const expectedLength = Number(parsed.length);
  if (bytes.length !== expectedLength) {
    throw new ClaimIntegrityError(
      `claim size mismatch for ${parsed.tenantId}/${parsed.objectId}: expected ${expectedLength}, got ${bytes.length}`,
    );
  }
  const sha256 = createHash("sha256").update(bytes).digest("hex");
  if (sha256 !== parsed.sha256) {
    throw new ClaimIntegrityError(`claim sha256 mismatch for ${parsed.tenantId}/${parsed.objectId}`);
  }
}
