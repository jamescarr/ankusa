import { createHash } from "node:crypto";
import createClient from "openapi-fetch";

import {
  ClaimCheckUnavailableError,
  ClaimIntegrityError,
  ClaimNotFoundError,
  ClaimRejectedError,
  InvalidClaimRefError,
} from "./errors.js";
import { parseClaimRef, type ParsedClaimRef } from "./ref.js";
import type { paths } from "./claim-check-schema.d.ts";

/** The `sha256` field of a queue message: 64 lowercase hex chars. */
const SHA256_PATTERN = /^[0-9a-f]{64}$/;

export type ClaimCheckClientOptions = {
  /** The `:claim_check` role's listener, e.g. `http://localhost:4001`. */
  baseUrl: string;
  /**
   * Headers attached to every request. The gateway itself does no auth (see
   * docs/claim-check.md) — this is for whatever a deployer's own boundary
   * (service mesh, Envoy, an API gateway) expects in front of it.
   */
  headers?: HeadersInit;
  /**
   * Per-request deadline in milliseconds, covering connect through the last
   * body byte. Defaults to `10_000`, matching the Python client's
   * `timeout=10.0`. Exceeding it rejects with `ClaimCheckUnavailableError`.
   */
  timeoutMs?: number;
  /** Override for testing; defaults to the global `fetch`. */
  fetch?: typeof fetch;
};

export type ClaimCheckClient = {
  /**
   * Redeem a claim-check ref: fetch its bytes and verify them against
   * `sha256` (the lowercase hex digest carried next to `claim` on the queue
   * message) before returning. A malformed ref or sha256 is rejected with
   * `InvalidClaimRefError` before any request is made. The gateway does
   * not check integrity itself — see "Redeem a claim" in
   * docs/claim-check.md — so this end-to-end check always runs here.
   *
   * Rejects with a `ClaimCheckError`; check `.retryable` to sort a failure
   * into dead-letter (`false`) or retry (`true`).
   */
  redeem(ref: string, sha256: string): Promise<Buffer>;
  /** Liveness probe: `GET /health`. */
  health(): Promise<{ status: "ok" }>;
};

export function createClaimCheckClient(options: ClaimCheckClientOptions): ClaimCheckClient {
  const timeoutMs = options.timeoutMs ?? 10_000;
  const http = createClient<paths>({
    baseUrl: options.baseUrl,
    headers: options.headers,
    fetch: options.fetch,
    // A 3xx is a gateway error, not a hop: the Python client doesn't follow
    // redirects either.
    redirect: "manual",
  });

  async function redeem(ref: string, sha256: string): Promise<Buffer> {
    const parsed = parseClaimRef(ref);
    if (typeof sha256 !== "string" || !SHA256_PATTERN.test(sha256)) {
      throw new InvalidClaimRefError(`invalid claim sha256: ${sha256}`);
    }
    const bytes = await fetchBytes(parsed);
    verifyIntegrity(parsed, sha256, bytes);
    return bytes;
  }

  async function fetchBytes(parsed: ParsedClaimRef): Promise<Buffer> {
    const { tenantId: tenant_id, claimId: claim_id } = parsed;
    let data: ArrayBuffer | undefined;
    let status: number;
    let errorBody: unknown;
    try {
      const result = await http.GET("/v1/claims/{tenant_id}/{claim_id}", {
        params: { path: { tenant_id, claim_id } },
        parseAs: "arrayBuffer",
        signal: AbortSignal.timeout(timeoutMs),
      });
      data = result.data as ArrayBuffer | undefined;
      status = result.response.status;
      errorBody = result.error;
    } catch (err) {
      throw new ClaimCheckUnavailableError(`claim-check gateway unreachable: ${(err as Error).message}`, err);
    }

    if (status === 404) {
      throw new ClaimNotFoundError(`claim not found: ${tenant_id}/${claim_id}`);
    }
    // An empty error body arrives as `undefined`; the Python client reports it
    // as "".
    const body = errorBody === undefined ? "" : errorBody;
    if (status >= 400 && status < 500) {
      throw new ClaimRejectedError(`claim-check rejected redeem (${status}): ${JSON.stringify(body)}`, status, body);
    }
    if (status !== 200) {
      throw new ClaimCheckUnavailableError(`claim-check gateway error (${status}): ${JSON.stringify(body)}`);
    }
    // An empty 200 body is valid bytes, verified against the expected sha256
    // like any other.
    return Buffer.from(data ?? new ArrayBuffer(0));
  }

  async function health(): Promise<{ status: "ok" }> {
    try {
      const { data, response } = await http.GET("/health", { signal: AbortSignal.timeout(timeoutMs) });
      if (response.status !== 200 || data === undefined) {
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
 * trusted from the gateway. Same discipline `Ankusa.ClaimCheck.redeem/3`
 * applies on the Elixir side.
 */
function verifyIntegrity(parsed: ParsedClaimRef, expectedSha256: string, bytes: Buffer): void {
  const sha256 = createHash("sha256").update(bytes).digest("hex");
  if (sha256 !== expectedSha256) {
    throw new ClaimIntegrityError(`claim sha256 mismatch for ${parsed.tenantId}/${parsed.claimId}`);
  }
}
