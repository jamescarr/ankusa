package io.github.jamescarr.ankusa.claimcheck;

/**
 * The bytes the gateway returned do not hash to the digest the caller expected.
 *
 * <p>The claim is not returned: a body that fails its own digest is evidence of a corrupted or
 * tampered transfer, so the caller must not process it. Never retryable — a retry of the same
 * reference would fetch the same bytes.
 */
public final class ClaimIntegrityError extends ClaimCheckError {

  /** The tenant named by the reference. */
  private final String tenantId;

  /** The claim id named by the reference. */
  private final String claimId;

  /**
   * Creates the error.
   *
   * @param tenantId the tenant named by the reference
   * @param claimId the claim id named by the reference
   */
  public ClaimIntegrityError(String tenantId, String claimId) {
    super("claim sha256 mismatch for " + tenantId + "/" + claimId);
    this.tenantId = tenantId;
    this.claimId = claimId;
  }

  /**
   * The tenant named by the reference.
   *
   * @return the tenant id
   */
  public String tenantId() {
    return tenantId;
  }

  /**
   * The claim id named by the reference.
   *
   * @return the claim id
   */
  public String claimId() {
    return claimId;
  }
}
