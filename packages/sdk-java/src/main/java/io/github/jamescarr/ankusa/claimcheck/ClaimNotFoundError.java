package io.github.jamescarr.ankusa.claimcheck;

/**
 * The gateway answered 404: the reference is well formed, but no claim is stored under it.
 *
 * <p>Ordinary rather than exceptional — a claim that was already redeemed, or that expired, is gone
 * — so this is never retryable.
 */
public final class ClaimNotFoundError extends ClaimCheckError {

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
  public ClaimNotFoundError(String tenantId, String claimId) {
    super("claim not found: " + tenantId + "/" + claimId);
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
