package io.github.jamescarr.ankusa.claimcheck;

/**
 * The claim-check reference, or the expected digest, is not the shape the gateway understands.
 *
 * <p>Raised before any request is made, so a caller that sees this has sent nothing.
 */
public final class InvalidClaimRefError extends ClaimCheckError {

  /**
   * Creates the error.
   *
   * @param message the whole message, naming the value that was rejected
   */
  public InvalidClaimRefError(String message) {
    super(message);
  }
}
