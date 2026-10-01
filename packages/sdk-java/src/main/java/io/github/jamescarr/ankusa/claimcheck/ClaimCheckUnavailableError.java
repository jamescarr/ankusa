package io.github.jamescarr.ankusa.claimcheck;

import org.jspecify.annotations.Nullable;

/**
 * The claim-check gateway could not be reached, or answered something that is not a claim.
 *
 * <p>The only retryable claim-check error: a 5xx, a 1xx, a 3xx that was not followed, a health
 * check that did not answer 200, and a transport failure are all conditions a later identical
 * request can succeed at.
 */
public final class ClaimCheckUnavailableError extends ClaimCheckError {

  /**
   * Creates the error.
   *
   * @param message the whole message, naming what failed
   */
  public ClaimCheckUnavailableError(String message) {
    super(message);
  }

  /**
   * Creates the error with the failure underneath it.
   *
   * @param message the whole message, naming what failed
   * @param cause the transport or decode failure, or null when there is none
   */
  public ClaimCheckUnavailableError(String message, @Nullable Throwable cause) {
    super(message, cause);
  }

  /**
   * Whether an identical request made later could succeed.
   *
   * @return true, always
   */
  @Override
  public boolean retryable() {
    return true;
  }
}
