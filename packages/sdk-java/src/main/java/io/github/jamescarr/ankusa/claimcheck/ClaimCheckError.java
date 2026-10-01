package io.github.jamescarr.ankusa.claimcheck;

import io.github.jamescarr.ankusa.AnkusaException;
import org.jspecify.annotations.Nullable;

/**
 * The base of every error {@link ClaimCheckClient} raises; no other error escapes it.
 *
 * <p>Sealed, so a catch of this type is exhaustive: the five subclasses below are everything a
 * claim-check call can raise.
 */
public abstract sealed class ClaimCheckError extends AnkusaException
    permits InvalidClaimRefError,
        ClaimNotFoundError,
        ClaimRejectedError,
        ClaimIntegrityError,
        ClaimCheckUnavailableError {

  /**
   * Creates an error carrying the message a caller sees.
   *
   * @param message the failure, already formatted
   */
  protected ClaimCheckError(String message) {
    super(message);
  }

  /**
   * Creates an error carrying the message a caller sees and the failure underneath it.
   *
   * @param message the failure, already formatted
   * @param cause the underlying failure, or null when there is none
   */
  protected ClaimCheckError(String message, @Nullable Throwable cause) {
    super(message, cause);
  }
}
