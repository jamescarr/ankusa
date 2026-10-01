package io.github.jamescarr.ankusa.admin;

import io.github.jamescarr.ankusa.AnkusaException;
import org.jspecify.annotations.Nullable;

/**
 * The base of every error {@link AdminClient} raises; no other error escapes it.
 *
 * <p>Sealed, so a catch of this type is exhaustive: the three subclasses below are everything an
 * admin call can raise.
 */
public abstract sealed class AdminError extends AnkusaException
    permits RoleNotEnabledError, AdminRejectedError, AdminUnavailableError {

  /**
   * Creates an error carrying the message a caller sees.
   *
   * @param message the failure, already formatted
   */
  protected AdminError(String message) {
    super(message);
  }

  /**
   * Creates an error carrying the message a caller sees and the failure underneath it.
   *
   * @param message the failure, already formatted
   * @param cause the underlying failure, or null when there is none
   */
  protected AdminError(String message, @Nullable Throwable cause) {
    super(message, cause);
  }
}
