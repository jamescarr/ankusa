package io.github.jamescarr.ankusa.admin;

import org.jspecify.annotations.Nullable;

/**
 * The admin listener could not be reached, or answered something other than a refusal.
 *
 * <p>The only retryable admin error: a 5xx, a 1xx, a 3xx that was not followed, a body that could
 * not be decoded, and a transport or timeout failure are all conditions a later identical request
 * can succeed at.
 */
public final class AdminUnavailableError extends AdminError {

  /**
   * Creates the error.
   *
   * @param message the whole message, naming what failed
   */
  public AdminUnavailableError(String message) {
    super(message);
  }

  /**
   * Creates the error with the failure underneath it.
   *
   * @param message the whole message, naming what failed
   * @param cause the transport or decode failure, or null when there is none
   */
  public AdminUnavailableError(String message, @Nullable Throwable cause) {
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
