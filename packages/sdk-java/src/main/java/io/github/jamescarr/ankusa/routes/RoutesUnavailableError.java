package io.github.jamescarr.ankusa.routes;

import org.jspecify.annotations.Nullable;

/**
 * The route-management listener could not be reached, or answered something that is not a route.
 *
 * <p>The only retryable routes error: a 5xx, a 1xx, a 3xx that was not followed, and a transport
 * failure or decode failure are all conditions a later identical request can succeed at.
 */
public final class RoutesUnavailableError extends RoutesError {

  /**
   * Creates the error.
   *
   * @param message the whole message, naming what failed
   */
  public RoutesUnavailableError(String message) {
    super(message);
  }

  /**
   * Creates the error with the failure underneath it.
   *
   * @param message the whole message, naming what failed
   * @param cause the transport or decode failure, or null when there is none
   */
  public RoutesUnavailableError(String message, @Nullable Throwable cause) {
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
