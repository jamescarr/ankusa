package io.github.jamescarr.ankusa.routes;

import io.github.jamescarr.ankusa.AnkusaException;
import org.jspecify.annotations.Nullable;

/**
 * The base of every error {@link RoutesClient} raises; no other error escapes it.
 *
 * <p>Sealed, so a catch of this type is exhaustive: the four subclasses below are everything a
 * routes call can raise.
 */
public abstract sealed class RoutesError extends AnkusaException
    permits InvalidRouteIdError, RouteNotFoundError, RoutesRejectedError, RoutesUnavailableError {

  /**
   * Creates an error carrying the message a caller sees.
   *
   * @param message the failure, already formatted
   */
  protected RoutesError(String message) {
    super(message);
  }

  /**
   * Creates an error carrying the message a caller sees and the failure underneath it.
   *
   * @param message the failure, already formatted
   * @param cause the underlying failure, or null when there is none
   */
  protected RoutesError(String message, @Nullable Throwable cause) {
    super(message, cause);
  }
}
