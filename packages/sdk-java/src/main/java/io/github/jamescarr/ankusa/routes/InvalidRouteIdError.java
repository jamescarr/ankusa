package io.github.jamescarr.ankusa.routes;

import org.jspecify.annotations.Nullable;

/**
 * A route id failed validation before any request was made.
 *
 * <p>An id must be non-null, not empty, and neither {@code .} nor {@code ..}; every other value
 * travels as one percent-encoded path segment.
 */
public final class InvalidRouteIdError extends RoutesError {

  /** The id that failed validation, or null when none was supplied. */
  private final @Nullable String id;

  /**
   * Creates the error.
   *
   * @param id the id that failed validation, or null when none was supplied
   */
  public InvalidRouteIdError(@Nullable String id) {
    super("invalid route id: \"" + id + "\"");
    this.id = id;
  }

  /**
   * The id that failed validation.
   *
   * @return the id, or null when none was supplied
   */
  public @Nullable String id() {
    return id;
  }
}
