package io.github.jamescarr.ankusa.routes;

import org.jspecify.annotations.Nullable;

/**
 * The listener answered 404: the route is well formed but no such route is configured.
 *
 * <p>Never retryable — a route that is absent now will be absent until something creates it.
 */
public final class RouteNotFoundError extends RoutesError {

  /** The id that was not found, or null for a collection call. */
  private final @Nullable String id;

  /**
   * Creates the error.
   *
   * @param id the id that was not found, or null for a collection call
   */
  public RouteNotFoundError(@Nullable String id) {
    super(id == null ? "route not found" : "route not found: \"" + id + "\"");
    this.id = id;
  }

  /**
   * The id that was not found.
   *
   * @return the id, or null for a collection call
   */
  public @Nullable String id() {
    return id;
  }
}
