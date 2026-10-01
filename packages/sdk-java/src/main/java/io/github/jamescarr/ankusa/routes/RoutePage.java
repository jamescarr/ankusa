package io.github.jamescarr.ankusa.routes;

import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * One page of routes.
 *
 * @param routes the routes on this page
 * @param nextCursor the cursor for the next page, or null when this is the last page
 */
public record RoutePage(List<Route> routes, @Nullable String nextCursor) {

  /**
   * Copies the page.
   *
   * <p>A null {@code routes} list from the wire becomes an empty list.
   */
  public RoutePage {
    routes = routes == null ? List.of() : List.copyOf(routes);
  }
}
