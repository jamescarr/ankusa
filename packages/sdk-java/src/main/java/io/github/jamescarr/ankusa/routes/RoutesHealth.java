package io.github.jamescarr.ankusa.routes;

import java.util.Objects;

/**
 * The route-management listener's health.
 *
 * @param status the listener's status, such as {@code ok}
 * @param routes how many routes are currently configured
 */
public record RoutesHealth(String status, int routes) {

  /**
   * Validates the components.
   *
   * @throws NullPointerException when {@code status} is null
   */
  public RoutesHealth {
    Objects.requireNonNull(status, "status");
  }
}
