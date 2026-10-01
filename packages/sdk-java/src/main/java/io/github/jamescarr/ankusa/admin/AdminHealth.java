package io.github.jamescarr.ankusa.admin;

import java.util.List;
import java.util.Objects;

/**
 * The operator listener's own health answer, as {@code GET /health} returns it.
 *
 * <p>Deliberately carries no version: this listener's body does not have one, and a component no
 * response sets would only add a null key to every comparison.
 *
 * @param status the listener's reported status, {@code "ok"} when it is serving
 * @param instance the node's configured name
 * @param roles the roles this node runs, as configured through {@code ANKUSA_ROLES}
 */
public record AdminHealth(String status, String instance, List<String> roles) {

  /**
   * Rejects a body that carried no status or instance.
   *
   * @throws NullPointerException when the body has no {@code status} or {@code instance} field
   */
  public AdminHealth {
    Objects.requireNonNull(status, "status");
    Objects.requireNonNull(instance, "instance");
    roles = roles == null ? List.of() : List.copyOf(roles);
  }
}
