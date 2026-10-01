package io.github.jamescarr.ankusa.routes;

import java.util.Objects;

/**
 * The IP rule a dry run matched, and how it was reached.
 *
 * @param action the action the rule took
 * @param cidr the rule's address range
 * @param scope where the rule lives, such as global or route
 */
public record DryRunIpRule(String action, String cidr, String scope) {

  /**
   * Validates the components.
   *
   * @throws NullPointerException when any component is null
   */
  public DryRunIpRule {
    Objects.requireNonNull(action, "action");
    Objects.requireNonNull(cidr, "cidr");
    Objects.requireNonNull(scope, "scope");
  }
}
