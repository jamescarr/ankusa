package io.github.jamescarr.ankusa.routes;

import java.util.Objects;

/**
 * One IP rule: an action taken for addresses in a range.
 *
 * @param action the action to take, such as {@code allow} or {@code deny}
 * @param cidr the address range, in CIDR notation
 */
public record IpRule(String action, String cidr) {

  /**
   * Validates the components.
   *
   * @throws NullPointerException when {@code action} or {@code cidr} is null
   */
  public IpRule {
    Objects.requireNonNull(action, "action");
    Objects.requireNonNull(cidr, "cidr");
  }
}
