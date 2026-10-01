package io.github.jamescarr.ankusa.routes;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

/**
 * The global IP rules: the action taken by default and the rules that override it.
 *
 * @param defaultAction the action taken for an address no rule matches, sent as {@code default}
 * @param rules the rules, in the order they are evaluated
 */
public record IpRules(@JsonProperty("default") String defaultAction, List<IpRule> rules) {

  /**
   * Validates and copies the components.
   *
   * <p>A null {@code rules} list from the wire becomes an empty list.
   *
   * @throws NullPointerException when {@code defaultAction} is null
   */
  public IpRules {
    Objects.requireNonNull(defaultAction, "defaultAction");
    rules = rules == null ? List.of() : List.copyOf(rules);
  }
}
