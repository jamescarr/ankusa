package io.github.jamescarr.ankusa.admin;

import org.jspecify.annotations.Nullable;

/**
 * The listener answered {@code 409 role_not_enabled}: this node does not run the role the operation
 * needs.
 *
 * <p>Not retryable against the same node, but another node in a fleet may run the role. {@link
 * #role()} names it; the operator adds it to {@code ANKUSA_ROLES} and restarts.
 */
public final class RoleNotEnabledError extends AdminError {

  /** The role this node does not run, or null when the body named none. */
  private final @Nullable String role;

  /**
   * Creates the error.
   *
   * @param role the role this node does not run, or null when the body named none
   */
  public RoleNotEnabledError(@Nullable String role) {
    super("role not enabled on this node: " + role);
    this.role = role;
  }

  /**
   * The role the listener said this node does not run.
   *
   * @return the role name, or null when the body named none
   */
  public @Nullable String role() {
    return role;
  }
}
