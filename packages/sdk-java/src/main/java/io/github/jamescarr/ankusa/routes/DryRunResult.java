package io.github.jamescarr.ankusa.routes;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * What the route table would do with a request.
 *
 * @param decision the decision, such as {@code allow} or {@code deny}
 * @param reason why the decision was made
 * @param routeId the route that matched, or null when none did
 * @param ipRule the IP rule that matched, or null when none did
 */
public record DryRunResult(
    String decision, String reason, @Nullable String routeId, @Nullable DryRunIpRule ipRule) {

  /**
   * Validates the components.
   *
   * @throws NullPointerException when {@code decision} or {@code reason} is null
   */
  public DryRunResult {
    Objects.requireNonNull(decision, "decision");
    Objects.requireNonNull(reason, "reason");
  }
}
