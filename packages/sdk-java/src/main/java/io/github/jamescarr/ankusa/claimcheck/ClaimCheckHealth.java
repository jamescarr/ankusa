package io.github.jamescarr.ankusa.claimcheck;

import java.util.Objects;

/**
 * The claim-check gateway's own health answer, as {@code GET /health} returns it.
 *
 * @param status the gateway's reported status, {@code "ok"} when it is serving
 */
public record ClaimCheckHealth(String status) {

  /**
   * Rejects a body that carried no status.
   *
   * @throws NullPointerException when the body has no {@code status} field
   */
  public ClaimCheckHealth {
    Objects.requireNonNull(status, "status");
  }
}
