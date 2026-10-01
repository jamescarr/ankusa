package io.github.jamescarr.ankusa.admin;

import java.util.Objects;

/**
 * One quarantined hook, as {@code GET /v1/quarantine} lists it.
 *
 * @param id the quarantine entry's id
 * @param sourceId the source that produced the hook
 * @param receivedAt when the listener received the hook, in Unix milliseconds
 * @param reason why the hook was quarantined
 */
public record QuarantineEntry(String id, String sourceId, long receivedAt, String reason) {

  /**
   * Rejects a body that carried no id, source id, or reason.
   *
   * @throws NullPointerException when the body has no {@code id}, {@code source_id}, or {@code
   *     reason} field
   */
  public QuarantineEntry {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(sourceId, "sourceId");
    Objects.requireNonNull(reason, "reason");
  }
}
