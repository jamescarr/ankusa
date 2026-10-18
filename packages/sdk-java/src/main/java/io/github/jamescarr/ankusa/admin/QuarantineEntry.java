package io.github.jamescarr.ankusa.admin;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One quarantined hook, as {@code GET /v1/quarantine} lists it.
 *
 * @param id the quarantine entry's id
 * @param sourceId the source that produced the hook
 * @param tenantId the hook's tenant, or null for an entry held before the listener recorded it
 * @param receivedAt when the listener received the hook, in Unix milliseconds
 * @param reason why the hook was quarantined
 * @param size the bytes the entry holds against {@code quarantine.max_bytes}, or null for an entry
 *     held before the listener recorded it
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record QuarantineEntry(
    String id,
    String sourceId,
    @Nullable String tenantId,
    long receivedAt,
    String reason,
    @Nullable Long size) {

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
