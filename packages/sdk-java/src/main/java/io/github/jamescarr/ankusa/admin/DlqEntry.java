package io.github.jamescarr.ankusa.admin;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One dead-lettered hook, as {@code GET /v1/dlq} lists it.
 *
 * @param id the dead-letter entry's id
 * @param sourceId the source that produced the hook
 * @param tenantId the tenant the hook belonged to, or null when it had none
 * @param seq the source's sequence number, or null when the entry carries none
 * @param receivedAt when the listener received the hook, in Unix milliseconds
 * @param deadLetteredAt when the delivery was dead-lettered, in Unix milliseconds
 * @param size the stored body's size in bytes
 * @param contentType the hook's content type, or null when it carried none
 * @param sink the index of the sink that was due this row in the source's {@code sinks} list, 0
 *     being the first, or null from a node older than 0.7
 * @param errorClass {@code "permanent"}, {@code "throttled"} or {@code "transient"}, or null for a
 *     row dead-lettered before the node recorded a class
 * @param reason why the delivery was dead-lettered
 */
public record DlqEntry(
    String id,
    String sourceId,
    @Nullable String tenantId,
    @Nullable Long seq,
    long receivedAt,
    long deadLetteredAt,
    long size,
    @Nullable String contentType,
    @Nullable Integer sink,
    @Nullable String errorClass,
    String reason) {

  /**
   * Rejects a body that carried no id, source id, or reason.
   *
   * @throws NullPointerException when the body has no {@code id}, {@code source_id}, or {@code
   *     reason} field
   */
  public DlqEntry {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(sourceId, "sourceId");
    Objects.requireNonNull(reason, "reason");
  }
}
