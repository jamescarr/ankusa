package io.github.jamescarr.ankusa.admin;

import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One row of the audit trail, as {@code GET /v1/audit} lists it.
 *
 * @param seq the row's place in commit order; strictly increasing, the row's key
 * @param at when the change was committed, as the node recorded it
 * @param actor the principal id that made the change; {@code system} for a change made from code
 * @param remoteIp the client address, behind {@code routes.trusted_proxies} the one {@code
 *     X-Forwarded-For} names, or null when none was recorded
 * @param action the action, e.g. {@code source.update} or {@code route.delete}
 * @param tenant the tenant the row belongs to, or null for a row that belongs to none
 * @param resource the subject's id ({@code <tenant>.<name>} for a source, the route id, the replay
 *     job id), or null when the action has none
 * @param beforeHash HMAC-SHA256 (hex) of the subject before the change, or null when there was none
 * @param afterHash HMAC-SHA256 (hex) of the subject after the change, or null for a delete
 * @param requestId the request's {@code X-Request-Id}, or the id the node made up, or null
 * @param result {@code ok}, {@code failed}, or {@code interrupted}
 * @param revision the subject's revision after the change, for a subject that has one, or null
 * @param detail non-secret particulars: a purge's filter and count, a replay's kind, a rate limit's
 *     numbers
 */
public record AuditEntry(
    long seq,
    String at,
    String actor,
    @Nullable String remoteIp,
    String action,
    @Nullable String tenant,
    @Nullable String resource,
    @Nullable String beforeHash,
    @Nullable String afterHash,
    @Nullable String requestId,
    String result,
    @Nullable Long revision,
    Map<String, Object> detail) {

  /**
   * Rejects a body that carried no {@code at}, {@code actor}, {@code action}, or {@code result},
   * and treats a body with no {@code detail} as an empty one.
   *
   * @throws NullPointerException when the body has no {@code at}, {@code actor}, {@code action}, or
   *     {@code result} field
   */
  public AuditEntry {
    Objects.requireNonNull(at, "at");
    Objects.requireNonNull(actor, "actor");
    Objects.requireNonNull(action, "action");
    Objects.requireNonNull(result, "result");
    detail = detail == null ? Map.of() : Map.copyOf(detail);
  }
}
