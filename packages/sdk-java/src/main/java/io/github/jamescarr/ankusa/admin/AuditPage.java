package io.github.jamescarr.ankusa.admin;

import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * One page of the audit trail, as {@code GET /v1/audit} returns it.
 *
 * @param entries the page's entries, newest first
 * @param nextCursor pass as {@code cursor} for the next page, or null on the last page
 */
public record AuditPage(List<AuditEntry> entries, @Nullable String nextCursor) {

  /** Treats a body without an {@code entries} list as an empty page. */
  public AuditPage {
    entries = entries == null ? List.of() : List.copyOf(entries);
  }
}
