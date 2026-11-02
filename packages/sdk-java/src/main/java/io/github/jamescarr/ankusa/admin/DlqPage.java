package io.github.jamescarr.ankusa.admin;

import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * One page of dead-lettered hooks, as {@code GET /v1/dlq} returns it.
 *
 * @param total the number of matching entries before {@code limit}, or null when the node cannot
 *     count them without a scan (any filter except {@code tenant})
 * @param entries the page's entries, newest first
 * @param nextCursor the cursor to pass as {@link ListDeadLettersParams.Builder#cursor(String)} for
 *     the next page, or null on the last one; a page can be short, or empty, while it is set
 */
public record DlqPage(
    @Nullable Integer total, List<DlqEntry> entries, @Nullable String nextCursor) {

  /** Treats a body without an {@code entries} list as an empty page. */
  public DlqPage {
    entries = entries == null ? List.of() : List.copyOf(entries);
  }
}
