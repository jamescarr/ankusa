package io.github.jamescarr.ankusa.admin;

import java.util.List;

/**
 * One page of dead-lettered hooks, as {@code GET /v1/dlq} returns it.
 *
 * @param total how many entries the queue holds in all
 * @param entries the page's entries, newest first
 */
public record DlqPage(int total, List<DlqEntry> entries) {

  /** Treats a body without an {@code entries} list as an empty page. */
  public DlqPage {
    entries = entries == null ? List.of() : List.copyOf(entries);
  }
}
