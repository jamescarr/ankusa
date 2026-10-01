package io.github.jamescarr.ankusa.admin;

import java.util.List;

/**
 * Recent quarantined hooks, as {@code GET /v1/quarantine} returns them.
 *
 * @param entries the quarantined entries, newest first
 */
public record QuarantinePage(List<QuarantineEntry> entries) {

  /** Treats a body without an {@code entries} list as an empty page. */
  public QuarantinePage {
    entries = entries == null ? List.of() : List.copyOf(entries);
  }
}
