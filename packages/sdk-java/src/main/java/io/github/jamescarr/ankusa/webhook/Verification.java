package io.github.jamescarr.ankusa.webhook;

/**
 * How Ankusa verified one hook at ingest.
 *
 * @param status {@code ok}, {@code failed} or {@code skipped}; any other value passes through
 *     untouched (forward-compatible)
 * @param flagged true when a failed check was accepted anyway ({@code on_verify_failure:
 *     accept_flag}): the consumer's to distrust
 */
public record Verification(String status, boolean flagged) {

  /**
   * Whether the node verified the hook and nothing flags it: true only for status {@code ok} with
   * {@code flagged} false. A flagged hook, a {@code failed} or {@code skipped} one, and any other
   * status are untrusted.
   *
   * <p>Named {@code trusted()}, not {@code isTrusted()}, so a bean serializer does not add a {@code
   * trusted} property to the record's JSON shape.
   *
   * @return true when the hook may be treated as verified
   */
  public boolean trusted() {
    return "ok".equals(status) && !flagged;
  }
}
