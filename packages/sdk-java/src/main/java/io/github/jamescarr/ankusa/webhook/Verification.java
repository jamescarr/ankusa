package io.github.jamescarr.ankusa.webhook;

/**
 * How Ankusa verified one hook at ingest.
 *
 * @param status {@code ok}, {@code failed} or {@code skipped}; any other value passes through
 *     untouched (forward-compatible)
 * @param flagged true when a failed check was accepted anyway ({@code on_verify_failure:
 *     accept_flag}): the consumer's to distrust
 */
public record Verification(String status, boolean flagged) {}
