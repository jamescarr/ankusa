package io.github.jamescarr.ankusa.admin;

/**
 * What {@code DELETE /v1/quarantine} removed.
 *
 * @param deleted how many entries were deleted
 * @param bytes the bytes they held against {@code quarantine.max_bytes}
 */
public record QuarantinePurge(long deleted, long bytes) {}
