package io.github.jamescarr.ankusa.admin;

/**
 * The result of a DLQ replay, as {@code POST /v1/dlq/replay} returns it.
 *
 * @param replayed how many dead letters were replayed
 */
public record Replayed(int replayed) {}
