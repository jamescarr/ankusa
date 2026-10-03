package io.github.jamescarr.ankusa.admin;

import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * A replay job, as the {@code /v1/replays} endpoints return it.
 *
 * <p>A job re-sends either dead-letter queue rows ({@code kind: "dlq"}) or hooks archived over a
 * time window ({@code kind: "archive"}). It moves rows only as fast as dispatch has spare capacity,
 * so it never delays live traffic. {@code moved}, {@code scanned} and {@code skipped} are exact;
 * {@code delivered} and {@code dead} are the outcomes the dispatcher reported and are approximate
 * across a restart.
 *
 * @param id the job id, a UUIDv7
 * @param kind {@code "dlq"} or {@code "archive"}
 * @param state {@code "running"}, {@code "paused"}, {@code "done"}, {@code "cancelled"}, or {@code
 *     "failed"}
 * @param filter exactly the filter keys the job was created with
 * @param rate the target items per second
 * @param maxLagMs the dispatcher lag above which the job pauses itself
 * @param createdAt when the job was created, in Unix milliseconds
 * @param updatedAt when the job last changed, in Unix milliseconds
 * @param finishedAt when the job finished, or null while it has not
 * @param moved rows revived ({@code dlq}) or hooks re-enqueued ({@code archive})
 * @param scanned keys or records examined
 * @param skipped archive records with no source, no bound sink, or an undecodable frame
 * @param delivered outcomes the dispatcher reported as delivered
 * @param dead outcomes the dispatcher reported as dead-lettered
 * @param error the auto-pause or failure reason, or null when there is none
 */
public record Replay(
    String id,
    String kind,
    String state,
    Map<String, @Nullable Object> filter,
    int rate,
    int maxLagMs,
    long createdAt,
    long updatedAt,
    @Nullable Long finishedAt,
    int moved,
    int scanned,
    int skipped,
    int delivered,
    int dead,
    @Nullable String error) {

  /** Treats a body without a {@code filter} object as an empty one. */
  public Replay {
    filter = filter == null ? Map.of() : Map.copyOf(filter);
  }
}
