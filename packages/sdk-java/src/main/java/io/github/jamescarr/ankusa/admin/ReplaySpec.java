package io.github.jamescarr.ankusa.admin;

import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * The body of {@code POST /v1/replays}: what to replay and how fast.
 *
 * <p>A null component is left out of the request body rather than sent as JSON null, so the body
 * carries exactly what the caller set and the listener applies its own defaults. {@code kind}
 * selects the filter's shape:
 *
 * <ul>
 *   <li>{@code "dlq"}: optional {@code sourceId}, {@code id}, and inclusive {@code since}/{@code
 *       until} bounds on the dead-letter time, in Unix milliseconds. The job only touches rows
 *       dead-lettered at or before its own creation time, so rows that die again are not picked up
 *       a second time.
 *   <li>{@code "archive"}: required inclusive {@code from}/{@code to} bounds on {@code receivedAt},
 *       in Unix milliseconds, plus optional {@code sourceId} and {@code sinks} (indexes into the
 *       source's current sink list; omitted means every current sink).
 *   <li>{@code "quarantine"}: optional {@code sourceId}, {@code id}, and inclusive {@code
 *       since}/{@code until} bounds on {@code receivedAt}, in Unix milliseconds. The job
 *       re-verifies held hooks against each source's current verifier and releases the ones that
 *       pass; only hooks quarantined at or before its own creation time are touched.
 * </ul>
 *
 * <p>{@code rate} is items per second (1..100000, default 1000); {@code maxLagMs} is the dispatcher
 * lag above which the job pauses itself (100..600000, default 2000).
 *
 * @param kind {@code "dlq"}, {@code "archive"}, or {@code "quarantine"}, or null to let the
 *     listener default it
 * @param sourceId replay only this source's rows, or null for every source
 * @param id replay only the dead-letter row ({@code dlq}) or held hook ({@code quarantine}) with
 *     this id, or null for any
 * @param since the earliest dead-letter time ({@code dlq}) or {@code receivedAt} ({@code
 *     quarantine}) to replay, inclusive, or null for any
 * @param until the latest dead-letter time ({@code dlq}) or {@code receivedAt} ({@code quarantine})
 *     to replay, inclusive, or null for any
 * @param from the earliest {@code receivedAt} to replay, inclusive, or null
 * @param to the latest {@code receivedAt} to replay, inclusive, or null
 * @param sinks the sink indexes to redrive archive hooks to, or null for every current sink
 * @param rate the target items per second, or null for the listener's default
 * @param maxLagMs the dispatcher lag in milliseconds above which to pause, or null for the default
 */
public record ReplaySpec(
    @Nullable String kind,
    @Nullable String sourceId,
    @Nullable String id,
    @Nullable Long since,
    @Nullable Long until,
    @Nullable Long from,
    @Nullable Long to,
    @Nullable List<Integer> sinks,
    @Nullable Integer rate,
    @Nullable Integer maxLagMs) {

  /**
   * Starts building a spec.
   *
   * @return an empty builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Collects the optional fields of a {@link ReplaySpec}. */
  public static final class Builder {

    private @Nullable String kind;
    private @Nullable String sourceId;
    private @Nullable String id;
    private @Nullable Long since;
    private @Nullable Long until;
    private @Nullable Long from;
    private @Nullable Long to;
    private @Nullable List<Integer> sinks;
    private @Nullable Integer rate;
    private @Nullable Integer maxLagMs;

    private Builder() {}

    /**
     * Sets the kind.
     *
     * @param kind {@code "dlq"}, {@code "archive"}, or {@code "quarantine"}, or null for the
     *     listener's default
     * @return this builder
     */
    public Builder kind(@Nullable String kind) {
      this.kind = kind;
      return this;
    }

    /**
     * Replays only one source's rows.
     *
     * @param sourceId the source id, or null for every source
     * @return this builder
     */
    public Builder sourceId(@Nullable String sourceId) {
      this.sourceId = sourceId;
      return this;
    }

    /**
     * Replays only one dead-letter row.
     *
     * @param id the row id, or null for any row
     * @return this builder
     */
    public Builder id(@Nullable String id) {
      this.id = id;
      return this;
    }

    /**
     * Sets the earliest dead-letter time to replay.
     *
     * @param since the Unix-millisecond time, inclusive, or null for any
     * @return this builder
     */
    public Builder since(@Nullable Long since) {
      this.since = since;
      return this;
    }

    /**
     * Sets the latest dead-letter time to replay.
     *
     * @param until the Unix-millisecond time, inclusive, or null for any
     * @return this builder
     */
    public Builder until(@Nullable Long until) {
      this.until = until;
      return this;
    }

    /**
     * Sets the earliest {@code receivedAt} to replay from the archive.
     *
     * @param from the Unix-millisecond time, inclusive, or null
     * @return this builder
     */
    public Builder from(@Nullable Long from) {
      this.from = from;
      return this;
    }

    /**
     * Sets the latest {@code receivedAt} to replay from the archive.
     *
     * @param to the Unix-millisecond time, inclusive, or null
     * @return this builder
     */
    public Builder to(@Nullable Long to) {
      this.to = to;
      return this;
    }

    /**
     * Sets the sink indexes to redrive archive hooks to.
     *
     * @param sinks the sink indexes, or null for every current sink
     * @return this builder
     */
    public Builder sinks(@Nullable List<Integer> sinks) {
      this.sinks = sinks;
      return this;
    }

    /**
     * Sets the target items per second.
     *
     * @param rate the rate, or null for the listener's default
     * @return this builder
     */
    public Builder rate(@Nullable Integer rate) {
      this.rate = rate;
      return this;
    }

    /**
     * Sets the dispatcher lag above which the job pauses itself.
     *
     * @param maxLagMs the lag in milliseconds, or null for the listener's default
     * @return this builder
     */
    public Builder maxLagMs(@Nullable Integer maxLagMs) {
      this.maxLagMs = maxLagMs;
      return this;
    }

    /**
     * Builds the spec.
     *
     * @return the spec
     */
    public ReplaySpec build() {
      return new ReplaySpec(kind, sourceId, id, since, until, from, to, sinks, rate, maxLagMs);
    }
  }
}
