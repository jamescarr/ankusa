package io.github.jamescarr.ankusa.admin;

import org.jspecify.annotations.Nullable;

/**
 * Which dead letters {@link AdminClient#replayDeadLetters(ReplayFilter)} replays.
 *
 * <p>Every component is optional: a filter with all three null replays the whole dead-letter queue.
 * Absent fields are omitted from the request rather than sent as JSON null.
 *
 * @param sourceId replay only this source's entries, or null for every source
 * @param id replay only the entry with this id, or null for any entry
 * @param since replay only entries received at or after this Unix-millisecond time, or null for any
 */
public record ReplayFilter(@Nullable String sourceId, @Nullable String id, @Nullable Long since) {

  /**
   * Starts building a filter.
   *
   * @return an empty builder, which replays everything
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Collects the optional fields of a {@link ReplayFilter}. */
  public static final class Builder {

    private @Nullable String sourceId;
    private @Nullable String id;
    private @Nullable Long since;

    private Builder() {}

    /**
     * Replays only one source's entries.
     *
     * @param sourceId the source id, or null for every source
     * @return this builder
     */
    public Builder sourceId(@Nullable String sourceId) {
      this.sourceId = sourceId;
      return this;
    }

    /**
     * Replays only one entry.
     *
     * @param id the dead-letter entry id, or null for any entry
     * @return this builder
     */
    public Builder id(@Nullable String id) {
      this.id = id;
      return this;
    }

    /**
     * Replays only entries received at or after a time.
     *
     * @param since the Unix-millisecond time, or null for any time
     * @return this builder
     */
    public Builder since(@Nullable Long since) {
      this.since = since;
      return this;
    }

    /**
     * Builds the filter.
     *
     * @return the filter
     */
    public ReplayFilter build() {
      return new ReplayFilter(sourceId, id, since);
    }
  }
}
