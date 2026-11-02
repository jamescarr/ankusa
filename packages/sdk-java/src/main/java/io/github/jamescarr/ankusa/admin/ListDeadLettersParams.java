package io.github.jamescarr.ankusa.admin;

import org.jspecify.annotations.Nullable;

/**
 * Which dead letters {@link AdminClient#listDeadLetters(ListDeadLettersParams)} lists.
 *
 * <p>Every component is optional, and a null one is left out of the query string rather than sent
 * without a value. {@link AdminClient#listDeadLetters(ListDeadLettersParams)} sends whichever are
 * set in this order: {@code source_id}, {@code tenant}, {@code sink}, {@code error_class}, {@code
 * since}, {@code limit}, {@code cursor}.
 *
 * @param sourceId list only this source's entries, or null for every source
 * @param tenant list only this tenant's entries, or null for every tenant the caller can see
 * @param sink list only the rows due to this sink, by its index in the source's {@code sinks}, or
 *     null for every sink
 * @param errorClass list only rows of this error class ({@code "permanent"}, {@code "throttled"} or
 *     {@code "transient"}), or null for every class
 * @param since list only entries received at or after this Unix-millisecond time, or null for any
 * @param limit return at most this many entries, or null for the listener's default
 * @param cursor resume from the previous page's {@code nextCursor}, or null for the first page
 */
public record ListDeadLettersParams(
    @Nullable String sourceId,
    @Nullable String tenant,
    @Nullable Integer sink,
    @Nullable String errorClass,
    @Nullable Long since,
    @Nullable Integer limit,
    @Nullable String cursor) {

  /**
   * Starts building parameters.
   *
   * @return an empty builder, which lists the listener's default page
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Collects the optional fields of a {@link ListDeadLettersParams}. */
  public static final class Builder {

    private @Nullable String sourceId;
    private @Nullable String tenant;
    private @Nullable Integer sink;
    private @Nullable String errorClass;
    private @Nullable Long since;
    private @Nullable Integer limit;
    private @Nullable String cursor;

    private Builder() {}

    /**
     * Lists only one source's entries.
     *
     * @param sourceId the source id, or null for every source
     * @return this builder
     */
    public Builder sourceId(@Nullable String sourceId) {
      this.sourceId = sourceId;
      return this;
    }

    /**
     * Lists only one tenant's entries.
     *
     * @param tenant the tenant id, or null for every tenant the caller can see
     * @return this builder
     */
    public Builder tenant(@Nullable String tenant) {
      this.tenant = tenant;
      return this;
    }

    /**
     * Lists only the rows due to one sink.
     *
     * @param sink the sink's index in the source's {@code sinks}, or null for every sink
     * @return this builder
     */
    public Builder sink(@Nullable Integer sink) {
      this.sink = sink;
      return this;
    }

    /**
     * Lists only one error class.
     *
     * @param errorClass {@code "permanent"}, {@code "throttled"} or {@code "transient"}, or null
     *     for every class
     * @return this builder
     */
    public Builder errorClass(@Nullable String errorClass) {
      this.errorClass = errorClass;
      return this;
    }

    /**
     * Lists only entries received at or after a time.
     *
     * @param since the Unix-millisecond time, or null for any time
     * @return this builder
     */
    public Builder since(@Nullable Long since) {
      this.since = since;
      return this;
    }

    /**
     * Caps how many entries come back.
     *
     * @param limit the page size, or null for the listener's default
     * @return this builder
     */
    public Builder limit(@Nullable Integer limit) {
      this.limit = limit;
      return this;
    }

    /**
     * Resumes from a previous page.
     *
     * @param cursor the previous page's {@code nextCursor}, or null for the first page
     * @return this builder
     */
    public Builder cursor(@Nullable String cursor) {
      this.cursor = cursor;
      return this;
    }

    /**
     * Builds the parameters.
     *
     * @return the parameters
     */
    public ListDeadLettersParams build() {
      return new ListDeadLettersParams(sourceId, tenant, sink, errorClass, since, limit, cursor);
    }
  }
}
