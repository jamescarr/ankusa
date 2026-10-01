package io.github.jamescarr.ankusa.admin;

import org.jspecify.annotations.Nullable;

/**
 * Which dead letters {@link AdminClient#listDeadLetters(ListDeadLettersParams)} lists.
 *
 * <p>Every component is optional, and a null one is left out of the query string rather than sent
 * without a value.
 *
 * @param sourceId list only this source's entries, or null for every source
 * @param since list only entries received at or after this Unix-millisecond time, or null for any
 * @param limit return at most this many entries, or null for the listener's default
 */
public record ListDeadLettersParams(
    @Nullable String sourceId, @Nullable Long since, @Nullable Integer limit) {

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
    private @Nullable Long since;
    private @Nullable Integer limit;

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
     * Builds the parameters.
     *
     * @return the parameters
     */
    public ListDeadLettersParams build() {
      return new ListDeadLettersParams(sourceId, since, limit);
    }
  }
}
