package io.github.jamescarr.ankusa.admin;

import org.jspecify.annotations.Nullable;

/**
 * Which quarantined hooks {@link AdminClient#purgeQuarantined(PurgeQuarantinedParams)} deletes.
 *
 * <p>Every filter is optional and the filters are ANDed; a null one is left out of the query string
 * rather than sent without a value. {@code since} and {@code until} are inclusive bounds on {@code
 * receivedAt}, in Unix milliseconds. The listener clamps {@code limit} to 1..10000 and defaults it
 * to 1000, so with no filter at all the call deletes the oldest {@code limit} entries.
 *
 * <p>Deleted hooks are gone for good. Run a {@code quarantine} replay job first to keep the ones
 * that now verify.
 *
 * <p>Nothing is validated here: a negative bound, or an {@code until} before {@code since}, comes
 * back from the listener as {@code 400 invalid_filter}.
 *
 * @param sourceId delete only this source's entries, or null for every source
 * @param id delete only the entry with this id, or null for any
 * @param since delete only entries received at or after this Unix-millisecond time, or null for any
 * @param until delete only entries received at or before this Unix-millisecond time, or null for
 *     any
 * @param limit delete at most this many entries, or null for the listener's default
 */
public record PurgeQuarantinedParams(
    @Nullable String sourceId,
    @Nullable String id,
    @Nullable Long since,
    @Nullable Long until,
    @Nullable Integer limit) {

  /**
   * Starts building parameters.
   *
   * @return an empty builder, which purges the listener's default batch of the oldest entries
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Collects the optional fields of a {@link PurgeQuarantinedParams}. */
  public static final class Builder {

    private @Nullable String sourceId;
    private @Nullable String id;
    private @Nullable Long since;
    private @Nullable Long until;
    private @Nullable Integer limit;

    private Builder() {}

    /**
     * Purges only one source's entries.
     *
     * @param sourceId the source id, or null for every source
     * @return this builder
     */
    public Builder sourceId(@Nullable String sourceId) {
      this.sourceId = sourceId;
      return this;
    }

    /**
     * Purges only one entry.
     *
     * @param id the quarantine entry's id, or null for any entry
     * @return this builder
     */
    public Builder id(@Nullable String id) {
      this.id = id;
      return this;
    }

    /**
     * Purges only entries received at or after a time.
     *
     * @param since the Unix-millisecond time, or null for any time
     * @return this builder
     */
    public Builder since(@Nullable Long since) {
      this.since = since;
      return this;
    }

    /**
     * Purges only entries received at or before a time.
     *
     * @param until the Unix-millisecond time, or null for any time
     * @return this builder
     */
    public Builder until(@Nullable Long until) {
      this.until = until;
      return this;
    }

    /**
     * Caps how many entries are deleted.
     *
     * @param limit the most entries to delete, or null for the listener's default
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
    public PurgeQuarantinedParams build() {
      return new PurgeQuarantinedParams(sourceId, id, since, until, limit);
    }
  }
}
