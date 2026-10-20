package io.github.jamescarr.ankusa.admin;

import org.jspecify.annotations.Nullable;

/**
 * Which rows {@link AdminClient#listAudit(ListAuditParams)} lists.
 *
 * <p>Every component is optional, and a null one is left out of the query string rather than sent
 * without a value. {@link AdminClient#listAudit(ListAuditParams)} sends whichever are set in this
 * order: {@code since}, {@code until}, {@code actor}, {@code tenant}, {@code action}, {@code
 * limit}, {@code cursor}.
 *
 * @param since list only rows committed at or after this Unix-millisecond time, or null for any
 * @param until list only rows committed at or before this Unix-millisecond time, or null for any
 * @param actor list only this principal's rows, or null for every principal
 * @param tenant list only this tenant's rows, or null for every tenant the caller can see; naming
 *     one the caller cannot see is a {@code 403}
 * @param action list only this action, or null for every action
 * @param limit return at most this many rows, or null for the listener's default
 * @param cursor resume from the previous page's {@code nextCursor}, or null for the first page
 */
public record ListAuditParams(
    @Nullable Long since,
    @Nullable Long until,
    @Nullable String actor,
    @Nullable String tenant,
    @Nullable String action,
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

  /** Collects the optional fields of a {@link ListAuditParams}. */
  public static final class Builder {

    private @Nullable Long since;
    private @Nullable Long until;
    private @Nullable String actor;
    private @Nullable String tenant;
    private @Nullable String action;
    private @Nullable Integer limit;
    private @Nullable String cursor;

    private Builder() {}

    /**
     * Lists only rows committed at or after a time.
     *
     * @param since the Unix-millisecond time, or null for any time
     * @return this builder
     */
    public Builder since(@Nullable Long since) {
      this.since = since;
      return this;
    }

    /**
     * Lists only rows committed at or before a time.
     *
     * @param until the Unix-millisecond time, or null for any time
     * @return this builder
     */
    public Builder until(@Nullable Long until) {
      this.until = until;
      return this;
    }

    /**
     * Lists only one principal's rows.
     *
     * @param actor the principal id, or null for every principal
     * @return this builder
     */
    public Builder actor(@Nullable String actor) {
      this.actor = actor;
      return this;
    }

    /**
     * Lists only one tenant's rows.
     *
     * @param tenant the tenant id, or null for every tenant the caller can see
     * @return this builder
     */
    public Builder tenant(@Nullable String tenant) {
      this.tenant = tenant;
      return this;
    }

    /**
     * Lists only one action.
     *
     * @param action the action, or null for every action
     * @return this builder
     */
    public Builder action(@Nullable String action) {
      this.action = action;
      return this;
    }

    /**
     * Caps how many rows come back.
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
    public ListAuditParams build() {
      return new ListAuditParams(since, until, actor, tenant, action, limit, cursor);
    }
  }
}
