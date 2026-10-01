package io.github.jamescarr.ankusa.routes;

import org.jspecify.annotations.Nullable;

/**
 * Filters for listing routes.
 *
 * <p>A null component means "absent": its query parameter is left out entirely.
 *
 * @param enabled list only routes that are, or are not, served
 * @param limit the most routes to return
 * @param cursor the cursor a previous page returned
 */
public record ListRoutesParams(
    @Nullable Boolean enabled, @Nullable Integer limit, @Nullable String cursor) {

  /**
   * Starts building filters.
   *
   * @return an empty builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Collects the filters of a {@link ListRoutesParams}. */
  public static final class Builder {

    private @Nullable Boolean enabled;
    private @Nullable Integer limit;
    private @Nullable String cursor;

    private Builder() {}

    /**
     * Lists only routes that are, or are not, served.
     *
     * @param enabled whether to keep served routes
     * @return this builder
     */
    public Builder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    /**
     * Sets the most routes to return.
     *
     * @param limit the page size
     * @return this builder
     */
    public Builder limit(int limit) {
      this.limit = limit;
      return this;
    }

    /**
     * Sets the cursor a previous page returned.
     *
     * @param cursor the cursor
     * @return this builder
     */
    public Builder cursor(String cursor) {
      this.cursor = cursor;
      return this;
    }

    /**
     * Builds the filters.
     *
     * @return the filters
     */
    public ListRoutesParams build() {
      return new ListRoutesParams(enabled, limit, cursor);
    }
  }
}
