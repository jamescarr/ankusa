package io.github.jamescarr.ankusa.admin;

import org.jspecify.annotations.Nullable;

/**
 * Which quarantined hooks {@link AdminClient#listQuarantined(ListQuarantinedParams)} lists.
 *
 * @param limit return at most this many entries, or null for the listener's default
 */
public record ListQuarantinedParams(@Nullable Integer limit) {

  /**
   * Starts building parameters.
   *
   * @return an empty builder, which lists the listener's default page
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Collects the optional field of a {@link ListQuarantinedParams}. */
  public static final class Builder {

    private @Nullable Integer limit;

    private Builder() {}

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
    public ListQuarantinedParams build() {
      return new ListQuarantinedParams(limit);
    }
  }
}
