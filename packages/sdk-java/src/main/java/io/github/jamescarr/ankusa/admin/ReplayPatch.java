package io.github.jamescarr.ankusa.admin;

import org.jspecify.annotations.Nullable;

/**
 * The body of {@code PATCH /v1/replays/{id}}: change a replay job's state or pace.
 *
 * <p>A null component is left out of the request body rather than sent as JSON null, so the body
 * carries exactly what the caller set. Setting {@code state} to {@code "running"} resumes a paused
 * job, clears its error, and resets its auto-pause window; {@code "paused"} pauses it; {@code
 * "cancelled"} stops it for good.
 *
 * @param state {@code "running"}, {@code "paused"}, or {@code "cancelled"}, or null to leave it
 * @param rate the new target items per second, or null to leave it
 * @param maxLagMs the new dispatcher-lag threshold, or null to leave it
 */
public record ReplayPatch(
    @Nullable String state, @Nullable Integer rate, @Nullable Integer maxLagMs) {

  /**
   * Starts building a patch.
   *
   * @return an empty builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Collects the optional fields of a {@link ReplayPatch}. */
  public static final class Builder {

    private @Nullable String state;
    private @Nullable Integer rate;
    private @Nullable Integer maxLagMs;

    private Builder() {}

    /**
     * Sets the job's state.
     *
     * @param state {@code "running"}, {@code "paused"}, or {@code "cancelled"}, or null to leave it
     * @return this builder
     */
    public Builder state(@Nullable String state) {
      this.state = state;
      return this;
    }

    /**
     * Sets the target items per second.
     *
     * @param rate the rate, or null to leave it
     * @return this builder
     */
    public Builder rate(@Nullable Integer rate) {
      this.rate = rate;
      return this;
    }

    /**
     * Sets the dispatcher-lag threshold.
     *
     * @param maxLagMs the lag in milliseconds, or null to leave it
     * @return this builder
     */
    public Builder maxLagMs(@Nullable Integer maxLagMs) {
      this.maxLagMs = maxLagMs;
      return this;
    }

    /**
     * Builds the patch.
     *
     * @return the patch
     */
    public ReplayPatch build() {
      return new ReplayPatch(state, rate, maxLagMs);
    }
  }
}
