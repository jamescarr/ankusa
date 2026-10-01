package io.github.jamescarr.ankusa.routes;

import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * A partial update to a route: only the fields the caller sets are sent.
 *
 * <p>A null component means "absent": it is left out of the request body rather than sent as JSON
 * null, so the body carries exactly what the caller set.
 *
 * @param enabled whether the route is served, or null to leave it unchanged
 * @param methods the HTTP methods the route accepts, or null to leave them unchanged
 * @param ipRules the route's IP rules, or null to leave them unchanged
 * @param metadata free-form metadata, or null to leave it unchanged
 */
public record RoutePatch(
    @Nullable Boolean enabled,
    @Nullable List<String> methods,
    @Nullable List<IpRule> ipRules,
    @Nullable Map<String, @Nullable Object> metadata) {

  /**
   * Starts building a patch.
   *
   * @return an empty builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Collects the fields of a {@link RoutePatch}. */
  public static final class Builder {

    private @Nullable Boolean enabled;
    private @Nullable List<String> methods;
    private @Nullable List<IpRule> ipRules;
    private @Nullable Map<String, @Nullable Object> metadata;

    private Builder() {}

    /**
     * Sets whether the route is served.
     *
     * @param enabled whether the route is served
     * @return this builder
     */
    public Builder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    /**
     * Sets the methods the route accepts.
     *
     * @param methods the accepted methods, or null to leave them unchanged
     * @return this builder
     */
    public Builder methods(@Nullable List<String> methods) {
      this.methods = methods;
      return this;
    }

    /**
     * Sets the route's IP rules.
     *
     * @param ipRules the rules, or null to leave them unchanged
     * @return this builder
     */
    public Builder ipRules(@Nullable List<IpRule> ipRules) {
      this.ipRules = ipRules;
      return this;
    }

    /**
     * Sets free-form metadata.
     *
     * @param metadata the metadata, or null to leave it unchanged
     * @return this builder
     */
    public Builder metadata(@Nullable Map<String, Object> metadata) {
      this.metadata = metadata;
      return this;
    }

    /**
     * Builds the patch.
     *
     * @return the patch
     */
    public RoutePatch build() {
      return new RoutePatch(enabled, methods, ipRules, metadata);
    }
  }
}
