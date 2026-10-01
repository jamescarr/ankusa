package io.github.jamescarr.ankusa.routes;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * The fields a route is created or replaced with.
 *
 * <p>A null component means "absent": it is left out of the request body rather than sent as JSON
 * null, so the body carries exactly what the caller set. An empty metadata map is sent as {@code
 * {}}.
 *
 * @param id the route's id, or null to let the listener assign one
 * @param path the path the route matches
 * @param methods the HTTP methods the route accepts, or null to leave them to the listener
 * @param enabled whether the route is served, or null for the listener's default
 * @param ipRules the route's IP rules, or null
 * @param metadata free-form metadata, or null
 */
public record RouteInput(
    @Nullable String id,
    String path,
    @Nullable List<String> methods,
    @Nullable Boolean enabled,
    @Nullable List<IpRule> ipRules,
    @Nullable Map<String, @Nullable Object> metadata) {

  /**
   * Validates the components.
   *
   * @throws NullPointerException when {@code path} is null
   */
  public RouteInput {
    Objects.requireNonNull(path, "path");
  }

  /**
   * Starts building an input for a path.
   *
   * @param path the path the route matches
   * @return a builder already holding {@code path}
   */
  public static Builder builder(String path) {
    return new Builder(path);
  }

  /** Collects the fields of a {@link RouteInput}. */
  public static final class Builder {

    private final String path;
    private @Nullable String id;
    private @Nullable List<String> methods;
    private @Nullable Boolean enabled;
    private @Nullable List<IpRule> ipRules;
    private @Nullable Map<String, @Nullable Object> metadata;

    private Builder(String path) {
      this.path = Objects.requireNonNull(path, "path");
    }

    /**
     * Sets the route's id.
     *
     * @param id the id to request, or null to let the listener assign one
     * @return this builder
     */
    public Builder id(@Nullable String id) {
      this.id = id;
      return this;
    }

    /**
     * Sets the methods the route accepts.
     *
     * @param methods the accepted methods, or null to leave them to the listener
     * @return this builder
     */
    public Builder methods(@Nullable List<String> methods) {
      this.methods = methods;
      return this;
    }

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
     * Sets the route's IP rules.
     *
     * @param ipRules the rules, or null
     * @return this builder
     */
    public Builder ipRules(@Nullable List<IpRule> ipRules) {
      this.ipRules = ipRules;
      return this;
    }

    /**
     * Sets free-form metadata returned verbatim.
     *
     * @param metadata the metadata, or null
     * @return this builder
     */
    public Builder metadata(@Nullable Map<String, Object> metadata) {
      this.metadata = metadata;
      return this;
    }

    /**
     * Builds the input.
     *
     * @return the input
     */
    public RouteInput build() {
      return new RouteInput(id, path, methods, enabled, ipRules, metadata);
    }
  }
}
