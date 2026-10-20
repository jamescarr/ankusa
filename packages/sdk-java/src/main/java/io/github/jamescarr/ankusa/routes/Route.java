package io.github.jamescarr.ankusa.routes;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * A route the deployment serves: the path, the methods it accepts, and the rules that guard it.
 *
 * @param id the route's id
 * @param path the path the route matches
 * @param methods the HTTP methods the route accepts
 * @param enabled whether the route is served
 * @param ipRules the rules that decide which addresses may reach it
 * @param metadata free-form caller metadata, returned verbatim
 * @param revision counts the definition's changes, from 1 at creation; re-creating a deleted route
 *     starts again at 1; null from a node older than the field
 * @param insertedAt when the route was created
 * @param updatedAt when the route was last changed
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Route(
    String id,
    String path,
    List<String> methods,
    boolean enabled,
    List<IpRule> ipRules,
    Map<String, @Nullable Object> metadata,
    @Nullable Integer revision,
    Instant insertedAt,
    Instant updatedAt) {

  /**
   * Validates and copies the components.
   *
   * <p>A null list or map from the wire becomes an empty one.
   *
   * @throws NullPointerException when a required component is null
   */
  public Route {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(path, "path");
    methods = methods == null ? List.of() : List.copyOf(methods);
    ipRules = ipRules == null ? List.of() : List.copyOf(ipRules);
    metadata =
        metadata == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
    Objects.requireNonNull(insertedAt, "insertedAt");
    Objects.requireNonNull(updatedAt, "updatedAt");
  }
}
