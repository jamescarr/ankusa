package io.github.jamescarr.ankusa.webhook;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/**
 * The Ankusa headers on an HTTP delivery.
 *
 * @param id the delivery id, from {@code x-ankusa-id}; never empty
 * @param source the source that accepted the hook, from {@code x-ankusa-source}; empty when absent
 * @param tenant the tenant the source belongs to, from {@code x-ankusa-tenant}, or null when the
 *     hook is not tenant-scoped
 * @param contentType the media type Ankusa received, from {@code content-type}, or null when absent
 */
public record HookHeaders(
    String id, String source, @Nullable String tenant, @Nullable String contentType) {

  /**
   * Reads the headers through a lookup function.
   *
   * <p>The function is asked for lower-case names — {@code x-ankusa-id}, {@code x-ankusa-source},
   * {@code x-ankusa-tenant}, {@code content-type} — and must compare header names
   * case-insensitively, as {@code HttpServletRequest::getHeader} does.
   *
   * @param lookup returns a header's value, or null when it is absent
   * @return the delivery's headers
   * @throws MissingHookIdError when {@code x-ankusa-id} is absent or empty
   */
  public static HookHeaders parse(Function<String, @Nullable String> lookup) {
    String id = lookup.apply("x-ankusa-id");
    if (id == null || id.isEmpty()) {
      throw new MissingHookIdError();
    }

    String source = lookup.apply("x-ankusa-source");

    return new HookHeaders(
        id,
        source == null ? "" : source,
        lookup.apply("x-ankusa-tenant"),
        lookup.apply("content-type"));
  }

  /**
   * Reads the headers out of a header map.
   *
   * <p>Each name is matched against the map's keys ignoring case, and the first non-empty value
   * wins, so a servlet that lower-cases its names and one that does not both work.
   *
   * @param headers the request's headers, in any case, each mapping to its values
   * @return the delivery's headers
   * @throws MissingHookIdError when {@code x-ankusa-id} is absent or empty
   */
  public static HookHeaders parse(Map<String, ? extends List<String>> headers) {
    return parse(
        name -> {
          for (Map.Entry<String, ? extends List<String>> entry : headers.entrySet()) {
            String key = entry.getKey();
            List<String> values = entry.getValue();
            if (key != null && key.equalsIgnoreCase(name) && values != null && !values.isEmpty()) {
              return values.get(0);
            }
          }
          return null;
        });
  }
}
