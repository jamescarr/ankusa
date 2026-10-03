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
 * @param dedupeKey the provider's event key, from {@code x-ankusa-dedupe-key}, or null when the
 *     header is absent or empty
 * @param replayId the replay job id, from {@code x-ankusa-replay-id}, or null when the header is
 *     absent or empty
 */
public record HookHeaders(
    String id,
    String source,
    @Nullable String tenant,
    @Nullable String contentType,
    @Nullable String dedupeKey,
    @Nullable String replayId) {

  /**
   * Reads the headers through a lookup function.
   *
   * <p>The function is asked for lower-case names — {@code x-ankusa-id}, {@code x-ankusa-source},
   * {@code x-ankusa-tenant}, {@code content-type}, {@code x-ankusa-dedupe-key}, {@code
   * x-ankusa-replay-id} — and must compare header names case-insensitively, as {@code
   * HttpServletRequest::getHeader} does.
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
        lookup.apply("content-type"),
        nonEmpty(lookup.apply("x-ankusa-dedupe-key")),
        nonEmpty(lookup.apply("x-ankusa-replay-id")));
  }

  /**
   * The key a worker dedupes on.
   *
   * <p>When {@link #dedupeKey()} is non-null and non-empty the key is {@code source:dedupe_key},
   * which collapses provider retries of one event; otherwise it is the delivery {@link #id()}. With
   * {@code includeReplay} and a {@link #replayId()}, the key gains a {@code #replay:<replay_id>}
   * suffix so a replay is processed again; omit it (false) to drop replays.
   *
   * @param includeReplay whether a replay should get a distinct key
   * @return the idempotency key
   */
  public String idempotencyKey(boolean includeReplay) {
    String key = dedupeKey != null && !dedupeKey.isEmpty() ? source + ":" + dedupeKey : id;
    if (includeReplay && replayId != null) {
      key = key + "#replay:" + replayId;
    }
    return key;
  }

  /** Maps an absent or empty header value to null, per the contract. */
  private static @Nullable String nonEmpty(@Nullable String value) {
    return value == null || value.isEmpty() ? null : value;
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
