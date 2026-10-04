package io.github.jamescarr.ankusa.internal;

import org.jspecify.annotations.Nullable;

/**
 * Internal: not part of the API; may change in any release.
 *
 * <p>The one idempotency-key rule, shared by {@code Message} and {@code HookHeaders} so it is
 * written down once.
 *
 * <p>The key Ankusa shipped (the message's {@code idempotency_key}, or the {@code
 * x-ankusa-idempotency-key} header) when it is a non-empty string. For a hook from a node that
 * predates the field it is computed: {@code tenant:source_id:dedupe_key} (tenant {@code default}
 * when there is none) for a non-empty dedupe key, else the id. With {@code includeReplay} and a
 * replay id, {@code #replay:<replay_id>} is appended.
 */
public final class IdempotencyKey {

  private IdempotencyKey() {}

  /**
   * Resolves the key.
   *
   * @param shipped the key Ankusa shipped, or null
   * @param tenant the tenant, or null when the hook is not tenant-scoped
   * @param source the source id
   * @param dedupeKey the provider's event key, or null
   * @param id the delivery id
   * @param replayId the replay job id, or null
   * @param includeReplay whether a replay should get a distinct key
   * @return the idempotency key
   */
  public static String resolve(
      @Nullable String shipped,
      @Nullable String tenant,
      String source,
      @Nullable String dedupeKey,
      String id,
      @Nullable String replayId,
      boolean includeReplay) {
    String key;
    if (shipped != null && !shipped.isEmpty()) {
      key = shipped;
    } else if (dedupeKey != null && !dedupeKey.isEmpty()) {
      key = (tenant == null ? "default" : tenant) + ":" + source + ":" + dedupeKey;
    } else {
      key = id;
    }
    if (includeReplay && replayId != null) {
      key = key + "#replay:" + replayId;
    }
    return key;
  }
}
