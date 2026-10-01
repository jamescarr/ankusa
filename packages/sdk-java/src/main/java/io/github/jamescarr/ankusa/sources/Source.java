package io.github.jamescarr.ankusa.sources;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * A stored source as the admin API reports it: the redacted spec plus the derived identity fields.
 *
 * <p>The secrets of a spec are never returned, so a {@code Source} read back is not something to
 * resend for an edit; build a {@link SourceSpec} instead.
 *
 * @param tenant the tenant that owns the source
 * @param name the source's name within the tenant
 * @param sourceId the source's full id, {@code <tenant>.<name>}
 * @param ingestPath the path hook deliveries for this source arrive on
 * @param verify the stored verification block, never null or empty
 * @param onVerifyFailure the stored failure mode, or null when the server stores none
 * @param sinks the stored sinks, never null
 */
public record Source(
    String tenant,
    String name,
    String sourceId,
    String ingestPath,
    Map<String, @Nullable Object> verify,
    @Nullable String onVerifyFailure,
    List<Map<String, @Nullable Object>> sinks) {

  /** The verification block a source without one is reported with. */
  private static final Map<String, @Nullable Object> NO_VERIFY = Map.of("type", "none");

  /**
   * Normalizes a decoded body: a missing verification block becomes {@code {"type": "none"}}, and a
   * missing sink list becomes empty.
   *
   * @throws NullPointerException when a required identity field is absent
   */
  public Source {
    Objects.requireNonNull(tenant, "tenant");
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(sourceId, "source_id");
    Objects.requireNonNull(ingestPath, "ingest_path");
    if (verify == null || verify.isEmpty()) {
      verify = NO_VERIFY;
    } else {
      verify = Collections.unmodifiableMap(new LinkedHashMap<>(verify));
    }
    sinks = sinks == null ? List.of() : List.copyOf(sinks);
  }
}
