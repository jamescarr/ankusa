package io.github.jamescarr.ankusa.sources;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * The writable fields of a source, as submitted to a create or an update.
 *
 * <p>Only {@code sinks} is required. An unset {@code verify} is left out of the request body, so
 * the server applies its own {@code {"type": "none"}} default; an unset failure mode is left out
 * too. A set field is sent as given, so an empty map is sent as {@code {}}.
 *
 * @param sinks the source's sinks; required, copied into an unmodifiable list
 * @param verify the verification block, or null to leave it to the server
 * @param onVerifyFailure what to do when verification fails, or null to leave it to the server
 */
public record SourceSpec(
    List<Map<String, @Nullable Object>> sinks,
    @Nullable Map<String, @Nullable Object> verify,
    @Nullable String onVerifyFailure) {

  /**
   * Rejects a missing sink list and copies it.
   *
   * @throws NullPointerException when {@code sinks} is null
   */
  public SourceSpec {
    sinks = List.copyOf(Objects.requireNonNull(sinks, "sinks"));
  }

  /**
   * Builds the body of a create or update request, in wire order.
   *
   * @return the body holding {@code sinks}, then {@code verify} and {@code on_verify_failure} when
   *     they are set
   */
  LinkedHashMap<String, Object> toBody() {
    LinkedHashMap<String, Object> body = new LinkedHashMap<>();
    body.put("sinks", sinks);
    if (verify != null) {
      body.put("verify", verify);
    }
    if (onVerifyFailure != null) {
      body.put("on_verify_failure", onVerifyFailure);
    }
    return body;
  }
}
