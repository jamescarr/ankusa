package io.github.jamescarr.ankusa;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * One response a {@link Transport} got back, body included.
 *
 * @param status the HTTP status code
 * @param headers the response headers, which may repeat; this record keeps an unmodifiable copy
 * @param body the whole response body, never null, empty when the response had none
 */
public record TransportResponse(int status, Map<String, List<String>> headers, byte[] body) {

  /** Copies the headers so a later change to the caller's map cannot reach the caller's view. */
  public TransportResponse {
    headers = Collections.unmodifiableMap(new LinkedHashMap<>(headers));
  }

  /**
   * Returns the first value of a header.
   *
   * @param name the header name, compared case-insensitively
   * @return the first value, or null when the header is absent or carries no value
   */
  public @Nullable String header(String name) {
    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(name)) {
        List<String> values = entry.getValue();
        return values.isEmpty() ? null : values.get(0);
      }
    }
    return null;
  }
}
