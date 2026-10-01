package io.github.jamescarr.ankusa;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * One request handed to a {@link Transport}.
 *
 * @param method the HTTP method, upper case
 * @param uri the absolute request URI, query string included
 * @param headers the request headers; this record keeps an unmodifiable copy
 * @param body the request body bytes, or null when the request has no body
 * @param timeout how long the whole exchange may take
 */
public record TransportRequest(
    String method, URI uri, Map<String, String> headers, byte @Nullable [] body, Duration timeout) {

  /** Copies the headers so a later change to the caller's map cannot reach the wire. */
  public TransportRequest {
    headers = Collections.unmodifiableMap(new LinkedHashMap<>(headers));
  }

  /**
   * Describes this request without ever printing a header value.
   *
   * <p>Header values carry bearer tokens and API keys, and a request ends up in logs and stack
   * traces, so only the names are shown.
   *
   * @return a one-line description naming the method, URI, timeout and header names
   */
  @Override
  public String toString() {
    return "TransportRequest[method="
        + method
        + ", uri="
        + uri
        + ", headers="
        + headers.keySet()
        + ", timeout="
        + timeout
        + "]";
  }
}
