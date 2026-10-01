package io.github.jamescarr.ankusa.conformance;

import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * A vector {@code Gateway}: unreachable, or one status/headers/body answer, optionally delayed.
 *
 * @param unreachable when true no server starts at all
 * @param status the status every request is answered with
 * @param headers the headers the answer carries, beyond the content length
 * @param body the answer's body
 * @param delayMs how long to sleep before the status line goes out
 */
record GatewaySpec(
    boolean unreachable,
    int status,
    Map<String, String> headers,
    @Nullable BodySpec body,
    int delayMs) {

  GatewaySpec {
    headers = headers == null ? Map.of() : Map.copyOf(headers);
  }
}
