package io.github.jamescarr.ankusa.conformance;

import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * A vector {@code Client}: the options the client under test is built with.
 *
 * @param headers the headers every request carries
 * @param timeoutMs the request timeout in milliseconds, or null for the SDK default
 * @param transport {@code "injected"} to replace the network, or null for the real transport
 */
record ClientSpec(
    Map<String, String> headers, @Nullable Integer timeoutMs, @Nullable String transport) {

  ClientSpec {
    headers = headers == null ? Map.of() : Map.copyOf(headers);
  }
}
