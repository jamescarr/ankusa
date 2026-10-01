package io.github.jamescarr.ankusa;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * What every client carries into every request: the headers it sends, how long it waits, and how it
 * reaches the network.
 *
 * <p>Immutable, so one instance is safe to share across threads. Build one when a client needs more
 * than the defaults:
 *
 * <pre>{@code
 * ClientOptions options = ClientOptions.builder()
 *     .header("authorization", "Bearer " + token)
 *     .timeout(Duration.ofSeconds(30))
 *     .build();
 * AdminClient admin = new AdminClient("http://127.0.0.1:4002", options);
 * }</pre>
 *
 * <p>The default transport is the JDK {@link java.net.http.HttpClient}; {@link
 * ClientOptions.Builder#transport} replaces it.
 *
 * @param headers the headers every request carries; this record keeps an unmodifiable copy
 * @param timeout how long a whole exchange may take, connect through last body byte
 * @param transport how requests reach the network
 */
public record ClientOptions(Map<String, String> headers, Duration timeout, Transport transport) {

  /** How long a request may take when the caller does not say: ten seconds. */
  public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

  /** RFC 9110 field-name, the only shape {@code HttpRequest.Builder.header} accepts. */
  private static final Pattern HEADER_NAME = Pattern.compile("[!#$%&'*+.^_`|~0-9A-Za-z-]+");

  /**
   * The header names the JDK client sets itself and refuses to be given again; see {@code
   * jdk.internal.net.http.common.Utils.DISALLOWED_HEADERS_SET}.
   */
  private static final Set<String> RESTRICTED_HEADERS =
      Set.of("connection", "content-length", "expect", "host", "upgrade");

  /**
   * Validates the options and copies the headers.
   *
   * @throws NullPointerException when any component is null
   * @throws IllegalArgumentException when the timeout is not positive, a header name is not an RFC
   *     9110 token, a header name is one the JDK client sets itself, or a header value carries CR,
   *     LF or NUL
   */
  public ClientOptions {
    headers = Objects.requireNonNull(headers, "headers");
    transport = Objects.requireNonNull(transport, "transport");

    if (timeout == null) {
      throw new NullPointerException("timeout");
    }
    if (timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must be positive, got " + timeout);
    }

    for (Map.Entry<String, String> header : headers.entrySet()) {
      String name = Objects.requireNonNull(header.getKey(), "header name");
      String value = Objects.requireNonNull(header.getValue(), "value of header " + name);

      if (!HEADER_NAME.matcher(name).matches()) {
        throw new IllegalArgumentException(
            "header name is not an RFC 9110 token: \"" + name + "\"");
      }
      if (RESTRICTED_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
        throw new IllegalArgumentException(
            "header is set by the HTTP client and cannot be overridden: \"" + name + "\"");
      }
      if (value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0 || value.indexOf('\0') >= 0) {
        throw new IllegalArgumentException(
            "header value for \"" + name + "\" contains CR, LF, or NUL");
      }
    }

    headers = Collections.unmodifiableMap(new LinkedHashMap<>(headers));
  }

  /**
   * Options with no headers, {@link #DEFAULT_TIMEOUT}, and the default transport.
   *
   * @return the defaults every client falls back to
   */
  public static ClientOptions defaults() {
    return builder().build();
  }

  /**
   * Starts building options.
   *
   * @return an empty builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Describes these options without ever printing a header value.
   *
   * <p>Header values carry bearer tokens and API keys, so only the names are shown.
   *
   * @return a one-line description naming the header names, timeout and transport
   */
  @Override
  public String toString() {
    return "ClientOptions[headers="
        + headers.keySet()
        + ", timeout="
        + timeout
        + ", transport="
        + transport
        + "]";
  }

  /** Collects headers, a timeout, and a transport into {@link ClientOptions}. */
  public static final class Builder {

    private final Map<String, String> headers = new LinkedHashMap<>();
    private Duration timeout = DEFAULT_TIMEOUT;
    private Transport transport = Transport.jdk();

    private Builder() {}

    /**
     * Sets a header every request through the resulting client carries.
     *
     * @param name the header name
     * @param value the header value
     * @return this builder
     */
    public Builder header(String name, String value) {
      headers.put(name, value);
      return this;
    }

    /**
     * Sets how long a whole exchange may take.
     *
     * @param timeout the timeout, connect through last body byte
     * @return this builder
     */
    public Builder timeout(Duration timeout) {
      this.timeout = timeout;
      return this;
    }

    /**
     * Sets how requests reach the network.
     *
     * @param transport the transport to send with
     * @return this builder
     */
    public Builder transport(Transport transport) {
      this.transport = transport;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options, validated
     */
    public ClientOptions build() {
      return new ClientOptions(headers, timeout, transport);
    }
  }
}
