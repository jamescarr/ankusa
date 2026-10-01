package io.github.jamescarr.ankusa.internal;

import io.github.jamescarr.ankusa.ClientOptions;
import io.github.jamescarr.ankusa.TransportRequest;
import io.github.jamescarr.ankusa.TransportResponse;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Internal: not part of the API; may change in any release.
 *
 * <p>The one place every client sends from: it holds the base URL and the {@link ClientOptions},
 * builds the absolute URI, and hands the request to the transport.
 */
public final class HttpCore {

  private static final char[] HEX = "0123456789ABCDEF".toCharArray();

  private final String base;
  private final ClientOptions options;

  /**
   * Parses the base URL every path is appended to.
   *
   * @param baseUrl an absolute http or https URL; trailing slashes are trimmed
   * @param options the headers, timeout and transport every request uses
   * @throws IllegalArgumentException when {@code baseUrl} is not an absolute http(s) URL
   * @throws NullPointerException when {@code options} is null
   */
  public HttpCore(String baseUrl, ClientOptions options) {
    this.base = trimTrailingSlashes(requireAbsoluteHttpUrl(baseUrl));
    this.options = Objects.requireNonNull(options, "options");
  }

  /**
   * Sends one request through the configured transport.
   *
   * @param method the HTTP method
   * @param pathAndQuery the absolute path, query string included
   * @param jsonBody the request body, or null for a request with no body; a body switches the
   *     content type to {@code application/json}, overriding anything the caller set
   * @return the response, body included
   * @throws IOException when no complete response arrives
   * @throws InterruptedException when the calling thread is interrupted
   */
  public TransportResponse send(String method, String pathAndQuery, byte @Nullable [] jsonBody)
      throws IOException, InterruptedException {
    Map<String, String> headers = new LinkedHashMap<>(options.headers());

    if (jsonBody != null) {
      headers.keySet().removeIf(name -> name.equalsIgnoreCase("content-type"));
      headers.put("content-type", "application/json");
    }

    TransportRequest request =
        new TransportRequest(
            method, URI.create(base + pathAndQuery), headers, jsonBody, options.timeout());

    return options.transport().send(request);
  }

  /**
   * Escapes a raw value so it travels as exactly one path segment.
   *
   * <p>Everything outside RFC 3986's unreserved set becomes {@code %XX} in upper-case hex, so an id
   * containing {@code /}, {@code ?}, {@code #}, {@code %} or a space cannot change the shape of the
   * request.
   *
   * @param raw the raw value
   * @return the escaped segment
   */
  public static String pathSegment(String raw) {
    byte[] bytes = raw.getBytes(StandardCharsets.UTF_8);
    StringBuilder escaped = new StringBuilder(bytes.length);

    for (byte b : bytes) {
      int c = b & 0xFF;
      if (isUnreserved(c)) {
        escaped.append((char) c);
      } else {
        escaped.append('%').append(HEX[c >> 4]).append(HEX[c & 0xF]);
      }
    }

    return escaped.toString();
  }

  /**
   * Appends query parameters in the map's order.
   *
   * @param path the path to append to
   * @param params the parameters; each value is form-encoded
   * @return the path with {@code ?k=v&k=v}, or {@code path} itself when there is nothing to add
   */
  public static String withQuery(String path, LinkedHashMap<String, String> params) {
    if (params.isEmpty()) {
      return path;
    }

    StringBuilder url = new StringBuilder(path);
    char separator = '?';

    for (Map.Entry<String, String> param : params.entrySet()) {
      url.append(separator)
          .append(param.getKey())
          .append('=')
          .append(URLEncoder.encode(param.getValue(), StandardCharsets.UTF_8));
      separator = '&';
    }

    return url.toString();
  }

  /** RFC 3986 unreserved: the only characters that may appear escaped nowhere. */
  private static boolean isUnreserved(int c) {
    return (c >= 'A' && c <= 'Z')
        || (c >= 'a' && c <= 'z')
        || (c >= '0' && c <= '9')
        || c == '-'
        || c == '.'
        || c == '_'
        || c == '~';
  }

  private static String requireAbsoluteHttpUrl(String baseUrl) {
    URI uri;
    try {
      uri = new URI(baseUrl);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "invalid base URL \"" + baseUrl + "\": must be an absolute http(s) URL", e);
    }

    String scheme = uri.getScheme();
    boolean isHttp =
        scheme != null && (scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"));
    if (!isHttp || uri.getHost() == null) {
      throw new IllegalArgumentException(
          "invalid base URL \"" + baseUrl + "\": must be an absolute http(s) URL");
    }

    return baseUrl;
  }

  private static String trimTrailingSlashes(String url) {
    int end = url.length();
    while (end > 0 && url.charAt(end - 1) == '/') {
      end--;
    }
    return url.substring(0, end);
  }
}
