package io.github.jamescarr.ankusa;

import java.io.IOException;
import java.net.http.HttpClient;

/**
 * How the SDK reaches the network.
 *
 * <p>Every client takes one through {@link ClientOptions}, so a test, a mock, or a different HTTP
 * stack can replace the JDK client without touching the SDK. An implementation must:
 *
 * <ul>
 *   <li>never follow a redirect: the SDK classifies a 3xx as an unreachable listener, because a
 *       redirect means the request did not reach the Ankusa API the caller configured;
 *   <li>honor {@link TransportRequest#timeout()}, bounding connect, request, and the last body byte
 *       alike — a response that stops mid-body is a timeout, not a short body;
 *   <li>return the whole body, so the caller can hash it;
 *   <li>throw {@link IOException} for any failure to get a response, and {@link
 *       InterruptedException} when the calling thread is interrupted.
 * </ul>
 */
@FunctionalInterface
public interface Transport {

  /**
   * Sends one request and waits for its complete response.
   *
   * @param request the request to send
   * @return the response, body included
   * @throws IOException when no complete response arrives
   * @throws InterruptedException when the calling thread is interrupted
   */
  TransportResponse send(TransportRequest request) throws IOException, InterruptedException;

  /**
   * The shared default transport: a JDK {@link HttpClient} that pins HTTP/1.1 and never follows a
   * redirect.
   *
   * @return the process-wide default transport
   */
  static Transport jdk() {
    return JdkTransport.DEFAULT;
  }

  /**
   * Wraps a caller-supplied {@link HttpClient}.
   *
   * @param client the client to send with; it must not follow redirects
   * @return a transport sending through {@code client}
   * @throws IllegalArgumentException when {@code client} is configured to follow redirects
   */
  static Transport jdk(HttpClient client) {
    if (client.followRedirects() != HttpClient.Redirect.NEVER) {
      throw new IllegalArgumentException("transport: the HttpClient must not follow redirects");
    }
    return new JdkTransport(client);
  }
}
