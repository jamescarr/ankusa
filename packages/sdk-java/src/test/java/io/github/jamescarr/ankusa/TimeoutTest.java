package io.github.jamescarr.ankusa;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import io.github.jamescarr.ankusa.claimcheck.ClaimCheckClient;
import io.github.jamescarr.ankusa.claimcheck.ClaimCheckUnavailableError;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

/**
 * The timeout bounds the whole exchange, not just the response headers.
 *
 * <p>A client that only set {@code HttpRequest.timeout} would wait forever on a body that stops
 * mid-flight, which is exactly what a stalled claim-check gateway looks like.
 */
class TimeoutTest {

  @Test
  void a_body_that_stops_after_one_byte_times_out_instead_of_hanging() {
    ExecutorService executor = Executors.newCachedThreadPool();
    HttpServer server;

    try {
      server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    } catch (IOException e) {
      throw new AssertionError(e);
    }

    // Ten bytes promised, one byte delivered, then a stall: the client is left waiting for the
    // rest of a body that is never coming.
    server.createContext(
        "/health", exchange -> answerAndStall(exchange, 10, Duration.ofSeconds(3).toMillis()));
    server.setExecutor(executor);
    server.start();

    InetSocketAddress bound = server.getAddress();
    String host = bound.getAddress().getHostAddress();
    String url =
        "http://" + (host.indexOf(':') >= 0 ? "[" + host + "]" : host) + ":" + bound.getPort();

    try {
      ClaimCheckClient client =
          new ClaimCheckClient(
              url, ClientOptions.builder().timeout(Duration.ofMillis(300)).build());

      long started = System.nanoTime();
      ClaimCheckUnavailableError error =
          assertThrows(ClaimCheckUnavailableError.class, client::health);
      long elapsedMs = (System.nanoTime() - started) / 1_000_000;

      assertTrue(
          elapsedMs < 2000,
          "took " + elapsedMs + "ms, so the timeout only bounded the response headers");
      assertTrue(error.retryable(), error.getMessage());
    } finally {
      server.stop(0);
      executor.shutdownNow();
    }
  }

  private static void answerAndStall(
      com.sun.net.httpserver.HttpExchange exchange, int promisedBytes, long stallMs)
      throws IOException {
    exchange.sendResponseHeaders(200, promisedBytes);

    try (OutputStream body = exchange.getResponseBody()) {
      body.write('x');
      body.flush();

      try {
        Thread.sleep(stallMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } catch (IOException e) {
      // The client already timed out and closed the connection.
    }
  }
}
