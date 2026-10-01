package io.github.jamescarr.ankusa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jamescarr.ankusa.claimcheck.ClaimCheckClient;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The base URL and option validation every client shares, and the one thing that must never be
 * printed: a header value.
 */
class ClientOptionsTest {

  @Test
  void every_client_rejects_a_base_url_that_is_not_an_absolute_http_url() {
    List<String> invalid = List.of("localhost:4001", "ftp://h", "http://", "not a url");

    for (String baseUrl : invalid) {
      assertThrows(
          IllegalArgumentException.class,
          () -> new ClaimCheckClient(baseUrl),
          "claim check accepted " + baseUrl);
      assertThrows(
          IllegalArgumentException.class,
          () -> new ClaimCheckClient(baseUrl, ClientOptions.defaults()),
          "claim check accepted " + baseUrl);
    }
  }

  @Test
  void a_trailing_slash_on_the_base_url_does_not_double_the_separator() {
    FakeTransport transport =
        new FakeTransport(request -> FakeTransport.json(200, "{\"status\":\"ok\"}"));
    ClientOptions options = ClientOptions.builder().transport(transport).build();

    new ClaimCheckClient("http://h/prefix/", options).health();

    assertEquals("http://h/prefix/health", transport.onlyRequest().uri().toString());
  }

  @Test
  void a_restricted_header_a_newline_and_a_non_positive_timeout_are_caller_misuse() {
    assertThrows(
        IllegalArgumentException.class, () -> ClientOptions.builder().header("Host", "h").build());
    assertThrows(
        IllegalArgumentException.class,
        () -> ClientOptions.builder().header("x-token", "a\r\nx-injected: 1").build());
    assertThrows(
        IllegalArgumentException.class,
        () -> ClientOptions.builder().timeout(Duration.ZERO).build());
  }

  @Test
  void a_transport_that_follows_redirects_is_rejected() {
    HttpClient following =
        HttpClient.newBuilder().followRedirects(HttpClient.Redirect.ALWAYS).build();

    assertThrows(IllegalArgumentException.class, () -> Transport.jdk(following));
    Transport.jdk(HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build());
  }

  @Test
  void neither_options_nor_a_request_ever_print_a_header_value() {
    FakeTransport transport = new FakeTransport(request -> FakeTransport.empty(404));
    ClientOptions options =
        ClientOptions.builder()
            .header("authorization", "Bearer s3cr3t")
            .transport(transport)
            .build();

    assertFalse(options.toString().contains("s3cr3t"), options.toString());
    assertTrue(options.toString().contains("authorization"), options.toString());

    try {
      new ClaimCheckClient("http://h", options).health();
    } catch (AnkusaException expected) {
      // The request is what matters, and the transport recorded it before the client raised.
    }

    String printed = transport.onlyRequest().toString();
    assertFalse(printed.contains("s3cr3t"), printed);
    assertTrue(printed.contains("authorization"), printed);
  }
}
