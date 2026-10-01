package io.github.jamescarr.ankusa.conformance;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.jamescarr.ankusa.Transport;
import io.github.jamescarr.ankusa.TransportRequest;
import io.github.jamescarr.ankusa.TransportResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.NullNode;
import tools.jackson.databind.node.StringNode;

/**
 * The mock a client under test talks to: a real HTTP server on a free loopback port, an in-process
 * transport that never touches the network, or nothing at all.
 *
 * <p>Either way it records every request through {@link Recorder}, so the vector can assert the
 * method, path, headers and body independent of whether a socket was involved.
 */
final class Gateway implements AutoCloseable {

  /** Where an unreachable gateway points: reserved, and refused immediately. */
  static final String UNREACHABLE_BASE_URL = "http://127.0.0.1:1";

  /** The injected transport's base URL; it never resolves, because nothing resolves it. */
  static final String INJECTED_BASE_URL = "http://gateway.invalid";

  private final String baseUrl;
  private final @Nullable Transport transport;
  private final @Nullable HttpServer server;
  private final @Nullable ExecutorService executor;

  private Gateway(
      String baseUrl,
      @Nullable Transport transport,
      @Nullable HttpServer server,
      @Nullable ExecutorService executor) {
    this.baseUrl = baseUrl;
    this.transport = transport;
    this.server = server;
    this.executor = executor;
  }

  /**
   * Starts the gateway a vector asks for.
   *
   * @param spec the vector's gateway
   * @param client the vector's client, whose {@code transport} decides between a real server and an
   *     injected transport
   * @param recorder where requests are recorded
   * @param mapper the runner's mapper, for parsing recorded bodies
   * @return the gateway, which the caller must close
   */
  static Gateway start(GatewaySpec spec, ClientSpec client, Recorder recorder, JsonMapper mapper) {
    if (spec.unreachable()) {
      return new Gateway(UNREACHABLE_BASE_URL, null, null, null);
    }
    if ("injected".equals(client.transport())) {
      return new Gateway(INJECTED_BASE_URL, transport(spec, recorder, mapper), null, null);
    }
    return startServer(spec, recorder, mapper);
  }

  /**
   * The base URL the client is built with.
   *
   * @return an absolute http URL ending in the authority, with no trailing slash
   */
  String baseUrl() {
    return baseUrl;
  }

  /**
   * The transport to inject, or null to use the SDK's default.
   *
   * @return the injected transport for an {@code injected} client, null otherwise
   */
  @Nullable Transport transport() {
    return transport;
  }

  /**
   * Stops the server, if there is one, and releases its threads.
   *
   * <p>A stopped server drops in-flight connections rather than draining them: a vector that
   * deliberately timed out already has what it needs.
   */
  @Override
  public void close() {
    if (server != null) {
      server.stop(0);
    }
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  private static Gateway startServer(GatewaySpec spec, Recorder recorder, JsonMapper mapper) {
    byte[] payload = bodyBytes(spec.body(), mapper);
    ExecutorService executor = Executors.newCachedThreadPool();
    HttpServer server;

    try {
      server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    } catch (IOException e) {
      executor.shutdownNow();
      throw new UncheckedIOException(e);
    }

    server.setExecutor(executor);
    server.createContext("/", exchange -> answer(exchange, spec, payload, recorder, mapper));
    server.start();

    InetSocketAddress bound = server.getAddress();
    String host = bound.getAddress().getHostAddress();
    String authority = host.indexOf(':') >= 0 ? "[" + host + "]" : host;

    return new Gateway("http://" + authority + ":" + bound.getPort(), null, server, executor);
  }

  private static void answer(
      HttpExchange exchange, GatewaySpec spec, byte[] payload, Recorder recorder, JsonMapper mapper)
      throws IOException {
    byte[] requestBody = readAll(exchange.getRequestBody());
    URI uri = exchange.getRequestURI();
    String query = uri.getRawQuery();

    recorder.add(
        new Recorder.Recorded(
            exchange.getRequestMethod(),
            uri.getRawPath() + (query == null ? "" : "?" + query),
            firstValues(exchange.getRequestHeaders()),
            parseBody(requestBody, mapper)));

    // Record before sleeping: a vector whose client abandons the response still asserts the
    // request that was made.
    if (spec.delayMs() > 0) {
      try {
        Thread.sleep(spec.delayMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    spec.headers().forEach((name, value) -> exchange.getResponseHeaders().set(name, value));

    // -1 sends Content-length: 0; 0 would switch the response to chunked encoding instead.
    exchange.sendResponseHeaders(spec.status(), payload.length == 0 ? -1 : payload.length);

    try (OutputStream body = exchange.getResponseBody()) {
      body.write(payload);
    } catch (IOException e) {
      // The client already gave up and closed the connection.
    }
  }

  private static Transport transport(GatewaySpec spec, Recorder recorder, JsonMapper mapper) {
    byte[] payload = bodyBytes(spec.body(), mapper);

    return request -> {
      recorder.add(record(request, mapper));

      Map<String, List<String>> headers = new LinkedHashMap<>();
      spec.headers().forEach((name, value) -> headers.put(name, List.of(value)));
      headers.put("content-length", List.of(Integer.toString(payload.length)));

      return new TransportResponse(spec.status(), headers, payload);
    };
  }

  private static Recorder.Recorded record(TransportRequest request, JsonMapper mapper) {
    URI uri = request.uri();
    String query = uri.getRawQuery();
    Map<String, String> headers = new LinkedHashMap<>();
    request.headers().forEach((name, value) -> headers.put(name.toLowerCase(), value));

    return new Recorder.Recorded(
        request.method(),
        uri.getRawPath() + (query == null ? "" : "?" + query),
        headers,
        parseBody(request.body() == null ? new byte[0] : request.body(), mapper));
  }

  /**
   * A vector Body in bytes: text as UTF-8, base64 decoded, json as compact JSON; an absent body is
   * empty.
   *
   * @param spec the body, or null when the vector has none
   * @param mapper the runner's mapper
   * @return the bytes to answer with
   */
  static byte[] bodyBytes(@Nullable BodySpec spec, JsonMapper mapper) {
    if (spec == null) {
      return new byte[0];
    }
    if (spec.text() != null) {
      return spec.text().getBytes(StandardCharsets.UTF_8);
    }
    if (spec.base64() != null) {
      return Base64.getDecoder().decode(spec.base64());
    }
    if (spec.json() != null) {
      return mapper.writeValueAsBytes(spec.json());
    }
    return new byte[0];
  }

  /**
   * A recorded body in the shape the vectors assert: parsed JSON when it is JSON, a string node
   * when it is not, and a null node when there was no body at all.
   *
   * @param raw the bytes the client sent
   * @param mapper the runner's mapper
   * @return the body as a node
   */
  static JsonNode parseBody(byte[] raw, JsonMapper mapper) {
    if (raw.length == 0) {
      return NullNode.instance;
    }
    try {
      return mapper.readTree(raw);
    } catch (RuntimeException e) {
      return StringNode.valueOf(new String(raw, StandardCharsets.UTF_8));
    }
  }

  private static Map<String, String> firstValues(Map<String, List<String>> headers) {
    Map<String, String> first = new LinkedHashMap<>();
    headers.forEach(
        (name, values) -> {
          if (!values.isEmpty()) {
            first.put(name.toLowerCase(), values.get(0));
          }
        });
    return first;
  }

  private static byte[] readAll(InputStream stream) {
    try (stream) {
      return stream.readAllBytes();
    } catch (IOException e) {
      return new byte[0];
    }
  }
}
