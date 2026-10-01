package io.github.jamescarr.ankusa;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A {@link Transport} that answers from a function and keeps every request it was given.
 *
 * <p>Tests use it instead of a socket: the path a client builds and the body it writes are the
 * whole assertion, and no port has to be free.
 */
public final class FakeTransport implements Transport {

  private final Function<TransportRequest, TransportResponse> responder;
  private final List<TransportRequest> requests = new ArrayList<>();

  /**
   * Creates a transport answering with {@code responder}.
   *
   * @param responder builds the response for each request
   */
  public FakeTransport(Function<TransportRequest, TransportResponse> responder) {
    this.responder = responder;
  }

  @Override
  public TransportResponse send(TransportRequest request) {
    requests.add(request);
    return responder.apply(request);
  }

  /**
   * The requests made so far, in order.
   *
   * @return an immutable copy
   */
  public List<TransportRequest> requests() {
    return List.copyOf(requests);
  }

  /**
   * The one request made, failing when that is not exactly one.
   *
   * @return the single request
   */
  public TransportRequest onlyRequest() {
    if (requests.size() != 1) {
      throw new AssertionError("expected exactly one request, got " + requests.size());
    }
    return requests.get(0);
  }

  /**
   * A response with a JSON body.
   *
   * @param status the status code
   * @param body the body, sent as UTF-8
   * @return the response
   */
  public static TransportResponse json(int status, String body) {
    return new TransportResponse(
        status, Map.of("content-type", List.of("application/json")), bytes(body));
  }

  /**
   * A response with no body at all.
   *
   * @param status the status code
   * @return the response
   */
  public static TransportResponse empty(int status) {
    return new TransportResponse(status, Map.of(), new byte[0]);
  }

  /**
   * A response with a plain-text body.
   *
   * @param status the status code
   * @param body the body, sent as UTF-8
   * @return the response
   */
  public static TransportResponse text(int status, String body) {
    return new TransportResponse(
        status, Map.of("content-type", List.of("text/plain")), bytes(body));
  }

  private static byte[] bytes(String body) {
    return body.getBytes(StandardCharsets.UTF_8);
  }
}
