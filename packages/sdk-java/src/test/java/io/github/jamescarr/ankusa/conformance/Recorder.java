package io.github.jamescarr.ankusa.conformance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import tools.jackson.databind.JsonNode;

/**
 * Collects, in order, every request the client under test made.
 *
 * <p>One instance per case, so requests cannot leak between cases even though the tests may run on
 * several threads.
 */
final class Recorder {

  /**
   * One request in the shape the vectors assert.
   *
   * @param method the HTTP method
   * @param path the request target as sent, query string included
   * @param headers the headers with lower-case names mapped to their first value
   * @param body the body parsed as JSON, a {@code NullNode} when there was none, or a string node
   *     when the body was not JSON
   */
  record Recorded(String method, String path, Map<String, String> headers, JsonNode body) {

    Recorded {
      headers = Map.copyOf(headers);
    }
  }

  private final List<Recorded> requests = new ArrayList<>();

  /**
   * Appends one request.
   *
   * @param request the request to record
   */
  synchronized void add(Recorded request) {
    requests.add(request);
  }

  /**
   * The requests recorded so far, in order.
   *
   * @return an immutable copy
   */
  synchronized List<Recorded> snapshot() {
    return List.copyOf(requests);
  }
}
