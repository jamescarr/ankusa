package io.github.jamescarr.ankusa.routes;

import io.github.jamescarr.ankusa.ClientOptions;
import io.github.jamescarr.ankusa.TransportResponse;
import io.github.jamescarr.ankusa.internal.HttpCore;
import io.github.jamescarr.ankusa.internal.Json;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;

/**
 * Manages the routes an Ankusa deployment serves, over the route-management listener.
 *
 * <p>That listener is the control plane: it configures what the edge listener accepts, and no call
 * here captures anything. Construct one per listener and share it — a client holds no mutable state
 * and every method is safe to call from any thread.
 *
 * <pre>{@code
 * RoutesClient routes = new RoutesClient("http://ankusa.example:4003");
 *
 * routes.createRoute(RouteInput.builder("/webhooks/stripe").methods(List.of("POST")).build());
 * DryRunResult result = routes.testRoute(new DryRunRequest("POST", "/webhooks/stripe", "203.0.113.7"));
 * }</pre>
 *
 * <p>Every call is one request and no redirect is ever followed: a 3xx is an unavailable listener.
 */
public final class RoutesClient {

  /** The route collection every route call hangs off. */
  private static final String ROUTES = "/admin/routes";

  private final HttpCore http;

  /**
   * Creates a client against a route-management listener.
   *
   * @param baseUrl the listener's base URL, e.g. {@code http://ankusa.example:4003}
   * @throws IllegalArgumentException when {@code baseUrl} is not an absolute http(s) URL
   */
  public RoutesClient(String baseUrl) {
    this(baseUrl, ClientOptions.defaults());
  }

  /**
   * Creates a client with headers, a timeout, or a transport of its own.
   *
   * @param baseUrl the listener's base URL
   * @param options what every request carries
   * @throws IllegalArgumentException when {@code baseUrl} is not an absolute http(s) URL
   */
  public RoutesClient(String baseUrl, ClientOptions options) {
    this.http = new HttpCore(baseUrl, options);
  }

  /**
   * Probes the route-management listener.
   *
   * @return the listener's health
   * @throws RouteNotFoundError when the listener has no health endpoint
   * @throws RoutesRejectedError when the listener refuses the request with a 4xx
   * @throws RoutesUnavailableError when the listener is unreachable or answers anything else that
   *     is not a decodable 2xx
   */
  public RoutesHealth health() {
    return decode(send("GET", "/health", null), RoutesHealth.class, null);
  }

  /**
   * Lists the first page of routes.
   *
   * @return one page of routes
   * @throws RouteNotFoundError when the listener has no route collection
   * @throws RoutesRejectedError when the listener refuses the request with a 4xx
   * @throws RoutesUnavailableError when the listener is unreachable or answers anything else that
   *     is not a decodable 2xx
   */
  public RoutePage listRoutes() {
    return listRoutes(ListRoutesParams.builder().build());
  }

  /**
   * Lists routes, filtered and paged.
   *
   * @param params the filters and paging; each non-null field becomes one query parameter
   * @return one page of routes
   * @throws NullPointerException when {@code params} is null
   * @throws RouteNotFoundError when the listener has no route collection
   * @throws RoutesRejectedError when the listener refuses the request with a 4xx
   * @throws RoutesUnavailableError when the listener is unreachable or answers anything else that
   *     is not a decodable 2xx
   */
  public RoutePage listRoutes(ListRoutesParams params) {
    Objects.requireNonNull(params, "params");

    LinkedHashMap<String, String> query = new LinkedHashMap<>();
    if (params.enabled() != null) {
      query.put("enabled", params.enabled().toString());
    }
    if (params.limit() != null) {
      query.put("limit", params.limit().toString());
    }
    if (params.cursor() != null) {
      query.put("cursor", params.cursor());
    }

    return decode(send("GET", HttpCore.withQuery(ROUTES, query), null), RoutePage.class, null);
  }

  /**
   * Creates a route.
   *
   * @param input the fields to create it with
   * @return the created route, as the listener stored it
   * @throws NullPointerException when {@code input} is null
   * @throws RouteNotFoundError when the listener has no route collection
   * @throws RoutesRejectedError when the listener refuses the request with a 4xx
   * @throws RoutesUnavailableError when the listener is unreachable or answers anything else that
   *     is not a decodable 2xx
   */
  public Route createRoute(RouteInput input) {
    Objects.requireNonNull(input, "input");
    return decode(send("POST", ROUTES, Json.write(input)), Route.class, null);
  }

  /**
   * Reads one route.
   *
   * @param id the route's id; it is validated before any request and sent as one path segment
   * @return the route
   * @throws InvalidRouteIdError when {@code id} is null, empty, {@code .} or {@code ..}; nothing is
   *     sent
   * @throws RouteNotFoundError when no route has that id
   * @throws RoutesRejectedError when the listener refuses the request with another 4xx
   * @throws RoutesUnavailableError when the listener is unreachable or answers anything else that
   *     is not a decodable 2xx
   */
  public Route getRoute(String id) {
    return decode(send("GET", routePath(id), null), Route.class, id);
  }

  /**
   * Replaces one route.
   *
   * @param id the route's id; it is validated before any request and sent as one path segment
   * @param input the fields to replace it with
   * @return the replaced route, as the listener stored it
   * @throws InvalidRouteIdError when {@code id} is null, empty, {@code .} or {@code ..}; nothing is
   *     sent
   * @throws NullPointerException when {@code input} is null
   * @throws RouteNotFoundError when no route has that id
   * @throws RoutesRejectedError when the listener refuses the request with another 4xx
   * @throws RoutesUnavailableError when the listener is unreachable or answers anything else that
   *     is not a decodable 2xx
   */
  public Route replaceRoute(String id, RouteInput input) {
    Objects.requireNonNull(input, "input");
    return decode(send("PUT", routePath(id), Json.write(input)), Route.class, id);
  }

  /**
   * Updates the fields of one route, leaving the rest as they are.
   *
   * @param id the route's id; it is validated before any request and sent as one path segment
   * @param patch the fields to change
   * @return the updated route, as the listener stored it
   * @throws InvalidRouteIdError when {@code id} is null, empty, {@code .} or {@code ..}; nothing is
   *     sent
   * @throws NullPointerException when {@code patch} is null
   * @throws RouteNotFoundError when no route has that id
   * @throws RoutesRejectedError when the listener refuses the request with another 4xx
   * @throws RoutesUnavailableError when the listener is unreachable or answers anything else that
   *     is not a decodable 2xx
   */
  public Route updateRoute(String id, RoutePatch patch) {
    Objects.requireNonNull(patch, "patch");
    return decode(send("PATCH", routePath(id), Json.write(patch)), Route.class, id);
  }

  /**
   * Deletes one route.
   *
   * <p>The response body is ignored; a 2xx is success whatever it carries.
   *
   * @param id the route's id; it is validated before any request and sent as one path segment
   * @throws InvalidRouteIdError when {@code id} is null, empty, {@code .} or {@code ..}; nothing is
   *     sent
   * @throws RouteNotFoundError when no route has that id
   * @throws RoutesRejectedError when the listener refuses the request with another 4xx
   * @throws RoutesUnavailableError when the listener is unreachable or answers anything else that
   *     is not a 2xx
   */
  public void deleteRoute(String id) {
    classify(send("DELETE", routePath(id), null), id);
  }

  /**
   * Reads the global IP rules.
   *
   * @return the global IP rules
   * @throws RouteNotFoundError when the listener has no IP-rule configuration
   * @throws RoutesRejectedError when the listener refuses the request with a 4xx
   * @throws RoutesUnavailableError when the listener is unreachable or answers anything else that
   *     is not a decodable 2xx
   */
  public IpRules getIpRules() {
    return decode(send("GET", "/admin/ip-rules", null), IpRules.class, null);
  }

  /**
   * Replaces the global IP rules.
   *
   * @param rules the rules to install
   * @return the rules, as the listener stored them
   * @throws NullPointerException when {@code rules} is null
   * @throws RouteNotFoundError when the listener has no IP-rule configuration
   * @throws RoutesRejectedError when the listener refuses the request with a 4xx
   * @throws RoutesUnavailableError when the listener is unreachable or answers anything else that
   *     is not a decodable 2xx
   */
  public IpRules putIpRules(IpRules rules) {
    Objects.requireNonNull(rules, "rules");
    return decode(send("PUT", "/admin/ip-rules", Json.write(rules)), IpRules.class, null);
  }

  /**
   * Runs a request against the route table without capturing anything.
   *
   * @param request the request to dry-run
   * @return the decision the route table would make
   * @throws NullPointerException when {@code request} is null
   * @throws RouteNotFoundError when the listener has no dry-run endpoint
   * @throws RoutesRejectedError when the listener refuses the request with a 4xx
   * @throws RoutesUnavailableError when the listener is unreachable or answers anything else that
   *     is not a decodable 2xx
   */
  public DryRunResult testRoute(DryRunRequest request) {
    Objects.requireNonNull(request, "request");
    return decode(send("POST", ROUTES + "/test", Json.write(request)), DryRunResult.class, null);
  }

  /** Sends one request, turning every transport failure into an unavailable listener. */
  private TransportResponse send(String method, String path, byte @Nullable [] body) {
    try {
      return http.send(method, path, body);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RoutesUnavailableError("routes listener unreachable", e);
    } catch (IOException e) {
      throw new RoutesUnavailableError("routes listener unreachable", e);
    }
  }

  /** Validates an id and turns it into the route's path, or fails before any request is sent. */
  private static String routePath(@Nullable String id) {
    if (id == null || id.isEmpty() || id.equals(".") || id.equals("..")) {
      throw new InvalidRouteIdError(id);
    }
    return ROUTES + "/" + HttpCore.pathSegment(id);
  }

  /** Rejects every non-2xx response, or returns when the status is a success. */
  private static void classify(TransportResponse response, @Nullable String id) {
    int status = response.status();

    if (status >= 200 && status <= 299) {
      return;
    }
    if (status == 404) {
      throw new RouteNotFoundError(id);
    }
    if (status >= 400 && status <= 499) {
      throw rejected(response);
    }
    throw new RoutesUnavailableError("routes listener error (" + status + ")", null);
  }

  /** Classifies a response and decodes its body into a record when the status is a success. */
  private static <T> T decode(TransportResponse response, Class<T> type, @Nullable String id) {
    classify(response, id);

    try {
      return Json.read(response.body(), type);
    } catch (JacksonException | NullPointerException e) {
      throw new RoutesUnavailableError(
          "routes listener returned an invalid JSON body (" + response.status() + ")", e);
    }
  }

  /** Builds a rejected error from whatever the body said, when it said anything. */
  private static RoutesRejectedError rejected(TransportResponse response) {
    Object body = Json.errorBody(response.body());

    if (body instanceof Map<?, ?> map) {
      return new RoutesRejectedError(
          response.status(),
          string(map.get("error")),
          string(map.get("field")),
          string(map.get("message")),
          string(map.get("conflicting_id")),
          integral(map.get("max_routes")));
    }

    return new RoutesRejectedError(response.status(), null, null, null, null, null);
  }

  /** The value as a string, or null when it is anything else. */
  private static @Nullable String string(@Nullable Object value) {
    return value instanceof String text ? text : null;
  }

  /** The value as an integer, or null when it is absent or not an integral number. */
  private static @Nullable Integer integral(@Nullable Object value) {
    if (value instanceof Double || value instanceof Float) {
      return null;
    }
    return value instanceof Number number ? number.intValue() : null;
  }
}
