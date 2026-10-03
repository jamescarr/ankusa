package io.github.jamescarr.ankusa.admin;

import io.github.jamescarr.ankusa.ClientOptions;
import io.github.jamescarr.ankusa.TransportResponse;
import io.github.jamescarr.ankusa.internal.HttpCore;
import io.github.jamescarr.ankusa.internal.Json;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;

/**
 * Operates against the Ankusa operator listener ({@code admin.port}, default 4002): health,
 * Prometheus metrics, the redacted configuration, the dead-letter queue, replay jobs, and the
 * quarantine list.
 *
 * <p>Every answer is node-local, so a fleet operator queries each node's admin port. The listener
 * authenticates nobody on its own; the headers in {@link ClientOptions} are for whatever boundary a
 * deployer puts in front of it.
 *
 * <pre>{@code
 * AdminClient admin = new AdminClient("http://ankusa.example:4002");
 *
 * AdminHealth health = admin.health();
 * DlqPage dead = admin.listDeadLetters(ListDeadLettersParams.builder().limit(50).build());
 * Replay replay = admin.createReplay(ReplaySpec.builder().kind("dlq").rate(500).build());
 * Replay paused = admin.updateReplay(replay.id(), ReplayPatch.builder().state("paused").build());
 * ReplayList jobs = admin.listReplays();
 * }</pre>
 *
 * <p>Construct one per listener and share it: a client holds no mutable state and every method is
 * safe to call from any thread. Each call is one request, and no redirect is ever followed — a 3xx
 * is an unavailable listener.
 */
public final class AdminClient {

  private static final String ROLE_NOT_ENABLED = "role_not_enabled";

  private final HttpCore http;

  /**
   * Creates a client against an operator listener.
   *
   * @param baseUrl the listener's base URL, e.g. {@code http://ankusa.example:4002}
   * @throws IllegalArgumentException when {@code baseUrl} is not an absolute http(s) URL
   */
  public AdminClient(String baseUrl) {
    this(baseUrl, ClientOptions.defaults());
  }

  /**
   * Creates a client with headers, a timeout, or a transport of its own.
   *
   * @param baseUrl the listener's base URL
   * @param options what every request carries
   * @throws IllegalArgumentException when {@code baseUrl} is not an absolute http(s) URL
   */
  public AdminClient(String baseUrl, ClientOptions options) {
    this.http = new HttpCore(baseUrl, options);
  }

  /**
   * Probes the listener.
   *
   * @return the listener's health answer
   * @throws RoleNotEnabledError when the listener refuses the call for a missing role
   * @throws AdminRejectedError when the listener refuses the call with another 4xx
   * @throws AdminUnavailableError when the listener is unreachable, answers anything but 2xx, or
   *     answers with a body that is not the health object
   */
  public AdminHealth health() {
    return decode(send("GET", "/health", null), AdminHealth.class);
  }

  /**
   * Reads the Prometheus text exposition.
   *
   * <p>The body is passed through as text, not parsed: this SDK reports the metrics; it does not
   * interpret them.
   *
   * @return the response body decoded as UTF-8
   * @throws RoleNotEnabledError when the listener refuses the call for a missing role
   * @throws AdminRejectedError when the listener refuses the call with another 4xx
   * @throws AdminUnavailableError when the listener is unreachable or answers anything but 2xx
   */
  public String metrics() {
    TransportResponse response = send("GET", "/metrics", null);
    requireSuccess(response);
    return new String(response.body(), StandardCharsets.UTF_8);
  }

  /**
   * Reads the effective, redacted configuration.
   *
   * @return the configuration's fields, in the order the body gave them
   * @throws RoleNotEnabledError when the listener refuses the call for a missing role
   * @throws AdminRejectedError when the listener refuses the call with another 4xx
   * @throws AdminUnavailableError when the listener is unreachable, answers anything but 2xx, or
   *     answers with a body that is not a JSON object
   */
  public Map<String, @Nullable Object> config() {
    TransportResponse response = send("GET", "/v1/config", null);
    requireSuccess(response);
    try {
      return Json.readObject(response.body());
    } catch (JacksonException | NullPointerException e) {
      throw invalidBody(response, e);
    }
  }

  /**
   * Lists the first page of the dead-letter queue.
   *
   * @return one page of dead letters
   * @throws RoleNotEnabledError when the listener refuses the call for a missing role
   * @throws AdminRejectedError when the listener refuses the call with another 4xx
   * @throws AdminUnavailableError when the listener is unreachable, answers anything but 2xx, or
   *     answers with a body that is not a DLQ page
   */
  public DlqPage listDeadLetters() {
    return listDeadLetters(ListDeadLettersParams.builder().build());
  }

  /**
   * Lists a filtered page of the dead-letter queue.
   *
   * @param params the source, lower bound, and page size to list with
   * @return one page of dead letters
   * @throws RoleNotEnabledError when the listener refuses the call for a missing role
   * @throws AdminRejectedError when the listener refuses the call with another 4xx
   * @throws AdminUnavailableError when the listener is unreachable, answers anything but 2xx, or
   *     answers with a body that is not a DLQ page
   */
  public DlqPage listDeadLetters(ListDeadLettersParams params) {
    LinkedHashMap<String, String> query = new LinkedHashMap<>();
    if (params.sourceId() != null) {
      query.put("source_id", params.sourceId());
    }
    if (params.since() != null) {
      query.put("since", params.since().toString());
    }
    if (params.limit() != null) {
      query.put("limit", params.limit().toString());
    }
    return decode(send("GET", HttpCore.withQuery("/v1/dlq", query), null), DlqPage.class);
  }

  /**
   * Creates a replay job.
   *
   * <p>The body is the spec as JSON; the listener answers {@code 202} with the new job, or {@code
   * 200} with an existing running or paused job whose filter matches, which makes a proxy retry
   * idempotent. A job never bulk-flips rows: it drips them into dispatch at {@code rate} and only
   * while dispatch has spare capacity.
   *
   * @param spec what to replay and how fast
   * @return the replay job
   * @throws RoleNotEnabledError when the listener refuses the call for a missing role
   * @throws AdminRejectedError when the listener refuses the call with another 4xx
   * @throws AdminUnavailableError when the listener is unreachable, answers anything but 2xx, or
   *     answers with a body that is not a replay
   */
  public Replay createReplay(ReplaySpec spec) {
    return decode(send("POST", "/v1/replays", Json.write(spec)), Replay.class);
  }

  /**
   * Reads one replay job.
   *
   * @param id the job id
   * @return the replay job
   * @throws RoleNotEnabledError when the listener refuses the call for a missing role
   * @throws AdminRejectedError when the listener refuses the call with another 4xx, including a
   *     {@code 404} whose code is {@code replay_not_found}
   * @throws AdminUnavailableError when the listener is unreachable, answers anything but 2xx, or
   *     answers with a body that is not a replay
   */
  public Replay getReplay(String id) {
    return decode(send("GET", "/v1/replays/" + HttpCore.pathSegment(id), null), Replay.class);
  }

  /**
   * Lists the replay jobs.
   *
   * @return the jobs, newest first
   * @throws RoleNotEnabledError when the listener refuses the call for a missing role
   * @throws AdminRejectedError when the listener refuses the call with another 4xx
   * @throws AdminUnavailableError when the listener is unreachable, answers anything but 2xx, or
   *     answers with a body that is not a replay page
   */
  public ReplayList listReplays() {
    return decode(send("GET", "/v1/replays", null), ReplayList.class);
  }

  /**
   * Pauses, resumes, cancels, or re-paces a replay job.
   *
   * <p>The body is the patch as JSON; only the fields the caller set travel. Resuming ({@code
   * state: "running"}) clears the job's error and resets its auto-pause window.
   *
   * @param id the job id
   * @param patch what to change
   * @return the updated replay job
   * @throws RoleNotEnabledError when the listener refuses the call for a missing role
   * @throws AdminRejectedError when the listener refuses the call with another 4xx, including a
   *     {@code 404} ({@code replay_not_found}) and a {@code 409} ({@code replay_finished})
   * @throws AdminUnavailableError when the listener is unreachable, answers anything but 2xx, or
   *     answers with a body that is not a replay
   */
  public Replay updateReplay(String id, ReplayPatch patch) {
    return decode(
        send("PATCH", "/v1/replays/" + HttpCore.pathSegment(id), Json.write(patch)), Replay.class);
  }

  /**
   * Lists recent quarantined hooks.
   *
   * @return the quarantined entries, newest first
   * @throws RoleNotEnabledError when the listener refuses the call for a missing role
   * @throws AdminRejectedError when the listener refuses the call with another 4xx
   * @throws AdminUnavailableError when the listener is unreachable, answers anything but 2xx, or
   *     answers with a body that is not a quarantine page
   */
  public QuarantinePage listQuarantined() {
    return listQuarantined(ListQuarantinedParams.builder().build());
  }

  /**
   * Lists recent quarantined hooks, at most a page at a time.
   *
   * @param params the page size to list with
   * @return the quarantined entries, newest first
   * @throws RoleNotEnabledError when the listener refuses the call for a missing role
   * @throws AdminRejectedError when the listener refuses the call with another 4xx
   * @throws AdminUnavailableError when the listener is unreachable, answers anything but 2xx, or
   *     answers with a body that is not a quarantine page
   */
  public QuarantinePage listQuarantined(ListQuarantinedParams params) {
    LinkedHashMap<String, String> query = new LinkedHashMap<>();
    if (params.limit() != null) {
      query.put("limit", params.limit().toString());
    }
    return decode(
        send("GET", HttpCore.withQuery("/v1/quarantine", query), null), QuarantinePage.class);
  }

  /** Sends one request, turning every transport failure into an unavailable listener. */
  private TransportResponse send(String method, String pathAndQuery, byte @Nullable [] body) {
    try {
      return http.send(method, pathAndQuery, body);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AdminUnavailableError("admin listener unreachable", e);
    } catch (IOException e) {
      throw new AdminUnavailableError("admin listener unreachable", e);
    }
  }

  /** Decodes a body once the response is a 2xx. */
  private static <T> T decode(TransportResponse response, Class<T> type) {
    requireSuccess(response);
    try {
      return Json.read(response.body(), type);
    } catch (JacksonException | NullPointerException e) {
      throw invalidBody(response, e);
    }
  }

  /**
   * Classifies a response, in this order: a 2xx passes; a 409 naming a missing role is a role
   * refusal; any other 4xx is a rejection; everything else is an unavailable listener.
   */
  private static void requireSuccess(TransportResponse response) {
    int status = response.status();
    if (status >= 200 && status <= 299) {
      return;
    }
    Object body = Json.errorBody(response.body());
    if (status == 409 && isRoleNotEnabled(body)) {
      throw new RoleNotEnabledError(stringField(body, "role"));
    }
    if (status >= 400 && status <= 499) {
      throw new AdminRejectedError(status, stringField(body, "error"));
    }
    throw new AdminUnavailableError("admin listener error (" + status + ")", null);
  }

  /** A body this SDK could not decode is a listener failure, not a caller error. */
  private static AdminUnavailableError invalidBody(TransportResponse response, Exception e) {
    return new AdminUnavailableError(
        "admin listener returned an invalid JSON body (" + response.status() + ")", e);
  }

  /** Whether a decoded error body says the node does not run the needed role. */
  private static boolean isRoleNotEnabled(@Nullable Object body) {
    return body instanceof Map<?, ?> map && ROLE_NOT_ENABLED.equals(map.get("error"));
  }

  /** Reads one field of a decoded error body, only when the field really is a string. */
  private static @Nullable String stringField(@Nullable Object body, String name) {
    if (body instanceof Map<?, ?> map && map.get(name) instanceof String value) {
      return value;
    }
    return null;
  }
}
