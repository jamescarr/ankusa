package io.github.jamescarr.ankusa.sources;

import io.github.jamescarr.ankusa.ClientOptions;
import io.github.jamescarr.ankusa.TransportResponse;
import io.github.jamescarr.ankusa.internal.HttpCore;
import io.github.jamescarr.ankusa.internal.Json;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;

/**
 * Manages a deployment's tenant-scoped sources through the Ankusa admin API.
 *
 * <p>The router exposes list, get, create, update and delete over a small JSON API. A tenant is the
 * operator's own customer identifier and a source id is {@code <tenant>.<name>}; this client builds
 * those paths. A tenant and a name must match {@code [A-Za-z0-9_-]{1,64}} before any path is built,
 * so a caller-supplied name can never escape its tenant.
 *
 * <p>The optional expected version is a safety latch: when one is set, the first call fetches
 * {@code GET /health} once, compares its {@code version} against the expected value, caches it, and
 * throws {@link VersionMismatchError} on any mismatch. Every later call re-checks the cached value
 * without another request.
 *
 * <p>A client holds one immutable {@link HttpCore}, so it is safe to share across threads; the only
 * mutable state is the cached server version.
 */
public final class SourcesClient {

  /** The only tenant and source-name shape this client accepts. */
  private static final Pattern SAFE_ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");

  private final HttpCore http;
  private final @Nullable String expectedVersion;
  private volatile @Nullable String serverVersion;

  /**
   * Creates a client against an admin API with the default options.
   *
   * @param baseUrl the admin API's base URL, e.g. {@code http://127.0.0.1:4002}
   * @throws IllegalArgumentException when {@code baseUrl} is not an absolute http(s) URL
   */
  public SourcesClient(String baseUrl) {
    this(baseUrl, ClientOptions.defaults());
  }

  /**
   * Creates a client with headers, a timeout, or a transport of its own.
   *
   * @param baseUrl the admin API's base URL
   * @param options what every request carries
   * @throws IllegalArgumentException when {@code baseUrl} is not an absolute http(s) URL
   */
  public SourcesClient(String baseUrl, ClientOptions options) {
    this(baseUrl, options, null);
  }

  /**
   * Creates a client that refuses to talk to an unexpected deployment version.
   *
   * @param baseUrl the admin API's base URL
   * @param options what every request carries
   * @param expectedVersion the version the deployment must report, or null to skip the check
   * @throws IllegalArgumentException when {@code baseUrl} is not an absolute http(s) URL
   */
  public SourcesClient(String baseUrl, ClientOptions options, @Nullable String expectedVersion) {
    this.http = new HttpCore(baseUrl, options);
    this.expectedVersion = expectedVersion;
  }

  /**
   * The deployment's Ankusa version, from {@code GET /health}.
   *
   * <p>Fetched once and cached; when an expected version was set this also enforces it.
   *
   * @return the version the server reported
   * @throws SourcesUnavailableError when the admin API is unreachable, answers anything but 200, or
   *     answers with a body that has no {@code version} string
   * @throws VersionMismatchError when an expected version was set and the server reports another
   */
  public String serverVersion() {
    String version = cachedVersion();
    checkVersion(version);
    return version;
  }

  /**
   * Lists a tenant's sources: {@code GET /v1/tenants/<tenant>/sources}.
   *
   * @param tenant the tenant whose sources to list
   * @return the tenant's sources, in the order the server returned them
   * @throws SourceInvalidError when {@code tenant} is not a valid identifier; nothing is sent
   * @throws SourcesUnavailableError when the admin API is unreachable or answers with a body that
   *     is not the expected shape
   * @throws VersionMismatchError when an expected version was set and the server reports another
   */
  public List<Source> listSources(String tenant) {
    validateTenant(tenant);
    ensureVersion();
    TransportResponse response = request("GET", sourcesPath(tenant), null);
    raiseForStatus(response);
    return decode(response, Entries.class).entries();
  }

  /**
   * Fetches one source: {@code GET /v1/tenants/<tenant>/sources/<name>}.
   *
   * @param tenant the tenant that owns the source
   * @param name the source's name
   * @return the source
   * @throws SourceInvalidError when {@code tenant} or {@code name} is not a valid identifier;
   *     nothing is sent
   * @throws SourceNotFoundError when the tenant has no such source
   * @throws SourcesUnavailableError when the admin API is unreachable or answers with a body that
   *     is not the expected shape
   * @throws VersionMismatchError when an expected version was set and the server reports another
   */
  public Source getSource(String tenant, String name) {
    validateTenant(tenant);
    validateName(name);
    ensureVersion();
    TransportResponse response = request("GET", sourcePath(tenant, name), null);
    raiseForStatus(response);
    return decode(response, Source.class);
  }

  /**
   * Creates a source: {@code POST /v1/tenants/<tenant>/sources}.
   *
   * <p>The source name travels in the body alongside the spec.
   *
   * @param tenant the tenant that will own the source
   * @param name the new source's name
   * @param spec the writable fields of the new source
   * @return the stored source as the server reports it
   * @throws SourceInvalidError when {@code tenant} or {@code name} is not a valid identifier
   *     (nothing is sent), or when the server rejects the spec
   * @throws SourceConflictError when a source with that name already exists
   * @throws SourceStoreReadOnlyError when the deployment's source store is read-only
   * @throws SourcesUnavailableError when the admin API is unreachable or answers with a body that
   *     is not the expected shape
   * @throws VersionMismatchError when an expected version was set and the server reports another
   */
  public Source createSource(String tenant, String name, SourceSpec spec) {
    validateTenant(tenant);
    validateName(name);
    ensureVersion();
    LinkedHashMap<String, Object> body = spec.toBody();
    body.put("name", name);
    TransportResponse response = request("POST", sourcesPath(tenant), Json.write(body));
    raiseForStatus(response);
    return decode(response, Source.class);
  }

  /**
   * Replaces a source: {@code PUT /v1/tenants/<tenant>/sources/<name>}.
   *
   * <p>The name comes from the path; the body never carries one.
   *
   * @param tenant the tenant that owns the source
   * @param name the source's name
   * @param spec the writable fields of the replacement
   * @return the stored source as the server reports it
   * @throws SourceInvalidError when {@code tenant} or {@code name} is not a valid identifier
   *     (nothing is sent), or when the server rejects the spec
   * @throws SourceNotFoundError when the tenant has no such source
   * @throws SourceConflictError when the change conflicts with the store
   * @throws SourceStoreReadOnlyError when the deployment's source store is read-only
   * @throws SourcesUnavailableError when the admin API is unreachable or answers with a body that
   *     is not the expected shape
   * @throws VersionMismatchError when an expected version was set and the server reports another
   */
  public Source updateSource(String tenant, String name, SourceSpec spec) {
    validateTenant(tenant);
    validateName(name);
    ensureVersion();
    byte[] body = Json.write(spec.toBody());
    TransportResponse response = request("PUT", sourcePath(tenant, name), body);
    raiseForStatus(response);
    return decode(response, Source.class);
  }

  /**
   * Deletes a source: {@code DELETE /v1/tenants/<tenant>/sources/<name>}.
   *
   * <p>The response body is ignored. A source that still has undelivered hooks on the server's node
   * is {@code 409 source_has_deliveries}; this method never sends the admin API's {@code
   * ?deliveries=dead_letter}.
   *
   * @param tenant the tenant that owns the source
   * @param name the source's name
   * @throws SourceInvalidError when {@code tenant} or {@code name} is not a valid identifier;
   *     nothing is sent
   * @throws SourceNotFoundError when the tenant has no such source
   * @throws SourceConflictError when the source still has undelivered hooks ({@code body()} carries
   *     {@code error: source_has_deliveries} and the {@code pending}/{@code inflight} counts)
   * @throws SourceStoreReadOnlyError when the deployment's source store is read-only
   * @throws SourcesUnavailableError when the admin API is unreachable
   * @throws VersionMismatchError when an expected version was set and the server reports another
   */
  public void deleteSource(String tenant, String name) {
    validateTenant(tenant);
    validateName(name);
    ensureVersion();
    TransportResponse response = request("DELETE", sourcePath(tenant, name), null);
    raiseForStatus(response);
  }

  /** The latch: fetch once when an expected version is set, then re-check the cached value. */
  private void ensureVersion() {
    if (expectedVersion != null) {
      checkVersion(cachedVersion());
    }
  }

  private void checkVersion(String actual) {
    String expected = expectedVersion;
    if (expected != null && !expected.equals(actual)) {
      throw new VersionMismatchError(expected, actual);
    }
  }

  /** The cached version, fetching and caching it if this is the first call. */
  private String cachedVersion() {
    String cached = serverVersion;
    if (cached != null) {
      return cached;
    }
    synchronized (this) {
      if (serverVersion == null) {
        serverVersion = fetchVersion();
      }
      return serverVersion;
    }
  }

  /** GET /health, for its {@code version} field. */
  private String fetchVersion() {
    TransportResponse response = request("GET", "/health", null);
    int status = response.status();

    if (status != 200) {
      throw new SourcesUnavailableError(
          "ankusa admin API health check failed (" + status + ")",
          status,
          Json.errorBody(response.body()));
    }

    Map<String, @Nullable Object> data;
    try {
      data = Json.readObject(response.body());
    } catch (JacksonException e) {
      throw new SourcesUnavailableError(
          "ankusa admin API health check returned a non-JSON body (" + status + ")",
          status,
          null,
          e);
    }

    Object version = data == null ? null : data.get("version");
    if (!(version instanceof String text)) {
      throw new SourcesUnavailableError(
          "ankusa admin API health check returned no version (" + status + ")", status, data);
    }
    return text;
  }

  /** Sends one request, turning every transport failure into an unavailable API. */
  private TransportResponse request(String method, String path, byte @Nullable [] jsonBody) {
    try {
      return http.send(method, path, jsonBody);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SourcesUnavailableError("ankusa admin API unreachable", null, null, e);
    } catch (IOException e) {
      throw new SourcesUnavailableError("ankusa admin API unreachable", null, null, e);
    }
  }

  /** Turns any non-2xx response into the matching sources error. */
  private static void raiseForStatus(TransportResponse response) {
    int status = response.status();
    if (status >= 200 && status < 300) {
      return;
    }

    Object body = Json.errorBody(response.body());

    if (status == 404) {
      throw new SourceNotFoundError("source not found (" + status + ")", status, body);
    }
    if (status == 400) {
      throw new SourceInvalidError(invalidMessage(body, status), status, body);
    }
    if (status == 409) {
      if (body instanceof Map<?, ?> map && "source_store_read_only".equals(map.get("error"))) {
        throw new SourceStoreReadOnlyError(
            "source store is read-only (" + status + ")", status, body);
      }
      throw new SourceConflictError("source already exists (" + status + ")", status, body);
    }

    throw new SourcesUnavailableError("ankusa admin API error (" + status + ")", status, body);
  }

  /** The 400 message: the server's message, else its error code, else a rendered body. */
  private static String invalidMessage(@Nullable Object body, int status) {
    if (body instanceof Map<?, ?> map) {
      Object message = map.get("message");
      if (message instanceof String text) {
        return text;
      }
      Object error = map.get("error");
      if (error instanceof String code) {
        return code;
      }
    }
    return "invalid source (" + status + "): " + body;
  }

  /** Decodes a 2xx body, turning any decode failure into an unavailable API. */
  private static <T> T decode(TransportResponse response, Class<T> type) {
    try {
      return Json.read(response.body(), type);
    } catch (JacksonException | NullPointerException e) {
      int status = response.status();
      throw new SourcesUnavailableError(
          "ankusa admin API returned an invalid JSON body (" + status + ")", status, null, e);
    }
  }

  private static String sourcesPath(String tenant) {
    return "/v1/tenants/" + tenant + "/sources";
  }

  private static String sourcePath(String tenant, String name) {
    return "/v1/tenants/" + tenant + "/sources/" + name;
  }

  private static void validateTenant(@Nullable String tenant) {
    if (tenant == null || !SAFE_ID.matcher(tenant).matches()) {
      throw new SourceInvalidError("invalid tenant: \"" + tenant + "\"", null, null);
    }
  }

  private static void validateName(@Nullable String name) {
    if (name == null || !SAFE_ID.matcher(name).matches()) {
      throw new SourceInvalidError("invalid source name: \"" + name + "\"", null, null);
    }
  }

  /** The body of a list call: the tenant's sources under {@code entries}. */
  private record Entries(List<Source> entries) {

    /**
     * Rejects a body that carried no {@code entries} array.
     *
     * @throws NullPointerException when the body has no {@code entries} field
     */
    Entries {
      entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
    }
  }
}
