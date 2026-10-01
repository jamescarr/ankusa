package io.github.jamescarr.ankusa.sources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jamescarr.ankusa.ClientOptions;
import io.github.jamescarr.ankusa.FakeTransport;
import io.github.jamescarr.ankusa.Transport;
import io.github.jamescarr.ankusa.TransportRequest;
import io.github.jamescarr.ankusa.TransportResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * The source-management client: the request shapes, the version latch, the error mapping, and the
 * ids that must be refused before anything is sent.
 *
 * <p>No conformance vectors cover this feature, so this suite is the behavioural contract; it
 * mirrors {@code packages/sdk-python/tests/test_sources.py}.
 */
class SourcesClientTest {

  private static final String BASE = "http://admin.test";

  private static final String SOURCE_JSON =
      "{\"tenant\":\"acme\",\"name\":\"billing\",\"source_id\":\"src_1\","
          + "\"ingest_path\":\"/in/acme/billing\",\"on_verify_failure\":null,"
          + "\"sinks\":[{\"type\":\"log\"}]}";

  private static final String HEALTH = "{\"status\":\"ok\",\"version\":\"0.3.0\"}";

  /** The six ids no tenant, and no source name, may be. */
  private static final List<String> INVALID_IDS =
      List.of("..", "a/b", "a?b=1", "a#b", "a".repeat(65), "");

  /** Built per test method: JUnit makes a new instance for each one. */
  private final FakeTransport transport = new FakeTransport(SourcesClientTest::answer);

  private final SourcesClient client = new SourcesClient(BASE, options(transport));

  @Test
  void server_version_probes_health_once_and_caches_it() {
    assertEquals("0.3.0", client.serverVersion());
    assertEquals("0.3.0", client.serverVersion());

    assertEquals(1, transport.requests().size());
    assertEquals("/health", transport.requests().get(0).uri().getPath());
  }

  @Test
  void a_matching_expected_version_probes_health_then_lists() {
    SourcesClient checked = checked("0.3.0");

    assertEquals(1, checked.listSources("acme").size());
    assertEquals("/health", transport.requests().get(0).uri().getPath());
    assertEquals("/v1/tenants/acme/sources", transport.requests().get(1).uri().getPath());
  }

  @Test
  void a_mismatched_expected_version_is_reraised_from_the_cache() {
    SourcesClient checked = checked("9.9.9");

    VersionMismatchError first =
        assertThrows(VersionMismatchError.class, () -> checked.listSources("acme"));

    assertEquals("9.9.9", first.expected());
    assertEquals("0.3.0", first.actual());
    assertEquals("expected Ankusa version \"9.9.9\", server reports \"0.3.0\"", first.getMessage());
    assertNull(first.status());

    assertThrows(VersionMismatchError.class, () -> checked.getSource("acme", "billing"));
    assertEquals(1, transport.requests().size(), "the cached version was probed again");
  }

  @Test
  void list_sources_reads_entries_and_defaults_a_missing_verify() {
    List<Source> sources = client.listSources("acme");

    assertEquals(1, sources.size());
    assertEquals("acme", sources.get(0).tenant());
    assertEquals("src_1", sources.get(0).sourceId());
    assertEquals(Map.of("type", "none"), sources.get(0).verify());
    assertEquals(List.of(Map.of("type", "log")), sources.get(0).sinks());
    assertEquals(1, transport.requests().size(), "listing probed health with no expected version");
  }

  @Test
  void get_source_needs_no_health_probe_without_an_expected_version() {
    Source source = client.getSource("acme", "billing");

    assertEquals("billing", source.name());
    assertEquals("/v1/tenants/acme/sources/billing", transport.onlyRequest().uri().getPath());
  }

  @Test
  void create_posts_the_spec_followed_by_the_name() {
    client.createSource("acme", "billing", spec());

    TransportRequest request = transport.onlyRequest();
    assertEquals("POST", request.method());
    assertEquals("/v1/tenants/acme/sources", request.uri().getPath());
    assertEquals(
        "{\"sinks\":[{\"type\":\"log\"}],\"verify\":{\"type\":\"hmac\"},"
            + "\"on_verify_failure\":\"accept\",\"name\":\"billing\"}",
        body(request));
  }

  @Test
  void create_omits_an_unset_verify_and_failure_mode() {
    client.createSource(
        "acme", "billing", new SourceSpec(List.of(Map.of("type", "log")), null, null));

    assertEquals(
        "{\"sinks\":[{\"type\":\"log\"}],\"name\":\"billing\"}", body(transport.onlyRequest()));
  }

  @Test
  void update_puts_the_spec_and_never_the_name() {
    client.updateSource("acme", "billing", spec());

    TransportRequest request = transport.onlyRequest();
    assertEquals("PUT", request.method());
    assertEquals("/v1/tenants/acme/sources/billing", request.uri().getPath());
    assertEquals(
        "{\"sinks\":[{\"type\":\"log\"}],\"verify\":{\"type\":\"hmac\"},"
            + "\"on_verify_failure\":\"accept\"}",
        body(request));
  }

  @Test
  void delete_sends_no_body_and_accepts_a_204() {
    client.deleteSource("acme", "billing");

    TransportRequest request = transport.onlyRequest();
    assertEquals("DELETE", request.method());
    assertEquals("/v1/tenants/acme/sources/billing", request.uri().getPath());
    assertNull(request.body());
    assertEquals(1, transport.requests().size());
  }

  @Test
  void a_valid_id_travels_verbatim() {
    client.listSources("acme-corp");
    client.deleteSource("acme-corp", "my_source-1");

    assertEquals("/v1/tenants/acme-corp/sources", transport.requests().get(0).uri().getPath());
    assertEquals(
        "/v1/tenants/acme-corp/sources/my_source-1", transport.requests().get(1).uri().getPath());
  }

  @Test
  void a_404_is_a_not_found_carrying_the_status_and_body() {
    SourceNotFoundError error =
        assertThrows(SourceNotFoundError.class, () -> client.getSource("acme", "missing"));

    assertEquals(404, error.status());
    assertEquals("source_not_found", ((Map<?, ?>) error.body()).get("error"));
  }

  @Test
  void a_409_is_read_only_only_when_the_body_says_so() {
    SourceStoreReadOnlyError readOnly =
        assertThrows(
            SourceStoreReadOnlyError.class, () -> client.updateSource("acme", "read_only", spec()));
    assertEquals(409, readOnly.status());

    SourceConflictError conflict =
        assertThrows(
            SourceConflictError.class, () -> client.updateSource("acme", "conflict", spec()));
    assertEquals(409, conflict.status());
  }

  @Test
  void a_400_uses_the_servers_message_then_its_error_code() {
    SourceInvalidError withMessage =
        assertThrows(SourceInvalidError.class, () -> client.deleteSource("acme", "seeded"));
    assertEquals("cannot delete a seeded source", withMessage.getMessage());
    assertEquals(400, withMessage.status());

    SourceInvalidError withCode =
        assertThrows(SourceInvalidError.class, () -> client.deleteSource("acme", "coded"));
    assertEquals("invalid_tenant", withCode.getMessage());
    assertEquals(400, withCode.status());

    SourceInvalidError withNeither =
        assertThrows(SourceInvalidError.class, () -> client.deleteSource("acme", "opaque"));
    assertEquals("invalid source (400): {detail=nope}", withNeither.getMessage());
  }

  @Test
  void a_5xx_is_retryable_and_a_transport_failure_has_no_status() {
    SourcesUnavailableError busy =
        assertThrows(SourcesUnavailableError.class, () -> client.deleteSource("acme", "busy"));

    assertEquals(503, busy.status());
    assertTrue(busy.retryable(), busy.getMessage());

    Transport refusing =
        request -> {
          throw new IOException("connection refused");
        };
    SourcesClient down =
        new SourcesClient(BASE, ClientOptions.builder().transport(refusing).build());

    SourcesUnavailableError error =
        assertThrows(SourcesUnavailableError.class, () -> down.getSource("acme", "billing"));

    assertNull(error.status());
    assertNull(error.body());
    assertTrue(error.retryable(), error.getMessage());
  }

  @TestFactory
  Stream<DynamicTest> every_method_rejects_an_invalid_tenant_before_sending_anything() {
    List<DynamicTest> tests = new ArrayList<>();

    for (String invalid : INVALID_IDS) {
      for (String method :
          List.of("listSources", "getSource", "createSource", "updateSource", "deleteSource")) {
        tests.add(
            DynamicTest.dynamicTest(
                "tenant " + display(invalid) + " via " + method,
                () -> {
                  FakeTransport exploding = new FakeTransport(SourcesClientTest::never);
                  SourcesClient guarded = new SourcesClient(BASE, options(exploding));

                  SourceInvalidError error =
                      assertThrows(
                          SourceInvalidError.class, () -> call(guarded, method, invalid, true));

                  assertNull(error.status());
                  assertNull(error.body());
                  assertEquals("invalid tenant: \"" + invalid + "\"", error.getMessage());
                  assertEquals(List.of(), exploding.requests());
                }));
      }
    }

    return tests.stream();
  }

  @TestFactory
  Stream<DynamicTest> every_name_taking_method_rejects_an_invalid_name_before_sending_anything() {
    List<DynamicTest> tests = new ArrayList<>();

    for (String invalid : INVALID_IDS) {
      for (String method : List.of("getSource", "createSource", "updateSource", "deleteSource")) {
        tests.add(
            DynamicTest.dynamicTest(
                "name " + display(invalid) + " via " + method,
                () -> {
                  FakeTransport exploding = new FakeTransport(SourcesClientTest::never);
                  SourcesClient guarded = new SourcesClient(BASE, options(exploding));

                  SourceInvalidError error =
                      assertThrows(
                          SourceInvalidError.class, () -> call(guarded, method, invalid, false));

                  assertNull(error.status());
                  assertEquals("invalid source name: \"" + invalid + "\"", error.getMessage());
                  assertEquals(List.of(), exploding.requests());
                }));
      }
    }

    return tests.stream();
  }

  private SourcesClient checked(String expectedVersion) {
    return new SourcesClient(BASE, options(transport), expectedVersion);
  }

  private static ClientOptions options(FakeTransport transport) {
    return ClientOptions.builder().transport(transport).build();
  }

  private static SourceSpec spec() {
    return new SourceSpec(List.of(Map.of("type", "log")), Map.of("type", "hmac"), "accept");
  }

  /** Calls one of the five methods with {@code id} in the tenant slot, or the name slot. */
  private static void call(SourcesClient client, String method, String id, boolean tenant) {
    String other = "billing";
    String first = tenant ? id : "acme";
    String second = tenant ? other : id;

    switch (method) {
      case "listSources" -> client.listSources(first);
      case "getSource" -> client.getSource(first, second);
      case "createSource" -> client.createSource(first, second, spec());
      case "updateSource" -> client.updateSource(first, second, spec());
      case "deleteSource" -> client.deleteSource(first, second);
      default -> throw new AssertionError("unknown method " + method);
    }
  }

  private static String display(String id) {
    if (id.isEmpty()) {
      return "<empty>";
    }
    return id.length() > 8 ? id.substring(0, 8) + "..." : id;
  }

  private static String body(TransportRequest request) {
    return new String(request.body(), StandardCharsets.UTF_8);
  }

  /** The one server every test talks to; the path and method decide the answer. */
  private static TransportResponse answer(TransportRequest request) {
    String path = request.uri().getPath();
    String method = request.method();

    if (path.equals("/health")) {
      return FakeTransport.json(200, HEALTH);
    }
    if (path.endsWith("/missing")) {
      return FakeTransport.json(404, "{\"error\":\"source_not_found\"}");
    }
    if (path.endsWith("/read_only")) {
      return FakeTransport.json(409, "{\"error\":\"source_store_read_only\"}");
    }
    if (path.endsWith("/conflict")) {
      return FakeTransport.json(409, "{\"error\":\"source_exists\"}");
    }
    if (path.endsWith("/seeded")) {
      return FakeTransport.json(400, "{\"message\":\"cannot delete a seeded source\"}");
    }
    if (path.endsWith("/coded")) {
      return FakeTransport.json(400, "{\"error\":\"invalid_tenant\"}");
    }
    if (path.endsWith("/opaque")) {
      return FakeTransport.json(400, "{\"detail\":\"nope\"}");
    }
    if (path.endsWith("/busy")) {
      return FakeTransport.json(503, "{\"error\":\"boom\"}");
    }
    if ("DELETE".equals(method)) {
      return FakeTransport.empty(204);
    }
    // Only the collection read answers with an envelope; everything else answers with one source.
    if ("GET".equals(method) && path.endsWith("/sources")) {
      return FakeTransport.json(200, "{\"entries\":[" + SOURCE_JSON + "]}");
    }
    return FakeTransport.json(200, SOURCE_JSON);
  }

  private static TransportResponse never(TransportRequest request) {
    throw new AssertionError("a request was sent: " + request.method() + " " + request.uri());
  }
}
