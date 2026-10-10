package io.github.jamescarr.ankusa.conformance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

import io.github.jamescarr.ankusa.AnkusaException;
import io.github.jamescarr.ankusa.ClientOptions;
import io.github.jamescarr.ankusa.admin.AdminClient;
import io.github.jamescarr.ankusa.admin.AdminRejectedError;
import io.github.jamescarr.ankusa.admin.AdminUnavailableError;
import io.github.jamescarr.ankusa.admin.ListDeadLettersParams;
import io.github.jamescarr.ankusa.admin.ListQuarantinedParams;
import io.github.jamescarr.ankusa.admin.ReplayPatch;
import io.github.jamescarr.ankusa.admin.ReplaySpec;
import io.github.jamescarr.ankusa.admin.RoleNotEnabledError;
import io.github.jamescarr.ankusa.claimcheck.ClaimCheckClient;
import io.github.jamescarr.ankusa.claimcheck.ClaimCheckUnavailableError;
import io.github.jamescarr.ankusa.claimcheck.ClaimIntegrityError;
import io.github.jamescarr.ankusa.claimcheck.ClaimNotFoundError;
import io.github.jamescarr.ankusa.claimcheck.ClaimRejectedError;
import io.github.jamescarr.ankusa.claimcheck.InvalidClaimRefError;
import io.github.jamescarr.ankusa.claimcheck.ParsedClaimRef;
import io.github.jamescarr.ankusa.message.InvalidMessageError;
import io.github.jamescarr.ankusa.message.Message;
import io.github.jamescarr.ankusa.routes.DryRunRequest;
import io.github.jamescarr.ankusa.routes.InvalidRouteIdError;
import io.github.jamescarr.ankusa.routes.IpRules;
import io.github.jamescarr.ankusa.routes.ListRoutesParams;
import io.github.jamescarr.ankusa.routes.RouteInput;
import io.github.jamescarr.ankusa.routes.RouteNotFoundError;
import io.github.jamescarr.ankusa.routes.RoutePatch;
import io.github.jamescarr.ankusa.routes.RoutesClient;
import io.github.jamescarr.ankusa.routes.RoutesRejectedError;
import io.github.jamescarr.ankusa.routes.RoutesUnavailableError;
import io.github.jamescarr.ankusa.webhook.HookHeaders;
import io.github.jamescarr.ankusa.webhook.InvalidSignatureError;
import io.github.jamescarr.ankusa.webhook.MissingHookIdError;
import io.github.jamescarr.ankusa.webhook.Signature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * The language-neutral conformance runner: it loads every vector from {@code conformance/cases},
 * drives each one through this SDK's public surface only, and compares the result, the error and
 * the requests that were made.
 *
 * <p>{@code mise run check:conformance} runs it, and {@code conformance/README.md} is the runner
 * contract.
 */
public final class ConformanceTest {

  /** The directory the vectors live in; {@code build.sbt}'s Test/javaOptions sets it. */
  static final String CASES_PROPERTY = "ankusa.conformance.cases";

  /**
   * The runner's own mapper, not the SDK's: nulls are written, dates are ISO strings, and
   * round-tripping a value through it normalizes both sides of every comparison to the same JSON
   * node types.
   */
  private static final JsonMapper MAPPER =
      JsonMapper.builder()
          .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
          .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
          .build();

  /** Every error class a vector may name, by the name it names it with. */
  private static final Map<String, Class<? extends AnkusaException>> ERROR_CLASSES =
      Map.ofEntries(
          Map.entry("InvalidClaimRefError", InvalidClaimRefError.class),
          Map.entry("ClaimNotFoundError", ClaimNotFoundError.class),
          Map.entry("ClaimRejectedError", ClaimRejectedError.class),
          Map.entry("ClaimIntegrityError", ClaimIntegrityError.class),
          Map.entry("ClaimCheckUnavailableError", ClaimCheckUnavailableError.class),
          Map.entry("InvalidMessageError", InvalidMessageError.class),
          Map.entry("MissingHookIdError", MissingHookIdError.class),
          Map.entry("InvalidSignatureError", InvalidSignatureError.class),
          Map.entry("InvalidRouteIdError", InvalidRouteIdError.class),
          Map.entry("RouteNotFoundError", RouteNotFoundError.class),
          Map.entry("RoutesRejectedError", RoutesRejectedError.class),
          Map.entry("RoutesUnavailableError", RoutesUnavailableError.class),
          Map.entry("RoleNotEnabledError", RoleNotEnabledError.class),
          Map.entry("AdminRejectedError", AdminRejectedError.class),
          Map.entry("AdminUnavailableError", AdminUnavailableError.class));

  /**
   * One test per vector, named by the vector's own id.
   *
   * @return every vector in {@code conformance/cases}, sorted by file name
   */
  @TestFactory
  Stream<DynamicTest> vectors() {
    String directory = System.getProperty(CASES_PROPERTY);
    if (directory == null) {
      fail(
          "system property "
              + CASES_PROPERTY
              + " is not set, so the conformance vectors cannot be found");
    }

    Path cases = Path.of(directory);
    List<CaseSpec> vectors = new ArrayList<>();

    for (Path file : caseFiles(cases)) {
      vectors.addAll(MAPPER.readValue(read(file), CaseFile.class).cases());
    }

    if (vectors.isEmpty()) {
      fail("no conformance cases found under " + cases);
    }

    return vectors.stream()
        .map(vector -> DynamicTest.dynamicTest(vector.id(), () -> runCase(vector)));
  }

  private static List<Path> caseFiles(Path directory) {
    try (Stream<Path> entries = Files.list(directory)) {
      return entries
          .filter(entry -> entry.getFileName().toString().endsWith(".json"))
          .sorted()
          .toList();
    } catch (IOException e) {
      throw new IllegalStateException("cannot list the conformance cases in " + directory, e);
    }
  }

  private static byte[] read(Path file) {
    try {
      return Files.readAllBytes(file);
    } catch (IOException e) {
      throw new IllegalStateException("cannot read " + file, e);
    }
  }

  /** Runs one vector: dispatch it, then check its result, its error, and its requests. */
  private static void runCase(CaseSpec vector) {
    Recorder recorder = new Recorder();
    Object result = null;
    AnkusaException failure = null;

    try {
      result = dispatch(vector, recorder);
    } catch (AnkusaException e) {
      // Only the SDK's own errors are expected outcomes; anything else is a bug in the runner or
      // the SDK, and must fail rather than be compared.
      failure = e;
    }

    List<Recorder.Recorded> requests = recorder.snapshot();
    JsonNode expectedOk = vector.expect().get("ok");
    JsonNode expectedError = vector.expect().get("error");

    if (expectedOk != null && expectedError != null) {
      fail(vector.id() + ": expect has both ok and error");
    }

    if (expectedOk != null) {
      if (failure != null) {
        throw new AssertionError(
            vector.id()
                + ": expected ok, got "
                + failure.getClass().getSimpleName()
                + ": "
                + failure.getMessage(),
            failure);
      }
      JsonNode expected =
          "redeem".equals(vector.operation()) ? redeemExpectation(expectedOk) : expectedOk;
      assertEquals(expected, normalize(result), vector.id() + ": result");
    } else if (expectedError != null) {
      assertError(vector, failure, expectedError);
    } else if (failure != null) {
      throw new AssertionError(
          vector.id()
              + ": expected no error, got "
              + failure.getClass().getSimpleName()
              + ": "
              + failure.getMessage(),
          failure);
    }

    JsonNode expectedRequests = vector.expect().get("requests");
    if (expectedRequests != null) {
      assertRequests(vector, requests, expectedRequests);
    }
  }

  /** Runs one operation from the vector's {@code operation} field. */
  private static @Nullable Object dispatch(CaseSpec vector, Recorder recorder) {
    return switch (vector.operation()) {
      case "parse_claim_ref" -> ParsedClaimRef.parse(text(vector, "ref"));
      case "parse_headers" -> HookHeaders.parse(headerMap(vector));
      case "verify_signature" -> verifySignature(vector);
      case "decode_message" -> Message.decode(text(vector, "message"));
      case "idempotency_key" -> Map.of("key", idempotencyKey(vector));
      case "redeem" ->
          withClient(
              vector,
              recorder,
              ClaimCheckClient::new,
              claimCheck -> {
                byte[] body = claimCheck.redeem(text(vector, "ref"), text(vector, "sha256"));
                return Map.of("body", Map.of("base64", Base64.getEncoder().encodeToString(body)));
              });
      case "health" ->
          withClient(vector, recorder, ClaimCheckClient::new, ClaimCheckClient::health);
      case "routes_health" -> withClient(vector, recorder, RoutesClient::new, RoutesClient::health);
      case "routes_list" ->
          withClient(
              vector,
              recorder,
              RoutesClient::new,
              routes -> {
                ListRoutesParams params = optional(vector, "params", ListRoutesParams.class);
                return params == null ? routes.listRoutes() : routes.listRoutes(params);
              });
      case "routes_create" ->
          withClient(
              vector,
              recorder,
              RoutesClient::new,
              routes -> routes.createRoute(decode(vector, "input", RouteInput.class)));
      case "routes_get" ->
          withClient(
              vector, recorder, RoutesClient::new, routes -> routes.getRoute(text(vector, "id")));
      case "routes_replace" ->
          withClient(
              vector,
              recorder,
              RoutesClient::new,
              routes ->
                  routes.replaceRoute(
                      text(vector, "id"), decode(vector, "input", RouteInput.class)));
      case "routes_update" ->
          withClient(
              vector,
              recorder,
              RoutesClient::new,
              routes ->
                  routes.updateRoute(
                      text(vector, "id"), decode(vector, "patch", RoutePatch.class)));
      case "routes_delete" ->
          withClient(
              vector,
              recorder,
              RoutesClient::new,
              routes -> {
                routes.deleteRoute(text(vector, "id"));
                return null;
              });
      case "routes_ip_rules_get" ->
          withClient(vector, recorder, RoutesClient::new, RoutesClient::getIpRules);
      case "routes_ip_rules_put" ->
          withClient(
              vector,
              recorder,
              RoutesClient::new,
              routes -> routes.putIpRules(decode(vector, "rules", IpRules.class)));
      case "routes_test" ->
          withClient(
              vector,
              recorder,
              RoutesClient::new,
              routes -> routes.testRoute(decode(vector, "request", DryRunRequest.class)));
      case "admin_health" -> withClient(vector, recorder, AdminClient::new, AdminClient::health);
      case "admin_metrics" ->
          withClient(vector, recorder, AdminClient::new, admin -> Map.of("text", admin.metrics()));
      case "admin_config" -> withClient(vector, recorder, AdminClient::new, AdminClient::config);
      case "admin_dlq_list" ->
          withClient(
              vector,
              recorder,
              AdminClient::new,
              admin -> {
                ListDeadLettersParams params =
                    optional(vector, "params", ListDeadLettersParams.class);
                return params == null ? admin.listDeadLetters() : admin.listDeadLetters(params);
              });
      case "admin_replay_create" ->
          withClient(
              vector,
              recorder,
              AdminClient::new,
              admin -> admin.createReplay(decode(vector, "spec", ReplaySpec.class)));
      case "admin_replay_get" ->
          withClient(
              vector, recorder, AdminClient::new, admin -> admin.getReplay(text(vector, "id")));
      case "admin_replay_list" ->
          withClient(vector, recorder, AdminClient::new, AdminClient::listReplays);
      case "admin_replay_update" ->
          withClient(
              vector,
              recorder,
              AdminClient::new,
              admin ->
                  admin.updateReplay(
                      text(vector, "id"), decode(vector, "patch", ReplayPatch.class)));
      case "admin_quarantine" ->
          withClient(
              vector,
              recorder,
              AdminClient::new,
              admin -> {
                ListQuarantinedParams params =
                    optional(vector, "params", ListQuarantinedParams.class);
                return params == null ? admin.listQuarantined() : admin.listQuarantined(params);
              });
      default -> fail("unknown conformance operation \"" + vector.operation() + "\"");
    };
  }

  /** Starts the vector's gateway, builds the client, makes the call, and stops the gateway. */
  private static <C> Object withClient(
      CaseSpec vector,
      Recorder recorder,
      BiFunction<String, ClientOptions, C> factory,
      Function<C, @Nullable Object> call) {
    GatewaySpec gatewaySpec = decode(vector, "gateway", GatewaySpec.class);
    ClientSpec clientSpec = optional(vector, "client", ClientSpec.class);
    if (clientSpec == null) {
      clientSpec = new ClientSpec(Map.of(), null, null);
    }

    try (Gateway gateway = Gateway.start(gatewaySpec, clientSpec, recorder, MAPPER)) {
      return call.apply(factory.apply(gateway.baseUrl(), clientOptions(clientSpec, gateway)));
    }
  }

  private static ClientOptions clientOptions(ClientSpec spec, Gateway gateway) {
    ClientOptions.Builder options = ClientOptions.builder();
    spec.headers().forEach(options::header);

    if (spec.timeoutMs() != null && spec.timeoutMs() > 0) {
      options.timeout(Duration.ofMillis(spec.timeoutMs()));
    }
    if (gateway.transport() != null) {
      options.transport(gateway.transport());
    }

    return options.build();
  }

  /** The {@code parse_headers} input, with each name exactly as the vector spells it. */
  private static Map<String, List<String>> headerMap(CaseSpec vector) {
    JsonNode node = vector.input().get("headers");
    if (node == null) {
      return fail(vector.id() + ": input.headers is missing");
    }

    Map<String, String> given =
        MAPPER.treeToValue(node, new TypeReference<Map<String, String>>() {});
    Map<String, List<String>> headers = new LinkedHashMap<>();
    given.forEach((name, value) -> headers.put(name, List.of(value)));
    return headers;
  }

  /**
   * Runs {@code verify_signature} with the vector's headers, body, secrets, clock and tolerance.
   */
  private static Map<String, Object> verifySignature(CaseSpec vector) {
    JsonNode body = vector.input().get("body");
    byte[] bytes =
        body == null
            ? new byte[0]
            : Gateway.bodyBytes(MAPPER.treeToValue(body, BodySpec.class), MAPPER);
    List<String> secrets =
        MAPPER.treeToValue(vector.input().get("secrets"), new TypeReference<List<String>>() {});
    JsonNode tolerance = vector.input().get("tolerance_seconds");
    Signature.Verified verified =
        Signature.verify(
            headerMap(vector),
            bytes,
            secrets,
            tolerance == null
                ? Signature.DEFAULT_TOLERANCE
                : Duration.ofSeconds(tolerance.asLong()),
            Instant.ofEpochSecond(vector.input().get("now").asLong()));
    return Map.of("id", verified.id(), "timestamp", verified.timestamp());
  }

  /**
   * Runs the {@code idempotency_key} operation: decode the message (or parse the headers), then
   * compute the key with the vector's {@code include_replay}.
   */
  private static String idempotencyKey(CaseSpec vector) {
    boolean includeReplay = booleanInput(vector, "include_replay");
    if (vector.input().containsKey("headers")) {
      return HookHeaders.parse(headerMap(vector)).idempotencyKey(includeReplay);
    }
    return Message.decode(text(vector, "message")).idempotencyKey(includeReplay);
  }

  /** An optional boolean input, false when absent. */
  private static boolean booleanInput(CaseSpec vector, String field) {
    JsonNode node = vector.input().get(field);
    return node != null && node.asBoolean(false);
  }

  /**
   * Rebuilds a {@code redeem} expectation from {@code {"body": Body}} into {@code {"body":
   * {"base64": ...}}}: bytes cannot round-trip through JSON.
   */
  private static JsonNode redeemExpectation(JsonNode ok) {
    BodySpec body = MAPPER.treeToValue(ok.get("body"), BodySpec.class);
    ObjectNode expected = MAPPER.createObjectNode();
    expected
        .putObject("body")
        .put("base64", Base64.getEncoder().encodeToString(Gateway.bodyBytes(body, MAPPER)));
    return expected;
  }

  /**
   * Round-trips a value through JSON so that a record and the vector's raw nodes compare in one
   * domain, with identical number node types on both sides.
   */
  private static JsonNode normalize(@Nullable Object value) {
    return MAPPER.readTree(MAPPER.writeValueAsString(value));
  }

  private static void assertError(
      CaseSpec vector, @Nullable AnkusaException error, JsonNode expected) {
    if (error == null) {
      fail(vector.id() + ": expected error " + expected + ", got no error");
    }

    JsonNode classNode = expected.get("class");
    if (classNode == null || !classNode.isString()) {
      fail(vector.id() + ": expect.error has no class: " + expected);
    }

    String className = classNode.stringValue();
    Class<? extends AnkusaException> type = ERROR_CLASSES.get(className);
    if (type == null) {
      fail(vector.id() + ": unmapped conformance error class " + className);
    }

    assertSame(type, error.getClass(), vector.id() + ": error class");

    for (Map.Entry<String, JsonNode> field : expected.properties()) {
      String key = field.getKey();
      if (key.equals("class")) {
        continue;
      }
      assertEquals(
          field.getValue(),
          normalize(errorValue(error, key)),
          vector.id() + ": " + className + "." + key);
    }
  }

  /**
   * The value a vector's error key names.
   *
   * <p>A key the error class has no accessor for is a runner bug, not a mismatch, so it fails.
   */
  private static @Nullable Object errorValue(AnkusaException error, String key) {
    if (key.equals("retryable")) {
      return error.retryable();
    }

    if (error instanceof ClaimRejectedError rejected) {
      switch (key) {
        case "status":
          return rejected.status();
        case "body":
          return rejected.body();
        default:
          break;
      }
    } else if (error instanceof RoutesRejectedError rejected) {
      switch (key) {
        case "status":
          return rejected.status();
        case "code":
          return rejected.code();
        case "field":
          return rejected.field();
        case "message":
          return rejected.detail();
        case "conflicting_id":
          return rejected.conflictingId();
        case "max_routes":
          return rejected.maxRoutes();
        default:
          break;
      }
    } else if (error instanceof AdminRejectedError rejected) {
      switch (key) {
        case "status":
          return rejected.status();
        case "code":
          return rejected.code();
        default:
          break;
      }
    } else if (error instanceof InvalidSignatureError invalid) {
      switch (key) {
        case "code":
          return invalid.code();
        case "field":
          return invalid.field();
        default:
          break;
      }
    } else if (error instanceof InvalidMessageError invalid) {
      switch (key) {
        case "code":
          return invalid.code();
        case "field":
          return invalid.field();
        default:
          break;
      }
    } else if (error instanceof RoleNotEnabledError role && key.equals("role")) {
      return role.role();
    }

    return fail("no accessor for error key \"" + key + "\" on " + error.getClass().getSimpleName());
  }

  private static void assertRequests(
      CaseSpec vector, List<Recorder.Recorded> got, JsonNode expected) {
    if (!expected.isArray()) {
      fail(vector.id() + ": expect.requests is not an array: " + expected);
    }

    assertEquals(
        expected.size(), got.size(), vector.id() + ": request count (recorded " + got + ")");

    for (int i = 0; i < expected.size(); i++) {
      JsonNode want = expected.get(i);
      Recorder.Recorded actual = got.get(i);
      String where = vector.id() + ": request " + i;

      assertEquals(want.get("method").stringValue(), actual.method(), where + " method");
      assertEquals(want.get("path").stringValue(), actual.path(), where + " path");

      JsonNode headers = want.get("headers");
      if (headers != null) {
        for (Map.Entry<String, JsonNode> header : headers.properties()) {
          assertEquals(
              header.getValue().stringValue(),
              actual.headers().get(header.getKey()),
              where + " header " + header.getKey());
        }
      }

      if (want.has("body")) {
        assertEquals(want.get("body"), actual.body(), where + " body");
      }
    }
  }

  private static String text(CaseSpec vector, String field) {
    JsonNode node = vector.input().get(field);
    if (node == null || !node.isString()) {
      fail(vector.id() + ": input." + field + " is not a string: " + node);
    }
    return node.stringValue();
  }

  private static <T> T decode(CaseSpec vector, String field, Class<T> type) {
    T value = optional(vector, field, type);
    if (value == null) {
      fail(vector.id() + ": input." + field + " is missing");
    }
    return value;
  }

  private static <T> @Nullable T optional(CaseSpec vector, String field, Class<T> type) {
    JsonNode node = vector.input().get(field);
    if (node == null || node.isNull()) {
      return null;
    }
    return MAPPER.treeToValue(node, type);
  }
}
