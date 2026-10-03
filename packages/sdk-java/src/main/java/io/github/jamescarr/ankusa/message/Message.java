package io.github.jamescarr.ankusa.message;

import io.github.jamescarr.ankusa.claimcheck.InvalidClaimRefError;
import io.github.jamescarr.ankusa.claimcheck.ParsedClaimRef;
import io.github.jamescarr.ankusa.internal.Json;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;

/**
 * One v1 queue message, as it travels from Ankusa to a consumer over any broker.
 *
 * <p>A worker consuming Ankusa's queue receives the message as JSON text. {@link #decode(String)}
 * turns that text into a {@code Message}: it validates every field, decodes and verifies the body,
 * and either returns the message or raises {@link InvalidMessageError} naming the rule that failed.
 *
 * <p>The body arrives in one of two forms. A small hook travels inline as {@link #bodyBase64()}
 * (standard base64); a large one travels as a {@link #claim()} naming the claim-check store entry.
 * The two are mutually exclusive. {@link #sha256()} is the digest of the body — required for a
 * claim, optional for an inline body — and is what a consumer verifies after fetching a claim.
 *
 * <p>Dedupe with {@link #idempotencyKey(boolean)}: Ankusa delivers at least once, so a worker must
 * treat repeated deliveries of one event as one.
 *
 * <pre>{@code
 * String json = consumer.poll();          // the broker's payload, as text
 * Message message = Message.decode(json); // throws InvalidMessageError on a bad message
 *
 * byte[] body =
 *     message.claim() != null
 *         ? claimCheck.redeem(message.claim(), message.sha256())
 *         : message.body();
 * String key = message.idempotencyKey(false); // drops replays of events already processed
 * }</pre>
 *
 * @param v the message version; always {@code 1}
 * @param id the delivery id, a UUIDv7
 * @param sourceId the source that accepted the hook
 * @param tenantId the tenant the source belongs to, or null when the source is not tenant-scoped
 * @param receivedAt when Ankusa received the hook, in Unix milliseconds
 * @param contentType the media type Ankusa received, or null when absent
 * @param size the body's length in bytes
 * @param bodyBase64 the inline body as standard base64, or null when this is a claim message
 * @param claim the claim-check reference, or null when the body is inline
 * @param sha256 the body's lowercase-hex SHA-256, or null when an inline message carries none
 * @param dedupeKey the provider's event key, or null when the source extracts none
 * @param replayId the replay job id when this delivery is a replay, else null
 * @param headers the forwarded provider request headers, lower-cased names, never null
 */
public record Message(
    int v,
    String id,
    String sourceId,
    @Nullable String tenantId,
    long receivedAt,
    @Nullable String contentType,
    long size,
    @Nullable String bodyBase64,
    @Nullable String claim,
    @Nullable String sha256,
    @Nullable String dedupeKey,
    @Nullable String replayId,
    Map<String, String> headers) {

  /** A lowercase-hex SHA-256, the only digest shape a message may carry. */
  private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");

  /** Treats an absent header map as an empty one. */
  public Message {
    headers = headers == null ? Map.of() : Map.copyOf(headers);
  }

  /**
   * Decodes a queue message, validating it in the order the contract fixes.
   *
   * <p>The first failure wins; each raises an {@link InvalidMessageError} with a {@code code}
   * naming the rule and, for a type failure, the {@code field} it failed on. Unknown keys are
   * ignored, so a newer producer may add fields without breaking an older consumer.
   *
   * @param json the message text
   * @return the decoded message
   * @throws InvalidMessageError when the text is not a valid v1 message
   */
  public static Message decode(String json) {
    JsonNode root;
    try {
      root = Json.readTree(json.getBytes(StandardCharsets.UTF_8));
    } catch (JacksonException e) {
      throw new InvalidMessageError("invalid_json", null, "message is not valid JSON");
    }

    if (!root.isObject()) {
      throw new InvalidMessageError("not_an_object", null, "message is not a JSON object");
    }

    JsonNode version = root.get("v");
    if (version == null || !version.isIntegralNumber() || version.longValue() != 1L) {
      throw new InvalidMessageError("unsupported_version", null, "message version is not 1");
    }

    String id = requiredText(root, "id");
    if (id == null || id.isEmpty()) {
      throw new InvalidMessageError("invalid_field", "id", "id must be a non-empty string");
    }

    String sourceId = requiredText(root, "source_id");
    if (sourceId == null) {
      throw new InvalidMessageError("invalid_field", "source_id", "source_id must be a string");
    }

    JsonNode receivedAt = integral(root, "received_at");
    JsonNode size = integral(root, "size");
    if (size.longValue() < 0) {
      throw new InvalidMessageError("invalid_field", "size", "size must be an integer >= 0");
    }

    String tenantId = nullableText(root, "tenant_id");
    String contentType = nullableText(root, "content_type");
    String dedupeKey = nullableText(root, "dedupe_key");
    String replayId = nullableText(root, "replay_id");
    Map<String, String> headers = headers(root);
    String sha256 = sha256(root);

    JsonNode bodyNode = root.get("body_base64");
    JsonNode claimNode = root.get("claim");
    boolean hasBody = bodyNode != null && !bodyNode.isNull();
    boolean hasClaim = claimNode != null && !claimNode.isNull();

    if (hasBody && hasClaim) {
      throw new InvalidMessageError(
          "ambiguous_body", null, "message has both body_base64 and claim");
    }
    if (!hasBody && !hasClaim) {
      throw new InvalidMessageError(
          "missing_body", null, "message has neither body_base64 nor claim");
    }

    if (hasBody) {
      byte[] decoded = decodeBase64(bodyNode);
      if (decoded.length != size.longValue()) {
        throw new InvalidMessageError("size_mismatch", null, "body length does not match size");
      }
      if (sha256 != null && !sha256.equals(sha256Hex(decoded))) {
        throw new InvalidMessageError("integrity", null, "body sha256 does not match");
      }

      return new Message(
          1,
          id,
          sourceId,
          tenantId,
          receivedAt.longValue(),
          contentType,
          size.longValue(),
          Base64.getEncoder().encodeToString(decoded),
          null,
          sha256,
          dedupeKey,
          replayId,
          headers);
    }

    if (!claimNode.isString()) {
      throw new InvalidMessageError("invalid_field", "claim", "claim must be a string");
    }

    String claim = claimNode.stringValue();
    ParsedClaimRef ref;
    try {
      ref = ParsedClaimRef.parse(claim);
    } catch (InvalidClaimRefError e) {
      throw new InvalidMessageError(
          "invalid_field", "claim", "claim is not a valid claim reference");
    }

    if (sha256 == null) {
      throw new InvalidMessageError("invalid_field", "sha256", "a claim message requires sha256");
    }
    if (tenantId != null && !tenantId.equals(ref.tenantId())) {
      throw new InvalidMessageError(
          "tenant_mismatch", null, "claim tenant does not match tenant_id");
    }

    return new Message(
        1,
        id,
        sourceId,
        tenantId,
        receivedAt.longValue(),
        contentType,
        size.longValue(),
        null,
        claim,
        sha256,
        dedupeKey,
        replayId,
        headers);
  }

  /**
   * The key a worker dedupes on.
   *
   * <p>When the message carries a non-empty {@link #dedupeKey()}, the key is {@code
   * source_id:dedupe_key}, which collapses provider retries of one event; otherwise it is the
   * delivery {@link #id()}, which collapses broker redeliveries of one delivery. With {@code
   * includeReplay} and a {@link #replayId()}, the key gains a {@code #replay:<replay_id>} suffix,
   * so a replay of an event the worker already processed is processed again — omit it (false) to
   * drop replays, which is what a consumer that keeps a processed-ids table usually wants.
   *
   * @param includeReplay whether a replay should get a distinct key
   * @return the idempotency key
   */
  public String idempotencyKey(boolean includeReplay) {
    String key = dedupeKey != null && !dedupeKey.isEmpty() ? sourceId + ":" + dedupeKey : id;
    if (includeReplay && replayId != null) {
      key = key + "#replay:" + replayId;
    }
    return key;
  }

  /**
   * The inline body's bytes.
   *
   * @return the decoded body, or null when this is a claim message
   */
  public byte @Nullable [] body() {
    return bodyBase64 == null ? null : Base64.getDecoder().decode(bodyBase64);
  }

  /** A required string field, or null when it is absent or not a string. */
  private static @Nullable String requiredText(JsonNode root, String key) {
    JsonNode node = root.get(key);
    return node != null && node.isString() ? node.stringValue() : null;
  }

  /** A required integer field. */
  private static JsonNode integral(JsonNode root, String key) {
    JsonNode node = root.get(key);
    if (node == null || !node.isIntegralNumber()) {
      throw new InvalidMessageError("invalid_field", key, key + " must be an integer");
    }
    return node;
  }

  /** An optional string field: absent or null passes, anything that is not a string fails. */
  private static @Nullable String nullableText(JsonNode root, String key) {
    JsonNode node = root.get(key);
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isString()) {
      throw new InvalidMessageError(
          "invalid_field", key, key + " must be a string, null, or absent");
    }
    return node.stringValue();
  }

  /** Decodes the inline body, rejecting anything that is not standard base64. */
  private static byte[] decodeBase64(JsonNode node) {
    if (!node.isString()) {
      throw new InvalidMessageError("invalid_body_base64", null, "body_base64 is not a string");
    }
    try {
      return Base64.getDecoder().decode(node.stringValue());
    } catch (IllegalArgumentException e) {
      throw new InvalidMessageError("invalid_body_base64", null, "body_base64 is not valid base64");
    }
  }

  /** The header object, rejecting any value that is not a string. */
  private static Map<String, String> headers(JsonNode root) {
    JsonNode node = root.get("headers");
    if (node == null) {
      return Map.of();
    }
    if (!node.isObject()) {
      throw new InvalidMessageError("invalid_field", "headers", "headers must be an object");
    }

    Map<String, String> headers = new LinkedHashMap<>();
    for (Map.Entry<String, JsonNode> entry : node.properties()) {
      JsonNode value = entry.getValue();
      if (!value.isString()) {
        throw new InvalidMessageError("invalid_field", "headers", "header values must be strings");
      }
      headers.put(entry.getKey(), value.stringValue());
    }
    return headers;
  }

  /** The digest field: absent passes, anything that is not 64 lowercase hex fails. */
  private static @Nullable String sha256(JsonNode root) {
    JsonNode node = root.get("sha256");
    if (node == null) {
      return null;
    }
    if (!node.isString() || !SHA256.matcher(node.stringValue()).matches()) {
      throw new InvalidMessageError(
          "invalid_field", "sha256", "sha256 must be 64 lowercase hex characters");
    }
    return node.stringValue();
  }

  /** The lowercase-hex SHA-256 of the body. */
  private static String sha256Hex(byte[] body) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable on this JVM", e);
    }
  }
}
