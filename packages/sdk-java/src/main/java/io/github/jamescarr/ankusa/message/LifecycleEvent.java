package io.github.jamescarr.ankusa.message;

import io.github.jamescarr.ankusa.internal.Json;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;

/**
 * One lifecycle event: the CloudEvents 1.0 (structured mode) event an Ankusa instance publishes
 * when a source or a route is created, updated or deleted, when a tenant is erased, or when the
 * instance is restored from a backup.
 *
 * <p>It arrives as the body of a queue message from the instance's lifecycle sinks (content type
 * {@code application/cloudevents+json}, {@code source_id} {@code ankusa:lifecycle}): decode the
 * message with {@link Message#decode(String)}, redeem its body, and hand the text to {@link
 * #decode(String)}.
 *
 * <p>Delivery is at least once and in commit order. {@link #id()} is derived from the change, so
 * the same event always has the same id, and {@link #revision()} (per {@link #subject()}) lets a
 * consumer ignore a stale update. An optional field the event omitted or said {@code null} is null.
 *
 * @param specversion the CloudEvents version; always {@code 1.0}
 * @param id the event id, derived from the change
 * @param source {@code urn:ankusa:instance:<instance>}
 * @param type {@code io.ankusa.source.created}, {@code io.ankusa.route.updated}, …; a type this SDK
 *     does not know passes through
 * @param subject the source id, the route id, the erased tenant or the instance name
 * @param time when the change committed, as the wire says it (an RFC 3339 string, not parsed)
 * @param datacontenttype the data's content type, or null
 * @param data the entity as the admin API shows it, secrets redacted, or null when the event
 *     carries none
 * @param revision the subject's change counter, at least 1, or null
 * @param actor the principal that made the change ({@code system} for code), or null
 * @param instance the instance's name, or null
 * @param sequence the instance's audit sequence number, the decimal string it is on the wire, or
 *     null
 * @param replayid the replay job id, on an event a replay re-emitted, or null
 * @param traceparent the W3C {@code traceparent} of the span that emitted the event, or null
 * @param tracestate the W3C {@code tracestate} that goes with {@code traceparent}, or null
 */
public record LifecycleEvent(
    String specversion,
    String id,
    String source,
    String type,
    String subject,
    String time,
    @Nullable String datacontenttype,
    @Nullable JsonNode data,
    @Nullable Long revision,
    @Nullable String actor,
    @Nullable String instance,
    @Nullable String sequence,
    @Nullable String replayid,
    @Nullable String traceparent,
    @Nullable String tracestate) {

  /** The only {@code sequence} shape: decimal digits. */
  private static final Pattern DECIMAL = Pattern.compile("[0-9]+");

  /**
   * Decodes one lifecycle event, validating it in a fixed order; the first failure wins.
   *
   * <p>Each failure raises an {@link InvalidMessageError} ({@code invalid_json}, {@code
   * not_an_object}, {@code unsupported_version} when {@code specversion} is not {@code 1.0}, or
   * {@code invalid_field} naming the key). Keys and event types this SDK does not know are ignored.
   *
   * @param json the event text
   * @return the decoded event
   * @throws InvalidMessageError when the text is not a valid lifecycle event
   */
  public static LifecycleEvent decode(String json) {
    JsonNode root;
    try {
      root = Json.readTree(json.getBytes(StandardCharsets.UTF_8));
    } catch (JacksonException e) {
      throw new InvalidMessageError("invalid_json", null, "event is not valid JSON");
    }

    // Jackson reads empty (or blank) input as a missing node instead of failing.
    if (root.isMissingNode()) {
      throw new InvalidMessageError("invalid_json", null, "event is not valid JSON");
    }

    if (!root.isObject()) {
      throw new InvalidMessageError("not_an_object", null, "event is not a JSON object");
    }

    JsonNode specversion = root.get("specversion");
    if (specversion == null
        || !specversion.isString()
        || !"1.0".equals(specversion.stringValue())) {
      throw new InvalidMessageError("unsupported_version", null, "specversion is not 1.0");
    }

    String id = required(root, "id");
    String source = required(root, "source");
    String type = required(root, "type");
    String subject = required(root, "subject");
    String time = required(root, "time");
    String datacontenttype = optional(root, "datacontenttype");

    JsonNode data = root.get("data");
    if (data != null && data.isNull()) {
      data = null;
    }
    if (data != null && !data.isObject()) {
      throw new InvalidMessageError("invalid_field", "data", "data must be an object or null");
    }

    Long revision = null;
    JsonNode revisionNode = root.get("revision");
    if (revisionNode != null && !revisionNode.isNull()) {
      if (!revisionNode.isIntegralNumber() || revisionNode.longValue() < 1) {
        throw new InvalidMessageError(
            "invalid_field", "revision", "revision must be an integer >= 1, null, or absent");
      }
      revision = revisionNode.longValue();
    }

    String actor = optional(root, "actor");
    String instance = optional(root, "instance");

    String sequence = optional(root, "sequence");
    if (sequence != null && !DECIMAL.matcher(sequence).matches()) {
      throw new InvalidMessageError(
          "invalid_field", "sequence", "sequence must be a string of decimal digits");
    }

    return new LifecycleEvent(
        "1.0",
        id,
        source,
        type,
        subject,
        time,
        datacontenttype,
        data,
        revision,
        actor,
        instance,
        sequence,
        optional(root, "replayid"),
        optional(root, "traceparent"),
        optional(root, "tracestate"));
  }

  /** A required, non-empty string field. */
  private static String required(JsonNode root, String key) {
    JsonNode node = root.get(key);
    if (node == null || !node.isString() || node.stringValue().isEmpty()) {
      throw new InvalidMessageError("invalid_field", key, key + " must be a non-empty string");
    }
    return node.stringValue();
  }

  /** An optional string field: absent or null passes, anything that is not a string fails. */
  private static @Nullable String optional(JsonNode root, String key) {
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
}
