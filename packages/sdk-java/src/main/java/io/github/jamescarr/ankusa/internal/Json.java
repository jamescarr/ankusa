package io.github.jamescarr.ankusa.internal;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;

/**
 * Internal: not part of the API; may change in any release.
 *
 * <p>The one mapper the SDK encodes and decodes with. The wire format is snake_case, a null record
 * property is omitted rather than sent as JSON null, and an unknown field in a response is ignored
 * instead of failing the decode — the server may add fields in a minor release.
 */
public final class Json {

  private static final JsonMapper MAPPER =
      JsonMapper.builder()
          .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
          .changeDefaultPropertyInclusion(
              inclusion -> inclusion.withValueInclusion(JsonInclude.Include.NON_NULL))
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .build();

  private static final TypeReference<Map<String, Object>> OBJECT = new TypeReference<>() {};

  private Json() {}

  /**
   * Parses a body into a JSON tree, without binding it to a type.
   *
   * @param body the JSON bytes
   * @return the parsed tree
   * @throws JacksonException when {@code body} is not valid JSON; the message decoder turns that
   *     into its own error
   */
  public static JsonNode readTree(byte[] body) {
    return MAPPER.readTree(body);
  }

  /**
   * Encodes a request body.
   *
   * @param value the value to encode
   * @return the JSON bytes
   * @throws IllegalArgumentException when {@code value} cannot be encoded; that is caller misuse of
   *     an input record, not a failure of the request
   */
  public static byte[] write(Object value) {
    try {
      return MAPPER.writeValueAsBytes(value);
    } catch (JacksonException e) {
      throw new IllegalArgumentException("cannot encode the request body as JSON", e);
    }
  }

  /**
   * Decodes a response body into a record.
   *
   * @param body the JSON bytes
   * @param type the record type to decode into
   * @param <T> the decoded type
   * @return the decoded value
   * @throws JacksonException when {@code body} is not valid JSON of that shape; every caller turns
   *     it into its family's unavailable error
   */
  public static <T> T read(byte[] body, Class<T> type) {
    return MAPPER.readValue(body, type);
  }

  /**
   * Decodes a response body into a JSON object.
   *
   * @param body the JSON bytes
   * @return the object's fields, in the order the body gave them
   * @throws JacksonException when {@code body} is not a JSON object
   */
  public static Map<String, @Nullable Object> readObject(byte[] body) {
    return MAPPER.readValue(body, OBJECT);
  }

  /**
   * Reads the value a rejected response carried, so an error can show what the server said.
   *
   * <p>Whatever the content type, a body that is valid JSON is decoded; anything else is returned
   * as text. An empty body becomes an empty string, which is the value Go and Python report too.
   *
   * @param body the response body
   * @return an empty string, the decoded JSON value, or the body as UTF-8 text
   */
  public static @Nullable Object errorBody(byte[] body) {
    if (body.length == 0) {
      return "";
    }
    try {
      JsonNode node = MAPPER.readTree(body);
      return MAPPER.treeToValue(node, Object.class);
    } catch (JacksonException e) {
      return new String(body, StandardCharsets.UTF_8);
    }
  }
}
