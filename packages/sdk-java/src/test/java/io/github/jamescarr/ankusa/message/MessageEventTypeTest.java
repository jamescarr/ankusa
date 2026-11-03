package io.github.jamescarr.ankusa.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** {@link Message#decode}'s {@code event_type} field. */
class MessageEventTypeTest {

  private static final String BASE =
      "{\"v\":1,\"id\":\"01a0\",\"source_id\":\"demo\",\"received_at\":1,\"size\":0,"
          + "\"body_base64\":\"\"%s}";

  @Test
  void absent_event_type_is_null() {
    assertNull(Message.decode(BASE.formatted("")).eventType());
  }

  @Test
  void a_null_event_type_is_null() {
    assertNull(Message.decode(BASE.formatted(",\"event_type\":null")).eventType());
  }

  @Test
  void an_event_type_is_the_providers_event_name() {
    Message message = Message.decode(BASE.formatted(",\"event_type\":\"invoice.paid\""));

    assertEquals("invoice.paid", message.eventType());
  }

  @Test
  void a_non_string_event_type_raises_invalid_field() {
    InvalidMessageError error =
        assertThrows(
            InvalidMessageError.class, () -> Message.decode(BASE.formatted(",\"event_type\":7")));

    assertEquals("invalid_field", error.code());
    assertEquals("event_type", error.field());
  }
}
