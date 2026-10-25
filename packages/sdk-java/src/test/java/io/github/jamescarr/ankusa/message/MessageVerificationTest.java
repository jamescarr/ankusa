package io.github.jamescarr.ankusa.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jamescarr.ankusa.webhook.Verification;
import org.junit.jupiter.api.Test;

/** {@link Message#decode}'s {@code verification} field. */
class MessageVerificationTest {

  private static final String BASE =
      "{\"v\":1,\"id\":\"01a0\",\"source_id\":\"demo\",\"received_at\":1,\"size\":0,"
          + "\"body_base64\":\"\"%s}";

  @Test
  void absent_verification_is_null() {
    Message message = Message.decode(BASE.formatted(""));

    assertNull(message.verification());
  }

  @Test
  void a_flagged_verification_object_decodes() {
    Message message =
        Message.decode(
            BASE.formatted(",\"verification\":{\"status\":\"failed\",\"flagged\":true}"));

    assertEquals(new Verification("failed", true), message.verification());
  }

  @Test
  void a_malformed_verification_raises_invalid_field() {
    InvalidMessageError error =
        assertThrows(
            InvalidMessageError.class,
            () -> Message.decode(BASE.formatted(",\"verification\":\"ok\"")));

    assertEquals("invalid_field", error.code());
    assertEquals("verification", error.field());
  }

  @Test
  void trusted_only_for_an_unflagged_ok() {
    assertTrue(
        Message.decode(BASE.formatted(",\"verification\":{\"status\":\"ok\",\"flagged\":false}"))
            .trusted());
    assertFalse(
        Message.decode(BASE.formatted(",\"verification\":{\"status\":\"failed\",\"flagged\":true}"))
            .trusted());
    assertFalse(
        Message.decode(
                BASE.formatted(",\"verification\":{\"status\":\"skipped\",\"flagged\":false}"))
            .trusted());
    assertFalse(Message.decode(BASE.formatted("")).trusted());
  }
}
