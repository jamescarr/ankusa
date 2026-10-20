package io.github.jamescarr.ankusa.webhook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** {@link HookHeaders#parse}'s {@code verification} field. */
class HookHeadersTest {

  @Test
  void absent_verification_header_is_null() {
    HookHeaders headers =
        HookHeaders.parse(
            Map.of("x-ankusa-id", List.of("01a0"), "x-ankusa-source", List.of("demo")));

    assertNull(headers.verification());
  }

  @Test
  void a_flagged_header_of_true_is_flagged() {
    HookHeaders headers =
        HookHeaders.parse(
            Map.of(
                "x-ankusa-id", List.of("01a0"),
                "x-ankusa-source", List.of("demo"),
                "x-ankusa-verification", List.of("failed"),
                "x-ankusa-verification-flagged", List.of("true")));

    assertEquals(new Verification("failed", true), headers.verification());
  }
}
