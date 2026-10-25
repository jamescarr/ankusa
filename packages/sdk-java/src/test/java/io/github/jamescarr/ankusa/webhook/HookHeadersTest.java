package io.github.jamescarr.ankusa.webhook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  @Test
  void trusted_only_for_an_unflagged_ok() {
    assertTrue(
        HookHeaders.parse(
                Map.of(
                    "x-ankusa-id", List.of("01a0"),
                    "x-ankusa-verification", List.of("ok")))
            .trusted());
    assertFalse(
        HookHeaders.parse(
                Map.of(
                    "x-ankusa-id", List.of("01a0"),
                    "x-ankusa-verification", List.of("failed"),
                    "x-ankusa-verification-flagged", List.of("true")))
            .trusted());
    assertFalse(
        HookHeaders.parse(
                Map.of(
                    "x-ankusa-id", List.of("01a0"),
                    "x-ankusa-verification", List.of("skipped")))
            .trusted());
    assertFalse(HookHeaders.parse(Map.of("x-ankusa-id", List.of("01a0"))).trusted());
  }
}
