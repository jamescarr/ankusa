package io.github.jamescarr.ankusa.webhook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * {@link HookHeaders#parse}'s {@code verification}, {@code receivedAt} and {@code bodySha256}
 * fields.
 */
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

  @Test
  void delivery_metadata_is_parsed() {
    String sha256 = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
    HookHeaders headers =
        HookHeaders.parse(
            Map.of(
                "x-ankusa-id", List.of("01a0"),
                "X-Ankusa-Received-At", List.of("1720000000000"),
                "x-ankusa-body-sha256", List.of(sha256)));

    assertEquals(1_720_000_000_000L, headers.receivedAt());
    assertEquals(sha256, headers.bodySha256());
  }

  @Test
  void malformed_delivery_metadata_is_null() {
    HookHeaders headers =
        HookHeaders.parse(
            Map.of(
                "x-ankusa-id", List.of("01a0"),
                "x-ankusa-received-at", List.of("-1"),
                "x-ankusa-body-sha256", List.of("ABC")));

    assertNull(headers.receivedAt());
    assertNull(headers.bodySha256());
  }
}
