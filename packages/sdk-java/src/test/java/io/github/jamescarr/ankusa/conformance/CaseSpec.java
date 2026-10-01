package io.github.jamescarr.ankusa.conformance;

import java.util.Map;
import tools.jackson.databind.JsonNode;

/**
 * One conformance vector.
 *
 * <p>{@code input} and {@code expect} are maps of raw nodes rather than typed fields so that an
 * absent key stays distinguishable from a key whose value is JSON null — {@code routes.delete.ok}
 * is {@code "ok": null}, and {@code expect.requests} is only asserted when it is present.
 *
 * @param id the globally unique test name every SDK registers
 * @param feature the feature id this vector covers
 * @param operation which operation to call
 * @param input the operation's input
 * @param expect exactly one of {@code ok} or {@code error}, plus an optional {@code requests}
 */
record CaseSpec(
    String id,
    String feature,
    String operation,
    Map<String, JsonNode> input,
    Map<String, JsonNode> expect) {}
