package io.github.jamescarr.ankusa.conformance;

import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * A vector {@code Body}: {@code text} (UTF-8), {@code base64}, or {@code json}, exactly one.
 *
 * @param text the body as UTF-8 text, or null
 * @param base64 the body as base64, or null
 * @param json the body as a JSON value, or null
 */
record BodySpec(@Nullable String text, @Nullable String base64, @Nullable JsonNode json) {}
