package io.github.jamescarr.ankusa.message;

import io.github.jamescarr.ankusa.AnkusaException;
import org.jspecify.annotations.Nullable;

/**
 * A queue message did not decode: it is not a valid v1 message.
 *
 * <p>Never retryable — the same bytes will always fail the same way, so a consumer should
 * dead-letter them rather than requeue. {@link #code()} names the rule that failed and {@link
 * #field()} names the offending key when the failure was a field.
 */
public final class InvalidMessageError extends AnkusaException {

  /** The rule that failed. */
  private final String code;

  /** The offending key, or null when the failure was not tied to one field. */
  private final @Nullable String field;

  /**
   * Creates the error.
   *
   * @param code the rule that failed, e.g. {@code invalid_json} or {@code integrity}
   * @param field the offending key, or null when the failure was not tied to one field
   * @param message the whole message, naming what failed
   */
  public InvalidMessageError(String code, @Nullable String field, String message) {
    super(message);
    this.code = code;
    this.field = field;
  }

  /**
   * The rule that failed.
   *
   * @return the code, never null
   */
  public String code() {
    return code;
  }

  /**
   * The key the failure was tied to.
   *
   * @return the field name, or null when the failure was not tied to one field
   */
  public @Nullable String field() {
    return field;
  }
}
