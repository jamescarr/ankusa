package io.github.jamescarr.ankusa.webhook;

import io.github.jamescarr.ankusa.AnkusaException;
import org.jspecify.annotations.Nullable;

/**
 * A delivery whose Standard Webhooks signature does not verify.
 *
 * <p>Never retryable: answer 401; the sender retries with the same bytes, which will not verify
 * either. {@link #code()} is {@code invalid_secret}, {@code missing_header}, {@code
 * invalid_timestamp}, {@code timestamp_out_of_tolerance} or {@code no_matching_signature}; {@link
 * #field()} names the header at fault.
 */
public final class InvalidSignatureError extends AnkusaException {

  /** The check that failed. */
  private final String code;

  /** The header at fault, or null for {@code invalid_secret}. */
  private final @Nullable String field;

  /**
   * Creates the error.
   *
   * @param code the check that failed
   * @param field the header at fault, or null for {@code invalid_secret}
   * @param message the whole message, naming what failed
   */
  public InvalidSignatureError(String code, @Nullable String field, String message) {
    super(message);
    this.code = code;
    this.field = field;
  }

  /**
   * The check that failed.
   *
   * @return the code, never null
   */
  public String code() {
    return code;
  }

  /**
   * The header at fault.
   *
   * @return the header name, or null for {@code invalid_secret}
   */
  public @Nullable String field() {
    return field;
  }
}
