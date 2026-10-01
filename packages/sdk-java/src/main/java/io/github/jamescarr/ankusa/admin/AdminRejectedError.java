package io.github.jamescarr.ankusa.admin;

import org.jspecify.annotations.Nullable;

/**
 * The listener answered a {@code 4xx} other than a role refusal: it understood the request and
 * refused it.
 *
 * <p>Never retryable — the same request will be refused the same way. {@link #code()} names why,
 * when the body said.
 */
public final class AdminRejectedError extends AdminError {

  /** The HTTP status the listener answered. */
  private final int status;

  /** The body's {@code error} field, or null when the body named none. */
  private final @Nullable String code;

  /**
   * Creates the error.
   *
   * @param status the HTTP status the listener answered
   * @param code the body's {@code error} field, or null when the body named none
   */
  public AdminRejectedError(int status, @Nullable String code) {
    super(message(status, code));
    this.status = status;
    this.code = code;
  }

  /**
   * The HTTP status the listener answered.
   *
   * @return the status code
   */
  public int status() {
    return status;
  }

  /**
   * Why the listener refused the request.
   *
   * @return the body's {@code error} field, or null when the body named none
   */
  public @Nullable String code() {
    return code;
  }

  private static String message(int status, @Nullable String code) {
    String base = "admin listener rejected the request (" + status + ")";
    return code == null ? base : base + ": " + code;
  }
}
