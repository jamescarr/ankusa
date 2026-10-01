package io.github.jamescarr.ankusa.routes;

import org.jspecify.annotations.Nullable;

/**
 * The listener answered a 4xx other than 404: it understood the request and refused it.
 *
 * <p>What the body said is on {@link #code()}, {@link #field()}, {@link #detail()}, {@link
 * #conflictingId()} and {@link #maxRoutes()}. Never retryable — the same request will be refused
 * the same way.
 */
public final class RoutesRejectedError extends RoutesError {

  /** The HTTP status the listener answered. */
  private final int status;

  /** The error code the body named, or null. */
  private final @Nullable String code;

  /** The field the body blamed, or null. */
  private final @Nullable String field;

  /** The message the body carried, or null. */
  private final @Nullable String detail;

  /** The id the body named as conflicting, or null. */
  private final @Nullable String conflictingId;

  /** The route limit the body named, or null. */
  private final @Nullable Integer maxRoutes;

  /**
   * Creates the error.
   *
   * @param status the HTTP status the listener answered
   * @param code the error code the body named, or null
   * @param field the field the body blamed, or null
   * @param detail the message the body carried, or null
   * @param conflictingId the id the body named as conflicting, or null
   * @param maxRoutes the route limit the body named, or null
   */
  public RoutesRejectedError(
      int status,
      @Nullable String code,
      @Nullable String field,
      @Nullable String detail,
      @Nullable String conflictingId,
      @Nullable Integer maxRoutes) {
    super(message(status, code, detail));
    this.status = status;
    this.code = code;
    this.field = field;
    this.detail = detail;
    this.conflictingId = conflictingId;
    this.maxRoutes = maxRoutes;
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
   * The error code the body named.
   *
   * @return the code, or null when the body named none
   */
  public @Nullable String code() {
    return code;
  }

  /**
   * The field the body blamed.
   *
   * @return the field name, or null when the body blamed none
   */
  public @Nullable String field() {
    return field;
  }

  /**
   * The message the body carried.
   *
   * @return the detail, or null when the body carried none
   */
  public @Nullable String detail() {
    return detail;
  }

  /**
   * The id the body named as conflicting.
   *
   * @return the conflicting id, or null when the body named none
   */
  public @Nullable String conflictingId() {
    return conflictingId;
  }

  /**
   * The route limit the body named.
   *
   * @return the limit, or null when the body named none
   */
  public @Nullable Integer maxRoutes() {
    return maxRoutes;
  }

  /** Builds the message from the status and whichever of the code and detail are present. */
  private static String message(int status, @Nullable String code, @Nullable String detail) {
    StringBuilder message =
        new StringBuilder("routes listener rejected the request (").append(status).append(')');
    if (code != null) {
      message.append(": ").append(code);
    }
    if (detail != null) {
      message.append(": ").append(detail);
    }
    return message.toString();
  }
}
