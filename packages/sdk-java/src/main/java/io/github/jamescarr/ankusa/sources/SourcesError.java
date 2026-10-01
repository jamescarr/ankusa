package io.github.jamescarr.ankusa.sources;

import io.github.jamescarr.ankusa.AnkusaException;
import org.jspecify.annotations.Nullable;

/**
 * The base of every error {@link SourcesClient} raises; no other error escapes it.
 *
 * <p>Sealed, so a catch of this type is exhaustive: the six subclasses below are everything a
 * sources call can raise. Each carries the HTTP status and decoded body of the response that
 * produced it, both null when no response was involved, as in a transport failure.
 */
public abstract sealed class SourcesError extends AnkusaException
    permits SourceNotFoundError,
        SourceConflictError,
        SourceStoreReadOnlyError,
        SourceInvalidError,
        SourcesUnavailableError,
        VersionMismatchError {

  /** The HTTP status the API answered, or null when no response was involved. */
  private final @Nullable Integer status;

  /** The response body decoded as JSON or text, or null when there was none. */
  private final @Nullable Object body;

  /**
   * Creates an error for a response the API returned.
   *
   * @param message the failure, already formatted
   * @param status the HTTP status, or null when no response was involved
   * @param body the response body decoded as JSON or text, or null when there was none
   */
  protected SourcesError(String message, @Nullable Integer status, @Nullable Object body) {
    super(message);
    this.status = status;
    this.body = body;
  }

  /**
   * Creates an error with the failure underneath it.
   *
   * @param message the failure, already formatted
   * @param status the HTTP status, or null when no response was involved
   * @param body the response body decoded as JSON or text, or null when there was none
   * @param cause the transport or decode failure, or null when there is none
   */
  protected SourcesError(
      String message, @Nullable Integer status, @Nullable Object body, @Nullable Throwable cause) {
    super(message, cause);
    this.status = status;
    this.body = body;
  }

  /**
   * The HTTP status the API answered.
   *
   * @return the status, or null when no response was involved
   */
  public @Nullable Integer status() {
    return status;
  }

  /**
   * What the API said.
   *
   * @return the body decoded as JSON or text, or null when there was none
   */
  public @Nullable Object body() {
    return body;
  }
}
