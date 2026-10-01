package io.github.jamescarr.ankusa.sources;

import org.jspecify.annotations.Nullable;

/**
 * The admin API could not be reached, or answered something no sources call expects.
 *
 * <p>The only retryable sources error: a transport failure, an unparseable body, and any status
 * outside 2xx, 400, 404 and 409 are conditions a later identical request can succeed at.
 */
public final class SourcesUnavailableError extends SourcesError {

  /**
   * Creates the error.
   *
   * @param message the whole message, naming what failed
   * @param status the HTTP status, or null when no response was involved
   * @param body the response body decoded as JSON or text, or null when there was none
   */
  public SourcesUnavailableError(String message, @Nullable Integer status, @Nullable Object body) {
    super(message, status, body);
  }

  /**
   * Creates the error with the failure underneath it.
   *
   * @param message the whole message, naming what failed
   * @param status the HTTP status, or null when no response was involved
   * @param body the response body decoded as JSON or text, or null when there was none
   * @param cause the transport or decode failure, or null when there is none
   */
  public SourcesUnavailableError(
      String message, @Nullable Integer status, @Nullable Object body, @Nullable Throwable cause) {
    super(message, status, body, cause);
  }

  /**
   * Whether an identical request made later could succeed.
   *
   * @return true, always
   */
  @Override
  public boolean retryable() {
    return true;
  }
}
