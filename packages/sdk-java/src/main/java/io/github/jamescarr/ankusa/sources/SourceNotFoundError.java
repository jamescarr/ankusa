package io.github.jamescarr.ankusa.sources;

import org.jspecify.annotations.Nullable;

/**
 * The admin API answered 404: no source with that name exists for the tenant.
 *
 * <p>Never retryable — an identical request would find the same absence.
 */
public final class SourceNotFoundError extends SourcesError {

  /**
   * Creates the error.
   *
   * @param message the whole message, naming what was not found
   * @param status the HTTP status the API answered
   * @param body the response body decoded as JSON or text
   */
  public SourceNotFoundError(String message, @Nullable Integer status, @Nullable Object body) {
    super(message, status, body);
  }
}
