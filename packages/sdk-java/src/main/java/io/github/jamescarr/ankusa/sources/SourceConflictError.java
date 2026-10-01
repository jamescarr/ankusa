package io.github.jamescarr.ankusa.sources;

import org.jspecify.annotations.Nullable;

/**
 * The admin API answered 409: a source with that name already exists for the tenant.
 *
 * <p>Never retryable — an identical request would be refused the same way.
 */
public final class SourceConflictError extends SourcesError {

  /**
   * Creates the error.
   *
   * @param message the whole message, naming the conflict
   * @param status the HTTP status the API answered
   * @param body the response body decoded as JSON or text
   */
  public SourceConflictError(String message, @Nullable Integer status, @Nullable Object body) {
    super(message, status, body);
  }
}
