package io.github.jamescarr.ankusa.sources;

import org.jspecify.annotations.Nullable;

/**
 * The deployment's source store is a static seed, so the admin API refused the write.
 *
 * <p>Never retryable — the same store answers the same way until an operator changes it.
 */
public final class SourceStoreReadOnlyError extends SourcesError {

  /**
   * Creates the error.
   *
   * @param message the whole message, saying the store is read-only
   * @param status the HTTP status the API answered
   * @param body the response body decoded as JSON or text
   */
  public SourceStoreReadOnlyError(String message, @Nullable Integer status, @Nullable Object body) {
    super(message, status, body);
  }
}
