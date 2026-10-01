package io.github.jamescarr.ankusa.sources;

import org.jspecify.annotations.Nullable;

/**
 * A tenant or name is not a valid identifier, or the server rejected a spec with 400.
 *
 * <p>This is also the error every method raises before any request when a caller-supplied tenant or
 * name fails {@code [A-Za-z0-9_-]{1,64}}; in that case {@link #status()} and {@link #body()} are
 * both null.
 */
public final class SourceInvalidError extends SourcesError {

  /**
   * Creates the error.
   *
   * @param message the whole message, naming what was rejected
   * @param status the HTTP status the API answered, or null before a request was sent
   * @param body the response body decoded as JSON or text, or null when there was none
   */
  public SourceInvalidError(String message, @Nullable Integer status, @Nullable Object body) {
    super(message, status, body);
  }
}
