package io.github.jamescarr.ankusa;

import org.jspecify.annotations.Nullable;

/**
 * The base of every error this SDK raises for a request that failed.
 *
 * <p>These are unchecked, so a caller that only wants the happy path does not have to wrap every
 * call in a try block. Each client family seals its own subclasses — {@code ClaimCheckError},
 * {@code RoutesError}, {@code AdminError}, {@code SourcesError} — and the sealed type is the
 * complete list that call can raise.
 *
 * <p>{@link #retryable()} is true only for a failure that a later identical request could succeed
 * at: an unreachable listener, a 5xx, or an unfollowed redirect. A rejected request, a missing
 * resource, and caller misuse are never retryable.
 *
 * <p>Caller misuse that can be detected without a request is not an {@code AnkusaException} at all:
 * an invalid base URL, a header the JDK client refuses to set, a body that cannot be encoded as
 * JSON, and a route id or source name that fails validation are {@link IllegalArgumentException},
 * raised before anything is sent.
 */
public abstract class AnkusaException extends RuntimeException {

  /**
   * Creates an exception carrying the message a caller sees.
   *
   * @param message the failure, already formatted
   */
  protected AnkusaException(String message) {
    super(message);
  }

  /**
   * Creates an exception carrying the message a caller sees and the failure underneath it.
   *
   * @param message the failure, already formatted
   * @param cause the underlying failure, or null when there is none
   */
  protected AnkusaException(String message, @Nullable Throwable cause) {
    super(message, cause);
  }

  /**
   * Whether an identical request made later could succeed.
   *
   * @return false; the four {@code *UnavailableError} classes override it to true
   */
  public boolean retryable() {
    return false;
  }
}
