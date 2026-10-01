package io.github.jamescarr.ankusa.claimcheck;

import org.jspecify.annotations.Nullable;

/**
 * The gateway answered a 4xx other than 404: it understood the reference and refused it.
 *
 * <p>What it said is on {@link #body()}. Never retryable — the same request will be refused the
 * same way.
 */
public final class ClaimRejectedError extends ClaimCheckError {

  /** The HTTP status the gateway answered. */
  private final int status;

  /** What the gateway said, decoded as JSON or kept as text. */
  private final @Nullable Object body;

  /**
   * Creates the error.
   *
   * @param status the HTTP status the gateway answered
   * @param body the response body decoded as JSON, or as text when it is not JSON
   */
  public ClaimRejectedError(int status, @Nullable Object body) {
    super("claim-check rejected redeem (" + status + "): " + body);
    this.status = status;
    this.body = body;
  }

  /**
   * The HTTP status the gateway answered.
   *
   * @return the status code
   */
  public int status() {
    return status;
  }

  /**
   * What the gateway said.
   *
   * @return the response body decoded as JSON, as text when it is not JSON, or an empty string when
   *     the body was empty
   */
  public @Nullable Object body() {
    return body;
  }
}
