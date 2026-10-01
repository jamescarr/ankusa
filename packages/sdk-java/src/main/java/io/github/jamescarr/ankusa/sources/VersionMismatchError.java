package io.github.jamescarr.ankusa.sources;

/**
 * The deployment reports a different Ankusa version than the one the caller expected.
 *
 * <p>Raised by the optional version latch before any source request is made, and by {@link
 * SourcesClient#serverVersion()}. Never retryable — the deployment keeps reporting its own version.
 */
public final class VersionMismatchError extends SourcesError {

  /** The version the caller expected. */
  private final String expected;

  /** The version the server reported. */
  private final String actual;

  /**
   * Creates the error.
   *
   * @param expected the version the caller expected
   * @param actual the version the server reported
   */
  public VersionMismatchError(String expected, String actual) {
    super(
        "expected Ankusa version \"" + expected + "\", server reports \"" + actual + "\"",
        null,
        null);
    this.expected = expected;
    this.actual = actual;
  }

  /**
   * The version the caller expected.
   *
   * @return the expected version
   */
  public String expected() {
    return expected;
  }

  /**
   * The version the server reported.
   *
   * @return the reported version
   */
  public String actual() {
    return actual;
  }
}
