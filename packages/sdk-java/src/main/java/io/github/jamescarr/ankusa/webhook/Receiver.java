package io.github.jamescarr.ankusa.webhook;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * The receiver-side decision on one HTTP-sink delivery: is it signed, and what is its identity.
 *
 * <p>Ankusa's HTTP sink signs every delivery unless it is configured {@code allow_unsigned: true},
 * so a receiver is closed by default: without the sink's secret (or an explicit {@link
 * Options#unsigned()}) it refuses every delivery. Otherwise any host that can reach the receiver's
 * URL could inject hooks.
 */
public final class Receiver {

  private Receiver() {}

  /**
   * How {@link #receive} judges a delivery.
   *
   * @param secrets the sink's secrets ({@code whsec_} + base64, or any other string used as its own
   *     bytes); several during a rotation. With any, the delivery must verify, and {@code
   *     allowUnsigned} does not weaken that
   * @param allowUnsigned accept a delivery when no secret is configured, unverified
   * @param tolerance how far either side of now {@code webhook-timestamp} may be
   * @param now the instant to judge the timestamp against, or null for the clock
   */
  public record Options(
      List<String> secrets, boolean allowUnsigned, Duration tolerance, @Nullable Instant now) {

    /**
     * Copies {@code secrets}.
     *
     * @param secrets the sink's secrets
     * @param allowUnsigned accept a delivery when no secret is configured
     * @param tolerance how far either side of now {@code webhook-timestamp} may be
     * @param now the instant to judge against, or null for the clock
     */
    public Options {
      secrets = List.copyOf(secrets);
    }

    /**
     * Options that verify every delivery against {@code secrets}, within the default tolerance.
     *
     * @param secrets the sink's secrets
     * @return the options
     */
    public static Options signed(List<String> secrets) {
      return new Options(secrets, false, Signature.DEFAULT_TOLERANCE, null);
    }

    /**
     * Options that accept an unsigned delivery: no secret is configured, so nothing is verified.
     * Any host that can reach the receiver's URL can then inject hooks.
     *
     * @return the options
     */
    public static Options unsigned() {
      return new Options(List.of(), true, Signature.DEFAULT_TOLERANCE, null);
    }

    /**
     * A copy with another timestamp tolerance.
     *
     * @param tolerance how far either side of now {@code webhook-timestamp} may be
     * @return the options
     */
    public Options withTolerance(Duration tolerance) {
      return new Options(secrets, allowUnsigned, tolerance, now);
    }

    /**
     * A copy that judges timestamps against {@code now} instead of the clock.
     *
     * @param now the instant to judge against
     * @return the options
     */
    public Options withNow(Instant now) {
      return new Options(secrets, allowUnsigned, tolerance, now);
    }
  }

  /**
   * An accepted delivery.
   *
   * @param headers the delivery's {@code x-ankusa-*} identity
   * @param verified false only for an unsigned delivery {@code allowUnsigned} let through
   */
  public record Received(HookHeaders headers, boolean verified) {}

  /**
   * Judges one delivery.
   *
   * @param headers the request's headers, names in any case
   * @param body the raw body, exactly as received
   * @param options the receiver's configuration
   * @return the accepted delivery
   * @throws InvalidSignatureError when there is no valid signature (answer 401): {@code
   *     invalid_secret} when no secret is configured and unsigned deliveries are not allowed
   * @throws MissingHookIdError when {@code x-ankusa-id} is absent (answer 400); the signature is
   *     judged first
   */
  public static Received receive(
      Map<String, ? extends List<String>> headers, byte[] body, Options options) {
    boolean verified;
    if (!options.secrets().isEmpty()) {
      Instant at = options.now();
      Instant now = at == null ? Instant.now() : at;
      Signature.verify(headers, body, options.secrets(), options.tolerance(), now);
      verified = true;
    } else if (options.allowUnsigned()) {
      verified = false;
    } else {
      throw new InvalidSignatureError(
          "invalid_secret",
          null,
          "no secret configured: deliveries are signed, so the receiver needs the sink's secret"
              + " (or Options.unsigned())");
    }
    return new Received(HookHeaders.parse(headers), verified);
  }
}
